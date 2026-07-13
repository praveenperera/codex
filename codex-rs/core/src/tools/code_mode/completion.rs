use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use codex_code_mode::CellId;

use crate::context::CodeCellCompletion;
use crate::context::ContextualUserFragment;
use crate::session::session::Session;

#[derive(Default)]
pub(super) struct CodeCellCompletionBroker {
    state: Mutex<CompletionBrokerState>,
}

#[derive(Default)]
struct CompletionBrokerState {
    registrations: HashMap<CellId, Arc<CodeCellCompletionRegistration>>,
    completed_before_registration: HashSet<CellId>,
}

struct CodeCellCompletionRegistration {
    cell_id: CellId,
    session: Weak<Session>,
    state: Mutex<CompletionRegistrationState>,
    delivery_pending: AtomicBool,
}

#[derive(Default)]
struct CompletionRegistrationState {
    armed: bool,
    completed: bool,
    enqueued: bool,
    observed: bool,
}

impl CodeCellCompletionBroker {
    pub(super) fn register(&self, cell_id: CellId, session: Weak<Session>) {
        let registration = Arc::new(CodeCellCompletionRegistration::new(
            cell_id.clone(),
            session,
        ));
        let completed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let completed = state.completed_before_registration.remove(&cell_id);
            state
                .registrations
                .insert(cell_id, Arc::clone(&registration));
            completed
        };
        if completed {
            registration.complete();
        }
    }

    pub(super) fn arm(&self, cell_id: &CellId) {
        if let Some(registration) = self.registration(cell_id) {
            registration.arm();
        }
    }

    pub(super) fn begin_direct_observation(&self, cell_id: &CellId) {
        if let Some(registration) = self.remove_registration(cell_id) {
            registration.observed();
        }
    }

    pub(super) fn finish(&self, cell_id: &CellId) {
        let registration = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.completed_before_registration.remove(cell_id);
            state.registrations.remove(cell_id)
        };
        if let Some(registration) = registration {
            registration.observed();
        }
    }

    pub(super) fn mark_delivered(&self, cell_id: &CellId) {
        if let Some(registration) = self.remove_registration(cell_id) {
            registration.delivered();
        }
    }

    pub(super) fn has_pending_delivery(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .registrations
            .values()
            .any(|registration| registration.delivery_pending.load(Ordering::Acquire))
    }

    pub(super) fn completed(&self, cell_id: &CellId) {
        let registration = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match state.registrations.get(cell_id).cloned() {
                Some(registration) => Some(registration),
                None => {
                    state.completed_before_registration.insert(cell_id.clone());
                    None
                }
            }
        };
        if let Some(registration) = registration {
            registration.complete();
        }
    }

    fn registration(&self, cell_id: &CellId) -> Option<Arc<CodeCellCompletionRegistration>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .registrations
            .get(cell_id)
            .cloned()
    }

    fn remove_registration(&self, cell_id: &CellId) -> Option<Arc<CodeCellCompletionRegistration>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .registrations
            .remove(cell_id)
    }
}

impl CodeCellCompletionRegistration {
    fn new(cell_id: CellId, session: Weak<Session>) -> Self {
        Self {
            cell_id,
            session,
            state: Mutex::new(CompletionRegistrationState::default()),
            delivery_pending: AtomicBool::new(false),
        }
    }

    fn arm(self: &Arc<Self>) {
        self.delivery_pending.store(true, Ordering::Release);
        let should_enqueue = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.armed = true;
            if state.completed && !state.enqueued && !state.observed {
                state.enqueued = true;
                true
            } else {
                false
            }
        };
        if should_enqueue {
            self.enqueue();
        }
    }

    fn complete(self: &Arc<Self>) {
        let should_enqueue = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.completed = true;
            if state.armed && !state.enqueued && !state.observed {
                state.enqueued = true;
                true
            } else {
                false
            }
        };
        if should_enqueue {
            self.enqueue();
        }
    }

    fn enqueue(self: &Arc<Self>) {
        let registration = Arc::clone(self);
        tokio::spawn(async move {
            let Some(session) = registration.session.upgrade() else {
                return;
            };
            let fragment = CodeCellCompletion::new(registration.cell_id.to_string());
            session
                .input_queue
                .enqueue_code_cell_wakeup(
                    registration.cell_id.clone(),
                    ContextualUserFragment::into(fragment),
                )
                .await;
            if registration
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .observed
            {
                session
                    .input_queue
                    .remove_code_cell_wakeup(&registration.cell_id)
                    .await;
                return;
            }
            session.maybe_start_turn_for_pending_work().await;
        });
    }

    fn observed(&self) {
        self.delivery_pending.store(false, Ordering::Release);
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observed = true;
        self.remove_queued_wakeup();
    }

    fn delivered(&self) {
        self.delivery_pending.store(false, Ordering::Release);
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observed = true;
    }

    fn remove_queued_wakeup(&self) {
        let Some(session) = self.session.upgrade() else {
            return;
        };
        let cell_id = self.cell_id.clone();
        tokio::spawn(async move {
            session.input_queue.remove_code_cell_wakeup(&cell_id).await;
        });
    }
}
