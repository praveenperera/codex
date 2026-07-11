use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::session::Session;

const EXEC_WAKEUP_BATCH_WINDOW: Duration = Duration::from_secs(5);

impl Session {
    pub(crate) fn schedule_exec_wakeup(self: &Arc<Self>) {
        if self
            .input_queue
            .exec_wakeup_timer_running
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        let session = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(EXEC_WAKEUP_BATCH_WINDOW).await;
            session.input_queue.finish_exec_wakeup_batch().await;
            session.maybe_start_turn_for_pending_work().await;
        });
    }
}
