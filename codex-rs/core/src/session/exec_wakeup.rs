use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::session::Session;

const EXEC_WAKEUP_BATCH_WINDOW: Duration = Duration::from_secs(5);

impl Session {
    pub(crate) fn record_exec_wakeup_event(&self, state: &'static str) {
        self.services.session_telemetry.counter(
            "codex.exec_wakeup",
            /*inc*/ 1,
            &[("state", state)],
        );
    }

    pub(crate) fn record_exec_wakeup_delivery_delay(&self, delay: Duration) {
        self.services.session_telemetry.histogram(
            "codex.exec_wakeup.delivery_delay_ms",
            i64::try_from(delay.as_millis()).unwrap_or(i64::MAX),
            &[],
        );
    }

    pub(crate) async fn has_pending_exec_wakeup_delivery(&self) -> bool {
        self.services
            .unified_exec_manager
            .has_pending_completion_wakeup()
            .await
    }

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
            session.record_exec_wakeup_event("batch_ready");
            session.maybe_start_turn_for_pending_work().await;
        });
    }
}
