use super::process::UnifiedExecProcess;
use crate::unified_exec::UnifiedExecError;
use codex_exec_server::ExecProcess;
use codex_exec_server::ExecProcessEventReceiver;
use codex_exec_server::ExecProcessFuture;
use codex_exec_server::ExecServerError;
use codex_exec_server::ProcessId;
use codex_exec_server::ProcessSignal;
use codex_exec_server::ReadResponse;
use codex_exec_server::StartedExecProcess;
use codex_exec_server::WriteResponse;
use codex_exec_server::WriteStatus;
use pretty_assertions::assert_eq;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::watch;

struct MockExecProcess {
    process_id: ProcessId,
    write_response: WriteResponse,
    read_responses: Mutex<VecDeque<ReadResponse>>,
    terminate_error: Option<String>,
    wake_tx: watch::Sender<u64>,
    signal_count: AtomicUsize,
    terminate_count: AtomicUsize,
    signal_blocker: Option<Arc<Notify>>,
    terminate_blocker: Option<Arc<Notify>>,
    terminate_started: Notify,
}

impl MockExecProcess {
    async fn read(&self) -> Result<ReadResponse, ExecServerError> {
        Ok(self
            .read_responses
            .lock()
            .await
            .pop_front()
            .unwrap_or(ReadResponse {
                chunks: Vec::new(),
                next_seq: 1,
                exited: false,
                exit_code: None,
                closed: false,
                failure: None,
                sandbox_denied: false,
            }))
    }

    async fn terminate(&self) -> Result<(), ExecServerError> {
        self.terminate_count.fetch_add(1, Ordering::SeqCst);
        self.terminate_started.notify_one();
        if let Some(blocker) = &self.terminate_blocker {
            blocker.notified().await;
        }
        if let Some(message) = &self.terminate_error {
            return Err(ExecServerError::Protocol(message.clone()));
        }
        Ok(())
    }
}

impl ExecProcess for MockExecProcess {
    fn process_id(&self) -> &ProcessId {
        &self.process_id
    }

    fn subscribe_wake(&self) -> watch::Receiver<u64> {
        self.wake_tx.subscribe()
    }

    fn subscribe_events(&self) -> ExecProcessEventReceiver {
        ExecProcessEventReceiver::empty()
    }

    fn read(
        &self,
        _after_seq: Option<u64>,
        _max_bytes: Option<usize>,
        _wait_ms: Option<u64>,
    ) -> ExecProcessFuture<'_, ReadResponse> {
        Box::pin(MockExecProcess::read(self))
    }

    fn write(&self, _chunk: Vec<u8>) -> ExecProcessFuture<'_, WriteResponse> {
        Box::pin(async { Ok(self.write_response.clone()) })
    }

    fn signal(&self, _signal: ProcessSignal) -> ExecProcessFuture<'_, ()> {
        self.signal_count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            if let Some(blocker) = &self.signal_blocker {
                blocker.notified().await;
            }
            Ok(())
        })
    }

    fn terminate(&self) -> ExecProcessFuture<'_, ()> {
        Box::pin(MockExecProcess::terminate(self))
    }
}

async fn remote_process(
    write_status: WriteStatus,
    terminate_error: Option<String>,
) -> UnifiedExecProcess {
    remote_process_with_handle(write_status, terminate_error)
        .await
        .0
}

async fn remote_process_with_handle(
    write_status: WriteStatus,
    terminate_error: Option<String>,
) -> (UnifiedExecProcess, Arc<MockExecProcess>) {
    remote_process_with_blockers(
        write_status,
        terminate_error,
        /*signal_blocker*/ None,
        /*terminate_blocker*/ None,
    )
    .await
}

async fn remote_process_with_blockers(
    write_status: WriteStatus,
    terminate_error: Option<String>,
    signal_blocker: Option<Arc<Notify>>,
    terminate_blocker: Option<Arc<Notify>>,
) -> (UnifiedExecProcess, Arc<MockExecProcess>) {
    let (wake_tx, _wake_rx) = watch::channel(0);
    let process = Arc::new(MockExecProcess {
        process_id: "test-process".to_string().into(),
        write_response: WriteResponse {
            status: write_status,
        },
        read_responses: Mutex::new(VecDeque::new()),
        terminate_error,
        wake_tx,
        signal_count: AtomicUsize::new(0),
        terminate_count: AtomicUsize::new(0),
        signal_blocker,
        terminate_blocker,
        terminate_started: Notify::new(),
    });
    let started = StartedExecProcess {
        process: process.clone(),
    };

    (
        UnifiedExecProcess::from_exec_server_started(started)
            .await
            .expect("remote process should start"),
        process,
    )
}

#[tokio::test]
async fn watchdog_with_zero_grace_hard_terminates_without_interrupt() {
    let (process, handle) =
        remote_process_with_handle(WriteStatus::Accepted, /*terminate_error*/ None).await;
    let process = Arc::new(process);

    process.start_watchdog(Some(crate::unified_exec::ExecCommandWatchdog {
        timeout_ms: 1,
        grace_period_ms: 0,
    }));
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        process.cancellation_token().cancelled(),
    )
    .await
    .expect("watchdog should terminate the process");

    assert_eq!(
        process.termination_reason(),
        Some(crate::unified_exec::ExecCommandTerminationReason::TimedOut)
    );
    assert_eq!(handle.signal_count.load(Ordering::SeqCst), 0);
    assert_eq!(handle.terminate_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn watchdog_interrupts_before_hard_termination_after_grace_period() {
    let (process, handle) =
        remote_process_with_handle(WriteStatus::Accepted, /*terminate_error*/ None).await;
    let process = Arc::new(process);

    process.start_watchdog(Some(crate::unified_exec::ExecCommandWatchdog {
        timeout_ms: 1,
        grace_period_ms: 10,
    }));
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        process.cancellation_token().cancelled(),
    )
    .await
    .expect("watchdog should terminate the process after grace");

    assert_eq!(handle.signal_count.load(Ordering::SeqCst), 1);
    assert_eq!(handle.terminate_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn watchdog_grace_bounds_stalled_remote_interrupt() {
    let signal_blocker = Arc::new(Notify::new());
    let (process, handle) = remote_process_with_blockers(
        WriteStatus::Accepted,
        /*terminate_error*/ None,
        Some(signal_blocker),
        /*terminate_blocker*/ None,
    )
    .await;
    let process = Arc::new(process);

    process.start_watchdog(Some(crate::unified_exec::ExecCommandWatchdog {
        timeout_ms: 1,
        grace_period_ms: 10,
    }));
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        process.cancellation_token().cancelled(),
    )
    .await
    .expect("stalled interrupt should not prevent hard termination");

    assert_eq!(handle.signal_count.load(Ordering::SeqCst), 1);
    assert_eq!(handle.terminate_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn watchdog_observes_remote_termination_before_reporting_completion() {
    let terminate_blocker = Arc::new(Notify::new());
    let (process, handle) = remote_process_with_blockers(
        WriteStatus::Accepted,
        /*terminate_error*/ None,
        /*signal_blocker*/ None,
        Some(Arc::clone(&terminate_blocker)),
    )
    .await;
    let process = Arc::new(process);

    process.start_watchdog(Some(crate::unified_exec::ExecCommandWatchdog {
        timeout_ms: 1,
        grace_period_ms: 0,
    }));
    handle.terminate_started.notified().await;
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(20),
            process.cancellation_token().cancelled(),
        )
        .await
        .is_err()
    );

    terminate_blocker.notify_waiters();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        process.cancellation_token().cancelled(),
    )
    .await
    .expect("completion should be released after remote termination finishes");
    assert_eq!(handle.terminate_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn watchdog_bounds_stalled_remote_termination() {
    let (process, handle) = remote_process_with_blockers(
        WriteStatus::Accepted,
        /*terminate_error*/ None,
        /*signal_blocker*/ None,
        Some(Arc::new(Notify::new())),
    )
    .await;
    let process = Arc::new(process);

    process.start_watchdog(Some(crate::unified_exec::ExecCommandWatchdog {
        timeout_ms: 1,
        grace_period_ms: 0,
    }));
    tokio::time::timeout(
        std::time::Duration::from_secs(6),
        process.cancellation_token().cancelled(),
    )
    .await
    .expect("stalled termination should be bounded");

    assert_eq!(handle.terminate_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        process.termination_reason(),
        Some(crate::unified_exec::ExecCommandTerminationReason::TimedOut)
    );
}

#[tokio::test]
async fn natural_exit_before_deadline_is_not_timed_out() {
    let (process, handle) =
        remote_process_with_handle(WriteStatus::UnknownProcess, /*terminate_error*/ None).await;
    let process = Arc::new(process);
    process
        .write(b"probe")
        .await
        .expect_err("unknown remote process should report a closed session");

    process.start_watchdog(Some(crate::unified_exec::ExecCommandWatchdog {
        timeout_ms: 1,
        grace_period_ms: 0,
    }));
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert_eq!(process.termination_reason(), None);
    assert_eq!(handle.terminate_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn remote_write_unknown_process_marks_process_exited() {
    let process = remote_process(WriteStatus::UnknownProcess, /*terminate_error*/ None).await;

    let err = process
        .write(b"hello")
        .await
        .expect_err("expected write failure");

    assert!(matches!(err, UnifiedExecError::WriteToStdin));
    assert!(process.has_exited());
}

#[tokio::test]
async fn remote_write_closed_stdin_marks_process_exited() {
    let process = remote_process(WriteStatus::StdinClosed, /*terminate_error*/ None).await;

    let err = process
        .write(b"hello")
        .await
        .expect_err("expected write failure");

    assert!(matches!(err, UnifiedExecError::WriteToStdin));
    assert!(process.has_exited());
}

#[tokio::test]
async fn fail_and_terminate_preserves_failure_message() {
    let process = remote_process(WriteStatus::Accepted, /*terminate_error*/ None).await;

    process.fail_and_terminate("network denied".to_string());
    process.fail_and_terminate("second failure".to_string());

    assert!(process.has_exited());
    assert_eq!(
        process.failure_message(),
        Some("network denied".to_string())
    );
}

#[tokio::test]
async fn remote_terminate_confirmed_updates_state_on_success_only() {
    let process = remote_process(
        WriteStatus::Accepted,
        Some("terminate unavailable".to_string()),
    )
    .await;

    let err = process
        .terminate_confirmed()
        .await
        .expect_err("expected terminate failure");

    assert!(matches!(err, UnifiedExecError::ProcessFailed { .. }));
    assert!(!process.has_exited());

    let process = remote_process(WriteStatus::Accepted, /*terminate_error*/ None).await;

    process
        .terminate_confirmed()
        .await
        .expect("terminate should succeed");

    assert!(process.has_exited());
}
