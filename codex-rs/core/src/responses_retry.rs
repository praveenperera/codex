//! Shared retry and transport fallback decisions for Responses requests.

use std::time::Duration;

use crate::client::ModelClientSession;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::util::backoff;
use codex_protocol::error::CodexErr;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::WarningEvent;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const SERVER_OVERLOADED_INITIAL_RETRY_DELAY_SECS: u64 = 1;
const SERVER_OVERLOADED_MAX_RETRY_DELAY_SECS: u64 = 256;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ResponsesStreamRequest {
    Sampling,
    RemoteCompactionV2,
}

pub(crate) async fn handle_server_overloaded_error(
    retries: &mut u64,
    err: CodexErr,
    sess: &Session,
    turn_context: &TurnContext,
    cancellation_token: &CancellationToken,
) -> Result<(), CodexErr> {
    if cancellation_token.is_cancelled() {
        return Err(CodexErr::TurnAborted);
    }

    *retries = retries.saturating_add(1);
    let retry_count = *retries;
    let delay = server_overloaded_retry_delay(retry_count);
    warn!(
        turn_id = %turn_context.sub_id,
        retry_count,
        ?delay,
        "model is at capacity; retrying sampling request after delay"
    );
    sess.notify_stream_error(
        turn_context,
        format!(
            "Model at capacity. Retrying in {}s (attempt {retry_count}).",
            delay.as_secs()
        ),
        err,
    )
    .await;

    tokio::select! {
        biased;
        _ = cancellation_token.cancelled() => Err(CodexErr::TurnAborted),
        _ = tokio::time::sleep(delay) => Ok(()),
    }
}

fn server_overloaded_retry_delay(retry_count: u64) -> Duration {
    let exponent = retry_count.saturating_sub(1).min(8) as u32;
    let delay_secs = SERVER_OVERLOADED_INITIAL_RETRY_DELAY_SECS << exponent;
    Duration::from_secs(delay_secs.min(SERVER_OVERLOADED_MAX_RETRY_DELAY_SECS))
}

/// Handles a retryable stream error and returns `Ok(())` when the caller should
/// retry the request loop.
pub(crate) async fn handle_retryable_response_stream_error(
    retries: &mut u64,
    max_retries: u64,
    err: CodexErr,
    client_session: &mut ModelClientSession,
    sess: &Session,
    turn_context: &TurnContext,
    request: ResponsesStreamRequest,
) -> Result<(), CodexErr> {
    if *retries >= max_retries
        && client_session.try_switch_fallback_transport(
            &turn_context.session_telemetry,
            &turn_context.model_info,
        )
    {
        sess.send_event(
            turn_context,
            EventMsg::Warning(WarningEvent {
                message: format!("Falling back from WebSockets to HTTPS transport. {err:#}"),
            }),
        )
        .await;
        *retries = 0;
        return Ok(());
    }

    if *retries < max_retries {
        *retries += 1;
        let retry_count = *retries;
        let delay = match &err {
            CodexErr::Stream(_, requested_delay) => {
                requested_delay.unwrap_or_else(|| backoff(retry_count))
            }
            _ => backoff(retry_count),
        };
        log_retry(request, turn_context, &err, retry_count, max_retries, delay);

        // In release builds, hide the first websocket retry notification to reduce noisy
        // transient reconnect messages. In debug builds, keep full visibility for diagnosis.
        let report_error = retry_count > 1
            || cfg!(debug_assertions)
            || !sess.services.model_client.responses_websocket_enabled();
        if report_error {
            // Surface retry information to any UI/front-end so the user understands what is
            // happening instead of staring at a seemingly frozen screen.
            sess.notify_stream_error(
                turn_context,
                format!("Reconnecting... {retry_count}/{max_retries}"),
                err,
            )
            .await;
        }
        tokio::time::sleep(delay).await;
        return Ok(());
    }

    Err(err)
}

fn log_retry(
    request: ResponsesStreamRequest,
    turn_context: &TurnContext,
    err: &CodexErr,
    retries: u64,
    max_retries: u64,
    delay: Duration,
) {
    match request {
        ResponsesStreamRequest::Sampling => {
            warn!(
                "stream disconnected - retrying sampling request ({retries}/{max_retries} in {delay:?})...",
            );
        }
        ResponsesStreamRequest::RemoteCompactionV2 => {
            warn!(
                turn_id = %turn_context.sub_id,
                retries,
                max_retries,
                compact_error = %err,
                "remote compaction v2 stream failed; retrying request after delay"
            );
        }
    }
}

#[cfg(test)]
#[path = "responses_retry_tests.rs"]
mod tests;
