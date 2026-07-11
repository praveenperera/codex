use std::time::Duration;

use codex_context_fragments::ContextualUserFragment;
use pretty_assertions::assert_eq;

use super::ExecCommandCompletion;
use crate::unified_exec::ExecCommandTerminationReason;

#[test]
fn timed_out_completion_includes_termination_reason() {
    let completion = ExecCommandCompletion::new(
        "call-1".to_string(),
        42,
        "sleep 10".to_string(),
        -1,
        Duration::from_secs(1),
        String::new(),
        Some(ExecCommandTerminationReason::TimedOut),
    );

    assert_eq!(
        completion.body(),
        "\n<call_id>call-1</call_id>\n<session_id>42</session_id>\n<command>sleep 10</command>\n<exit_code>-1</exit_code>\n<termination_reason>timed_out</termination_reason>\n<duration_seconds>1.0000</duration_seconds>\n<output>\n\n</output>\n"
    );
}
