use super::ExecCommandWatchdog;

#[test]
fn watchdog_rejects_timeout_that_cannot_be_scheduled() {
    let error = serde_json::from_value::<ExecCommandWatchdog>(serde_json::json!({
        "timeout_ms": u64::MAX,
    }))
    .expect_err("oversized watchdog timeout should be rejected");

    assert!(
        error
            .to_string()
            .contains("timeout_ms must be between 1 and 9223372036854")
    );
}
