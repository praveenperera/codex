use super::*;
use pretty_assertions::assert_eq;

#[test]
fn server_overloaded_retry_delay_doubles_then_stays_capped() {
    let delays = (1..=12)
        .map(server_overloaded_retry_delay)
        .collect::<Vec<_>>();

    assert_eq!(
        delays,
        vec![
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(8),
            Duration::from_secs(16),
            Duration::from_secs(32),
            Duration::from_secs(64),
            Duration::from_secs(128),
            Duration::from_secs(256),
            Duration::from_secs(256),
            Duration::from_secs(256),
            Duration::from_secs(256),
        ]
    );
    assert_eq!(
        server_overloaded_retry_delay(u64::MAX),
        Duration::from_secs(256)
    );
}
