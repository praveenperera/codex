use std::time::Duration;

use anyhow::Result;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::sse_failed;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_server_overloaded_until_success() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let response_mock = mount_sse_sequence(
        &server,
        vec![
            sse_failed(
                "resp-overloaded-1",
                "server_is_overloaded",
                "model is at capacity",
            ),
            sse_failed(
                "resp-overloaded-2",
                "slow_down",
                "model is still at capacity",
            ),
            sse(vec![
                ev_response_created("resp-success"),
                ev_assistant_message("msg-success", "capacity restored"),
                ev_completed("resp-success"),
            ]),
        ],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build(&server)
        .await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "wait for capacity".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;

    let mut retry_messages = Vec::new();
    let completed = loop {
        let event = timeout(Duration::from_secs(10), test.codex.next_event())
            .await
            .expect("timeout waiting for capacity retry")
            .expect("event stream ended unexpectedly")
            .msg;
        match event {
            EventMsg::StreamError(event) => retry_messages.push(event.message),
            EventMsg::Error(error) => panic!("unexpected terminal error: {error:?}"),
            EventMsg::TurnComplete(event) => break event,
            _ => {}
        }
    };

    assert_eq!(
        retry_messages,
        vec![
            "Model at capacity. Retrying in 1s (attempt 1).",
            "Model at capacity. Retrying in 2s (attempt 2).",
        ]
    );
    assert_eq!(completed.error, None);
    assert_eq!(
        completed.last_agent_message.as_deref(),
        Some("capacity restored")
    );
    assert_eq!(response_mock.requests().len(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_cancels_server_overloaded_retry_wait() -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let response_mock = mount_sse_sequence(
        &server,
        vec![sse_failed(
            "resp-overloaded",
            "server_is_overloaded",
            "model is at capacity",
        )],
    )
    .await;
    let test = test_codex()
        .with_config(|config| {
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(0);
        })
        .build(&server)
        .await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "cancel capacity wait".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;

    let EventMsg::StreamError(retry) = wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::StreamError(_))
    })
    .await
    else {
        unreachable!("predicate guarantees a stream error")
    };
    assert_eq!(
        retry.message,
        "Model at capacity. Retrying in 1s (attempt 1)."
    );

    test.codex.submit(Op::Interrupt).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnAborted(_))
    })
    .await;
    assert_eq!(response_mock.requests().len(), 1);

    Ok(())
}
