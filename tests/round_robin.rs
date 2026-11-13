mod support;
use resilience::{
    ClientError, ExponentialBackoff, MultiClient, MultiStrategy, ServerOutcome, SingleStrategy,
};
use std::sync::Arc;
use support::*;
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn advances_cursor_across_requests_and_wraps_around() {
    let trace = Arc::new(Trace::default());
    let mut client = MultiClient::new(
        latency(100, 10),
        MultiStrategy::RoundRobin(SingleStrategy::Default),
    )
    .unwrap();
    // Fresh sender objects represent the same positional servers on every call.
    for expected_index in [0, 1, 2, 0] {
        let senders = sender_list(
            vec![
                vec![immediate(success("a"))],
                vec![immediate(success("b"))],
                vec![immediate(success("c"))],
            ],
            &trace,
        );
        let response = client.execute(&senders).await.unwrap();
        assert_eq!(response.body, [b'a' + expected_index]);
    }
    assert_eq!(trace.order(), [0, 1, 2, 0]);
}

#[tokio::test(start_paused = true)]
async fn retries_rotate_after_client_server_and_timeout_failures() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![
            vec![
                immediate(ServerOutcome::ClientError),
                immediate(success("wrapped")),
            ],
            vec![immediate(ServerOutcome::ServerError)],
            vec![Step::Hang],
        ],
        &trace,
    );
    let mut client = MultiClient::new(
        latency(100, 10),
        MultiStrategy::RoundRobin(SingleStrategy::Retry(retry_budget(4))),
    )
    .unwrap();
    assert_eq!(client.execute(&senders).await.unwrap().body, b"wrapped");
    assert_eq!(trace.order(), [0, 1, 2, 0]);
}

#[tokio::test(start_paused = true)]
async fn composes_backoff_with_per_attempt_rotation() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![
            vec![immediate(ServerOutcome::ServerError)],
            vec![immediate(success("b"))],
        ],
        &trace,
    );
    let mut client = MultiClient::new(
        latency(100, 10),
        MultiStrategy::RoundRobin(SingleStrategy::Backoff {
            budget: retry_budget(2),
            backoff: ExponentialBackoff {
                initial_delay: millis(5),
                max_delay: millis(20),
            },
        }),
    )
    .unwrap();
    let started_at = Instant::now();
    client.execute(&senders).await.unwrap();
    assert_eq!(trace.order(), [0, 1]);
    assert_eq!(trace.times()[1] - started_at, millis(5));
}

#[tokio::test(start_paused = true)]
async fn returns_budget_exhaustion_after_full_rotation() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![
            vec![immediate(ServerOutcome::ServerError)],
            vec![immediate(ServerOutcome::ClientError)],
        ],
        &trace,
    );
    let mut client = MultiClient::new(
        latency(100, 10),
        MultiStrategy::RoundRobin(SingleStrategy::Retry(retry_budget(2))),
    )
    .unwrap();
    assert!(matches!(client.execute(&senders).await,
        Err(ClientError::BudgetExhausted { stats, .. }) if stats.attempts_started == 2));
    assert_eq!(trace.order(), [0, 1]);
}

#[tokio::test(start_paused = true)]
async fn all_attempts_share_one_deadline() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(vec![vec![Step::Hang], vec![Step::Hang]], &trace);
    let mut client = MultiClient::new(
        latency(15, 10),
        MultiStrategy::RoundRobin(SingleStrategy::Retry(retry_budget(5))),
    )
    .unwrap();
    let started_at = Instant::now();
    assert!(matches!(client.execute(&senders).await,
        Err(ClientError::DeadlineExceeded { stats })
        if stats.attempts_started == 2 && stats.timeouts == 1));
    assert_eq!(trace.order(), [0, 1]);
    assert_eq!(started_at.elapsed(), millis(15));
}

#[tokio::test(start_paused = true)]
async fn rejects_empty_sender_list_and_invalid_budget() {
    let mut client = MultiClient::new(
        latency(100, 10),
        MultiStrategy::RoundRobin(SingleStrategy::Default),
    )
    .unwrap();
    assert_eq!(client.execute(&[]).await, Err(ClientError::NoSenders));
    assert!(matches!(
        MultiClient::new(
            latency(100, 10),
            MultiStrategy::RoundRobin(SingleStrategy::Retry(retry_budget(0)))
        ),
        Err(ClientError::InvalidConfiguration(_))
    ));
}
