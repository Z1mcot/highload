mod support;
use resilience::{
    Client, ClientError, ExponentialBackoff, FailureKind, ServerOutcome, SingleStrategy,
};
use support::*;
use tokio::time::Instant;

fn strategy(attempts: usize, initial_delay: u64, max_delay: u64) -> SingleStrategy {
    SingleStrategy::Backoff {
        budget: retry_budget(attempts),
        backoff: ExponentialBackoff {
            initial_delay: millis(initial_delay),
            max_delay: millis(max_delay),
        },
    }
}

#[tokio::test(start_paused = true)]
async fn doubles_delays_after_server_errors_and_timeouts_up_to_cap() {
    let sender = ScriptedSender::new(vec![
        immediate(ServerOutcome::ServerError),
        Step::Hang,
        immediate(ServerOutcome::ServerError),
        immediate(ServerOutcome::ServerError),
        immediate(success("ok")),
    ]);
    let client = Client::new(latency(200, 5), strategy(5, 10, 25)).unwrap();
    let started_at = Instant::now();
    assert_eq!(client.execute(&sender).await.unwrap().body, b"ok");
    let offsets: Vec<_> = sender
        .trace
        .times()
        .iter()
        .map(|time| *time - started_at)
        .collect();
    assert_eq!(
        offsets,
        [millis(0), millis(10), millis(35), millis(60), millis(85)]
    );
}

#[tokio::test(start_paused = true)]
async fn client_error_has_no_delay_and_does_not_reset_growth() {
    let sender = ScriptedSender::new(vec![
        immediate(ServerOutcome::ClientError),
        immediate(ServerOutcome::ServerError),
        immediate(ServerOutcome::ClientError),
        immediate(ServerOutcome::ServerError),
        immediate(success("ok")),
    ]);
    let client = Client::new(latency(100, 10), strategy(5, 10, 100)).unwrap();
    let started_at = Instant::now();
    client.execute(&sender).await.unwrap();
    let offsets: Vec<_> = sender
        .trace
        .times()
        .iter()
        .map(|time| *time - started_at)
        .collect();
    assert_eq!(
        offsets,
        [millis(0), millis(0), millis(10), millis(10), millis(30)]
    );
}

#[tokio::test(start_paused = true)]
async fn overall_deadline_during_backoff_prevents_next_attempt() {
    let sender = ScriptedSender::new(vec![immediate(ServerOutcome::ServerError)]);
    let client = Client::new(latency(15, 10), strategy(3, 20, 100)).unwrap();
    let started_at = Instant::now();
    assert!(matches!(client.execute(&sender).await,
        Err(ClientError::DeadlineExceeded { stats })
        if stats.attempts_started == 1 && stats.server_errors == 1));
    assert_eq!(started_at.elapsed(), millis(15));
    assert_eq!(sender.trace.order(), [0]);
}

#[tokio::test(start_paused = true)]
async fn exhausted_budget_returns_immediately_without_final_backoff() {
    let sender = ScriptedSender::new(vec![immediate(ServerOutcome::ServerError)]);
    let client = Client::new(latency(100, 10), strategy(1, 20, 100)).unwrap();
    let started_at = Instant::now();
    assert!(matches!(
        client.execute(&sender).await,
        Err(ClientError::BudgetExhausted {
            last_failure: FailureKind::ServerError,
            ..
        })
    ));
    assert_eq!(started_at.elapsed(), millis(0));
}

#[test]
fn rejects_zero_initial_delay_or_cap_below_initial_delay() {
    for invalid in [strategy(3, 0, 10), strategy(3, 20, 10), strategy(0, 10, 20)] {
        assert!(matches!(
            Client::new(latency(100, 10), invalid),
            Err(ClientError::InvalidConfiguration(_))
        ));
    }
}
