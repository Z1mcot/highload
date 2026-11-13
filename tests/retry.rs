mod support;
use resilience::{Client, ClientError, FailureKind, RequestStats, ServerOutcome, SingleStrategy};
use support::*;
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn retries_each_failure_category_until_success() {
    let sender = ScriptedSender::new(vec![
        immediate(ServerOutcome::ClientError),
        immediate(ServerOutcome::ServerError),
        Step::Hang,
        immediate(success("recovered")),
    ]);
    let client = Client::new(latency(100, 10), SingleStrategy::Retry(retry_budget(4))).unwrap();
    assert_eq!(client.execute(&sender).await.unwrap().body, b"recovered");
    assert_eq!(sender.trace.order(), [0, 0, 0, 0]);
    assert_eq!(*sender.trace.completed.lock().unwrap(), 3);
    assert_eq!(*sender.trace.canceled.lock().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn stops_when_each_error_category_budget_is_reached() {
    for failure in [
        FailureKind::ClientError,
        FailureKind::ServerError,
        FailureKind::Timeout,
    ] {
        for limit in [0, 1, 2] {
            let mut budget = retry_budget(10);
            match failure {
                FailureKind::ClientError => budget.max_client_errors = limit,
                FailureKind::ServerError => budget.max_server_errors = limit,
                FailureKind::Timeout => budget.max_timeouts = limit,
            }
            let attempt_count = limit.max(1);
            let steps = (0..attempt_count)
                .map(|_| match failure {
                    FailureKind::ClientError => immediate(ServerOutcome::ClientError),
                    FailureKind::ServerError => immediate(ServerOutcome::ServerError),
                    FailureKind::Timeout => Step::Hang,
                })
                .collect();
            let sender = ScriptedSender::new(steps);
            let client = Client::new(latency(100, 10), SingleStrategy::Retry(budget)).unwrap();
            assert!(matches!(client.execute(&sender).await,
                Err(ClientError::BudgetExhausted { last_failure, stats })
                if last_failure == failure && stats.attempts_started == attempt_count));
            assert_eq!(sender.trace.order().len(), attempt_count);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn total_attempt_budget_counts_mixed_failures_and_does_not_start_extra_work() {
    let sender = ScriptedSender::new(vec![
        immediate(ServerOutcome::ClientError),
        immediate(ServerOutcome::ServerError),
        Step::Hang,
    ]);
    let client = Client::new(latency(100, 10), SingleStrategy::Retry(retry_budget(3))).unwrap();
    assert_eq!(
        client.execute(&sender).await,
        Err(ClientError::BudgetExhausted {
            last_failure: FailureKind::Timeout,
            stats: RequestStats {
                attempts_started: 3,
                client_errors: 1,
                server_errors: 1,
                timeouts: 1,
            },
        })
    );
    assert_eq!(sender.trace.order().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn overall_deadline_is_not_restarted_by_retry() {
    let sender = ScriptedSender::new(vec![Step::Hang, Step::Hang, Step::Hang]);
    let client = Client::new(latency(25, 10), SingleStrategy::Retry(retry_budget(10))).unwrap();
    let started_at = Instant::now();
    assert_eq!(
        client.execute(&sender).await,
        Err(ClientError::DeadlineExceeded {
            stats: RequestStats {
                attempts_started: 3,
                timeouts: 2,
                ..Default::default()
            },
        })
    );
    assert_eq!(started_at.elapsed(), millis(25));
    assert_eq!(*sender.trace.active.lock().unwrap(), 0);
}

#[test]
fn rejects_zero_total_attempts() {
    assert!(matches!(
        Client::new(latency(100, 10), SingleStrategy::Retry(retry_budget(0))),
        Err(ClientError::InvalidConfiguration(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn each_request_has_fresh_budgets_and_unused_error_categories_do_not_block_retry() {
    let mut budget = retry_budget(2);
    budget.max_client_errors = 0;
    budget.max_timeouts = 0;
    let client = Client::new(latency(100, 10), SingleStrategy::Retry(budget)).unwrap();
    for _ in 0..2 {
        let sender = ScriptedSender::new(vec![
            immediate(ServerOutcome::ServerError),
            immediate(success("fresh request")),
        ]);
        assert_eq!(
            client.execute(&sender).await.unwrap().body,
            b"fresh request"
        );
        assert_eq!(sender.trace.order(), [0, 0]);
    }
}
