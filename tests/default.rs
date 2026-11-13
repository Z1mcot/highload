mod support;
use resilience::{
    Client, ClientError, FailureKind, RequestStats, Response, ServerOutcome, SingleStrategy,
};
use support::*;
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn returns_success_payload_with_exactly_one_attempt() {
    let client = Client::new(latency(100, 20), SingleStrategy::Default).unwrap();
    for body in [b"payload".to_vec(), vec![], vec![0, 255, 42]] {
        let sender = ScriptedSender::new(vec![immediate(ServerOutcome::Success(Response {
            body: body.clone(),
        }))]);
        assert_eq!(client.execute(&sender).await.unwrap().body, body);
        assert_eq!(sender.trace.order(), [0]);
    }
}

#[tokio::test(start_paused = true)]
async fn stops_after_one_client_or_server_error() {
    for (outcome, failure) in [
        (ServerOutcome::ClientError, FailureKind::ClientError),
        (ServerOutcome::ServerError, FailureKind::ServerError),
    ] {
        let sender = ScriptedSender::new(vec![immediate(outcome)]);
        let client = Client::new(latency(100, 20), SingleStrategy::Default).unwrap();
        let error = client.execute(&sender).await.unwrap_err();
        assert!(
            matches!(error, ClientError::AttemptFailed { failure: actual, stats }
            if actual == failure && stats.attempts_started == 1)
        );
        assert_eq!(sender.trace.order(), [0]);
    }
}

#[tokio::test(start_paused = true)]
async fn per_attempt_timeout_cancels_the_only_attempt() {
    let sender = ScriptedSender::new(vec![Step::Hang]);
    let client = Client::new(latency(100, 20), SingleStrategy::Default).unwrap();
    let started_at = Instant::now();
    assert_eq!(
        client.execute(&sender).await,
        Err(ClientError::AttemptFailed {
            failure: FailureKind::Timeout,
            stats: RequestStats {
                attempts_started: 1,
                timeouts: 1,
                ..Default::default()
            },
        })
    );
    assert_eq!(started_at.elapsed(), millis(20));
    assert_eq!(*sender.trace.canceled.lock().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn overall_deadline_shortens_attempt_without_counting_per_attempt_timeout() {
    let sender = ScriptedSender::new(vec![Step::Hang]);
    let client = Client::new(latency(10, 100), SingleStrategy::Default).unwrap();
    let started_at = Instant::now();
    assert_eq!(
        client.execute(&sender).await,
        Err(ClientError::DeadlineExceeded {
            stats: RequestStats {
                attempts_started: 1,
                ..Default::default()
            },
        })
    );
    assert_eq!(started_at.elapsed(), millis(10));
    assert_eq!(*sender.trace.active.lock().unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn elapsed_attempt_or_overall_deadline_takes_precedence_over_ready_response() {
    for (overall, per_attempt, response_delay, expected_failure) in [
        (100, 10, 10, Some(FailureKind::Timeout)),
        (10, 100, 10, None),
    ] {
        let sender =
            ScriptedSender::new(vec![Step::Respond(millis(response_delay), success("late"))]);
        let client = Client::new(latency(overall, per_attempt), SingleStrategy::Default).unwrap();
        let result = client.execute(&sender).await;
        if let Some(expected) = expected_failure {
            assert!(
                matches!(result, Err(ClientError::AttemptFailed { failure, .. }) if failure == expected)
            );
        } else {
            assert!(matches!(result, Err(ClientError::DeadlineExceeded { .. })));
        }
    }
}

#[test]
fn rejects_zero_or_unrepresentable_latency_budgets() {
    for invalid in [
        latency(0, 1),
        latency(1, 0),
        resilience::LatencyBudget {
            overall: std::time::Duration::MAX,
            per_attempt: millis(1),
        },
    ] {
        assert!(matches!(
            Client::new(invalid, SingleStrategy::Default),
            Err(ClientError::InvalidConfiguration(_))
        ));
    }
}
