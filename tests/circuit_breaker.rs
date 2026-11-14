mod support;
use resilience::{
    CircuitBreakerConfig, ClientError, MultiClient, MultiStrategy, ServerOutcome, SingleStrategy,
};
use std::sync::Arc;
use support::*;
use tokio::time::{advance, Instant};

fn client(window_size: usize, failure_threshold: f64, strategy: SingleStrategy) -> MultiClient {
    MultiClient::new(
        latency(100, 10),
        MultiStrategy::CircuitBreaking {
            strategy,
            breaker: CircuitBreakerConfig {
                window_size,
                failure_threshold,
                cooldown: millis(20),
            },
        },
    )
    .unwrap()
}

async fn call(
    client: &mut MultiClient,
    scripts: Vec<Vec<Step>>,
    trace: &Arc<Trace>,
) -> Result<resilience::Response, ClientError> {
    client.execute(&sender_list(scripts, trace)).await
}

#[tokio::test(start_paused = true)]
async fn healthy_senders_return_payloads_in_round_robin_order() {
    let trace = Arc::new(Trace::default());
    let mut client = client(2, 0.5, SingleStrategy::Default);
    for expected in [b"a", b"b", b"a"] {
        let response = call(
            &mut client,
            vec![vec![immediate(success("a"))], vec![immediate(success("b"))]],
            &trace,
        )
        .await
        .unwrap();
        assert_eq!(response.body, expected);
    }
    assert_eq!(trace.order(), [0, 1, 0]);
}

#[tokio::test(start_paused = true)]
async fn full_sliding_window_opens_only_above_strict_threshold() {
    let trace = Arc::new(Trace::default());
    let mut client = client(2, 0.5, SingleStrategy::Default);
    for outcome in [
        ServerOutcome::ServerError,
        success("ok"),
        ServerOutcome::ServerError,
        success("ok"),
        ServerOutcome::ServerError,
        ServerOutcome::ServerError,
    ] {
        let expected_success = matches!(outcome, ServerOutcome::Success(_));
        let result = call(&mut client, vec![vec![immediate(outcome)]], &trace).await;
        if expected_success {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(ClientError::AttemptFailed { .. })));
        }
    }
    assert!(matches!(call(&mut client, vec![vec![]], &trace).await,
        Err(ClientError::NoAvailableSenders { stats }) if stats.attempts_started == 0));
    assert_eq!(trace.order().len(), 6);
}

#[tokio::test(start_paused = true)]
async fn client_errors_server_errors_and_timeouts_each_isolate_sender() {
    for step in [
        immediate(ServerOutcome::ClientError),
        immediate(ServerOutcome::ServerError),
        Step::Hang,
    ] {
        let trace = Arc::new(Trace::default());
        let mut client = client(1, 0.0, SingleStrategy::Default);
        assert!(matches!(
            call(&mut client, vec![vec![step]], &trace).await,
            Err(ClientError::AttemptFailed { .. })
        ));
        assert!(matches!(
            call(&mut client, vec![vec![]], &trace).await,
            Err(ClientError::NoAvailableSenders { .. })
        ));
        assert_eq!(trace.order(), [0]);
    }
}

#[tokio::test(start_paused = true)]
async fn excludes_open_sender_and_reenters_rotation_exactly_after_cooldown() {
    let trace = Arc::new(Trace::default());
    let mut client = client(1, 0.0, SingleStrategy::Default);
    assert!(call(
        &mut client,
        vec![vec![immediate(ServerOutcome::ServerError)], vec![]],
        &trace
    )
    .await
    .is_err());
    for _ in 0..2 {
        call(
            &mut client,
            vec![vec![], vec![immediate(success("healthy"))]],
            &trace,
        )
        .await
        .unwrap();
    }
    advance(millis(19)).await;
    call(
        &mut client,
        vec![vec![], vec![immediate(success("healthy"))]],
        &trace,
    )
    .await
    .unwrap();
    advance(millis(1)).await;
    assert_eq!(
        call(
            &mut client,
            vec![vec![immediate(success("probe"))], vec![]],
            &trace
        )
        .await
        .unwrap()
        .body,
        b"probe"
    );
    assert_eq!(trace.order(), [0, 1, 1, 1, 0]);
}

#[tokio::test(start_paused = true)]
async fn successful_half_open_probe_resets_history_and_closes_breaker() {
    let trace = Arc::new(Trace::default());
    let mut client = client(2, 0.5, SingleStrategy::Default);
    for _ in 0..2 {
        assert!(call(
            &mut client,
            vec![vec![immediate(ServerOutcome::ServerError)]],
            &trace
        )
        .await
        .is_err());
    }
    advance(millis(20)).await;
    call(&mut client, vec![vec![immediate(success("probe"))]], &trace)
        .await
        .unwrap();
    // The first post-recovery failure cannot open a fresh two-sample window.
    assert!(matches!(
        call(
            &mut client,
            vec![vec![immediate(ServerOutcome::ServerError)]],
            &trace
        )
        .await,
        Err(ClientError::AttemptFailed { .. })
    ));
    call(
        &mut client,
        vec![vec![immediate(success("closed"))]],
        &trace,
    )
    .await
    .unwrap();
    assert_eq!(trace.order().len(), 5);
}

#[tokio::test(start_paused = true)]
async fn failed_half_open_probe_reopens_for_a_full_new_cooldown() {
    let trace = Arc::new(Trace::default());
    let mut client = client(1, 0.0, SingleStrategy::Default);
    assert!(call(
        &mut client,
        vec![vec![immediate(ServerOutcome::ServerError)]],
        &trace
    )
    .await
    .is_err());
    advance(millis(20)).await;
    assert!(call(
        &mut client,
        vec![vec![immediate(ServerOutcome::ClientError)]],
        &trace
    )
    .await
    .is_err());
    advance(millis(19)).await;
    assert!(matches!(
        call(&mut client, vec![vec![]], &trace).await,
        Err(ClientError::NoAvailableSenders { .. })
    ));
    advance(millis(1)).await;
    call(
        &mut client,
        vec![vec![immediate(success("recovered"))]],
        &trace,
    )
    .await
    .unwrap();
    assert_eq!(trace.order(), [0, 0, 0]);
}

#[tokio::test(start_paused = true)]
async fn all_open_returns_immediately_and_does_not_spend_remaining_attempt_budget() {
    let trace = Arc::new(Trace::default());
    let mut client = client(1, 0.0, SingleStrategy::Retry(retry_budget(10)));
    let started_at = Instant::now();
    assert!(
        matches!(call(&mut client, vec![vec![immediate(ServerOutcome::ServerError)],
        vec![immediate(ServerOutcome::ClientError)]], &trace).await,
        Err(ClientError::NoAvailableSenders { stats }) if stats.attempts_started == 2)
    );
    assert!(
        matches!(call(&mut client, vec![vec![], vec![]], &trace).await,
        Err(ClientError::NoAvailableSenders { stats }) if stats.attempts_started == 0)
    );
    assert_eq!(started_at.elapsed(), millis(0));
    assert_eq!(trace.order(), [0, 1]);
}

#[tokio::test(start_paused = true)]
async fn canceled_half_open_probe_is_not_counted_and_can_be_retried() {
    let trace = Arc::new(Trace::default());
    let mut client = client(1, 0.0, SingleStrategy::Default);
    assert!(call(
        &mut client,
        vec![vec![immediate(ServerOutcome::ServerError)]],
        &trace
    )
    .await
    .is_err());
    advance(millis(20)).await;
    let senders = sender_list(vec![vec![Step::Hang]], &trace);
    let mut probe = Box::pin(client.execute(&senders));
    tokio::select! {
        biased;
        result = &mut probe => panic!("unexpected early result: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    drop(probe);
    assert_eq!(*trace.active.lock().unwrap(), 0);
    call(
        &mut client,
        vec![vec![immediate(success("retry probe"))]],
        &trace,
    )
    .await
    .unwrap();
    assert_eq!(trace.order(), [0, 0, 0]);
}

#[tokio::test(start_paused = true)]
async fn overall_deadline_counts_as_health_failure_but_not_per_attempt_timeout() {
    let trace = Arc::new(Trace::default());
    let mut client = MultiClient::new(
        latency(5, 10),
        MultiStrategy::CircuitBreaking {
            strategy: SingleStrategy::Retry(retry_budget(5)),
            breaker: CircuitBreakerConfig {
                window_size: 1,
                failure_threshold: 0.0,
                cooldown: millis(20),
            },
        },
    )
    .unwrap();
    let started_at = Instant::now();
    assert!(
        matches!(call(&mut client, vec![vec![Step::Hang]], &trace).await,
        Err(ClientError::DeadlineExceeded { stats }) if stats.attempts_started == 1 && stats.timeouts == 0)
    );
    assert_eq!(started_at.elapsed(), millis(5));
    assert!(matches!(
        call(&mut client, vec![vec![]], &trace).await,
        Err(ClientError::NoAvailableSenders { .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn rejects_empty_or_changed_sender_list_without_resetting_existing_history() {
    let trace = Arc::new(Trace::default());
    let mut client = client(1, 0.0, SingleStrategy::Default);
    assert_eq!(client.execute(&[]).await, Err(ClientError::NoSenders));
    assert!(call(
        &mut client,
        vec![vec![immediate(ServerOutcome::ServerError)]],
        &trace
    )
    .await
    .is_err());
    assert!(matches!(
        call(&mut client, vec![vec![], vec![]], &trace).await,
        Err(ClientError::InvalidConfiguration(_))
    ));
    assert!(matches!(
        call(&mut client, vec![vec![]], &trace).await,
        Err(ClientError::NoAvailableSenders { .. })
    ));
    assert_eq!(trace.order(), [0]);
}

#[test]
fn rejects_invalid_window_threshold_and_cooldown() {
    let valid = CircuitBreakerConfig {
        window_size: 2,
        failure_threshold: 0.5,
        cooldown: millis(20),
    };
    for breaker in [
        CircuitBreakerConfig {
            window_size: 0,
            ..valid
        },
        CircuitBreakerConfig {
            failure_threshold: f64::NAN,
            ..valid
        },
        CircuitBreakerConfig {
            failure_threshold: f64::INFINITY,
            ..valid
        },
        CircuitBreakerConfig {
            failure_threshold: -0.1,
            ..valid
        },
        CircuitBreakerConfig {
            failure_threshold: 1.1,
            ..valid
        },
        CircuitBreakerConfig {
            cooldown: millis(0),
            ..valid
        },
        CircuitBreakerConfig {
            cooldown: std::time::Duration::MAX,
            ..valid
        },
    ] {
        assert!(matches!(
            MultiClient::new(
                latency(100, 10),
                MultiStrategy::CircuitBreaking {
                    strategy: SingleStrategy::Default,
                    breaker,
                }
            ),
            Err(ClientError::InvalidConfiguration(_))
        ));
    }
}
