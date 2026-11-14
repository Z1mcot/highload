mod support;
use resilience::{ClientError, MultiClient, MultiStrategy, ServerOutcome};
use std::sync::Arc;
use support::*;
use tokio::time::{advance, Instant};

fn client(overall: u64, delay: u64) -> MultiClient {
    MultiClient::new(
        latency(overall, 1),
        MultiStrategy::Hedging {
            hedging_delay: millis(delay),
        },
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn early_success_preserves_payload_and_never_fans_out() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![vec![Step::Respond(millis(5), success("first"))], vec![]],
        &trace,
    );
    assert_eq!(
        client(100, 10).execute(&senders).await.unwrap().body,
        b"first"
    );
    assert_eq!(trace.order(), [0]);
}

#[tokio::test(start_paused = true)]
async fn fanout_starts_all_remaining_senders_exactly_at_delay_and_fastest_success_wins() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![
            vec![Step::Hang],
            vec![Step::Hang],
            vec![Step::Respond(millis(3), success("fastest"))],
        ],
        &trace,
    );
    let mut client = client(100, 10);
    let started_at = Instant::now();
    let mut request = Box::pin(client.execute(&senders));
    tokio::select! {
        biased;
        result = &mut request => panic!("unexpected early result: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(trace.order(), [0]);
    advance(millis(9)).await;
    tokio::select! {
        biased;
        result = &mut request => panic!("unexpected early result: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(trace.order(), [0]);
    assert_eq!(request.await.unwrap().body, b"fastest");
    assert_eq!(trace.order(), [0, 1, 2]);
    assert_eq!(
        trace.times(),
        [started_at, started_at + millis(10), started_at + millis(10)]
    );
    assert_eq!(started_at.elapsed(), millis(13));
    assert_eq!(*trace.canceled.lock().unwrap(), 2);
    assert_eq!(*trace.active.lock().unwrap(), 0);
}

#[tokio::test(start_paused = true)]
async fn early_client_or_server_error_suppresses_fanout_and_waits_until_deadline() {
    for outcome in [ServerOutcome::ClientError, ServerOutcome::ServerError] {
        let trace = Arc::new(Trace::default());
        let senders = sender_list(vec![vec![immediate(outcome)], vec![]], &trace);
        let started_at = Instant::now();
        assert!(matches!(client(40, 10).execute(&senders).await,
            Err(ClientError::DeadlineExceeded { stats })
            if stats.attempts_started == 1 && stats.client_errors + stats.server_errors == 1));
        assert_eq!(trace.order(), [0]);
        assert_eq!(started_at.elapsed(), millis(40));
    }
}

#[tokio::test(start_paused = true)]
async fn all_failed_hedges_wait_until_overall_deadline() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![
            vec![Step::Respond(millis(15), ServerOutcome::ClientError)],
            vec![immediate(ServerOutcome::ServerError)],
        ],
        &trace,
    );
    let started_at = Instant::now();
    assert!(matches!(client(40, 10).execute(&senders).await,
        Err(ClientError::DeadlineExceeded { stats })
        if stats.attempts_started == 2 && stats.client_errors == 1 && stats.server_errors == 1));
    assert_eq!(started_at.elapsed(), millis(40));
}

#[tokio::test(start_paused = true)]
async fn overall_deadline_cancels_all_hung_hedges_without_per_attempt_timeouts() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(vec![vec![Step::Hang], vec![Step::Hang]], &trace);
    assert!(matches!(client(40, 10).execute(&senders).await,
        Err(ClientError::DeadlineExceeded { stats })
        if stats.attempts_started == 2 && stats.timeouts == 0));
    assert_eq!(*trace.active.lock().unwrap(), 0);
    assert_eq!(*trace.canceled.lock().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn response_ready_at_fanout_boundary_takes_precedence_over_delay() {
    for outcome in [success("boundary"), ServerOutcome::ServerError] {
        let trace = Arc::new(Trace::default());
        let senders = sender_list(
            vec![vec![Step::Respond(millis(10), outcome.clone())], vec![]],
            &trace,
        );
        let result = client(40, 10).execute(&senders).await;
        if let ServerOutcome::Success(response) = outcome {
            assert_eq!(result, Ok(response));
        } else {
            assert!(matches!(result, Err(ClientError::DeadlineExceeded { .. })));
        }
        assert_eq!(trace.order(), [0]);
    }
}

#[tokio::test(start_paused = true)]
async fn deadline_before_fanout_or_with_only_one_sender_starts_no_extra_attempts() {
    for (server_count, delay) in [(2, 100), (1, 10), (2, 40)] {
        let trace = Arc::new(Trace::default());
        let senders = sender_list(
            (0..server_count).map(|_| vec![Step::Hang]).collect(),
            &trace,
        );
        let started_at = Instant::now();
        assert!(matches!(client(40, delay).execute(&senders).await,
            Err(ClientError::DeadlineExceeded { stats }) if stats.attempts_started == 1));
        assert_eq!(started_at.elapsed(), millis(40));
        assert_eq!(trace.order(), [0]);
    }
}

#[tokio::test(start_paused = true)]
async fn original_sender_can_win_after_fanout_and_cancel_others() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![
            vec![Step::Respond(millis(15), success("original"))],
            vec![Step::Hang],
        ],
        &trace,
    );
    assert_eq!(
        client(40, 10).execute(&senders).await.unwrap().body,
        b"original"
    );
    assert_eq!(trace.order(), [0, 1]);
    assert_eq!(*trace.canceled.lock().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn immediate_hedge_success_still_starts_every_sender_once() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(
        vec![
            vec![Step::Hang],
            vec![immediate(success("ready"))],
            vec![Step::Hang],
        ],
        &trace,
    );
    client(40, 0).execute(&senders).await.unwrap();
    assert_eq!(trace.order(), [0, 1, 2]);
    assert_eq!(*trace.canceled.lock().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn canceling_caller_drops_pending_hedges_immediately() {
    let trace = Arc::new(Trace::default());
    let senders = sender_list(vec![vec![Step::Hang], vec![Step::Hang]], &trace);
    let mut client = client(100, 0);
    let mut request = Box::pin(client.execute(&senders));
    tokio::select! {
        biased;
        result = &mut request => panic!("unexpected early result: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(*trace.active.lock().unwrap(), 2);
    drop(request);
    assert_eq!(*trace.active.lock().unwrap(), 0);
    assert_eq!(*trace.canceled.lock().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn rejects_empty_sender_list() {
    assert_eq!(
        client(40, 10).execute(&[]).await,
        Err(ClientError::NoSenders)
    );
}
