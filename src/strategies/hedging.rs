use std::future::{poll_fn, Future};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::time::{sleep_until, Instant};

use crate::model::{AttemptOutcome, ClientError, RequestStats, Response};
use crate::strategies::run_subrequest;
use crate::transport::SubrequestSender;

pub(crate) async fn execute_hedging(
    senders: &[Arc<dyn SubrequestSender>],
    deadline: Instant,
    hedging_delay: Duration,
) -> Result<Response, ClientError> {
    let mut stats = RequestStats::default();
    let now = Instant::now();
    if now >= deadline {
        return Err(ClientError::DeadlineExceeded { stats });
    }
    // Only the overall latency budget applies to hedging.
    let remaining_time = deadline - now;
    let fanout_at = now + hedging_delay.min(remaining_time);
    let mut first_attempt = Box::pin(run_subrequest(
        senders[0].as_ref(),
        deadline,
        remaining_time,
    ));
    stats.attempts_started += 1;
    tokio::select! {
        biased;
        _ = sleep_until(deadline) => return Err(ClientError::DeadlineExceeded { stats }),
        outcome = &mut first_attempt => {
            match outcome {
                AttemptOutcome::Success(response) => return Ok(response),
                AttemptOutcome::Failure(failure) => stats.record_failure(failure),
                AttemptOutcome::DeadlineExceeded => {}
            }
            // Any response suppresses fanout, but only success ends the request early.
            sleep_until(deadline).await;
            return Err(ClientError::DeadlineExceeded { stats });
        }
        _ = sleep_until(fanout_at) => {}
    }

    let mut attempts = vec![(true, Some(first_attempt))];
    for sender in &senders[1..] {
        attempts.push((
            false,
            Some(Box::pin(run_subrequest(
                sender.as_ref(),
                deadline,
                remaining_time,
            ))),
        ));
    }

    // Poll every hedge before choosing a winner: even an immediately ready 2xx
    // must not prevent the other scheduled senders from starting at fanout.
    // Futures stay in this scope; returning or canceling drops every loser.
    let first_success = poll_fn(|context| {
        let mut winner = None;
        for (started, attempt) in &mut attempts {
            let Some(future) = attempt.as_mut() else {
                continue;
            };
            if !*started {
                stats.attempts_started += 1;
                *started = true;
            }
            if let Poll::Ready(outcome) = future.as_mut().poll(context) {
                *attempt = None;
                match outcome {
                    AttemptOutcome::Success(response) => {
                        if winner.is_none() {
                            winner = Some(response);
                        }
                    }
                    AttemptOutcome::Failure(failure) => stats.record_failure(failure),
                    AttemptOutcome::DeadlineExceeded => {}
                }
            }
        }
        match winner {
            Some(response) => Poll::Ready(response),
            None => Poll::Pending,
        }
    });
    tokio::select! {
        biased;
        _ = sleep_until(deadline) => {}
        response = first_success => return Ok(response),
    }
    Err(ClientError::DeadlineExceeded { stats })
}
