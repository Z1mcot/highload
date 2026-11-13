use std::time::Duration;
use tokio::time::{sleep_until, Instant};

use crate::budget::RetryBudget;
use crate::model::{AttemptOutcome, ClientError, FailureKind, RequestStats, Response};
use crate::strategies::backoff::BackoffSchedule;
use crate::strategies::{run_subrequest, Routing, SingleStrategy};

pub(crate) async fn execute_sequential(
    routing: &mut Routing<'_>,
    strategy: SingleStrategy,
    deadline: Instant,
    per_attempt: Duration,
) -> Result<Response, ClientError> {
    let budget = match strategy {
        SingleStrategy::Default => RetryBudget {
            max_attempts: 1,
            max_client_errors: 0,
            max_server_errors: 0,
            max_timeouts: 0,
        },
        SingleStrategy::Retry(budget) | SingleStrategy::Backoff { budget, .. } => budget,
    };
    let mut backoff = match strategy {
        SingleStrategy::Backoff { backoff, .. } => Some(BackoffSchedule::new(backoff)),
        _ => None,
    };
    let mut stats = RequestStats::default();
    loop {
        if Instant::now() >= deadline {
            return Err(ClientError::DeadlineExceeded { stats });
        }
        let server_index = routing.select(stats)?;
        stats.attempts_started += 1;
        let outcome = run_subrequest(routing.sender(server_index), deadline, per_attempt).await;
        routing.record(server_index, &outcome);
        let failure = match outcome {
            AttemptOutcome::Success(response) => return Ok(response),
            AttemptOutcome::DeadlineExceeded => {
                return Err(ClientError::DeadlineExceeded { stats })
            }
            AttemptOutcome::Failure(failure) => failure,
        };
        stats.record_failure(failure);
        if matches!(strategy, SingleStrategy::Default) {
            return Err(ClientError::AttemptFailed { failure, stats });
        }
        let category_exhausted = match failure {
            FailureKind::ClientError => stats.client_errors >= budget.max_client_errors,
            FailureKind::ServerError => stats.server_errors >= budget.max_server_errors,
            FailureKind::Timeout => stats.timeouts >= budget.max_timeouts,
        };
        if stats.attempts_started >= budget.max_attempts || category_exhausted {
            return Err(ClientError::BudgetExhausted {
                last_failure: failure,
                stats,
            });
        }
        if failure != FailureKind::ClientError {
            if let Some(schedule) = &mut backoff {
                let now = Instant::now();
                let remaining_time = deadline.saturating_duration_since(now);
                sleep_until(now + schedule.next().min(remaining_time)).await;
            }
        }
        // Immediately ready failures must still allow cancellation and other
        // runtime work between attempts, even with a very large attempt limit.
        tokio::task::yield_now().await;
    }
}
