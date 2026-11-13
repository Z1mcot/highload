use std::time::Duration;
use tokio::time::{timeout_at, Instant};

use crate::model::{AttemptOutcome, FailureKind, ServerOutcome};
use crate::transport::SubrequestSender;

pub(crate) async fn run_subrequest(
    sender: &dyn SubrequestSender,
    deadline: Instant,
    per_attempt: Duration,
) -> AttemptOutcome {
    let now = Instant::now();
    if now >= deadline {
        return AttemptOutcome::DeadlineExceeded;
    }
    let remaining_time = deadline - now;
    let attempt_deadline = now + per_attempt.min(remaining_time);
    let outcome = timeout_at(attempt_deadline, sender.send()).await;

    // Tokio may poll a ready response before its timer. Give elapsed deadlines
    // precedence, including when both become ready in the same runtime tick.
    let completed_at = Instant::now();
    if completed_at >= deadline {
        return AttemptOutcome::DeadlineExceeded;
    }
    if completed_at >= attempt_deadline {
        return AttemptOutcome::Failure(FailureKind::Timeout);
    }
    match outcome {
        Ok(ServerOutcome::Success(response)) => AttemptOutcome::Success(response),
        Ok(ServerOutcome::ClientError) => AttemptOutcome::Failure(FailureKind::ClientError),
        Ok(ServerOutcome::ServerError) => AttemptOutcome::Failure(FailureKind::ServerError),
        Err(_) => AttemptOutcome::Failure(FailureKind::Timeout),
    }
}
