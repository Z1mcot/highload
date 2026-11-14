//! Validated limits and a single monotonic deadline per logical request.
use std::time::Duration;
use tokio::time::Instant;

use crate::model::ClientError;

/// Time limits; every attempt shares the same overall deadline.
#[derive(Debug, Clone, Copy)]
pub struct LatencyBudget {
    /// Maximum elapsed time including retries and sleeps.
    pub overall: Duration,
    /// Maximum wait for one sequential subrequest. Hedging uses only `overall`.
    pub per_attempt: Duration,
}

impl LatencyBudget {
    pub(crate) fn validate(self) -> Result<(), ClientError> {
        if self.overall.is_zero() || self.per_attempt.is_zero() {
            return Err(ClientError::InvalidConfiguration(
                "latency budgets must be positive",
            ));
        }
        deadline_after(self.overall)?;
        Ok(())
    }
}

/// Limits on launched subrequests and observed failed outcomes.
///
/// A category maximum forbids further attempts once reached. Zero also stops
/// after the first error in that category; it never forbids the initial attempt.
#[derive(Debug, Clone, Copy)]
pub struct RetryBudget {
    /// Total attempts, including the first (`1` means exactly one call).
    pub max_attempts: usize,
    /// Maximum observed 4xx outcomes before stopping.
    pub max_client_errors: usize,
    /// Maximum observed 5xx outcomes before stopping.
    pub max_server_errors: usize,
    /// Maximum per-attempt timeouts before stopping.
    pub max_timeouts: usize,
}

impl RetryBudget {
    pub(crate) fn validate(self) -> Result<(), ClientError> {
        if self.max_attempts == 0 {
            return Err(ClientError::InvalidConfiguration(
                "max_attempts must be at least one",
            ));
        }
        Ok(())
    }
}

pub(crate) fn deadline_after(duration: Duration) -> Result<Instant, ClientError> {
    Instant::now()
        .checked_add(duration)
        .ok_or(ClientError::InvalidConfiguration(
            "duration exceeds monotonic clock range",
        ))
}
