use std::time::Duration;

use crate::model::ClientError;

/// Exponential delays after 5xx/timeouts; 4xx neither sleeps nor resets growth.
#[derive(Debug, Clone, Copy)]
pub struct ExponentialBackoff {
    /// First delay after an eligible failed attempt.
    pub initial_delay: Duration,
    /// Upper bound on every delay, including arithmetic overflow.
    pub max_delay: Duration,
}

impl ExponentialBackoff {
    pub(crate) fn validate(self) -> Result<(), ClientError> {
        if self.initial_delay.is_zero() || self.max_delay < self.initial_delay {
            return Err(ClientError::InvalidConfiguration(
                "backoff needs 0 < initial_delay <= max_delay",
            ));
        }
        Ok(())
    }
}

pub(crate) struct BackoffSchedule {
    next_delay: Duration,
    max_delay: Duration,
}

impl BackoffSchedule {
    pub(crate) fn new(config: ExponentialBackoff) -> Self {
        Self {
            next_delay: config.initial_delay,
            max_delay: config.max_delay,
        }
    }

    pub(crate) fn next(&mut self) -> Duration {
        let delay = self.next_delay;
        self.next_delay = delay.saturating_mul(2).min(self.max_delay);
        delay
    }
}
