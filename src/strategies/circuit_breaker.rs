use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

use crate::budget::deadline_after;
use crate::model::{AttemptOutcome, ClientError};

/// Sliding-window isolation policy, scoped to one `MultiClient` instance.
#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerConfig {
    /// Evaluate failure rate only after this many completed attempts.
    pub window_size: usize,
    /// Open when failure share is strictly greater than this value in [0, 1].
    pub failure_threshold: f64,
    /// Time to exclude the sender before allowing a recovery probe.
    pub cooldown: Duration,
}

impl CircuitBreakerConfig {
    pub(crate) fn validate(self) -> Result<(), ClientError> {
        if self.window_size == 0 {
            return Err(ClientError::InvalidConfiguration(
                "breaker window must be positive",
            ));
        }
        if !self.failure_threshold.is_finite() || !(0.0..=1.0).contains(&self.failure_threshold) {
            return Err(ClientError::InvalidConfiguration(
                "failure threshold must be finite and in [0, 1]",
            ));
        }
        if self.cooldown.is_zero() {
            return Err(ClientError::InvalidConfiguration(
                "breaker cooldown must be positive",
            ));
        }
        deadline_after(self.cooldown)?;
        Ok(())
    }
}

#[derive(Default)]
enum BreakerState {
    #[default]
    Closed,
    // None means that the cooldown exceeded the clock's representable range.
    Open {
        until: Option<Instant>,
    },
    HalfOpen,
}

#[derive(Default)]
struct SenderHealth {
    state: BreakerState,
    failures: VecDeque<bool>,
}

pub(crate) struct CircuitBreaker {
    config: CircuitBreakerConfig,
    senders: Vec<SenderHealth>,
}

impl CircuitBreaker {
    pub(crate) fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            senders: Vec::new(),
        }
    }

    pub(crate) fn prepare(&mut self, sender_count: usize) -> Result<(), ClientError> {
        if self.senders.is_empty() {
            self.senders = (0..sender_count).map(|_| SenderHealth::default()).collect();
        } else if self.senders.len() != sender_count {
            return Err(ClientError::InvalidConfiguration(
                "breaker sender count and positional identities must remain stable",
            ));
        }
        Ok(())
    }

    pub(crate) fn select(&mut self, next_index: &mut usize) -> Option<usize> {
        for offset in 0..self.senders.len() {
            let server_index = (*next_index + offset) % self.senders.len();
            let health = &mut self.senders[server_index];
            match health.state {
                BreakerState::Open { until: None } => continue,
                BreakerState::Open { until: Some(until) } if Instant::now() < until => continue,
                BreakerState::Open { .. } => health.state = BreakerState::HalfOpen,
                BreakerState::Closed | BreakerState::HalfOpen => {}
            }
            // Exclusive &mut access means no other invocation can claim this probe.
            // A canceled probe has no outcome; the next invocation may probe again.
            *next_index = (server_index + 1) % self.senders.len();
            return Some(server_index);
        }
        None
    }

    pub(crate) fn record(&mut self, server_index: usize, outcome: &AttemptOutcome) {
        let health = &mut self.senders[server_index];
        // An attempt cut short by the overall deadline also failed to respond
        // in time. Caller cancellation never reaches this method.
        let failed = !matches!(outcome, AttemptOutcome::Success(_));
        if matches!(health.state, BreakerState::HalfOpen) {
            if failed {
                health.state = Self::open_state(self.config.cooldown);
            } else {
                health.state = BreakerState::Closed;
                health.failures.clear();
            }
            return;
        }
        if health.failures.len() == self.config.window_size {
            health.failures.pop_front();
        }
        health.failures.push_back(failed);
        if health.failures.len() == self.config.window_size {
            let failure_count = health.failures.iter().filter(|failed| **failed).count();
            let failure_rate = failure_count as f64 / self.config.window_size as f64;
            if failure_rate > self.config.failure_threshold {
                health.state = Self::open_state(self.config.cooldown);
            }
        }
    }

    fn open_state(cooldown: Duration) -> BreakerState {
        BreakerState::Open {
            until: Instant::now().checked_add(cooldown),
        }
    }
}
