//! Strategy configuration and shared attempt execution.
mod backoff;
mod circuit_breaker;
mod default;
mod hedging;
mod retry;
mod routing;

use std::time::Duration;

use crate::budget::RetryBudget;
use crate::model::ClientError;

/// Single-sender execution policy, also composable with multi-client routing.
#[derive(Debug, Clone, Copy)]
pub enum SingleStrategy {
    /// Exactly one attempt; stops on any failed outcome.
    Default,
    /// Sequential retries while total, category, and latency budgets permit.
    Retry(RetryBudget),
    /// The same retry loop, with capped exponential delays after 5xx/timeouts.
    Backoff {
        /// Total and category limits.
        budget: RetryBudget,
        /// Delay growth parameters.
        backoff: ExponentialBackoff,
    },
}

/// Routing policy for multiple model endpoints.
#[derive(Debug, Clone, Copy)]
pub enum MultiStrategy {
    /// Advance the persistent cursor on every attempt, using this retry policy.
    RoundRobin(SingleStrategy),
    /// Start sender zero immediately; fan out only if no response arrives in time.
    Hedging {
        /// Zero allows immediate fanout, unless the first response is already ready.
        hedging_delay: Duration,
    },
    /// Round-robin with per-position health history and one half-open probe.
    CircuitBreaking {
        /// Sequential attempt policy, including optional backoff.
        strategy: SingleStrategy,
        /// Sliding window and recovery settings.
        breaker: CircuitBreakerConfig,
    },
}

impl MultiStrategy {
    pub(crate) fn validate(self) -> Result<(), ClientError> {
        match self {
            Self::RoundRobin(strategy) => strategy.validate(),
            Self::Hedging { .. } => Ok(()),
            Self::CircuitBreaking { strategy, breaker } => {
                strategy.validate()?;
                breaker.validate()
            }
        }
    }
}

impl SingleStrategy {
    pub(crate) fn validate(self) -> Result<(), ClientError> {
        match self {
            Self::Default => Ok(()),
            Self::Retry(budget) => budget.validate(),
            Self::Backoff { budget, backoff } => {
                budget.validate()?;
                backoff.validate()
            }
        }
    }
}

pub use backoff::ExponentialBackoff;
pub(crate) use circuit_breaker::CircuitBreaker;
pub use circuit_breaker::CircuitBreakerConfig;
pub(crate) use default::run_subrequest;
pub(crate) use hedging::execute_hedging;
pub(crate) use retry::execute_sequential;
pub(crate) use routing::Routing;
