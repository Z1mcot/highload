//! Public single-sender and multi-sender execution APIs.
use std::sync::Arc;

use crate::budget::{deadline_after, LatencyBudget};
use crate::model::{ClientError, Response};
use crate::strategies::{
    execute_hedging, execute_sequential, CircuitBreaker, MultiStrategy, Routing, SingleStrategy,
};
use crate::transport::SubrequestSender;

/// Validated single-sender policy; each execution creates fresh request budgets.
pub struct Client {
    latency: LatencyBudget,
    strategy: SingleStrategy,
}

impl Client {
    /// Validates configuration without starting any work.
    pub fn new(latency: LatencyBudget, strategy: SingleStrategy) -> Result<Self, ClientError> {
        latency.validate()?;
        strategy.validate()?;
        Ok(Self { latency, strategy })
    }

    /// Executes one logical request using this request's sender object.
    pub async fn execute(&self, sender: &dyn SubrequestSender) -> Result<Response, ClientError> {
        let deadline = deadline_after(self.latency.overall)?;
        execute_sequential(
            &mut Routing::Single(sender),
            self.strategy,
            deadline,
            self.latency.per_attempt,
        )
        .await
    }
}

/// Stateful routing; `&mut self` permits one logical invocation at a time.
/// Sender objects belong to each request; positional server identities are stable.
pub struct MultiClient {
    latency: LatencyBudget,
    strategy: MultiStrategy,
    next_index: usize,
    breaker: Option<CircuitBreaker>,
}

impl MultiClient {
    /// Validates budgets and creates a cursor starting at the first sender.
    pub fn new(latency: LatencyBudget, strategy: MultiStrategy) -> Result<Self, ClientError> {
        latency.validate()?;
        strategy.validate()?;
        let breaker = match strategy {
            MultiStrategy::CircuitBreaking { breaker, .. } => Some(CircuitBreaker::new(breaker)),
            _ => None,
        };
        Ok(Self {
            latency,
            strategy,
            next_index: 0,
            breaker,
        })
    }

    /// Executes one logical request with the configured routing policy.
    ///
    /// Circuit breaking requires the same sender count and positional server
    /// identities on every call. Each call may provide fresh sender objects.
    pub async fn execute(
        &mut self,
        senders: &[Arc<dyn SubrequestSender>],
    ) -> Result<Response, ClientError> {
        if senders.is_empty() {
            return Err(ClientError::NoSenders);
        }
        let deadline = deadline_after(self.latency.overall)?;
        if let Some(breaker) = &mut self.breaker {
            breaker.prepare(senders.len())?;
        }
        match self.strategy {
            MultiStrategy::Hedging { hedging_delay } => {
                execute_hedging(senders, deadline, hedging_delay).await
            }
            MultiStrategy::RoundRobin(strategy)
            | MultiStrategy::CircuitBreaking { strategy, .. } => {
                let mut routing = Routing::RoundRobin {
                    senders,
                    next_index: &mut self.next_index,
                    breaker: self.breaker.as_mut(),
                };
                execute_sequential(&mut routing, strategy, deadline, self.latency.per_attempt).await
            }
        }
    }
}
