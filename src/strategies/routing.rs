use std::sync::Arc;

use crate::model::{AttemptOutcome, ClientError, RequestStats};
use crate::strategies::CircuitBreaker;
use crate::transport::SubrequestSender;

// Routing and health observations are synchronous. Only sending needs an await.
pub(crate) enum Routing<'a> {
    Single(&'a dyn SubrequestSender),
    RoundRobin {
        senders: &'a [Arc<dyn SubrequestSender>],
        next_index: &'a mut usize,
        breaker: Option<&'a mut CircuitBreaker>,
    },
}

impl Routing<'_> {
    pub(crate) fn select(&mut self, stats: RequestStats) -> Result<usize, ClientError> {
        match self {
            Self::Single(_) => Ok(0),
            Self::RoundRobin {
                senders,
                next_index,
                breaker,
            } => {
                if let Some(breaker) = breaker {
                    return breaker
                        .select(next_index)
                        .ok_or(ClientError::NoAvailableSenders { stats });
                }
                let server_index = **next_index % senders.len();
                **next_index = (server_index + 1) % senders.len();
                Ok(server_index)
            }
        }
    }

    pub(crate) fn sender(&self, server_index: usize) -> &dyn SubrequestSender {
        match self {
            Self::Single(sender) => *sender,
            Self::RoundRobin { senders, .. } => senders[server_index].as_ref(),
        }
    }

    pub(crate) fn record(&mut self, server_index: usize, outcome: &AttemptOutcome) {
        if let Self::RoundRobin {
            breaker: Some(breaker),
            ..
        } = self
        {
            breaker.record(server_index, outcome);
        }
    }
}
