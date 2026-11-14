//! Server responses, client errors, and observable request accounting.
use std::fmt;

/// Useful payload returned by a successful (2xx) subrequest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// Unmodified response body.
    pub body: Vec<u8>,
}

/// A completed server response. Timeouts are observed by the client instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerOutcome {
    /// A 2xx response with its payload.
    Success(Response),
    /// A 4xx response.
    ClientError,
    /// A 5xx response.
    ServerError,
}

/// Failed attempt categories with independent retry budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// A 4xx response.
    ClientError,
    /// A 5xx response.
    ServerError,
    /// The per-attempt timeout expired before the overall deadline.
    Timeout,
}

/// Counts starts separately from observed failures and per-attempt timeouts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RequestStats {
    /// Sender operations started, including canceled or deadline-limited ones.
    pub attempts_started: usize,
    /// Observed 4xx responses.
    pub client_errors: usize,
    /// Observed 5xx responses.
    pub server_errors: usize,
    /// Per-attempt timeouts; an overall deadline is counted separately.
    pub timeouts: usize,
}

impl RequestStats {
    pub(crate) fn record_failure(&mut self, failure: FailureKind) {
        match failure {
            FailureKind::ClientError => self.client_errors += 1,
            FailureKind::ServerError => self.server_errors += 1,
            FailureKind::Timeout => self.timeouts += 1,
        }
    }
}

/// Errors preserve the stopping reason and accounting where work was started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// Invalid parameters are rejected before any sender is called.
    InvalidConfiguration(&'static str),
    /// A multi-client received no senders.
    NoSenders,
    /// No eligible sender remains in the circuit breaker rotation.
    NoAvailableSenders {
        /// Work already performed by this logical request.
        stats: RequestStats,
    },
    /// Default stops immediately on a failed attempt.
    AttemptFailed {
        /// The observed failure.
        failure: FailureKind,
        /// Work performed by this logical request.
        stats: RequestStats,
    },
    /// A total or category budget forbids another attempt.
    BudgetExhausted {
        /// Last observed failure.
        last_failure: FailureKind,
        /// Work performed by this logical request.
        stats: RequestStats,
    },
    /// The one overall monotonic deadline expired.
    DeadlineExceeded {
        /// Starts may exceed completed outcomes because pending work is dropped.
        stats: RequestStats,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(reason) => {
                write!(formatter, "invalid configuration: {reason}")
            }
            Self::NoSenders => write!(formatter, "sender list is empty"),
            Self::NoAvailableSenders { .. } => write!(formatter, "all senders are isolated"),
            Self::AttemptFailed { failure, .. } => write!(formatter, "attempt failed: {failure:?}"),
            Self::BudgetExhausted { last_failure, .. } => {
                write!(formatter, "retry budget exhausted after {last_failure:?}")
            }
            Self::DeadlineExceeded { .. } => write!(formatter, "overall deadline exceeded"),
        }
    }
}

impl std::error::Error for ClientError {}

pub(crate) enum AttemptOutcome {
    Success(Response),
    Failure(FailureKind),
    DeadlineExceeded,
}
