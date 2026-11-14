//! The transport knows neither strategy nor client timeout settings.
use async_trait::async_trait;

use crate::model::ServerOutcome;

/// Sends one physical subrequest to a model endpoint.
///
/// Dropping `send()` must stop waiting and release its local resources. A sender
/// must yield to the runtime; blocking work cannot be interrupted by a timer.
/// Use one sender object per logical request (reused for its retry attempts).
#[async_trait]
pub trait SubrequestSender: Send + Sync {
    /// Returns a response, or remains pending until canceled by the caller.
    async fn send(&self) -> ServerOutcome;
}
