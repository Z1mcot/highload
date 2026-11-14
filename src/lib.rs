//! Resilience strategies for a cancellable, asynchronous model service.
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod budget;
pub mod client;
pub mod model;
pub mod strategies;
pub mod transport;

pub use budget::{LatencyBudget, RetryBudget};
pub use client::{Client, MultiClient};
pub use model::{ClientError, FailureKind, RequestStats, Response, ServerOutcome};
pub use strategies::{CircuitBreakerConfig, ExponentialBackoff, MultiStrategy, SingleStrategy};
pub use transport::SubrequestSender;
