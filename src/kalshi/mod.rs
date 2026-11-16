//! The Kalshi side of the collector: authenticating, discovering markets, and keeping websocket
//! connections subscribed, with each connection's books rebuilt and checked locally.
//!
//! - [`connection`]: one websocket connection, reconnecting as needed, reporting its books' changes
//!   as [`SourceEvent`](crate::collector::SourceEvent)s.
//! - [`session`], [`subscriptions`], [`book`]: the logic for one connection, with no I/O.
//! - [`discovery`]: which markets to follow, and listing them over REST.

pub mod auth;
pub mod book;
pub mod connection;
pub mod convert;
pub mod discovery;
pub mod fixed;
pub mod messages;
pub mod session;
pub mod subscriptions;

#[cfg(test)]
mod sim;

/// A delay so long that a timer set for it never goes off, in practice. (Tokio has no "never" timer,
/// and this keeps `select!` arms simple.)
pub(crate) const NEVER: std::time::Duration = std::time::Duration::from_secs(1 << 40);
