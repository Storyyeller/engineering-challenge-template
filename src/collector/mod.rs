//! The exchange-agnostic half of the collector.
//!
//! Each source (in practice, one websocket connection) maintains its own books and reports what
//! happens to them as [`SourceEvent`]s. Several sources can follow the same symbol, for redundancy.
//! The [`Publisher`] merges them into a single stream of snapshots and price level updates: for each
//! symbol it follows one source at a time, and switches to another (publishing a fresh snapshot) when
//! that source's book goes out of sync, its connection drops, or it falls behind another source.
//!
//! Prices and quantities are in the output's fixed point: 1_000_000 = 1 USDC, or one contract.

mod publisher;

pub use publisher::{Message, Output, Publisher, PublisherStats, REORDER_WINDOW, run_publisher};

use chrono::{DateTime, Utc};

use crate::Side;

/// Identifies a source; sources are numbered from 0.
pub type SourceId = usize;

/// One price level: price and absolute quantity, in fixed point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub qty: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The source's book for `symbol` is in sync, and is exactly this. Sent when the book is
    /// (re)built from an exchange snapshot, and in reply to a book request (see
    /// [`Publisher::take_requests`]).
    Book {
        symbol: String,
        /// Best (highest) first.
        bids: Vec<Level>,
        /// Best (lowest) first.
        asks: Vec<Level>,
        /// The exchange's sequence clock for this book: higher means more recent. Used to tell
        /// whether a source has fallen behind another.
        clock: u64,
        /// When the exchange produced this state, if known; otherwise the publisher uses the time
        /// of the source's last change to the book.
        exchange_ts: Option<DateTime<Utc>>,
    },
    /// One level of an in-sync book changed.
    Level {
        symbol: String,
        side: Side,
        price: i64,
        /// The new absolute quantity; zero removes the level.
        qty: i64,
        clock: u64,
        exchange_ts: DateTime<Utc>,
    },
    /// The source's book for `symbol` is wrong or missing, until its next [`Event::Book`].
    Stale { symbol: String },
    /// The source no longer follows `symbol`.
    Removed { symbol: String },
    /// The source's connection is gone, along with all of its books. Later events from the source
    /// come from a new connection.
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEvent {
    pub source: SourceId,
    /// When the source received the data behind the event (for events it generated itself, when
    /// it did). A source's events must be sent in order; their times may still go backwards (the
    /// wall clock isn't monotonic) without harm.
    pub received_at: DateTime<Utc>,
    pub event: Event,
}
