//! [`Publisher`]: merges redundant sources into one output stream.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};
use crossbeam::channel::{Receiver, RecvTimeoutError};

use super::{Event, Level, SourceEvent, SourceId};
use crate::Side;
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// A symbol switches to another source once that source has been ahead of the followed one (seen a
/// later exchange clock) for this long.
const LAG_SWITCH: TimeDelta = TimeDelta::seconds(1);
/// How often to check for sources that have fallen behind.
const LAG_CHECK_EVERY: Duration = Duration::from_millis(200);
/// Events are held this long before being published, so that events from different sources can be
/// published in the order they were received. (Sources stamp events independently, then race to the
/// publisher; in practice they arrive well under a millisecond out of order.)
pub const REORDER_WINDOW: TimeDelta = TimeDelta::milliseconds(20);

/// Where published messages go.
pub trait Output {
    fn snapshot(&mut self, msg: &SnapshotDomain) -> Result<()>;
    fn update(&mut self, msg: &PriceLevelUpdateDomain) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

impl Output for AlignedSbeWriter {
    fn snapshot(&mut self, msg: &SnapshotDomain) -> Result<()> {
        self.write_msg(msg)
    }

    fn update(&mut self, msg: &PriceLevelUpdateDomain) -> Result<()> {
        self.write_msg(msg)
    }

    fn flush(&mut self) -> Result<()> {
        AlignedSbeWriter::flush(self)
    }
}

/// A published message, for collecting output in memory (e.g. in tests).
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

impl Output for Vec<Message> {
    fn snapshot(&mut self, msg: &SnapshotDomain) -> Result<()> {
        self.push(Message::Snapshot(msg.clone()));
        Ok(())
    }

    fn update(&mut self, msg: &PriceLevelUpdateDomain) -> Result<()> {
        self.push(Message::Update(msg.clone()));
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PublisherStats {
    /// Snapshots published, including periodic ones.
    pub snapshots: u64,
    pub updates: u64,
    /// Times a symbol switched to another source because its source went out of sync or
    /// disconnected. (Not when a source stops following a symbol, which is normal: when a market
    /// closes, say.)
    pub failovers: u64,
    /// Times a symbol switched to another source because its source fell behind (not counted in
    /// `failovers`).
    pub lag_switches: u64,
    /// Times a symbol's source went out of sync with no other source in sync to take over.
    pub unfollowed: u64,
    /// Events that arrived after [`REORDER_WINDOW`], and so were published later than events
    /// received after them.
    pub late: u64,
}

impl fmt::Display for PublisherStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "published snapshots={} updates={} failovers={} lag_switches={} unfollowed={} late={}",
            self.snapshots,
            self.updates,
            self.failovers,
            self.lag_switches,
            self.unfollowed,
            self.late
        )
    }
}

#[derive(Debug)]
struct SourceState {
    id: SourceId,
    /// The source's book is in sync.
    synced: bool,
    /// Exchange clock of the source's last change to the book.
    clock: u64,
    exchange_ts: Option<DateTime<Utc>>,
}

/// One symbol's book, as published.
#[derive(Debug, Default)]
struct PublishedBook {
    /// Price -> quantity.
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    /// Exchange time of the last published change.
    exchange_ts: DateTime<Utc>,
}

impl PublishedBook {
    fn replace(&mut self, bids: &[Level], asks: &[Level], exchange_ts: DateTime<Utc>) {
        let levels = |levels: &[Level]| levels.iter().map(|l| (l.price, l.qty)).collect();
        self.bids = levels(bids);
        self.asks = levels(asks);
        self.exchange_ts = exchange_ts;
    }

    fn set_level(&mut self, side: Side, price: i64, qty: i64, exchange_ts: DateTime<Utc>) {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if qty == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
        self.exchange_ts = exchange_ts;
    }

    fn snapshot(&self, symbol: &str, received_at: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp: self.exchange_ts,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

/// Why the followed source is being switched away from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    /// Its book went out of sync, or its connection dropped.
    Failover,
    /// It fell behind another source.
    Lag,
    /// It stopped following the symbol, which isn't a problem with the source.
    Removed,
}

/// The outcome of switching away from the followed source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Switch {
    /// Following this source now, once it has sent its book: ask it for it.
    To(SourceId),
    /// No other source is in sync; whichever next sends a book will be followed.
    Unfollowed,
    /// Wasn't following any source, and still isn't.
    Unchanged,
}

/// Which of a symbol's sources is followed. Only bookkeeping: methods say what the publisher should
/// do about the published book.
#[derive(Debug, Default)]
struct Selector {
    sources: Vec<SourceState>,
    /// The source being followed, if any.
    primary: Option<SourceId>,
    /// We've switched to `primary` and asked it for its book; until that arrives, the published
    /// book is out of date and the source's level updates can't be applied to it.
    awaiting_book: bool,
    /// When some other source got ahead of the followed one.
    behind_since: Option<DateTime<Utc>>,
}

impl Selector {
    fn source_mut(&mut self, id: SourceId) -> Option<&mut SourceState> {
        self.sources.iter_mut().find(|s| s.id == id)
    }

    /// The followed source, if its book is in sync and is the one published.
    fn following(&self) -> Option<&SourceState> {
        let primary = self.primary.filter(|_| !self.awaiting_book)?;
        self.sources.iter().find(|s| s.id == primary && s.synced)
    }

    /// `source` has sent its whole book, now in sync. Returns whether to publish it (the symbol now
    /// follows `source`), and the exchange time to publish it with.
    fn on_book(
        &mut self,
        source: SourceId,
        clock: u64,
        exchange_ts: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
    ) -> (bool, DateTime<Utc>) {
        // Take over if this is the source we follow, or we aren't following one in sync.
        let take_over = match self.primary {
            Some(p) => p == source || !self.sources.iter().any(|s| s.id == p && s.synced),
            None => true,
        };
        let primary_clock = self.following().map(|p| p.clock);
        let src = match self.sources.iter().position(|s| s.id == source) {
            Some(i) => &mut self.sources[i],
            None => {
                self.sources.push(SourceState {
                    id: source,
                    synced: false,
                    clock: 0,
                    exchange_ts: None,
                });
                self.sources.last_mut().unwrap()
            }
        };
        src.synced = true;
        // A snapshot's clock may be later than the last change to the book, so it doesn't show that
        // a backup is ahead of the source we follow: only its updates do.
        src.clock = match primary_clock {
            Some(p) if !take_over => clock.min(p),
            _ => clock,
        };
        let exchange_ts = exchange_ts.or(src.exchange_ts).unwrap_or(received_at);
        src.exchange_ts = Some(exchange_ts);
        if take_over {
            self.primary = Some(source);
            self.awaiting_book = false;
        }
        (take_over, exchange_ts)
    }

    /// A level of `source`'s book changed. Returns whether to publish the change.
    fn on_level(&mut self, source: SourceId, clock: u64, exchange_ts: DateTime<Utc>) -> bool {
        let Some(src) = self.source_mut(source).filter(|s| s.synced) else {
            return false;
        };
        // Never backwards: a stray older update mustn't make the source look behind.
        src.clock = src.clock.max(clock);
        src.exchange_ts = Some(exchange_ts);
        self.primary == Some(source) && !self.awaiting_book
    }

    /// `source`'s book went out of sync. Returns the switch, if it was the one followed.
    fn on_stale(&mut self, source: SourceId) -> Option<Switch> {
        self.source_mut(source)?.synced = false;
        (self.primary == Some(source)).then(|| self.fail_over())
    }

    /// `source` no longer follows the symbol. Returns the switch, if it was the one followed.
    fn remove(&mut self, source: SourceId) -> Option<Switch> {
        self.sources.retain(|s| s.id != source);
        (self.primary == Some(source)).then(|| self.fail_over())
    }

    /// Stop following the primary: switch to the in-sync source furthest ahead, if there is one.
    fn fail_over(&mut self) -> Switch {
        let next = self
            .sources
            .iter()
            .filter(|s| s.synced && Some(s.id) != self.primary)
            .max_by_key(|s| s.clock)
            .map(|s| s.id);
        match next {
            Some(next) => {
                self.primary = Some(next);
                self.awaiting_book = true;
                Switch::To(next)
            }
            None => {
                self.awaiting_book = false;
                if self.primary.take().is_some() {
                    Switch::Unfollowed
                } else {
                    Switch::Unchanged
                }
            }
        }
    }

    /// Update `behind_since`: whether some other source is ahead of the one we follow.
    fn refresh_lag(&mut self, now: DateTime<Utc>) {
        let behind = self.following().is_some_and(|p| {
            self.sources
                .iter()
                .any(|s| s.synced && s.id != p.id && s.clock > p.clock)
        });
        if !behind {
            self.behind_since = None;
        } else if self.behind_since.is_none() {
            self.behind_since = Some(now);
        }
    }
}

#[derive(Debug, Default)]
struct SymbolState {
    book: PublishedBook,
    selector: Selector,
}

/// Merges [`SourceEvent`]s into published snapshots and price level updates.
///
/// For each symbol it follows one source, the primary, publishing that source's book changes.
/// When the primary's book goes out of sync, it stops following the symbol, or its connection
/// drops, the publisher switches to another source whose book is in sync: it asks that source for
/// its book (see [`Publisher::take_requests`]), and publishes it as a snapshot when it arrives. It
/// also switches when another source stays ahead of the primary for [`LAG_SWITCH`], which covers
/// a connection that's stalled or buffering without having dropped.
///
/// Every published message reflects the book of the source being followed, so the stream is
/// consistent: applying the updates to the latest snapshot gives the source's book.
#[derive(Debug, Default)]
pub struct Publisher {
    symbols: HashMap<String, SymbolState>,
    /// Symbols whose selector has `behind_since` set.
    behind: HashSet<String>,
    /// Book requests to deliver to sources.
    requests: Vec<(SourceId, String)>,
    pub stats: PublisherStats,
}

impl Publisher {
    /// Number of symbols with at least one source.
    pub fn symbols(&self) -> usize {
        self.symbols.len()
    }

    /// Number of symbols whose primary's book is in sync and published.
    pub fn followed(&self) -> usize {
        self.symbols
            .values()
            .filter(|s| s.selector.following().is_some())
            .count()
    }

    /// Requests to deliver: each source should reply with an [`Event::Book`] of its book for the
    /// symbol if it has one in sync, or an [`Event::Stale`] if it doesn't.
    pub fn take_requests(&mut self) -> Vec<(SourceId, String)> {
        std::mem::take(&mut self.requests)
    }

    /// Handle one event. Events must be handled in the order they were received.
    pub fn handle(&mut self, ev: SourceEvent, out: &mut impl Output) -> Result<()> {
        let (source, received_at) = (ev.source, ev.received_at);
        let symbol = match ev.event {
            Event::Book {
                symbol,
                bids,
                asks,
                clock,
                exchange_ts,
            } => {
                let sym = self.symbols.entry(symbol.clone()).or_default();
                let (publish, exchange_ts) =
                    sym.selector
                        .on_book(source, clock, exchange_ts, received_at);
                if publish {
                    sym.book.replace(&bids, &asks, exchange_ts);
                    out.snapshot(&sym.book.snapshot(&symbol, received_at))?;
                    self.stats.snapshots += 1;
                }
                symbol
            }
            Event::Level {
                symbol,
                side,
                price,
                qty,
                clock,
                exchange_ts,
            } => {
                let Some(sym) = self.symbols.get_mut(&symbol) else {
                    return Ok(());
                };
                if sym.selector.on_level(source, clock, exchange_ts) {
                    sym.book.set_level(side, price, qty, exchange_ts);
                    out.update(&PriceLevelUpdateDomain {
                        backtest_timestamp: received_at,
                        exchange_timestamp: exchange_ts,
                        price_level: price,
                        quantity: qty,
                        side,
                        symbol: symbol.clone(),
                    })?;
                    self.stats.updates += 1;
                }
                symbol
            }
            Event::Stale { symbol } => {
                let Some(sym) = self.symbols.get_mut(&symbol) else {
                    return Ok(());
                };
                if let Some(switch) = sym.selector.on_stale(source) {
                    self.switched(&symbol, switch, Reason::Failover);
                }
                symbol
            }
            Event::Removed { symbol } => {
                self.remove_source(&symbol, source, received_at, Reason::Removed);
                return Ok(());
            }
            Event::Disconnected => {
                let affected: Vec<String> = self
                    .symbols
                    .iter()
                    .filter(|(_, s)| s.selector.sources.iter().any(|s| s.id == source))
                    .map(|(t, _)| t.clone())
                    .collect();
                for symbol in affected {
                    self.remove_source(&symbol, source, received_at, Reason::Failover);
                }
                return Ok(());
            }
        };
        self.track_lag(&symbol, received_at);
        Ok(())
    }

    /// Switch symbols whose source has stayed behind another for [`LAG_SWITCH`].
    pub fn check_lag(&mut self, now: DateTime<Utc>) {
        let due: Vec<String> = self
            .behind
            .iter()
            .filter(|t| {
                let since = self.symbols.get(*t).and_then(|s| s.selector.behind_since);
                since.is_some_and(|since| now - since >= LAG_SWITCH)
            })
            .cloned()
            .collect();
        for symbol in due {
            let switch = self.symbols.get_mut(&symbol).unwrap().selector.fail_over();
            self.switched(&symbol, switch, Reason::Lag);
            self.track_lag(&symbol, now);
        }
    }

    /// Publish a snapshot of every symbol whose book is in sync. A symbol that's switching source
    /// isn't included: its snapshot is published when the new source's book arrives.
    pub fn snapshot_all(&mut self, now: DateTime<Utc>, out: &mut impl Output) -> Result<()> {
        let mut symbols: Vec<&String> = self
            .symbols
            .iter()
            .filter(|(_, s)| s.selector.following().is_some())
            .map(|(t, _)| t)
            .collect();
        symbols.sort(); // deterministic output
        for symbol in symbols {
            out.snapshot(&self.symbols[symbol].book.snapshot(symbol, now))?;
            self.stats.snapshots += 1;
        }
        Ok(())
    }

    fn remove_source(
        &mut self,
        symbol: &str,
        source: SourceId,
        now: DateTime<Utc>,
        reason: Reason,
    ) {
        let Some(sym) = self.symbols.get_mut(symbol) else {
            return;
        };
        let switch = sym.selector.remove(source);
        if sym.selector.sources.is_empty() {
            self.symbols.remove(symbol);
            self.behind.remove(symbol);
            return;
        }
        if let Some(switch) = switch {
            self.switched(symbol, switch, reason);
        }
        self.track_lag(symbol, now);
    }

    /// Act on a switch.
    fn switched(&mut self, symbol: &str, switch: Switch, reason: Reason) {
        match (switch, reason) {
            (Switch::To(next), _) => {
                self.requests.push((next, symbol.to_owned()));
                match reason {
                    Reason::Failover => self.stats.failovers += 1,
                    Reason::Lag => self.stats.lag_switches += 1,
                    Reason::Removed => {}
                }
            }
            (Switch::Unfollowed, Reason::Failover | Reason::Lag) => self.stats.unfollowed += 1,
            _ => {}
        }
    }

    /// Update whether `symbol`'s followed source is behind, and the set of those that are.
    fn track_lag(&mut self, symbol: &str, now: DateTime<Utc>) {
        let Some(sym) = self.symbols.get_mut(symbol) else {
            return;
        };
        sym.selector.refresh_lag(now);
        if sym.selector.behind_since.is_some() {
            if !self.behind.contains(symbol) {
                self.behind.insert(symbol.to_owned());
            }
        } else {
            self.behind.remove(symbol);
        }
    }
}

/// The next whole minute after `now`.
fn next_minute(now: DateTime<Utc>) -> DateTime<Utc> {
    let minute = 60_000_000;
    DateTime::from_timestamp_micros((now.timestamp_micros().div_euclid(minute) + 1) * minute)
        .unwrap_or(now)
}

/// An event waiting to be released, ordered by (time, arrival).
#[derive(Debug, PartialEq, Eq)]
struct Queued {
    key: (DateTime<Utc>, u64),
    event: SourceEvent,
}

impl Ord for Queued {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Puts events from several sources back in the order they were received, keeping each source's
/// events in the order sent.
#[derive(Debug, Default)]
struct Reorder {
    heap: BinaryHeap<Reverse<Queued>>,
    arrivals: u64,
    /// Per source, the time of its last event; later ones are ordered no earlier.
    last: HashMap<SourceId, DateTime<Utc>>,
    /// Everything up to here has been released.
    released: DateTime<Utc>,
}

impl Reorder {
    /// Queue an event. Returns false if it's late: received before events already released.
    fn push(&mut self, event: SourceEvent) -> bool {
        let last = self.last.entry(event.source).or_default();
        let at = event.received_at.max(*last).max(self.released);
        *last = at;
        self.arrivals += 1;
        let on_time = event.received_at >= self.released;
        self.heap.push(Reverse(Queued {
            key: (at, self.arrivals),
            event,
        }));
        on_time
    }

    fn next_at(&self) -> Option<DateTime<Utc>> {
        self.heap.peek().map(|q| q.0.key.0)
    }

    /// The next event, if it was received no later than `until`.
    fn pop(&mut self, until: DateTime<Utc>) -> Option<SourceEvent> {
        let at = self.next_at().filter(|&at| at <= until)?;
        self.released = at;
        self.heap.pop().map(|q| q.0.event)
    }
}

/// Publish events from `events` to `out` until every sender has gone, then flush.
///
/// Events are published in the order they were received, [`REORDER_WINDOW`] after they were
/// received. At the start of every minute (by receive time: after everything received before it)
/// a snapshot of every symbol in sync is published. `request` delivers book requests to sources (see
/// [`Publisher::take_requests`]); `report` is called once a minute.
pub fn run_publisher(
    events: Receiver<SourceEvent>,
    out: &mut impl Output,
    mut request: impl FnMut(SourceId, String),
    mut report: impl FnMut(&Publisher),
) -> Result<Publisher> {
    let mut publisher = Publisher::default();
    let mut queue = Reorder::default();
    let mut next_snapshot = next_minute(Utc::now());
    let mut next_lag_check = Instant::now() + LAG_CHECK_EVERY;
    let mut done = false;
    let result = loop {
        // Wake for the next event due out, the next snapshot, or the next lag check.
        let until = |t: DateTime<Utc>| {
            (t + REORDER_WINDOW - Utc::now())
                .to_std()
                .unwrap_or_default()
        };
        let mut wait =
            until(next_snapshot).min(next_lag_check.saturating_duration_since(Instant::now()));
        if let Some(at) = queue.next_at() {
            wait = wait.min(until(at));
        }
        if !done {
            match events.recv_timeout(wait) {
                Ok(ev) => {
                    if !queue.push(ev) {
                        publisher.stats.late += 1;
                    }
                    // Take everything already waiting before publishing anything.
                    while let Ok(ev) = events.try_recv() {
                        if !queue.push(ev) {
                            publisher.stats.late += 1;
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                // Publish whatever is left, then stop.
                Err(RecvTimeoutError::Disconnected) => done = true,
            }
        }

        // Everything received up to here can go out (everything, once the sources are gone).
        let horizon = if done {
            DateTime::<Utc>::MAX_UTC
        } else {
            Utc::now() - REORDER_WINDOW
        };
        let step = (|| {
            loop {
                let next = queue.next_at().filter(|&at| at <= horizon);
                if !done && next_snapshot <= horizon && next.is_none_or(|at| at >= next_snapshot) {
                    publisher.snapshot_all(next_snapshot, out)?;
                    report(&publisher);
                    next_snapshot = next_minute(next_snapshot);
                    continue;
                }
                let Some(ev) = queue.pop(horizon) else { break };
                publisher.handle(ev, out)?;
            }
            anyhow::Ok(())
        })();
        if let Err(e) = step {
            break Err(e);
        }
        if done {
            break Ok(());
        }
        if Instant::now() >= next_lag_check {
            publisher.check_lag(horizon);
            next_lag_check = Instant::now() + LAG_CHECK_EVERY;
        }
        for (source, symbol) in publisher.take_requests() {
            request(source, symbol);
        }
    };
    // Flush whatever was written, even after an error.
    let flushed = out.flush();
    result.and(flushed)?;
    Ok(publisher)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_700_000_000_000 + ms).unwrap()
    }

    fn book(source: SourceId, bid: (i64, i64), clock: u64, ms: i64) -> SourceEvent {
        SourceEvent {
            source,
            received_at: t(ms),
            event: Event::Book {
                symbol: "M".into(),
                bids: vec![Level {
                    price: bid.0,
                    qty: bid.1,
                }],
                asks: vec![
                    Level {
                        price: 600_000,
                        qty: 1_000_000,
                    },
                    Level {
                        price: 700_000,
                        qty: 2_000_000,
                    },
                ],
                clock,
                exchange_ts: Some(t(ms - 1)),
            },
        }
    }

    fn level(source: SourceId, price: i64, qty: i64, clock: u64, ms: i64) -> SourceEvent {
        SourceEvent {
            source,
            received_at: t(ms),
            event: Event::Level {
                symbol: "M".into(),
                side: Side::Buy,
                price,
                qty,
                clock,
                exchange_ts: t(ms - 1),
            },
        }
    }

    fn ev(source: SourceId, event: Event) -> SourceEvent {
        SourceEvent {
            source,
            received_at: t(0),
            event,
        }
    }

    fn stale(source: SourceId) -> SourceEvent {
        ev(source, Event::Stale { symbol: "M".into() })
    }

    struct H {
        p: Publisher,
        out: Vec<Message>,
    }

    impl H {
        fn new() -> Self {
            Self {
                p: Publisher::default(),
                out: vec![],
            }
        }

        fn send(&mut self, mut e: SourceEvent, now_ms: i64) {
            e.received_at = t(now_ms);
            self.p.handle(e, &mut self.out).unwrap();
        }

        /// Messages published since the last call.
        fn take(&mut self) -> Vec<Message> {
            std::mem::take(&mut self.out)
        }

        fn bids(&self) -> Vec<(i64, i64)> {
            self.p.symbols["M"]
                .book
                .bids
                .iter()
                .map(|(&p, &q)| (p, q))
                .collect()
        }
    }

    fn snapshot_bids(m: &Message) -> Vec<(i64, i64)> {
        let Message::Snapshot(s) = m else {
            panic!("expected snapshot, got {m:?}")
        };
        s.bids.iter().map(|l| (l.price_level, l.quantity)).collect()
    }

    #[test]
    fn follows_first_source_and_ignores_others() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 1), 1);
        let out = h.take();
        assert_eq!(out.len(), 1);
        let Message::Snapshot(s) = &out[0] else {
            panic!()
        };
        assert_eq!(
            (s.symbol.as_str(), s.received_at, s.exchange_timestamp),
            ("M", t(0), t(-1))
        );
        // Asks best (lowest) first.
        assert_eq!(
            s.asks.iter().map(|l| l.price_level).collect::<Vec<_>>(),
            vec![600_000, 700_000]
        );

        h.send(level(0, 410_000, 3, 11, 2), 2);
        h.send(level(1, 410_000, 3, 11, 3), 3);
        let out = h.take();
        assert_eq!(out.len(), 1);
        let Message::Update(u) = &out[0] else {
            panic!()
        };
        assert_eq!(
            (u.price_level, u.quantity, u.side, u.backtest_timestamp),
            (410_000, 3, Side::Buy, t(2))
        );
        h.send(level(0, 400_000, 0, 12, 4), 4);
        assert_eq!(h.bids(), vec![(410_000, 3)]);
    }

    #[test]
    fn stale_primary_fails_over_with_a_snapshot() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 0), 0);
        h.take();
        h.send(stale(0), 1);
        assert_eq!(h.p.take_requests(), vec![(1, "M".to_string())]);
        // Source 1's updates sent before it answers are part of the book it answers with.
        h.send(level(1, 410_000, 3, 11, 2), 2);
        assert!(h.take().is_empty());
        h.send(book(1, (410_000, 3), 11, 3), 3);
        let out = h.take();
        assert_eq!(out.len(), 1);
        assert_eq!(snapshot_bids(&out[0]), vec![(410_000, 3)]);
        assert_eq!(h.p.stats.failovers, 1);
        // Source 0 resyncing doesn't take the symbol back.
        h.send(book(0, (410_000, 3), 11, 4), 4);
        h.send(level(0, 420_000, 1, 12, 5), 5);
        assert!(h.take().is_empty());
    }

    #[test]
    fn no_source_in_sync_waits_for_the_next_book() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 0), 0);
        h.send(stale(1), 1);
        h.send(stale(0), 1);
        assert!(h.p.take_requests().is_empty());
        assert_eq!(h.p.stats.unfollowed, 1);
        h.take();
        let mut snap = Vec::new();
        h.p.snapshot_all(t(2), &mut snap).unwrap();
        assert!(snap.is_empty()); // nothing in sync to publish
        h.send(book(1, (420_000, 1), 12, 3), 3);
        assert_eq!(snapshot_bids(&h.take()[0]), vec![(420_000, 1)]);
        h.send(level(1, 420_000, 2, 13, 4), 4);
        assert_eq!(h.take().len(), 1);
    }

    #[test]
    fn disconnect_and_removal() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 0), 0);
        h.send(ev(0, Event::Disconnected), 1);
        assert_eq!(h.p.take_requests(), vec![(1, "M".to_string())]);
        h.send(book(1, (400_000, 5), 10, 2), 2);
        h.send(ev(1, Event::Removed { symbol: "M".into() }), 3);
        assert_eq!(h.p.symbols(), 0);
        assert_eq!(h.p.stats.failovers, 1); // the disconnect
        // A source that isn't following the symbol any more is ignored.
        h.take();
        h.send(level(1, 400_000, 1, 11, 4), 4);
        h.send(stale(1), 4);
        assert!(h.take().is_empty());
    }

    #[test]
    fn removal_switches_source_without_counting_a_failover() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 0), 0);
        h.send(ev(0, Event::Removed { symbol: "M".into() }), 1);
        assert_eq!(h.p.take_requests(), vec![(1, "M".to_string())]);
        assert_eq!((h.p.stats.failovers, h.p.stats.unfollowed), (0, 0));
    }

    #[test]
    fn lagging_source_is_replaced() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 0), 0);
        // Source 1 sees a change source 0 doesn't deliver.
        h.send(level(1, 400_000, 6, 11, 100), 100);
        h.p.check_lag(t(900));
        assert!(h.p.take_requests().is_empty()); // not yet
        // Source 0 catching up resets the timer.
        h.send(level(0, 400_000, 6, 11, 950), 950);
        h.p.check_lag(t(1200));
        assert!(h.p.take_requests().is_empty());
        h.send(level(1, 400_000, 7, 12, 1300), 1300);
        h.p.check_lag(t(2300));
        assert_eq!(h.p.take_requests(), vec![(1, "M".to_string())]);
        assert_eq!((h.p.stats.lag_switches, h.p.stats.failovers), (1, 0));
    }

    #[test]
    fn backup_snapshot_clock_alone_is_not_lag() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 0), 0);
        h.send(stale(1), 1);
        // Source 1 resyncs with a later clock but the same book: not a sign source 0 is behind.
        h.send(book(1, (400_000, 5), 15, 2), 2);
        h.p.check_lag(t(5_000));
        assert!(h.p.take_requests().is_empty());
        // An update source 0 hasn't delivered is.
        h.send(level(1, 400_000, 6, 16, 5_000), 5_000);
        h.p.check_lag(t(6_000));
        assert_eq!(h.p.take_requests(), vec![(1, "M".to_string())]);
    }

    #[test]
    fn older_update_does_not_move_clock_back() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(book(1, (400_000, 5), 10, 0), 0);
        h.send(level(0, 400_000, 6, 12, 1), 1);
        h.send(level(1, 400_000, 6, 12, 1), 1);
        h.send(level(0, 410_000, 1, 11, 2), 2); // an older change arriving late
        h.p.check_lag(t(5_000));
        assert!(h.p.take_requests().is_empty());
    }

    #[test]
    fn periodic_snapshots() {
        let mut h = H::new();
        h.send(book(0, (400_000, 5), 10, 0), 0);
        h.send(level(0, 400_000, 6, 11, 5), 5);
        let mut snap = Vec::new();
        h.p.snapshot_all(t(60_000), &mut snap).unwrap();
        assert_eq!(snap.len(), 1);
        let Message::Snapshot(s) = &snap[0] else {
            panic!()
        };
        assert_eq!((s.received_at, s.exchange_timestamp), (t(60_000), t(4)));
        assert_eq!(snapshot_bids(&snap[0]), vec![(400_000, 6)]);
    }

    /// Through the real writer and reader.
    #[test]
    fn round_trips_through_sbe_file() {
        use crate::util::dtos::challenge_orderbook::AnyMessage;
        use crate::util::{AlignedSbeReader, OrderbookSchema};

        let (tx, rx) = crossbeam::channel::unbounded();
        tx.send(book(0, (400_000, 5), 10, 0)).unwrap();
        tx.send(level(0, 410_000, 3, 11, 2)).unwrap();
        drop(tx);
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer = AlignedSbeWriter::create(file.path(), 64 << 20).unwrap();
        let p = run_publisher(rx, &mut writer, |_, _| {}, |_| {}).unwrap();
        drop(writer);
        assert_eq!((p.stats.snapshots, p.stats.updates), (1, 1));

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let Some(AnyMessage::Snapshot(s)) = reader.read_msg().unwrap() else {
            panic!("expected snapshot")
        };
        let s = SnapshotDomain::try_from_decoder(s).unwrap();
        assert_eq!(
            (s.symbol.as_str(), s.bids.len(), s.asks.len(), s.received_at),
            ("M", 1, 2, t(0))
        );
        let Some(AnyMessage::PriceLevelUpdate(u)) = reader.read_msg().unwrap() else {
            panic!("expected update")
        };
        let u = PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
        assert_eq!(
            (u.price_level, u.quantity, u.side, u.exchange_timestamp),
            (410_000, 3, Side::Buy, t(1))
        );
        assert!(reader.read_msg().unwrap().is_none());
    }

    #[test]
    fn reorder_releases_in_receive_order() {
        let mut q = Reorder::default();
        assert!(q.push(level(0, 1, 1, 1, 10)));
        assert!(q.push(level(1, 2, 1, 1, 5)));
        assert!(q.push(level(0, 3, 1, 1, 8))); // clock went backwards: stays after source 0's first
        assert!(q.pop(t(4)).is_none());
        let order: Vec<i64> = std::iter::from_fn(|| q.pop(t(100)))
            .map(|e| match e.event {
                Event::Level { price, .. } => price,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(order, vec![2, 1, 3]);
        assert!(!q.push(level(1, 4, 1, 1, 9))); // received before events already released: late
        assert_eq!(q.pop(t(100)).map(|e| e.received_at), Some(t(9)));
    }

    #[test]
    fn next_minute_boundary() {
        let at = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            next_minute(at("2026-01-01T00:00:59.999Z")),
            at("2026-01-01T00:01:00Z")
        );
        assert_eq!(
            next_minute(at("2026-01-01T00:01:00Z")),
            at("2026-01-01T00:02:00Z")
        );
    }
}
