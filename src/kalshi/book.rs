//! In-memory orderbooks rebuilt from snapshots + deltas, with sequence-gap detection.
//!
//! Besides maintaining the books, [`OrderbookState`] records every [`Change`] that someone downstream
//! of the books needs to hear about (see [`OrderbookState::take_changes`]).

use crate::kalshi::fixed::{Price, Qty};
use crate::kalshi::messages::{Body, Delta, Frame, Side, Snapshot};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use tracing::{debug, warn};

/// One market's book. Both sides hold resting *bids*: `yes` = bids to buy YES, `no` = bids to buy NO.
/// A NO bid at price p is equivalent to a YES ask at $1 - p.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Book {
    pub yes: BTreeMap<Price, Qty>,
    pub no: BTreeMap<Price, Qty>,
    /// Exchange clock of the last applied change.
    pub clock: u64,
}

impl Book {
    /// Levels without a positive quantity are left out (see [`Stats::bad_levels`]).
    pub fn from_snapshot(s: &Snapshot) -> Self {
        let levels = |l: &[(Price, Qty)]| l.iter().copied().filter(|&(_, q)| q > Qty::ZERO).collect();
        Self { yes: levels(&s.yes), no: levels(&s.no), clock: s.clock }
    }

    /// Apply a delta. Returns Err if a level would go negative, which means our book is out of sync
    /// (the level is removed in that case).
    pub fn apply(&mut self, d: &Delta) -> Result<(), String> {
        let levels = match d.side {
            Side::Yes => &mut self.yes,
            Side::No => &mut self.no,
        };
        let qty = levels.entry(d.price).or_insert(Qty::ZERO);
        *qty += d.delta;
        let new_qty = *qty;
        if new_qty <= Qty::ZERO {
            levels.remove(&d.price);
        }
        self.clock = d.clock;
        if new_qty < Qty::ZERO {
            return Err(format!(
                "{:?} level {} went negative ({new_qty}) after delta {}",
                d.side, d.price, d.delta
            ));
        }
        Ok(())
    }

    pub fn side(&self, side: Side) -> &BTreeMap<Price, Qty> {
        match side {
            Side::Yes => &self.yes,
            Side::No => &self.no,
        }
    }

    pub fn best_yes_bid(&self) -> Option<(Price, Qty)> {
        self.yes.last_key_value().map(|(&p, &q)| (p, q))
    }

    pub fn best_no_bid(&self) -> Option<(Price, Qty)> {
        self.no.last_key_value().map(|(&p, &q)| (p, q))
    }

    /// Cheapest price to buy YES (= $1 - best NO bid).
    pub fn best_yes_ask(&self) -> Option<(Price, Qty)> {
        self.best_no_bid().map(|(p, q)| (p.complement(), q))
    }

    /// True if the best YES and NO bids add up to $1 or more. Those would match against each other,
    /// so a correct book should never be in this state.
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_yes_bid(), self.best_no_bid()), (Some((y, _)), Some((n, _))) if y >= n.complement())
    }

    /// Short description of the first difference from `other`, for mismatch reports.
    pub fn first_difference(&self, other: &Book) -> Option<String> {
        for (name, a, b) in [("yes", &self.yes, &other.yes), ("no", &self.no, &other.no)] {
            let prices: std::collections::BTreeSet<_> = a.keys().chain(b.keys()).collect();
            for p in prices {
                let (qa, qb) = (a.get(p).copied().unwrap_or_default(), b.get(p).copied().unwrap_or_default());
                if qa != qb {
                    return Some(format!("{name} {p}: {qa} vs {qb}"));
                }
            }
        }
        None
    }
}

impl fmt::Display for Book {
    /// e.g. "bid 0.2800 x 505.00 / ask 0.3200 x 1.00 (12 yes / 9 no levels)"
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let side = |x: Option<(Price, Qty)>| match x {
            Some((p, q)) => format!("{p} x {q}"),
            None => "-".to_string(),
        };
        write!(
            f,
            "bid {} / ask {} ({} yes / {} no levels)",
            side(self.best_yes_bid()),
            side(self.best_yes_ask()),
            self.yes.len(),
            self.no.len()
        )
    }
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub book: Book,
    /// Set when we know the book is wrong and a fresh snapshot has been requested.
    pub stale: bool,
    /// Set when we've requested a snapshot to compare against our (supposedly in-sync) book.
    pub verify_pending: bool,
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub snapshots: u64,
    pub deltas: u64,
    pub seq_gaps: u64,
    pub negative_levels: u64,
    /// Number of deltas after which the book was crossed.
    pub crossed: u64,
    pub deltas_for_unknown_market: u64,
    /// Snapshots the server sent without us asking (e.g. an empty book when a market closes).
    pub unsolicited_snapshots: u64,
    /// Results of comparing requested snapshots against our in-sync books.
    pub verify_ok: u64,
    pub verify_mismatch: u64,
    /// Messages whose body didn't parse.
    pub malformed: u64,
    /// Snapshot levels with a zero or negative quantity (dropped).
    pub bad_levels: u64,
    /// Snapshots and deltas for markets we've unsubscribed from (ignored).
    pub after_removal: u64,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "snapshots={} deltas={} seq_gaps={} negative_levels={} crossed={} unknown_market={} unsolicited_snapshots={} verify_ok={} verify_mismatch={} malformed={} bad_levels={} after_removal={}",
            self.snapshots,
            self.deltas,
            self.seq_gaps,
            self.negative_levels,
            self.crossed,
            self.deltas_for_unknown_market,
            self.unsolicited_snapshots,
            self.verify_ok,
            self.verify_mismatch,
            self.malformed,
            self.bad_levels,
            self.after_removal
        )
    }
}

/// Something that happened to a book, for consumers that mirror the books (e.g. the output stream).
/// Applying the changes in order to a copy of the in-sync books reproduces them exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// The book was (re)built from a snapshot: it's in sync now, and wasn't before or has changed.
    /// Read it from [`OrderbookState::markets`]. A snapshot that just confirms an in-sync book isn't a change.
    Snapshot { ticker: String, sending_ts_ms: Option<u64> },
    /// A level of an in-sync book now holds `qty` (zero: the level is gone).
    Level { ticker: String, side: Side, price: Price, qty: Qty, clock: u64, ts_ms: Option<u64> },
    /// The book is known to be wrong (or missing); a snapshot will be requested.
    Stale { ticker: String },
    /// Unsubscribed: the book is gone.
    Removed { ticker: String },
}

/// All books on a connection, which come from its one orderbook subscription. Feed every server
/// message to [`OrderbookState::handle`].
#[derive(Debug, Default)]
pub struct OrderbookState {
    pub markets: BTreeMap<String, MarketState>,
    last_seq: HashMap<u64, u64>,
    /// Markets we've unsubscribed from; messages still in flight for them are ignored.
    removed: HashSet<String>,
    pub stats: Stats,
    /// Changes since the last [`OrderbookState::take_changes`].
    changes: Vec<Change>,
}

impl OrderbookState {
    /// Process one message. Returns the markets whose books need a fresh snapshot (empty if none).
    /// Warnings go to stderr.
    pub fn handle(&mut self, frame: &Frame) -> Vec<String> {
        // Lifecycle notifications are on their own subscription, which is tracked separately.
        if frame.body.is_lifecycle() {
            return vec![];
        }
        // Every message on the subscription (snapshots, deltas, `ok` responses) carries the next seq.
        let mut resync = match (frame.sid, frame.seq) {
            (Some(sid), Some(seq)) => self.check_seq(sid, seq),
            _ => vec![],
        };
        let market_resync = match &frame.body {
            Body::OrderbookSnapshot { msg } if !self.ignored(&msg.market_ticker) => {
                self.apply_snapshot(msg, frame.sending_ts_ms);
                vec![]
            }
            Body::OrderbookDelta { msg } if !self.ignored(&msg.market_ticker) => self.apply_delta(msg, frame.sending_ts_ms),
            Body::Malformed { kind, market_ticker, error } => {
                self.stats.malformed += 1;
                warn!("malformed {kind} message ({error})");
                match kind.as_str() {
                    "orderbook_snapshot" | "orderbook_delta" => self.malformed(market_ticker.as_deref()),
                    _ => vec![],
                }
            }
            _ => vec![],
        };
        // Both can be needed: a gap resyncs the markets we know, and this message may be about one we
        // don't know yet.
        for t in market_resync {
            if !resync.contains(&t) {
                resync.push(t);
            }
        }
        resync
    }

    /// Changes to the books since the last call, in order.
    pub fn take_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }

    /// The book for `ticker`, if it's in sync.
    pub fn synced_book(&self, ticker: &str) -> Option<&Book> {
        self.markets.get(ticker).filter(|m| !m.stale).map(|m| &m.book)
    }

    /// Candidates for a verification snapshot request (see [`OrderbookState::expect_verification`]): of
    /// the (up to) `n` markets following `after` in ticker order, wrapping around, those whose books
    /// are in sync and not already being verified. Also returns the last market looked at, to pass
    /// as `after` next time.
    pub fn verifiable_markets(&self, after: Option<&str>, n: usize) -> (Vec<String>, Option<String>) {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let after = after.unwrap_or("");
        let looked: Vec<(&String, &MarketState)> = self
            .markets
            .range::<str, _>((Excluded(after), Unbounded))
            .chain(self.markets.range::<str, _>((Unbounded, Included(after))))
            .take(n)
            .collect();
        let last = looked.last().map(|(t, _)| (*t).clone());
        let verifiable = looked.into_iter().filter(|(_, m)| !m.stale && !m.verify_pending).map(|(t, _)| t.clone()).collect();
        (verifiable, last)
    }

    /// Forget a market after unsubscribing from it. Its messages still in flight are ignored until
    /// [`OrderbookState::expect_markets`] says it's wanted again.
    pub fn remove_market(&mut self, ticker: &str) {
        if self.markets.remove(ticker).is_some() {
            self.changes.push(Change::Removed { ticker: ticker.to_string() });
        }
        self.removed.insert(ticker.to_string());
    }

    /// These markets are being subscribed to (again).
    pub fn expect_markets(&mut self, tickers: &[String]) {
        for t in tickers {
            self.removed.remove(t);
        }
    }

    fn ignored(&mut self, ticker: &str) -> bool {
        let ignored = self.removed.contains(ticker);
        self.stats.after_removal += u64::from(ignored);
        ignored
    }

    /// Snapshots have been requested for these markets: the ones whose books are in sync will be
    /// compared against the snapshots when they arrive. Call when a `get_snapshot` is sent.
    pub fn expect_verification(&mut self, tickers: &[String]) {
        for t in tickers {
            if let Some(m) = self.markets.get_mut(t).filter(|m| !m.stale) {
                m.verify_pending = true;
            }
        }
    }

    /// A requested snapshot isn't coming (the request failed or timed out): stop waiting to verify
    /// these markets. Stale markets stay stale and are listed by [`OrderbookState::stale_markets`].
    pub fn cancel_verification(&mut self, tickers: &[String]) {
        for t in tickers {
            if let Some(m) = self.markets.get_mut(t) {
                m.verify_pending = false;
            }
        }
    }

    /// Markets known to be out of sync, which need a fresh snapshot.
    pub fn stale_markets(&self) -> Vec<String> {
        self.markets.iter().filter(|(_, m)| m.stale).map(|(t, _)| t.clone()).collect()
    }

    fn check_seq(&mut self, sid: u64, seq: u64) -> Vec<String> {
        // Every subscription's seq starts at 1, so a lost first message is a gap too.
        let prev = self.last_seq.insert(sid, seq).unwrap_or(0);
        if seq == prev + 1 {
            return vec![];
        }
        self.stats.seq_gaps += 1;
        warn!("seq gap on sid {sid}: {prev} -> {seq}; resyncing every market");
        self.resync_all()
    }

    /// Mark every market stale.
    fn resync_all(&mut self) -> Vec<String> {
        for (ticker, m) in self.markets.iter_mut().filter(|(_, m)| !m.stale) {
            m.stale = true;
            self.changes.push(Change::Stale { ticker: ticker.clone() });
        }
        self.markets.keys().cloned().collect()
    }

    /// A snapshot or delta for `ticker` (if known) didn't parse, so that book may now be wrong.
    fn malformed(&mut self, ticker: Option<&str>) -> Vec<String> {
        let Some(ticker) = ticker else {
            // Can't tell which market it was for.
            return self.resync_all();
        };
        if self.removed.contains(ticker) {
            return vec![];
        }
        match self.markets.get_mut(ticker) {
            Some(m) if m.stale => vec![],
            Some(m) => {
                m.stale = true;
                self.changes.push(Change::Stale { ticker: ticker.to_string() });
                vec![ticker.to_string()]
            }
            None => self.missing(ticker),
        }
    }

    /// We need a snapshot for a market we have no book for. Track it as a stale (empty) book, so it's
    /// retried via [`OrderbookState::stale_markets`] if this request can't be made yet.
    fn missing(&mut self, ticker: &str) -> Vec<String> {
        let state = MarketState { book: Book::default(), stale: true, verify_pending: false };
        self.markets.insert(ticker.to_string(), state);
        vec![ticker.to_string()]
    }

    fn apply_snapshot(&mut self, s: &Snapshot, sending_ts_ms: Option<u64>) {
        self.stats.snapshots += 1;
        let bad = s.yes.iter().chain(&s.no).filter(|&&(_, q)| q <= Qty::ZERO).count();
        if bad > 0 {
            self.stats.bad_levels += bad as u64;
            warn!("{} snapshot has {bad} level(s) without a positive quantity; dropped", s.market_ticker);
        }
        let book = Book::from_snapshot(s);
        // Compare levels only: the clock moves on without the book changing.
        let unchanged = self
            .markets
            .get(&s.market_ticker)
            .is_some_and(|m| !m.stale && m.book.yes == book.yes && m.book.no == book.no);
        if !unchanged {
            self.changes.push(Change::Snapshot { ticker: s.market_ticker.clone(), sending_ts_ms });
        }
        if let Some(existing) = self.markets.get(&s.market_ticker) {
            if existing.verify_pending && !existing.stale {
                // We asked for this to check our book, which should match exactly.
                match existing.book.first_difference(&book) {
                    None => self.stats.verify_ok += 1,
                    Some(diff) => {
                        self.stats.verify_mismatch += 1;
                        warn!("{} local book differs from snapshot: {diff}", s.market_ticker);
                    }
                }
            } else if !existing.stale {
                // Server-initiated reset; Kalshi sends an empty snapshot when a market closes, for example.
                self.stats.unsolicited_snapshots += 1;
                debug!("unsolicited snapshot for {} replaces local book ({book})", s.market_ticker);
            }
        }
        let state = MarketState { book, stale: false, verify_pending: false };
        self.markets.insert(s.market_ticker.clone(), state);
    }

    fn apply_delta(&mut self, d: &Delta, sending_ts_ms: Option<u64>) -> Vec<String> {
        self.stats.deltas += 1;
        let Some(m) = self.markets.get_mut(&d.market_ticker) else {
            self.stats.deltas_for_unknown_market += 1;
            warn!("delta for {} before its snapshot; requesting one", d.market_ticker);
            return self.missing(&d.market_ticker);
        };
        if m.stale {
            // Already known to be wrong; the requested snapshot will replace it.
            return vec![];
        }
        let result = m.book.apply(d);
        if m.book.is_crossed() {
            self.stats.crossed += 1;
        }
        if let Err(e) = result {
            self.stats.negative_levels += 1;
            warn!("{}: {e}", d.market_ticker);
            m.stale = true;
            self.changes.push(Change::Stale { ticker: d.market_ticker.clone() });
            return vec![d.market_ticker.clone()];
        }
        let qty = m.book.side(d.side).get(&d.price).copied().unwrap_or(Qty::ZERO);
        self.changes.push(Change::Level {
            ticker: d.market_ticker.clone(),
            side: d.side,
            price: d.price,
            qty,
            clock: d.clock,
            ts_ms: d.ts_ms.or(sending_ts_ms),
        });
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(p: &str, q: &str) -> Option<(Price, Qty)> {
        Some((p.parse().unwrap(), q.parse().unwrap()))
    }

    fn frame(json: &str) -> Frame {
        serde_json::from_str(json).unwrap()
    }

    fn snapshot(seq: u64, yes: &str, no: &str) -> Frame {
        frame(&format!(
            r#"{{"type":"orderbook_snapshot","sid":1,"seq":{seq},"msg":{{"market_ticker":"M","clock":1,
                "yes_dollars_fp":{yes},"no_dollars_fp":{no}}}}}"#
        ))
    }

    fn delta(seq: u64, side: &str, price: &str, d: &str) -> Frame {
        frame(&format!(
            r#"{{"type":"orderbook_delta","sid":1,"seq":{seq},"msg":{{"market_ticker":"M","clock":{seq},
                "side":"{side}","price_dollars":"{price}","delta_fp":"{d}"}}}}"#
        ))
    }

    #[test]
    fn snapshot_then_deltas() {
        let mut st = OrderbookState::default();
        assert_eq!(st.handle(&snapshot(1, r#"[["0.4000","10.00"]]"#, r#"[["0.5500","5.00"]]"#)), Vec::<String>::new());
        assert_eq!(st.handle(&delta(2, "yes", "0.4200", "3.00")), Vec::<String>::new());
        assert_eq!(st.handle(&delta(3, "no", "0.5500", "-5.00")), Vec::<String>::new());
        let b = &st.markets["M"].book;
        assert_eq!(b.best_yes_bid(), level("0.42", "3"));
        assert_eq!(b.best_yes_ask(), None); // NO level fully removed
        assert_eq!(b.yes.len(), 2);
        assert_eq!(b.clock, 3);
        assert!(!b.is_crossed());
    }

    #[test]
    fn yes_ask_from_no_bid() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, "[]", r#"[["0.5500","5.00"],["0.3000","1.00"]]"#));
        assert_eq!(st.markets["M"].book.best_yes_ask(), level("0.45", "5"));
    }

    #[test]
    fn crossed_detection() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","10.00"]]"#, r#"[["0.5500","5.00"]]"#));
        st.handle(&delta(2, "yes", "0.4500", "1.00")); // 0.45 + 0.55 = 1.00 -> crossed
        assert_eq!(st.stats.crossed, 1);
    }

    #[test]
    fn seq_gap_requests_resync_and_snapshot_clears_it() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","10.00"]]"#, "[]"));
        let r = st.handle(&delta(5, "yes", "0.4000", "1.00"));
        assert_eq!(r, ["M"]);
        assert!(st.markets["M"].stale);
        st.handle(&snapshot(6, r#"[["0.4000","2.00"]]"#, "[]"));
        assert!(!st.markets["M"].stale);
        assert_eq!(st.markets["M"].book.best_yes_bid(), level("0.40", "2"));
        // Snapshot replacing a stale book is not a verification.
        assert_eq!((st.stats.verify_ok, st.stats.verify_mismatch), (0, 0));
    }

    #[test]
    fn negative_level_requests_resync_once() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","1.00"]]"#, "[]"));
        assert!(!st.handle(&delta(2, "yes", "0.4000", "-2.00")).is_empty());
        assert!(st.handle(&delta(3, "yes", "0.3000", "-2.00")).is_empty()); // stale: not applied
        assert_eq!(st.stats.negative_levels, 1);
        assert!(st.markets["M"].book.yes.is_empty());
    }

    #[test]
    fn ok_message_advances_seq() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, "[]", "[]"));
        st.handle(&frame(r#"{"id":2,"sid":1,"seq":2,"type":"ok","msg":{}}"#));
        assert_eq!(st.handle(&delta(3, "yes", "0.1000", "1.00")), Vec::<String>::new());
        assert_eq!(st.stats.seq_gaps, 0);
    }

    #[test]
    fn verification_snapshot() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","1.00"]]"#, "[]"));
        st.handle(&delta(2, "yes", "0.4000", "1.00"));
        let all = |st: &OrderbookState| st.verifiable_markets(None, usize::MAX).0;
        assert_eq!(all(&st), vec!["M".to_string()]);
        st.expect_verification(&all(&st));
        assert_eq!(all(&st), Vec::<String>::new()); // already pending
        st.handle(&snapshot(3, r#"[["0.4000","2.00"]]"#, "[]"));
        st.expect_verification(&["M".into()]);
        st.handle(&snapshot(4, r#"[["0.4000","3.00"]]"#, "[]"));
        assert_eq!((st.stats.verify_ok, st.stats.verify_mismatch), (1, 1));
    }

    #[test]
    fn unsolicited_snapshot_replaces_book() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","1.00"]]"#, "[]"));
        st.handle(&snapshot(2, "[]", "[]"));
        assert_eq!((st.stats.unsolicited_snapshots, st.stats.verify_mismatch), (1, 0));
        assert!(st.markets["M"].book.yes.is_empty());
    }

    #[test]
    fn stale_retry_and_cancel_verification() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","1.00"]]"#, "[]"));
        assert!(st.stale_markets().is_empty());
        st.expect_verification(&["M".into()]);
        st.cancel_verification(&["M".into()]);
        assert!(!st.markets["M"].verify_pending);
        st.handle(&delta(2, "yes", "0.4000", "-2.00"));
        assert_eq!(st.stale_markets(), vec!["M".to_string()]);
        st.remove_market("M");
        assert!(st.stale_markets().is_empty());
    }

    #[test]
    fn removed_market_messages_ignored_until_re_added() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","1.00"]]"#, "[]"));
        st.remove_market("M");
        assert_eq!(st.handle(&delta(2, "yes", "0.4000", "1.00")), Vec::<String>::new()); // no "before its snapshot" resync
        st.handle(&snapshot(3, "[]", "[]")); // e.g. the empty snapshot sent when a market closes
        assert!(st.markets.is_empty());
        assert_eq!((st.stats.after_removal, st.stats.deltas_for_unknown_market, st.stats.seq_gaps), (2, 0, 0));
        st.expect_markets(&["M".into()]);
        st.handle(&snapshot(4, "[]", "[]"));
        assert!(st.markets.contains_key("M"));
    }

    fn raw(json: &str) -> Frame {
        Frame::parse(&serde_json::from_str(json).unwrap()).unwrap()
    }

    #[test]
    fn malformed_message_resyncs_only_its_market() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, "[]", "[]"));
        st.handle(&frame(r#"{"type":"orderbook_snapshot","sid":1,"seq":2,"msg":{"market_ticker":"N","clock":1}}"#));
        let bad = raw(r#"{"type":"orderbook_delta","sid":1,"seq":3,"msg":{"market_ticker":"M","price_dollars":"9.0000"}}"#);
        assert_eq!(st.handle(&bad), ["M"]);
        assert_eq!(st.handle(&delta(4, "yes", "0.1000", "1.00")), Vec::<String>::new()); // seq advanced past the bad one
        assert_eq!((st.stats.malformed, st.stats.seq_gaps), (1, 0));
        assert!(st.markets["M"].stale && !st.markets["N"].stale);
        // Without a ticker, every market on the subscription is suspect.
        let bad = raw(r#"{"type":"orderbook_delta","sid":1,"seq":5,"msg":{}}"#);
        assert_eq!(st.handle(&bad), ["M", "N"]);
    }

    #[test]
    fn non_positive_snapshot_levels_dropped() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","1.00"],["0.3000","0.00"]]"#, r#"[["0.2000","-1.00"]]"#));
        let b = &st.markets["M"].book;
        assert_eq!((b.yes.len(), b.no.len(), st.stats.bad_levels), (1, 0, 2));
    }

    #[test]
    fn expect_verification_skips_stale_and_unknown() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, r#"[["0.4000","1.00"]]"#, "[]"));
        st.expect_verification(&["M".into(), "UNKNOWN".into()]);
        assert!(st.markets["M"].verify_pending);
        st.handle(&snapshot(2, r#"[["0.4000","1.00"]]"#, "[]"));
        assert_eq!(st.stats.verify_ok, 1);
        st.handle(&delta(3, "yes", "0.4000", "-2.00")); // now stale
        st.expect_verification(&["M".into()]);
        assert!(!st.markets["M"].verify_pending);
    }

    #[test]
    fn lifecycle_frames_dont_affect_seq() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, "[]", "[]"));
        st.handle(&frame(r#"{"type":"market_lifecycle_v2","sid":1,"seq":7,"msg":{"market_ticker":"X","event_type":"created"}}"#));
        st.handle(&frame(r#"{"type":"event_lifecycle","sid":1,"seq":9,"msg":{"event_ticker":"E"}}"#));
        assert_eq!(st.handle(&delta(2, "yes", "0.1000", "1.00")), Vec::<String>::new());
        assert_eq!(st.stats.seq_gaps, 0);
    }

    #[test]
    fn lost_first_message_is_a_gap() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(2, "[]", "[]"));
        assert_eq!(st.stats.seq_gaps, 1);
    }

    #[test]
    fn gap_and_new_market_resynced_together() {
        let mut st = OrderbookState::default();
        st.handle(&snapshot(1, "[]", "[]"));
        let r = st.handle(&frame(
            r#"{"type":"orderbook_delta","sid":1,"seq":5,"msg":{"market_ticker":"N","clock":5,
                "side":"yes","price_dollars":"0.4000","delta_fp":"1.00"}}"#,
        ));
        assert_eq!(r, ["M", "N"]);
    }

    #[test]
    fn delta_before_snapshot() {
        let mut st = OrderbookState::default();
        let r = st.handle(&delta(1, "yes", "0.4000", "1.00"));
        assert_eq!(r, ["M"]);
        // Remembered as stale, so the request is retried if it can't be sent now.
        assert_eq!(st.stale_markets(), vec!["M".to_string()]);
        assert_eq!(st.handle(&delta(2, "yes", "0.4000", "1.00")), Vec::<String>::new());
    }
}
