//! The collector's logic for one connection, with no I/O: a [`Session`] holds the connection's books
//! ([`OrderbookState`]) and its [`Subscriptions`], with one method per event (message, tick,
//! verification timer, discovery result). Each returns the commands to send; call [`Session::sent`]
//! for each one once it has actually gone out, since sending some commands changes the books.

use crate::kalshi::book::{Book, Change, OrderbookState};
use crate::kalshi::discovery::Universe;
use crate::kalshi::messages::{Action, Body, Command, Frame};
use crate::kalshi::subscriptions::{Effects, Subscriptions};
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeSet;
use tracing::{debug, warn};

/// One live connection's logic.
#[derive(Debug)]
pub struct Session {
    pub books: OrderbookState,
    pub subs: Subscriptions,
    /// Subscribed markets with no book at the last tick (their snapshot was lost, say).
    missing: BTreeSet<String>,
    /// Where the next verification slice starts (see [`Session::on_verify`]).
    verify_after: Option<String>,
}

impl Session {
    pub fn new(universe: Universe) -> Self {
        Self {
            books: OrderbookState::default(),
            subs: Subscriptions::new(universe),
            missing: BTreeSet::new(),
            verify_after: None,
        }
    }

    /// Initial subscriptions.
    pub fn start(
        &mut self,
        tickers: impl IntoIterator<Item = String>,
        now_ms: u64,
    ) -> Vec<Command> {
        self.subs.start(tickers, now_ms)
    }

    /// A command from an [`Effects`] (or [`Session::start`]) has been sent.
    pub fn sent(&mut self, cmd: &Command) {
        match cmd.action() {
            // In-sync books get compared against the snapshots this produces.
            Some(Action::GetSnapshot) => self.books.expect_verification(cmd.tickers()),
            Some(Action::DeleteMarkets) => {
                for t in cmd.tickers() {
                    self.books.remove_market(t);
                }
            }
            Some(Action::AddMarkets) | None => self.books.expect_markets(cmd.tickers()),
        }
        self.subs.sent(cmd);
    }

    /// A server message arrived (record it before calling this). Errors are fatal.
    pub fn on_message(&mut self, raw: &Value, now_ms: u64) -> Result<Effects> {
        let frame = match Frame::parse(raw) {
            Ok(f) => f,
            Err(e) => {
                debug!("unparseable message ({e}): {raw}");
                return Ok(Effects::default());
            }
        };
        // Subscription acks, command responses, errors: rare and worth seeing. Lifecycle messages cover
        // the whole exchange, so only the relevant ones are logged (by `subs`).
        // Malformed messages are reported by the book.
        if !matches!(
            frame.body,
            Body::OrderbookSnapshot { .. } | Body::OrderbookDelta { .. } | Body::Malformed { .. }
        ) && !frame.body.is_lifecycle()
        {
            debug!("{raw}");
        }
        let fx = self.subs.handle(&frame, now_ms)?;
        let mut out = self.apply(fx);
        let resync = self.books.handle(&frame);
        let cmds = self.subs.snapshot_request(&resync, now_ms);
        if !cmds.is_empty() {
            debug!("requesting snapshot for {resync:?}");
        }
        out.send.extend(cmds);
        Ok(out)
    }

    /// Periodic housekeeping: timeouts, retries, and snapshots for books still out of sync or missing.
    pub fn on_tick(&mut self, now_ms: u64) -> Effects {
        let fx = self.subs.expire(now_ms);
        let mut out = self.apply(fx);
        let mut wanted = self.books.stale_markets();
        // A subscribed market's first snapshot normally follows within moments; one still missing a whole
        // tick later was lost.
        let missing: BTreeSet<String> = self
            .subs
            .subscribed()
            .filter(|t| !self.books.markets.contains_key(*t))
            .cloned()
            .collect();
        let overdue: Vec<String> = missing.intersection(&self.missing).cloned().collect();
        if !overdue.is_empty() {
            warn!("no snapshot received for {overdue:?}; requesting one");
            wanted.extend(overdue);
        }
        self.missing = missing;
        out.send.extend(self.subs.snapshot_request(&wanted, now_ms));
        out
    }

    /// Request snapshots to check in-sync books against, for the next `1/slices` of the markets: every
    /// `slices` calls check every book, without one big burst of requests. Markets are marked as being
    /// verified only when the request is sent (via [`Session::sent`]), so ones skipped here aren't left
    /// waiting.
    pub fn on_verify(&mut self, now_ms: u64, slices: usize) -> Vec<Command> {
        let n = self.books.markets.len().div_ceil(slices.max(1));
        let (tickers, last) = self
            .books
            .verifiable_markets(self.verify_after.as_deref(), n);
        self.verify_after = last;
        self.subs.snapshot_request(&tickers, now_ms)
    }

    /// REST discovery listed these markets (possibly only some of them, while listing is in progress).
    pub fn on_discovered(
        &mut self,
        markets: impl IntoIterator<Item = String>,
        now_ms: u64,
    ) -> Vec<Command> {
        self.subs.add_markets(markets, now_ms)
    }

    /// REST discovery finished listing every open market: unsubscribe from ones that have closed
    /// without our hearing about it (see [`Subscriptions::on_listing`]).
    pub fn on_listing(&mut self, listed: &BTreeSet<String>, now_ms: u64) -> Vec<Command> {
        let fx = self.subs.on_listing(listed, now_ms);
        self.apply(fx).send
    }

    /// Changes to the books since the last call (see [`Change`]).
    pub fn take_changes(&mut self) -> Vec<Change> {
        self.books.take_changes()
    }

    /// The book for `ticker`, if it's in sync.
    pub fn synced_book(&self, ticker: &str) -> Option<&Book> {
        self.books.synced_book(ticker)
    }

    /// One-line status: subscription counts and stats.
    pub fn summary(&self) -> String {
        let (subscribed, wanted) = self.subs.counts();
        let s = &self.subs;
        format!(
            "markets={subscribed}/{wanted} {} commands={:?} command_errors={:?}",
            self.books.stats, s.commands, s.command_errors
        )
    }

    /// Apply what `Subscriptions` decided to the books, and pass it on.
    fn apply(&mut self, fx: Effects) -> Effects {
        // Books of settled markets are also dropped when the delete_markets is sent; this covers markets
        // that never needed one (e.g. still waiting to be added).
        for t in &fx.removed {
            self.books.remove_market(t);
        }
        self.books.cancel_verification(&fx.snapshot_failed);
        fx
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::kalshi::subscriptions::COMMAND_TIMEOUT_MS;
    use serde_json::json;

    /// Drives a session like the I/O layer does, sending (and reporting) every command immediately.
    pub(crate) struct Harness {
        pub session: Session,
        now: u64,
        seq: u64,
    }

    impl Harness {
        /// Session on `markets`, with both subscriptions confirmed (lifecycle sid 1, orderbook sid 2)
        /// and a snapshot for each market.
        pub(crate) fn started(markets: &[&str]) -> Self {
            let mut h = Self {
                session: Session::new(Universe::filtered(vec![])),
                now: 0,
                seq: 0,
            };
            let cmds = h.session.start(markets.iter().map(|m| m.to_string()), 0);
            h.send(cmds);
            h.msg(json!({"id": 1, "type": "subscribed", "msg": {"channel": "market_lifecycle_v2", "sid": 1}}), 0);
            h.msg(json!({"id": 2, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": 2}}), 0);
            for m in markets {
                h.snapshot(m, "1.00", 0);
            }
            h
        }

        pub(crate) fn send(&mut self, cmds: impl Into<Effects>) -> Effects {
            let out = cmds.into();
            for c in &out.send {
                self.session.sent(c);
            }
            out
        }

        pub(crate) fn msg(&mut self, raw: Value, now: u64) -> Effects {
            self.now = now;
            let out = self.session.on_message(&raw, now).unwrap();
            self.send(out)
        }

        pub(crate) fn next_seq(&mut self) -> u64 {
            self.seq += 1;
            self.seq
        }

        pub(crate) fn snapshot(&mut self, market: &str, qty: &str, now: u64) -> Effects {
            let seq = self.next_seq();
            self.msg(
                json!({"type": "orderbook_snapshot", "sid": 2, "seq": seq,
                       "msg": {"market_ticker": market, "clock": 1, "yes_dollars_fp": [["0.4000", qty]]}}),
                now,
            )
        }

        pub(crate) fn delta(&mut self, market: &str, delta: &str, now: u64) -> Effects {
            let seq = self.next_seq();
            self.msg(
                json!({"type": "orderbook_delta", "sid": 2, "seq": seq,
                       "msg": {"market_ticker": market, "clock": seq, "side": "yes", "price_dollars": "0.4000", "delta_fp": delta}}),
                now,
            )
        }

        pub(crate) fn verify(&mut self, now: u64) -> Effects {
            self.now = now;
            let cmds = self.session.on_verify(now, 1);
            self.send(cmds)
        }

        pub(crate) fn tick(&mut self, now: u64) -> Effects {
            self.now = now;
            let out = self.session.on_tick(now);
            self.send(out)
        }

        pub(crate) fn stats(&self) -> &crate::kalshi::book::Stats {
            &self.session.books.stats
        }
    }

    fn tickers(out: &Effects) -> Vec<Vec<String>> {
        out.send.iter().map(|c| c.tickers().to_vec()).collect()
    }

    #[test]
    fn verification_round_trip() {
        let mut h = Harness::started(&["A", "B"]);
        let out = h.verify(1);
        assert_eq!(tickers(&out), vec![vec!["A".to_string(), "B".to_string()]]);
        assert!(h.verify(1).send.is_empty()); // already being verified
        h.snapshot("A", "1.00", 2);
        h.snapshot("B", "9.00", 2);
        assert_eq!(
            (
                h.stats().verify_ok,
                h.stats().verify_mismatch,
                h.stats().unsolicited_snapshots
            ),
            (1, 1, 0)
        );
        assert_eq!(h.session.subs.commands["get_snapshot"], 1);
    }

    #[test]
    fn verification_in_slices() {
        let mut h = Harness::started(&["A", "B", "C", "D", "E"]);
        fn slice(h: &mut Harness) -> Vec<String> {
            let cmds = h.session.on_verify(1, 2); // half the markets each time
            tickers(&h.send(cmds)).concat()
        }
        assert_eq!(slice(&mut h), ["A", "B", "C"]);
        assert_eq!(slice(&mut h), ["D", "E"]); // and A, already being verified
        h.snapshot("A", "1.00", 2);
        assert_eq!(slice(&mut h), Vec::<String>::new()); // B, C, D: still being verified
        assert_eq!(slice(&mut h), ["A"]); // E, A, B
    }

    #[test]
    fn listing_drops_closed_markets_and_their_books() {
        let mut h = Harness::started(&["A", "B"]);
        let listed: BTreeSet<String> = ["A".to_string()].into();
        assert!(h.session.on_listing(&listed, 1).is_empty());
        let cmds = h.session.on_listing(&listed, 2);
        assert_eq!(cmds[0].action(), Some(Action::DeleteMarkets));
        assert!(!h.session.books.markets.contains_key("B"));
        assert!(
            h.session
                .take_changes()
                .contains(&Change::Removed { ticker: "B".into() })
        );
    }

    #[test]
    fn skipped_markets_are_not_left_waiting_for_verification() {
        let mut h = Harness::started(&["A"]);
        let id = h.verify(1).send[0].id;
        h.msg(
            json!({"id": id, "type": "error", "msg": {"code": 1, "msg": "x"}}),
            1,
        );
        assert!(!h.session.books.markets["A"].verify_pending);
        assert_eq!(h.session.subs.command_errors["get_snapshot"], 1);
        // Backing off: no request, and not marked as pending, so a later round still verifies it.
        assert!(h.verify(2).send.is_empty());
        assert!(!h.session.books.markets["A"].verify_pending);
        assert_eq!(h.verify(1 + 10_000).send.len(), 1);
        h.snapshot("A", "1.00", 10_002);
        assert_eq!(h.stats().verify_ok, 1);
    }

    #[test]
    fn verification_timeout_cancels() {
        let mut h = Harness::started(&["A"]);
        h.verify(0);
        assert!(h.session.books.markets["A"].verify_pending);
        h.tick(COMMAND_TIMEOUT_MS);
        assert!(!h.session.books.markets["A"].verify_pending);
    }

    #[test]
    fn negative_level_resyncs_without_counting_as_verification() {
        let mut h = Harness::started(&["A"]);
        let out = h.delta("A", "-5.00", 1);
        assert_eq!(tickers(&out), vec![vec!["A".to_string()]]);
        assert!(h.session.books.markets["A"].stale);
        h.snapshot("A", "1.00", 2);
        assert!(!h.session.books.markets["A"].stale);
        assert_eq!(
            (h.stats().verify_ok, h.stats().unsolicited_snapshots),
            (0, 0)
        );
    }

    #[test]
    fn stale_markets_retried_on_tick() {
        let mut h = Harness::started(&["A"]);
        let id = h.delta("A", "-5.00", 1).send[0].id;
        h.msg(
            json!({"id": id, "type": "error", "msg": {"code": 1, "msg": "x"}}),
            1,
        );
        assert!(h.tick(2).send.is_empty()); // backing off
        assert_eq!(tickers(&h.tick(1 + 10_000)), vec![vec!["A".to_string()]]);
    }

    #[test]
    fn settled_market_is_removed() {
        let mut h = Harness::started(&["A", "B"]);
        let seq = 1; // lifecycle sid has its own seq
        let out = h.msg(
            json!({"type": "market_lifecycle_v2", "sid": 1, "seq": seq,
                               "msg": {"market_ticker": "A", "event_type": "settled"}}),
            1,
        );
        assert_eq!(out.removed, vec!["A".to_string()]);
        assert_eq!(out.send[0].action(), Some(Action::DeleteMarkets));
        assert!(!h.session.books.markets.contains_key("A"));
        assert!(h.session.books.markets.contains_key("B"));
    }

    #[test]
    fn unparseable_message_is_skipped() {
        let mut h = Harness::started(&["A"]);
        assert_eq!(h.msg(json!({"bogus": true}), 1), Effects::default());
        assert_eq!(h.stats().seq_gaps, 0);
    }

    #[test]
    fn malformed_message_resyncs_its_market() {
        let mut h = Harness::started(&["A", "B"]);
        let seq = h.next_seq();
        let out = h.msg(json!({"type": "orderbook_delta", "sid": 2, "seq": seq, "msg": {"market_ticker": "A", "price_dollars": "1.0100"}}), 1);
        assert_eq!(tickers(&out), vec![vec!["A".to_string()]]);
        assert!(h.delta("B", "1.00", 2).send.is_empty());
        assert_eq!((h.stats().malformed, h.stats().seq_gaps), (1, 0));
    }

    #[test]
    fn lost_first_snapshot_is_requested() {
        let mut h = Harness::started(&["A"]);
        let cmds = h.session.on_discovered(["B".to_string()], 0);
        let id = h.send(cmds).send[0].id;
        let seq = h.next_seq();
        h.msg(json!({"id": id, "type": "ok", "sid": 2, "seq": seq}), 0);
        // B's snapshot never arrives.
        assert!(h.tick(10_000).send.is_empty()); // might just be on its way
        assert_eq!(tickers(&h.tick(20_000)), vec![vec!["B".to_string()]]);
        h.snapshot("B", "1.00", 20_001);
        assert!(h.tick(30_000).send.is_empty());
        assert_eq!(h.stats().unsolicited_snapshots, 0);
    }

    #[test]
    fn late_messages_after_settle_ignored() {
        let mut h = Harness::started(&["A", "B"]);
        h.msg(json!({"type": "market_lifecycle_v2", "sid": 1, "seq": 1, "msg": {"market_ticker": "A", "event_type": "settled"}}), 1);
        assert!(h.delta("A", "1.00", 2).send.is_empty());
        h.snapshot("A", "3.00", 3);
        assert!(!h.session.books.markets.contains_key("A"));
        assert_eq!(
            (h.stats().after_removal, h.stats().deltas_for_unknown_market),
            (2, 0)
        );
    }
}
