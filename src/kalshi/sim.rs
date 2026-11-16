//! Randomised end-to-end tests: a simulated Kalshi exchange driven against a [`Session`], with faults
//! injected (errors, lost/late/garbled messages, disconnects).
//!
//! After every step the session's bookkeeping must be consistent and every book it believes is in sync
//! must match the exchange, and so must a copy of the in-sync books kept up to date from nothing but
//! the session's [`Change`]s (as the output stream is). After a calm period at the end, every market that should be followed must be
//! subscribed with a correct book, and nothing must still be retrying. A failure prints the seed and the
//! recent events; rerun one seed with `SIM_SEED=<n> cargo test sim -- --nocapture` (`SIM_LOG=<n>` for
//! more events, `SIM_SEEDS=<n>` to try more seeds).

use crate::kalshi::book::{Book, Change};
use crate::kalshi::discovery::{Filter, Universe};
use crate::kalshi::messages::{Action, Command, CommandKind};
use crate::kalshi::session::Session;
use crate::kalshi::subscriptions::{COMMAND_TIMEOUT_MS, Effects, LIFECYCLE_CHANNEL, ORDERBOOK_CHANNEL};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

const ERR_TRANSIENT: i64 = 18;
const ERR_BAD_MARKET: i64 = 16;
const ERR_UNKNOWN_SID: i64 = 7;
/// Listed by discovery but rejected when subscribing.
const BAD_MARKET: &str = "S-BAD";

/// splitmix64: small, deterministic, good enough for test choices.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn percent(&mut self, p: u64) -> bool {
        self.below(100) < p
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        (!items.is_empty()).then(|| &items[self.below(items.len() as u64) as usize])
    }
}

/// One side's levels: price in hundredths of a cent (as the client stores it) -> quantity in 1/100 contract.
type Levels = BTreeMap<u32, i64>;
/// [yes, no]
type TrueBook = [Levels; 2];

fn price_str(p: u32) -> String {
    format!("{}.{:04}", p / 10_000, p % 10_000)
}

fn qty_str(q: i64) -> String {
    format!("{}.{:02}", q / 100, q % 100)
}

fn levels_json(l: &Levels) -> Value {
    l.iter().map(|(&p, &q)| json!([price_str(p), qty_str(q)])).collect()
}

/// What the client should have for a book, in its display format, for comparisons.
fn render(b: &TrueBook) -> Vec<Vec<(String, String)>> {
    b.iter().map(|l| l.iter().map(|(&p, &q)| (price_str(p), qty_str(q))).collect()).collect()
}

/// A message waiting to be delivered. `seq` is assigned at delivery, like the server does at send time.
#[derive(Debug, Clone)]
struct Out {
    sid: Option<u64>,
    msg: Value,
    /// For snapshots and deltas: the market and its true book once this message is applied.
    truth: Option<(String, TrueBook)>,
    /// Command responses can be held back to arrive late.
    is_response: bool,
}

/// Server side of one connection.
#[derive(Default)]
struct Conn {
    next_sid: u64,
    lifecycle_sid: Option<u64>,
    /// The orderbook subscription's sid, once there is one (kept after the server drops it: the
    /// client's books still come from it).
    orderbook_sid: Option<u64>,
    /// Live orderbook subscriptions (at most one, as on Kalshi): sid -> markets.
    subs: BTreeMap<u64, BTreeSet<String>>,
    seq: HashMap<u64, u64>,
    outbox: VecDeque<Out>,
    held: Vec<Out>,
    /// Subscriptions with a dropped message the client hasn't had a chance to notice yet.
    undetected_drop: HashSet<u64>,
}

struct Sim {
    rng: Rng,
    now: u64,
    /// Fault rates, percent; zero during the calm phase.
    faults: bool,
    books: BTreeMap<String, TrueBook>,
    open: BTreeSet<String>,
    next_market: u32,
    clock: u64,
    conn: Conn,
    session: Session,
    /// Truth as of the last delivered (or lost) message about each market, per subscription (a market
    /// can be on two, whose messages are interleaved).
    seen: HashMap<(u64, String), TrueBook>,
    /// Markets reported settled; not re-added on reconnect (like the connection's `removed`).
    removed: BTreeSet<String>,
    /// The in-sync books, rebuilt from the session's changes alone.
    mirror: BTreeMap<String, Book>,
    log: VecDeque<String>,
    /// Events kept for the failure report (`SIM_LOG`, default 60).
    log_len: usize,
    commands_sent: u64,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let mut sim = Sim {
            rng: Rng(seed),
            now: 0,
            faults: true,
            books: BTreeMap::new(),
            open: BTreeSet::new(),
            next_market: 0,
            clock: 0,
            conn: Conn::default(),
            session: Session::new(Universe::filtered(vec![Filter::Series("S".into())])),
            seen: HashMap::new(),
            removed: BTreeSet::new(),
            mirror: BTreeMap::new(),
            log: VecDeque::new(),
            log_len: std::env::var("SIM_LOG").ok().and_then(|s| s.parse().ok()).unwrap_or(60),
            commands_sent: 0,
        };
        for _ in 0..12 {
            sim.create_market(false);
        }
        sim.connect();
        sim
    }

    fn note(&mut self, s: String) {
        if self.log.len() >= self.log_len {
            self.log.pop_front();
        }
        self.log.push_back(format!("[{}] {s}", self.now));
    }

    // ---- exchange ----

    fn create_market(&mut self, announce: bool) {
        self.next_market += 1;
        // Some markets are outside the followed series.
        let series = if self.rng.percent(25) { "T" } else { "S" };
        let ticker = format!("{series}-{}", self.next_market);
        let mut book = TrueBook::default();
        for side in &mut book {
            for _ in 0..self.rng.below(4) {
                side.insert(100 * (1 + self.rng.below(99) as u32), 100 * (1 + self.rng.below(50) as i64));
            }
        }
        self.books.insert(ticker.clone(), book);
        self.open.insert(ticker.clone());
        if announce {
            self.note(format!("exchange: created {ticker}"));
            self.lifecycle(json!({"type": "market_lifecycle_v2", "msg": {"market_ticker": ticker, "event_type": "created"}}));
        }
    }

    fn lifecycle(&mut self, msg: Value) {
        if let Some(sid) = self.conn.lifecycle_sid {
            self.conn.outbox.push_back(Out { sid: Some(sid), msg, truth: None, is_response: false });
        }
    }

    /// Change one level of an open market and send the delta to every subscription that has it.
    fn trade(&mut self) {
        let open: Vec<String> = self.open.iter().cloned().collect();
        let Some(ticker) = self.rng.pick(&open).cloned() else { return };
        let side = self.rng.below(2) as usize;
        let price = 100 * (1 + self.rng.below(99) as u32);
        let book = self.books.get_mut(&ticker).unwrap();
        let current = book[side].get(&price).copied().unwrap_or(0);
        let delta = if current > 0 && self.rng.percent(50) {
            -(1 + self.rng.below(current as u64) as i64) // never below zero
        } else {
            1 + self.rng.below(5000) as i64
        };
        let new = current + delta;
        if new == 0 {
            book[side].remove(&price);
        } else {
            book[side].insert(price, new);
        }
        self.clock += 1;
        let book = book.clone();
        let side_name = ["yes", "no"][side];
        for (&sid, markets) in &self.conn.subs {
            if markets.contains(&ticker) {
                let msg = json!({"type": "orderbook_delta", "msg": {"market_ticker": ticker, "clock": self.clock,
                    "side": side_name, "price_dollars": price_str(price), "delta_fp": qty_str(delta)}});
                self.conn.outbox.push_back(Out { sid: Some(sid), msg, truth: Some((ticker.clone(), book.clone())), is_response: false });
            }
        }
    }

    fn settle(&mut self) {
        let open: Vec<String> = self.open.iter().cloned().collect();
        let Some(ticker) = self.rng.pick(&open).cloned() else { return };
        self.note(format!("exchange: settled {ticker}"));
        self.open.remove(&ticker);
        self.lifecycle(json!({"type": "market_lifecycle_v2", "msg": {"market_ticker": ticker, "event_type": "settled"}}));
        // Kalshi clears the book when a market closes.
        self.books.insert(ticker.clone(), TrueBook::default());
        let subs: Vec<u64> = self.conn.subs.iter().filter(|(_, m)| m.contains(&ticker)).map(|(&s, _)| s).collect();
        for sid in subs {
            self.snapshot(sid, &ticker);
        }
    }

    fn snapshot(&mut self, sid: u64, ticker: &str) {
        let book = self.books[ticker].clone();
        self.clock += 1;
        let mut body = json!({"market_ticker": ticker, "clock": self.clock});
        // Kalshi leaves out empty sides.
        if !book[0].is_empty() {
            body["yes_dollars_fp"] = levels_json(&book[0]);
        }
        if !book[1].is_empty() {
            body["no_dollars_fp"] = levels_json(&book[1]);
        }
        let msg = json!({"type": "orderbook_snapshot", "msg": body});
        self.conn.outbox.push_back(Out { sid: Some(sid), msg, truth: Some((ticker.to_string(), book)), is_response: false });
    }

    /// The REST listing: open markets in the series, plus one that can't be subscribed to.
    fn discover(&mut self) -> Effects {
        let mut found: Vec<String> = self.open.iter().filter(|t| t.starts_with("S-")).cloned().collect();
        found.push(BAD_MARKET.to_string());
        found.retain(|t| !self.removed.contains(t));
        let listed: BTreeSet<String> = found.iter().cloned().collect();
        let mut send = self.session.on_discovered(found, self.now);
        send.extend(self.session.on_listing(&listed, self.now));
        send.into()
    }

    // ---- server protocol ----

    fn respond(&mut self, sid: Option<u64>, msg: Value) {
        self.conn.outbox.push_back(Out { sid, msg, truth: None, is_response: true });
    }

    /// The server drops an orderbook subscription, as Kalshi does when it can't keep up with one.
    fn drop_subscription(&mut self) {
        let sids: Vec<u64> = self.conn.subs.keys().copied().collect();
        let Some(&sid) = self.rng.pick(&sids) else { return };
        self.note(format!("server: drops sid {sid}"));
        self.conn.subs.remove(&sid);
        self.respond(Some(sid), json!({"type": "error", "msg": {"code": 25, "msg": "Subscription buffer overflow"}}));
    }

    fn error(&mut self, id: u64, code: i64) {
        self.respond(None, json!({"id": id, "type": "error", "msg": {"code": code, "msg": format!("error {code}")}}));
    }

    fn handle_command(&mut self, cmd: &Command) {
        self.commands_sent += 1;
        self.note(format!("client sent {}", cmd.to_json()));
        if self.faults && self.rng.percent(3) {
            self.note("  ...lost".into());
            return;
        }
        if self.faults && self.rng.percent(5) {
            self.error(cmd.id, ERR_TRANSIENT);
            return;
        }
        let mut responses_before = self.conn.outbox.len();
        let existing_sid = self.conn.subs.keys().next().copied();
        match &cmd.kind {
            CommandKind::Subscribe { channels, market_tickers } => {
                if channels.iter().any(|c| c == LIFECYCLE_CHANNEL) {
                    let sid = self.new_sid();
                    self.conn.lifecycle_sid = Some(sid);
                    self.respond(None, json!({"id": cmd.id, "type": "subscribed", "msg": {"channel": LIFECYCLE_CHANNEL, "sid": sid}}));
                } else if let Some(code) = self.reject(market_tickers) {
                    self.error(cmd.id, code);
                } else if let Some(sid) = existing_sid {
                    // Like Kalshi: one orderbook subscription per connection, so another subscribe is
                    // merged into it, with a plain `ok`.
                    let current = &self.conn.subs[&sid];
                    let new: Vec<String> = market_tickers.iter().filter(|t| !current.contains(*t)).cloned().collect();
                    self.conn.subs.get_mut(&sid).unwrap().extend(new.iter().cloned());
                    self.respond(Some(sid), json!({"id": cmd.id, "type": "ok", "msg": {}}));
                    for t in &new {
                        self.snapshot(sid, t);
                    }
                } else {
                    let sid = self.new_sid();
                    self.conn.orderbook_sid = Some(sid);
                    self.conn.subs.insert(sid, market_tickers.iter().cloned().collect());
                    self.respond(None, json!({"id": cmd.id, "type": "subscribed", "msg": {"channel": ORDERBOOK_CHANNEL, "sid": sid}}));
                    for t in market_tickers {
                        self.snapshot(sid, t);
                    }
                }
            }
            CommandKind::UpdateSubscription { sid, market_tickers, action } => {
                let sid = *sid;
                let Some(current) = self.conn.subs.get(&sid).cloned() else {
                    self.error(cmd.id, ERR_UNKNOWN_SID);
                    return;
                };
                let ok = json!({"id": cmd.id, "type": "ok", "msg": {}});
                match action {
                    Action::AddMarkets => {
                        let new: Vec<String> = market_tickers.iter().filter(|t| !current.contains(*t)).cloned().collect();
                        if let Some(code) = self.reject(&new) {
                            self.error(cmd.id, code);
                        } else {
                            self.conn.subs.get_mut(&sid).unwrap().extend(new.iter().cloned());
                            self.respond(Some(sid), ok);
                            for t in &new {
                                self.snapshot(sid, t);
                            }
                        }
                    }
                    Action::DeleteMarkets => {
                        let subs = self.conn.subs.get_mut(&sid).unwrap();
                        for t in market_tickers {
                            subs.remove(t);
                        }
                        self.respond(Some(sid), ok);
                    }
                    Action::GetSnapshot => {
                        if market_tickers.iter().any(|t| !current.contains(t)) {
                            self.error(cmd.id, ERR_BAD_MARKET);
                        } else {
                            // Not every get_snapshot gets an `ok`.
                            if self.rng.percent(50) {
                                self.respond(Some(sid), ok);
                            } else {
                                responses_before = usize::MAX;
                            }
                            for t in market_tickers {
                                self.snapshot(sid, t);
                            }
                        }
                    }
                }
            }
        }
        // Hold the response (only) back, to arrive late.
        if self.faults && responses_before < self.conn.outbox.len() && self.rng.percent(5) {
            let r = self.conn.outbox.remove(responses_before).unwrap();
            assert!(r.is_response);
            self.note(format!("  ...response held: {}", r.msg));
            self.conn.held.push(r);
        }
    }

    /// Why subscribing to `tickers` would fail.
    fn reject(&self, tickers: &[String]) -> Option<i64> {
        tickers.iter().any(|t| !self.open.contains(t)).then_some(ERR_BAD_MARKET)
    }

    fn new_sid(&mut self) -> u64 {
        self.conn.next_sid += 1;
        self.conn.next_sid
    }

    // ---- client side ----

    fn connect(&mut self) {
        self.note("client: connect".into());
        self.conn = Conn::default();
        self.seen.clear();
        // The old connection's books are gone (the collector reports a disconnect).
        self.mirror.clear();
        self.session = Session::new(Universe::filtered(vec![Filter::Series("S".into())]));
        let start = self.session.start(Vec::<String>::new(), self.now);
        self.send(start.into());
        let out = self.discover();
        self.process(out);
    }

    fn send(&mut self, out: Effects) {
        for c in &out.send {
            self.handle_command(c);
            self.session.sent(c);
        }
    }

    fn process(&mut self, out: Effects) {
        if out.reconnect.is_some() {
            self.connect();
            return;
        }
        self.removed.extend(out.removed.iter().cloned());
        let rediscover = out.rediscover;
        self.send(out);
        if rediscover {
            let out = self.discover();
            self.process(out);
        }
    }

    /// Deliver (or lose, or garble) the next message.
    fn deliver(&mut self) -> bool {
        let Some(mut out) = self.conn.outbox.pop_front() else { return false };
        if let Some(sid) = out.sid {
            let seq = self.conn.seq.entry(sid).or_default();
            *seq += 1;
            out.msg["sid"] = json!(sid);
            out.msg["seq"] = json!(*seq);
        }
        if let (Some((t, book)), Some(sid)) = (&out.truth, out.sid) {
            self.seen.insert((sid, t.clone()), book.clone());
        }
        let has_seq = out.sid.is_some() && !out.is_response;
        if self.faults && has_seq && self.rng.percent(2) {
            self.note(format!("lost: {}", out.msg));
            self.conn.undetected_drop.insert(out.sid.unwrap());
            return true;
        }
        if self.faults && has_seq && self.rng.percent(2) {
            self.note(format!("garbled: {}", out.msg));
            let ticker = out.msg["msg"]["market_ticker"].clone();
            out.msg["msg"] = if self.rng.percent(50) { json!({"market_ticker": ticker, "price_dollars": "9.0000"}) } else { json!({}) };
        }
        if let Some(sid) = out.sid {
            self.conn.undetected_drop.remove(&sid);
        }
        self.note(format!("deliver: {}", out.msg));
        let result = self.session.on_message(&out.msg, self.now).expect("session error");
        self.process(result);
        true
    }

    fn tick(&mut self, ms: u64) {
        self.now += ms;
        let out = self.session.on_tick(self.now);
        self.process(out);
    }

    fn step(&mut self) {
        match self.rng.below(100) {
            0..40 => {
                for _ in 0..1 + self.rng.below(6) {
                    self.deliver();
                }
            }
            40..65 => self.trade(),
            65..75 => {
                let ms = 1 + self.rng.below(15_000);
                self.tick(ms);
            }
            75..80 => {
                let cmds = self.session.on_verify(self.now, 3);
                self.send(cmds.into());
            }
            80..83 => self.create_market(true),
            83..85 => self.settle(),
            85..89 => self.lifecycle(json!({"type": "event_lifecycle", "msg": {"event_ticker": "E"}})),
            89..92 => {
                let held = std::mem::take(&mut self.conn.held);
                self.conn.outbox.extend(held);
            }
            92..93 if self.faults && self.rng.percent(30) => self.connect(),
            93..94 if self.faults && self.rng.percent(20) => self.drop_subscription(),
            _ => {}
        }
    }

    // ---- checks ----

    /// Every book the client thinks is in sync matches the exchange (as of the messages delivered).
    fn check_books(&self) {
        self.session.subs.check_invariants();
        for (t, m) in &self.session.books.markets {
            if m.stale {
                continue;
            }
            let Some(sid) = self.conn.orderbook_sid else { panic!("{t}: client has a book, but there's no orderbook subscription") };
            if self.conn.undetected_drop.contains(&sid) {
                continue;
            }
            let Some(truth) = self.seen.get(&(sid, t.clone())) else {
                panic!("{t}: client has a book but no message about it was delivered on sid {sid}")
            };
            let client: Vec<Vec<(String, String)>> = [&m.book.yes, &m.book.no]
                .iter()
                .map(|l| l.iter().map(|(p, q)| (p.to_string(), q.to_string())).collect())
                .collect();
            assert_eq!(client, render(truth), "{t}: book differs from exchange");
        }
    }

    /// Apply the session's changes to the mirror, and check it matches the in-sync books exactly.
    fn check_mirror(&mut self) {
        // Books out of sync again already (several messages are delivered per step): a Stale or Removed
        // follows, and level changes before it can't be applied.
        let mut gone_again = HashSet::new();
        for change in self.session.take_changes() {
            match change {
                Change::Snapshot { ticker, .. } => match self.session.synced_book(&ticker) {
                    Some(book) => {
                        gone_again.remove(&ticker);
                        self.mirror.insert(ticker, book.clone());
                    }
                    None => {
                        self.mirror.remove(&ticker);
                        gone_again.insert(ticker);
                    }
                },
                Change::Level { ticker, .. } if gone_again.contains(&ticker) => {}
                Change::Level { ticker, side, price, qty, .. } => {
                    let book = self.mirror.get_mut(&ticker).unwrap_or_else(|| panic!("{ticker}: level change without a book"));
                    let levels = match side {
                        crate::kalshi::messages::Side::Yes => &mut book.yes,
                        crate::kalshi::messages::Side::No => &mut book.no,
                    };
                    if qty == crate::kalshi::fixed::Qty::ZERO {
                        levels.remove(&price);
                    } else {
                        levels.insert(price, qty);
                    }
                }
                Change::Stale { ticker } | Change::Removed { ticker } => {
                    gone_again.remove(&ticker);
                    self.mirror.remove(&ticker);
                }
            }
        }
        let synced: BTreeMap<&String, (&BTreeMap<_, _>, &BTreeMap<_, _>)> = self
            .session
            .books
            .markets
            .iter()
            .filter(|(_, m)| !m.stale)
            .map(|(t, m)| (t, (&m.book.yes, &m.book.no)))
            .collect();
        let mirrored: BTreeMap<&String, (&BTreeMap<_, _>, &BTreeMap<_, _>)> =
            self.mirror.iter().map(|(t, b)| (t, (&b.yes, &b.no))).collect();
        assert_eq!(mirrored, synced, "books rebuilt from changes differ from the session's");
    }

    /// No faults: everything should settle down.
    fn calm_down(&mut self) {
        self.faults = false;
        let held = std::mem::take(&mut self.conn.held);
        self.conn.outbox.extend(held);
        for round in 0..120 {
            // Occasional lifecycle traffic and verification, as on the real exchange, reveal earlier losses.
            self.lifecycle(json!({"type": "event_lifecycle", "msg": {"event_ticker": "E"}}));
            if round % 5 == 0 {
                let cmds = self.session.on_verify(self.now, 1);
                self.send(cmds.into());
            }
            while self.deliver() {}
            self.check_books();
            self.check_mirror();
            self.tick(60_000);
            while self.deliver() {}
            self.check_books();
            self.check_mirror();
        }
        // Quiet: nothing left to send except the (hourly) retry of the bad market.
        let before = self.commands_sent;
        for _ in 0..3 {
            self.tick(COMMAND_TIMEOUT_MS);
            while self.deliver() {}
        }
        assert!(self.commands_sent - before <= 1, "still sending commands after calming down");

        let subs = &self.session.subs;
        let books = &self.session.books.markets;
        for t in self.open.iter().filter(|t| t.starts_with("S-") && !self.removed.contains(*t)) {
            assert!(subs.is_subscribed(t), "{t}: open and in the series, but not subscribed");
            let m = books.get(t).unwrap_or_else(|| panic!("{t}: subscribed but no book"));
            assert!(!m.stale, "{t}: still stale");
        }
        assert!(!subs.is_subscribed(BAD_MARKET));
        let wanted: BTreeSet<&String> = subs.wanted().collect();
        for t in books.keys() {
            assert!(wanted.contains(t), "{t}: book kept for a market we don't follow");
        }
        // Every book matches the exchange now that everything's delivered.
        for (t, m) in books {
            let client: Vec<Vec<(String, String)>> = [&m.book.yes, &m.book.no]
                .iter()
                .map(|l| l.iter().map(|(p, q)| (p.to_string(), q.to_string())).collect())
                .collect();
            assert_eq!(client, render(&self.books[t]), "{t}: final book differs from exchange");
        }
    }
}

fn run(seed: u64, steps: usize) {
    let mut sim = Sim::new(seed);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for _ in 0..steps {
            sim.step();
            sim.check_books();
            sim.check_mirror();
        }
        sim.calm_down();
    }));
    if let Err(e) = result {
        eprintln!("---- seed {seed} failed; last events:");
        for l in &sim.log {
            eprintln!("{l}");
        }
        std::panic::resume_unwind(e);
    }
}

#[test]
fn simulated_exchange() {
    if let Ok(seed) = std::env::var("SIM_SEED") {
        run(seed.parse().expect("SIM_SEED must be a number"), 3000);
        return;
    }
    let seeds: u64 = std::env::var("SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
    for seed in 0..seeds {
        run(seed, 3000);
    }
}
