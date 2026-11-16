//! Subscription bookkeeping: which markets we want, the commands we've sent, and their responses.
//!
//! Pure logic with no I/O. Methods return the commands for the caller to send, plus [`Effects`]
//! describing what the caller should do to its books.
//!
//! Kalshi gives each connection one `orderbook_delta` subscription, so every market goes on it: the
//! first batch with the `subscribe`, the rest with `add_markets` once it's confirmed. Commands carry at
//! most [`MAX_BATCH`] markets. When a batch is rejected it's split in half and retried, so one bad
//! market can't take down the rest; markets rejected on their own, and failed snapshot requests, are
//! retried with backoff.

use crate::kalshi::discovery::Universe;
use crate::kalshi::messages::{Action, Body, Command, Frame, Lifecycle};
use anyhow::{Result, bail};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tracing::{debug, info, warn};

pub const ORDERBOOK_CHANNEL: &str = "orderbook_delta";
pub const LIFECYCLE_CHANNEL: &str = "market_lifecycle_v2";
/// How long to wait for a command response, or for a requested snapshot, before giving up on it.
pub const COMMAND_TIMEOUT_MS: u64 = 30_000;
/// How long to remember timed-out adds, in case their response arrives late.
const LATE_RESPONSE_MS: u64 = 10 * COMMAND_TIMEOUT_MS;
/// Most markets sent in one command.
pub const MAX_BATCH: usize = 100;

/// "Adding markets would exceed the per-subscription market limit." With one subscription per
/// connection, that's the connection's limit.
const ERR_MARKET_LIMIT: i64 = 26;
/// "Unknown subscription ID": the subscription a command referred to no longer exists.
const ERR_UNKNOWN_SID: i64 = 7;
/// Server timeout, outbound buffer exceeded, command rate limit: worth retrying as-is, later.
const TRANSIENT_ERRORS: [i64; 3] = [18, 25, 27];

/// Retry delays: base doubling per consecutive failure, up to cap.
const TRANSIENT_BACKOFF: (u64, u64) = (5_000, 300_000);
const REJECTED_BACKOFF: (u64, u64) = (60_000, 3_600_000);
const SNAPSHOT_BACKOFF: (u64, u64) = (10_000, 300_000);

fn backoff_ms((base, cap): (u64, u64), failures: u32) -> u64 {
    base.saturating_mul(1 << failures.saturating_sub(1).min(20)).min(cap)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubState {
    /// `subscribe` sent with this command id.
    Subscribing(u64),
    /// Confirmed, with this sid.
    Active(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    /// In a subscribe/add command that hasn't been answered.
    Adding { cmd: u64 },
    /// Waiting for the orderbook subscription to be confirmed.
    Queued,
    Subscribed,
    /// Adding failed; try again at this time.
    Backoff { at: u64 },
}

#[derive(Debug, Clone)]
enum Pending {
    SubscribeLifecycle,
    SubscribeOrderbook { tickers: Vec<String> },
    Add { tickers: Vec<String> },
    Delete,
    Snapshot,
}

impl Pending {
    /// The command's name, as in [`Command::name`].
    fn name(&self) -> &'static str {
        match self {
            Pending::SubscribeLifecycle | Pending::SubscribeOrderbook { .. } => "subscribe",
            Pending::Add { .. } => "add_markets",
            Pending::Delete => "delete_markets",
            Pending::Snapshot => "get_snapshot",
        }
    }
}

/// What the caller should do after a call into [`Subscriptions`] (or a
/// [`Session`](crate::kalshi::session::Session), which has already dealt with the effects on its books).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Effects {
    /// Commands to send, in order (then report each one via `Session::sent`).
    pub send: Vec<Command>,
    /// Markets that were unsubscribed (settled): drop their books, and don't resubscribe to them
    /// after a reconnect.
    pub removed: Vec<String>,
    /// Markets whose requested snapshot isn't coming (error or timeout). Cancel pending verifications;
    /// stale markets are retried via [`Subscriptions::snapshot_request`] once their backoff allows.
    pub snapshot_failed: Vec<String>,
    /// Lifecycle notifications may have been missed: re-run REST discovery.
    pub rediscover: bool,
    /// The connection is unusable (a subscription was lost or never confirmed): reconnect, for
    /// this reason.
    pub reconnect: Option<String>,
}

impl From<Vec<Command>> for Effects {
    fn from(send: Vec<Command>) -> Self {
        Self { send, ..Default::default() }
    }
}

#[derive(Debug)]
pub struct Subscriptions {
    /// Commands sent, by name (subscribe, add_markets, delete_markets, get_snapshot).
    pub commands: BTreeMap<&'static str, u64>,
    /// Error responses, by the name of the command they answered ("unknown" if it isn't known, e.g.
    /// it had already timed out).
    pub command_errors: BTreeMap<&'static str, u64>,
    universe: Universe,
    next_id: u64,
    /// Commands awaiting a response: id -> (command, sent time).
    pending: HashMap<u64, (Pending, u64)>,
    /// Adds we gave up waiting for, in case the response turns up late: id -> (tickers, time out).
    timed_out_adds: HashMap<u64, (Vec<String>, u64)>,
    lifecycle: Option<SubState>,
    lifecycle_seq: Option<u64>,
    /// The orderbook subscription; None before the first subscribe, or after one failed.
    orderbook: Option<SubState>,
    /// Markets in the subscribe that were removed before it was confirmed: unsubscribed once it is.
    queued_deletes: BTreeSet<String>,
    /// Every market we want, and where it stands.
    markets: BTreeMap<String, Status>,
    /// Consecutive failed attempts to add a market.
    add_failures: HashMap<String, u32>,
    /// Markets with an outstanding snapshot request: ticker -> (command id, sent time).
    snapshot_inflight: HashMap<String, (u64, u64)>,
    /// Markets whose snapshot requests failed: ticker -> (consecutive failures, next attempt time).
    snapshot_backoff: HashMap<String, (u32, u64)>,
    /// Wanted markets missing from the last complete listing (see [`Subscriptions::on_listing`]).
    unlisted: BTreeSet<String>,
}

impl Subscriptions {
    pub fn new(universe: Universe) -> Self {
        Self {
            commands: BTreeMap::new(),
            command_errors: BTreeMap::new(),
            universe,
            next_id: 1,
            pending: HashMap::new(),
            timed_out_adds: HashMap::new(),
            lifecycle: None,
            lifecycle_seq: None,
            orderbook: None,
            queued_deletes: BTreeSet::new(),
            markets: BTreeMap::new(),
            add_failures: HashMap::new(),
            snapshot_inflight: HashMap::new(),
            snapshot_backoff: HashMap::new(),
            unlisted: BTreeSet::new(),
        }
    }

    /// A command went out (for stats).
    pub fn sent(&mut self, cmd: &Command) {
        *self.commands.entry(cmd.name()).or_default() += 1;
    }

    /// (markets subscribed, markets wanted).
    pub fn counts(&self) -> (usize, usize) {
        (self.subscribed().count(), self.markets.len())
    }

    /// Markets confirmed subscribed.
    pub fn subscribed(&self) -> impl Iterator<Item = &String> {
        self.markets.iter().filter(|(_, s)| **s == Status::Subscribed).map(|(t, _)| t)
    }

    /// Initial commands: the lifecycle subscription (used to follow new and settling markets), and
    /// the orderbook subscription for `tickers`.
    pub fn start(&mut self, tickers: impl IntoIterator<Item = String>, now_ms: u64) -> Vec<Command> {
        let cmd = self.command(Pending::SubscribeLifecycle, now_ms, |id| Command::subscribe(id, LIFECYCLE_CHANNEL, vec![]));
        self.lifecycle = Some(SubState::Subscribing(cmd.id));
        let mut send = vec![cmd];
        send.extend(self.add_markets(tickers, now_ms));
        send
    }

    /// Subscribe to any of `tickers` we aren't already following (or waiting to retry).
    pub fn add_markets(&mut self, tickers: impl IntoIterator<Item = String>, now_ms: u64) -> Vec<Command> {
        let new: Vec<String> = tickers.into_iter().filter(|t| !self.markets.contains_key(t)).collect::<BTreeSet<_>>().into_iter().collect();
        if new.is_empty() {
            return vec![];
        }
        debug!("adding {} market(s): {}", new.len(), summarize(&new));
        self.place_all(new, now_ms)
    }

    /// Unsubscribe from a market. Returns the commands to send (empty if we weren't following it).
    pub fn remove_market(&mut self, ticker: &str, now_ms: u64) -> Vec<Command> {
        let Some(status) = self.markets.remove(ticker) else { return vec![] };
        debug!("removing market {ticker}");
        self.add_failures.remove(ticker);
        self.snapshot_inflight.remove(ticker);
        self.snapshot_backoff.remove(ticker);
        match (status, self.orderbook) {
            (Status::Adding { .. } | Status::Subscribed, Some(SubState::Active(sid))) => {
                let tickers = vec![ticker.to_string()];
                vec![self.command(Pending::Delete, now_ms, |id| Command::update(id, sid, Action::DeleteMarkets, tickers))]
            }
            // In the subscribe.
            (Status::Adding { .. }, Some(SubState::Subscribing(_))) => {
                self.queued_deletes.insert(ticker.to_string());
                vec![]
            }
            _ => vec![],
        }
    }

    /// A complete listing of the open markets (at least those in this connection's universe). Markets
    /// missing from it and from the previous one are no longer open, and their lifecycle notification
    /// was missed: unsubscribe from them (listed in [`Effects::removed`]). One listing isn't enough,
    /// since a market that opens while a listing is being taken can be missing from it.
    pub fn on_listing(&mut self, listed: &BTreeSet<String>, now_ms: u64) -> Effects {
        let mut unlisted: BTreeSet<String> = self.markets.keys().filter(|t| !listed.contains(*t)).cloned().collect();
        let gone: Vec<String> = unlisted.intersection(&self.unlisted).cloned().collect();
        let mut fx = Effects::default();
        if !gone.is_empty() {
            info!("{} no longer listed; unsubscribing", summarize(&gone));
        }
        for t in &gone {
            unlisted.remove(t);
            fx.send.extend(self.remove_market(t, now_ms));
        }
        self.unlisted = unlisted;
        fx.removed = gone;
        fx
    }

    /// Request fresh snapshots. Skips markets that aren't confirmed subscribed, already have a request
    /// out, or are backing off after failed requests.
    pub fn snapshot_request(&mut self, tickers: &[String], now_ms: u64) -> Vec<Command> {
        let Some(SubState::Active(sid)) = self.orderbook else { return vec![] };
        let wanted: Vec<String> = tickers
            .iter()
            .filter(|t| {
                self.markets.get(*t) == Some(&Status::Subscribed)
                    && !self.snapshot_inflight.contains_key(*t)
                    && !self.snapshot_backoff.get(*t).is_some_and(|&(_, at)| at > now_ms)
            })
            .cloned()
            .collect();
        let mut send = Vec::new();
        for batch in wanted.chunks(MAX_BATCH) {
            let cmd = self.command(Pending::Snapshot, now_ms, |id| Command::update(id, sid, Action::GetSnapshot, batch.to_vec()));
            for t in batch {
                self.snapshot_inflight.insert(t.clone(), (cmd.id, now_ms));
            }
            send.push(cmd);
        }
        send
    }

    fn is_lifecycle_sid(&self, sid: Option<u64>) -> bool {
        matches!((self.lifecycle, sid), (Some(SubState::Active(a)), Some(b)) if a == b)
    }

    fn is_orderbook_sid(&self, sid: Option<u64>) -> bool {
        matches!((self.orderbook, sid), (Some(SubState::Active(a)), Some(b)) if a == b)
    }

    /// Process a server message. Errors are fatal (a subscription we can't do without was rejected).
    pub fn handle(&mut self, frame: &Frame, now_ms: u64) -> Result<Effects> {
        let mut fx = Effects::default();
        // Every message on the lifecycle subscription (market and event lifecycle) carries its next seq.
        if self.is_lifecycle_sid(frame.sid)
            && let Some(seq) = frame.seq
            && let prev = self.lifecycle_seq.replace(seq).unwrap_or(0) // starts at 1
            && seq != prev + 1
        {
            warn!("lifecycle seq gap {prev} -> {seq}; will rediscover markets");
            fx.rediscover = true;
        }
        // Responses (subscribed / ok / error) settle the command they answer.
        let answered = match frame.body {
            Body::Subscribed { .. } | Body::Ok {} | Body::Error { .. } => {
                frame.id.and_then(|id| self.pending.remove(&id).map(|(c, _)| (id, c)))
            }
            _ => None,
        };
        match &frame.body {
            Body::OrderbookSnapshot { msg } => {
                self.snapshot_inflight.remove(&msg.market_ticker);
                self.snapshot_backoff.remove(&msg.market_ticker);
            }
            Body::Subscribed { msg } => match answered {
                Some((_, Pending::SubscribeLifecycle)) => {
                    info!("lifecycle subscription active: sid {}", msg.sid);
                    self.lifecycle = Some(SubState::Active(msg.sid));
                }
                Some((id, Pending::SubscribeOrderbook { tickers })) => {
                    info!("orderbook subscription active: sid {}", msg.sid);
                    fx.send = self.orderbook_subscribed(id, msg.sid, &tickers, now_ms);
                }
                other => warn!("unexpected subscribed message {msg:?} (command {other:?})"),
            },
            Body::Ok {} => match (answered, frame.id) {
                (Some((id, Pending::Add { tickers })), _) => self.confirm(id, &tickers),
                // Kalshi answers a subscribe this way when it merges it into a subscription the
                // connection already has. We only subscribe when there isn't one, so something's off.
                (Some((_, Pending::SubscribeOrderbook { .. })), _) => {
                    fx.reconnect = Some(format!("orderbook subscribe answered with ok (sid {:?}), not subscribed", frame.sid));
                }
                (None, Some(id)) => self.late_add_ok(id),
                _ => {}
            },
            Body::Error { msg } => {
                let what = format!("error {}: {}", msg.code, msg.msg);
                let name = match &answered {
                    Some((_, p)) => p.name(),
                    None if frame.id.is_some_and(|id| self.timed_out_adds.contains_key(&id)) => "add_markets",
                    None => "unknown",
                };
                *self.command_errors.entry(name).or_default() += 1;
                // The server drops a subscription it can't keep up with ("Subscription buffer overflow"),
                // reporting it with an error on the subscription. Its markets get no more updates, and
                // we'd only find out from commands on it failing, so start over on a new connection.
                let lost = match (frame.id, frame.sid) {
                    (None, Some(_)) => self.is_lifecycle_sid(frame.sid) || self.is_orderbook_sid(frame.sid),
                    _ => msg.code == ERR_UNKNOWN_SID,
                };
                if lost {
                    fx.reconnect = Some(format!("subscription lost ({what}, sid {:?})", frame.sid));
                    return Ok(fx);
                }
                match answered {
                    Some((_, Pending::SubscribeLifecycle)) if TRANSIENT_ERRORS.contains(&msg.code) => {
                        fx.reconnect = Some(format!("subscribing to {LIFECYCLE_CHANNEL} failed ({what})"));
                    }
                    Some((_, Pending::SubscribeLifecycle)) => bail!("subscribing to {LIFECYCLE_CHANNEL} failed: {what}"),
                    Some((id, Pending::SubscribeOrderbook { tickers })) => {
                        fx.send = self.add_failed(id, tickers, true, Some(msg.code), &what, now_ms);
                    }
                    Some((id, Pending::Add { tickers })) => {
                        fx.send = self.add_failed(id, tickers, false, Some(msg.code), &what, now_ms);
                    }
                    Some((_, Pending::Delete)) => warn!("unsubscribing failed: {what}"),
                    Some((id, Pending::Snapshot)) => {
                        let failed = self.take_inflight(|&(cmd_id, _)| cmd_id == id);
                        warn!("snapshot request for {} failed: {what}", summarize(&failed));
                        self.snapshot_failed(&failed, now_ms);
                        fx.snapshot_failed = failed;
                    }
                    None if frame.id.is_some_and(|id| self.timed_out_adds.remove(&id).is_some()) => {
                        info!("late error for a timed-out add: {what}");
                    }
                    None => warn!("server error for unknown command {:?}: {what}", frame.id),
                }
            }
            Body::MarketLifecycleV2 { msg } if self.is_lifecycle_sid(frame.sid) => self.on_lifecycle(msg, now_ms, &mut fx),
            // A market lifecycle notification we couldn't read is as good as missed.
            Body::Malformed { kind, .. } if kind == LIFECYCLE_CHANNEL && self.is_lifecycle_sid(frame.sid) => {
                fx.rediscover = true;
            }
            _ => {}
        }
        Ok(fx)
    }

    /// Periodic housekeeping: give up on unanswered commands and snapshot requests, and retry markets
    /// whose backoff has elapsed.
    pub fn expire(&mut self, now_ms: u64) -> Effects {
        let mut fx = Effects::default();
        let mut expired: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, (_, sent))| now_ms.saturating_sub(*sent) >= COMMAND_TIMEOUT_MS)
            .map(|(&id, _)| id)
            .collect();
        expired.sort();
        for id in expired {
            match self.pending.remove(&id).map(|(c, _)| c) {
                Some(Pending::SubscribeLifecycle | Pending::SubscribeOrderbook { .. }) => {
                    fx.reconnect = Some(format!("no response to subscribe after {COMMAND_TIMEOUT_MS} ms"));
                }
                Some(Pending::Add { tickers }) => {
                    self.timed_out_adds.insert(id, (tickers.clone(), now_ms));
                    fx.send.extend(self.add_failed(id, tickers, false, None, "timed out", now_ms));
                }
                Some(Pending::Delete) => warn!("unsubscribe timed out"),
                // Not every get_snapshot gets an `ok`; the snapshots themselves are tracked below.
                Some(Pending::Snapshot) | None => {}
            }
        }
        self.timed_out_adds.retain(|_, (_, at)| now_ms.saturating_sub(*at) < LATE_RESPONSE_MS);
        fx.snapshot_failed = self.take_inflight(|&(_, sent)| now_ms.saturating_sub(sent) >= COMMAND_TIMEOUT_MS);
        if !fx.snapshot_failed.is_empty() {
            warn!("requested snapshots never arrived for {}", summarize(&fx.snapshot_failed));
            self.snapshot_failed(&fx.snapshot_failed.clone(), now_ms);
        }
        let due: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, s)| matches!(s, Status::Backoff { at } if *at <= now_ms))
            .map(|(t, _)| t.clone())
            .collect();
        if !due.is_empty() {
            debug!("retrying {} market(s): {}", due.len(), summarize(&due));
            fx.send.extend(self.place_all(due, now_ms));
        }
        fx
    }

    /// Put markets on the orderbook subscription, in batches: added if it's active, queued if it's
    /// being subscribed to, or as the subscribe if there isn't one.
    fn place_all(&mut self, tickers: Vec<String>, now_ms: u64) -> Vec<Command> {
        tickers.chunks(MAX_BATCH).filter_map(|b| self.place(b.to_vec(), now_ms)).collect()
    }

    /// Put one batch (≤ MAX_BATCH) on the orderbook subscription.
    fn place(&mut self, batch: Vec<String>, now_ms: u64) -> Option<Command> {
        if batch.is_empty() {
            return None;
        }
        match self.orderbook {
            Some(SubState::Active(sid)) => Some(self.send_add(sid, batch, now_ms)),
            Some(SubState::Subscribing(_)) => {
                for t in batch {
                    self.markets.insert(t, Status::Queued);
                }
                None
            }
            None => {
                let kind = Pending::SubscribeOrderbook { tickers: batch.clone() };
                let cmd = self.command(kind, now_ms, |id| Command::subscribe(id, ORDERBOOK_CHANNEL, batch.clone()));
                for t in batch {
                    self.markets.insert(t, Status::Adding { cmd: cmd.id });
                }
                self.orderbook = Some(SubState::Subscribing(cmd.id));
                Some(cmd)
            }
        }
    }

    fn send_add(&mut self, sid: u64, batch: Vec<String>, now_ms: u64) -> Command {
        let kind = Pending::Add { tickers: batch.clone() };
        let cmd = self.command(kind, now_ms, |id| Command::update(id, sid, Action::AddMarkets, batch.clone()));
        for t in batch {
            self.markets.insert(t, Status::Adding { cmd: cmd.id });
        }
        cmd
    }

    /// Markets waiting for the orderbook subscription.
    fn queued(&self) -> Vec<String> {
        self.markets.iter().filter(|(_, s)| **s == Status::Queued).map(|(t, _)| t.clone()).collect()
    }

    fn orderbook_subscribed(&mut self, id: u64, sid: u64, tickers: &[String], now_ms: u64) -> Vec<Command> {
        self.orderbook = Some(SubState::Active(sid));
        self.confirm(id, tickers);
        let queued = self.queued();
        let mut send = self.place_all(queued, now_ms);
        let deletes: Vec<String> = std::mem::take(&mut self.queued_deletes).into_iter().collect();
        if !deletes.is_empty() {
            send.push(self.command(Pending::Delete, now_ms, |id| Command::update(id, sid, Action::DeleteMarkets, deletes)));
        }
        send
    }

    /// An add we'd given up on succeeded after all. Markets still waiting to retry are subscribed now;
    /// ones already being re-added are left to that attempt.
    fn late_add_ok(&mut self, id: u64) {
        let Some((tickers, _)) = self.timed_out_adds.remove(&id) else { return };
        let mut late = Vec::new();
        for t in tickers {
            if matches!(self.markets.get(&t), Some(Status::Backoff { .. })) {
                self.add_failures.remove(&t);
                self.markets.insert(t.clone(), Status::Subscribed);
                late.push(t);
            }
        }
        if !late.is_empty() {
            info!("timed-out add succeeded after all for {}", summarize(&late));
        }
    }

    /// Mark markets from a successful subscribe/add as subscribed.
    fn confirm(&mut self, id: u64, tickers: &[String]) {
        for t in tickers {
            if self.markets.get(t) == Some(&Status::Adding { cmd: id }) {
                self.markets.insert(t.clone(), Status::Subscribed);
                self.add_failures.remove(t);
            }
        }
    }

    /// A subscribe or add_markets command failed (`code` None = timed out). Decide what to retry.
    fn add_failed(&mut self, id: u64, tickers: Vec<String>, was_subscribe: bool, code: Option<i64>, what: &str, now_ms: u64) -> Vec<Command> {
        // Ignore markets removed (or re-placed) since the command was sent.
        let tickers: Vec<String> =
            tickers.into_iter().filter(|t| self.markets.get(t) == Some(&Status::Adding { cmd: id })).collect();
        let mut untested = Vec::new();
        if was_subscribe {
            // There's no subscription after all: whatever was waiting for it is placed again, after
            // these.
            self.orderbook = None;
            self.queued_deletes.clear();
            untested = self.queued();
        }
        let mut send = Vec::new();
        match code {
            Some(ERR_MARKET_LIMIT) => {
                warn!("this connection can't take more markets; more shards (KALSHI_SHARDS) would spread them out");
                self.back_off(&tickers, TRANSIENT_BACKOFF, what, now_ms);
            }
            None => self.back_off(&tickers, TRANSIENT_BACKOFF, what, now_ms),
            Some(c) if TRANSIENT_ERRORS.contains(&c) => self.back_off(&tickers, TRANSIENT_BACKOFF, what, now_ms),
            _ if tickers.len() > 1 => {
                // Find the culprit(s): retry each half as its own command.
                debug!("adding {} markets failed ({what}); retrying in halves", tickers.len());
                let (a, b) = tickers.split_at(tickers.len() / 2);
                send.extend(self.place(a.to_vec(), now_ms));
                send.extend(self.place(b.to_vec(), now_ms));
            }
            _ => self.back_off(&tickers, REJECTED_BACKOFF, what, now_ms),
        }
        send.extend(self.place_all(untested, now_ms));
        send
    }

    fn back_off(&mut self, tickers: &[String], schedule: (u64, u64), what: &str, now_ms: u64) {
        let mut delays = BTreeSet::new();
        for t in tickers {
            let failures = self.add_failures.entry(t.clone()).or_default();
            *failures += 1;
            let delay = backoff_ms(schedule, *failures);
            delays.insert(delay / 1000);
            self.markets.insert(t.clone(), Status::Backoff { at: now_ms + delay });
        }
        if !tickers.is_empty() {
            warn!("adding {} failed ({what}); retrying in {delays:?} s", summarize(tickers));
        }
    }

    fn snapshot_failed(&mut self, tickers: &[String], now_ms: u64) {
        for t in tickers {
            let entry = self.snapshot_backoff.entry(t.clone()).or_insert((0, 0));
            entry.0 += 1;
            entry.1 = now_ms + backoff_ms(SNAPSHOT_BACKOFF, entry.0);
        }
    }

    fn on_lifecycle(&mut self, msg: &Lifecycle, now_ms: u64, fx: &mut Effects) {
        let ticker = &msg.market_ticker;
        match msg.event_type.as_str() {
            "created" | "activated" => {
                let event = msg.additional_metadata.as_ref().and_then(|m| m.event_ticker.as_deref());
                match self.markets.get(ticker) {
                    None if self.universe.matches(ticker, event) => {
                        debug!("lifecycle: {ticker} {}", msg.event_type);
                        fx.send.extend(self.add_markets([ticker.clone()], now_ms));
                    }
                    // Subscribing may have failed because the market wasn't open yet: try again now.
                    Some(Status::Backoff { .. }) if msg.event_type == "activated" => {
                        debug!("lifecycle: {ticker} activated; retrying it now");
                        fx.send.extend(self.place_all(vec![ticker.clone()], now_ms));
                    }
                    _ => {}
                }
            }
            "determined" | "settled" if self.markets.contains_key(ticker) => {
                debug!("lifecycle: {ticker} {}", msg.event_type);
                fx.send.extend(self.remove_market(ticker, now_ms));
                fx.removed.push(ticker.clone());
            }
            _ => {}
        }
    }

    fn take_inflight(&mut self, pred: impl Fn(&(u64, u64)) -> bool) -> Vec<String> {
        let mut taken: Vec<String> =
            self.snapshot_inflight.iter().filter(|(_, v)| pred(v)).map(|(t, _)| t.clone()).collect();
        taken.sort();
        for t in &taken {
            self.snapshot_inflight.remove(t);
        }
        taken
    }

    /// Allocate an id for a new command and remember what it was for.
    fn command(&mut self, pending: Pending, now_ms: u64, build: impl FnOnce(u64) -> Command) -> Command {
        let id = self.next_id;
        self.next_id += 1;
        self.pending.insert(id, (pending, now_ms));
        build(id)
    }
}

/// Consistency checks across the bookkeeping, for tests.
#[cfg(test)]
impl Subscriptions {
    pub(crate) fn is_subscribed(&self, ticker: &str) -> bool {
        self.markets.get(ticker) == Some(&Status::Subscribed)
    }

    pub(crate) fn wanted(&self) -> impl Iterator<Item = &String> {
        self.markets.keys()
    }

    /// Panics with a description of the first inconsistency found.
    pub(crate) fn check_invariants(&self) {
        for (t, status) in &self.markets {
            match *status {
                Status::Adding { cmd } => match self.pending.get(&cmd) {
                    Some((Pending::SubscribeOrderbook { tickers } | Pending::Add { tickers }, _)) if tickers.contains(t) => {}
                    other => panic!("{t}: Adding via command {cmd}, but that's pending as {other:?}"),
                },
                Status::Queued => {
                    assert!(matches!(self.orderbook, Some(SubState::Subscribing(_))), "{t}: queued, but the subscription is {:?}", self.orderbook);
                }
                Status::Subscribed => {
                    assert!(matches!(self.orderbook, Some(SubState::Active(_))), "{t}: subscribed, but the subscription is {:?}", self.orderbook);
                }
                Status::Backoff { .. } => {}
            }
        }
        for t in self.snapshot_inflight.keys() {
            assert!(self.is_subscribed(t), "{t}: snapshot in flight but not subscribed ({:?})", self.markets.get(t));
        }
        if let Some(SubState::Subscribing(id)) = self.orderbook
            && let Some((p, _)) = self.pending.get(&id)
        {
            assert!(matches!(p, Pending::SubscribeOrderbook { .. }), "subscribing via {id}, pending as {p:?}");
        }
        if !matches!(self.orderbook, Some(SubState::Subscribing(_))) {
            assert!(self.queued_deletes.is_empty(), "queued deletes with the subscription {:?}", self.orderbook);
        }
    }
}

/// Short description of a ticker list for logs.
fn summarize(tickers: &[String]) -> String {
    const SHOWN: usize = 5;
    if tickers.len() <= SHOWN {
        format!("{tickers:?}")
    } else {
        format!("{:?} and {} more", &tickers[..SHOWN], tickers.len() - SHOWN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use crate::kalshi::discovery::Filter;

    fn frame(json: &str) -> Frame {
        serde_json::from_str(json).unwrap()
    }

    /// Commands as JSON, to check the wire format.
    fn cmd(c: &Command) -> Value {
        serde_json::to_value(c).unwrap()
    }

    fn id_of(c: &Command) -> u64 {
        c.id
    }

    fn tickers(prefix: &str, n: usize) -> Vec<String> {
        (0..n).map(|i| format!("{prefix}{i:03}")).collect()
    }

    fn subscribed(s: &mut Subscriptions, id: u64, channel: &str, sid: u64) -> Effects {
        s.handle(&frame(&format!(r#"{{"id":{id},"type":"subscribed","msg":{{"channel":"{channel}","sid":{sid}}}}}"#)), 0)
            .unwrap()
    }

    fn ok(s: &mut Subscriptions, id: u64) -> Effects {
        s.handle(&frame(&format!(r#"{{"id":{id},"type":"ok","msg":{{}}}}"#)), 0).unwrap()
    }

    fn error(s: &mut Subscriptions, id: u64, code: i64, now: u64) -> Result<Effects> {
        s.handle(&frame(&format!(r#"{{"id":{id},"type":"error","msg":{{"code":{code},"msg":"x"}}}}"#)), now)
    }

    fn lifecycle(seq: u64, ticker: &str, event_type: &str) -> Frame {
        frame(&format!(
            r#"{{"type":"market_lifecycle_v2","sid":10,"seq":{seq},"msg":{{"market_ticker":"{ticker}","event_type":"{event_type}"}}}}"#
        ))
    }

    fn snapshot(ticker: &str) -> Frame {
        frame(&format!(r#"{{"type":"orderbook_snapshot","sid":20,"seq":5,"msg":{{"market_ticker":"{ticker}","clock":1}}}}"#))
    }

    /// Started with market A, both subscriptions confirmed (lifecycle sid 10, orderbook sid 20).
    fn active() -> Subscriptions {
        let mut s = Subscriptions::new(Universe::filtered(vec![Filter::Series("SER".into())]));
        let sent = s.start(["A".to_string()], 0);
        assert_eq!(sent.len(), 2);
        assert_eq!(cmd(&sent[0])["params"]["channels"][0], LIFECYCLE_CHANNEL);
        assert_eq!(cmd(&sent[1])["params"]["market_tickers"], json!(["A"]));
        subscribed(&mut s, 1, LIFECYCLE_CHANNEL, 10);
        subscribed(&mut s, 2, ORDERBOOK_CHANNEL, 20);
        assert_eq!(s.counts(), (1, 1));
        s
    }

    #[test]
    fn adds_wait_for_orderbook_subscription() {
        let mut s = Subscriptions::new(Universe::filtered(vec![]));
        assert_eq!(s.start(Vec::<String>::new(), 0).len(), 1); // lifecycle only
        let sent = s.add_markets(["A".to_string(), "B".to_string()], 0);
        assert_eq!(cmd(&sent[0])["cmd"], "subscribe");
        assert!(s.add_markets(["C".to_string(), "A".to_string()], 0).is_empty()); // queued / duplicate
        assert!(s.remove_market("B", 0).is_empty()); // in the initial subscribe -> queued delete
        let fx = subscribed(&mut s, 2, ORDERBOOK_CHANNEL, 20);
        let sent: Vec<Value> = fx.send.iter().map(cmd).collect();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0]["params"], json!({"sid": 20, "market_tickers": ["C"], "action": "add_markets"}));
        assert_eq!(sent[1]["params"], json!({"sid": 20, "market_tickers": ["B"], "action": "delete_markets"}));
        assert_eq!(s.counts(), (1, 2));
        ok(&mut s, id_of(&fx.send[0]));
        assert_eq!(s.counts(), (2, 2));
    }

    #[test]
    fn large_adds_are_batched() {
        let mut s = Subscriptions::new(Universe::filtered(vec![]));
        let sent = s.start(tickers("M", 250), 0);
        assert_eq!(sent.len(), 2); // lifecycle + subscribe with the first batch; the rest queued
        assert_eq!(cmd(&sent[1])["params"]["market_tickers"].as_array().unwrap().len(), MAX_BATCH);
        let fx = subscribed(&mut s, 2, ORDERBOOK_CHANNEL, 20);
        let sizes: Vec<usize> =
            fx.send.iter().map(|c| cmd(c)["params"]["market_tickers"].as_array().unwrap().len()).collect();
        assert_eq!(sizes, vec![100, 50]);
    }

    #[test]
    fn lost_subscription_reconnects() {
        let mut s = active();
        let fx = s.handle(&frame(r#"{"type":"error","sid":20,"seq":9,"msg":{"code":25,"msg":"Subscription buffer overflow"}}"#), 0);
        assert!(fx.unwrap().reconnect.is_some());
        let mut s = active();
        let id = id_of(&s.snapshot_request(&["A".to_string()], 0)[0]);
        assert!(error(&mut s, id, ERR_UNKNOWN_SID, 0).unwrap().reconnect.is_some());
        // An error on a subscription we don't know about is just logged.
        let mut s = active();
        let fx = s.handle(&frame(r#"{"type":"error","sid":99,"msg":{"code":25,"msg":"x"}}"#), 0);
        assert!(fx.unwrap().reconnect.is_none());
    }

    #[test]
    fn rejected_subscribe_is_bisected() {
        let mut s = Subscriptions::new(Universe::filtered(vec![]));
        s.start(["A", "B", "C", "BAD"].map(String::from), 0);
        s.add_markets(["Q".to_string()], 0); // queued for the subscription that's about to fail
        let fx = error(&mut s, 2, 10, 0).unwrap();
        // A new subscribe with the first half; the second half and Q wait for it.
        assert_eq!(fx.send.len(), 1);
        assert_eq!(cmd(&fx.send[0])["params"]["market_tickers"], json!(["A", "B"]));
        s.check_invariants();
        let new_id = id_of(&fx.send[0]);
        let fx = subscribed(&mut s, new_id, ORDERBOOK_CHANNEL, 21);
        let sent: Vec<Value> = fx.send.iter().map(cmd).collect();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0]["params"], json!({"sid": 21, "market_tickers": ["BAD", "C", "Q"], "action": "add_markets"}));
        // That fails -> halves; BAD alone fails -> backoff, not fatal.
        let fx = error(&mut s, sent[0]["id"].as_u64().unwrap(), 10, 0).unwrap();
        let halves: Vec<Value> = fx.send.iter().map(cmd).collect();
        assert_eq!(halves[0]["params"]["market_tickers"], json!(["BAD"]));
        assert_eq!(halves[1]["params"]["market_tickers"], json!(["C", "Q"]));
        assert!(error(&mut s, halves[0]["id"].as_u64().unwrap(), 10, 0).unwrap().send.is_empty());
        ok(&mut s, halves[1]["id"].as_u64().unwrap());
        assert_eq!(s.markets["BAD"], Status::Backoff { at: REJECTED_BACKOFF.0 });
        assert_eq!(s.counts(), (4, 5)); // A, B, C, Q subscribed; BAD waiting
        s.check_invariants();
        // Discovery doesn't bypass the backoff; expire retries once it has elapsed.
        assert!(s.add_markets(["BAD".to_string()], 1).is_empty());
        assert!(s.expire(REJECTED_BACKOFF.0 - 1).send.is_empty());
        let retry = s.expire(REJECTED_BACKOFF.0).send;
        assert_eq!(retry.len(), 1);
        assert_eq!(cmd(&retry[0])["params"]["market_tickers"], json!(["BAD"]));
    }

    #[test]
    fn market_limit_backs_off() {
        let mut s = active();
        let sent = s.add_markets(["B".to_string(), "C".to_string()], 0);
        assert!(error(&mut s, id_of(&sent[0]), ERR_MARKET_LIMIT, 0).unwrap().send.is_empty());
        assert_eq!(s.markets["B"], Status::Backoff { at: TRANSIENT_BACKOFF.0 });
        let retry = s.expire(TRANSIENT_BACKOFF.0).send;
        assert_eq!(cmd(&retry[0])["params"], json!({"sid": 20, "market_tickers": ["B", "C"], "action": "add_markets"}));
    }

    #[test]
    fn unexplained_ok_to_subscribe_reconnects() {
        let mut s = Subscriptions::new(Universe::filtered(vec![]));
        s.start(["A".to_string()], 0);
        let fx = s.handle(&frame(r#"{"id":2,"type":"ok","sid":99,"seq":1}"#), 0).unwrap();
        assert!(fx.reconnect.is_some());
    }

    #[test]
    fn markets_missing_from_two_listings_are_removed() {
        let mut s = active();
        s.add_markets(["B".to_string(), "C".to_string()], 0);
        let listing = |l: &[&str]| l.iter().map(|t| t.to_string()).collect::<BTreeSet<_>>();
        assert!(s.on_listing(&listing(&["A", "B"]), 0).removed.is_empty()); // C: first strike
        assert!(s.on_listing(&listing(&["A", "B", "C"]), 0).removed.is_empty()); // listed again: reset
        assert!(s.on_listing(&listing(&["A"]), 0).removed.is_empty());
        let fx = s.on_listing(&listing(&["A", "C"]), 0);
        assert_eq!(fx.removed, vec!["B".to_string()]);
        assert_eq!(s.wanted().collect::<Vec<_>>(), vec!["A", "C"]);
        s.check_invariants();
    }

    #[test]
    fn activation_retries_a_backed_off_market() {
        let mut s = active();
        let sent = s.add_markets(["SER-1-X".to_string()], 0);
        error(&mut s, id_of(&sent[0]), 10, 0).unwrap();
        assert!(matches!(s.markets["SER-1-X"], Status::Backoff { .. }));
        assert!(s.handle(&lifecycle(1, "SER-1-X", "created"), 1).unwrap().send.is_empty());
        let fx = s.handle(&lifecycle(2, "SER-1-X", "activated"), 1).unwrap();
        assert_eq!(cmd(&fx.send[0])["params"]["market_tickers"], json!(["SER-1-X"]));
    }

    #[test]
    fn transient_errors_back_off_without_bisecting() {
        let mut s = active();
        let sent = s.add_markets(["B".to_string(), "C".to_string()], 0);
        assert!(error(&mut s, id_of(&sent[0]), 27, 0).unwrap().send.is_empty());
        assert_eq!(s.markets["B"], Status::Backoff { at: TRANSIENT_BACKOFF.0 });
        assert!(s.expire(TRANSIENT_BACKOFF.0 - 1).send.is_empty());
        let retry = s.expire(TRANSIENT_BACKOFF.0).send;
        assert_eq!(cmd(&retry[0])["params"]["market_tickers"], json!(["B", "C"]));
        // Second failure doubles the delay.
        error(&mut s, id_of(&retry[0]), 27, TRANSIENT_BACKOFF.0).unwrap();
        assert_eq!(s.markets["B"], Status::Backoff { at: 3 * TRANSIENT_BACKOFF.0 });
    }

    #[test]
    fn add_timeout_backs_off() {
        let mut s = active();
        s.add_markets(["B".to_string()], 0);
        assert!(s.expire(COMMAND_TIMEOUT_MS).send.is_empty());
        assert_eq!(s.markets["B"], Status::Backoff { at: COMMAND_TIMEOUT_MS + TRANSIENT_BACKOFF.0 });
    }

    #[test]
    fn late_ok_for_timed_out_add_confirms() {
        let mut s = active();
        let sent = s.add_markets(["B".to_string(), "C".to_string()], 0);
        s.expire(COMMAND_TIMEOUT_MS);
        assert_eq!(s.counts(), (1, 3));
        s.handle(&frame(&format!(r#"{{"id":{},"type":"ok","sid":20,"seq":9}}"#, id_of(&sent[0]))), COMMAND_TIMEOUT_MS + 1).unwrap();
        assert_eq!(s.counts(), (3, 3));
        assert!(s.expire(COMMAND_TIMEOUT_MS + TRANSIENT_BACKOFF.0).send.is_empty()); // no re-add
    }

    #[test]
    fn lifecycle_adds_matching_and_removes_settled() {
        let mut s = active();
        let fx = s.handle(&lifecycle(1, "SER-1-X", "created"), 0).unwrap();
        assert_eq!(cmd(&fx.send[0])["params"]["action"], "add_markets");
        assert!(s.handle(&lifecycle(2, "OTHER-1-X", "created"), 0).unwrap().send.is_empty());
        assert!(s.handle(&lifecycle(3, "SER-1-X", "activated"), 0).unwrap().send.is_empty()); // already wanted
        let fx = s.handle(&lifecycle(4, "A", "settled"), 0).unwrap();
        assert_eq!(cmd(&fx.send[0])["params"], json!({"sid": 20, "market_tickers": ["A"], "action": "delete_markets"}));
        assert_eq!(fx.removed, vec!["A".to_string()]);
        assert!(!fx.rediscover);
        assert!(s.handle(&lifecycle(6, "Z", "created"), 0).unwrap().rediscover);
    }

    /// Event lifecycle messages share the market lifecycle subscription's seq.
    #[test]
    fn event_lifecycle_advances_lifecycle_seq() {
        let mut s = active();
        s.handle(&lifecycle(70, "OTHER-1-X", "created"), 0).unwrap();
        let ev = frame(r#"{"type":"event_lifecycle","sid":10,"seq":71,"msg":{"event_ticker":"OTHER-1","series_ticker":"OTHER","title":"t"}}"#);
        assert_eq!(s.handle(&ev, 0).unwrap(), Effects::default());
        assert!(!s.handle(&lifecycle(72, "OTHER-1-Y", "created"), 0).unwrap().rediscover);
        assert!(s.handle(&lifecycle(74, "OTHER-1-Z", "created"), 0).unwrap().rediscover);
    }

    #[test]
    fn lost_first_lifecycle_message_rediscovers() {
        let mut s = active();
        assert!(s.handle(&lifecycle(2, "OTHER-1-X", "created"), 0).unwrap().rediscover);
    }

    #[test]
    fn lifecycle_on_other_sid_ignored() {
        let mut s = active();
        let f = frame(r#"{"type":"market_lifecycle_v2","sid":99,"seq":1,"msg":{"market_ticker":"SER-1-X","event_type":"created"}}"#);
        assert_eq!(s.handle(&f, 0).unwrap(), Effects::default());
    }

    #[test]
    fn lifecycle_subscribe_error_is_fatal() {
        let mut s = Subscriptions::new(Universe::filtered(vec![]));
        s.start(["A".to_string()], 0);
        assert!(error(&mut s, 1, 8, 0).is_err());
    }

    #[test]
    fn lifecycle_subscribe_transient_error_reconnects() {
        let mut s = Subscriptions::new(Universe::filtered(vec![]));
        s.start(["A".to_string()], 0);
        assert!(error(&mut s, 1, 18, 0).unwrap().reconnect.is_some());
    }

    #[test]
    fn subscribe_timeout_requests_reconnect() {
        let mut s = Subscriptions::new(Universe::filtered(vec![]));
        s.start(["A".to_string()], 0);
        assert!(s.expire(COMMAND_TIMEOUT_MS - 1).reconnect.is_none());
        assert!(s.expire(COMMAND_TIMEOUT_MS).reconnect.is_some());
    }

    #[test]
    fn snapshot_requests_dedupe_and_back_off() {
        let mut s = active();
        let r = vec!["A".to_string(), "NOTWANTED".to_string()];
        let sent = s.snapshot_request(&r, 0);
        assert_eq!(cmd(&sent[0])["params"], json!({"sid": 20, "market_tickers": ["A"], "action": "get_snapshot"}));
        assert!(s.snapshot_request(&r, 0).is_empty()); // already in flight
        let fx = error(&mut s, id_of(&sent[0]), 1, 0).unwrap();
        assert_eq!(fx.snapshot_failed, vec!["A".to_string()]);
        assert!(s.snapshot_request(&r, SNAPSHOT_BACKOFF.0 - 1).is_empty()); // backing off
        let sent = s.snapshot_request(&r, SNAPSHOT_BACKOFF.0);
        assert_eq!(sent.len(), 1);
        // Times out -> second failure, longer backoff; a snapshot arriving resets it.
        let fx = s.expire(SNAPSHOT_BACKOFF.0 + COMMAND_TIMEOUT_MS);
        assert_eq!(fx.snapshot_failed, vec!["A".to_string()]);
        assert_eq!(s.snapshot_backoff["A"], (2, SNAPSHOT_BACKOFF.0 + COMMAND_TIMEOUT_MS + 2 * SNAPSHOT_BACKOFF.0));
        s.handle(&snapshot("A"), 0).unwrap();
        assert_eq!(s.snapshot_request(&r, 0).len(), 1);
    }

    #[test]
    fn snapshots_only_for_confirmed_markets() {
        let mut s = active();
        s.add_markets(["B".to_string()], 0);
        assert!(s.snapshot_request(&["B".to_string()], 0).is_empty());
    }
}
