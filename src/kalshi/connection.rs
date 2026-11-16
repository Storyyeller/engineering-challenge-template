//! One Kalshi websocket connection, kept alive: connects, subscribes to its shard of the markets,
//! maintains their books (via [`Session`]) and reports what happens to them as [`SourceEvent`]s.
//! Reconnects with exponential backoff when the connection errors, closes, goes quiet, or is due to
//! be recycled. Only rejected subscriptions, and credentials rejected before they've ever worked, are
//! fatal.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use crossbeam::channel::Sender;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, Interval, Sleep, timeout};
use tokio_tungstenite::tungstenite::{self, Message, client::IntoClientRequest, http::HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};

use crate::collector::{Event, SourceEvent, SourceId};
use crate::kalshi::NEVER;
use crate::kalshi::auth::{AuthKey, WS_PATH};
use crate::kalshi::book::Change;
use crate::kalshi::convert;
use crate::kalshi::discovery::{Listing, Universe};
use crate::kalshi::messages::Command;
use crate::kalshi::session::Session;
use crate::kalshi::subscriptions::Effects;

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// A connection that lasted this long counts as healthy, resetting the reconnect backoff.
const STABLE_CONNECTION: Duration = Duration::from_secs(60);
/// Give up on a websocket handshake or send after this long.
const IO_TIMEOUT: Duration = Duration::from_secs(15);
/// How often to run housekeeping (timeouts, retries, resyncs).
const TICK_EVERY: Duration = Duration::from_secs(10);
/// How often to log a status line.
const STATUS_EVERY: Duration = Duration::from_secs(60);
/// Each round of verification (see [`Config::verify_secs`]) is spread over this many requests, rather
/// than one burst for every market.
const VERIFY_SLICES: usize = 90;

/// Settings shared by every connection.
pub struct Config {
    pub key_id: String,
    pub auth_key: AuthKey,
    pub host: &'static str,
    /// How often to check each local book against a fresh snapshot (0 = never).
    pub verify_secs: u64,
    /// Reconnect if nothing (not even a pong) arrives for this long.
    pub idle_timeout: Duration,
}

/// One connection's identity and schedule.
pub struct Worker {
    pub source: SourceId,
    pub universe: Universe,
    /// Wait this long before the first connection, to spread out the startup burst.
    pub start_delay: Duration,
    /// Reconnect after this long, to bound the damage of anything going slowly wrong on a
    /// connection. The first connection is recycled after `first_recycle`, so that replicas of a
    /// shard take turns.
    pub recycle: Option<Duration>,
    pub first_recycle: Option<Duration>,
}

/// How a connection talks to the rest of the collector.
pub struct Links {
    pub events: Sender<SourceEvent>,
    /// Book requests from the publisher (symbols).
    pub book_requests: mpsc::UnboundedReceiver<String>,
    /// The markets most recently listed by discovery (all shards).
    pub markets: watch::Receiver<Arc<Listing>>,
    /// Ask for discovery to run again soon, e.g. when lifecycle notifications may have been missed.
    pub rediscover: mpsc::UnboundedSender<()>,
    pub shutdown: watch::Receiver<bool>,
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How a connection ended, short of a fatal error.
enum End {
    Shutdown,
    /// Closed on schedule (see [`Worker::recycle`]).
    Recycled,
    Lost(anyhow::Error),
}

fn now_ms(now: DateTime<Utc>) -> u64 {
    now.timestamp_millis().try_into().unwrap_or(0)
}

/// An interval that ticks every `period`, starting `first` from now (never, if `period` is zero).
fn interval(first: Duration, period: Duration) -> Interval {
    let (first, period) = if period.is_zero() {
        (NEVER, NEVER)
    } else {
        (first, period)
    };
    let mut i = tokio::time::interval_at(Instant::now() + first, period);
    i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    i
}

/// Somewhere between half and one and a half times `d`, at random, so that connections dropped
/// together don't all reconnect at the same moment.
fn jittered(d: Duration) -> Duration {
    use std::hash::{BuildHasher, Hasher};
    // Each RandomState has fresh random keys, which is all the randomness this needs.
    let r = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    d.mul_f64(0.5 + r as f64 / u64::MAX as f64)
}

/// Resolves once shutdown has been requested (never, if the signal handler couldn't be installed).
async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    if rx.wait_for(|&v| v).await.is_err() {
        std::future::pending::<()>().await;
    }
}

async fn send_all<S>(write: &mut S, msgs: impl IntoIterator<Item = Message>) -> Result<()>
where
    S: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    for m in msgs {
        timeout(IO_TIMEOUT, write.send(m))
            .await
            .map_err(|_| anyhow!("websocket send timed out"))??;
    }
    Ok(())
}

async fn flush<S>(write: &mut S) -> Result<()>
where
    S: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    timeout(IO_TIMEOUT, write.flush())
        .await
        .map_err(|_| anyhow!("websocket flush timed out"))??;
    Ok(())
}

/// Send commands, reporting each one to the session once it's been sent. An error means the
/// connection is lost.
async fn send_cmds<S>(write: &mut S, session: &mut Session, cmds: Vec<Command>) -> Result<()>
where
    S: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    for c in cmds {
        send_all(write, [Message::text(c.to_json())]).await?;
        session.sent(&c);
    }
    Ok(())
}

/// Open an authenticated websocket. The outer error is fatal (credentials rejected); the inner one
/// is worth retrying.
async fn connect(cfg: &Config) -> Result<Result<WsStream>> {
    let timestamp_ms = Utc::now().timestamp_millis().to_string();
    let signature = cfg.auth_key.sign(&timestamp_ms)?;
    let url = format!("wss://{}{WS_PATH}", cfg.host);
    let mut request = url.as_str().into_client_request()?;
    let headers = request.headers_mut();
    headers.insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&cfg.key_id)?);
    headers.insert(
        "KALSHI-ACCESS-SIGNATURE",
        HeaderValue::from_str(&signature)?,
    );
    headers.insert(
        "KALSHI-ACCESS-TIMESTAMP",
        HeaderValue::from_str(&timestamp_ms)?,
    );

    info!("connecting to {url}");
    match timeout(IO_TIMEOUT, tokio_tungstenite::connect_async(request)).await {
        Err(_) => Ok(Err(anyhow!("websocket handshake timed out"))),
        Ok(Ok((ws, response))) => {
            info!("connected: HTTP {}", response.status());
            Ok(Ok(ws))
        }
        Ok(Err(tungstenite::Error::Http(response)))
            if matches!(response.status().as_u16(), 401 | 403) =>
        {
            bail!(
                "websocket handshake rejected: HTTP {} (check key ID / key file / KALSHI_ENV / clock)",
                response.status()
            )
        }
        Ok(Err(e)) => Ok(Err(
            anyhow::Error::new(e).context("websocket handshake failed")
        )),
    }
}

/// Keeps one connection going until shutdown. Errors are fatal.
pub async fn run(cfg: Arc<Config>, worker: Worker, mut links: Links) -> Result<()> {
    tokio::select! {
        _ = tokio::time::sleep(worker.start_delay) => {}
        _ = wait_shutdown(&mut links.shutdown) => return Ok(()),
    }
    // Markets that were determined/settled; not resubscribed after a reconnect.
    let mut removed = BTreeSet::new();
    let mut recycle = worker.first_recycle;
    let mut backoff = MIN_BACKOFF;
    let mut first = true;
    loop {
        let started = Instant::now();
        let conn = tokio::select! {
            r = connect(&cfg) => match r {
                Ok(conn) => conn,
                // Credentials that have worked before are more likely being rejected by a passing
                // problem on Kalshi's side than revoked, so keep trying.
                Err(e) if !first => Err(e),
                Err(e) => return Err(e),
            },
            _ = wait_shutdown(&mut links.shutdown) => return Ok(()),
        };
        let end = match conn {
            Err(e) => End::Lost(e),
            Ok(ws) => {
                if !first {
                    // Markets may have been created while we were disconnected.
                    let _ = links.rediscover.send(());
                }
                first = false;
                let end =
                    run_connection(&cfg, &worker, &mut links, &mut removed, recycle, ws).await;
                // This connection's books are gone. (Not worth failing over when everything's
                // shutting down.)
                if !matches!(end, Ok(End::Shutdown)) {
                    let disconnected = SourceEvent {
                        source: worker.source,
                        received_at: Utc::now(),
                        event: Event::Disconnected,
                    };
                    send_event(&links.events, disconnected)?;
                }
                end?
            }
        };
        match end {
            End::Shutdown => return Ok(()),
            End::Recycled => {
                // Planned, so no backoff; the replicas' schedules stay staggered.
                info!("recycling connection after {recycle:?}");
                backoff = MIN_BACKOFF;
                recycle = worker.recycle;
            }
            End::Lost(e) => {
                if started.elapsed() >= STABLE_CONNECTION {
                    backoff = MIN_BACKOFF;
                    recycle = worker.recycle;
                }
                let wait = jittered(backoff);
                warn!("connection lost: {e:#}; reconnecting in {wait:.1?}");
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = wait_shutdown(&mut links.shutdown) => return Ok(()),
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// The listed markets this connection should follow. Listings are already filtered, so only the
/// shard needs checking.
fn shard_markets(
    listed: &BTreeSet<String>,
    universe: &Universe,
    removed: &BTreeSet<String>,
) -> Vec<String> {
    listed
        .iter()
        .filter(|t| universe.shard.contains(t) && !removed.contains(*t))
        .cloned()
        .collect()
}

/// Pass an event to the publisher. The publisher only stops before the connections if it failed
/// (e.g. writing the output), and then there's no point carrying on.
fn send_event(events: &Sender<SourceEvent>, event: SourceEvent) -> Result<()> {
    events
        .send(event)
        .map_err(|_| anyhow!("the publisher has stopped"))
}

/// Report book changes to the publisher.
fn publish(
    session: &mut Session,
    source: SourceId,
    received_at: DateTime<Utc>,
    events: &Sender<SourceEvent>,
) -> Result<()> {
    for change in session.take_changes() {
        let event = match change {
            Change::Snapshot {
                ticker,
                sending_ts_ms,
            } => {
                // Gone again already if, say, it was unsubscribed by the same message.
                let Some(book) = session.synced_book(&ticker) else {
                    continue;
                };
                convert::book_event(ticker, book, sending_ts_ms.and_then(convert::from_ms))
            }
            Change::Level {
                ticker,
                side,
                price,
                qty,
                clock,
                ts_ms,
            } => {
                let exchange_ts = ts_ms.and_then(convert::from_ms).unwrap_or(received_at);
                convert::level_event(ticker, side, price, qty, clock, exchange_ts)
            }
            Change::Stale { ticker } => Event::Stale { symbol: ticker },
            Change::Removed { ticker } => Event::Removed { symbol: ticker },
        };
        send_event(
            events,
            SourceEvent {
                source,
                received_at,
                event,
            },
        )?;
    }
    Ok(())
}

/// Something that happened to a connection: what [`Connection::next_input`] waits for.
enum Input {
    Shutdown,
    /// Nothing received for the idle timeout.
    Idle,
    /// Time to recycle the connection.
    Recycle,
    /// Time to send a ping.
    Ping,
    /// Time for housekeeping (timeouts, retries, resyncs).
    Tick,
    /// Time to log a status line.
    Status,
    /// Time to request snapshots to check the local books against.
    Verify,
    /// Discovery listed markets.
    Discovered,
    /// Discovery has stopped (shutting down).
    DiscoveryStopped,
    /// The publisher asks for this symbol's book.
    BookRequest(String),
    /// From the websocket: a frame, an error, or `None` if the stream ended.
    Frame(Option<Result<Message, tungstenite::Error>>),
}

/// One live websocket connection: its session, its I/O and its timers.
struct Connection<'a> {
    cfg: &'a Config,
    worker: &'a Worker,
    links: &'a mut Links,
    /// Markets that were determined/settled (kept across reconnects).
    removed: &'a mut BTreeSet<String>,
    session: Session,
    write: SplitSink<WsStream, Message>,
    read: SplitStream<WsStream>,
    tick: Interval,
    status_tick: Interval,
    verify_tick: Interval,
    ping_tick: Interval,
    idle: Pin<Box<Sleep>>,
    recycle_at: Pin<Box<Sleep>>,
    discovery_running: bool,
}

/// Subscribe and maintain books on one connection until it ends.
/// Errors are fatal; a lost connection is `Ok(End::Lost)`. The logic lives in [`Session`]; this is
/// the I/O around it.
async fn run_connection(
    cfg: &Config,
    worker: &Worker,
    links: &mut Links,
    removed: &mut BTreeSet<String>,
    recycle: Option<Duration>,
    ws: WsStream,
) -> Result<End> {
    let (mut write, read) = ws.split();
    let mut session = Session::new(worker.universe.clone());

    // The lifecycle subscription goes first, so markets created after the listing we start from
    // aren't missed.
    let tickers = shard_markets(
        &links.markets.borrow_and_update().markets,
        &worker.universe,
        removed,
    );
    let start = session.start(tickers, now_ms(Utc::now()));
    if let Err(e) = send_cmds(&mut write, &mut session, start).await {
        return Ok(End::Lost(e));
    }

    let mut conn = Connection {
        cfg,
        worker,
        links,
        removed,
        session,
        write,
        read,
        tick: interval(TICK_EVERY, TICK_EVERY),
        status_tick: interval(STATUS_EVERY, STATUS_EVERY),
        // Not during the initial burst of subscribing: the first round starts a whole period in.
        verify_tick: {
            let every = Duration::from_secs(cfg.verify_secs);
            interval(every, every / VERIFY_SLICES as u32)
        },
        // Our pings guarantee regular traffic (pongs), so silence means the connection is dead.
        ping_tick: {
            let every = (cfg.idle_timeout / 3).max(Duration::from_secs(1));
            interval(every, every)
        },
        idle: Box::pin(tokio::time::sleep(cfg.idle_timeout)),
        recycle_at: Box::pin(tokio::time::sleep(recycle.unwrap_or(NEVER))),
        discovery_running: true,
    };
    let end = loop {
        let input = conn.next_input().await;
        // When the input happened: only once it's arrived, not when we started waiting.
        let received_at = Utc::now();
        if let ControlFlow::Break(end) = conn.handle(input, received_at).await? {
            break end;
        }
    };
    publish(
        &mut conn.session,
        worker.source,
        Utc::now(),
        &conn.links.events,
    )?;
    info!("{}", conn.session.summary());
    Ok(end)
}

impl Connection<'_> {
    /// Wait for the next thing to happen.
    async fn next_input(&mut self) -> Input {
        tokio::select! {
            _ = wait_shutdown(&mut self.links.shutdown) => Input::Shutdown,
            _ = &mut self.idle => Input::Idle,
            _ = &mut self.recycle_at => Input::Recycle,
            _ = self.ping_tick.tick() => Input::Ping,
            _ = self.tick.tick() => Input::Tick,
            _ = self.status_tick.tick() => Input::Status,
            _ = self.verify_tick.tick() => Input::Verify,
            changed = self.links.markets.changed(), if self.discovery_running => match changed {
                Ok(()) => Input::Discovered,
                Err(_) => Input::DiscoveryStopped,
            },
            Some(symbol) = self.links.book_requests.recv() => Input::BookRequest(symbol),
            frame = self.read.next() => Input::Frame(frame),
        }
    }

    /// Act on one input. Errors are fatal; `Break` ends the connection.
    async fn handle(
        &mut self,
        input: Input,
        received_at: DateTime<Utc>,
    ) -> Result<ControlFlow<End>> {
        let now_ms = now_ms(received_at);
        let fx: Effects = match input {
            Input::Shutdown => {
                self.close().await;
                return Ok(ControlFlow::Break(End::Shutdown));
            }
            Input::Idle => {
                return Ok(lost(anyhow!(
                    "nothing received for {:?}",
                    self.cfg.idle_timeout
                )));
            }
            Input::Recycle => {
                self.close().await;
                return Ok(ControlFlow::Break(End::Recycled));
            }
            Input::Ping => {
                return Ok(
                    match send_all(&mut self.write, [Message::Ping(Default::default())]).await {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(e) => lost(e),
                    },
                );
            }
            Input::Tick => self.session.on_tick(now_ms),
            Input::Status => {
                info!("{}", self.session.summary());
                return Ok(ControlFlow::Continue(()));
            }
            Input::Verify => self.session.on_verify(now_ms, VERIFY_SLICES).into(),
            Input::Discovered => {
                let listing = self.links.markets.borrow_and_update().clone();
                if listing.complete {
                    // Settled markets only need keeping out while listings still include them.
                    self.removed.retain(|t| listing.markets.contains(t));
                }
                let found = shard_markets(&listing.markets, &self.worker.universe, self.removed);
                let mut send = self.session.on_discovered(found, now_ms);
                if listing.complete {
                    send.extend(self.session.on_listing(&listing.markets, now_ms));
                }
                send.into()
            }
            Input::DiscoveryStopped => {
                self.discovery_running = false;
                return Ok(ControlFlow::Continue(()));
            }
            Input::BookRequest(symbol) => {
                let event = match self.session.synced_book(&symbol) {
                    Some(book) => convert::book_event(symbol, book, None),
                    None => Event::Stale { symbol },
                };
                send_event(
                    &self.links.events,
                    SourceEvent {
                        source: self.worker.source,
                        received_at,
                        event,
                    },
                )?;
                return Ok(ControlFlow::Continue(()));
            }
            Input::Frame(frame) => {
                self.idle
                    .as_mut()
                    .reset(Instant::now() + self.cfg.idle_timeout);
                match self.on_frame(frame).await {
                    ControlFlow::Break(end) => return Ok(ControlFlow::Break(end)),
                    ControlFlow::Continue(None) => return Ok(ControlFlow::Continue(())),
                    ControlFlow::Continue(Some(raw)) => self.session.on_message(&raw, now_ms)?,
                }
            }
        };
        self.apply(fx, received_at).await
    }

    /// Handle a websocket frame at the protocol level. Returns the JSON message it carries, if any.
    async fn on_frame(
        &mut self,
        frame: Option<Result<Message, tungstenite::Error>>,
    ) -> ControlFlow<End, Option<serde_json::Value>> {
        let text = match frame {
            None => return lost(anyhow!("stream ended")),
            Some(Err(e)) => return lost(anyhow::Error::new(e).context("websocket error")),
            Some(Ok(Message::Text(text))) => text,
            // tungstenite queues pong replies automatically; flush so they go out.
            Some(Ok(Message::Ping(_))) => {
                return match flush(&mut self.write).await {
                    Ok(()) => ControlFlow::Continue(None),
                    Err(e) => lost(e),
                };
            }
            Some(Ok(Message::Pong(_))) => return ControlFlow::Continue(None),
            Some(Ok(Message::Close(frame))) => return lost(anyhow!("server closed: {frame:?}")),
            Some(Ok(other)) => {
                debug!("other frame: {other:?}");
                return ControlFlow::Continue(None);
            }
        };
        match serde_json::from_str(&text) {
            Ok(raw) => ControlFlow::Continue(Some(raw)),
            Err(e) => {
                warn!("non-JSON message ({e}): {text}");
                ControlFlow::Continue(None)
            }
        }
    }

    /// Carry out what the session asked for, and report its book changes to the publisher.
    async fn apply(&mut self, fx: Effects, received_at: DateTime<Utc>) -> Result<ControlFlow<End>> {
        if let Some(reason) = fx.reconnect {
            return Ok(lost(anyhow!(reason)));
        }
        self.removed.extend(fx.removed);
        if fx.rediscover {
            let _ = self.links.rediscover.send(());
        }
        // Report changes before sending, which can take a while if the socket is backed up: the
        // publisher only waits a moment for each event.
        publish(
            &mut self.session,
            self.worker.source,
            received_at,
            &self.links.events,
        )?;
        let sent = send_cmds(&mut self.write, &mut self.session, fx.send).await;
        // Sending can change books too (unsubscribing drops them). These count even if a send failed.
        publish(
            &mut self.session,
            self.worker.source,
            received_at,
            &self.links.events,
        )?;
        Ok(match sent {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => lost(e),
        })
    }

    /// Say goodbye to the server, briefly; the connection is being dropped either way.
    async fn close(&mut self) {
        let _ = timeout(
            Duration::from_secs(1),
            self.write.send(Message::Close(None)),
        )
        .await;
    }
}

fn lost<T>(e: anyhow::Error) -> ControlFlow<End, T> {
    ControlFlow::Break(End::Lost(e))
}
