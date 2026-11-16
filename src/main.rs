//! Kalshi orderbook collector: follows every open, non-multivariate Kalshi market, and writes their
//! order books to stdout as SBE messages (see [`engineering_challenge::util::AlignedSbeWriter`]): a
//! price level update for every change, a snapshot whenever a book is (re)synced or switches source,
//! and a snapshot of every book in sync at the start of every minute.
//!
//! Usage:
//!   cargo run --release -- [series:SERIES_TICKER | event:EVENT_TICKER]... > orderbooks.sbe
//!
//! With no arguments every market is followed; filters restrict it to some series or events.
//!
//! Markets are split into shards by ticker, and each shard is followed on several websocket
//! connections (replicas). For each market the output follows one connection, switching to
//! another one (with a fresh snapshot) if that connection's book goes out of sync, the connection
//! drops, or it falls behind. On ctrl-c the websockets are closed and the output is flushed; press
//! ctrl-c again to exit immediately.
//!
//! Logs go to stderr, labelled with the connection they came from. Set
//! `RUST_LOG=engineering_challenge=debug` for per-market detail (subscriptions, lifecycle events,
//! resyncs), or `RUST_LOG=warn` for problems only.
//!
//! Env vars:
//!   KALSHI_API_KEY_ID        API key ID (UUID shown when creating the key)
//!   KALSHI_PRIVATE_KEY_PATH  path to the downloaded private key (Ed25519 or RSA, PEM)
//!   KALSHI_ENV               "prod" (default) or "demo"
//!   KALSHI_SHARDS            number of shards the markets are split into (default 32)
//!   KALSHI_REPLICAS          connections per shard (default 2)
//!   KALSHI_RECYCLE_MINS      reconnect each connection after this long, replicas taking turns
//!                            (default 360 with more than one replica, 0 = never)
//!   KALSHI_VERIFY_SECS       how often to check each local book against a fresh snapshot (default 900,
//!                            0 = off); the requests are spread out over the period
//!   KALSHI_DISCOVERY_SECS    how often to list markets over REST (default 900, 0 = only at startup and
//!                            when lifecycle notifications may have been missed). New markets are
//!                            normally picked up from lifecycle notifications.
//!   KALSHI_IDLE_TIMEOUT_SECS reconnect if nothing (not even a pong) arrives for this long (default 30)

use std::io::IsTerminal;
use std::os::fd::AsFd;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use engineering_challenge::collector::{self, Publisher, SourceEvent};
use engineering_challenge::kalshi::auth::{AuthKey, check_key_id};
use engineering_challenge::kalshi::connection::{self, Config, Links, Worker};
use engineering_challenge::kalshi::discovery::{self, Filter, Listing, Shard, Universe};
use engineering_challenge::util::AlignedSbeWriter;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::{Instrument, error, info, info_span};

/// The chunk size [`engineering_challenge::util::AlignedSbeReader`] reads with.
const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;
/// Delay between starting successive connections, to spread out the subscription burst.
const START_STAGGER: Duration = Duration::from_millis(500);

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(v) => v.parse().with_context(|| format!("{name} must be a number, got {v:?}")),
        Err(_) => Ok(default),
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // reqwest is built without a bundled crypto provider; use the same one as the websocket.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let key_id = std::env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY_ID not set")?;
    check_key_id(&key_id)?;
    let key_path = std::env::var("KALSHI_PRIVATE_KEY_PATH").context("KALSHI_PRIVATE_KEY_PATH not set")?;
    let (host, rest_base) = match std::env::var("KALSHI_ENV").as_deref() {
        Ok("prod") | Err(_) => ("external-api-ws.kalshi.com", "https://external-api.kalshi.com/trade-api/v2"),
        Ok("demo") => ("external-api-ws.demo.kalshi.co", "https://external-api.demo.kalshi.co/trade-api/v2"),
        Ok(other) => bail!("KALSHI_ENV must be 'demo' or 'prod', got {other:?}"),
    };
    let idle_secs = env_u64("KALSHI_IDLE_TIMEOUT_SECS", 30)?;
    if idle_secs == 0 {
        bail!("KALSHI_IDLE_TIMEOUT_SECS must be positive");
    }
    let shards = env_u64("KALSHI_SHARDS", 32)? as usize;
    let replicas = env_u64("KALSHI_REPLICAS", 2)? as usize;
    if shards == 0 || replicas == 0 {
        bail!("KALSHI_SHARDS and KALSHI_REPLICAS must be positive");
    }
    let recycle_mins = env_u64("KALSHI_RECYCLE_MINS", if replicas > 1 { 360 } else { 0 })?;
    let verify_secs = env_u64("KALSHI_VERIFY_SECS", 900)?;
    let discovery_secs = env_u64("KALSHI_DISCOVERY_SECS", 900)?;

    let mut filters = Vec::new();
    for arg in std::env::args().skip(1) {
        match Filter::parse(&arg) {
            Some(f) => filters.push(f),
            None => bail!(
                "unrecognized argument {arg:?}\n\
                 usage: engineering-challenge [series:SERIES_TICKER | event:EVENT_TICKER]... > FILE"
            ),
        }
    }
    if filters.is_empty() {
        info!("following every open non-multivariate market");
    } else {
        info!("following markets matching {filters:?}");
    }

    if std::io::stdout().is_terminal() {
        bail!("the output is binary SBE; redirect stdout to a file or pipe");
    }
    // Our own handle on stdout, rather than reopening /dev/stdout, which would truncate a file opened
    // for appending (`>>`).
    let stdout = std::fs::File::from(std::io::stdout().as_fd().try_clone_to_owned()?);
    let mut writer = AlignedSbeWriter::from_file(stdout, CHUNK_SIZE_64_MEBIBYTES)?;

    let auth_key = AuthKey::load(&key_path)?;
    info!("loaded {} key; {shards} shard(s) x {replicas} replica(s)", auth_key.kind());
    let cfg = Arc::new(Config {
        key_id,
        auth_key,
        host,
        verify_secs,
        idle_timeout: Duration::from_secs(idle_secs),
    });

    let (events_tx, events_rx) = crossbeam::channel::unbounded::<SourceEvent>();
    let (shutdown_tx, shutdown) = watch::channel(false);
    let shutdown_tx = Arc::new(shutdown_tx);
    let (listed_tx, listed) = watch::channel(Arc::new(Listing::default()));
    let (rediscover_tx, rediscover_rx) = mpsc::unbounded_channel();

    let recycle = (recycle_mins > 0).then(|| Duration::from_secs(recycle_mins * 60));
    let mut workers = Vec::new();
    let mut book_request_txs = Vec::new();
    for shard in 0..shards {
        for replica in 0..replicas {
            let source = workers.len();
            let (tx, book_requests) = mpsc::unbounded_channel();
            book_request_txs.push(tx);
            let worker = Worker {
                source,
                universe: Universe { filters: filters.clone(), shard: Shard { index: shard, count: shards } },
                start_delay: START_STAGGER * source as u32,
                recycle,
                // Replicas of a shard take turns, so one of them is always connected.
                first_recycle: recycle.map(|r| r * (replica as u32 + 1) / replicas as u32),
            };
            let links = Links {
                events: events_tx.clone(),
                book_requests,
                markets: listed.clone(),
                rediscover: rediscover_tx.clone(),
                shutdown: shutdown.clone(),
            };
            // Labels everything logged for this connection.
            let span = info_span!("conn", shard = shard + 1, replica = replica + 1);
            let name = format!("shard {}/{shards} replica {}/{replicas}", shard + 1, replica + 1);
            workers.push((worker, links, span, name));
        }
    }
    // Only the connections may hold senders, so the publisher finishes once they're all gone.
    drop(events_tx);
    drop(rediscover_tx);

    let publisher = std::thread::Builder::new().name("publisher".into()).spawn(move || {
        let _span = info_span!("publisher").entered();
        let request = |source: usize, symbol: String| {
            let _ = book_request_txs[source].send(symbol);
        };
        let report = |p: &Publisher| {
            info!("symbols={} in_sync={} {}", p.symbols(), p.followed(), p.stats);
        };
        let result = collector::run_publisher(events_rx, &mut writer, request, report);
        drop(writer); // closes stdout
        result
    })?;

    let runtime = tokio::runtime::Runtime::new()?;
    let collected: Result<()> = runtime.block_on(async move {
        // ctrl-c once: shut down cleanly. Twice: exit now.
        let ctrl_c_shutdown = shutdown_tx.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                info!("ctrl-c: closing connections and flushing output (ctrl-c again to exit now)");
                let _ = ctrl_c_shutdown.send(true);
                if tokio::signal::ctrl_c().await.is_ok() {
                    std::process::exit(130);
                }
            }
        });

        let http = reqwest::Client::builder().timeout(Duration::from_secs(60)).build()?;
        let discovery_task = tokio::spawn(discovery::run_discovery(
            http,
            rest_base,
            filters,
            Duration::from_secs(discovery_secs),
            listed_tx,
            rediscover_rx,
            shutdown.clone(),
        ).instrument(info_span!("discovery")));

        let mut connections = JoinSet::new();
        for (worker, links, span, name) in workers {
            let cfg = cfg.clone();
            let run = async move { connection::run(cfg, worker, links).await.with_context(|| name) };
            connections.spawn(run.instrument(span));
        }
        let mut result = Ok(());
        while let Some(done) = connections.join_next().await {
            let done = done.context("connection task panicked").and_then(|r| r);
            if let Err(e) = done
                && result.is_ok()
            {
                error!("fatal: {e:#}; shutting down");
                let _ = shutdown_tx.send(true);
                result = Err(e);
            }
        }
        discovery_task.abort();
        result
    });
    // Background tasks (the ctrl-c listener) are dropped with the runtime.
    runtime.shutdown_timeout(Duration::from_secs(1));

    let published = publisher.join().map_err(|_| anyhow::anyhow!("publisher thread panicked"))?;
    match (collected, published) {
        // A publisher failure stops the connections too; it's the root cause.
        (_, Err(e)) => Err(e.context("publishing")),
        (Err(e), Ok(_)) => Err(e),
        (Ok(()), Ok(p)) => {
            info!("done: {}", p.stats);
            Ok(())
        }
    }
}
