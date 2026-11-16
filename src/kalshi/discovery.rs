//! Finding markets to record: which markets a connection follows ([`Universe`]), and REST lookup of
//! the markets currently open.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::kalshi::NEVER;

/// Discovery runs triggered by requests are at least this far apart: with many connections, a burst
/// of reconnects shouldn't mean a burst of full listings.
const MIN_REDISCOVER_GAP: Duration = Duration::from_secs(60);
/// Retry a failed listing after this long.
const RETRY_AFTER: Duration = Duration::from_secs(30);

/// A set of markets to follow, including ones that are created later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    /// Every market in a series, e.g. `KXHIGHNY`.
    Series(String),
    /// Every market in an event, e.g. `KXHIGHNY-25AUG05`.
    Event(String),
}

impl Filter {
    /// Parse `series:X` or `event:X` (ticker case-insensitive). Anything else is not a filter.
    pub fn parse(arg: &str) -> Option<Self> {
        let (kind, ticker) = arg.split_once(':')?;
        let ticker = ticker.trim().to_ascii_uppercase();
        if ticker.is_empty() {
            return None;
        }
        match kind.to_ascii_lowercase().as_str() {
            "series" => Some(Self::Series(ticker)),
            "event" => Some(Self::Event(ticker)),
            _ => None,
        }
    }

    /// Whether a market belongs to this filter. Kalshi tickers are hierarchical
    /// (`SERIES-EVENTSUFFIX-MARKETSUFFIX`), so a prefix match works when the event ticker isn't known.
    pub fn matches(&self, market_ticker: &str, event_ticker: Option<&str>) -> bool {
        let has_prefix = |p: &str| {
            market_ticker.len() > p.len()
                && market_ticker.starts_with(p)
                && market_ticker.as_bytes()[p.len()] == b'-'
        };
        match self {
            Self::Series(s) => {
                has_prefix(s)
                    || event_ticker.is_some_and(|e| e == s || e.starts_with(&format!("{s}-")))
            }
            Self::Event(e) => has_prefix(e) || event_ticker == Some(e.as_str()),
        }
    }
}

/// Multivariate (combo) markets, which the collector leaves out. Their tickers all start with this
/// (e.g. `KXMVECROSSCATEGORY-...`); REST listings exclude them with `mve_filter=exclude`.
const MVE_PREFIX: &str = "KXMVE";

pub fn is_mve(market_ticker: &str) -> bool {
    market_ticker.starts_with(MVE_PREFIX)
}

/// One of `count` disjoint slices of the markets, so they can be spread over several connections.
/// Markets are assigned by a hash of their ticker, which is stable across runs and reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    pub index: usize,
    pub count: usize,
}

impl Shard {
    pub const ALL: Shard = Shard { index: 0, count: 1 };

    pub fn contains(&self, market_ticker: &str) -> bool {
        // FNV-1a: tiny, and unlike std's hasher guaranteed not to change between releases.
        let hash = market_ticker
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
                (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
            });
        hash % self.count as u64 == self.index as u64
    }
}

/// The markets one connection follows: those matching any of `filters` (every market, if there are
/// none), except multivariate ones, that fall in `shard`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Universe {
    pub filters: Vec<Filter>,
    pub shard: Shard,
}

impl Universe {
    /// Every non-multivariate market matching `filters`, on one connection.
    pub fn filtered(filters: Vec<Filter>) -> Self {
        Self {
            filters,
            shard: Shard::ALL,
        }
    }

    pub fn matches(&self, market_ticker: &str, event_ticker: Option<&str>) -> bool {
        !is_mve(market_ticker)
            && self.shard.contains(market_ticker)
            && (self.filters.is_empty()
                || self
                    .filters
                    .iter()
                    .any(|f| f.matches(market_ticker, event_ticker)))
    }
}

/// Markets listed by [`run_discovery`].
#[derive(Debug, Default)]
pub struct Listing {
    pub markets: BTreeSet<String>,
    /// Every open market, as opposed to a listing still in progress (the previous complete listing,
    /// plus what's been found so far). Markets missing from a complete listing have closed.
    pub complete: bool,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
}

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

/// Most REST requests per second while listing markets.
const REQUESTS_PER_SEC: u64 = 5;
/// Attempts per page before a listing gives up.
const PAGE_ATTEMPTS: u32 = 8;
/// Report progress every this many pages.
const PROGRESS_EVERY: usize = 10;

/// GET one page of markets, retrying rate limits (429), server errors and network errors with
/// backoff, honouring `Retry-After`.
async fn get_page(
    client: &reqwest::Client,
    url: &str,
    query: &[(&str, &str)],
    what: &str,
) -> Result<MarketsPage> {
    let mut delay = Duration::from_secs(1);
    for attempt in 1.. {
        let (problem, wait) = match client.get(url).query(query).send().await {
            Ok(r)
                if r.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || r.status().is_server_error() =>
            {
                let retry_after = r
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()?.parse().ok())
                    .map(Duration::from_secs);
                (
                    format!("HTTP {}", r.status()),
                    retry_after.unwrap_or(delay).max(delay),
                )
            }
            // Other client errors won't go away by retrying.
            Ok(r) => {
                let r = r
                    .error_for_status()
                    .with_context(|| format!("GET /markets ({what})"))?;
                return r
                    .json()
                    .await
                    .with_context(|| format!("parsing /markets response ({what})"));
            }
            Err(e) => (format!("{e}"), delay),
        };
        if attempt >= PAGE_ATTEMPTS {
            anyhow::bail!("GET /markets ({what}): {problem}, {attempt} times in a row");
        }
        info!("GET /markets ({what}): {problem}; retrying in {wait:?}");
        tokio::time::sleep(wait).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
    unreachable!()
}

/// Tickers of all open, non-multivariate markets matching any filter (every such market, if there are
/// no filters), via the public `GET /markets` endpoint. Markets that haven't opened yet are picked up
/// from their `activated` lifecycle notification instead. `progress` is called with the markets found
/// so far every few pages, since listing every market takes a while.
pub async fn discover(
    client: &reqwest::Client,
    rest_base: &str,
    filters: &[Filter],
    mut progress: impl FnMut(&BTreeSet<String>),
) -> Result<BTreeSet<String>> {
    let queries: Vec<Option<(&str, &str)>> = if filters.is_empty() {
        vec![None]
    } else {
        filters
            .iter()
            .map(|f| match f {
                Filter::Series(s) => Some(("series_ticker", s.as_str())),
                Filter::Event(e) => Some(("event_ticker", e.as_str())),
            })
            .collect()
    };
    let url = format!("{rest_base}/markets");
    let mut pace = tokio::time::interval(Duration::from_millis(1000 / REQUESTS_PER_SEC));
    pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut found = BTreeSet::new();
    let mut pages = 0;
    for filter in queries {
        let what = filter.map_or("all markets".to_string(), |(k, v)| format!("{k}={v}"));
        let mut cursor = String::new();
        loop {
            let mut query = vec![
                ("status", "open"),
                ("mve_filter", "exclude"),
                ("limit", "1000"),
            ];
            query.extend(filter);
            if !cursor.is_empty() {
                query.push(("cursor", &cursor));
            }
            pace.tick().await;
            let page = get_page(client, &url, &query, &what).await?;
            found.extend(
                page.markets
                    .into_iter()
                    .map(|m| m.ticker)
                    .filter(|t| !is_mve(t)),
            );
            pages += 1;
            if pages % PROGRESS_EVERY == 0 {
                progress(&found);
            }
            match page.cursor {
                Some(c) if !c.is_empty() => cursor = c,
                _ => break,
            }
        }
    }
    Ok(found)
}

/// List markets now, every `every` (if nonzero), and soon after each request on `rediscover`,
/// publishing each listing on `listed`. Runs until shutdown.
pub async fn run_discovery(
    client: reqwest::Client,
    rest_base: &str,
    filters: Vec<Filter>,
    every: Duration,
    listed: watch::Sender<Arc<Listing>>,
    mut rediscover: mpsc::UnboundedReceiver<()>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let started = Instant::now();
        // Partial listings are published as they come, so subscribing starts sooner. Connections
        // only drop markets missing from complete listings, so a partial one is never a problem.
        let progress = |found: &BTreeSet<String>| {
            let mut markets = listed.borrow().markets.clone();
            markets.extend(found.iter().cloned());
            info!("{} market(s) listed so far", found.len());
            listed.send_replace(Arc::new(Listing {
                markets,
                complete: false,
            }));
        };
        let result = tokio::select! {
            r = discover(&client, rest_base, &filters, progress) => r,
            _ = shutdown.wait_for(|&v| v) => return,
        };
        let next = match result {
            Ok(found) => {
                info!(
                    "{} market(s) listed in {:.1?}",
                    found.len(),
                    started.elapsed()
                );
                listed.send_replace(Arc::new(Listing {
                    markets: found,
                    complete: true,
                }));
                started + if every.is_zero() { NEVER } else { every }
            }
            Err(e) => {
                warn!("market discovery failed (retrying in {RETRY_AFTER:?}): {e:#}");
                Instant::now() + RETRY_AFTER
            }
        };
        let requested = tokio::select! {
            _ = tokio::time::sleep_until(next) => false,
            Some(()) = rediscover.recv() => true,
            _ = shutdown.wait_for(|&v| v) => return,
        };
        if requested {
            tokio::select! {
                _ = tokio::time::sleep_until(next.min(started + MIN_REDISCOVER_GAP)) => {}
                _ = shutdown.wait_for(|&v| v) => return,
            }
        }
        // Requests made up to now are answered by the run about to start.
        while rediscover.try_recv().is_ok() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_filters() {
        assert_eq!(
            Filter::parse("series:kxhighny"),
            Some(Filter::Series("KXHIGHNY".into()))
        );
        assert_eq!(
            Filter::parse("EVENT:KXHIGHNY-25AUG05"),
            Some(Filter::Event("KXHIGHNY-25AUG05".into()))
        );
        assert_eq!(Filter::parse("KXHIGHNY-25AUG05-B80"), None);
        assert_eq!(Filter::parse("series:"), None);
        assert_eq!(Filter::parse("market:X"), None);
    }

    #[test]
    fn matching() {
        let s = Filter::Series("KXHIGHNY".into());
        assert!(s.matches("KXHIGHNY-25AUG05-B80", None));
        assert!(!s.matches("KXHIGHNYC-25AUG05-B80", None)); // different series sharing a prefix
        assert!(!s.matches("KXHIGHNY", None));
        assert!(s.matches("ODDTICKER", Some("KXHIGHNY-25AUG05")));
        let e = Filter::Event("KXHIGHNY-25AUG05".into());
        assert!(e.matches("KXHIGHNY-25AUG05-B80", None));
        assert!(!e.matches("KXHIGHNY-25AUG06-B80", None));
        assert!(e.matches("ODDTICKER", Some("KXHIGHNY-25AUG05")));
        assert!(!e.matches("ODDTICKER", Some("KXHIGHNY-25AUG06")));
    }

    #[test]
    fn universe() {
        let all = Universe::filtered(vec![]);
        assert!(all.matches("KXHIGHNY-25AUG05-B80", None));
        assert!(!all.matches("KXMVECROSSCATEGORY-S2026TZ7QB64IL4VJQLJP-26ZEEQ7YZ", None));
        let series = Universe::filtered(vec![Filter::Series("KXHIGHNY".into())]);
        assert!(series.matches("KXHIGHNY-25AUG05-B80", None));
        assert!(!series.matches("KXHIGHCHI-25AUG05-B80", None));
    }

    #[test]
    fn shards_partition_markets() {
        let tickers: Vec<String> = (0..1000).map(|i| format!("KXTEST-26JAN01-T{i}")).collect();
        let shards: Vec<Shard> = (0..4).map(|index| Shard { index, count: 4 }).collect();
        let mut sizes = [0; 4];
        for t in &tickers {
            let owners: Vec<usize> = shards
                .iter()
                .filter(|s| s.contains(t))
                .map(|s| s.index)
                .collect();
            assert_eq!(owners.len(), 1, "{t}: {owners:?}");
            sizes[owners[0]] += 1;
        }
        assert!(sizes.iter().all(|&n| n > 200), "{sizes:?}"); // roughly even
        assert!(tickers.iter().all(|t| Shard::ALL.contains(t)));
    }
}
