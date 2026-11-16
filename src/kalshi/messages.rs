//! Typed Kalshi WebSocket messages (the subset we act on) and the commands we send.

use crate::kalshi::fixed::{Price, Qty};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Yes,
    No,
}

/// Full book for one market. Both sides are *bids*: `yes` = offers to buy YES, `no` = offers to buy NO.
#[derive(Debug, Clone, Deserialize)]
pub struct Snapshot {
    pub market_ticker: String,
    pub clock: u64,
    /// Absent when there are no YES bids.
    #[serde(default, rename = "yes_dollars_fp")]
    pub yes: Vec<(Price, Qty)>,
    /// Absent when there are no NO bids.
    #[serde(default, rename = "no_dollars_fp")]
    pub no: Vec<(Price, Qty)>,
}

/// Change in resting quantity at one price level.
#[derive(Debug, Clone, Deserialize)]
pub struct Delta {
    pub market_ticker: String,
    pub clock: u64,
    pub side: Side,
    #[serde(rename = "price_dollars")]
    pub price: Price,
    #[serde(rename = "delta_fp")]
    pub delta: Qty,
    pub ts_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ErrorMsg {
    pub code: i64,
    pub msg: String,
}

/// Confirmation of a `subscribe` command.
#[derive(Debug, Clone, Deserialize)]
pub struct Subscribed {
    pub channel: String,
    pub sid: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LifecycleMetadata {
    pub event_ticker: Option<String>,
}

/// `market_lifecycle_v2` notification. Sent for every market on the exchange (no ticker filtering).
#[derive(Debug, Clone, Deserialize)]
pub struct Lifecycle {
    pub market_ticker: String,
    /// created, activated, deactivated, close_date_updated, determined, settled, metadata_updated, ...
    pub event_type: String,
    /// Present on `created` events.
    pub additional_metadata: Option<LifecycleMetadata>,
}

/// Message body, discriminated by the `type` field.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Body {
    OrderbookSnapshot { msg: Snapshot },
    OrderbookDelta { msg: Delta },
    #[serde(rename = "market_lifecycle_v2")]
    MarketLifecycleV2 { msg: Lifecycle },
    /// Event created/updated. Delivered on the market lifecycle subscription, sharing its seq; not acted on.
    EventLifecycle {},
    Subscribed { msg: Subscribed },
    /// Success response to a command; the payload varies by command and isn't needed.
    Ok {},
    Error { msg: ErrorMsg },
    /// A message whose body didn't parse (e.g. a price out of range). Only produced by [`Frame::parse`],
    /// which keeps the message's `sid`/`seq` so sequence tracking isn't thrown off.
    #[serde(skip)]
    Malformed {
        /// The `type` field.
        kind: String,
        /// `msg.market_ticker`, if it could be read.
        market_ticker: Option<String>,
        error: String,
    },
    /// unsubscribed, and anything else we don't act on.
    #[serde(other)]
    Other,
}

const LIFECYCLE_TYPES: [&str; 2] = ["market_lifecycle_v2", "event_lifecycle"];

impl Body {
    /// Messages delivered on the market lifecycle subscription.
    pub fn is_lifecycle(&self) -> bool {
        match self {
            Body::MarketLifecycleV2 { .. } | Body::EventLifecycle {} => true,
            Body::Malformed { kind, .. } => LIFECYCLE_TYPES.contains(&kind.as_str()),
            _ => false,
        }
    }
}

/// Any server message. `sid`/`seq` live at the top level and are present on every message
/// belonging to a subscription (including `ok` responses), so they're parsed separately from the body.
#[derive(Debug, Clone, Deserialize)]
pub struct Frame {
    pub id: Option<u64>,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    pub sending_ts_ms: Option<u64>,
    #[serde(flatten)]
    pub body: Body,
}

impl Frame {
    /// Parse a server message. A message with a readable `type` but an unexpected body becomes
    /// [`Body::Malformed`], keeping its `id`/`sid`/`seq`; only messages without even that are errors.
    pub fn parse(raw: &Value) -> Result<Self, serde_json::Error> {
        #[derive(Deserialize)]
        struct Header {
            id: Option<u64>,
            sid: Option<u64>,
            seq: Option<u64>,
            sending_ts_ms: Option<u64>,
            #[serde(rename = "type")]
            kind: String,
            msg: Option<Value>,
        }
        let error = match Frame::deserialize(raw) {
            Ok(f) => return Ok(f),
            Err(e) => e,
        };
        let Ok(h) = Header::deserialize(raw) else { return Err(error) };
        let market_ticker = h.msg.as_ref().and_then(|m| m.get("market_ticker")?.as_str()).map(String::from);
        let body = Body::Malformed { kind: h.kind, market_ticker, error: error.to_string() };
        Ok(Frame { id: h.id, sid: h.sid, seq: h.seq, sending_ts_ms: h.sending_ts_ms, body })
    }
}

/// A command sent to the server: `{"id": .., "cmd": .., "params": {..}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Command {
    pub id: u64,
    #[serde(flatten)]
    pub kind: CommandKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "cmd", content = "params", rename_all = "snake_case")]
pub enum CommandKind {
    Subscribe {
        channels: Vec<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        market_tickers: Vec<String>,
    },
    UpdateSubscription {
        sid: u64,
        market_tickers: Vec<String>,
        action: Action,
    },
}

/// `update_subscription` actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    AddMarkets,
    DeleteMarkets,
    GetSnapshot,
}

impl Command {
    pub fn subscribe(id: u64, channel: &str, market_tickers: Vec<String>) -> Self {
        Self { id, kind: CommandKind::Subscribe { channels: vec![channel.to_string()], market_tickers } }
    }

    pub fn update(id: u64, sid: u64, action: Action, market_tickers: Vec<String>) -> Self {
        Self { id, kind: CommandKind::UpdateSubscription { sid, market_tickers, action } }
    }

    pub fn tickers(&self) -> &[String] {
        match &self.kind {
            CommandKind::Subscribe { market_tickers, .. } | CommandKind::UpdateSubscription { market_tickers, .. } => {
                market_tickers
            }
        }
    }

    pub fn action(&self) -> Option<Action> {
        match self.kind {
            CommandKind::UpdateSubscription { action, .. } => Some(action),
            CommandKind::Subscribe { .. } => None,
        }
    }

    /// Short name for stats and logs: "subscribe", "add_markets", "delete_markets", "get_snapshot".
    pub fn name(&self) -> &'static str {
        match self.action() {
            None => "subscribe",
            Some(Action::AddMarkets) => "add_markets",
            Some(Action::DeleteMarkets) => "delete_markets",
            Some(Action::GetSnapshot) => "get_snapshot",
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("commands always serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Price {
        s.parse().unwrap()
    }

    fn q(s: &str) -> Qty {
        s.parse().unwrap()
    }

    #[test]
    fn parse_snapshot() {
        let f: Frame = serde_json::from_str(
            r#"{"type":"orderbook_snapshot","sending_ts_ms":1,"sid":2,"seq":2,"msg":{
                "market_ticker":"FED","market_id":"x","clock":5,
                "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]]}}"#,
        )
        .unwrap();
        assert_eq!((f.sid, f.seq), (Some(2), Some(2)));
        let Body::OrderbookSnapshot { msg } = f.body else { panic!("{f:?}") };
        assert_eq!(msg.yes, vec![(p("0.08"), q("300")), (p("0.22"), q("333"))]);
        assert!(msg.no.is_empty());
    }

    #[test]
    fn parse_delta() {
        let f: Frame = serde_json::from_str(
            r#"{"type":"orderbook_delta","sid":2,"seq":3,"msg":{"market_ticker":"FED","market_id":"x",
                "clock":6,"price_dollars":"0.9600","delta_fp":"-54.00","side":"yes","ts_ms":7}}"#,
        )
        .unwrap();
        let Body::OrderbookDelta { msg } = f.body else { panic!("{f:?}") };
        assert_eq!((msg.side, msg.price, msg.delta, msg.ts_ms), (Side::Yes, p("0.96"), q("-54"), Some(7)));
    }

    #[test]
    fn parse_other_types() {
        let f: Frame = serde_json::from_str(r#"{"id":3,"sid":4,"seq":9,"type":"ok","msg":{"market_tickers":[]}}"#).unwrap();
        assert!(matches!(f.body, Body::Ok {}));
        assert_eq!((f.id, f.sid, f.seq), (Some(3), Some(4), Some(9)));
        let f: Frame = serde_json::from_str(r#"{"id":3,"type":"ok"}"#).unwrap();
        assert!(matches!(f.body, Body::Ok {}));
        let f: Frame = serde_json::from_str(r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":1}}"#).unwrap();
        assert!(matches!(f.body, Body::Subscribed { msg: Subscribed { sid: 1, .. } }));
        let f: Frame = serde_json::from_str(
            r#"{"msg":{"collateral_return_type":"MECNET","event_ticker":"E-1","exchange_index":3,"series_ticker":"E",
                "subtitle":"","title":"t"},"sending_ts_ms":1,"seq":71,"sid":1,"type":"event_lifecycle"}"#,
        )
        .unwrap();
        assert!(f.body.is_lifecycle() && matches!(f.body, Body::EventLifecycle {}));
        let f: Frame = serde_json::from_str(r#"{"sid":1,"seq":2,"type":"unsubscribed"}"#).unwrap();
        assert!(matches!(f.body, Body::Other));
        let f: Frame = serde_json::from_str(r#"{"id":1,"type":"error","msg":{"code":7,"msg":"Unknown sid"}}"#).unwrap();
        assert!(matches!(f.body, Body::Error { msg: ErrorMsg { code: 7, .. } }));
    }

    #[test]
    fn parse_lifecycle() {
        let f: Frame = serde_json::from_str(
            r#"{"type":"market_lifecycle_v2","sid":13,"seq":3,"msg":{"market_ticker":"INXD-23SEP14-B4487",
                "open_ts":1694635200,"event_type":"created","additional_metadata":{"event_ticker":"INXD-23SEP14","floor_strike":4487}}}"#,
        )
        .unwrap();
        let Body::MarketLifecycleV2 { msg } = f.body else { panic!("{f:?}") };
        assert_eq!((msg.market_ticker.as_str(), msg.event_type.as_str()), ("INXD-23SEP14-B4487", "created"));
        assert_eq!(msg.additional_metadata.unwrap().event_ticker.as_deref(), Some("INXD-23SEP14"));
        let f: Frame = serde_json::from_str(
            r#"{"type":"market_lifecycle_v2","sid":13,"seq":4,"msg":{"market_ticker":"X","event_type":"settled","settled_ts":1}}"#,
        )
        .unwrap();
        assert!(matches!(f.body, Body::MarketLifecycleV2 { msg: Lifecycle { additional_metadata: None, .. } }));
    }

    #[test]
    fn command_wire_format() {
        let c = Command::subscribe(1, "market_lifecycle_v2", vec![]);
        assert_eq!(c.to_json(), r#"{"id":1,"cmd":"subscribe","params":{"channels":["market_lifecycle_v2"]}}"#);
        let c = Command::update(5, 2, Action::GetSnapshot, vec!["M".into()]);
        assert_eq!(
            c.to_json(),
            r#"{"id":5,"cmd":"update_subscription","params":{"sid":2,"market_tickers":["M"],"action":"get_snapshot"}}"#
        );
        let sub = Command::subscribe(2, "orderbook_delta", vec!["A".into()]);
        assert_eq!(sub.to_json(), r#"{"id":2,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["A"]}}"#);
        assert_eq!((sub.name(), sub.tickers()), ("subscribe", &["A".to_string()][..]));
    }

    #[test]
    fn malformed_keeps_header() {
        let bad = serde_json::json!({"type": "orderbook_delta", "sid": 2, "seq": 3, "msg": {"market_ticker": "FED", "clock": 6,
                                     "price_dollars": "1.5000", "delta_fp": "1.00", "side": "yes"}});
        let f = Frame::parse(&bad).unwrap();
        assert_eq!((f.sid, f.seq), (Some(2), Some(3)));
        let Body::Malformed { kind, market_ticker, error } = f.body else { panic!("{f:?}") };
        assert_eq!((kind.as_str(), market_ticker.as_deref()), ("orderbook_delta", Some("FED")));
        assert!(error.contains("invalid price"), "{error}");
        assert!(Frame::parse(&serde_json::json!({"no_type": 1})).is_err());
        let f = Frame::parse(&serde_json::json!({"type": "market_lifecycle_v2", "sid": 1, "seq": 1, "msg": {}})).unwrap();
        assert!(f.body.is_lifecycle());
    }

    #[test]
    fn snapshot_keeps_non_positive_levels_for_the_book_to_drop() {
        let f: Frame = serde_json::from_str(
            r#"{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{"market_ticker":"M","clock":5,"no_dollars_fp":[["0.1000","0.00"]]}}"#,
        )
        .unwrap();
        let Body::OrderbookSnapshot { msg } = f.body else { panic!("{f:?}") };
        assert_eq!(msg.no, vec![(p("0.1"), Qty::ZERO)]);
    }
}
