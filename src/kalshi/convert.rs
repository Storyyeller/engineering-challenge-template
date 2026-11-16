//! Kalshi books in the collector's terms.
//!
//! Kalshi books hold bids on both sides of a binary market: bids to buy YES, and bids to buy NO. A NO
//! bid at price p is equivalent to a YES ask at $1 - p, so the published book is the YES book: bids
//! are YES bids, and asks are NO bids at the complementary price.

use chrono::{DateTime, Utc};

use crate::Side;
use crate::collector::{Event, Level};
use crate::kalshi::book::Book;
use crate::kalshi::fixed::{Price, Qty};
use crate::kalshi::messages;

pub fn from_ms(ms: u64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms.try_into().ok()?)
}

/// A YES book level for a bid on `side` at `price`.
fn yes_level(side: messages::Side, price: Price) -> (Side, i64) {
    match side {
        messages::Side::Yes => (Side::Buy, price.micros()),
        messages::Side::No => (Side::Sell, price.complement().micros()),
    }
}

pub fn book_event(symbol: String, book: &Book, exchange_ts: Option<DateTime<Utc>>) -> Event {
    // Best first: the highest YES bid, and the lowest YES ask (= the highest NO bid).
    let bids = book
        .yes
        .iter()
        .rev()
        .map(|(p, q)| Level {
            price: p.micros(),
            qty: q.micros(),
        })
        .collect();
    let asks = book
        .no
        .iter()
        .rev()
        .map(|(p, q)| Level {
            price: p.complement().micros(),
            qty: q.micros(),
        })
        .collect();
    Event::Book {
        symbol,
        bids,
        asks,
        clock: book.clock,
        exchange_ts,
    }
}

pub fn level_event(
    symbol: String,
    side: messages::Side,
    price: Price,
    qty: Qty,
    clock: u64,
    exchange_ts: DateTime<Utc>,
) -> Event {
    let (side, price) = yes_level(side, price);
    Event::Level {
        symbol,
        side,
        price,
        qty: qty.micros(),
        clock,
        exchange_ts,
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
    fn yes_book() {
        let mut book = Book::default();
        book.yes.insert(p("0.40"), q("10"));
        book.yes.insert(p("0.42"), q("1.5"));
        book.no.insert(p("0.55"), q("5"));
        book.no.insert(p("0.30"), q("2"));
        book.clock = 7;
        let t = from_ms(1_700_000_000_000).unwrap();
        let Event::Book {
            bids, asks, clock, ..
        } = book_event("M".into(), &book, None)
        else {
            panic!()
        };
        let prices = |l: &[Level]| l.iter().map(|l| (l.price, l.qty)).collect::<Vec<_>>();
        assert_eq!(
            prices(&bids),
            vec![(420_000, 1_500_000), (400_000, 10_000_000)]
        );
        assert_eq!(
            prices(&asks),
            vec![(450_000, 5_000_000), (700_000, 2_000_000)]
        );
        assert_eq!(clock, 7);

        let e = level_event("M".into(), messages::Side::No, p("0.55"), q("0"), 8, t);
        assert!(matches!(
            e,
            Event::Level {
                side: Side::Sell,
                price: 450_000,
                qty: 0,
                clock: 8,
                ..
            }
        ));
        let e = level_event("M".into(), messages::Side::Yes, p("0.42"), q("3"), 9, t);
        assert!(matches!(
            e,
            Event::Level {
                side: Side::Buy,
                price: 420_000,
                qty: 3_000_000,
                ..
            }
        ));
    }
}
