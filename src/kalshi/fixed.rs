//! Exact fixed-point prices and quantities. Kalshi sends both as decimal strings ("0.2800", "-54.00");
//! these types parse them without floating point and keep the scale in one place.

use serde::{Deserialize, Deserializer};
use std::fmt;
use std::ops::{Add, AddAssign};
use std::str::FromStr;

/// Parse a decimal string like "-12.5" into an integer scaled by 10^decimals (-1250 for 2 decimals).
/// Returns None on malformed input or if the string has more than `decimals` fractional digits.
fn parse_fixed(s: &str, decimals: u32) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    let all_digits = |x: &str| x.bytes().all(|b| b.is_ascii_digit());
    if (int.is_empty() && frac.is_empty())
        || !all_digits(int)
        || !all_digits(frac)
        || frac.len() > decimals as usize
    {
        return None;
    }
    let int_val: i64 = if int.is_empty() { 0 } else { int.parse().ok()? };
    let frac_val: i64 = if frac.is_empty() {
        0
    } else {
        frac.parse().ok()?
    };
    let scaled = int_val
        .checked_mul(10i64.pow(decimals))?
        .checked_add(frac_val * 10i64.pow(decimals - frac.len() as u32))?;
    Some(if neg { -scaled } else { scaled })
}

/// Format `units / 10^decimals` with all decimals, e.g. (-61, 2) -> "-0.61".
fn fmt_fixed(f: &mut fmt::Formatter<'_>, units: i64, decimals: u32) -> fmt::Result {
    let scale = 10u64.pow(decimals);
    let sign = if units < 0 { "-" } else { "" };
    let abs = units.unsigned_abs();
    let s = format!(
        "{sign}{}.{:0width$}",
        abs / scale,
        abs % scale,
        width = decimals as usize
    );
    f.pad(&s)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    what: &'static str,
    input: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid {} {:?}", self.what, self.input)
    }
}

impl std::error::Error for ParseError {}

/// A price between $0 and $1 inclusive, exact to 1/10,000 dollar (Kalshi sends 4 decimals).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(u32);

impl Price {
    const DECIMALS: u32 = 4;
    const SCALE: u32 = 10u32.pow(Self::DECIMALS);
    /// $1.00, the payout of one contract.
    pub const ONE_DOLLAR: Price = Price(Self::SCALE);

    /// The other side's price: a NO bid at p is a YES ask at $1 - p.
    pub fn complement(self) -> Price {
        Price(Self::SCALE - self.0)
    }

    /// In millionths of a dollar, the output format's fixed point.
    pub fn micros(self) -> i64 {
        i64::from(self.0) * (MICROS / i64::from(Self::SCALE))
    }
}

/// Fixed-point scale of the output format: 1_000_000 = $1, or one contract.
const MICROS: i64 = 1_000_000;

impl FromStr for Price {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, ParseError> {
        parse_fixed(s, Self::DECIMALS)
            .and_then(|v| u32::try_from(v).ok())
            .filter(|&v| v <= Self::SCALE)
            .map(Price)
            .ok_or_else(|| ParseError {
                what: "price",
                input: s.to_string(),
            })
    }
}

impl fmt::Display for Price {
    /// Dollars with all 4 decimals, e.g. "0.2800".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_fixed(f, self.0.into(), Self::DECIMALS)
    }
}

/// A (possibly negative) number of contracts, exact to 1/100 contract (Kalshi sends 2 decimals).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(i64);

impl Qty {
    const DECIMALS: u32 = 2;
    pub const ZERO: Qty = Qty(0);

    /// In millionths of a contract, the output format's fixed point.
    pub fn micros(self) -> i64 {
        self.0 * (MICROS / 10i64.pow(Self::DECIMALS))
    }
}

impl FromStr for Qty {
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, ParseError> {
        parse_fixed(s, Self::DECIMALS)
            .map(Qty)
            .ok_or_else(|| ParseError {
                what: "quantity",
                input: s.to_string(),
            })
    }
}

impl fmt::Display for Qty {
    /// With 2 decimals, e.g. "505.00".
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_fixed(f, self.0, Self::DECIMALS)
    }
}

impl Add for Qty {
    type Output = Qty;
    fn add(self, rhs: Qty) -> Qty {
        Qty(self.0 + rhs.0)
    }
}

impl AddAssign for Qty {
    fn add_assign(&mut self, rhs: Qty) {
        self.0 += rhs.0;
    }
}

/// Both deserialize from the decimal strings Kalshi sends.
macro_rules! deserialize_from_str {
    ($t:ty) => {
        impl<'de> Deserialize<'de> for $t {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                s.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}
deserialize_from_str!(Price);
deserialize_from_str!(Qty);

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
    fn fixed_parsing() {
        assert_eq!(parse_fixed("0.2800", 4), Some(2800));
        assert_eq!(parse_fixed("1.0000", 4), Some(10000));
        assert_eq!(parse_fixed("505.00", 2), Some(50500));
        assert_eq!(parse_fixed("-336.66", 2), Some(-33666));
        assert_eq!(parse_fixed("0.61", 2), Some(61));
        assert_eq!(parse_fixed("3", 2), Some(300));
        assert_eq!(parse_fixed("2.5", 2), Some(250));
        assert_eq!(parse_fixed(".5", 2), Some(50));
        assert_eq!(parse_fixed("0.001", 2), None);
        assert_eq!(parse_fixed("", 2), None);
        assert_eq!(parse_fixed("-", 2), None);
        assert_eq!(parse_fixed("1e5", 2), None);
        assert_eq!(parse_fixed("+1", 2), None);
    }

    #[test]
    fn price() {
        assert_eq!(p("0.28"), Price(2800));
        assert_eq!(p("1"), Price::ONE_DOLLAR);
        assert_eq!(p("0.2800").to_string(), "0.2800");
        assert_eq!(Price::ONE_DOLLAR.to_string(), "1.0000");
        assert_eq!(p("0.55").complement(), p("0.45"));
        assert!(p("0.3") < p("0.31"));
        for bad in ["1.0001", "-0.01", "0.00001", "x"] {
            assert_eq!(
                bad.parse::<Price>().unwrap_err().to_string(),
                format!("invalid price {bad:?}")
            );
        }
    }

    #[test]
    fn qty() {
        assert_eq!(q("505"), Qty(50500));
        assert_eq!(q("505.00").to_string(), "505.00");
        assert_eq!(q("-0.61").to_string(), "-0.61");
        assert_eq!(q("1.5") + q("-2"), q("-0.5"));
        assert!("0.001".parse::<Qty>().is_err());
    }

    #[test]
    fn micros() {
        assert_eq!(p("0.2800").micros(), 280_000);
        assert_eq!(Price::ONE_DOLLAR.micros(), 1_000_000);
        assert_eq!(q("505.00").micros(), 505_000_000);
        assert_eq!(q("0.61").micros(), 610_000);
    }

    #[test]
    fn deserialize() {
        let v: Vec<(Price, Qty)> = serde_json::from_str(r#"[["0.0800","300.00"]]"#).unwrap();
        assert_eq!(v, vec![(p("0.08"), q("300"))]);
        assert!(serde_json::from_str::<Price>("0.08").is_err()); // must be a string
    }
}
