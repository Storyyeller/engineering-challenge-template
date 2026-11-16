//! Signing websocket handshakes with a Kalshi API key.

use anyhow::{Context, Result, bail};
use base64::Engine;
use rsa::PrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey as RsaSigningKey;
use rsa::sha2::Sha256;
use rsa::signature::{SignatureEncoding, Signer};

pub const WS_PATH: &str = "/trade-api/ws/v2";

/// Kalshi API keys are either Ed25519 (PKCS#8 PEM) or RSA (PKCS#1 or PKCS#8 PEM).
pub enum AuthKey {
    Ed25519(ed25519_dalek::SigningKey),
    Rsa(RsaSigningKey<Sha256>),
}

impl AuthKey {
    pub fn load(path: &str) -> Result<Self> {
        let pem =
            std::fs::read_to_string(path).with_context(|| format!("reading key file {path}"))?;
        if let Ok(k) = ed25519_dalek::SigningKey::from_pkcs8_pem(&pem) {
            return Ok(Self::Ed25519(k));
        }
        let rsa = PrivateKey::from_pkcs1_pem(&pem)
            .or_else(|_| PrivateKey::from_pkcs8_pem(&pem))
            .context("parsing private key (expected Ed25519 PKCS#8, or RSA PKCS#1/PKCS#8 PEM)")?;
        // RSA-PSS with SHA-256, salt length = digest length (matches Kalshi's reference code).
        Ok(Self::Rsa(RsaSigningKey::new(rsa)))
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Ed25519(_) => "Ed25519",
            Self::Rsa(_) => "RSA",
        }
    }

    /// Sign `timestamp + "GET" + path`, base64-encoded.
    pub fn sign(&self, timestamp_ms: &str) -> Result<String> {
        let msg = format!("{timestamp_ms}GET{WS_PATH}");
        let bytes = match self {
            Self::Ed25519(k) => k.try_sign(msg.as_bytes())?.to_bytes().to_vec(),
            Self::Rsa(k) => k.try_sign(msg.as_bytes())?.to_vec(),
        };
        Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
    }
}

/// Checks that a key ID looks like the UUID Kalshi shows (8-4-4-4-12 hex digits), since anything else
/// only fails later as an unexplained 401.
pub fn check_key_id(id: &str) -> Result<()> {
    let groups: Vec<&str> = id.split('-').collect();
    let is_uuid = groups.iter().map(|g| g.len()).eq([8, 4, 4, 4, 12])
        && groups
            .iter()
            .all(|g| g.bytes().all(|b| b.is_ascii_hexdigit()));
    if is_uuid {
        return Ok(());
    }
    bail!(
        "KALSHI_API_KEY_ID should be a UUID like xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, got {id:?}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_id_must_be_a_uuid() {
        assert!(check_key_id("a1b2c3d4-0000-4abc-8DEF-0123456789ab").is_ok());
        for bad in [
            "",
            "keyid.txt",
            "a1b2c3d4-0000-4abc-8def-0123456789a",
            "a1b2c3d4-0000-4abc-8def-0123456789ab\n",
            "g1b2c3d4-0000-4abc-8def-0123456789ab",
            "a1b2c3d40000-4abc-8def-0123456789ab-",
        ] {
            assert!(check_key_id(bad).is_err(), "{bad:?}");
        }
    }
}
