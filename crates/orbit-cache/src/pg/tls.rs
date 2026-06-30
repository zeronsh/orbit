//! TLS for Postgres connections (managed PG such as Railway/Neon/Supabase).
//!
//! Two connection paths need TLS: the regular `tokio-postgres` SQL connections
//! (initial sync, slot/publication setup, write-through, CVR, change-log) and
//! Orbit's own raw logical-replication socket ([`super::proto`]). Both build their
//! rustls config from here, so a single [`PgTlsMode`] governs the whole engine.
//!
//! Modes follow libpq's `sslmode`:
//! * `disable`     — plaintext (the default; local trust-auth dev + private nets).
//! * `require`     — encrypt, but DON'T verify the server certificate.
//! * `verify-full` — encrypt and verify the cert chain against the Mozilla roots.
//!
//! `prefer`/`allow` map to `require` (Orbit does not auto-fall-back to plaintext;
//! use `disable` explicitly for that). `verify-ca` maps to `verify-full`.

use std::sync::Arc;

use anyhow::{Context, Result};

/// How to secure the Postgres connection. See the module docs for libpq mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PgTlsMode {
    /// Plaintext — no TLS (default).
    #[default]
    Disable,
    /// Encrypt the connection but do not verify the server certificate.
    Require,
    /// Encrypt and verify the certificate chain (Mozilla webpki roots).
    VerifyFull,
}

impl PgTlsMode {
    /// Parse an `sslmode`-style string (case-insensitive). Unknown non-empty
    /// values default to `require` (encrypt) rather than silently disabling TLS.
    pub fn parse(s: &str) -> PgTlsMode {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "disable" | "off" | "false" | "no" | "0" => PgTlsMode::Disable,
            "require" | "on" | "true" | "yes" | "1" | "prefer" | "allow" => PgTlsMode::Require,
            "verify-ca" | "verify-full" | "verify" => PgTlsMode::VerifyFull,
            other => {
                eprintln!("orbit: unknown sslmode {other:?}; using `require`");
                PgTlsMode::Require
            }
        }
    }

    /// Read the mode from `ORBIT_PG_SSLMODE` (or `PGSSLMODE`); `disable` if unset.
    pub fn from_env() -> PgTlsMode {
        match std::env::var("ORBIT_PG_SSLMODE").or_else(|_| std::env::var("PGSSLMODE")) {
            Ok(v) => PgTlsMode::parse(&v),
            Err(_) => PgTlsMode::Disable,
        }
    }

    pub fn is_enabled(self) -> bool {
        self != PgTlsMode::Disable
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Build a rustls client config for `mode` (must not be `Disable`).
pub fn client_config(mode: PgTlsMode) -> Result<rustls::ClientConfig> {
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .context("rustls protocol versions")?;
    let cfg = match mode {
        PgTlsMode::VerifyFull => {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            builder.with_root_certificates(roots).with_no_client_auth()
        }
        PgTlsMode::Require => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider())))
            .with_no_client_auth(),
        PgTlsMode::Disable => anyhow::bail!("client_config called with PgTlsMode::Disable"),
    };
    Ok(cfg)
}

/// A `tokio-rustls` connector for the raw replication socket ([`super::proto`]).
pub fn connector(mode: PgTlsMode) -> Result<tokio_rustls::TlsConnector> {
    Ok(tokio_rustls::TlsConnector::from(Arc::new(client_config(mode)?)))
}

/// Resolve a host string to a rustls `ServerName` (IP literal or DNS name).
pub fn server_name(host: &str) -> Result<rustls::pki_types::ServerName<'static>> {
    rustls::pki_types::ServerName::try_from(host.to_owned())
        .with_context(|| format!("invalid TLS server name {host:?}"))
}

/// The connection driver future returned by [`connect`]; the caller spawns it
/// (`tokio::spawn` or `spawn_local`) to drive the connection. Boxed so the same
/// type covers both the plaintext and TLS connections.
pub type PgDriver = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// Connect a `tokio-postgres` client over `mode`. Returns the client plus a
/// driver future the caller must spawn. The connection string carries the
/// password (see [`ServerConfig::conn_str`](crate::ServerConfig)).
pub async fn connect(conn_str: &str, mode: PgTlsMode) -> Result<(tokio_postgres::Client, PgDriver)> {
    if mode.is_enabled() {
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(client_config(mode)?);
        let (client, conn) = tokio_postgres::connect(conn_str, tls).await?;
        let driver: PgDriver = Box::pin(async move {
            if let Err(e) = conn.await {
                eprintln!("postgres connection error: {e}");
            }
        });
        Ok((client, driver))
    } else {
        let (client, conn) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
        let driver: PgDriver = Box::pin(async move {
            if let Err(e) = conn.await {
                eprintln!("postgres connection error: {e}");
            }
        });
        Ok((client, driver))
    }
}

/// A certificate verifier that accepts any server cert — used for `require`
/// (encrypt without verifying), matching libpq's `sslmode=require`. Signatures
/// are still checked via the crypto provider.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sslmode() {
        assert_eq!(PgTlsMode::parse(""), PgTlsMode::Disable);
        assert_eq!(PgTlsMode::parse("disable"), PgTlsMode::Disable);
        assert_eq!(PgTlsMode::parse("require"), PgTlsMode::Require);
        assert_eq!(PgTlsMode::parse("PREFER"), PgTlsMode::Require);
        assert_eq!(PgTlsMode::parse("verify-full"), PgTlsMode::VerifyFull);
        assert_eq!(PgTlsMode::parse("verify-ca"), PgTlsMode::VerifyFull);
        assert!(PgTlsMode::Require.is_enabled());
        assert!(!PgTlsMode::Disable.is_enabled());
    }

    #[test]
    fn builds_rustls_configs() {
        assert!(client_config(PgTlsMode::Require).is_ok());
        assert!(client_config(PgTlsMode::VerifyFull).is_ok());
        assert!(client_config(PgTlsMode::Disable).is_err());
    }
}
