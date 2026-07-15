//! Minimal PostgreSQL wire-protocol client for *logical replication streaming*.
//!
//! `tokio-postgres` (0.7.x) has no `CopyBoth`/replication support and there is
//! no off-the-shelf crate, so we speak the protocol directly. This handles only
//! what streaming logical replication needs: a startup with `replication=
//! database`, `trust` auth, issuing `START_REPLICATION`, reading `CopyData`
//! frames, and sending Standby Status Updates. Normal SQL (DDL, slot creation,
//! initial snapshot) goes through `tokio-postgres` on a separate connection.
//!
//! Reference: PostgreSQL "Frontend/Backend Protocol" and "Logical Streaming
//! Replication Protocol".

use std::pin::Pin;
use std::task::{Context as TaskCtx, Poll};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::pg::tls::{self, PgTlsMode};

type HmacSha256 = Hmac<Sha256>;

const PROTOCOL_VERSION: i32 = 196608; // 3.0
/// The magic SSLRequest code (see the PostgreSQL protocol): asks the server to
/// negotiate TLS before startup.
const SSL_REQUEST_CODE: i32 = 80877103;

/// A backend protocol message: a type byte plus its payload.
pub struct BackendMessage {
    pub tag: u8,
    pub body: Vec<u8>,
}

/// A TCP stream, optionally wrapped in TLS after the SSLRequest handshake. The
/// rest of the protocol code is identical either way (it just sees a stream).
enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// A raw replication connection.
pub struct RawConn {
    stream: BufReader<MaybeTlsStream>,
}

impl RawConn {
    /// Connect and perform startup as a logical-replication connection
    /// (`replication=database`). Supports `trust`, cleartext, and SCRAM-SHA-256
    /// auth. When `tls_mode` is enabled the connection negotiates TLS via the
    /// PostgreSQL SSLRequest handshake before startup. `password` falls back to
    /// `ORBIT_PG_PASSWORD` / `PGPASSWORD` when `None` (local trust auth ignores it).
    pub async fn connect_replication(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        tls_mode: PgTlsMode,
    ) -> Result<RawConn> {
        let mut tcp = TcpStream::connect((host, port)).await?;
        tcp.set_nodelay(true).ok();

        let stream = if tls_mode.is_enabled() {
            // SSLRequest: a startup-shaped packet (length=8, code=80877103). The
            // server replies with a single byte: 'S' = proceed with TLS, 'N' = no.
            let mut req = Vec::with_capacity(8);
            req.extend_from_slice(&8i32.to_be_bytes());
            req.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
            tcp.write_all(&req).await?;
            tcp.flush().await?;
            let mut resp = [0u8; 1];
            tcp.read_exact(&mut resp).await?;
            match resp[0] {
                b'S' => {
                    let connector = tls::connector(tls_mode)?;
                    let name = tls::server_name(host)?;
                    let tls_stream = connector
                        .connect(name, tcp)
                        .await
                        .context("postgres replication TLS handshake")?;
                    MaybeTlsStream::Tls(Box::new(tls_stream))
                }
                b'N' => bail!("postgres server refused SSL but sslmode requires TLS"),
                other => bail!("unexpected response to SSLRequest: {other:#x}"),
            }
        } else {
            MaybeTlsStream::Plain(tcp)
        };

        let mut conn = RawConn {
            stream: BufReader::new(stream),
        };
        conn.send_startup(&[
            ("user", user),
            ("database", database),
            ("replication", "database"),
            ("client_encoding", "UTF8"),
        ])
        .await?;
        let password = password
            .map(str::to_owned)
            .or_else(|| {
                std::env::var("ORBIT_PG_PASSWORD")
                    .or_else(|_| std::env::var("PGPASSWORD"))
                    .ok()
            })
            .unwrap_or_default();
        conn.finish_startup(&password).await?;
        Ok(conn)
    }

    async fn send_startup(&mut self, params: &[(&str, &str)]) -> Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        for (k, v) in params {
            payload.extend_from_slice(k.as_bytes());
            payload.push(0);
            payload.extend_from_slice(v.as_bytes());
            payload.push(0);
        }
        payload.push(0); // terminator
        let len = (payload.len() + 4) as i32;
        self.stream.get_mut().write_all(&len.to_be_bytes()).await?;
        self.stream.get_mut().write_all(&payload).await?;
        self.stream.get_mut().flush().await?;
        Ok(())
    }

    /// Read backend messages until ReadyForQuery, completing whatever auth the
    /// server asks for (trust / cleartext / SCRAM-SHA-256).
    async fn finish_startup(&mut self, password: &str) -> Result<()> {
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'R' => {
                    // Authentication: int32 auth type.
                    if msg.body.len() < 4 {
                        bail!("short authentication message");
                    }
                    if msg.body.len() < 4 {
            bail!("short authentication message");
        }
        let code = i32::from_be_bytes(msg.body[0..4].try_into().unwrap());
                    match code {
                        0 => {}                  // AuthenticationOk
                        3 => self.send_password_message(password).await?, // cleartext
                        10 => self.scram_sha256(password).await?,         // SASL
                        _ => bail!(
                            "auth method {code} not supported (use trust, cleartext, or SCRAM-SHA-256)"
                        ),
                    }
                }
                b'E' => bail!("postgres error during startup: {}", parse_error(&msg.body)),
                b'Z' => return Ok(()), // ReadyForQuery
                // ParameterStatus 'S', BackendKeyData 'K', NoticeResponse 'N': ignore.
                _ => {}
            }
        }
    }

    /// Send a PasswordMessage ('p') with raw bytes (cleartext password or a SASL
    /// response body).
    async fn send_p_message(&mut self, body: &[u8]) -> Result<()> {
        let len = (body.len() + 4) as i32;
        let w = self.stream.get_mut();
        w.write_all(b"p").await?;
        w.write_all(&len.to_be_bytes()).await?;
        w.write_all(body).await?;
        w.flush().await?;
        Ok(())
    }

    async fn send_password_message(&mut self, password: &str) -> Result<()> {
        let mut body = password.as_bytes().to_vec();
        body.push(0);
        self.send_p_message(&body).await
    }

    /// Perform the SCRAM-SHA-256 SASL exchange (PostgreSQL auth method 10).
    async fn scram_sha256(&mut self, password: &str) -> Result<()> {
        let b64 = base64::engine::general_purpose::STANDARD;

        // Client first message. PostgreSQL ignores the SCRAM username (it uses the
        // startup `user`), so we send an empty one.
        let nonce: String = {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            (0..24).map(|_| {
                // printable, comma-free ASCII per the SCRAM nonce rules
                let c = rng.gen_range(0x30u8..0x7f);
                (if c == b',' { b'~' } else { c }) as char
            }).collect()
        };
        let client_first_bare = format!("n=,r={nonce}");
        // SASLInitialResponse: mechanism CString + int32 len + initial-response.
        let initial = format!("n,,{client_first_bare}");
        let mut body = Vec::new();
        body.extend_from_slice(b"SCRAM-SHA-256\0");
        body.extend_from_slice(&(initial.len() as i32).to_be_bytes());
        body.extend_from_slice(initial.as_bytes());
        self.send_p_message(&body).await?;

        // Server first message (AuthenticationSASLContinue, code 11).
        let msg = self.read_message().await?;
        if msg.tag == b'E' {
            bail!("SCRAM error: {}", parse_error(&msg.body));
        }
        let code = i32::from_be_bytes(msg.body[0..4].try_into().unwrap());
        anyhow::ensure!(code == 11, "expected SASLContinue, got auth code {code}");
        let server_first = std::str::from_utf8(&msg.body[4..]).context("SASL server-first utf8")?.to_string();
        let (mut r, mut s, mut i) = (String::new(), String::new(), 0u32);
        for part in server_first.split(',') {
            if let Some(v) = part.strip_prefix("r=") { r = v.to_string(); }
            else if let Some(v) = part.strip_prefix("s=") { s = v.to_string(); }
            else if let Some(v) = part.strip_prefix("i=") { i = v.parse().unwrap_or(4096); }
        }
        anyhow::ensure!(r.starts_with(&nonce), "SCRAM server nonce mismatch");
        let salt = b64.decode(s.as_bytes()).context("SCRAM salt b64")?;

        // Salted password + keys.
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, i, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = Sha256::digest(client_key);
        let client_final_bare = format!("c=biws,r={r}");
        let auth_message = format!("{client_first_bare},{server_first},{client_final_bare}");
        let client_sig = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(client_sig.iter()).map(|(a, b)| a ^ b).collect();
        let client_final = format!("{client_final_bare},p={}", b64.encode(proof));
        self.send_p_message(client_final.as_bytes()).await?;

        // Server final (AuthenticationSASLFinal, code 12) — we don't verify the
        // server signature; the subsequent AuthenticationOk/error is authoritative.
        let msg = self.read_message().await?;
        if msg.tag == b'E' {
            bail!("SCRAM auth failed: {}", parse_error(&msg.body));
        }
        Ok(())
    }

    /// Read one full backend message (1-byte tag + i32 length + payload).
    pub async fn read_message(&mut self) -> Result<BackendMessage> {
        let mut tag = [0u8; 1];
        self.stream.read_exact(&mut tag).await?;
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = i32::from_be_bytes(len_buf) as usize;
        if len < 4 {
            bail!("invalid message length {len}");
        }
        // Sanity cap: a corrupted / desynced frame length must fail cleanly, not
        // attempt a multi-GB allocation. 1 GiB comfortably exceeds any real
        // logical-replication message (rows are bounded by PG's 1 GB field cap).
        const MAX_MESSAGE: usize = 1 << 30;
        if len - 4 > MAX_MESSAGE {
            bail!("implausible message length {len} (corrupt or desynced stream)");
        }
        let mut body = vec![0u8; len - 4];
        self.stream.read_exact(&mut body).await?;
        Ok(BackendMessage { tag: tag[0], body })
    }

    /// Send a simple Query message.
    pub async fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut payload = Vec::with_capacity(sql.len() + 1);
        payload.extend_from_slice(sql.as_bytes());
        payload.push(0);
        let len = (payload.len() + 4) as i32;
        let w = self.stream.get_mut();
        w.write_all(b"Q").await?;
        w.write_all(&len.to_be_bytes()).await?;
        w.write_all(&payload).await?;
        w.flush().await?;
        Ok(())
    }

    /// `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput (SNAPSHOT 'export')`:
    /// creates the slot AND exports the snapshot it was created at. A SQL
    /// session that runs `SET TRANSACTION SNAPSHOT '<name>'` then reads the
    /// database EXACTLY at the slot's consistent point — the initial sync and
    /// the stream tile perfectly with no overlap window. The snapshot stays
    /// valid only while THIS connection lives; keep it open until the sync
    /// commits. Returns `(consistent_point_lsn, snapshot_name)`.
    pub async fn create_replication_slot_exported(
        &mut self,
        slot: &str,
    ) -> Result<(u64, String)> {
        self.send_query(&format!(
            "CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput (SNAPSHOT 'export')"
        ))
        .await?;
        let mut fields: Vec<Option<String>> = Vec::new();
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'D' => {
                    // DataRow: u16 col count, then per column i32 len + bytes.
                    let b = &msg.body;
                    if b.len() < 2 {
                        bail!("short DataRow");
                    }
                    let n = u16::from_be_bytes([b[0], b[1]]) as usize;
                    let mut off = 2usize;
                    for _ in 0..n {
                        if off + 4 > b.len() {
                            bail!("short DataRow column header");
                        }
                        let len = i32::from_be_bytes(b[off..off + 4].try_into().unwrap());
                        off += 4;
                        if len < 0 {
                            fields.push(None);
                        } else {
                            let len = len as usize;
                            if off + len > b.len() {
                                bail!("short DataRow column");
                            }
                            fields.push(Some(
                                String::from_utf8_lossy(&b[off..off + len]).into_owned(),
                            ));
                            off += len;
                        }
                    }
                }
                b'E' => bail!("CREATE_REPLICATION_SLOT failed: {}", parse_error(&msg.body)),
                b'Z' => break, // ReadyForQuery
                _ => {}        // RowDescription / CommandComplete / ParameterStatus
            }
        }
        // Columns: slot_name, consistent_point, snapshot_name, output_plugin.
        let lsn = fields
            .get(1)
            .and_then(|f| f.as_deref())
            .ok_or_else(|| anyhow::anyhow!("no consistent_point in CREATE_REPLICATION_SLOT reply"))?;
        let snapshot = fields
            .get(2)
            .and_then(|f| f.clone())
            .ok_or_else(|| anyhow::anyhow!("no snapshot_name in CREATE_REPLICATION_SLOT reply"))?;
        Ok((parse_lsn(lsn)?, snapshot))
    }

    /// Begin `START_REPLICATION` on `slot`/`publication` from `start_lsn`.
    /// After this the server enters CopyBoth mode and streams `CopyData`.
    pub async fn start_replication(
        &mut self,
        slot: &str,
        publication: &str,
        start_lsn: u64,
    ) -> Result<()> {
        let lsn = format_lsn(start_lsn);
        let sql = format!(
            "START_REPLICATION SLOT {slot} LOGICAL {lsn} \
             (proto_version '1', publication_names '{publication}')"
        );
        self.send_query(&sql).await?;
        // Expect CopyBothResponse ('W'); surface errors instead.
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'W' => return Ok(()),
                b'E' => bail!("START_REPLICATION failed: {}", parse_error(&msg.body)),
                _ => {} // ParameterStatus etc.
            }
        }
    }

    /// Send a Standby Status Update (`CopyData` with an `'r'` payload) to ack
    /// `lsn` as received/flushed/applied, keeping the connection alive.
    pub async fn send_standby_status(&mut self, lsn: u64, reply_now: bool) -> Result<()> {
        // 'r' + 3x int64 LSN + int64 timestamp + 1 byte replyNow.
        let mut inner = Vec::with_capacity(34);
        inner.push(b'r');
        inner.extend_from_slice(&lsn.to_be_bytes());
        inner.extend_from_slice(&lsn.to_be_bytes());
        inner.extend_from_slice(&lsn.to_be_bytes());
        inner.extend_from_slice(&0i64.to_be_bytes()); // timestamp (0 ok)
        inner.push(if reply_now { 1 } else { 0 });

        let len = (inner.len() + 4) as i32;
        let w = self.stream.get_mut();
        w.write_all(b"d").await?; // CopyData
        w.write_all(&len.to_be_bytes()).await?;
        w.write_all(&inner).await?;
        w.flush().await?;
        Ok(())
    }
}

/// HMAC-SHA256(key, msg).
fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// Format an LSN as PostgreSQL's `H/L` hex text form.
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Parse a `H/L` hex LSN into a u64.
pub fn parse_lsn(s: &str) -> Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow!("invalid LSN {s:?}"))?;
    let hi = u64::from_str_radix(hi.trim(), 16)?;
    let lo = u64::from_str_radix(lo.trim(), 16)?;
    Ok((hi << 32) | lo)
}

/// Extract the human-readable message from an ErrorResponse body.
fn parse_error(body: &[u8]) -> String {
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let field = body[i];
        let start = i + 1;
        let mut end = start;
        while end < body.len() && body[end] != 0 {
            end += 1;
        }
        let text = String::from_utf8_lossy(&body[start..end]);
        if field == b'M' {
            return text.into_owned();
        }
        i = end + 1;
    }
    "unknown error".to_string()
}
