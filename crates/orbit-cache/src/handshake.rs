//! Zero-client connection handshake compatibility.
//!
//! The real Zero TypeScript client passes its `initConnection` message (and auth
//! token) through the WebSocket `Sec-WebSocket-Protocol` header, as
//! `encodeURIComponent(btoa(JSON.stringify({initConnectionMessage, authToken})))`
//! (see `zero-protocol/src/connect.ts` `encodeSecProtocols`). The server decodes
//! it there and must echo the offered subprotocol back so the browser accepts
//! the handshake.

use base64::Engine;
use orbit_protocol::QueriesPatchOp;
use percent_encoding::percent_decode_str;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::WebSocketStream;

/// What the server extracts from the client's `Sec-WebSocket-Protocol` header.
#[derive(Debug, Default, Clone)]
pub struct ConnectInfo {
    pub auth_token: Option<String>,
    /// The `Cookie` header from the upgrade request (for `forward_cookies`).
    pub cookie: Option<String>,
    /// The `desiredQueriesPatch` from the `initConnection` message, if present.
    pub desired_queries: Vec<QueriesPatchOp>,
    /// The `clientID` query param from the connect URL (Zero passes it there).
    /// Identifies the client across reconnects for shared-CVR delta resume.
    pub client_id: Option<String>,
    /// The `baseCookie` query param: the last cookie the client successfully
    /// applied. The server fast-resumes only if it matches the stored CVR version.
    pub base_cookie: Option<u64>,
}

/// Extract a query-string parameter's (percent-decoded) value from a URL query.
fn query_param(query: Option<&str>, key: &str) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| percent_decode_str(v).decode_utf8_lossy().into_owned())
    })
}

/// Decode a `Sec-WebSocket-Protocol` value produced by `encodeSecProtocols`.
///
/// Returns `None` if the value isn't an Orbit/Zero secProtocol (e.g. a plain
/// subprotocol token), so callers can fall back to reading `initConnection` as a
/// normal message.
pub fn decode_sec_protocol(value: &str) -> Option<ConnectInfo> {
    // The value may be a comma-separated list of offered subprotocols; the
    // encoded payload is the (single) entry.
    let token = value.split(',').next()?.trim();
    let uri_decoded = percent_decode_str(token).decode_utf8().ok()?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(uri_decoded.as_bytes())
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;

    let mut info = ConnectInfo::default();
    if let Some(token) = json.get("authToken").and_then(|v| v.as_str()) {
        info.auth_token = Some(token.to_string());
    }
    // initConnectionMessage is `["initConnection", { desiredQueriesPatch, ... }]`.
    if let Some(msg) = json.get("initConnectionMessage").and_then(|v| v.as_array()) {
        if msg.first().and_then(|v| v.as_str()) == Some("initConnection") {
            if let Some(body) = msg.get(1) {
                if let Some(patch) = body.get("desiredQueriesPatch") {
                    if let Ok(ops) = serde_json::from_value::<Vec<QueriesPatchOp>>(patch.clone()) {
                        info.desired_queries = ops;
                    }
                }
            }
        }
    }
    Some(info)
}

/// Accept a WebSocket connection the way the Zero client expects: read the
/// `Sec-WebSocket-Protocol` header (carrying `initConnection` + auth), echo it
/// back so the browser accepts the handshake, and return the decoded
/// [`ConnectInfo`].
pub async fn accept_zero_ws<S>(stream: S) -> anyhow::Result<(WebSocketStream<S>, ConnectInfo)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Captured during the handshake callback: (sec-protocol, cookie, clientID, baseCookie).
    type Captured = (Option<String>, Option<String>, Option<String>, Option<String>);
    let captured: Arc<Mutex<Captured>> = Arc::new(Mutex::new((None, None, None, None)));
    let cap = captured.clone();
    let callback = move |req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> {
        let mut g = cap.lock().unwrap();
        if let Some(proto) = req.headers().get("sec-websocket-protocol") {
            if let Ok(s) = proto.to_str() {
                g.0 = Some(s.to_string());
            }
            // Echo the offered subprotocol so the handshake succeeds.
            resp.headers_mut().insert("sec-websocket-protocol", proto.clone());
        }
        if let Some(cookie) = req.headers().get("cookie") {
            if let Ok(s) = cookie.to_str() {
                g.1 = Some(s.to_string());
            }
        }
        let q = req.uri().query();
        g.2 = query_param(q, "clientID");
        g.3 = query_param(q, "baseCookie");
        Ok(resp)
    };
    let ws = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
    let (proto, cookie, client_id, base_cookie) = {
        let mut g = captured.lock().unwrap();
        (g.0.take(), g.1.take(), g.2.take(), g.3.take())
    };
    let mut info = proto.and_then(|s| decode_sec_protocol(&s)).unwrap_or_default();
    info.cookie = cookie;
    info.client_id = client_id;
    info.base_cookie = base_cookie.and_then(|s| s.parse::<u64>().ok());
    Ok((ws, info))
}

/// Encode a `Sec-WebSocket-Protocol` value the way the Zero client does. Used by
/// tests (and any Rust client) to simulate the handshake.
pub fn encode_sec_protocol(init_connection_body: &serde_json::Value, auth: Option<&str>) -> String {
    let payload = serde_json::json!({
        "initConnectionMessage": ["initConnection", init_connection_body],
        "authToken": auth,
    });
    let json = serde_json::to_vec(&payload).unwrap();
    let b64 = base64::engine::general_purpose::STANDARD.encode(json);
    percent_encoding::utf8_percent_encode(&b64, percent_encoding::NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_sec_protocol() {
        let body = serde_json::json!({
            "desiredQueriesPatch": [{"op": "put", "hash": "h1"}]
        });
        let encoded = encode_sec_protocol(&body, Some("tok"));
        let info = decode_sec_protocol(&encoded).expect("decodes");
        assert_eq!(info.auth_token.as_deref(), Some("tok"));
        assert_eq!(info.desired_queries.len(), 1);
        assert!(matches!(&info.desired_queries[0], QueriesPatchOp::Put { hash, .. } if hash == "h1"));
    }

    #[test]
    fn non_orbit_subprotocol_returns_none_or_empty() {
        // A plain token isn't valid base64-JSON -> None.
        assert!(decode_sec_protocol("chat").is_none());
    }
}
