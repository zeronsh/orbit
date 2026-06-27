//! Verifies orbit-cache forwards custom mutations/queries to the app's API
//! endpoint with the client's auth attached (the way Zero's `fetchFromAPIServer`
//! works). Uses a tiny mock HTTP server to capture the forwarded request.

use orbit_cache::{AuthContext, ForwardConfig, Forwarder};
use orbit_protocol::Mutation;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Accept one HTTP request, capture it, and reply with `response_body`.
async fn mock_endpoint(response_body: &'static str) -> (String, oneshot::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let mut req = String::from_utf8_lossy(&buf[..n]).to_string();
        // Read the rest of the body if Content-Length exceeds the first read.
        if let Some(cl) = content_length(&req) {
            let header_end = req.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req.len());
            while req.len() - header_end < cl {
                let m = sock.read(&mut buf).await.unwrap();
                if m == 0 {
                    break;
                }
                req.push_str(&String::from_utf8_lossy(&buf[..m]));
            }
        }
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
        sock.flush().await.unwrap();
        let _ = tx.send(req);
    });
    (format!("http://{addr}"), rx)
}

fn content_length(req: &str) -> Option<usize> {
    for line in req.lines() {
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            return v.trim().parse().ok();
        }
    }
    None
}

#[tokio::test]
async fn forwards_mutations_with_bearer_and_cookie() {
    let (base, rx) = mock_endpoint("{}").await;
    let fwd = Forwarder::new(ForwardConfig {
        mutate_url: Some(format!("{base}/push")),
        api_key: Some("secret-key".into()),
        forward_cookies: true,
        ..Default::default()
    });
    assert!(fwd.forwards_mutations());

    let auth = AuthContext { token: Some("jwt-abc".into()), cookie: Some("session=xyz".into()) };
    let mutation = Mutation::Custom {
        id: 1,
        client_id: "c1".into(),
        name: "createTodo".into(),
        args: vec![serde_json::json!({ "text": "hi" })],
        timestamp: 0.0,
    };
    fwd.push(&auth, &[mutation]).await.unwrap();

    let req = rx.await.unwrap();
    assert!(req.starts_with("POST /push "), "method/path: {}", req.lines().next().unwrap());
    assert!(req.contains("authorization: Bearer jwt-abc"), "missing bearer:\n{req}");
    assert!(req.contains("x-api-key: secret-key"), "missing api key");
    assert!(req.contains("cookie: session=xyz"), "missing cookie");
    assert!(req.contains("\"createTodo\"") && req.contains("\"text\":\"hi\""), "missing body");
}

#[tokio::test]
async fn does_not_forward_cookie_when_disabled() {
    let (base, rx) = mock_endpoint("{}").await;
    let fwd = Forwarder::new(ForwardConfig {
        mutate_url: Some(format!("{base}/push")),
        forward_cookies: false,
        ..Default::default()
    });
    let auth = AuthContext { token: Some("t".into()), cookie: Some("c=1".into()) };
    fwd.push(&auth, &[]).await.unwrap();
    let req = rx.await.unwrap();
    assert!(req.contains("authorization: Bearer t"));
    assert!(!req.to_ascii_lowercase().contains("cookie:"), "cookie should not be forwarded:\n{req}");
}

#[tokio::test]
async fn transforms_named_query_returning_ast() {
    let (base, rx) = mock_endpoint(r#"{"ast":{"table":"todo","orderBy":[["created","asc"]]}}"#).await;
    let fwd = Forwarder::new(ForwardConfig {
        query_url: Some(format!("{base}/query")),
        ..Default::default()
    });
    assert!(fwd.forwards_queries());
    let auth = AuthContext { token: Some("tok".into()), ..Default::default() };
    let ast = fwd.transform(&auth, "allTodos", &[]).await.unwrap();
    assert_eq!(ast.table, "todo");

    let req = rx.await.unwrap();
    assert!(req.starts_with("POST /query "));
    assert!(req.contains("authorization: Bearer tok"));
    assert!(req.contains("\"allTodos\""), "missing query name in body:\n{req}");
}
