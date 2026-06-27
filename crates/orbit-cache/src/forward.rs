//! Forwarding custom mutations + queries to the app's API server — the way Zero
//! works (`fetchFromAPIServer`).
//!
//! Custom mutators and custom queries are **not** executed in orbit-cache. The
//! client sends them by name; orbit-cache forwards an HTTP `POST` to a configured
//! endpoint, attaching the client's auth so the app server can authenticate and
//! supply context:
//!
//! * `Authorization: Bearer <token>` — the client's auth token (from the connect
//!   handshake).
//! * `Cookie: <cookie>` — the connection's cookies (only if `forward_cookies`).
//! * `X-Api-Key: <api_key>` — a shared secret authorizing orbit-cache → app.
//!
//! The **push** endpoint runs the mutators (authoritatively, with context) and
//! writes to Postgres; the change flows back via replication. The **query**
//! endpoint transforms a named query into an AST (e.g. filtered by the
//! authenticated user) which orbit-cache then materializes.

use anyhow::{bail, Context, Result};
use oql::ast::Ast;
use orbit_protocol::Mutation;
use serde::{Deserialize, Serialize};

/// Configured endpoint URLs + secrets (mirrors Zero's `mutate`/`query` config).
#[derive(Clone, Default)]
pub struct ForwardConfig {
    /// The app's push endpoint (runs custom mutators). `None` = don't forward.
    pub mutate_url: Option<String>,
    /// The app's query endpoint (transforms named queries). `None` = don't forward.
    pub query_url: Option<String>,
    /// Shared secret sent as `X-Api-Key` to authorize orbit-cache → app.
    pub api_key: Option<String>,
    /// Forward the connection's `Cookie` header to the endpoints.
    pub forward_cookies: bool,
}

/// Per-connection auth captured from the handshake, forwarded to the app server.
#[derive(Clone, Default)]
pub struct AuthContext {
    pub token: Option<String>,
    pub cookie: Option<String>,
}

/// Forwards to the app's API server with auth, like Zero's `fetchFromAPIServer`.
pub struct Forwarder {
    config: ForwardConfig,
    http: reqwest::Client,
}

#[derive(Serialize)]
struct PushBody<'a> {
    mutations: &'a [Mutation],
}

#[derive(Serialize)]
struct QueryBody<'a> {
    name: &'a str,
    args: &'a [serde_json::Value],
}

#[derive(Deserialize)]
struct QueryResponse {
    ast: Ast,
}

impl Forwarder {
    pub fn new(config: ForwardConfig) -> Self {
        Forwarder { config, http: reqwest::Client::new() }
    }

    pub fn forwards_mutations(&self) -> bool {
        self.config.mutate_url.is_some()
    }
    pub fn forwards_queries(&self) -> bool {
        self.config.query_url.is_some()
    }

    fn auth(&self, mut req: reqwest::RequestBuilder, auth: &AuthContext) -> reqwest::RequestBuilder {
        if let Some(key) = &self.config.api_key {
            req = req.header("X-Api-Key", key);
        }
        if let Some(token) = &auth.token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        if self.config.forward_cookies {
            if let Some(cookie) = &auth.cookie {
                req = req.header("Cookie", cookie);
            }
        }
        req
    }

    /// Forward custom mutations to the push endpoint. The endpoint runs the
    /// mutators with context and writes to Postgres.
    pub async fn push(&self, auth: &AuthContext, mutations: &[Mutation]) -> Result<()> {
        let url = self.config.mutate_url.as_ref().context("mutate_url not configured")?;
        let resp = self
            .auth(self.http.post(url).json(&PushBody { mutations }), auth)
            .send()
            .await
            .context("forwarding mutations to the push endpoint")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("push endpoint returned {status}: {body}");
        }
        Ok(())
    }

    /// Forward a named query to the query endpoint; it returns the (possibly
    /// permission-filtered) AST to materialize.
    pub async fn transform(
        &self,
        auth: &AuthContext,
        name: &str,
        args: &[serde_json::Value],
    ) -> Result<Ast> {
        let url = self.config.query_url.as_ref().context("query_url not configured")?;
        let resp = self
            .auth(self.http.post(url).json(&QueryBody { name, args }), auth)
            .send()
            .await
            .context("forwarding query to the transform endpoint")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!("query endpoint returned {status}: {body}");
        }
        let parsed: QueryResponse = resp.json().await.context("parsing transform response")?;
        Ok(parsed.ast)
    }
}
