//! Protocol message bodies and the `[tag, body]` envelopes.
//!
//! Port of `zero-protocol` (`connect.ts`, `poke.ts`, `change-desired-queries.ts`,
//! `error.ts`, `ping.ts`, `pong.ts`). Each message is a 2-element JSON array
//! `["tag", body]`; the [`Downstream`] / [`Upstream`] enums serialize to exactly
//! that.

use crate::patches::{QueriesPatch, RowsPatch};
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A lexicographic version cookie.
pub type Version = String;

/// The protocol version Orbit speaks (matches Zero's `PROTOCOL_VERSION`).
pub const PROTOCOL_VERSION: u32 = 51;

// ---- bodies ----------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectedBody {
    pub wsid: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub timestamp: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaVersions {
    #[serde(rename = "minSupportedVersion")]
    pub min_supported_version: u32,
    #[serde(rename = "maxSupportedVersion")]
    pub max_supported_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PokeStartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    /// Always present; `null` matches Replicache's initial cookie state.
    #[serde(rename = "baseCookie")]
    pub base_cookie: Option<Version>,
    #[serde(rename = "schemaVersions", skip_serializing_if = "Option::is_none", default)]
    pub schema_versions: Option<SchemaVersions>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub timestamp: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PokePartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    #[serde(rename = "lastMutationIDChanges", skip_serializing_if = "Option::is_none", default)]
    pub last_mutation_id_changes: Option<HashMap<String, u64>>,
    #[serde(rename = "desiredQueriesPatches", skip_serializing_if = "Option::is_none", default)]
    pub desired_queries_patches: Option<HashMap<String, QueriesPatch>>,
    #[serde(rename = "gotQueriesPatch", skip_serializing_if = "Option::is_none", default)]
    pub got_queries_patch: Option<QueriesPatch>,
    #[serde(rename = "rowsPatch", skip_serializing_if = "Option::is_none", default)]
    pub rows_patch: Option<RowsPatch>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PokeEndBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    pub cookie: Version,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub cancel: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InitConnectionBody {
    #[serde(rename = "desiredQueriesPatch")]
    pub desired_queries_patch: QueriesPatch,
    // Other optional fields (clientSchema, user push/query URLs, etc.) are
    // accepted but not yet modeled.
    #[serde(flatten, default)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeDesiredQueriesBody {
    #[serde(rename = "desiredQueriesPatch")]
    pub desired_queries_patch: QueriesPatch,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub traceparent: Option<String>,
}

/// Kinds of protocol error (subset of Zero's `ErrorKind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    AuthInvalidated,
    ClientNotFound,
    InvalidConnectionRequest,
    InvalidMessage,
    InvalidPush,
    MutationFailed,
    MutationRateLimited,
    Unauthorized,
    VersionNotSupported,
    SchemaVersionNotSupported,
    Internal,
    Rebalance,
    Rehome,
    ServerOverloaded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub kind: ErrorKind,
    pub message: String,
}

// ---- envelopes -------------------------------------------------------------

/// Server → client messages.
#[derive(Debug, Clone, PartialEq)]
pub enum Downstream {
    Connected(ConnectedBody),
    PokeStart(PokeStartBody),
    PokePart(PokePartBody),
    PokeEnd(PokeEndBody),
    Pong,
    Error(ErrorBody),
}

impl Serialize for Downstream {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Downstream::Connected(b) => ("connected", b).serialize(s),
            Downstream::PokeStart(b) => ("pokeStart", b).serialize(s),
            Downstream::PokePart(b) => ("pokePart", b).serialize(s),
            Downstream::PokeEnd(b) => ("pokeEnd", b).serialize(s),
            Downstream::Pong => ("pong", Empty {}).serialize(s),
            Downstream::Error(b) => ("error", b).serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for Downstream {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (tag, body): (String, serde_json::Value) = Deserialize::deserialize(d)?;
        Ok(match tag.as_str() {
            "connected" => Downstream::Connected(from_body::<_, D::Error>(body)?),
            "pokeStart" => Downstream::PokeStart(from_body::<_, D::Error>(body)?),
            "pokePart" => Downstream::PokePart(from_body::<_, D::Error>(body)?),
            "pokeEnd" => Downstream::PokeEnd(from_body::<_, D::Error>(body)?),
            "pong" => Downstream::Pong,
            "error" => Downstream::Error(from_body::<_, D::Error>(body)?),
            other => return Err(D::Error::custom(format!("unknown downstream message {other:?}"))),
        })
    }
}

/// Client → server messages.
#[derive(Debug, Clone, PartialEq)]
pub enum Upstream {
    InitConnection(InitConnectionBody),
    ChangeDesiredQueries(ChangeDesiredQueriesBody),
    Push(crate::mutation::PushBody),
    Ping,
}

impl Serialize for Upstream {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Upstream::InitConnection(b) => ("initConnection", b).serialize(s),
            Upstream::ChangeDesiredQueries(b) => ("changeDesiredQueries", b).serialize(s),
            Upstream::Push(b) => ("push", b).serialize(s),
            Upstream::Ping => ("ping", Empty {}).serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for Upstream {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let (tag, body): (String, serde_json::Value) = Deserialize::deserialize(d)?;
        Ok(match tag.as_str() {
            "initConnection" => Upstream::InitConnection(from_body::<_, D::Error>(body)?),
            "changeDesiredQueries" => Upstream::ChangeDesiredQueries(from_body::<_, D::Error>(body)?),
            "push" => Upstream::Push(from_body::<_, D::Error>(body)?),
            "ping" => Upstream::Ping,
            other => return Err(D::Error::custom(format!("unknown upstream message {other:?}"))),
        })
    }
}

/// Empty body `{}` for parameterless messages.
#[derive(Serialize)]
struct Empty {}

/// Deserialize a message body from a JSON value, mapping errors to `E`.
fn from_body<T, E>(v: serde_json::Value) -> Result<T, E>
where
    T: serde::de::DeserializeOwned,
    E: serde::de::Error,
{
    serde_json::from_value(v).map_err(E::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn poke_start_envelope_round_trips() {
        let msg = Downstream::PokeStart(PokeStartBody {
            poke_id: "p1".into(),
            base_cookie: None,
            schema_versions: None,
            timestamp: None,
        });
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(v, json!(["pokeStart", {"pokeID": "p1", "baseCookie": null}]));
        let back: Downstream = serde_json::from_value(v).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn poke_end_with_cookie() {
        let msg = Downstream::PokeEnd(PokeEndBody {
            poke_id: "p1".into(),
            cookie: "01".into(),
            cancel: None,
        });
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            json!(["pokeEnd", {"pokeID": "p1", "cookie": "01"}])
        );
    }

    #[test]
    fn ping_pong_empty_body() {
        assert_eq!(serde_json::to_value(Upstream::Ping).unwrap(), json!(["ping", {}]));
        assert_eq!(serde_json::to_value(Downstream::Pong).unwrap(), json!(["pong", {}]));
    }

    #[test]
    fn change_desired_queries_round_trip() {
        use crate::patches::QueriesPatchOp;
        let msg = Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put {
                hash: "h1".into(),
                ttl: None,
                ast: None,
                name: None,
                args: None,
            }],
            traceparent: None,
        });
        let v = serde_json::to_value(&msg).unwrap();
        assert_eq!(
            v,
            json!(["changeDesiredQueries", {"desiredQueriesPatch": [{"op": "put", "hash": "h1"}]}])
        );
        let back: Upstream = serde_json::from_value(v).unwrap();
        assert_eq!(back, msg);
    }
}
