//! Mutation + push protocol types (the write path).
//!
//! Port of `zero-protocol/src/mutation.ts` and `push.ts`. CRUD mutations carry
//! ops over named tables; custom mutations carry an arbitrary name + JSON args.

use oql::value::Row;
use serde::{Deserialize, Serialize};

/// The reserved CRUD mutation name.
pub const CRUD_MUTATION_NAME: &str = "_zero_crud";

/// A single CRUD operation. Tagged by `op`. `primaryKey` lists the PK columns;
/// `value` is the full row (for delete it is just the PK columns).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum CrudOp {
    Insert {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: Vec<String>,
        value: Row,
    },
    Upsert {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: Vec<String>,
        value: Row,
    },
    Update {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: Vec<String>,
        value: Row,
    },
    Delete {
        #[serde(rename = "tableName")]
        table_name: String,
        #[serde(rename = "primaryKey")]
        primary_key: Vec<String>,
        value: Row,
    },
}

/// The single argument of a CRUD mutation: a list of ops.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrudArg {
    pub ops: Vec<CrudOp>,
}

/// A client mutation. Tagged by `type` (`crud` / `custom`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Mutation {
    Crud {
        id: u64,
        #[serde(rename = "clientID")]
        client_id: String,
        name: String,
        args: Vec<CrudArg>,
        timestamp: f64,
    },
    Custom {
        id: u64,
        #[serde(rename = "clientID")]
        client_id: String,
        name: String,
        args: Vec<serde_json::Value>,
        timestamp: f64,
    },
}

impl Mutation {
    pub fn id(&self) -> u64 {
        match self {
            Mutation::Crud { id, .. } | Mutation::Custom { id, .. } => *id,
        }
    }
    pub fn client_id(&self) -> &str {
        match self {
            Mutation::Crud { client_id, .. } | Mutation::Custom { client_id, .. } => client_id,
        }
    }
}

/// The body of a `push` message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PushBody {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub mutations: Vec<Mutation>,
    #[serde(rename = "pushVersion")]
    pub push_version: u32,
    #[serde(rename = "schemaVersion", skip_serializing_if = "Option::is_none", default)]
    pub schema_version: Option<u32>,
    pub timestamp: f64,
    #[serde(rename = "requestID")]
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub traceparent: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn crud_mutation_round_trip() {
        let m = Mutation::Crud {
            id: 1,
            client_id: "c1".into(),
            name: CRUD_MUTATION_NAME.into(),
            args: vec![CrudArg {
                ops: vec![CrudOp::Insert {
                    table_name: "issue".into(),
                    primary_key: vec!["id".into()],
                    value: [("id".to_string(), oql::value::Value::from("i1"))]
                        .into_iter()
                        .collect(),
                }],
            }],
            timestamp: 0.0,
        };
        let v = serde_json::to_value(&m).unwrap();
        assert_eq!(v["type"], "crud");
        assert_eq!(v["args"][0]["ops"][0]["op"], "insert");
        assert_eq!(v["args"][0]["ops"][0]["tableName"], "issue");
        let back: Mutation = serde_json::from_value(v).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn push_body_shape() {
        let b = PushBody {
            client_group_id: "g1".into(),
            mutations: vec![],
            push_version: 1,
            schema_version: None,
            timestamp: 123.0,
            request_id: "r1".into(),
            traceparent: None,
        };
        let v = serde_json::to_value(&b).unwrap();
        assert_eq!(v, json!({
            "clientGroupID": "g1",
            "mutations": [],
            "pushVersion": 1,
            "timestamp": 123.0,
            "requestID": "r1"
        }));
    }
}
