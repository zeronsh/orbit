//! Patch operations carried inside pokes: query-set patches and row patches.
//!
//! Port of `zero-protocol/src/queries-patch.ts` and `row-patch.ts`. Tagged by
//! the `op` field, matching Zero's JSON exactly.

// These are wire-message enums: the size gap between the rich `Put`/`Update`
// variants and the tiny `Del`/`Clear` ones is inherent to the protocol, and
// boxing would complicate (de)serialization for no real benefit.
#![allow(clippy::large_enum_variant)]

use oql::ast::Ast;
use oql::value::Row;
use serde::{Deserialize, Serialize};

/// A patch to a client's query set (desired or "got").
///
/// The `put` variant covers both downstream (`hash`+`ttl`) and upstream
/// (`ast`/`name`/`args` for client vs custom queries) forms; unused fields are
/// omitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum QueriesPatchOp {
    Put {
        hash: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        ttl: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        ast: Option<Ast>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        args: Option<Vec<serde_json::Value>>,
    },
    Del {
        hash: String,
    },
    Clear,
}

pub type QueriesPatch = Vec<QueriesPatchOp>;

/// A patch to the set of synced rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum RowPatchOp {
    Put {
        #[serde(rename = "tableName")]
        table_name: String,
        value: Row,
    },
    Update {
        #[serde(rename = "tableName")]
        table_name: String,
        id: Row,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        merge: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        constrain: Option<Vec<String>>,
    },
    Del {
        #[serde(rename = "tableName")]
        table_name: String,
        id: Row,
    },
    Clear,
}

pub type RowsPatch = Vec<RowPatchOp>;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn row_put_wire_shape() {
        let op = RowPatchOp::Put {
            table_name: "issue".into(),
            value: [("id".to_string(), oql::value::Value::from("i1"))]
                .into_iter()
                .collect(),
        };
        assert_eq!(
            serde_json::to_value(&op).unwrap(),
            json!({"op": "put", "tableName": "issue", "value": {"id": "i1"}})
        );
    }

    #[test]
    fn queries_del_and_clear() {
        assert_eq!(
            serde_json::to_value(QueriesPatchOp::Del { hash: "abc".into() }).unwrap(),
            json!({"op": "del", "hash": "abc"})
        );
        assert_eq!(
            serde_json::to_value(QueriesPatchOp::Clear).unwrap(),
            json!({"op": "clear"})
        );
    }
}
