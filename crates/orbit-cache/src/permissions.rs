//! Read permissions (auth rules), the server-side analog of Zero's permission
//! system.
//!
//! A read rule is a [`Condition`] (typically using `authData` static params)
//! that is ANDed into every query on its table. Before building a pipeline the
//! rules are applied and their `authData` params resolved from the connection's
//! auth data — so a client only ever materializes rows it is allowed to see.

use oql::ast::{Ast, Condition};
use oql::value::Row;
use oql::{eval_condition, resolve_cond_params_with_row, resolve_static_params};
use std::collections::HashMap;

/// The kind of write a rule guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteOp {
    Insert,
    Update,
    Delete,
}

/// Per-table read and write rules.
#[derive(Default, Clone)]
pub struct Permissions {
    read_rules: HashMap<String, Condition>,
    write_rules: HashMap<(String, WriteOp), Condition>,
}

impl Permissions {
    pub fn new() -> Self {
        Permissions::default()
    }

    /// Add a read rule for `table` (a condition over that table's rows).
    pub fn allow_read(&mut self, table: impl Into<String>, rule: Condition) {
        self.read_rules.insert(table.into(), rule);
    }

    /// Add a write rule for `table`/`op`. The rule may use `authData` and (for
    /// update/delete) `preMutationRow` params.
    pub fn allow_write(&mut self, table: impl Into<String>, op: WriteOp, rule: Condition) {
        self.write_rules.insert((table.into(), op), rule);
    }

    /// Authorize a write. `row` is the row being written (for insert) or the
    /// pre-mutation row (for update/delete). Returns true if allowed; tables
    /// without a rule are allowed by default (open — add rules to restrict).
    pub fn can_write(&self, table: &str, op: WriteOp, row: &Row, auth: &serde_json::Value) -> bool {
        match self.write_rules.get(&(table.to_string(), op)) {
            None => true,
            Some(rule) => {
                let resolved = resolve_cond_params_with_row(rule, auth, Some(row));
                eval_condition(&resolved, row)
            }
        }
    }

    /// Apply read rules to `ast` (and its related subqueries), resolving
    /// `authData` params from `auth`. Each table's rule is ANDed into its WHERE.
    pub fn apply(&self, ast: &Ast, auth: &serde_json::Value) -> Ast {
        let with_rules = self.and_rules(ast);
        resolve_static_params(&with_rules, auth)
    }

    fn and_rules(&self, ast: &Ast) -> Ast {
        let mut out = ast.clone();
        if let Some(rule) = self.read_rules.get(&ast.table) {
            out.where_ = Some(match out.where_.take() {
                None => rule.clone(),
                Some(existing) => Condition::And {
                    conditions: vec![existing, rule.clone()],
                },
            });
        }
        out.related = ast.related.as_ref().map(|rels| {
            rels.iter()
                .map(|r| {
                    let mut r2 = r.clone();
                    r2.subquery = Box::new(self.and_rules(&r.subquery));
                    r2
                })
                .collect()
        });
        out
    }
}

/// Decode auth data from a token. Accepts a JWT (uses the unverified payload —
/// signature verification is a deployment concern) or a raw JSON claims string.
pub fn decode_auth(token: &str) -> serde_json::Value {
    // JWT: header.payload.signature — decode the payload.
    if let Some((_, rest)) = token.split_once('.') {
        if let Some((payload_b64, _)) = rest.split_once('.') {
            use base64::Engine;
            if let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload_b64) {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    return json;
                }
            }
        }
    }
    // Otherwise try parsing as raw JSON claims.
    serde_json::from_str(token).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oql::ast::{Condition, ParameterAnchor, ParameterField, SimpleOperator, ValuePosition};

    #[test]
    fn read_rule_anded_and_params_resolved() {
        // Rule: owner = authData.userId
        let rule = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "owner".into() },
            right: ValuePosition::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("userId".into()),
            },
        };
        let mut perms = Permissions::new();
        perms.allow_read("doc", rule);

        let ast = Ast::new("doc");
        let auth = serde_json::json!({ "userId": "u1" });
        let applied = perms.apply(&ast, &auth);

        // WHERE owner = 'u1' (literal resolved from authData).
        match applied.where_.unwrap() {
            Condition::Simple { right: ValuePosition::Literal { value }, .. } => {
                assert_eq!(value, oql::ast::LiteralValue::String("u1".into()));
            }
            other => panic!("expected resolved simple condition, got {other:?}"),
        }
    }

    #[test]
    fn write_rule_checks_pre_mutation_row() {
        // Allow update/delete only when the row's owner == authData.userId.
        let rule = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "owner".into() },
            right: ValuePosition::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("userId".into()),
            },
        };
        let mut perms = Permissions::new();
        perms.allow_write("doc", WriteOp::Update, rule);

        let auth = serde_json::json!({ "userId": "u1" });
        let mine: Row = [("id".to_string(), oql::value::Value::from("d1")), ("owner".to_string(), oql::value::Value::from("u1"))].into_iter().collect();
        let theirs: Row = [("id".to_string(), oql::value::Value::from("d2")), ("owner".to_string(), oql::value::Value::from("u2"))].into_iter().collect();

        assert!(perms.can_write("doc", WriteOp::Update, &mine, &auth));
        assert!(!perms.can_write("doc", WriteOp::Update, &theirs, &auth));
        // No rule for delete -> allowed by default.
        assert!(perms.can_write("doc", WriteOp::Delete, &theirs, &auth));
    }

    #[test]
    fn decode_jwt_payload() {
        // {"userId":"u1"} as a JWT-ish token (header.payload.sig).
        use base64::Engine;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"userId":"u1"}"#);
        let token = format!("h.{payload}.s");
        assert_eq!(decode_auth(&token)["userId"], "u1");
    }
}
