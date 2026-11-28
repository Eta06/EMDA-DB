//! Derived tables `FROM (SELECT ...) [AS] name` and non-recursive common
//! table expressions `WITH name AS (SELECT ...) ...`.
//!
//! The source query runs first, inside the statement's transaction, and its
//! rows (at most `max_select_rows`) are materialized. The outer query then
//! reads only that source: `WHERE`, its select list or aggregates with
//! `GROUP BY` and `HAVING`, `ORDER BY`, `OFFSET` and `LIMIT`. Source queries
//! may not use parameters; the outer query may. A `WITH` query reads one of
//! its CTEs, and CTE bodies read tables, not other CTEs.
use super::aggregate::{self, Resolver};
use super::{ColRef, JExpr, JoinParser, JoinSelect, OrderItem, SelectItem, TableRef, limit_value};
use crate::document::DocumentTransaction;
use crate::sql::exec::{SqlCache, SqlValueRef, compare_order_cell_ref};
use crate::sql::lexer::{Token, TokenKind};
use crate::sql::parser::{CompareOp, SqlStatement, parse_statement};
use crate::sql::value::{SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue};
use parking_lot::RwLock;
use std::cmp::Ordering;

/// CTEs one `WITH` may declare.
pub(crate) const MAX_SQL_CTES: usize = 8;

/// Whether tokens start a statement this module parses: `WITH`, or a
/// `SELECT` whose top-level `FROM` is followed by `(`.
pub(crate) fn is_derived_select(tokens: &[Token]) -> bool {
    let is_kw = |token: &Token, word: &str| matches!(&token.kind, TokenKind::Ident { text, quoted: false } if text == word);
    match tokens.first() {
        Some(token) if is_kw(token, "with") => true,
        Some(token) if is_kw(token, "select") => {
            let mut depth = 0usize;
            for (i, token) in tokens.iter().enumerate() {
                match &token.kind {
                    TokenKind::LParen => depth += 1,
                    TokenKind::RParen => depth = depth.saturating_sub(1),
                    _ if depth == 0 && is_kw(token, "from") => {
                        return matches!(
                            tokens.get(i + 1).map(|t| &t.kind),
                            Some(TokenKind::LParen)
                        );
                    }
                    _ => {}
                }
            }
            false
        }
        _ => false,
    }
}

impl JoinParser<'_> {
    /// `"(" select ")"` at the cursor, parsed from its own text.
    fn source_query(&mut self, sql: &str) -> Result<SqlStatement, SqlError> {
        let open = self.pos;
        self.expect(&TokenKind::LParen, "(")?;
        let mut depth = 1usize;
        let mut close = self.pos;
        while depth > 0 {
            match self.tokens.get(close).map(|t| &t.kind) {
                Some(TokenKind::LParen) => depth += 1,
                Some(TokenKind::RParen) => depth -= 1,
                Some(TokenKind::Eof) | None => return Err(self.syntax("unclosed '('")),
                _ => {}
            }
            close += 1;
        }
        let close = close - 1;
        let text = &sql[self.tokens[open + 1].offset..self.tokens[close].offset];
        let statement = parse_statement(text, self.limits).map_err(|error| match error {
            SqlError::Syntax { offset, message } => SqlError::Syntax {
                offset: offset + self.tokens[open + 1].offset,
                message,
            },
            other => other,
        })?;
        if statement.param_count() > 0 {
            return Err(SqlError::Unsupported {
                feature: "parameters inside a derived table or CTE".into(),
                offset: Some(self.tokens[open].offset),
            });
        }
        self.pos = close + 1;
        Ok(statement)
    }

    pub(super) fn derived_select(&mut self, sql: &str) -> Result<JoinSelect, SqlError> {
        let mut ctes: Vec<(String, SqlStatement)> = Vec::new();
        if self.eat_kw("with") {
            if self.at_kw("recursive") {
                return Err(self.unsupported("WITH RECURSIVE"));
            }
            loop {
                let name = self.ident("CTE name")?;
                if ctes.iter().any(|(existing, _)| *existing == name) {
                    return Err(self.syntax("a CTE name is declared twice"));
                }
                if matches!(self.peek().kind, TokenKind::LParen) {
                    return Err(self.unsupported("CTE column lists"));
                }
                self.expect_kw("as")?;
                ctes.push((name, self.source_query(sql)?));
                if ctes.len() > MAX_SQL_CTES {
                    return Err(SqlError::Bounds(format!(
                        "a WITH declares at most {MAX_SQL_CTES} CTEs"
                    )));
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        self.expect_kw("select")?;
        let mut items = Vec::new();
        loop {
            items.push(self.select_item()?);
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        self.expect_kw("from")?;
        let (source, alias) = if matches!(self.peek().kind, TokenKind::LParen) {
            let source = self.source_query(sql)?;
            self.eat_kw("as");
            (source, self.ident("derived table alias")?)
        } else {
            let name = self.ident("CTE name")?;
            let index = ctes
                .iter()
                .position(|(cte, _)| *cte == name)
                .ok_or_else(|| {
                    self.unsupported("WITH reading a table that is not one of its CTEs")
                })?;
            let source = ctes.swap_remove(index).1;
            let alias = if self.eat_kw("as") || self.at_plain_ident() {
                self.ident("alias")?
            } else {
                name
            };
            (source, alias)
        };
        if self.at_kw("join")
            || self.at_kw("inner")
            || self.at_kw("left")
            || self.at_kw("right")
            || self.at_kw("full")
        {
            return Err(self.unsupported("joins with a derived table or CTE"));
        }
        let filter = if self.eat_kw("where") {
            Some(self.expr()?)
        } else {
            None
        };
        let mut group_by = Vec::new();
        if self.eat_kw("group") {
            self.expect_kw("by")?;
            loop {
                group_by.push(self.col_ref()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        let having = if self.eat_kw("having") {
            Some(self.expr()?)
        } else {
            None
        };
        let order_by = self.order_by_clause()?;
        let (mut limit, mut offset) = (None, None);
        if self.eat_kw("limit") {
            limit = Some(self.limit_value()?);
            if self.eat_kw("offset") {
                offset = Some(self.limit_value()?);
            }
        }
        Ok(JoinSelect {
            items,
            tables: vec![TableRef {
                name: alias.clone(),
                alias,
            }],
            joins: Vec::new(),
            filter,
            order_by,
            limit,
            offset,
            group_by,
            having,
            source: Some(Box::new(source)),
            cached_plan: RwLock::new(None),
        })
    }
}

fn value_ref(value: &SqlValue) -> SqlValueRef<'_> {
    match value {
        SqlValue::Null => SqlValueRef::Null,
        SqlValue::Integer(n) => SqlValueRef::Integer(*n),
        SqlValue::Real(f) => SqlValueRef::Real(*f),
        SqlValue::Text(s) => SqlValueRef::Text(s.as_str()),
        SqlValue::Boolean(b) => SqlValueRef::Boolean(*b),
    }
}

/// `WHERE` over one source row, in three-valued logic.
fn eval_row(
    expr: &JExpr,
    row: &[SqlValue],
    resolver: &Resolver<'_>,
    params: &[SqlValue],
) -> Result<Option<bool>, SqlError> {
    let value = |e: &JExpr| -> Result<SqlValue, SqlError> {
        Ok(match e {
            JExpr::Literal(v) => v.clone(),
            JExpr::Param(i) => params.get(*i).cloned().unwrap_or(SqlValue::Null),
            JExpr::Column(column) => row[resolver.resolve(column)?.0.1].clone(),
            JExpr::Agg(call) => {
                return Err(SqlError::Syntax {
                    offset: call.offset,
                    message: "aggregate functions are not allowed in WHERE".into(),
                });
            }
            _ => unreachable!("only operands are valued"),
        })
    };
    Ok(match expr {
        JExpr::And(items) => {
            let mut result = Some(true);
            for item in items {
                match eval_row(item, row, resolver, params)? {
                    Some(false) => return Ok(Some(false)),
                    None => result = None,
                    Some(true) => {}
                }
            }
            result
        }
        JExpr::Or(items) => {
            let mut result = Some(false);
            for item in items {
                match eval_row(item, row, resolver, params)? {
                    Some(true) => return Ok(Some(true)),
                    None => result = None,
                    Some(false) => {}
                }
            }
            result
        }
        JExpr::Not(inner) => eval_row(inner, row, resolver, params)?.map(|b| !b),
        JExpr::IsNull(inner, negated) => Some(matches!(value(inner)?, SqlValue::Null) != *negated),
        JExpr::Compare(op, left, right) => {
            let (left, right) = (value(left)?, value(right)?);
            aggregate::compare(&left, &right)?.map(|ordering| match op {
                CompareOp::Eq => ordering == Ordering::Equal,
                CompareOp::Ne => ordering != Ordering::Equal,
                CompareOp::Lt => ordering == Ordering::Less,
                CompareOp::Le => ordering != Ordering::Greater,
                CompareOp::Gt => ordering == Ordering::Greater,
                CompareOp::Ge => ordering != Ordering::Less,
            })
        }
        other => match value(other)? {
            SqlValue::Boolean(b) => Some(b),
            SqlValue::Null => None,
            _ => return Err(SqlError::Type("WHERE requires a boolean condition".into())),
        },
    })
}

/// A source column's type, from its first non-NULL value; a column with no
/// value counts as INTEGER, which only `SUM` and `AVG` observe, as NULL.
fn column_types(names: &[String], rows: &[Vec<SqlValue>]) -> Vec<SqlType> {
    (0..names.len())
        .map(|i| {
            rows.iter()
                .find_map(|row| match &row[i] {
                    SqlValue::Null => None,
                    SqlValue::Integer(_) => Some(SqlType::Integer),
                    SqlValue::Real(_) => Some(SqlType::Real),
                    SqlValue::Text(_) => Some(SqlType::Text { max_chars: None }),
                    SqlValue::Boolean(_) => Some(SqlType::Boolean),
                })
                .unwrap_or(SqlType::Integer)
        })
        .collect()
}

pub(super) fn run(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    source: &SqlStatement,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let (names, rows) = match crate::sql::exec::run_read_in_tx(tx, source, limits, cache)? {
        SqlOutcome::Rows { columns, rows } => (columns, rows),
        _ => unreachable!("a SELECT returns rows"),
    };
    let alias = statement.tables[0].alias.clone();
    let types = column_types(&names, &rows);
    let resolver = Resolver {
        names: vec![alias.clone()],
        aliases: vec![alias.clone()],
        defs: Vec::new(),
        rows: Some((alias, names.clone(), types)),
    };
    let mut kept = Vec::new();
    for row in rows {
        if let Some(filter) = &statement.filter {
            if eval_row(filter, &row, &resolver, params)? != Some(true) {
                continue;
            }
        }
        kept.push(row);
    }
    if aggregate::is_aggregate(statement) {
        let bound = aggregate::bind(statement, &resolver)?;
        let projected: Vec<Vec<SqlValue>> = kept
            .iter()
            .map(|row| bound.needed.iter().map(|&(_, c)| row[c].clone()).collect())
            .collect();
        return aggregate::group_rows(&projected, &bound, statement, &resolver, params, limits);
    }
    // Plain select list over the source columns.
    let mut columns = Vec::new();
    let mut picks = Vec::new();
    for item in &statement.items {
        match item {
            SelectItem::All => {
                for (i, name) in names.iter().enumerate() {
                    columns.push(name.clone());
                    picks.push(i);
                }
            }
            SelectItem::TableAll(qualifier, offset) => {
                if *qualifier != statement.tables[0].alias {
                    return Err(SqlError::Syntax {
                        offset: *offset,
                        message: format!("unknown table or alias '{qualifier}'"),
                    });
                }
                for (i, name) in names.iter().enumerate() {
                    columns.push(name.clone());
                    picks.push(i);
                }
            }
            SelectItem::Column(column) => {
                let ((_, i), col) = resolver.resolve(column)?;
                columns.push(col.name);
                picks.push(i);
            }
            SelectItem::Aggregate(..) => unreachable!("aggregates take the path above"),
        }
    }
    let order: Vec<(usize, &OrderItem)> = statement
        .order_by
        .iter()
        .map(|item| {
            Ok((
                resolve_order(&item.column, &resolver, &statement.items)?,
                item,
            ))
        })
        .collect::<Result<_, SqlError>>()?;
    if !order.is_empty() {
        kept.sort_by(|a, b| {
            for (i, item) in &order {
                let nulls_first = item.nulls_first.unwrap_or(!item.descending);
                let ordering = compare_order_cell_ref(
                    value_ref(&a[*i]),
                    value_ref(&b[*i]),
                    item.descending,
                    nulls_first,
                );
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            Ordering::Equal
        });
    }
    let limit = limit_value(statement.limit, params)?;
    let offset = limit_value(statement.offset, params)?.unwrap_or(0);
    let mut out = Vec::new();
    for row in kept.into_iter().skip(offset) {
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
        if out.len() >= limits.max_select_rows {
            return Err(SqlError::Bounds(format!(
                "SELECT result exceeds max_select_rows ({})",
                limits.max_select_rows
            )));
        }
        out.push(picks.iter().map(|&i| row[i].clone()).collect());
    }
    Ok(SqlOutcome::Rows { columns, rows: out })
}

fn resolve_order(
    column: &ColRef,
    resolver: &Resolver<'_>,
    _items: &[SelectItem],
) -> Result<usize, SqlError> {
    Ok(resolver.resolve(column)?.0.1)
}
