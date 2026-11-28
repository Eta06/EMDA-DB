//! Bounded inner and outer joins over SQL tables.
//!
//! A join statement is `SELECT items FROM t [AS a] {[INNER] | LEFT [OUTER]}
//! JOIN u [AS b] ON expr ... [WHERE expr] [ORDER BY col ...] [LIMIT n
//! [OFFSET m]]`. Tables join left to right. For each joined table the
//! executor chooses, from the `ON` conjuncts, a primary-key point lookup
//! (`u.pk = <expression over earlier tables>`), a hash join on an equality
//! between a column of `u` and an expression over earlier tables, or a
//! nested loop. The complete `ON` expression is evaluated on every candidate
//! pair, so the access path never changes the result. The whole statement
//! runs in one read-set document transaction.
use super::catalog::{self, ColumnDef, IndexState, TableDef};
use super::exec::{self, SqlCache};
use super::lexer::{Token, TokenKind};
use super::parser::{AggregateKind, CompareOp, LimitValue, SqlStatement};
use super::plan::AccessPlan;

#[path = "join_aggregate.rs"]
mod aggregate;
#[path = "join_derived.rs"]
mod derived;
use super::value::{
    MAX_SQL_COLUMNS, MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue,
    compare_i64_f64, encode_i64_pk_bytes,
};
use crate::MAX_SCAN_ENTRIES;
use crate::document::flat::FlatScalar;
use crate::document::{DocumentDatabase, DocumentTransaction, IndexValue, StoredDocument};
pub(crate) use derived::is_derived_select;
use parking_lot::RwLock;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io;
use std::sync::Arc;

/// Tables in one join statement, including the first.
pub(crate) const MAX_JOIN_TABLES: usize = 8;

#[derive(Debug)]
pub(crate) struct JoinSelect {
    items: Vec<SelectItem>,
    tables: Vec<TableRef>,
    /// `joins[i]` joins `tables[i + 1]`.
    joins: Vec<JoinClause>,
    filter: Option<JExpr>,
    order_by: Vec<OrderItem>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
    /// `GROUP BY` columns; with aggregate items, the statement aggregates.
    group_by: Vec<ColRef>,
    having: Option<JExpr>,
    /// A derived table or CTE read as `tables[0]` instead of a stored table.
    source: Option<Box<SqlStatement>>,
    cached_plan: RwLock<Option<Arc<JoinPlan>>>,
}

impl Clone for JoinSelect {
    fn clone(&self) -> Self {
        Self {
            items: self.items.clone(),
            tables: self.tables.clone(),
            joins: self.joins.clone(),
            filter: self.filter.clone(),
            order_by: self.order_by.clone(),
            limit: self.limit,
            offset: self.offset,
            group_by: self.group_by.clone(),
            having: self.having.clone(),
            source: self.source.clone(),
            cached_plan: RwLock::new(None),
        }
    }
}

impl PartialEq for JoinSelect {
    fn eq(&self, other: &Self) -> bool {
        self.items == other.items
            && self.tables == other.tables
            && self.joins == other.joins
            && self.filter == other.filter
            && self.order_by == other.order_by
            && self.limit == other.limit
            && self.offset == other.offset
            && self.group_by == other.group_by
            && self.having == other.having
            && self.source == other.source
    }
}

#[derive(Clone, Debug, PartialEq)]
struct TableRef {
    name: String,
    alias: String,
    query: Option<Arc<derived::Source>>,
}

type CteSources = Vec<(String, Arc<derived::Source>)>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinKind {
    fn preserves_left(self) -> bool {
        matches!(self, Self::Left | Self::Full)
    }

    fn preserves_right(self) -> bool {
        matches!(self, Self::Right | Self::Full)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct JoinClause {
    kind: JoinKind,
    on: JExpr,
}

#[derive(Clone, Debug, PartialEq)]
enum SelectItem {
    All,
    TableAll(String, usize),
    Column(ColRef),
    /// An aggregate call with an optional `AS` name.
    Aggregate(AggCall, Option<String>),
}

#[derive(Clone, Debug, PartialEq)]
struct AggCall {
    kind: AggregateKind,
    /// `None` for `COUNT(*)`.
    arg: Option<ColRef>,
    offset: usize,
}

#[derive(Clone, Debug, PartialEq)]
struct ColRef {
    qualifier: Option<String>,
    column: String,
    offset: usize,
}

#[derive(Clone, Debug, PartialEq)]
struct OrderItem {
    column: ColRef,
    descending: bool,
    nulls_first: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
enum JExpr {
    Literal(SqlValue),
    Param(usize),
    Column(ColRef),
    IsNull(Box<JExpr>, bool),
    Not(Box<JExpr>),
    Compare(CompareOp, Box<JExpr>, Box<JExpr>),
    And(Vec<JExpr>),
    Or(Vec<JExpr>),
    /// An aggregate call; allowed only in `HAVING`.
    Agg(AggCall),
}

/// Whether a tokenized statement is a SELECT containing `JOIN`.
pub(crate) fn is_join_select(tokens: &[Token]) -> bool {
    let keyword = |token: &Token, word: &str| matches!(&token.kind, TokenKind::Ident { text, quoted: false } if text == word);
    tokens.first().is_some_and(|token| keyword(token, "select"))
        && tokens.iter().any(|token| keyword(token, "join"))
}

/// Parses a join SELECT, returning the statement and its parameter count.
pub(crate) fn parse(
    tokens: Vec<Token>,
    sql: &str,
    limits: &SqlLimits,
) -> Result<(JoinSelect, usize), SqlError> {
    let mut parser = JoinParser {
        tokens,
        pos: 0,
        params: 0,
        depth: 0,
        limits,
    };
    let statement = parser.select(sql)?;
    parser.eat(&TokenKind::Semicolon);
    if !matches!(parser.peek().kind, TokenKind::Eof) {
        return Err(parser.syntax("multiple statements or trailing tokens are not supported"));
    }
    Ok((statement, parser.params))
}

struct JoinParser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    params: usize,
    depth: usize,
    limits: &'a SqlLimits,
}

const RESERVED: &[&str] = &[
    "select", "from", "where", "join", "inner", "left", "right", "full", "outer", "cross",
    "natural", "on", "using", "order", "by", "limit", "offset", "group", "having", "and", "or",
    "not", "is", "null", "true", "false", "as", "asc", "desc", "nulls", "first", "last", "union",
];

impl JoinParser<'_> {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos.min(self.tokens.len() - 1)]
    }

    fn offset(&self) -> usize {
        self.peek().offset
    }

    fn advance(&mut self) -> Token {
        let token = self.peek().clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        token
    }

    fn syntax(&self, message: &str) -> SqlError {
        SqlError::Syntax {
            offset: self.offset(),
            message: message.to_owned(),
        }
    }

    fn unsupported(&self, feature: &str) -> SqlError {
        SqlError::Unsupported {
            feature: feature.to_owned(),
            offset: Some(self.offset()),
        }
    }

    fn at_kw(&self, word: &str) -> bool {
        matches!(&self.peek().kind, TokenKind::Ident { text, quoted: false } if text == word)
    }

    fn eat_kw(&mut self, word: &str) -> bool {
        let found = self.at_kw(word);
        if found {
            self.advance();
        }
        found
    }

    fn expect_kw(&mut self, word: &str) -> Result<(), SqlError> {
        if self.eat_kw(word) {
            Ok(())
        } else {
            Err(self.syntax(&format!("expected {}", word.to_ascii_uppercase())))
        }
    }

    fn eat(&mut self, kind: &TokenKind) -> bool {
        let found = &self.peek().kind == kind;
        if found {
            self.advance();
        }
        found
    }

    fn expect(&mut self, kind: &TokenKind, what: &str) -> Result<(), SqlError> {
        if self.eat(kind) {
            Ok(())
        } else {
            Err(self.syntax(&format!("expected {what}")))
        }
    }

    /// An identifier that is not a reserved keyword, unless quoted.
    fn ident(&mut self, what: &str) -> Result<String, SqlError> {
        match &self.peek().kind {
            TokenKind::Ident { text, quoted } if *quoted || !RESERVED.contains(&text.as_str()) => {
                let text = text.clone();
                self.advance();
                Ok(text)
            }
            _ => Err(self.syntax(&format!("expected {what}"))),
        }
    }

    fn at_plain_ident(&self) -> bool {
        matches!(&self.peek().kind, TokenKind::Ident { text, quoted }
            if *quoted || !RESERVED.contains(&text.as_str()))
    }

    fn select(&mut self, sql: &str) -> Result<JoinSelect, SqlError> {
        let ctes = self.cte_sources(sql)?;
        self.select_with_ctes(sql, &ctes)
    }

    fn select_with_ctes(&mut self, sql: &str, ctes: &CteSources) -> Result<JoinSelect, SqlError> {
        self.expect_kw("select")?;
        if self.at_kw("distinct") || self.at_kw("all") {
            return Err(self.unsupported("SELECT DISTINCT or ALL in a join"));
        }
        let mut items = Vec::new();
        loop {
            items.push(self.select_item()?);
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        self.expect_kw("from")?;
        let mut tables = vec![self.table_ref(sql, ctes)?];
        let mut joins = Vec::new();
        // `a RIGHT JOIN b ON c` is `b LEFT JOIN a ON c` with `a`'s columns
        // first; it is accepted as the first join, where both tables are
        // visible to every later clause either way.
        let mut right_first = false;
        loop {
            if self.at_kw("cross") || self.at_kw("natural") {
                return Err(self.unsupported("CROSS or NATURAL JOIN"));
            }
            if matches!(self.peek().kind, TokenKind::Comma) {
                return Err(self.unsupported("comma-separated FROM tables"));
            }
            let explicit_kind = if self.eat_kw("right") {
                self.eat_kw("outer");
                if joins.is_empty() {
                    right_first = true;
                    Some(JoinKind::Left)
                } else {
                    Some(JoinKind::Right)
                }
            } else if self.eat_kw("full") {
                self.eat_kw("outer");
                Some(JoinKind::Full)
            } else if self.eat_kw("left") {
                self.eat_kw("outer");
                Some(JoinKind::Left)
            } else if self.eat_kw("inner") {
                Some(JoinKind::Inner)
            } else {
                None
            };
            if !self.eat_kw("join") {
                if explicit_kind.is_some() {
                    return Err(self.syntax("expected JOIN"));
                }
                break;
            }
            tables.push(self.table_ref(sql, ctes)?);
            if tables.len() > MAX_JOIN_TABLES {
                return Err(SqlError::Bounds(format!(
                    "join has more than {MAX_JOIN_TABLES} tables"
                )));
            }
            if self.at_kw("using") {
                return Err(self.unsupported("JOIN ... USING"));
            }
            self.expect_kw("on")?;
            let on = self.expr()?;
            joins.push(JoinClause {
                kind: explicit_kind.unwrap_or(JoinKind::Inner),
                on,
            });
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
        } else if self.at_kw("offset") {
            return Err(self.unsupported("OFFSET without LIMIT"));
        }
        if right_first {
            // Keep the written column order for `*`, then put the preserved
            // (right) table first.
            let mut expanded = Vec::with_capacity(items.len() + 1);
            for item in items {
                match item {
                    SelectItem::All => {
                        expanded.extend(
                            tables
                                .iter()
                                .map(|table| SelectItem::TableAll(table.alias.clone(), 0)),
                        );
                    }
                    other => expanded.push(other),
                }
            }
            items = expanded;
            tables.swap(0, 1);
        }
        let source = if tables.len() == 1 {
            match tables[0].query.as_deref() {
                Some(derived::Source::Select(statement)) => Some(statement.clone()),
                _ => None,
            }
        } else {
            None
        };
        Ok(JoinSelect {
            items,
            tables,
            joins,
            filter,
            order_by,
            limit,
            offset,
            group_by,
            having,
            source,
            cached_plan: RwLock::new(None),
        })
    }

    fn order_by_clause(&mut self) -> Result<Vec<OrderItem>, SqlError> {
        let mut order_by = Vec::new();
        if self.eat_kw("order") {
            self.expect_kw("by")?;
            loop {
                let column = self.col_ref()?;
                let descending = if self.eat_kw("desc") {
                    true
                } else {
                    self.eat_kw("asc");
                    false
                };
                let nulls_first = if self.eat_kw("nulls") {
                    if self.eat_kw("first") {
                        Some(true)
                    } else {
                        self.expect_kw("last")?;
                        Some(false)
                    }
                } else {
                    None
                };
                order_by.push(OrderItem {
                    column,
                    descending,
                    nulls_first,
                });
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        Ok(order_by)
    }

    /// The aggregate named `name`, if `name(` starts one.
    fn aggregate_kind(&self, name: &str) -> Option<AggregateKind> {
        if !matches!(self.peek().kind, TokenKind::LParen) {
            return None;
        }
        match name {
            "count" => Some(AggregateKind::Count),
            "sum" => Some(AggregateKind::Sum),
            "avg" => Some(AggregateKind::Avg),
            "min" => Some(AggregateKind::Min),
            "max" => Some(AggregateKind::Max),
            _ => None,
        }
    }

    /// The parenthesized argument of an aggregate whose name was read.
    fn aggregate_call(&mut self, kind: AggregateKind, offset: usize) -> Result<AggCall, SqlError> {
        self.expect(&TokenKind::LParen, "(")?;
        let call = if kind == AggregateKind::Count && self.eat(&TokenKind::Star) {
            AggCall {
                kind: AggregateKind::CountStar,
                arg: None,
                offset,
            }
        } else {
            let distinct = self.eat_kw("distinct");
            if distinct && kind != AggregateKind::Count {
                return Err(self.unsupported("DISTINCT in aggregates other than COUNT"));
            }
            AggCall {
                kind: if distinct {
                    AggregateKind::CountDistinct
                } else {
                    kind
                },
                arg: Some(self.col_ref()?),
                offset,
            }
        };
        self.expect(&TokenKind::RParen, ")")?;
        Ok(call)
    }

    fn select_item(&mut self) -> Result<SelectItem, SqlError> {
        if self.eat(&TokenKind::Star) {
            return Ok(SelectItem::All);
        }
        let offset = self.offset();
        let first = self.ident("column or table name")?;
        if self.eat(&TokenKind::Dot) {
            if self.eat(&TokenKind::Star) {
                return Ok(SelectItem::TableAll(first, offset));
            }
            let column = self.ident("column name")?;
            return self.no_alias(SelectItem::Column(ColRef {
                qualifier: Some(first),
                column,
                offset,
            }));
        }
        if let Some(kind) = self.aggregate_kind(&first) {
            let call = self.aggregate_call(kind, offset)?;
            let alias = if self.eat_kw("as") {
                Some(self.ident("column alias")?)
            } else {
                None
            };
            return Ok(SelectItem::Aggregate(call, alias));
        }
        if matches!(self.peek().kind, TokenKind::LParen) {
            return Err(self.unsupported("functions in a join"));
        }
        self.no_alias(SelectItem::Column(ColRef {
            qualifier: None,
            column: first,
            offset,
        }))
    }

    fn no_alias(&self, item: SelectItem) -> Result<SelectItem, SqlError> {
        if self.at_kw("as") {
            return Err(self.unsupported("column aliases in a join"));
        }
        Ok(item)
    }

    fn table_ref(&mut self, sql: &str, ctes: &CteSources) -> Result<TableRef, SqlError> {
        if matches!(self.peek().kind, TokenKind::LParen) {
            let query = Arc::new(derived::Source::Select(Box::new(self.source_query(sql)?)));
            self.eat_kw("as");
            let alias = self.ident("derived table alias")?;
            return Ok(TableRef {
                name: alias.clone(),
                alias,
                query: Some(query),
            });
        }
        let name = self.ident("table name")?;
        if matches!(self.peek().kind, TokenKind::LParen) {
            return Err(self.unsupported("table functions or subqueries"));
        }
        let explicit = self.eat_kw("as");
        let alias = if explicit || self.at_plain_ident() {
            self.ident("table alias")?
        } else {
            name.clone()
        };
        let query = ctes
            .iter()
            .find(|(cte, _)| cte == &name)
            .map(|(_, query)| Arc::clone(query));
        Ok(TableRef { name, alias, query })
    }

    fn col_ref(&mut self) -> Result<ColRef, SqlError> {
        let offset = self.offset();
        let first = self.ident("column name")?;
        if self.eat(&TokenKind::Dot) {
            let column = self.ident("column name")?;
            return Ok(ColRef {
                qualifier: Some(first),
                column,
                offset,
            });
        }
        Ok(ColRef {
            qualifier: None,
            column: first,
            offset,
        })
    }

    fn limit_value(&mut self) -> Result<LimitValue, SqlError> {
        match self.advance().kind {
            TokenKind::Integer(text) => text
                .parse::<usize>()
                .map(LimitValue::Literal)
                .map_err(|_| self.syntax("LIMIT or OFFSET value out of range")),
            TokenKind::Question => {
                self.params += 1;
                Ok(LimitValue::Param(self.params - 1))
            }
            _ => Err(self.syntax("expected a non-negative integer or ? for LIMIT or OFFSET")),
        }
    }

    fn nested<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, SqlError>,
    ) -> Result<T, SqlError> {
        self.depth += 1;
        if self.depth > self.limits.max_expr_depth {
            return Err(SqlError::Bounds(format!(
                "expression nesting exceeds max_expr_depth ({})",
                self.limits.max_expr_depth
            )));
        }
        let result = f(self);
        self.depth -= 1;
        result
    }

    fn expr(&mut self) -> Result<JExpr, SqlError> {
        self.nested(|p| {
            let mut items = vec![p.and_expr()?];
            while p.eat_kw("or") {
                items.push(p.and_expr()?);
            }
            Ok(if items.len() == 1 {
                items.pop().unwrap()
            } else {
                JExpr::Or(items)
            })
        })
    }

    fn and_expr(&mut self) -> Result<JExpr, SqlError> {
        let mut items = vec![self.not_expr()?];
        while self.eat_kw("and") {
            items.push(self.not_expr()?);
        }
        Ok(if items.len() == 1 {
            items.pop().unwrap()
        } else {
            JExpr::And(items)
        })
    }

    fn not_expr(&mut self) -> Result<JExpr, SqlError> {
        if self.eat_kw("not") {
            return self.nested(|p| Ok(JExpr::Not(Box::new(p.not_expr()?))));
        }
        let left = self.operand()?;
        if self.eat_kw("is") {
            let negated = self.eat_kw("not");
            self.expect_kw("null")?;
            return Ok(JExpr::IsNull(Box::new(left), negated));
        }
        let op = match self.peek().kind {
            TokenKind::Eq => CompareOp::Eq,
            TokenKind::Ne => CompareOp::Ne,
            TokenKind::Lt => CompareOp::Lt,
            TokenKind::Le => CompareOp::Le,
            TokenKind::Gt => CompareOp::Gt,
            TokenKind::Ge => CompareOp::Ge,
            _ => {
                if self.at_kw("in") || self.at_kw("like") || self.at_kw("between") {
                    return Err(self.unsupported("IN, LIKE or BETWEEN"));
                }
                return Ok(left);
            }
        };
        self.advance();
        let right = self.operand()?;
        Ok(JExpr::Compare(op, Box::new(left), Box::new(right)))
    }

    fn operand(&mut self) -> Result<JExpr, SqlError> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::LParen => {
                self.advance();
                if self.at_kw("select") {
                    return Err(self.unsupported("subqueries"));
                }
                let inner = self.expr()?;
                self.expect(&TokenKind::RParen, ")")?;
                Ok(inner)
            }
            TokenKind::Question => {
                self.advance();
                self.params += 1;
                if self.params > self.limits.max_params {
                    return Err(SqlError::Bounds(format!(
                        "parameter count exceeds max_params ({})",
                        self.limits.max_params
                    )));
                }
                Ok(JExpr::Param(self.params - 1))
            }
            TokenKind::Integer(_) | TokenKind::Real(_) => self.number(false),
            TokenKind::Minus => {
                self.advance();
                self.number(true)
            }
            TokenKind::String(text) => {
                self.advance();
                Ok(JExpr::Literal(SqlValue::Text(text)))
            }
            TokenKind::Ident {
                ref text,
                quoted: false,
            } if matches!(text.as_str(), "null" | "true" | "false") => {
                self.advance();
                Ok(JExpr::Literal(match text.as_str() {
                    "null" => SqlValue::Null,
                    "true" => SqlValue::Boolean(true),
                    _ => SqlValue::Boolean(false),
                }))
            }
            TokenKind::Ident { ref text, .. } => {
                let call_follows = matches!(
                    self.tokens.get(self.pos + 1).map(|token| &token.kind),
                    Some(TokenKind::LParen)
                );
                let kind = match text.as_str() {
                    "count" => Some(AggregateKind::Count),
                    "sum" => Some(AggregateKind::Sum),
                    "avg" => Some(AggregateKind::Avg),
                    "min" => Some(AggregateKind::Min),
                    "max" => Some(AggregateKind::Max),
                    _ => None,
                };
                if let (Some(kind), true) = (kind, call_follows) {
                    let offset = self.offset();
                    self.advance();
                    return Ok(JExpr::Agg(self.aggregate_call(kind, offset)?));
                }
                let column = self.col_ref()?;
                if matches!(self.peek().kind, TokenKind::LParen) {
                    return Err(self.unsupported("functions in a join"));
                }
                Ok(JExpr::Column(column))
            }
            TokenKind::Plus | TokenKind::Slash | TokenKind::Percent | TokenKind::Concat => {
                Err(self.unsupported("arithmetic or string operators"))
            }
            _ => Err(self.syntax("expected an expression")),
        }
    }

    fn number(&mut self, negative: bool) -> Result<JExpr, SqlError> {
        let token = self.advance();
        match token.kind {
            TokenKind::Integer(text) => {
                let text = if negative { format!("-{text}") } else { text };
                text.parse::<i64>()
                    .map(|n| JExpr::Literal(SqlValue::Integer(n)))
                    .map_err(|_| SqlError::Syntax {
                        offset: token.offset,
                        message: format!("integer literal '{text}' out of signed 64-bit range"),
                    })
            }
            TokenKind::Real(text) => {
                let value: f64 = text.parse().map_err(|_| SqlError::Syntax {
                    offset: token.offset,
                    message: format!("invalid REAL literal '{text}'"),
                })?;
                if !value.is_finite() {
                    return Err(SqlError::Syntax {
                        offset: token.offset,
                        message: format!("REAL literal '{text}' is not finite"),
                    });
                }
                Ok(JExpr::Literal(SqlValue::Real(if negative {
                    -value
                } else {
                    value
                })))
            }
            _ => Err(SqlError::Syntax {
                offset: token.offset,
                message: "unary minus requires a numeric literal".into(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Binding

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Slot {
    table: usize,
    column: usize,
}

#[derive(Debug)]
enum BExpr {
    Literal(SqlValue),
    Param(usize),
    Column(Slot),
    IsNull(Box<BExpr>, bool),
    Not(Box<BExpr>),
    Compare(CompareOp, Box<BExpr>, Box<BExpr>),
    And(Vec<BExpr>),
    Or(Vec<BExpr>),
}

#[derive(Clone, Copy, Debug)]
enum Ty {
    Null,
    Typed(SqlType),
    Param(usize),
}

#[derive(Clone, Copy, Debug)]
enum ParamCheck {
    Boolean {
        index: usize,
        what: &'static str,
    },
    Predicate {
        index: usize,
        what: &'static str,
    },
    CompareWith {
        index: usize,
        other: SqlType,
        param_first: bool,
    },
    CompareParams {
        left: usize,
        right: usize,
    },
}

impl ParamCheck {
    fn check(self, params: &[SqlValue]) -> Result<(), SqlError> {
        match self {
            Self::Boolean { index, what } => match value_type(&params[index]) {
                Ty::Null | Ty::Typed(SqlType::Boolean) => Ok(()),
                Ty::Typed(other) => Err(SqlError::Type(format!(
                    "{what} requires BOOLEAN operands, got {}",
                    other.display_name()
                ))),
                Ty::Param(_) => unreachable!(),
            },
            Self::Predicate { index, what } => match value_type(&params[index]) {
                Ty::Null | Ty::Typed(SqlType::Boolean) => Ok(()),
                Ty::Typed(other) => Err(SqlError::Type(format!(
                    "{what} must evaluate to BOOLEAN, got {}",
                    other.display_name()
                ))),
                Ty::Param(_) => unreachable!(),
            },
            Self::CompareWith {
                index,
                other,
                param_first,
            } => {
                if let Ty::Typed(param_ty) = value_type(&params[index]) {
                    if !(param_ty.matches_base(other)
                        || (param_ty.is_numeric() && other.is_numeric()))
                    {
                        let (a, b) = if param_first {
                            (param_ty, other)
                        } else {
                            (other, param_ty)
                        };
                        return Err(SqlError::Type(format!(
                            "cannot compare {} and {}",
                            a.display_name(),
                            b.display_name()
                        )));
                    }
                }
                Ok(())
            }
            Self::CompareParams { left, right } => {
                if let (Ty::Typed(a), Ty::Typed(b)) =
                    (value_type(&params[left]), value_type(&params[right]))
                {
                    if !(a.matches_base(b) || (a.is_numeric() && b.is_numeric())) {
                        return Err(SqlError::Type(format!(
                            "cannot compare {} and {}",
                            a.display_name(),
                            b.display_name()
                        )));
                    }
                }
                Ok(())
            }
        }
    }
}

impl BExpr {
    /// The highest table index referenced, or `None` for constants.
    fn max_table(&self) -> Option<usize> {
        match self {
            Self::Literal(_) | Self::Param(_) => None,
            Self::Column(slot) => Some(slot.table),
            Self::IsNull(inner, _) | Self::Not(inner) => inner.max_table(),
            Self::Compare(_, left, right) => left.max_table().max(right.max_table()),
            Self::And(items) | Self::Or(items) => items.iter().filter_map(Self::max_table).max(),
        }
    }

    fn mark_needed(&self, needed: &mut [Vec<bool>]) {
        match self {
            Self::Literal(_) | Self::Param(_) => {}
            Self::Column(slot) => needed[slot.table][slot.column] = true,
            Self::IsNull(inner, _) | Self::Not(inner) => inner.mark_needed(needed),
            Self::Compare(_, left, right) => {
                left.mark_needed(needed);
                right.mark_needed(needed);
            }
            Self::And(items) | Self::Or(items) => {
                for item in items {
                    item.mark_needed(needed);
                }
            }
        }
    }
}

struct Binder<'a> {
    defs: &'a [Arc<TableDef>],
    aliases: &'a [String],
    /// Tables visible to the expression being bound.
    visible: usize,
    param_checks: &'a mut Vec<ParamCheck>,
}

impl Binder<'_> {
    fn resolve(&self, column: &ColRef) -> Result<Slot, SqlError> {
        match &column.qualifier {
            Some(qualifier) => {
                let table = self.aliases[..self.visible]
                    .iter()
                    .position(|alias| alias == qualifier)
                    .ok_or_else(|| SqlError::Syntax {
                        offset: column.offset,
                        message: format!("unknown table or alias '{qualifier}' in this scope"),
                    })?;
                let index = self.defs[table]
                    .column_index(&column.column)
                    .ok_or_else(|| SqlError::NoSuchColumn {
                        table: self.defs[table].name.clone(),
                        column: column.column.clone(),
                    })?;
                Ok(Slot {
                    table,
                    column: index,
                })
            }
            None => {
                let mut found = None;
                for (table, def) in self.defs[..self.visible].iter().enumerate() {
                    if let Some(index) = def.column_index(&column.column) {
                        if found.is_some() {
                            return Err(SqlError::Syntax {
                                offset: column.offset,
                                message: format!(
                                    "column '{}' is ambiguous; qualify it with a table",
                                    column.column
                                ),
                            });
                        }
                        found = Some(Slot {
                            table,
                            column: index,
                        });
                    }
                }
                found.ok_or_else(|| SqlError::NoSuchColumn {
                    table: self.aliases[..self.visible].join(", "),
                    column: column.column.clone(),
                })
            }
        }
    }

    fn require_boolean(&mut self, ty: Ty, what: &'static str) -> Result<(), SqlError> {
        match ty {
            Ty::Null | Ty::Typed(SqlType::Boolean) => Ok(()),
            Ty::Param(index) => {
                self.param_checks.push(ParamCheck::Boolean { index, what });
                Ok(())
            }
            Ty::Typed(other) => Err(SqlError::Type(format!(
                "{what} requires BOOLEAN operands, got {}",
                other.display_name()
            ))),
        }
    }

    fn bind(&mut self, expr: &JExpr) -> Result<(BExpr, Ty), SqlError> {
        Ok(match expr {
            JExpr::Agg(call) => {
                return Err(SqlError::Syntax {
                    offset: call.offset,
                    message: "aggregate functions are allowed only in the select list and HAVING"
                        .into(),
                });
            }
            JExpr::Literal(value) => (BExpr::Literal(value.clone()), value_type(value)),
            JExpr::Param(index) => (BExpr::Param(*index), Ty::Param(*index)),
            JExpr::Column(column) => {
                let slot = self.resolve(column)?;
                let ty = self.defs[slot.table].columns[slot.column].col_type;
                (BExpr::Column(slot), Ty::Typed(ty))
            }
            JExpr::IsNull(inner, negated) => (
                BExpr::IsNull(Box::new(self.bind(inner)?.0), *negated),
                Ty::Typed(SqlType::Boolean),
            ),
            JExpr::Not(inner) => {
                let (bound, ty) = self.bind(inner)?;
                self.require_boolean(ty, "NOT")?;
                (BExpr::Not(Box::new(bound)), Ty::Typed(SqlType::Boolean))
            }
            JExpr::And(items) | JExpr::Or(items) => {
                let what = if matches!(expr, JExpr::And(_)) {
                    "AND"
                } else {
                    "OR"
                };
                let mut bound = Vec::with_capacity(items.len());
                for item in items {
                    let (b, ty) = self.bind(item)?;
                    self.require_boolean(ty, what)?;
                    bound.push(b);
                }
                let node = if matches!(expr, JExpr::And(_)) {
                    BExpr::And(bound)
                } else {
                    BExpr::Or(bound)
                };
                (node, Ty::Typed(SqlType::Boolean))
            }
            JExpr::Compare(op, left, right) => {
                let (l, lt) = self.bind(left)?;
                let (r, rt) = self.bind(right)?;
                match (lt, rt) {
                    (Ty::Typed(a), Ty::Typed(b)) => {
                        if !(a.matches_base(b) || (a.is_numeric() && b.is_numeric())) {
                            return Err(SqlError::Type(format!(
                                "cannot compare {} and {}",
                                a.display_name(),
                                b.display_name()
                            )));
                        }
                    }
                    (Ty::Param(index), Ty::Typed(other)) => {
                        self.param_checks.push(ParamCheck::CompareWith {
                            index,
                            other,
                            param_first: true,
                        });
                    }
                    (Ty::Typed(other), Ty::Param(index)) => {
                        self.param_checks.push(ParamCheck::CompareWith {
                            index,
                            other,
                            param_first: false,
                        });
                    }
                    (Ty::Param(left), Ty::Param(right)) => {
                        self.param_checks
                            .push(ParamCheck::CompareParams { left, right });
                    }
                    (Ty::Null, _) | (_, Ty::Null) => {}
                }
                (
                    BExpr::Compare(*op, Box::new(l), Box::new(r)),
                    Ty::Typed(SqlType::Boolean),
                )
            }
        })
    }

    fn bind_predicate(&mut self, expr: &JExpr, what: &'static str) -> Result<BExpr, SqlError> {
        let (bound, ty) = self.bind(expr)?;
        match ty {
            Ty::Null | Ty::Typed(SqlType::Boolean) => Ok(bound),
            Ty::Param(index) => {
                self.param_checks
                    .push(ParamCheck::Predicate { index, what });
                Ok(bound)
            }
            Ty::Typed(other) => Err(SqlError::Type(format!(
                "{what} must evaluate to BOOLEAN, got {}",
                other.display_name()
            ))),
        }
    }
}

fn value_type(value: &SqlValue) -> Ty {
    match value {
        SqlValue::Null => Ty::Null,
        SqlValue::Integer(_) => Ty::Typed(SqlType::Integer),
        SqlValue::Real(_) => Ty::Typed(SqlType::Real),
        SqlValue::Text(_) => Ty::Typed(SqlType::Text { max_chars: None }),
        SqlValue::Boolean(_) => Ty::Typed(SqlType::Boolean),
    }
}

fn conjuncts(expr: BExpr) -> Vec<BExpr> {
    match expr {
        BExpr::And(items) => items.into_iter().flat_map(conjuncts).collect(),
        other => vec![other],
    }
}

/// How each table finds candidate rows for one combined row of the tables
/// before it. For the first table, keys are constants or parameters.
#[derive(Debug)]
enum Access {
    /// `table.pk = key`, where `key` uses only earlier tables.
    Point(BExpr),
    /// `table.column = key` through a ready secondary index whose leading
    /// column is `column`.
    Index {
        name: String,
        column: usize,
        single_column: bool,
        key: BExpr,
    },
    /// `table.column = key` through a hash table built once per statement.
    Hash { column: usize, key: BExpr },
    /// Every row of the table.
    Loop,
}

#[derive(Debug)]
struct Step {
    kind: JoinKind,
    on: Vec<FastPred>,
    access: Access,
}

#[derive(Debug)]
enum FastPred {
    SlotCmpSlot {
        left: Slot,
        op: CompareOp,
        right: Slot,
    },
    SlotCmpParam {
        slot: Slot,
        op: CompareOp,
        param: usize,
    },
    SlotCmpLit {
        slot: Slot,
        op: CompareOp,
        lit: SqlValue,
    },
    SlotIsNull {
        slot: Slot,
        negated: bool,
    },
    Expr(BExpr),
}

#[inline]
fn flip_op(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

impl FastPred {
    fn compile(expr: BExpr) -> Self {
        match expr {
            BExpr::Compare(op, left, right) => match (left.as_ref(), right.as_ref()) {
                (BExpr::Column(l), BExpr::Column(r)) => Self::SlotCmpSlot {
                    left: *l,
                    op,
                    right: *r,
                },
                (BExpr::Column(slot), BExpr::Param(param)) => Self::SlotCmpParam {
                    slot: *slot,
                    op,
                    param: *param,
                },
                (BExpr::Param(param), BExpr::Column(slot)) => Self::SlotCmpParam {
                    slot: *slot,
                    op: flip_op(op),
                    param: *param,
                },
                (BExpr::Column(slot), BExpr::Literal(lit)) => Self::SlotCmpLit {
                    slot: *slot,
                    op,
                    lit: lit.clone(),
                },
                (BExpr::Literal(lit), BExpr::Column(slot)) => Self::SlotCmpLit {
                    slot: *slot,
                    op: flip_op(op),
                    lit: lit.clone(),
                },
                _ => Self::Expr(BExpr::Compare(op, left, right)),
            },
            BExpr::IsNull(inner, negated) => match inner.as_ref() {
                BExpr::Column(slot) => Self::SlotIsNull {
                    slot: *slot,
                    negated,
                },
                _ => Self::Expr(BExpr::IsNull(inner, negated)),
            },
            other => Self::Expr(other),
        }
    }

    fn mark_needed(&self, needed: &mut [Vec<bool>]) {
        match self {
            Self::SlotCmpSlot { left, right, .. } => {
                needed[left.table][left.column] = true;
                needed[right.table][right.column] = true;
            }
            Self::SlotCmpParam { slot, .. }
            | Self::SlotCmpLit { slot, .. }
            | Self::SlotIsNull { slot, .. } => {
                needed[slot.table][slot.column] = true;
            }
            Self::Expr(expr) => expr.mark_needed(needed),
        }
    }
}

#[inline]
fn eval_cmp_op(op: CompareOp, ordering: Ordering) -> bool {
    match op {
        CompareOp::Eq => ordering == Ordering::Equal,
        CompareOp::Ne => ordering != Ordering::Equal,
        CompareOp::Lt => ordering == Ordering::Less,
        CompareOp::Le => ordering != Ordering::Greater,
        CompareOp::Gt => ordering == Ordering::Greater,
        CompareOp::Ge => ordering != Ordering::Less,
    }
}

fn choose_access(table: usize, def: &TableDef, on: &[BExpr], preserve_build: bool) -> Access {
    // Prefer a primary-key equality, then any column equality.
    let mut best: Option<(usize, bool, usize)> = None; // (conjunct, is_pk, column)
    for (index, conjunct) in on.iter().enumerate() {
        if let BExpr::Compare(CompareOp::Eq, left, right) = conjunct {
            for (own, other) in [(left, right), (right, left)] {
                if let BExpr::Column(slot) = own.as_ref() {
                    if slot.table == table && other.max_table().is_none_or(|max| max < table) {
                        let is_pk = def.has_scalar_pk() && slot.column == def.primary_key_index;
                        if best.is_none_or(|(_, best_pk, _)| is_pk && !best_pk) {
                            best = Some((index, is_pk, slot.column));
                        }
                    }
                }
            }
        }
    }
    let Some((index, is_pk, column)) = best else {
        return Access::Loop;
    };
    // The equality stays in `on` and is re-evaluated on every candidate;
    // the access path only narrows the candidates.
    let BExpr::Compare(_, left, right) = &on[index] else {
        unreachable!()
    };
    let key = match (left.as_ref(), right.as_ref()) {
        (BExpr::Column(slot), other) | (other, BExpr::Column(slot))
            if slot.table == table && slot.column == column =>
        {
            clone_expr(other)
        }
        _ => unreachable!(),
    };
    if preserve_build {
        return Access::Hash { column, key };
    }
    if is_pk {
        return Access::Point(key);
    }
    match def.indexes.iter().find(|index| {
        // Documents missing any indexed field are not indexed, so a
        // compound index only covers every row with this leading value
        // when its other columns can never be NULL.
        index.state == IndexState::Ready
            && index.columns.first() == Some(&column)
            && index.columns[1..]
                .iter()
                .all(|&other| def.columns[other].not_null)
    }) {
        Some(index) => Access::Index {
            name: index.name.clone(),
            column,
            single_column: index.columns.len() == 1,
            key,
        },
        None => Access::Hash { column, key },
    }
}

fn clone_expr(expr: &BExpr) -> BExpr {
    match expr {
        BExpr::Literal(value) => BExpr::Literal(value.clone()),
        BExpr::Param(index) => BExpr::Param(*index),
        BExpr::Column(slot) => BExpr::Column(*slot),
        BExpr::IsNull(inner, negated) => BExpr::IsNull(Box::new(clone_expr(inner)), *negated),
        BExpr::Not(inner) => BExpr::Not(Box::new(clone_expr(inner))),
        BExpr::Compare(op, left, right) => {
            BExpr::Compare(*op, Box::new(clone_expr(left)), Box::new(clone_expr(right)))
        }
        BExpr::And(items) => BExpr::And(items.iter().map(clone_expr).collect()),
        BExpr::Or(items) => BExpr::Or(items.iter().map(clone_expr).collect()),
    }
}

// ---------------------------------------------------------------------------
// Evaluation

type RowIdx = u32;
const NULL_ROW: RowIdx = u32::MAX;
const NO_NEXT: RowIdx = u32::MAX;

/// One combined row: an arena index per table, or `NULL_ROW` when a LEFT JOIN
/// extended the row with NULLs.
type Combined = [RowIdx; MAX_JOIN_TABLES];

/// Contiguous row storage for one table, avoiding a heap `Vec` and `Arc` per row.
struct TableArena {
    stride: usize,
    data: Vec<SqlValue>,
}

impl TableArena {
    fn new(stride: usize) -> Self {
        Self {
            stride,
            data: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn len(&self) -> usize {
        self.data.len().checked_div(self.stride).unwrap_or(0)
    }

    #[inline]
    fn row(&self, idx: RowIdx) -> &[SqlValue] {
        let start = idx as usize * self.stride;
        &self.data[start..start + self.stride]
    }

    #[inline]
    fn push_borrowed(&mut self, vals: &[ValRef<'_>], needed: &[bool]) -> RowIdx {
        let idx = self.len() as RowIdx;
        self.data.reserve(self.stride);
        for (val, &need) in vals.iter().zip(needed) {
            self.data.push(if need {
                val.to_sql_value()
            } else {
                SqlValue::Null
            });
        }
        idx
    }
}

/// Borrowed SQL scalar value for zero-allocation evaluation, comparison, and hashing.
#[derive(Clone, Copy, Debug)]
enum ValRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Boolean(bool),
}

impl<'a> ValRef<'a> {
    #[inline]
    fn from_sql(value: &'a SqlValue) -> Self {
        match value {
            SqlValue::Null => Self::Null,
            SqlValue::Integer(n) => Self::Integer(*n),
            SqlValue::Real(f) => Self::Real(*f),
            SqlValue::Text(s) => Self::Text(s.as_str()),
            SqlValue::Boolean(b) => Self::Boolean(*b),
        }
    }

    #[inline]
    fn to_sql_value(self) -> SqlValue {
        match self {
            Self::Null => SqlValue::Null,
            Self::Integer(n) => SqlValue::Integer(n),
            Self::Real(f) => SqlValue::Real(f),
            Self::Text(s) => SqlValue::Text(s.to_owned()),
            Self::Boolean(b) => SqlValue::Boolean(b),
        }
    }

    fn kind_name(self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Integer(_) => "INTEGER",
            Self::Real(_) => "REAL",
            Self::Text(_) => "TEXT",
            Self::Boolean(_) => "BOOLEAN",
        }
    }
}

#[inline]
fn slot_val<'a>(row: &Combined, arenas: &'a [TableArena], slot: Slot) -> ValRef<'a> {
    let idx = row[slot.table];
    if idx == NULL_ROW {
        ValRef::Null
    } else {
        ValRef::from_sql(&arenas[slot.table].row(idx)[slot.column])
    }
}

fn eval_ref<'a>(
    expr: &'a BExpr,
    row: &Combined,
    arenas: &'a [TableArena],
    params: &'a [SqlValue],
) -> Result<ValRef<'a>, SqlError> {
    Ok(match expr {
        BExpr::Literal(value) => ValRef::from_sql(value),
        BExpr::Param(index) => ValRef::from_sql(&params[*index]),
        BExpr::Column(slot) => slot_val(row, arenas, *slot),
        BExpr::IsNull(inner, negated) => {
            let is_null = matches!(eval_ref(inner, row, arenas, params)?, ValRef::Null);
            ValRef::Boolean(is_null != *negated)
        }
        BExpr::Not(inner) => match eval_ref(inner, row, arenas, params)? {
            ValRef::Boolean(b) => ValRef::Boolean(!b),
            _ => ValRef::Null,
        },
        BExpr::And(items) => {
            let mut unknown = false;
            for item in items {
                match eval_ref(item, row, arenas, params)? {
                    ValRef::Boolean(false) => return Ok(ValRef::Boolean(false)),
                    ValRef::Boolean(true) => {}
                    _ => unknown = true,
                }
            }
            if unknown {
                ValRef::Null
            } else {
                ValRef::Boolean(true)
            }
        }
        BExpr::Or(items) => {
            let mut unknown = false;
            for item in items {
                match eval_ref(item, row, arenas, params)? {
                    ValRef::Boolean(true) => return Ok(ValRef::Boolean(true)),
                    ValRef::Boolean(false) => {}
                    _ => unknown = true,
                }
            }
            if unknown {
                ValRef::Null
            } else {
                ValRef::Boolean(false)
            }
        }
        BExpr::Compare(op, left, right) => {
            let l = eval_ref(left, row, arenas, params)?;
            let r = eval_ref(right, row, arenas, params)?;
            match compare_ref(l, r)? {
                None => ValRef::Null,
                Some(ordering) => ValRef::Boolean(match op {
                    CompareOp::Eq => ordering == Ordering::Equal,
                    CompareOp::Ne => ordering != Ordering::Equal,
                    CompareOp::Lt => ordering == Ordering::Less,
                    CompareOp::Le => ordering != Ordering::Greater,
                    CompareOp::Gt => ordering == Ordering::Greater,
                    CompareOp::Ge => ordering != Ordering::Less,
                }),
            }
        }
    })
}

/// Evaluates an expression referencing only table 0 against a borrowed row slice.
fn eval_table0_ref<'a>(
    expr: &'a BExpr,
    vals: &'a [ValRef<'a>],
    params: &'a [SqlValue],
) -> Result<ValRef<'a>, SqlError> {
    Ok(match expr {
        BExpr::Literal(value) => ValRef::from_sql(value),
        BExpr::Param(index) => ValRef::from_sql(&params[*index]),
        BExpr::Column(slot) => vals[slot.column],
        BExpr::IsNull(inner, negated) => {
            let is_null = matches!(eval_table0_ref(inner, vals, params)?, ValRef::Null);
            ValRef::Boolean(is_null != *negated)
        }
        BExpr::Not(inner) => match eval_table0_ref(inner, vals, params)? {
            ValRef::Boolean(b) => ValRef::Boolean(!b),
            _ => ValRef::Null,
        },
        BExpr::And(items) => {
            let mut unknown = false;
            for item in items {
                match eval_table0_ref(item, vals, params)? {
                    ValRef::Boolean(false) => return Ok(ValRef::Boolean(false)),
                    ValRef::Boolean(true) => {}
                    _ => unknown = true,
                }
            }
            if unknown {
                ValRef::Null
            } else {
                ValRef::Boolean(true)
            }
        }
        BExpr::Or(items) => {
            let mut unknown = false;
            for item in items {
                match eval_table0_ref(item, vals, params)? {
                    ValRef::Boolean(true) => return Ok(ValRef::Boolean(true)),
                    ValRef::Boolean(false) => {}
                    _ => unknown = true,
                }
            }
            if unknown {
                ValRef::Null
            } else {
                ValRef::Boolean(false)
            }
        }
        BExpr::Compare(op, left, right) => {
            let l = eval_table0_ref(left, vals, params)?;
            let r = eval_table0_ref(right, vals, params)?;
            match compare_ref(l, r)? {
                None => ValRef::Null,
                Some(ordering) => ValRef::Boolean(match op {
                    CompareOp::Eq => ordering == Ordering::Equal,
                    CompareOp::Ne => ordering != Ordering::Equal,
                    CompareOp::Lt => ordering == Ordering::Less,
                    CompareOp::Le => ordering != Ordering::Greater,
                    CompareOp::Gt => ordering == Ordering::Greater,
                    CompareOp::Ge => ordering != Ordering::Less,
                }),
            }
        }
    })
}

#[inline]
fn truthy_fast_pred(
    pred: &FastPred,
    row: &Combined,
    arenas: &[TableArena],
    params: &[SqlValue],
) -> Result<bool, SqlError> {
    match pred {
        FastPred::SlotCmpSlot { left, op, right } => {
            let l = slot_val(row, arenas, *left);
            let r = slot_val(row, arenas, *right);
            Ok(compare_ref(l, r)?.is_some_and(|ord| eval_cmp_op(*op, ord)))
        }
        FastPred::SlotCmpParam { slot, op, param } => {
            let l = slot_val(row, arenas, *slot);
            let r = ValRef::from_sql(&params[*param]);
            Ok(compare_ref(l, r)?.is_some_and(|ord| eval_cmp_op(*op, ord)))
        }
        FastPred::SlotCmpLit { slot, op, lit } => {
            let l = slot_val(row, arenas, *slot);
            let r = ValRef::from_sql(lit);
            Ok(compare_ref(l, r)?.is_some_and(|ord| eval_cmp_op(*op, ord)))
        }
        FastPred::SlotIsNull { slot, negated } => {
            let is_null = matches!(slot_val(row, arenas, *slot), ValRef::Null);
            Ok(is_null != *negated)
        }
        FastPred::Expr(expr) => Ok(matches!(
            eval_ref(expr, row, arenas, params)?,
            ValRef::Boolean(true)
        )),
    }
}

#[inline]
fn passes_early(
    early: &[FastPred],
    vals: &[ValRef<'_>],
    params: &[SqlValue],
) -> Result<bool, SqlError> {
    for pred in early {
        let matched = match pred {
            FastPred::SlotCmpSlot { left, op, right } => {
                compare_ref(vals[left.column], vals[right.column])?
                    .is_some_and(|ord| eval_cmp_op(*op, ord))
            }
            FastPred::SlotCmpParam { slot, op, param } => {
                compare_ref(vals[slot.column], ValRef::from_sql(&params[*param]))?
                    .is_some_and(|ord| eval_cmp_op(*op, ord))
            }
            FastPred::SlotCmpLit { slot, op, lit } => {
                compare_ref(vals[slot.column], ValRef::from_sql(lit))?
                    .is_some_and(|ord| eval_cmp_op(*op, ord))
            }
            FastPred::SlotIsNull { slot, negated } => {
                matches!(vals[slot.column], ValRef::Null) != *negated
            }
            FastPred::Expr(expr) => {
                matches!(eval_table0_ref(expr, vals, params)?, ValRef::Boolean(true))
            }
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

/// SQL comparison; `None` when either side is NULL.
#[inline]
fn compare_ref(left: ValRef<'_>, right: ValRef<'_>) -> Result<Option<Ordering>, SqlError> {
    Ok(Some(match (left, right) {
        (ValRef::Null, _) | (_, ValRef::Null) => return Ok(None),
        (ValRef::Integer(a), ValRef::Integer(b)) => a.cmp(&b),
        (ValRef::Real(a), ValRef::Real(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
        (ValRef::Integer(a), ValRef::Real(b)) => compare_i64_f64(a, b),
        (ValRef::Real(a), ValRef::Integer(b)) => compare_i64_f64(b, a).reverse(),
        (ValRef::Text(a), ValRef::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        (ValRef::Boolean(a), ValRef::Boolean(b)) => a.cmp(&b),
        _ => {
            return Err(SqlError::Type(format!(
                "cannot compare {} and {}",
                left.kind_name(),
                right.kind_name()
            )));
        }
    }))
}

#[inline]
fn real_as_exact_i64(f: f64) -> Option<i64> {
    let f = if f == 0.0 { 0.0 } else { f };
    let n = f as i64;
    (f.fract() == 0.0 && compare_i64_f64(n, f) == Ordering::Equal).then_some(n)
}

/// Hash key for REAL build columns so equal INTEGER and REAL values collide.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum NumHashKey {
    Integer(i64),
    Real(u64),
}

#[inline]
fn num_hash_key(value: ValRef<'_>) -> Option<NumHashKey> {
    Some(match value {
        ValRef::Integer(n) => NumHashKey::Integer(n),
        ValRef::Real(f) => {
            let f = if f == 0.0 { 0.0 } else { f };
            match real_as_exact_i64(f) {
                Some(n) => NumHashKey::Integer(n),
                None => NumHashKey::Real(f.to_bits()),
            }
        }
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Execution

struct Budget<'a> {
    visited: usize,
    combined: usize,
    limits: &'a SqlLimits,
}

impl Budget<'_> {
    #[inline]
    fn visit(&mut self, rows: usize) -> Result<(), SqlError> {
        self.visited += rows;
        if self.visited > self.limits.max_visited_rows {
            return Err(SqlError::Bounds(format!(
                "join visited row count exceeds max_visited_rows ({})",
                self.limits.max_visited_rows
            )));
        }
        Ok(())
    }

    #[inline]
    fn combine(&mut self) -> Result<(), SqlError> {
        self.combined += 1;
        if self.combined > self.limits.max_visited_rows {
            return Err(SqlError::Bounds(format!(
                "join intermediate row count exceeds max_visited_rows ({})",
                self.limits.max_visited_rows
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum PkKey<'a> {
    Int(i64),
    Text(&'a str),
}

/// Coerce a key to the primary-key column's domain. `None` means no row can
/// match (NULL, a fractional REAL for an INTEGER key, an oversized TEXT key,
/// or a type that cannot equal the key column).
#[inline]
fn coerce_pk_key(value: ValRef<'_>, pk_type: SqlType) -> Option<PkKey<'_>> {
    match (pk_type, value) {
        (SqlType::Integer, ValRef::Integer(n)) => Some(PkKey::Int(n)),
        (SqlType::Integer, ValRef::Real(f)) => real_as_exact_i64(f).map(PkKey::Int),
        (SqlType::Text { .. }, ValRef::Text(s)) if s.len() <= MAX_SQL_TEXT_PK_BYTES => {
            Some(PkKey::Text(s))
        }
        _ => None,
    }
}

#[inline]
fn write_pk_id<'b>(key: PkKey<'_>, buf: &'b mut [u8; 256]) -> &'b str {
    match key {
        PkKey::Int(n) => {
            let mut tmp = [0u8; 18];
            encode_i64_pk_bytes(n, &mut tmp);
            buf[..18].copy_from_slice(&tmp);
            std::str::from_utf8(&buf[..18]).expect("ASCII hex PK")
        }
        PkKey::Text(s) => {
            buf[0] = b't';
            buf[1] = b':';
            let end = 2 + s.len();
            buf[2..end].copy_from_slice(s.as_bytes());
            std::str::from_utf8(&buf[..end]).expect("valid UTF-8 PK")
        }
    }
}

#[inline]
fn pk_val_matches_id(value: ValRef<'_>, id: &str) -> bool {
    match value {
        ValRef::Integer(n) => {
            let mut buf = [0u8; 18];
            encode_i64_pk_bytes(n, &mut buf);
            id.as_bytes() == buf
        }
        ValRef::Text(s) if s.len() <= MAX_SQL_TEXT_PK_BYTES => id.strip_prefix("t:") == Some(s),
        _ => false,
    }
}

/// A stored member as a row decoder sees it: a JSON value, or a borrowed
/// flat scalar, which answers each question as the JSON value would.
trait Member<'a> {
    fn is_null(&self) -> bool;
    fn as_i64(&self) -> Option<i64>;
    fn as_f64(&self) -> Option<f64>;
    fn as_str(&self) -> Option<&'a str>;
    fn as_bool(&self) -> Option<bool>;
}

impl<'a> Member<'a> for &'a Value {
    fn is_null(&self) -> bool {
        Value::is_null(self)
    }
    fn as_i64(&self) -> Option<i64> {
        Value::as_i64(self)
    }
    fn as_f64(&self) -> Option<f64> {
        Value::as_f64(self)
    }
    fn as_str(&self) -> Option<&'a str> {
        Value::as_str(self)
    }
    fn as_bool(&self) -> Option<bool> {
        Value::as_bool(self)
    }
}

impl<'a> Member<'a> for FlatScalar<'a> {
    fn is_null(&self) -> bool {
        matches!(self, FlatScalar::Null)
    }
    fn as_i64(&self) -> Option<i64> {
        FlatScalar::as_i64(self)
    }
    fn as_f64(&self) -> Option<f64> {
        FlatScalar::as_f64(self)
    }
    fn as_str(&self) -> Option<&'a str> {
        match self {
            FlatScalar::Str(text) => Some(text),
            _ => None,
        }
    }
    fn as_bool(&self) -> Option<bool> {
        match self {
            FlatScalar::Bool(value) => Some(*value),
            _ => None,
        }
    }
}

fn corrupt(message: String) -> SqlError {
    SqlError::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn decode_borrowed_members<'a, M: Member<'a>>(
    def: &TableDef,
    id: &str,
    get: impl Fn(&str) -> Option<M>,
    out: &mut [ValRef<'a>],
) -> Result<(), SqlError> {
    for (index, column) in def.columns.iter().enumerate() {
        let value = match get(&column.name) {
            None => None,
            Some(value) if value.is_null() => None,
            Some(value) => Some(value),
        };
        out[index] = match value {
            None => {
                if column.not_null {
                    return Err(corrupt(format!(
                        "stored NOT NULL column '{}.{}' is null",
                        def.name, column.name
                    )));
                }
                ValRef::Null
            }
            Some(value) => match column.col_type {
                SqlType::Integer => value.as_i64().map(ValRef::Integer),
                SqlType::Real => value.as_f64().filter(|f| f.is_finite()).map(ValRef::Real),
                SqlType::Text { .. } => value.as_str().map(ValRef::Text),
                SqlType::Boolean => value.as_bool().map(ValRef::Boolean),
            }
            .ok_or_else(|| {
                corrupt(format!(
                    "stored column '{}.{}' does not match its type",
                    def.name, column.name
                ))
            })?,
        };
    }
    let matches = if def.has_scalar_pk() {
        pk_val_matches_id(out[def.primary_key_index], id)
    } else {
        let compound = exec::compound_row_id(def, |index| {
            Ok(match out[index] {
                ValRef::Integer(n) => exec::SqlValueRef::Integer(n),
                ValRef::Text(s) => exec::SqlValueRef::Text(s),
                _ => exec::SqlValueRef::Null,
            })
        });
        compound.is_ok_and(|compound| compound == id)
    };
    if !matches {
        return Err(corrupt(format!(
            "row ID does not match its primary key in table '{}'",
            def.name
        )));
    }
    Ok(())
}

fn decode_borrowed_stored<'a>(
    def: &TableDef,
    id: &str,
    stored: &'a StoredDocument<'a>,
    out: &mut [ValRef<'a>],
) -> Result<(), SqlError> {
    match stored {
        StoredDocument::Flat(document) => {
            decode_borrowed_members(def, id, |name| document.get(name), out)
        }
        StoredDocument::Full(document) => {
            let object = document
                .as_object()
                .ok_or_else(|| corrupt(format!("stored row in '{}' is not an object", def.name)))?;
            decode_borrowed_members(def, id, |name| object.get(name), out)
        }
    }
}

/// Decodes a stored document into a right-sized stack buffer (avoiding zeroing
/// all 64 column slots for narrow tables) and passes the borrowed slice to `f`.
#[inline]
fn with_decoded_stored<R>(
    def: &TableDef,
    id: &str,
    stored: &StoredDocument<'_>,
    f: impl FnOnce(&[ValRef<'_>]) -> Result<R, SqlError>,
) -> Result<R, SqlError> {
    let cols = def.columns.len();
    if cols <= 4 {
        let mut vals = [ValRef::Null; 4];
        decode_borrowed_stored(def, id, stored, &mut vals[..cols])?;
        f(&vals[..cols])
    } else if cols <= 8 {
        let mut vals = [ValRef::Null; 8];
        decode_borrowed_stored(def, id, stored, &mut vals[..cols])?;
        f(&vals[..cols])
    } else if cols <= 16 {
        let mut vals = [ValRef::Null; 16];
        decode_borrowed_stored(def, id, stored, &mut vals[..cols])?;
        f(&vals[..cols])
    } else {
        let mut vals = [ValRef::Null; MAX_SQL_COLUMNS];
        decode_borrowed_stored(def, id, stored, &mut vals[..cols])?;
        f(&vals[..cols])
    }
}

fn limit_value(value: Option<LimitValue>, params: &[SqlValue]) -> Result<Option<usize>, SqlError> {
    Ok(match value {
        None => None,
        Some(LimitValue::Literal(n)) => Some(n),
        Some(LimitValue::Param(index)) => match &params[index] {
            SqlValue::Integer(n) if *n >= 0 => Some(usize::try_from(*n).unwrap_or(usize::MAX)),
            SqlValue::Integer(_) => {
                return Err(SqlError::Bounds(
                    "LIMIT or OFFSET must not be negative".into(),
                ));
            }
            other => {
                return Err(SqlError::Type(format!(
                    "LIMIT or OFFSET must be INTEGER, got {}",
                    other.kind_name()
                )));
            }
        },
    })
}

pub(crate) fn execute(
    db: &DocumentDatabase<'_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    exec::run_tx(db, |tx| run(tx, statement, params, limits, cache))
}

pub(crate) fn execute_in_tx(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    run(tx, statement, params, limits, cache)
}

/// Bound tables, per-table access and the clauses of one join statement.
#[derive(Debug)]
struct JoinPlan {
    defs: Vec<Arc<TableDef>>,
    aliases: Vec<String>,
    first: Access,
    steps: Vec<Step>,
    early: Vec<FastPred>,
    late: Vec<FastPred>,
    order: Vec<(Slot, bool, bool)>,
    sorted: bool,
    projection: Vec<Slot>,
    names: Vec<String>,
    needed: Vec<Vec<bool>>,
    all_steps_point_from: [bool; MAX_JOIN_TABLES + 1],
    prefix_all_point: [bool; MAX_JOIN_TABLES],
    proj_unique: bool,
    param_checks: Vec<ParamCheck>,
}

fn load_table_defs(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Vec<Arc<TableDef>>, SqlError> {
    let mut defs: Vec<Arc<TableDef>> = Vec::with_capacity(statement.tables.len());
    for (index, table) in statement.tables.iter().enumerate() {
        if let Some(query) = &table.query {
            let columns = query.columns(tx, limits, cache)?;
            let mut names = std::collections::HashSet::new();
            if columns.iter().any(|col| !names.insert(col.name.as_str())) {
                return Err(SqlError::Syntax {
                    offset: 0,
                    message: format!("derived table '{}' has duplicate column names", table.alias),
                });
            }
            // Materialized sources share the binder's column representation
            // but have no catalog record, stored row ID, or index.
            defs.push(Arc::new(TableDef {
                version: 0,
                name: table.name.clone(),
                columns,
                primary_key_index: 0,
                additional_primary_key_columns: Vec::new(),
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                referenced_by: Vec::new(),
                tuple_foreign_keys: Vec::new(),
                tuple_referenced_by: Vec::new(),
                checks: Vec::new(),
            }));
        } else if let Some(prev) = statement.tables[..index]
            .iter()
            .position(|other| other.query.is_none() && other.name == table.name)
        {
            defs.push(Arc::clone(&defs[prev]));
        } else {
            defs.push(Arc::clone(
                &catalog::load_table_cached(tx, &table.name, cache.tables())?.def,
            ));
        }
    }
    Ok(defs)
}

pub(super) fn read_columns(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Vec<ColumnDef>, SqlError> {
    if aggregate::is_aggregate(statement) {
        return aggregate::prepare(tx, statement, limits, cache)?.columns();
    }
    let plan = plan(tx, statement, &[], limits, cache)?;
    Ok(plan
        .projection
        .iter()
        .zip(&plan.names)
        .map(|(slot, name)| ColumnDef {
            name: name.clone(),
            col_type: plan.defs[slot.table].columns[slot.column].col_type,
            not_null: false,
            default: None,
        })
        .collect())
}

fn plan(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Arc<JoinPlan>, SqlError> {
    let defs = load_table_defs(tx, statement, limits, cache)?;
    {
        let guard = statement.cached_plan.read();
        if let Some(existing) = guard.as_ref() {
            if existing.defs.len() == defs.len()
                && existing
                    .defs
                    .iter()
                    .zip(&defs)
                    .all(|(a, b)| Arc::ptr_eq(a, b))
            {
                if existing.projection.len() > limits.max_columns {
                    return Err(SqlError::Bounds(format!(
                        "join result has more than max_columns ({}) columns",
                        limits.max_columns
                    )));
                }
                for check in &existing.param_checks {
                    check.check(params)?;
                }
                return Ok(Arc::clone(existing));
            }
        }
    }

    let aliases: Vec<String> = statement.tables.iter().map(|t| t.alias.clone()).collect();
    for (index, alias) in aliases.iter().enumerate() {
        if aliases[..index].contains(alias) {
            return Err(SqlError::Syntax {
                offset: 0,
                message: format!("table name or alias '{alias}' is used twice; give each an alias"),
            });
        }
    }
    let all = defs.len();
    let mut param_checks = Vec::new();

    // ON clauses see the tables joined so far; WHERE, ORDER BY and the
    // select list see all of them.
    let mut steps = Vec::with_capacity(statement.joins.len());
    for (index, join) in statement.joins.iter().enumerate() {
        let table = index + 1;
        let on = {
            let mut binder = Binder {
                defs: &defs,
                aliases: &aliases,
                visible: table + 1,
                param_checks: &mut param_checks,
            };
            conjuncts(binder.bind_predicate(&join.on, "ON")?)
        };
        let access = choose_access(
            table,
            &defs[table],
            &on,
            join.kind.preserves_right() || statement.tables[table].query.is_some(),
        );
        steps.push(Step {
            kind: join.kind,
            on: on.into_iter().map(FastPred::compile).collect(),
            access,
        });
    }
    let preserves_right = steps.iter().any(|step| step.kind.preserves_right());
    let (early_exprs, late_exprs): (Vec<BExpr>, Vec<BExpr>) = match &statement.filter {
        Some(filter) => {
            let mut binder = Binder {
                defs: &defs,
                aliases: &aliases,
                visible: all,
                param_checks: &mut param_checks,
            };
            conjuncts(binder.bind_predicate(filter, "WHERE")?)
                .into_iter()
                .partition(|conjunct| {
                    !preserves_right && conjunct.max_table().is_none_or(|max| max == 0)
                })
        }
        None => (Vec::new(), Vec::new()),
    };
    let order: Vec<(Slot, bool, bool)> = {
        let binder = Binder {
            defs: &defs,
            aliases: &aliases,
            visible: all,
            param_checks: &mut param_checks,
        };
        statement
            .order_by
            .iter()
            .map(|item| {
                let slot = binder.resolve(&item.column)?;
                Ok((
                    slot,
                    item.descending,
                    item.nulls_first.unwrap_or(!item.descending),
                ))
            })
            .collect::<Result<_, SqlError>>()?
    };
    let mut projection: Vec<Slot> = Vec::new();
    let mut names = Vec::new();
    {
        let binder = Binder {
            defs: &defs,
            aliases: &aliases,
            visible: all,
            param_checks: &mut param_checks,
        };
        for item in &statement.items {
            let tables: Vec<usize> = match item {
                SelectItem::Aggregate(call, _) => {
                    return Err(SqlError::Syntax {
                        offset: call.offset,
                        message: "an aggregate join is planned as its inner join".into(),
                    });
                }
                SelectItem::All => (0..all).collect(),
                SelectItem::TableAll(alias, offset) => {
                    vec![aliases.iter().position(|a| a == alias).ok_or_else(|| {
                        SqlError::Syntax {
                            offset: *offset,
                            message: format!("unknown table or alias '{alias}'"),
                        }
                    })?]
                }
                SelectItem::Column(column) => {
                    let slot = binder.resolve(column)?;
                    projection.push(slot);
                    names.push(column.column.clone());
                    continue;
                }
            };
            for table in tables {
                for (column, def) in defs[table].columns.iter().enumerate() {
                    projection.push(Slot { table, column });
                    names.push(def.name.clone());
                }
            }
        }
    }
    if projection.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "join result has more than max_columns ({}) columns",
            limits.max_columns
        )));
    }
    for check in &param_checks {
        check.check(params)?;
    }

    let first_is_stored = statement.tables[0].query.is_none();
    let first = if first_is_stored {
        choose_access(0, &defs[0], &early_exprs, false)
    } else {
        Access::Loop
    };
    let early: Vec<FastPred> = early_exprs.into_iter().map(FastPred::compile).collect();
    let late: Vec<FastPred> = late_exprs.into_iter().map(FastPred::compile).collect();
    let mut all_steps_point_from = [false; MAX_JOIN_TABLES + 1];
    all_steps_point_from[steps.len()] = true;
    for i in (0..steps.len()).rev() {
        all_steps_point_from[i] =
            all_steps_point_from[i + 1] && matches!(steps[i].access, Access::Point(_));
    }
    let mut prefix_all_point = [false; MAX_JOIN_TABLES];
    prefix_all_point[0] = true;
    for t in 1..all {
        let prev_point = if t == 1 {
            matches!(first, Access::Point(_))
        } else {
            matches!(steps[t - 2].access, Access::Point(_))
        };
        prefix_all_point[t] = prefix_all_point[t - 1] && prev_point;
    }

    // Rows come out in the first table's primary-key order when the first
    // table is read by PK lookup, full scan, or single-column equality index.
    // `ORDER BY` on that key alone is therefore already satisfied, as is any
    // trailing tie-breaker when every join step matches at most one row.
    let first_pk_ordered = match &first {
        Access::Point(_) | Access::Hash { .. } | Access::Loop => true,
        Access::Index { single_column, .. } => *single_column,
    };
    let first_key = Slot {
        table: 0,
        column: defs[0].primary_key_index,
    };
    let order_satisfied = order.is_empty()
        || (!preserves_right
            && first_is_stored
            && defs[0].has_scalar_pk()
            && first_pk_ordered
            && (matches!(order.as_slice(), [(slot, false, _)] if *slot == first_key)
                || (all_steps_point_from[0]
                    && matches!(order.first(), Some((slot, false, _)) if *slot == first_key))));
    let sorted = !order_satisfied;

    let mut needed: Vec<Vec<bool>> = defs
        .iter()
        .map(|def| vec![false; def.columns.len()])
        .collect();
    for expr in &early {
        expr.mark_needed(&mut needed);
    }
    for expr in &late {
        expr.mark_needed(&mut needed);
    }
    for (idx, step) in steps.iter().enumerate() {
        for expr in &step.on {
            expr.mark_needed(&mut needed);
        }
        match &step.access {
            Access::Point(key) => {
                needed[idx + 1][defs[idx + 1].primary_key_index] = true;
                key.mark_needed(&mut needed);
            }
            Access::Index { column, key, .. } | Access::Hash { column, key } => {
                needed[idx + 1][*column] = true;
                key.mark_needed(&mut needed);
            }
            Access::Loop => {}
        }
    }
    for &(slot, _, _) in &order {
        needed[slot.table][slot.column] = true;
    }
    for &slot in &projection {
        needed[slot.table][slot.column] = true;
    }
    let proj_unique = (0..projection.len()).all(|i| !projection[..i].contains(&projection[i]));

    let built_plan = Arc::new(JoinPlan {
        defs,
        aliases,
        first,
        steps,
        early,
        late,
        order,
        sorted,
        projection,
        names,
        needed,
        all_steps_point_from,
        prefix_all_point,
        proj_unique,
        param_checks,
    });
    *statement.cached_plan.write() = Some(Arc::clone(&built_plan));
    Ok(built_plan)
}

// ---------------------------------------------------------------------------
// Hash tables, point/index caches, and output sinks

enum HashBuckets {
    Integer(HashMap<i64, (RowIdx, RowIdx)>),
    Real(HashMap<NumHashKey, (RowIdx, RowIdx)>),
    Text(HashMap<String, (RowIdx, RowIdx)>),
    Boolean([Option<(RowIdx, RowIdx)>; 2]),
}

struct BuiltHash {
    buckets: HashBuckets,
    next: Vec<RowIdx>,
}

impl BuiltHash {
    fn new(col_type: SqlType) -> Self {
        let buckets = match col_type {
            SqlType::Integer => HashBuckets::Integer(HashMap::new()),
            SqlType::Real => HashBuckets::Real(HashMap::new()),
            SqlType::Text { .. } => HashBuckets::Text(HashMap::new()),
            SqlType::Boolean => HashBuckets::Boolean([None, None]),
        };
        Self {
            buckets,
            next: Vec::new(),
        }
    }

    fn insert(&mut self, key: ValRef<'_>, idx: RowIdx) {
        debug_assert_eq!(idx as usize, self.next.len());
        self.next.push(NO_NEXT);
        let next = &mut self.next;
        let mut append = |entry: &mut (RowIdx, RowIdx)| {
            next[entry.1 as usize] = idx;
            entry.1 = idx;
        };
        match (&mut self.buckets, key) {
            (HashBuckets::Integer(map), ValRef::Integer(n)) => {
                map.entry(n).and_modify(append).or_insert((idx, idx));
            }
            (HashBuckets::Real(map), ValRef::Real(_) | ValRef::Integer(_)) => {
                if let Some(k) = num_hash_key(key) {
                    map.entry(k).and_modify(append).or_insert((idx, idx));
                }
            }
            (HashBuckets::Text(map), ValRef::Text(s)) => {
                if let Some(entry) = map.get_mut(s) {
                    append(entry);
                } else {
                    map.insert(s.to_owned(), (idx, idx));
                }
            }
            (HashBuckets::Boolean(slots), ValRef::Boolean(b)) => {
                let slot = &mut slots[usize::from(b)];
                match slot {
                    Some(entry) => append(entry),
                    None => *slot = Some((idx, idx)),
                }
            }
            _ => {}
        }
    }

    #[inline]
    fn probe(&self, key: ValRef<'_>) -> Option<RowIdx> {
        match (&self.buckets, key) {
            (HashBuckets::Integer(map), ValRef::Integer(n)) => map.get(&n).map(|&(head, _)| head),
            (HashBuckets::Integer(map), ValRef::Real(f)) => {
                real_as_exact_i64(f).and_then(|n| map.get(&n).map(|&(head, _)| head))
            }
            (HashBuckets::Real(map), ValRef::Integer(_) | ValRef::Real(_)) => {
                num_hash_key(key).and_then(|k| map.get(&k).map(|&(head, _)| head))
            }
            (HashBuckets::Text(map), ValRef::Text(s)) => map.get(s).map(|&(head, _)| head),
            (HashBuckets::Boolean(slots), ValRef::Boolean(b)) => {
                slots[usize::from(b)].map(|(head, _)| head)
            }
            _ => None,
        }
    }
}

enum Built {
    Hash(BuiltHash),
    Loop(RowIdx),
}

const MAX_POINT_CACHE_ENTRIES: usize = 4096;

#[derive(Clone, Copy)]
enum CacheSlot {
    Pending,
    Absent,
    Present(RowIdx),
}

enum PointCache {
    Disabled,
    Integer(HashMap<i64, CacheSlot>),
    Text(HashMap<String, CacheSlot>),
}

impl PointCache {
    fn new(enabled: bool, pk_type: SqlType) -> Self {
        if !enabled {
            return Self::Disabled;
        }
        match pk_type {
            SqlType::Integer => Self::Integer(HashMap::new()),
            SqlType::Text { .. } => Self::Text(HashMap::new()),
            _ => Self::Disabled,
        }
    }
}

const MAX_INDEX_CACHE_KEYS: usize = 1024;
const MAX_INDEX_CACHE_ROWS: usize = 8192;

enum IndexCache {
    Disabled,
    Integer {
        map: HashMap<i64, (RowIdx, u32)>,
        cached_rows: usize,
    },
    Text {
        map: HashMap<String, (RowIdx, u32)>,
        cached_rows: usize,
    },
    Boolean {
        slots: [Option<(RowIdx, u32)>; 2],
    },
}

impl IndexCache {
    fn new(enabled: bool, col_type: SqlType) -> Self {
        if !enabled {
            return Self::Disabled;
        }
        match col_type {
            SqlType::Integer => Self::Integer {
                map: HashMap::new(),
                cached_rows: 0,
            },
            SqlType::Text { .. } => Self::Text {
                map: HashMap::new(),
                cached_rows: 0,
            },
            SqlType::Boolean => Self::Boolean {
                slots: [None, None],
            },
            SqlType::Real => Self::Disabled,
        }
    }

    fn get(&self, value: &IndexValue) -> Option<(RowIdx, u32)> {
        match (self, value) {
            (Self::Integer { map, .. }, IndexValue::Integer(n)) => map.get(n).copied(),
            (Self::Text { map, .. }, IndexValue::String(s)) => map.get(s.as_str()).copied(),
            (Self::Boolean { slots }, IndexValue::Boolean(b)) => slots[usize::from(*b)],
            _ => None,
        }
    }

    fn insert(&mut self, value: IndexValue, start: RowIdx, count: u32) {
        let add = count as usize;
        match (self, value) {
            (Self::Integer { map, cached_rows }, IndexValue::Integer(n)) => {
                if map.len() < MAX_INDEX_CACHE_KEYS && *cached_rows + add <= MAX_INDEX_CACHE_ROWS {
                    map.insert(n, (start, count));
                    *cached_rows += add;
                }
            }
            (Self::Text { map, cached_rows }, IndexValue::String(s)) => {
                if map.len() < MAX_INDEX_CACHE_KEYS && *cached_rows + add <= MAX_INDEX_CACHE_ROWS {
                    map.insert(s, (start, count));
                    *cached_rows += add;
                }
            }
            (Self::Boolean { slots }, IndexValue::Boolean(b)) => {
                slots[usize::from(b)] = Some((start, count));
            }
            _ => {}
        }
    }
}

#[derive(Clone, Copy)]
enum OrderSource {
    Projected(usize),
    Extra(usize),
}

struct HeapRow {
    projected: Vec<SqlValue>,
    extra: Vec<SqlValue>,
    seq: usize,
    spec: Arc<[(OrderSource, bool, bool)]>,
}

impl HeapRow {
    #[inline]
    fn order_val(&self, source: OrderSource) -> ValRef<'_> {
        match source {
            OrderSource::Projected(i) => ValRef::from_sql(&self.projected[i]),
            OrderSource::Extra(i) => ValRef::from_sql(&self.extra[i]),
        }
    }
}

#[inline]
fn compare_order_pair(
    x: ValRef<'_>,
    y: ValRef<'_>,
    descending: bool,
    nulls_first: bool,
) -> Ordering {
    match (x, y) {
        (ValRef::Null, ValRef::Null) => Ordering::Equal,
        (ValRef::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, ValRef::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        _ => match compare_ref(x, y) {
            Ok(Some(ordering)) if descending => ordering.reverse(),
            Ok(Some(ordering)) => ordering,
            _ => Ordering::Equal,
        },
    }
}

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapRow {}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        for &(source, descending, nulls_first) in self.spec.iter() {
            let ord = compare_order_pair(
                self.order_val(source),
                other.order_val(source),
                descending,
                nulls_first,
            );
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.seq.cmp(&other.seq)
    }
}

enum OutputSink {
    Stream {
        rows: Vec<Vec<SqlValue>>,
        offset: usize,
        skipped: usize,
        cap: usize,
        limited: bool,
        move_unique: bool,
    },
    TopK {
        heap: BinaryHeap<HeapRow>,
        cap: usize,
        seq: usize,
        extra_slots: Vec<Slot>,
        spec: Arc<[(OrderSource, bool, bool)]>,
    },
    SortAll {
        rows: Vec<Combined>,
        max_sort_rows: usize,
    },
}

impl OutputSink {
    fn new(plan: &JoinPlan, wanted: Option<usize>, offset: usize, limits: &SqlLimits) -> Self {
        if !plan.sorted {
            let (cap, limited) = match wanted {
                Some(w) if w <= limits.max_select_rows.saturating_add(offset) => (w, true),
                _ => (limits.max_select_rows.saturating_add(offset), false),
            };
            let single_possible_row =
                matches!(plan.first, Access::Point(_)) && plan.all_steps_point_from[0];
            Self::Stream {
                rows: Vec::with_capacity(if limited {
                    cap.saturating_sub(offset).min(64)
                } else {
                    0
                }),
                offset,
                skipped: 0,
                cap,
                limited,
                move_unique: single_possible_row && plan.proj_unique,
            }
        } else if let Some(k) = wanted.filter(|&k| k <= limits.max_sort_rows) {
            let mut extra_slots = Vec::new();
            let mut spec = Vec::with_capacity(plan.order.len());
            for &(slot, descending, nulls_first) in &plan.order {
                let source = if let Some(pos) = plan.projection.iter().position(|&p| p == slot) {
                    OrderSource::Projected(pos)
                } else if let Some(pos) = extra_slots.iter().position(|&p| p == slot) {
                    OrderSource::Extra(pos)
                } else {
                    let pos = extra_slots.len();
                    extra_slots.push(slot);
                    OrderSource::Extra(pos)
                };
                spec.push((source, descending, nulls_first));
            }
            Self::TopK {
                heap: BinaryHeap::with_capacity(k.min(256)),
                cap: k,
                seq: 0,
                extra_slots,
                spec: Arc::from(spec),
            }
        } else {
            Self::SortAll {
                rows: Vec::new(),
                max_sort_rows: limits.max_sort_rows,
            }
        }
    }

    #[inline]
    fn is_done(&self) -> bool {
        match self {
            Self::Stream {
                rows,
                skipped,
                cap,
                limited,
                ..
            } => *limited && *skipped + rows.len() >= *cap,
            Self::TopK { .. } | Self::SortAll { .. } => false,
        }
    }

    #[inline]
    fn remaining_wanted(&self) -> Option<usize> {
        match self {
            Self::Stream {
                rows,
                skipped,
                cap,
                limited: true,
                ..
            } => Some(cap.saturating_sub(*skipped + rows.len())),
            _ => None,
        }
    }

    #[inline]
    fn can_clear_unshared_arena(&self) -> bool {
        !matches!(self, Self::SortAll { .. })
    }

    fn push(
        &mut self,
        comb: Combined,
        arenas: &mut [TableArena],
        plan: &JoinPlan,
    ) -> Result<(), SqlError> {
        match self {
            Self::Stream {
                rows,
                offset,
                skipped,
                cap,
                limited,
                move_unique,
            } => {
                if *skipped + rows.len() >= *cap {
                    if *limited {
                        return Ok(());
                    }
                    return Err(SqlError::Bounds(format!(
                        "join result exceeds its row bound ({}; see max_select_rows and max_sort_rows)",
                        *cap
                    )));
                }
                if *skipped < *offset {
                    *skipped += 1;
                    return Ok(());
                }
                let mut out_row = Vec::with_capacity(plan.projection.len());
                if *move_unique {
                    for &slot in &plan.projection {
                        let idx = comb[slot.table];
                        if idx == NULL_ROW {
                            out_row.push(SqlValue::Null);
                        } else {
                            let stride = arenas[slot.table].stride;
                            let pos = idx as usize * stride + slot.column;
                            out_row.push(std::mem::replace(
                                &mut arenas[slot.table].data[pos],
                                SqlValue::Null,
                            ));
                        }
                    }
                } else {
                    for &slot in &plan.projection {
                        let idx = comb[slot.table];
                        if idx == NULL_ROW {
                            out_row.push(SqlValue::Null);
                        } else {
                            out_row.push(arenas[slot.table].row(idx)[slot.column].clone());
                        }
                    }
                }
                rows.push(out_row);
                Ok(())
            }
            Self::TopK {
                heap,
                cap,
                seq,
                extra_slots,
                spec,
            } => {
                let my_seq = *seq;
                *seq += 1;
                if heap.len() == *cap {
                    if let Some(worst) = heap.peek() {
                        let mut ord = Ordering::Equal;
                        for (i, &(slot, descending, nulls_first)) in plan.order.iter().enumerate() {
                            let cand_val = slot_val(&comb, arenas, slot);
                            let worst_val = worst.order_val(spec[i].0);
                            ord = compare_order_pair(cand_val, worst_val, descending, nulls_first);
                            if ord != Ordering::Equal {
                                break;
                            }
                        }
                        if ord == Ordering::Equal {
                            ord = my_seq.cmp(&worst.seq);
                        }
                        if ord != Ordering::Less {
                            return Ok(());
                        }
                    }
                    heap.pop();
                }
                let mut projected = Vec::with_capacity(plan.projection.len());
                for &slot in &plan.projection {
                    let idx = comb[slot.table];
                    if idx == NULL_ROW {
                        projected.push(SqlValue::Null);
                    } else {
                        projected.push(arenas[slot.table].row(idx)[slot.column].clone());
                    }
                }
                let mut extra = Vec::with_capacity(extra_slots.len());
                for &slot in extra_slots.iter() {
                    let idx = comb[slot.table];
                    if idx == NULL_ROW {
                        extra.push(SqlValue::Null);
                    } else {
                        extra.push(arenas[slot.table].row(idx)[slot.column].clone());
                    }
                }
                heap.push(HeapRow {
                    projected,
                    extra,
                    seq: my_seq,
                    spec: Arc::clone(spec),
                });
                Ok(())
            }
            Self::SortAll {
                rows,
                max_sort_rows,
            } => {
                if rows.len() >= *max_sort_rows {
                    return Err(SqlError::Bounds(format!(
                        "join result exceeds its row bound ({}; see max_select_rows and max_sort_rows)",
                        *max_sort_rows
                    )));
                }
                rows.push(comb);
                Ok(())
            }
        }
    }

    fn finish(
        self,
        arenas: &mut [TableArena],
        plan: &JoinPlan,
        wanted: Option<usize>,
        offset: usize,
        limits: &SqlLimits,
    ) -> Result<Vec<Vec<SqlValue>>, SqlError> {
        let rows = match self {
            Self::Stream { rows, .. } => rows,
            Self::TopK { heap, .. } => {
                let sorted = heap.into_sorted_vec();
                let start = offset.min(sorted.len());
                sorted
                    .into_iter()
                    .skip(start)
                    .map(|row| row.projected)
                    .collect()
            }
            Self::SortAll { mut rows, .. } => {
                let mut failure = None;
                // A stable sort keeps the join order for ties, so results are
                // deterministic.
                if let &[(slot, descending, nulls_first)] = plan.order.as_slice() {
                    rows.sort_by(|a, b| {
                        let x = slot_val(a, arenas, slot);
                        let y = slot_val(b, arenas, slot);
                        match (x, y) {
                            (ValRef::Null, ValRef::Null) => Ordering::Equal,
                            (ValRef::Null, _) => {
                                if nulls_first {
                                    Ordering::Less
                                } else {
                                    Ordering::Greater
                                }
                            }
                            (_, ValRef::Null) => {
                                if nulls_first {
                                    Ordering::Greater
                                } else {
                                    Ordering::Less
                                }
                            }
                            _ => match compare_ref(x, y) {
                                Ok(Some(ordering)) if descending => ordering.reverse(),
                                Ok(Some(ordering)) => ordering,
                                Ok(None) => Ordering::Equal,
                                Err(error) => {
                                    failure.get_or_insert(error);
                                    Ordering::Equal
                                }
                            },
                        }
                    });
                } else {
                    rows.sort_by(|a, b| {
                        for &(slot, descending, nulls_first) in &plan.order {
                            let x = slot_val(a, arenas, slot);
                            let y = slot_val(b, arenas, slot);
                            let ordering = match (x, y) {
                                (ValRef::Null, ValRef::Null) => Ordering::Equal,
                                (ValRef::Null, _) => {
                                    if nulls_first {
                                        Ordering::Less
                                    } else {
                                        Ordering::Greater
                                    }
                                }
                                (_, ValRef::Null) => {
                                    if nulls_first {
                                        Ordering::Greater
                                    } else {
                                        Ordering::Less
                                    }
                                }
                                _ => match compare_ref(x, y) {
                                    Ok(Some(ordering)) if descending => ordering.reverse(),
                                    Ok(Some(ordering)) => ordering,
                                    Ok(None) => Ordering::Equal,
                                    Err(error) => {
                                        failure.get_or_insert(error);
                                        Ordering::Equal
                                    }
                                },
                            };
                            if ordering != Ordering::Equal {
                                return ordering;
                            }
                        }
                        Ordering::Equal
                    });
                }
                if let Some(error) = failure {
                    return Err(error);
                }
                let end = wanted.unwrap_or(usize::MAX).min(rows.len());
                let start = offset.min(end);
                // When every join step matches at most one row, each table-0
                // row appears at most once in `rows`, so unique projections
                // can move table-0 values out without cloning Strings.
                let move_table0 = plan.all_steps_point_from[0] && plan.proj_unique;
                rows[start..end]
                    .iter()
                    .map(|comb| {
                        let mut out_row = Vec::with_capacity(plan.projection.len());
                        for &slot in &plan.projection {
                            let idx = comb[slot.table];
                            if idx == NULL_ROW {
                                out_row.push(SqlValue::Null);
                            } else if move_table0 && slot.table == 0 {
                                let stride = arenas[0].stride;
                                let pos = idx as usize * stride + slot.column;
                                out_row.push(std::mem::replace(
                                    &mut arenas[0].data[pos],
                                    SqlValue::Null,
                                ));
                            } else {
                                out_row.push(arenas[slot.table].row(idx)[slot.column].clone());
                            }
                        }
                        out_row
                    })
                    .collect()
            }
        };
        if rows.len() > limits.max_select_rows {
            return Err(SqlError::Bounds(format!(
                "SELECT result exceeds max_select_rows ({})",
                limits.max_select_rows
            )));
        }
        Ok(rows)
    }
}

/// The index value a key must equal, or `None` if no indexed value can equal
/// it. Indexed TEXT values are at most 256 bytes; REAL columns are not
/// indexable, and an INTEGER column matches only integral numbers.
#[inline]
fn index_value_ref(key: ValRef<'_>, column_type: SqlType) -> Option<IndexValue> {
    match (column_type, key) {
        (_, ValRef::Null) => None,
        (SqlType::Integer, ValRef::Integer(n)) => Some(IndexValue::Integer(n)),
        (SqlType::Integer, ValRef::Real(f)) => real_as_exact_i64(f).map(IndexValue::Integer),
        (SqlType::Text { .. }, ValRef::Text(s)) if s.len() <= 256 => {
            Some(IndexValue::String(s.to_owned()))
        }
        (SqlType::Boolean, ValRef::Boolean(b)) => Some(IndexValue::Boolean(b)),
        _ => None,
    }
}

struct Executor<'a, 'b, 'tx, 'store> {
    tx: &'a DocumentTransaction<'tx, 'store>,
    plan: &'a JoinPlan,
    metas: &'a [Arc<dyn std::any::Any + Send + Sync>],
    built: &'a [Option<Built>],
    params: &'b [SqlValue],
    wanted: Option<usize>,
    arenas: [TableArena; MAX_JOIN_TABLES],
    point_caches: [PointCache; MAX_JOIN_TABLES],
    index_caches: [IndexCache; MAX_JOIN_TABLES],
    /// For right-preserving steps, indexed by position in the built arena.
    matched_right: [Option<Vec<bool>>; MAX_JOIN_TABLES],
    budget: Budget<'a>,
    sink: OutputSink,
}

impl Executor<'_, '_, '_, '_> {
    fn lookup_point_one(
        &mut self,
        table: usize,
        pk: PkKey<'_>,
    ) -> Result<Option<RowIdx>, SqlError> {
        self.budget.visit(1)?;
        match (&self.point_caches[table - 1], pk) {
            (PointCache::Integer(map), PkKey::Int(n)) => {
                if let Some(&slot) = map.get(&n) {
                    return Ok(match slot {
                        CacheSlot::Present(idx) => Some(idx),
                        CacheSlot::Absent | CacheSlot::Pending => None,
                    });
                }
            }
            (PointCache::Text(map), PkKey::Text(s)) => {
                if let Some(&slot) = map.get(s) {
                    return Ok(match slot {
                        CacheSlot::Present(idx) => Some(idx),
                        CacheSlot::Absent | CacheSlot::Pending => None,
                    });
                }
            }
            _ => {}
        }

        let def = &self.plan.defs[table];
        let needed = &self.plan.needed[table];
        let mut id_buf = [0u8; 256];
        let id = write_pk_id(pk, &mut id_buf);
        let mut decode_result: Option<Result<RowIdx, SqlError>> = None;
        let arena = &mut self.arenas[table];
        self.tx
            .read_stored_document_with_meta(&def.name, self.metas[table].as_ref(), id, |stored| {
                decode_result = Some(with_decoded_stored(def, id, &stored, |vals| {
                    Ok(arena.push_borrowed(vals, needed))
                }));
            })
            .map_err(exec::classify_io_error)?;
        let row_idx = decode_result.transpose()?;
        let cache_slot = match row_idx {
            Some(idx) => CacheSlot::Present(idx),
            None => CacheSlot::Absent,
        };
        match (&mut self.point_caches[table - 1], pk) {
            (PointCache::Integer(map), PkKey::Int(n)) if map.len() < MAX_POINT_CACHE_ENTRIES => {
                map.insert(n, cache_slot);
            }
            (PointCache::Text(map), PkKey::Text(s)) if map.len() < MAX_POINT_CACHE_ENTRIES => {
                map.insert(s.to_owned(), cache_slot);
            }
            _ => {}
        }
        Ok(row_idx)
    }

    fn process_table0_batch(&mut self, batch: Vec<Combined>) -> Result<(), SqlError> {
        if batch.is_empty() {
            return Ok(());
        }
        if !self.plan.sorted && self.wanted.is_some() && self.plan.all_steps_point_from[0] {
            let mut offset = 0;
            while offset < batch.len() && !self.sink.is_done() {
                let rem = self.sink.remaining_wanted().unwrap_or(batch.len() - offset);
                if rem == 0 {
                    break;
                }
                let end = (offset + rem).min(batch.len());
                self.extend_batch(0, batch[offset..end].to_vec())?;
                offset = end;
            }
            return Ok(());
        }
        self.extend_batch(0, batch)
    }

    fn extend_batch(&mut self, done: usize, batch: Vec<Combined>) -> Result<(), SqlError> {
        if batch.is_empty() || self.sink.is_done() {
            return Ok(());
        }
        let Some(step) = self.plan.steps.get(done) else {
            for comb in batch {
                let mut keep = true;
                for conjunct in &self.plan.late {
                    if !truthy_fast_pred(conjunct, &comb, &self.arenas, self.params)? {
                        keep = false;
                        break;
                    }
                }
                if keep {
                    self.sink.push(comb, &mut self.arenas, self.plan)?;
                    if self.sink.is_done() {
                        return Ok(());
                    }
                }
            }
            return Ok(());
        };

        let table = done + 1;
        let can_batch_point = batch.len() > 1
            && matches!(step.access, Access::Point(_))
            && (self.plan.sorted || self.wanted.is_none() || self.plan.all_steps_point_from[done])
            && !matches!(self.point_caches[done], PointCache::Disabled);

        if can_batch_point {
            if let Access::Point(key_expr) = &step.access {
                let def = &self.plan.defs[table];
                let pk_type = def.columns[def.primary_key_index].col_type;
                let mut n_visits = 0_usize;
                for comb in &batch {
                    let val = eval_ref(key_expr, comb, &self.arenas, self.params)?;
                    if coerce_pk_key(val, pk_type).is_some() {
                        n_visits += 1;
                    }
                }
                if self.budget.visited + n_visits <= self.budget.limits.max_visited_rows {
                    self.prefetch_point_batch(done, table, key_expr, pk_type, &batch)?;
                    let mut next_batch = Vec::with_capacity(batch.len());
                    for mut comb in batch {
                        let pk = {
                            let val = eval_ref(key_expr, &comb, &self.arenas, self.params)?;
                            match coerce_pk_key(val, pk_type) {
                                Some(PkKey::Int(n)) => Some((true, n, None)),
                                Some(PkKey::Text(s)) => Some((false, 0, Some(s.to_owned()))),
                                None => None,
                            }
                        };
                        let mut matched = false;
                        if let Some((is_int, n, text)) = pk {
                            self.budget.visit(1)?;
                            let cached = if is_int {
                                match &self.point_caches[done] {
                                    PointCache::Integer(map) => map.get(&n).copied(),
                                    _ => None,
                                }
                            } else {
                                match (&self.point_caches[done], text.as_deref()) {
                                    (PointCache::Text(map), Some(s)) => map.get(s).copied(),
                                    _ => None,
                                }
                            };
                            if let Some(CacheSlot::Present(row_idx)) = cached {
                                self.budget.combine()?;
                                comb[table] = row_idx;
                                let mut keep = true;
                                for conjunct in &step.on {
                                    if !truthy_fast_pred(
                                        conjunct,
                                        &comb,
                                        &self.arenas,
                                        self.params,
                                    )? {
                                        keep = false;
                                        break;
                                    }
                                }
                                if keep {
                                    matched = true;
                                    next_batch.push(comb);
                                }
                            }
                        }
                        if !matched && step.kind.preserves_left() {
                            self.budget.combine()?;
                            comb[table] = NULL_ROW;
                            next_batch.push(comb);
                        }
                    }
                    return self.extend_batch(table, next_batch);
                }
            }
        }

        for comb in batch {
            self.extend_one(done, comb)?;
            if self.sink.is_done() {
                return Ok(());
            }
        }
        Ok(())
    }

    fn prefetch_point_batch(
        &mut self,
        done: usize,
        table: usize,
        key_expr: &BExpr,
        pk_type: SqlType,
        batch: &[Combined],
    ) -> Result<(), SqlError> {
        let def = &self.plan.defs[table];
        let needed = &self.plan.needed[table];
        match pk_type {
            SqlType::Integer => {
                if let PointCache::Integer(map) = &mut self.point_caches[done] {
                    if map.len() + batch.len() > MAX_POINT_CACHE_ENTRIES {
                        map.clear();
                    }
                    let mut to_fetch: Vec<i64> = Vec::new();
                    for comb in batch {
                        let val = eval_ref(key_expr, comb, &self.arenas, self.params)?;
                        if let Some(PkKey::Int(n)) = coerce_pk_key(val, pk_type) {
                            if let std::collections::hash_map::Entry::Vacant(e) = map.entry(n) {
                                e.insert(CacheSlot::Pending);
                                to_fetch.push(n);
                            }
                        }
                    }
                    if to_fetch.is_empty() {
                        return Ok(());
                    }
                    let mut raw_ids: Vec<[u8; 18]> = Vec::with_capacity(to_fetch.len());
                    for &n in &to_fetch {
                        let mut buf = [0u8; 18];
                        encode_i64_pk_bytes(n, &mut buf);
                        raw_ids.push(buf);
                    }
                    let id_refs: Vec<&str> = raw_ids
                        .iter()
                        .map(|b| std::str::from_utf8(b).expect("ASCII hex PK"))
                        .collect();
                    let mut results = vec![CacheSlot::Absent; to_fetch.len()];
                    let mut failure: Option<SqlError> = None;
                    let arena = &mut self.arenas[table];
                    self.tx
                        .read_stored_documents_batch_with_meta(
                            &def.name,
                            self.metas[table].as_ref(),
                            &id_refs,
                            |i, id, stored| {
                                if let Some(stored) = stored {
                                    match with_decoded_stored(def, id, &stored, |vals| {
                                        Ok(arena.push_borrowed(vals, needed))
                                    }) {
                                        Ok(row_idx) => results[i] = CacheSlot::Present(row_idx),
                                        Err(err) => {
                                            failure = Some(err);
                                            return false;
                                        }
                                    }
                                }
                                true
                            },
                        )
                        .map_err(exec::classify_io_error)?;
                    if let Some(err) = failure {
                        return Err(err);
                    }
                    for (n, slot) in to_fetch.into_iter().zip(results) {
                        map.insert(n, slot);
                    }
                }
            }
            SqlType::Text { .. } => {
                if let PointCache::Text(map) = &mut self.point_caches[done] {
                    if map.len() + batch.len() > MAX_POINT_CACHE_ENTRIES {
                        map.clear();
                    }
                    let mut to_fetch: Vec<String> = Vec::new();
                    for comb in batch {
                        let val = eval_ref(key_expr, comb, &self.arenas, self.params)?;
                        if let Some(PkKey::Text(s)) = coerce_pk_key(val, pk_type) {
                            if !map.contains_key(s) {
                                let owned = s.to_owned();
                                map.insert(owned.clone(), CacheSlot::Pending);
                                to_fetch.push(owned);
                            }
                        }
                    }
                    if to_fetch.is_empty() {
                        return Ok(());
                    }
                    let mut id_bytes: Vec<u8> = Vec::new();
                    let mut id_ranges: Vec<(usize, usize)> = Vec::with_capacity(to_fetch.len());
                    for s in &to_fetch {
                        let start = id_bytes.len();
                        id_bytes.extend_from_slice(b"t:");
                        id_bytes.extend_from_slice(s.as_bytes());
                        id_ranges.push((start, id_bytes.len()));
                    }
                    let id_refs: Vec<&str> = id_ranges
                        .iter()
                        .map(|&(s, e)| std::str::from_utf8(&id_bytes[s..e]).expect("valid UTF-8"))
                        .collect();
                    let mut results = vec![CacheSlot::Absent; to_fetch.len()];
                    let mut failure: Option<SqlError> = None;
                    let arena = &mut self.arenas[table];
                    self.tx
                        .read_stored_documents_batch_with_meta(
                            &def.name,
                            self.metas[table].as_ref(),
                            &id_refs,
                            |i, id, stored| {
                                if let Some(stored) = stored {
                                    match with_decoded_stored(def, id, &stored, |vals| {
                                        Ok(arena.push_borrowed(vals, needed))
                                    }) {
                                        Ok(row_idx) => results[i] = CacheSlot::Present(row_idx),
                                        Err(err) => {
                                            failure = Some(err);
                                            return false;
                                        }
                                    }
                                }
                                true
                            },
                        )
                        .map_err(exec::classify_io_error)?;
                    if let Some(err) = failure {
                        return Err(err);
                    }
                    for (s, slot) in to_fetch.into_iter().zip(results) {
                        map.insert(s, slot);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn extend_one(&mut self, done: usize, mut comb: Combined) -> Result<(), SqlError> {
        let Some(step) = self.plan.steps.get(done) else {
            for conjunct in &self.plan.late {
                if !truthy_fast_pred(conjunct, &comb, &self.arenas, self.params)? {
                    return Ok(());
                }
            }
            self.sink.push(comb, &mut self.arenas, self.plan)?;
            return Ok(());
        };
        let table = done + 1;
        let def = &self.plan.defs[table];
        let mut matched = false;
        match (&step.access, &self.built[done]) {
            (Access::Point(key_expr), _) => {
                let pk_type = def.columns[def.primary_key_index].col_type;
                let pk_owned = {
                    let val = eval_ref(key_expr, &comb, &self.arenas, self.params)?;
                    match coerce_pk_key(val, pk_type) {
                        Some(PkKey::Int(n)) => Some((true, n, String::new())),
                        Some(PkKey::Text(s)) => Some((false, 0, s.to_owned())),
                        None => None,
                    }
                };
                if let Some((is_int, n, text)) = pk_owned {
                    let pk = if is_int {
                        PkKey::Int(n)
                    } else {
                        PkKey::Text(text.as_str())
                    };
                    if let Some(row_idx) = self.lookup_point_one(table, pk)? {
                        self.budget.combine()?;
                        comb[table] = row_idx;
                        let mut keep = true;
                        for conjunct in &step.on {
                            if !truthy_fast_pred(conjunct, &comb, &self.arenas, self.params)? {
                                keep = false;
                                break;
                            }
                        }
                        if keep {
                            matched = true;
                            self.extend_one(table, comb)?;
                            if self.sink.is_done() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            (
                Access::Index {
                    name, column, key, ..
                },
                _,
            ) => {
                let index_val = {
                    let val = eval_ref(key, &comb, &self.arenas, self.params)?;
                    index_value_ref(val, def.columns[*column].col_type)
                };
                if let Some(index_val) = index_val {
                    if let Some((start, count)) = self.index_caches[done].get(&index_val) {
                        self.budget.visit(count as usize)?;
                        for offset in 0..count {
                            self.budget.combine()?;
                            comb[table] = start + offset;
                            let mut keep = true;
                            for conjunct in &step.on {
                                if !truthy_fast_pred(conjunct, &comb, &self.arenas, self.params)? {
                                    keep = false;
                                    break;
                                }
                            }
                            if keep {
                                matched = true;
                                self.extend_one(table, comb)?;
                                if self.sink.is_done() {
                                    return Ok(());
                                }
                            }
                        }
                    } else {
                        let start_idx = self.arenas[table].len() as RowIdx;
                        let can_cache = !matches!(self.index_caches[done], IndexCache::Disabled);
                        let mut cursor: Option<Vec<u8>> = None;
                        let mut total_fetched: u32 = 0;
                        let mut completed_all = true;
                        loop {
                            let page_start = self.arenas[table].len() as RowIdx;
                            let mut failure: Option<SqlError> = None;
                            let needed = &self.plan.needed[table];
                            let arena = &mut self.arenas[table];
                            let (count, next) = self
                                .tx
                                .visit_index_stored_eq_with_meta(
                                    (&def.name, self.metas[table].as_ref()),
                                    name,
                                    &index_val,
                                    cursor.as_deref(),
                                    256,
                                    |id, stored| match with_decoded_stored(
                                        def,
                                        id,
                                        &stored,
                                        |vals| {
                                            arena.push_borrowed(vals, needed);
                                            Ok(())
                                        },
                                    ) {
                                        Ok(()) => true,
                                        Err(err) => {
                                            failure = Some(err);
                                            false
                                        }
                                    },
                                )
                                .map_err(exec::classify_io_error)?;
                            if let Some(err) = failure {
                                return Err(err);
                            }
                            if count == 0 {
                                break;
                            }
                            self.budget.visit(count)?;
                            total_fetched += count as u32;
                            for offset in 0..count as u32 {
                                self.budget.combine()?;
                                comb[table] = page_start + offset;
                                let mut keep = true;
                                for conjunct in &step.on {
                                    if !truthy_fast_pred(
                                        conjunct,
                                        &comb,
                                        &self.arenas,
                                        self.params,
                                    )? {
                                        keep = false;
                                        break;
                                    }
                                }
                                if keep {
                                    matched = true;
                                    self.extend_one(table, comb)?;
                                    if self.sink.is_done() {
                                        completed_all = false;
                                        break;
                                    }
                                }
                            }
                            if !completed_all {
                                break;
                            }
                            match next {
                                Some(next_cursor) => cursor = Some(next_cursor),
                                None => break,
                            }
                        }
                        if completed_all && can_cache {
                            self.index_caches[done].insert(index_val, start_idx, total_fetched);
                        } else if !can_cache && self.sink.can_clear_unshared_arena() {
                            self.arenas[table].clear();
                        }
                        if self.sink.is_done() {
                            return Ok(());
                        }
                    }
                }
            }
            (Access::Hash { key, .. }, Some(Built::Hash(hash))) => {
                let head = {
                    let val = eval_ref(key, &comb, &self.arenas, self.params)?;
                    hash.probe(val)
                };
                if let Some(mut cur) = head {
                    while cur != NO_NEXT {
                        self.budget.combine()?;
                        comb[table] = cur;
                        let mut keep = true;
                        for conjunct in &step.on {
                            if !truthy_fast_pred(conjunct, &comb, &self.arenas, self.params)? {
                                keep = false;
                                break;
                            }
                        }
                        if keep {
                            matched = true;
                            if let Some(seen) = &mut self.matched_right[done] {
                                seen[cur as usize] = true;
                            }
                            self.extend_one(table, comb)?;
                            if self.sink.is_done() {
                                return Ok(());
                            }
                        }
                        cur = hash.next[cur as usize];
                    }
                }
            }
            (Access::Loop, Some(Built::Loop(count))) => {
                for row_idx in 0..*count {
                    self.budget.combine()?;
                    comb[table] = row_idx;
                    let mut keep = true;
                    for conjunct in &step.on {
                        if !truthy_fast_pred(conjunct, &comb, &self.arenas, self.params)? {
                            keep = false;
                            break;
                        }
                    }
                    if keep {
                        matched = true;
                        if let Some(seen) = &mut self.matched_right[done] {
                            seen[row_idx as usize] = true;
                        }
                        self.extend_one(table, comb)?;
                        if self.sink.is_done() {
                            return Ok(());
                        }
                    }
                }
            }
            _ => unreachable!("join step without its built side"),
        }
        if !matched && step.kind.preserves_left() {
            self.budget.combine()?;
            comb[table] = NULL_ROW;
            self.extend_one(table, comb)?;
        }
        Ok(())
    }

    fn emit_unmatched_right(&mut self) -> Result<(), SqlError> {
        // An unmatched row at this step can still match a later step.
        // Finish each step in join order before examining the next one.
        for done in 0..self.plan.steps.len() {
            let Some(seen) = self.matched_right[done].take() else {
                continue;
            };
            for (row_idx, matched) in seen.into_iter().enumerate() {
                if self.sink.is_done() {
                    return Ok(());
                }
                if !matched {
                    let table = done + 1;
                    let mut combined = [NULL_ROW; MAX_JOIN_TABLES];
                    combined[table] = row_idx as RowIdx;
                    self.budget.combine()?;
                    self.extend_one(table, combined)?;
                }
            }
        }
        Ok(())
    }
}

fn run(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    if let Some(source) = &statement.source {
        return derived::run(tx, statement, source, params, limits, cache);
    }
    if aggregate::is_aggregate(statement) {
        return aggregate::run(tx, statement, params, limits, cache);
    }
    let join_plan = plan(tx, statement, params, limits, cache)?;
    let limit = limit_value(statement.limit, params)?;
    let offset = limit_value(statement.offset, params)?.unwrap_or(0);
    if limit == Some(0) {
        return Ok(SqlOutcome::Rows {
            columns: join_plan.names.clone(),
            rows: Vec::new(),
        });
    }

    let n_tables = join_plan.defs.len();
    let mut table_names = [""; MAX_JOIN_TABLES];
    let mut stored_count = 0;
    let mut materialized: [Option<Arc<Vec<Vec<SqlValue>>>>; MAX_JOIN_TABLES] =
        [const { None }; MAX_JOIN_TABLES];
    for (index, table) in statement.tables.iter().enumerate() {
        if let Some(query) = &table.query {
            let prior = statement.tables[..index].iter().position(|other| {
                other
                    .query
                    .as_ref()
                    .is_some_and(|other| Arc::ptr_eq(query, other))
            });
            let rows = if let Some(prior) = prior {
                Arc::clone(
                    materialized[prior]
                        .as_ref()
                        .expect("earlier derived source"),
                )
            } else {
                query.rows(tx, limits, cache)?
            };
            materialized[index] = Some(rows);
        } else {
            table_names[stored_count] = table.name.as_str();
            stored_count += 1;
        }
    }
    let stored_metas = tx
        .load_stored_metadata_batch(&table_names[..stored_count])
        .map_err(exec::classify_io_error)?;
    let metas = if stored_count == n_tables {
        stored_metas
    } else {
        let mut stored_metas = stored_metas.into_iter();
        materialized[..n_tables]
            .iter()
            .map(|source| {
                if source.is_some() {
                    Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>
                } else {
                    stored_metas.next().expect("one handle per stored table")
                }
            })
            .collect()
    };

    let mut arenas: [TableArena; MAX_JOIN_TABLES] = std::array::from_fn(|i| {
        TableArena::new(join_plan.defs.get(i).map_or(0, |d| d.columns.len()))
    });
    let mut budget = Budget {
        visited: 0,
        combined: 0,
        limits,
    };

    // Build sides: hash tables and loop tables are read once.
    let mut built: [Option<Built>; MAX_JOIN_TABLES] = [const { None }; MAX_JOIN_TABLES];
    for (index, step) in join_plan.steps.iter().enumerate() {
        let table = index + 1;
        let def = &join_plan.defs[table];
        let needed = &join_plan.needed[table];
        if let Some(rows) = &materialized[table] {
            budget.visit(rows.len())?;
            let arena = &mut arenas[table];
            built[index] = match &step.access {
                Access::Hash { column, .. } => {
                    let mut hash = BuiltHash::new(def.columns[*column].col_type);
                    for row in rows.iter() {
                        let values: Vec<_> = row.iter().map(ValRef::from_sql).collect();
                        let key = values[*column];
                        if step.kind.preserves_right() || !matches!(key, ValRef::Null) {
                            let index = arena.push_borrowed(&values, needed);
                            hash.insert(key, index);
                        }
                    }
                    Some(Built::Hash(hash))
                }
                Access::Loop => {
                    for row in rows.iter() {
                        let values: Vec<_> = row.iter().map(ValRef::from_sql).collect();
                        arena.push_borrowed(&values, needed);
                    }
                    Some(Built::Loop(arena.len() as RowIdx))
                }
                _ => unreachable!("derived sources have no stored index"),
            };
            continue;
        }
        built[index] = match &step.access {
            Access::Point(_) | Access::Index { .. } => None,
            Access::Hash { column, .. } => {
                let mut hash = BuiltHash::new(def.columns[*column].col_type);
                let mut after: Option<String> = None;
                let mut page_size = 256;
                loop {
                    let rem_visit = limits.max_visited_rows.saturating_sub(budget.visited);
                    let fetch = page_size
                        .min(rem_visit.saturating_add(1))
                        .clamp(1, MAX_SCAN_ENTRIES);
                    let mut failure: Option<SqlError> = None;
                    let arena = &mut arenas[table];
                    let (count, last) = tx
                        .visit_stored_documents_with_meta(
                            &def.name,
                            metas[table].as_ref(),
                            after.as_deref(),
                            fetch,
                            |id, stored| match with_decoded_stored(def, id, &stored, |vals| {
                                let key_val = vals[*column];
                                if step.kind.preserves_right() || !matches!(key_val, ValRef::Null) {
                                    let row_idx = arena.push_borrowed(vals, needed);
                                    hash.insert(key_val, row_idx);
                                }
                                Ok(())
                            }) {
                                Ok(()) => true,
                                Err(err) => {
                                    failure = Some(err);
                                    false
                                }
                            },
                        )
                        .map_err(exec::classify_io_error)?;
                    page_size = (page_size * 2).min(MAX_SCAN_ENTRIES);
                    if let Some(err) = failure {
                        return Err(err);
                    }
                    if count == 0 {
                        break;
                    }
                    budget.visit(count)?;
                    after = last;
                }
                Some(Built::Hash(hash))
            }
            Access::Loop => {
                let mut after: Option<String> = None;
                let mut page_size = 256;
                loop {
                    let rem_visit = limits.max_visited_rows.saturating_sub(budget.visited);
                    let fetch = page_size
                        .min(rem_visit.saturating_add(1))
                        .clamp(1, MAX_SCAN_ENTRIES);
                    let mut failure: Option<SqlError> = None;
                    let arena = &mut arenas[table];
                    let (count, last) = tx
                        .visit_stored_documents_with_meta(
                            &def.name,
                            metas[table].as_ref(),
                            after.as_deref(),
                            fetch,
                            |id, stored| match with_decoded_stored(def, id, &stored, |vals| {
                                arena.push_borrowed(vals, needed);
                                Ok(())
                            }) {
                                Ok(()) => true,
                                Err(err) => {
                                    failure = Some(err);
                                    false
                                }
                            },
                        )
                        .map_err(exec::classify_io_error)?;
                    page_size = (page_size * 2).min(MAX_SCAN_ENTRIES);
                    if let Some(err) = failure {
                        return Err(err);
                    }
                    if count == 0 {
                        break;
                    }
                    budget.visit(count)?;
                    after = last;
                }
                Some(Built::Loop(arenas[table].len() as RowIdx))
            }
        };
    }

    let wanted = limit.map(|limit| limit.saturating_add(offset));
    let mut point_caches: [PointCache; MAX_JOIN_TABLES] =
        [const { PointCache::Disabled }; MAX_JOIN_TABLES];
    let mut index_caches: [IndexCache; MAX_JOIN_TABLES] =
        [const { IndexCache::Disabled }; MAX_JOIN_TABLES];
    for (idx, step) in join_plan.steps.iter().enumerate() {
        let table = idx + 1;
        let def = &join_plan.defs[table];
        if matches!(step.access, Access::Point(_)) && !join_plan.prefix_all_point[table] {
            point_caches[idx] = PointCache::new(true, def.columns[def.primary_key_index].col_type);
        } else if let Access::Index { column, .. } = &step.access {
            if !join_plan.prefix_all_point[table] {
                index_caches[idx] = IndexCache::new(true, def.columns[*column].col_type);
            }
        }
    }
    let sink = OutputSink::new(&join_plan, wanted, offset, limits);
    let matched_right = std::array::from_fn(|done| {
        join_plan
            .steps
            .get(done)
            .filter(|step| step.kind.preserves_right())
            .map(|_| vec![false; arenas[done + 1].len()])
    });

    let mut executor = Executor {
        tx,
        plan: &join_plan,
        metas: &metas,
        built: &built[..join_plan.steps.len()],
        params,
        wanted,
        arenas,
        point_caches,
        index_caches,
        matched_right,
        budget,
        sink,
    };

    let def0 = &join_plan.defs[0];
    let needed0 = &join_plan.needed[0];
    let empty_comb = [NULL_ROW; MAX_JOIN_TABLES];

    match (&materialized[0], &join_plan.first) {
        (Some(rows), _) => {
            for page in rows.chunks(256) {
                let mut batch = Vec::new();
                for row in page {
                    executor.budget.visit(1)?;
                    let values: Vec<_> = row.iter().map(ValRef::from_sql).collect();
                    if passes_early(&join_plan.early, &values, params)? {
                        let mut combined = empty_comb;
                        combined[0] = executor.arenas[0].push_borrowed(&values, needed0);
                        batch.push(combined);
                    }
                }
                executor.process_table0_batch(batch)?;
                if executor.sink.is_done() {
                    break;
                }
                if executor.sink.can_clear_unshared_arena() {
                    executor.arenas[0].clear();
                }
            }
        }
        (None, Access::Point(key_expr)) => {
            let pk_type = def0.columns[def0.primary_key_index].col_type;
            let val = eval_ref(key_expr, &empty_comb, &executor.arenas, params)?;
            if let Some(pk) = coerce_pk_key(val, pk_type) {
                executor.budget.visit(1)?;
                let mut id_buf = [0u8; 256];
                let id = write_pk_id(pk, &mut id_buf);
                let mut matched_idx: Option<Result<Option<RowIdx>, SqlError>> = None;
                let arena0 = &mut executor.arenas[0];
                tx.read_stored_document_with_meta(&def0.name, metas[0].as_ref(), id, |stored| {
                    matched_idx = Some(with_decoded_stored(def0, id, &stored, |vals| {
                        if passes_early(&join_plan.early, vals, params)? {
                            Ok(Some(arena0.push_borrowed(vals, needed0)))
                        } else {
                            Ok(None)
                        }
                    }));
                })
                .map_err(exec::classify_io_error)?;
                if let Some(row_idx) = matched_idx.transpose()?.flatten() {
                    let mut comb = empty_comb;
                    comb[0] = row_idx;
                    executor.extend_one(0, comb)?;
                }
            }
        }
        (
            None,
            Access::Index {
                name, column, key, ..
            },
        ) => {
            let val = eval_ref(key, &empty_comb, &executor.arenas, params)?;
            if let Some(index_val) = index_value_ref(val, def0.columns[*column].col_type) {
                let mut cursor: Option<Vec<u8>> = None;
                let mut page_limit = executor
                    .sink
                    .remaining_wanted()
                    .map_or(256, |rem| rem.clamp(1, 256));
                loop {
                    let mut failure: Option<SqlError> = None;
                    let mut batch: Vec<Combined> = Vec::new();
                    let arena0 = &mut executor.arenas[0];
                    let (count, next) = tx
                        .visit_index_stored_eq_with_meta(
                            (&def0.name, metas[0].as_ref()),
                            name,
                            &index_val,
                            cursor.as_deref(),
                            page_limit,
                            |id, stored| match with_decoded_stored(def0, id, &stored, |vals| {
                                if passes_early(&join_plan.early, vals, params)? {
                                    let row_idx = arena0.push_borrowed(vals, needed0);
                                    let mut comb = empty_comb;
                                    comb[0] = row_idx;
                                    batch.push(comb);
                                }
                                Ok(())
                            }) {
                                Ok(()) => true,
                                Err(err) => {
                                    failure = Some(err);
                                    false
                                }
                            },
                        )
                        .map_err(exec::classify_io_error)?;
                    if let Some(err) = failure {
                        return Err(err);
                    }
                    if count == 0 {
                        break;
                    }
                    executor.budget.visit(count)?;
                    executor.process_table0_batch(batch)?;
                    if executor.sink.is_done() {
                        break;
                    }
                    if executor.sink.can_clear_unshared_arena() {
                        executor.arenas[0].clear();
                    }
                    match next {
                        Some(next_cursor) => {
                            cursor = Some(next_cursor);
                            page_limit = (page_limit * 2).min(256);
                        }
                        None => break,
                    }
                }
            }
        }
        (None, Access::Hash { .. } | Access::Loop) => {
            let mut after: Option<String> = None;
            let mut page_size = match executor.sink.remaining_wanted() {
                Some(rem) if join_plan.early.is_empty() => rem.clamp(1, MAX_SCAN_ENTRIES),
                Some(rem) => rem.saturating_mul(16).clamp(64, 512),
                None => 256,
            };
            let mut match_cap_scale = 1_usize;
            loop {
                let rem_visit = limits
                    .max_visited_rows
                    .saturating_sub(executor.budget.visited);
                let fetch = page_size
                    .min(rem_visit.saturating_add(1))
                    .clamp(1, MAX_SCAN_ENTRIES);
                let match_cap = executor.sink.remaining_wanted().map(|rem| {
                    rem.saturating_mul(match_cap_scale)
                        .clamp(1, MAX_SCAN_ENTRIES)
                });
                let mut failure: Option<SqlError> = None;
                let mut batch: Vec<Combined> = Vec::new();
                let arena0 = &mut executor.arenas[0];
                let (count, last) = tx
                    .visit_stored_documents_with_meta(
                        &def0.name,
                        metas[0].as_ref(),
                        after.as_deref(),
                        fetch,
                        |id, stored| match with_decoded_stored(def0, id, &stored, |vals| {
                            if passes_early(&join_plan.early, vals, params)? {
                                let row_idx = arena0.push_borrowed(vals, needed0);
                                let mut comb = empty_comb;
                                comb[0] = row_idx;
                                batch.push(comb);
                                Ok(match_cap.is_none_or(|cap| batch.len() < cap))
                            } else {
                                Ok(true)
                            }
                        }) {
                            Ok(go_on) => go_on,
                            Err(err) => {
                                failure = Some(err);
                                false
                            }
                        },
                    )
                    .map_err(exec::classify_io_error)?;
                page_size = (page_size * 2).min(MAX_SCAN_ENTRIES);
                if let Some(err) = failure {
                    return Err(err);
                }
                if count == 0 {
                    break;
                }
                executor.budget.visit(count)?;
                after = last;
                let before_rem = executor.sink.remaining_wanted();
                let batch_len = batch.len();
                executor.process_table0_batch(batch)?;
                if executor.sink.is_done() {
                    break;
                }
                if let (Some(before), Some(after_rem)) =
                    (before_rem, executor.sink.remaining_wanted())
                {
                    if batch_len > 0 && before - after_rem <= batch_len / 2 {
                        match_cap_scale = (match_cap_scale * 2).min(16);
                    }
                }
                if executor.sink.can_clear_unshared_arena() {
                    executor.arenas[0].clear();
                }
            }
        }
    }

    executor.emit_unmatched_right()?;
    super::subquery::record_visited_rows(executor.budget.visited);
    let rows = executor
        .sink
        .finish(&mut executor.arenas, &join_plan, wanted, offset, limits)?;
    Ok(SqlOutcome::Rows {
        columns: join_plan.names.clone(),
        rows,
    })
}

/// Describes a join's plan, one row per table and one for ordering, without
/// reading rows: the same binding and access choice as execution.
pub(crate) fn explain(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    describe_plan(tx, statement, params, limits, cache, true)
}

pub(crate) fn validate_subquery(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<(), SqlError> {
    describe_plan(tx, statement, params, limits, cache, false).map(|_| ())
}

fn describe_plan(
    tx: &DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
    estimate: bool,
) -> Result<Vec<Vec<SqlValue>>, SqlError> {
    if let Some(source) = &statement.source {
        let kind = match &source.inner {
            super::parser::Statement::Join(_) => "join",
            _ => "single-table SELECT",
        };
        return Ok(vec![vec![
            SqlValue::Text("derived".into()),
            SqlValue::Text(statement.tables[0].alias.clone()),
            SqlValue::Text(format!(
                "materialize the source {kind}, at most max_select_rows rows"
            )),
            SqlValue::Text("WHERE, grouping, ORDER BY and LIMIT apply to its rows".into()),
        ]]);
    }
    if aggregate::is_aggregate(statement) {
        let prepared = aggregate::prepare(tx, statement, limits, cache)?;
        let mut rows = describe_plan(tx, &prepared.inner, params, limits, cache, estimate)?;
        rows.push(vec![
            SqlValue::Text("aggregate".into()),
            SqlValue::Text(String::new()),
            SqlValue::Text(prepared.describe()),
            SqlValue::Text("HAVING, ORDER BY and LIMIT apply to the groups".into()),
        ]);
        return Ok(rows);
    }
    let plan = plan(tx, statement, params, limits, cache)?;
    let limit = limit_value(statement.limit, params)?;
    let offset = limit_value(statement.offset, params)?.unwrap_or(0);
    let wanted = limit.map(|l| l.saturating_add(offset));
    let text = |value: String| SqlValue::Text(value);
    let mut rows = Vec::new();
    for (index, def) in plan.defs.iter().enumerate() {
        let access = if index == 0 {
            &plan.first
        } else {
            &plan.steps[index - 1].access
        };
        let column = |column: usize| def.columns[column].name.clone();
        let description = match access {
            Access::Point(_) => "primary-key lookup".to_owned(),
            Access::Index { name, .. } => format!("index {name} lookup"),
            Access::Hash { column: c, .. } if index > 0 => {
                format!("hash table on {} built once", column(*c))
            }
            Access::Loop if index > 0 => "nested loop over every row".to_owned(),
            Access::Hash { .. } | Access::Loop if statement.tables[index].query.is_some() => {
                "scan materialized source".to_owned()
            }
            Access::Hash { .. } | Access::Loop => "full scan in primary-key order".to_owned(),
        };
        let detail = if index == 0 {
            match plan.early.len() {
                0 => "first table".to_owned(),
                n => format!("first table; {n} WHERE condition(s) checked while reading"),
            }
        } else {
            let kind = match plan.steps[index - 1].kind {
                JoinKind::Inner => "INNER JOIN",
                JoinKind::Left => "LEFT JOIN",
                JoinKind::Right => "RIGHT JOIN",
                JoinKind::Full => "FULL JOIN",
            };
            format!("{kind}; ON checked on every candidate")
        };
        let estimate = if !estimate {
            String::new()
        } else if let Some(source) = &statement.tables[index].query {
            let description = if matches!(source.as_ref(), derived::Source::Recursive(_)) {
                "recursive materialized source"
            } else {
                "materialized source"
            };
            format!(
                "{description}; candidate rows: at most {}",
                limits.max_select_rows
            )
        } else {
            match access {
                Access::Point(_) => "candidate rows: at most 1 per lookup".to_owned(),
                Access::Index {
                    name, column, key, ..
                } => {
                    let value = match key {
                        BExpr::Literal(value) => Some(value),
                        BExpr::Param(param) => Some(&params[*param]),
                        _ => None,
                    };
                    if let Some(value) = value {
                        let access = if let Some(value) =
                            index_value_ref(ValRef::from_sql(value), def.columns[*column].col_type)
                        {
                            let index_idx = def
                                .indexes
                                .iter()
                                .position(|index| index.name == *name)
                                .expect("planned index belongs to table");
                            AccessPlan::Index {
                                index_idx,
                                equal: vec![value],
                                lower: None,
                                upper: None,
                                unique_equality: def.indexes[index_idx].unique
                                    && def.indexes[index_idx].columns.len() == 1,
                                preserves_pk_asc: false,
                                satisfies_order_by: false,
                            }
                        } else {
                            AccessPlan::Empty
                        };
                        exec::explain_access_count(tx, def, &access, limits)?
                    } else {
                        "candidate rows: depends on the preceding row's index key".to_owned()
                    }
                }
                Access::Hash { .. } | Access::Loop => exec::explain_table_count(tx, def, limits)?,
            }
        };
        let step = if index == 0 { "scan" } else { "join" };
        rows.push(vec![
            text(step.into()),
            text(format!("{} ({})", def.name, plan.aliases[index])),
            text(description),
            text(format!("{detail}; {estimate}")),
        ]);
    }
    let order = if plan.sorted {
        if wanted.is_some_and(|k| k <= limits.max_sort_rows) {
            "top OFFSET + LIMIT rows are sorted by ORDER BY"
        } else {
            "rows are sorted by ORDER BY"
        }
    } else if plan.steps.iter().any(|step| step.kind.preserves_right()) {
        "matched rows stream, then unmatched right rows are passed to later joins"
    } else if statement.tables[0].query.is_some() {
        "rows stream in the first source's result order"
    } else if statement.limit.is_some() {
        "rows stream in the first table's key order and reading stops at OFFSET + LIMIT"
    } else {
        "rows stream in the first table's key order"
    };
    let late = plan.late.len();
    rows.push(vec![
        text("order".into()),
        text(String::new()),
        text(order.into()),
        text(if late == 0 {
            "no other WHERE conditions".into()
        } else {
            format!("{late} WHERE condition(s) checked on each joined row")
        }),
    ]);
    Ok(rows)
}
