//! Bounded INNER and LEFT joins over SQL tables.
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
use super::catalog::{self, CachedTable, IndexState, TableDef};
use super::exec::{self, SqlCache};
use super::lexer::{Token, TokenKind};
use super::parser::{CompareOp, LimitValue};
use super::value::{
    MAX_SQL_TEXT_PK_BYTES, SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue, compare_i64_f64,
    encode_i64_pk,
};
use crate::MAX_SCAN_ENTRIES;
use crate::document::{DocumentDatabase, DocumentTransaction, IndexQuery, IndexValue};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// Tables in one join statement, including the first.
pub(crate) const MAX_JOIN_TABLES: usize = 8;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JoinSelect {
    items: Vec<SelectItem>,
    tables: Vec<TableRef>,
    /// `joins[i]` joins `tables[i + 1]`.
    joins: Vec<JoinClause>,
    filter: Option<JExpr>,
    order_by: Vec<OrderItem>,
    limit: Option<LimitValue>,
    offset: Option<LimitValue>,
}

#[derive(Clone, Debug, PartialEq)]
struct TableRef {
    name: String,
    alias: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKind {
    Inner,
    Left,
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
    limits: &SqlLimits,
) -> Result<(JoinSelect, usize), SqlError> {
    let mut parser = JoinParser {
        tokens,
        pos: 0,
        params: 0,
        depth: 0,
        limits,
    };
    let statement = parser.select()?;
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

    fn select(&mut self) -> Result<JoinSelect, SqlError> {
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
        let mut tables = vec![self.table_ref()?];
        let mut joins = Vec::new();
        loop {
            if self.at_kw("right") || self.at_kw("full") {
                return Err(self.unsupported("RIGHT or FULL JOIN"));
            }
            if self.at_kw("cross") || self.at_kw("natural") {
                return Err(self.unsupported("CROSS or NATURAL JOIN"));
            }
            if matches!(self.peek().kind, TokenKind::Comma) {
                return Err(self.unsupported("comma-separated FROM tables"));
            }
            let kind = if self.eat_kw("left") {
                self.eat_kw("outer");
                JoinKind::Left
            } else {
                self.eat_kw("inner");
                JoinKind::Inner
            };
            if !self.eat_kw("join") {
                if kind == JoinKind::Left {
                    return Err(self.syntax("expected JOIN"));
                }
                break;
            }
            tables.push(self.table_ref()?);
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
            joins.push(JoinClause { kind, on });
        }
        let filter = if self.eat_kw("where") {
            Some(self.expr()?)
        } else {
            None
        };
        if self.at_kw("group") || self.at_kw("having") {
            return Err(self.unsupported("GROUP BY or HAVING in a join"));
        }
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
        let (mut limit, mut offset) = (None, None);
        if self.eat_kw("limit") {
            limit = Some(self.limit_value()?);
            if self.eat_kw("offset") {
                offset = Some(self.limit_value()?);
            }
        } else if self.at_kw("offset") {
            return Err(self.unsupported("OFFSET without LIMIT"));
        }
        Ok(JoinSelect {
            items,
            tables,
            joins,
            filter,
            order_by,
            limit,
            offset,
        })
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
        if matches!(self.peek().kind, TokenKind::LParen) {
            return Err(self.unsupported("functions or aggregates in a join"));
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

    fn table_ref(&mut self) -> Result<TableRef, SqlError> {
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
        Ok(TableRef { name, alias })
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
            TokenKind::Ident { .. } => {
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
}

struct Binder<'a> {
    defs: &'a [Arc<TableDef>],
    aliases: &'a [String],
    params: &'a [SqlValue],
    /// Tables visible to the expression being bound.
    visible: usize,
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

    fn bind(&self, expr: &JExpr) -> Result<(BExpr, Ty), SqlError> {
        let boolean = |ty: Ty, what: &str| match ty {
            Ty::Null | Ty::Typed(SqlType::Boolean) => Ok(()),
            Ty::Typed(other) => Err(SqlError::Type(format!(
                "{what} requires BOOLEAN operands, got {}",
                other.display_name()
            ))),
        };
        Ok(match expr {
            JExpr::Literal(value) => (BExpr::Literal(value.clone()), value_type(value)),
            JExpr::Param(index) => (BExpr::Param(*index), value_type(&self.params[*index])),
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
                boolean(ty, "NOT")?;
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
                    boolean(ty, what)?;
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
                if let (Ty::Typed(a), Ty::Typed(b)) = (lt, rt) {
                    if !(a.matches_base(b) || (a.is_numeric() && b.is_numeric())) {
                        return Err(SqlError::Type(format!(
                            "cannot compare {} and {}",
                            a.display_name(),
                            b.display_name()
                        )));
                    }
                }
                (
                    BExpr::Compare(*op, Box::new(l), Box::new(r)),
                    Ty::Typed(SqlType::Boolean),
                )
            }
        })
    }

    fn bind_predicate(&self, expr: &JExpr, what: &str) -> Result<BExpr, SqlError> {
        let (bound, ty) = self.bind(expr)?;
        match ty {
            Ty::Null | Ty::Typed(SqlType::Boolean) => Ok(bound),
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
enum Access {
    /// `table.pk = key`, where `key` uses only earlier tables.
    Point(BExpr),
    /// `table.column = key` through a ready secondary index whose leading
    /// column is `column`.
    Index {
        name: String,
        column: usize,
        key: BExpr,
    },
    /// `table.column = key` through a hash table built once per statement.
    Hash { column: usize, key: BExpr },
    /// Every row of the table.
    Loop,
}

struct Step {
    kind: JoinKind,
    on: Vec<BExpr>,
    access: Access,
}

fn choose_access(table: usize, def: &TableDef, on: &[BExpr]) -> Access {
    // Prefer a primary-key equality, then any column equality.
    let mut best: Option<(usize, bool, usize)> = None; // (conjunct, is_pk, column)
    for (index, conjunct) in on.iter().enumerate() {
        if let BExpr::Compare(CompareOp::Eq, left, right) = conjunct {
            for (own, other) in [(left, right), (right, left)] {
                if let BExpr::Column(slot) = own.as_ref() {
                    if slot.table == table && other.max_table().is_none_or(|max| max < table) {
                        let is_pk = slot.column == def.primary_key_index;
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

/// One combined row: a value vector per table, or `None` when a LEFT JOIN
/// extended the row with NULLs.
type Combined = Vec<Option<Arc<Vec<SqlValue>>>>;

fn eval(expr: &BExpr, row: &Combined, params: &[SqlValue]) -> Result<SqlValue, SqlError> {
    Ok(match expr {
        BExpr::Literal(value) => value.clone(),
        BExpr::Param(index) => params[*index].clone(),
        BExpr::Column(slot) => row
            .get(slot.table)
            .and_then(Option::as_ref)
            .map_or(SqlValue::Null, |values| values[slot.column].clone()),
        BExpr::IsNull(inner, negated) => {
            let is_null = matches!(eval(inner, row, params)?, SqlValue::Null);
            SqlValue::Boolean(is_null != *negated)
        }
        BExpr::Not(inner) => match eval(inner, row, params)? {
            SqlValue::Boolean(b) => SqlValue::Boolean(!b),
            _ => SqlValue::Null,
        },
        BExpr::And(items) => {
            let mut unknown = false;
            for item in items {
                match eval(item, row, params)? {
                    SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                    SqlValue::Boolean(true) => {}
                    _ => unknown = true,
                }
            }
            if unknown {
                SqlValue::Null
            } else {
                SqlValue::Boolean(true)
            }
        }
        BExpr::Or(items) => {
            let mut unknown = false;
            for item in items {
                match eval(item, row, params)? {
                    SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                    SqlValue::Boolean(false) => {}
                    _ => unknown = true,
                }
            }
            if unknown {
                SqlValue::Null
            } else {
                SqlValue::Boolean(false)
            }
        }
        BExpr::Compare(op, left, right) => {
            let (l, r) = (eval(left, row, params)?, eval(right, row, params)?);
            match compare(&l, &r)? {
                None => SqlValue::Null,
                Some(ordering) => SqlValue::Boolean(match op {
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

fn truthy(expr: &BExpr, row: &Combined, params: &[SqlValue]) -> Result<bool, SqlError> {
    Ok(matches!(eval(expr, row, params)?, SqlValue::Boolean(true)))
}

/// SQL comparison; `None` when either side is NULL.
fn compare(left: &SqlValue, right: &SqlValue) -> Result<Option<Ordering>, SqlError> {
    Ok(Some(match (left, right) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => return Ok(None),
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Real(a), SqlValue::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Integer(a), SqlValue::Real(b)) => compare_i64_f64(*a, *b),
        (SqlValue::Real(a), SqlValue::Integer(b)) => compare_i64_f64(*b, *a).reverse(),
        (SqlValue::Text(a), SqlValue::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
        _ => {
            return Err(SqlError::Type(format!(
                "cannot compare {} and {}",
                left.kind_name(),
                right.kind_name()
            )));
        }
    }))
}

/// A hash key under which values that compare equal collide. Numbers that
/// are exact integers hash as integers; other REAL values by their bits
/// (`-0.0` is normalized). NULL has no key and never matches.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum HashKey {
    Integer(i64),
    Real(u64),
    Text(String),
    Boolean(bool),
}

fn hash_key(value: &SqlValue) -> Option<HashKey> {
    Some(match value {
        SqlValue::Null => return None,
        SqlValue::Integer(n) => HashKey::Integer(*n),
        SqlValue::Real(f) => {
            let f = if *f == 0.0 { 0.0 } else { *f };
            if f.fract() == 0.0 && compare_i64_f64(f as i64, f) == Ordering::Equal {
                HashKey::Integer(f as i64)
            } else {
                HashKey::Real(f.to_bits())
            }
        }
        SqlValue::Text(s) => HashKey::Text(s.clone()),
        SqlValue::Boolean(b) => HashKey::Boolean(*b),
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

/// The document ID of a primary-key value, or `None` if no row can have it.
fn pk_id(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Integer(n) => Some(encode_i64_pk(*n)),
        SqlValue::Text(s) if s.len() <= MAX_SQL_TEXT_PK_BYTES => Some(format!("t:{s}")),
        _ => None,
    }
}

fn decode(def: &TableDef, id: &str, document: &Value) -> Result<Vec<SqlValue>, SqlError> {
    let corrupt =
        |message: String| SqlError::Io(io::Error::new(io::ErrorKind::InvalidData, message));
    let object = document
        .as_object()
        .ok_or_else(|| corrupt(format!("stored row in '{}' is not an object", def.name)))?;
    let mut row = Vec::with_capacity(def.columns.len());
    for column in &def.columns {
        let value = match object.get(&column.name) {
            None | Some(Value::Null) => {
                if column.not_null {
                    return Err(corrupt(format!(
                        "stored NOT NULL column '{}.{}' is null",
                        def.name, column.name
                    )));
                }
                SqlValue::Null
            }
            Some(value) => match column.col_type {
                SqlType::Integer => value.as_i64().map(SqlValue::Integer),
                SqlType::Real => value.as_f64().filter(|f| f.is_finite()).map(SqlValue::Real),
                SqlType::Text { .. } => value.as_str().map(|s| SqlValue::Text(s.to_owned())),
                SqlType::Boolean => value.as_bool().map(SqlValue::Boolean),
            }
            .ok_or_else(|| {
                corrupt(format!(
                    "stored column '{}.{}' does not match its type",
                    def.name, column.name
                ))
            })?,
        };
        row.push(value);
    }
    if pk_id(&row[def.primary_key_index]).as_deref() != Some(id) {
        return Err(corrupt(format!(
            "row ID does not match its primary key in table '{}'",
            def.name
        )));
    }
    Ok(row)
}

fn scan_table(
    tx: &DocumentTransaction<'_, '_>,
    def: &TableDef,
    budget: &mut Budget<'_>,
    mut each: impl FnMut(Vec<SqlValue>, &mut Budget<'_>) -> Result<bool, SqlError>,
) -> Result<(), SqlError> {
    let mut after: Option<String> = None;
    // Pages grow from 64 rows so a scan that stops early under LIMIT reads
    // little, while long scans still use full pages.
    let mut page_size = 64;
    loop {
        let page = tx
            .scan_documents(&def.name, after.as_deref(), page_size)
            .map_err(exec::classify_io_error)?;
        page_size = (page_size * 2).min(MAX_SCAN_ENTRIES);
        let Some(last) = page.last() else {
            return Ok(());
        };
        after = Some(last.id.clone());
        budget.visit(page.len())?;
        for entry in page {
            let row = decode(def, &entry.id, &entry.document)?;
            if !each(row, budget)? {
                return Ok(());
            }
        }
    }
}

/// Coerce a key to the primary-key column's domain. `None` means no row can
/// match (NULL, a fractional REAL for an INTEGER key, or a type that cannot
/// equal the key column).
fn point_key(value: SqlValue, pk_type: SqlType) -> Option<SqlValue> {
    match (pk_type, value) {
        (_, SqlValue::Null) => None,
        (SqlType::Integer, SqlValue::Integer(n)) => Some(SqlValue::Integer(n)),
        (SqlType::Integer, SqlValue::Real(f)) => {
            let n = f as i64;
            (f.fract() == 0.0 && compare_i64_f64(n, f) == Ordering::Equal)
                .then_some(SqlValue::Integer(n))
        }
        (SqlType::Text { .. }, SqlValue::Text(s)) => Some(SqlValue::Text(s)),
        _ => None,
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

fn run(
    tx: &mut DocumentTransaction<'_, '_>,
    statement: &JoinSelect,
    params: &[SqlValue],
    limits: &SqlLimits,
    cache: &SqlCache,
) -> Result<SqlOutcome, SqlError> {
    let cached: Vec<Arc<CachedTable>> = statement
        .tables
        .iter()
        .map(|table| catalog::load_table_cached(tx, &table.name, cache.tables()))
        .collect::<Result<_, _>>()?;
    let defs: Vec<Arc<TableDef>> = cached.iter().map(|entry| Arc::clone(&entry.def)).collect();
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
    let binder = |visible| Binder {
        defs: &defs,
        aliases: &aliases,
        params,
        visible,
    };

    // ON clauses see the tables joined so far; WHERE, ORDER BY and the
    // select list see all of them.
    let mut steps = Vec::with_capacity(statement.joins.len());
    for (index, join) in statement.joins.iter().enumerate() {
        let table = index + 1;
        let on = conjuncts(binder(table + 1).bind_predicate(&join.on, "ON")?);
        let access = choose_access(table, &defs[table], &on);
        steps.push(Step {
            kind: join.kind,
            on,
            access,
        });
    }
    let (early, late): (Vec<BExpr>, Vec<BExpr>) = match &statement.filter {
        Some(filter) => conjuncts(binder(all).bind_predicate(filter, "WHERE")?)
            .into_iter()
            .partition(|conjunct| conjunct.max_table().is_none_or(|max| max == 0)),
        None => (Vec::new(), Vec::new()),
    };
    let order: Vec<(Slot, bool, bool)> = statement
        .order_by
        .iter()
        .map(|item| {
            let slot = binder(all).resolve(&item.column)?;
            Ok((
                slot,
                item.descending,
                item.nulls_first.unwrap_or(!item.descending),
            ))
        })
        .collect::<Result<_, SqlError>>()?;
    let mut projection: Vec<Slot> = Vec::new();
    let mut names = Vec::new();
    for item in &statement.items {
        let tables: Vec<usize> = match item {
            SelectItem::All => (0..all).collect(),
            SelectItem::TableAll(alias, offset) => vec![
                aliases
                    .iter()
                    .position(|a| a == alias)
                    .ok_or_else(|| SqlError::Syntax {
                        offset: *offset,
                        message: format!("unknown table or alias '{alias}'"),
                    })?,
            ],
            SelectItem::Column(column) => {
                let slot = binder(all).resolve(column)?;
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
    if projection.len() > limits.max_columns {
        return Err(SqlError::Bounds(format!(
            "join result has more than max_columns ({}) columns",
            limits.max_columns
        )));
    }
    let limit = limit_value(statement.limit, params)?;
    let offset = limit_value(statement.offset, params)?.unwrap_or(0);
    if limit == Some(0) {
        return Ok(SqlOutcome::Rows {
            columns: names,
            rows: Vec::new(),
        });
    }

    let mut budget = Budget {
        visited: 0,
        combined: 0,
        limits,
    };
    // Build sides: hash tables and loop tables are read once.
    let mut built: Vec<Option<Built>> = Vec::with_capacity(steps.len());
    for (index, step) in steps.iter().enumerate() {
        let def = &defs[index + 1];
        built.push(match &step.access {
            Access::Point(_) | Access::Index { .. } => None,
            Access::Hash { column, .. } => {
                let mut map: HashMap<HashKey, Vec<Arc<Vec<SqlValue>>>> = HashMap::new();
                scan_table(tx, def, &mut budget, |row, _| {
                    if let Some(key) = hash_key(&row[*column]) {
                        map.entry(key).or_default().push(Arc::new(row));
                    }
                    Ok(true)
                })?;
                Some(Built::Hash(map))
            }
            Access::Loop => {
                let mut rows = Vec::new();
                scan_table(tx, def, &mut budget, |row, _| {
                    rows.push(Arc::new(row));
                    Ok(true)
                })?;
                Some(Built::Rows(rows))
            }
        });
    }

    // Rows come out in the first table's primary-key order: scans and
    // point or equality lookups read it in key order, and each joined table
    // only appends its matches. `ORDER BY` on that key alone is therefore
    // already satisfied and can stop early under LIMIT.
    let first_key = Slot {
        table: 0,
        column: defs[0].primary_key_index,
    };
    let sorted =
        !(order.is_empty() || matches!(order.as_slice(), [(slot, false, _)] if *slot == first_key));
    let wanted = limit.map(|limit| limit.saturating_add(offset));
    let mut out: Vec<Combined> = Vec::new();
    let context = Context {
        tx,
        defs: &defs,
        steps: &steps,
        built: &built,
        late: &late,
        params,
    };
    // Without ORDER BY, rows come out in join order, so reading stops once
    // OFFSET + LIMIT rows exist. Sorted output keeps at most max_sort_rows.
    let cap = match (sorted, wanted) {
        (true, _) => Cap {
            rows: limits.max_sort_rows,
            limited: false,
        },
        (false, Some(wanted)) if wanted <= limits.max_select_rows.saturating_add(offset) => Cap {
            rows: wanted,
            limited: true,
        },
        (false, _) => Cap {
            rows: limits.max_select_rows.saturating_add(offset),
            limited: false,
        },
    };
    // The first table uses a key from WHERE when a conjunct compares its
    // primary key or an indexed column with a constant or parameter. The
    // complete WHERE is still evaluated on every row.
    let first = choose_access(0, &defs[0], &early);
    let mut seed = |row: Arc<Vec<SqlValue>>, budget: &mut Budget<'_>| -> Result<bool, SqlError> {
        let mut combined: Combined = vec![Some(row)];
        for conjunct in &early {
            if !truthy(conjunct, &combined, params)? {
                return Ok(true);
            }
        }
        context.extend(0, &mut combined, budget, &mut out, cap)?;
        Ok(sorted || wanted.is_none_or(|wanted| out.len() < wanted))
    };
    match &first {
        Access::Point(key) => {
            let key = eval(key, &Vec::new(), params)?;
            for row in point_rows(tx, &defs[0], key, &mut budget)? {
                seed(row, &mut budget)?;
            }
        }
        Access::Index { name, column, key } => {
            let key = eval(key, &Vec::new(), params)?;
            for row in index_rows(tx, &defs[0], name, *column, key, &mut budget)? {
                if !seed(row, &mut budget)? {
                    break;
                }
            }
        }
        Access::Hash { .. } | Access::Loop => {
            scan_table(tx, &defs[0], &mut budget, |row, budget| {
                seed(Arc::new(row), budget)
            })?;
        }
    }

    if sorted {
        let key = |row: &Combined, slot: Slot| {
            row[slot.table]
                .as_ref()
                .map_or(SqlValue::Null, |values| values[slot.column].clone())
        };
        let mut failure = None;
        // A stable sort keeps the join order for ties, so results are
        // deterministic.
        out.sort_by(|a, b| {
            for &(slot, descending, nulls_first) in &order {
                let (x, y) = (key(a, slot), key(b, slot));
                let ordering = match (&x, &y) {
                    (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
                    (SqlValue::Null, _) => {
                        if nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (_, SqlValue::Null) => {
                        if nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    _ => match compare(&x, &y) {
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
        if let Some(error) = failure {
            return Err(error);
        }
    }
    let end = wanted.unwrap_or(usize::MAX).min(out.len());
    let start = offset.min(end);
    let rows: Vec<Vec<SqlValue>> = out[start..end]
        .iter()
        .map(|row| {
            projection
                .iter()
                .map(|slot| {
                    row[slot.table]
                        .as_ref()
                        .map_or(SqlValue::Null, |values| values[slot.column].clone())
                })
                .collect()
        })
        .collect();
    if rows.len() > limits.max_select_rows {
        return Err(SqlError::Bounds(format!(
            "SELECT result exceeds max_select_rows ({})",
            limits.max_select_rows
        )));
    }
    Ok(SqlOutcome::Rows {
        columns: names,
        rows,
    })
}

fn point_rows(
    tx: &DocumentTransaction<'_, '_>,
    def: &TableDef,
    key: SqlValue,
    budget: &mut Budget<'_>,
) -> Result<Vec<Arc<Vec<SqlValue>>>, SqlError> {
    let pk_type = def.columns[def.primary_key_index].col_type;
    let Some(id) = point_key(key, pk_type).as_ref().and_then(pk_id) else {
        return Ok(Vec::new());
    };
    budget.visit(1)?;
    Ok(
        match tx.get(&def.name, &id).map_err(exec::classify_io_error)? {
            Some(document) => vec![Arc::new(decode(def, &id, &document)?)],
            None => Vec::new(),
        },
    )
}

/// The index value a key must equal, or `None` if no indexed value can equal
/// it. Indexed TEXT values are at most 256 bytes; REAL columns are not
/// indexable, and an INTEGER column matches only integral numbers.
fn index_value(key: SqlValue, column_type: SqlType) -> Option<IndexValue> {
    match (column_type, key) {
        (_, SqlValue::Null) => None,
        (SqlType::Integer, key) => match point_key(key, SqlType::Integer) {
            Some(SqlValue::Integer(n)) => Some(IndexValue::Integer(n)),
            _ => None,
        },
        (SqlType::Text { .. }, SqlValue::Text(s)) if s.len() <= 256 => Some(IndexValue::String(s)),
        (SqlType::Boolean, SqlValue::Boolean(b)) => Some(IndexValue::Boolean(b)),
        _ => None,
    }
}

fn index_rows(
    tx: &DocumentTransaction<'_, '_>,
    def: &TableDef,
    name: &str,
    column: usize,
    key: SqlValue,
    budget: &mut Budget<'_>,
) -> Result<Vec<Arc<Vec<SqlValue>>>, SqlError> {
    let Some(value) = index_value(key, def.columns[column].col_type) else {
        return Ok(Vec::new());
    };
    let mut query = IndexQuery {
        equal: vec![value],
        limit: 256,
        ..IndexQuery::default()
    };
    let mut rows = Vec::new();
    loop {
        let page = tx
            .query_index(&def.name, name, &query)
            .map_err(exec::classify_io_error)?;
        if page.documents.is_empty() {
            return Ok(rows);
        }
        budget.visit(page.documents.len())?;
        let full = page.documents.len() == query.limit;
        for entry in page.documents {
            rows.push(Arc::new(decode(def, &entry.id, &entry.document)?));
        }
        if !full {
            return Ok(rows);
        }
        query.after = page.next;
    }
}

#[derive(Clone, Copy)]
struct Cap {
    rows: usize,
    /// Rows past `rows` are beyond OFFSET + LIMIT and are not returned.
    limited: bool,
}

enum Built {
    Hash(HashMap<HashKey, Vec<Arc<Vec<SqlValue>>>>),
    Rows(Vec<Arc<Vec<SqlValue>>>),
}

struct Context<'a, 'b, 'tx, 'store> {
    tx: &'a DocumentTransaction<'tx, 'store>,
    defs: &'a [Arc<TableDef>],
    steps: &'a [Step],
    built: &'a [Option<Built>],
    late: &'a [BExpr],
    params: &'b [SqlValue],
}

impl Context<'_, '_, '_, '_> {
    /// Joins tables after `done` to `combined`, appending finished rows.
    fn extend(
        &self,
        done: usize,
        combined: &mut Combined,
        budget: &mut Budget<'_>,
        out: &mut Vec<Combined>,
        cap: Cap,
    ) -> Result<(), SqlError> {
        let Some(step) = self.steps.get(done) else {
            for conjunct in self.late {
                if !truthy(conjunct, combined, self.params)? {
                    return Ok(());
                }
            }
            if out.len() >= cap.rows {
                // Unsorted rows past OFFSET + LIMIT are never returned, so
                // dropping them is not truncation. Anything else is a bound.
                if cap.limited {
                    return Ok(());
                }
                return Err(SqlError::Bounds(format!(
                    "join result exceeds its row bound ({}; see max_select_rows and max_sort_rows)",
                    cap.rows
                )));
            }
            out.push(combined.clone());
            return Ok(());
        };
        let table = done + 1;
        let def = &self.defs[table];
        let fetched;
        let candidates: &[Arc<Vec<SqlValue>>] = match (&step.access, &self.built[done]) {
            (Access::Point(key), _) => {
                let key = eval(key, combined, self.params)?;
                fetched = point_rows(self.tx, def, key, budget)?;
                &fetched
            }
            (Access::Index { name, column, key }, _) => {
                let key = eval(key, combined, self.params)?;
                fetched = index_rows(self.tx, def, name, *column, key, budget)?;
                &fetched
            }
            (Access::Hash { key, .. }, Some(Built::Hash(map))) => {
                match hash_key(&eval(key, combined, self.params)?) {
                    Some(key) => map.get(&key).map_or(&[][..], Vec::as_slice),
                    None => &[],
                }
            }
            (Access::Loop, Some(Built::Rows(rows))) => rows,
            _ => unreachable!("join step without its built side"),
        };
        let mut matched = false;
        for candidate in candidates {
            budget.combine()?;
            combined.push(Some(Arc::clone(candidate)));
            let mut keep = true;
            for conjunct in &step.on {
                if !truthy(conjunct, combined, self.params)? {
                    keep = false;
                    break;
                }
            }
            if keep {
                matched = true;
                self.extend(table, combined, budget, out, cap)?;
            }
            combined.pop();
        }
        if !matched && step.kind == JoinKind::Left {
            budget.combine()?;
            combined.push(None);
            self.extend(table, combined, budget, out, cap)?;
            combined.pop();
        }
        Ok(())
    }
}
