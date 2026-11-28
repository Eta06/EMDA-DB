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
use super::{ColRef, JExpr, JoinParser, JoinSelect, OrderItem, SelectItem, limit_value};
use crate::document::DocumentTransaction;
use crate::sql::catalog::ColumnDef;
use crate::sql::exec::{self, ScalarKey, SqlCache, SqlValueRef, compare_order_cell_ref};
use crate::sql::lexer::{Token, TokenKind, tokenize};
use crate::sql::parser::{CompareOp, SqlStatement, Statement, parse_statement};
use crate::sql::subquery::{VisitScopeGuard, record_visited_rows};
use crate::sql::value::{SqlError, SqlLimits, SqlOutcome, SqlType, SqlValue};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

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

type MaterializedRows = Vec<Vec<SqlValue>>;

#[derive(Debug, PartialEq)]
pub(super) enum Source {
    Select(Box<SqlStatement>),
    Recursive(Box<RecursiveSource>),
    /// A placeholder in the parsed recursive member.
    Frontier,
    Working {
        columns: Arc<Vec<ColumnDef>>,
        rows: Arc<MaterializedRows>,
    },
}

impl Source {
    pub(super) fn columns(
        &self,
        tx: &DocumentTransaction<'_, '_>,
        limits: &SqlLimits,
        cache: &SqlCache,
    ) -> Result<Vec<ColumnDef>, SqlError> {
        match self {
            Self::Select(statement) => exec::read_columns(tx, statement, limits, cache),
            Self::Recursive(source) => source.columns(tx, limits, cache),
            Self::Working { columns, .. } => Ok((**columns).clone()),
            Self::Frontier => unreachable!("recursive member is bound to a working table"),
        }
    }

    pub(super) fn rows(
        &self,
        tx: &DocumentTransaction<'_, '_>,
        limits: &SqlLimits,
        cache: &SqlCache,
    ) -> Result<Arc<MaterializedRows>, SqlError> {
        match self {
            Self::Select(statement) => {
                let SqlOutcome::Rows { rows, .. } =
                    exec::run_read_in_tx(tx, statement, limits, cache)?
                else {
                    unreachable!("a SELECT returns rows");
                };
                Ok(Arc::new(rows))
            }
            Self::Recursive(source) => source.rows(tx, limits, cache).map(Arc::new),
            Self::Working { rows, .. } => Ok(Arc::clone(rows)),
            Self::Frontier => unreachable!("recursive member is bound to a working table"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RecursiveSource {
    anchor: SqlStatement,
    member: JoinSelect,
    frontier: usize,
    aliases: Vec<String>,
    distinct: bool,
}

impl RecursiveSource {
    fn member_with_frontier(
        &self,
        columns: Arc<Vec<ColumnDef>>,
        rows: Arc<MaterializedRows>,
    ) -> JoinSelect {
        let mut member = self.member.clone();
        member.tables[self.frontier].query = Some(Arc::new(Source::Working { columns, rows }));
        member
    }

    fn columns(
        &self,
        tx: &DocumentTransaction<'_, '_>,
        limits: &SqlLimits,
        cache: &SqlCache,
    ) -> Result<Vec<ColumnDef>, SqlError> {
        let mut columns = exec::read_columns(tx, &self.anchor, limits, cache)?;
        if !self.aliases.is_empty() {
            if columns.len() != self.aliases.len() {
                return Err(SqlError::Type(
                    "recursive CTE column list has the wrong length".into(),
                ));
            }
            for (column, name) in columns.iter_mut().zip(&self.aliases) {
                column.name = name.clone();
            }
        }
        let member = self.member_with_frontier(Arc::new(columns.clone()), Arc::new(Vec::new()));
        let recursive = super::read_columns(tx, &member, limits, cache)?;
        if columns.len() != recursive.len()
            || columns
                .iter()
                .zip(recursive)
                .any(|(a, b)| !a.col_type.matches_base(b.col_type))
        {
            return Err(SqlError::Type(
                "recursive CTE terms must have matching column counts and types".into(),
            ));
        }
        Ok(columns)
    }

    fn rows(
        &self,
        tx: &DocumentTransaction<'_, '_>,
        limits: &SqlLimits,
        cache: &SqlCache,
    ) -> Result<MaterializedRows, SqlError> {
        let columns = Arc::new(self.columns(tx, limits, cache)?);
        let scope = VisitScopeGuard::enter();
        let SqlOutcome::Rows { mut rows, .. } =
            exec::run_read_in_tx(tx, &self.anchor, limits, cache)?
        else {
            unreachable!("a SELECT returns rows");
        };
        let mut visits = scope.take_and_restore();
        record_visited_rows(visits);
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        let mut bytes = 0_usize;
        loop {
            if self.distinct {
                rows.retain(|row| {
                    seen.insert(
                        row.iter()
                            .map(|cell| ScalarKey::from_value_ref(value_ref(cell)))
                            .collect::<Vec<_>>(),
                    )
                });
            }
            if rows.is_empty() {
                return Ok(result);
            }
            if result.len() + rows.len() > limits.max_select_rows {
                return Err(SqlError::Bounds(
                    "recursive CTE exceeds max_select_rows".into(),
                ));
            }
            for row in &rows {
                bytes = bytes.saturating_add(row.len() * std::mem::size_of::<SqlValue>());
                for cell in row {
                    if let SqlValue::Text(text) = cell {
                        bytes = bytes.saturating_add(text.len());
                    }
                }
                if bytes > crate::MAX_ATOMIC_BATCH_BYTES {
                    return Err(SqlError::Bounds(
                        "recursive CTE exceeds its 16 MiB result buffer".into(),
                    ));
                }
            }
            result.extend(rows.iter().cloned());
            if visits >= limits.max_visited_rows {
                return Err(SqlError::Bounds(
                    "recursive CTE exceeds max_visited_rows".into(),
                ));
            }
            let remaining = SqlLimits {
                max_visited_rows: limits.max_visited_rows - visits,
                ..*limits
            };
            let member = self.member_with_frontier(Arc::clone(&columns), Arc::new(rows));
            let scope = VisitScopeGuard::enter();
            let SqlOutcome::Rows { rows: next, .. } =
                super::run(tx, &member, &[], &remaining, cache)?
            else {
                unreachable!("a SELECT returns rows");
            };
            let read = scope.take_and_restore();
            visits = visits.saturating_add(read);
            record_visited_rows(read);
            rows = next;
        }
    }
}

impl JoinParser<'_> {
    /// Token range inside parentheses, leaving the cursor after the closing
    /// parenthesis. Bound nesting before recursively parsing source text.
    fn source_tokens(&mut self) -> Result<Range<usize>, SqlError> {
        self.expect(&TokenKind::LParen, "(")?;
        let start = self.pos;
        let mut depth = 1usize;
        let mut close = start;
        loop {
            match self.tokens.get(close).map(|t| &t.kind) {
                Some(TokenKind::LParen) => {
                    depth += 1;
                    if depth > self.limits.max_expr_depth {
                        return Err(SqlError::Bounds(
                            "derived source exceeds max_expr_depth".into(),
                        ));
                    }
                }
                Some(TokenKind::RParen) if depth == 1 => break,
                Some(TokenKind::RParen) => depth -= 1,
                Some(TokenKind::Eof) | None => return Err(self.syntax("unclosed '('")),
                _ => {}
            }
            close += 1;
        }
        self.pos = close + 1;
        Ok(start..close)
    }

    fn read_source_text(&self, sql: &str, tokens: Range<usize>) -> Result<SqlStatement, SqlError> {
        let start = self.tokens[tokens.start].offset;
        let end = self.tokens[tokens.end].offset;
        let statement =
            parse_statement(&sql[start..end], self.limits).map_err(|error| match error {
                SqlError::Syntax { offset, message } => SqlError::Syntax {
                    offset: offset + start,
                    message,
                },
                other => other,
            })?;
        if !matches!(&statement.inner, Statement::Select(_) | Statement::Join(_)) {
            return Err(self.unsupported("a derived table or CTE that is not a SELECT"));
        }
        if statement.param_count() > 0 {
            return Err(SqlError::Unsupported {
                feature: "parameters inside a derived table or CTE".into(),
                offset: Some(start),
            });
        }
        Ok(statement)
    }

    pub(super) fn source_query(&mut self, sql: &str) -> Result<SqlStatement, SqlError> {
        let tokens = self.source_tokens()?;
        self.read_source_text(sql, tokens)
    }

    fn recursive_source(
        &self,
        sql: &str,
        body: Range<usize>,
        union: usize,
        name: &str,
        aliases: Vec<String>,
        ctes: &super::CteSources,
    ) -> Result<Source, SqlError> {
        let anchor = self.read_source_text(sql, body.start..union)?;
        let all = matches!(&self.tokens[union + 1].kind, TokenKind::Ident { text, quoted: false } if text == "all");
        let start_token = union + 1 + usize::from(all);
        if start_token >= body.end {
            return Err(self.syntax("expected a recursive SELECT after UNION"));
        }
        let text = &sql[self.tokens[start_token].offset..self.tokens[body.end].offset];
        let mut member_parser = JoinParser {
            tokens: tokenize(text, self.limits)?,
            pos: 0,
            params: 0,
            depth: 0,
            limits: self.limits,
        };
        let mut member_sources = ctes.clone();
        member_sources.push((name.to_owned(), Arc::new(Source::Frontier)));
        let member = member_parser.select_with_ctes(text, &member_sources)?;
        if !matches!(member_parser.peek().kind, TokenKind::Eof) {
            return Err(member_parser.unsupported("recursive CTE with multiple recursive terms"));
        }
        let mut fronts = member.tables.iter().enumerate().filter_map(|(i, table)| {
            matches!(table.query.as_deref(), Some(Source::Frontier)).then_some(i)
        });
        let frontier = fronts
            .next()
            .ok_or_else(|| self.unsupported("recursive CTE without a recursive table reference"))?;
        if fronts.next().is_some()
            || member_parser.params != 0
            || aggregate::is_aggregate(&member)
            || member.limit.is_some()
            || !member.order_by.is_empty()
        {
            return Err(self.unsupported("recursive CTE member needs one recursive reference and no aggregate, parameters, ordering or limit"));
        }
        Ok(Source::Recursive(Box::new(RecursiveSource {
            anchor,
            member,
            frontier,
            aliases,
            distinct: !all,
        })))
    }

    pub(super) fn cte_sources(&mut self, sql: &str) -> Result<super::CteSources, SqlError> {
        let mut ctes = super::CteSources::new();
        if !self.eat_kw("with") {
            return Ok(ctes);
        }
        let recursive = self.eat_kw("recursive");
        loop {
            let name = self.ident("CTE name")?;
            if ctes.iter().any(|(existing, _)| *existing == name) {
                return Err(self.syntax("a CTE name is declared twice"));
            }
            let mut aliases = Vec::new();
            if self.eat(&TokenKind::LParen) {
                loop {
                    let alias = self.ident("CTE column name")?;
                    if aliases.contains(&alias) {
                        return Err(self.syntax("a CTE column is declared twice"));
                    }
                    aliases.push(alias);
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
                self.expect(&TokenKind::RParen, ")")?;
            }
            self.expect_kw("as")?;
            let body = self.source_tokens()?;
            let mut depth = 0_usize;
            let union = body.clone().find(|&pos| match &self.tokens[pos].kind {
                TokenKind::LParen => {
                    depth += 1;
                    false
                }
                TokenKind::RParen => {
                    depth -= 1;
                    false
                }
                TokenKind::Ident {
                    text,
                    quoted: false,
                } => depth == 0 && text == "union",
                _ => false,
            });
            let source = match (recursive, union) {
                (true, Some(union)) => {
                    self.recursive_source(sql, body, union, &name, aliases, &ctes)?
                }
                _ if !aliases.is_empty() => {
                    return Err(self.unsupported("column lists on non-recursive CTEs"));
                }
                _ => Source::Select(Box::new(self.read_source_text(sql, body)?)),
            };
            ctes.push((name, Arc::new(source)));
            if ctes.len() > MAX_SQL_CTES {
                return Err(SqlError::Bounds(format!(
                    "a WITH declares at most {MAX_SQL_CTES} CTEs"
                )));
            }
            if !self.eat(&TokenKind::Comma) {
                return Ok(ctes);
            }
        }
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
