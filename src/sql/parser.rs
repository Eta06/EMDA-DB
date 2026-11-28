//! Hand-written recursive-descent SQL parser and AST definitions.
use super::catalog::ForeignKeyAction;
use super::lexer::{Token, TokenKind, tokenize};
use super::value::{
    MAX_SQL_INDEX_COLUMNS, MAX_SQL_INDEXES_PER_TABLE, MAX_SQL_SUBQUERY_DEPTH, SqlError, SqlLimits,
    SqlType,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_STMT_ID: AtomicU64 = AtomicU64::new(1);

/// Parsed, reusable SQL statement for this embedded subset.
#[derive(Clone, Debug)]
pub struct SqlStatement {
    pub(crate) inner: Statement,
    pub(crate) param_count: usize,
    pub(crate) stmt_id: u64,
}

impl PartialEq for SqlStatement {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner && self.param_count == other.param_count
    }
}

impl SqlStatement {
    /// Parses a statement with the supplied bounds without accessing a database.
    pub fn parse_with_limits(sql: &str, limits: &SqlLimits) -> Result<Self, SqlError> {
        limits.validate()?;
        parse_statement(sql, limits)
    }

    /// Returns the number of `?` positional parameters expected when executing.
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    /// Returns the sole dataset for a SELECT or row mutation with no subqueries.
    /// Row mutations also require a transaction-time foreign-key check before
    /// they can be isolated to this dataset.
    pub fn isolated_table(&self) -> Option<&str> {
        let local_filter = |filter: &Option<Expr>| filter.as_ref().is_none_or(Expr::is_local);
        let table = match &self.inner {
            Statement::Select(select)
                if local_filter(&select.filter)
                    && local_filter(&select.having)
                    && match &select.columns {
                        SelectColumns::All => true,
                        SelectColumns::Items(items) => items.iter().all(|item| {
                            matches!(
                                item.expr,
                                SelectExpr::Column { .. }
                                    | SelectExpr::Literal { .. }
                                    | SelectExpr::Aggregate(_)
                                    | SelectExpr::Param { .. }
                            )
                        }),
                    } =>
            {
                &select.table
            }
            Statement::Insert(insert) if insert.rows.iter().flatten().all(Expr::is_local) => {
                &insert.table
            }
            Statement::Update(update)
                if local_filter(&update.filter)
                    && update
                        .assignments
                        .iter()
                        .all(|assignment| assignment.value.is_local()) =>
            {
                &update.table
            }
            Statement::Delete(delete) if local_filter(&delete.filter) => &delete.table,
            _ => return None,
        };
        Some(table)
    }

    /// Returns the effect of executing the parsed statement.
    pub fn kind(&self) -> SqlStatementKind {
        match self.inner {
            Statement::Select(_) | Statement::Join(_) | Statement::Explain(_) => {
                SqlStatementKind::Query
            }
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                SqlStatementKind::DataMutation
            }
            Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::DropTable(_)
            | Statement::AlterTable(_) => SqlStatementKind::Schema,
        }
    }
}

/// The access category of a parsed SQL statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlStatementKind {
    /// Reads or explains a query.
    Query,
    /// Changes rows without modifying schema.
    DataMutation,
    /// Changes the schema or starts index maintenance.
    Schema,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Statement {
    CreateTable(CreateTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Join(Box<super::join::JoinSelect>),
    DropTable(super::ddl::DropTable),
    AlterTable(super::ddl::AlterTable),
    Explain(Box<SqlStatement>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ForeignKeySpec {
    pub(crate) column: usize,
    pub(crate) ref_table: String,
    pub(crate) ref_column: Option<String>,
    pub(crate) on_delete: ForeignKeyAction,
    pub(crate) on_update: ForeignKeyAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TupleForeignKeySpec {
    pub(crate) columns: Vec<usize>,
    pub(crate) ref_table: String,
    pub(crate) ref_columns: Vec<String>,
    pub(crate) on_delete: ForeignKeyAction,
    pub(crate) on_update: ForeignKeyAction,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateTableStmt {
    pub(crate) if_not_exists: bool,
    pub(crate) name: String,
    pub(crate) columns: Vec<ColumnSpec>,
    pub(crate) primary_key_columns: Vec<usize>,
    pub(crate) unique_constraints: Vec<Vec<usize>>,
    pub(crate) foreign_keys: Vec<ForeignKeySpec>,
    pub(crate) tuple_foreign_keys: Vec<TupleForeignKeySpec>,
    /// `CHECK` expressions, column- and table-level, as written.
    pub(crate) checks: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateIndexStmt {
    pub(crate) unique: bool,
    pub(crate) if_not_exists: bool,
    pub(crate) name: String,
    pub(crate) table: String,
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DropIndexStmt {
    pub(crate) if_exists: bool,
    pub(crate) name: String,
    pub(crate) table: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ColumnSpec {
    pub(crate) name: String,
    pub(crate) col_type: SqlType,
    pub(crate) not_null: bool,
    /// `DEFAULT` literal, checked against the column type at execution.
    pub(crate) default: Option<(LiteralValue, usize)>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InsertStmt {
    pub(crate) table: String,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) rows: Vec<Vec<Expr>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum AggregateKind {
    CountStar,
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateKind {
    pub(crate) fn sql_name(self) -> &'static str {
        match self {
            Self::CountStar | Self::Count | Self::CountDistinct => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateCall {
    pub(crate) kind: AggregateKind,
    pub(crate) column: Option<String>,
    pub(crate) offset: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SubqueryBody {
    Select(Box<SelectStmt>),
    Join {
        join: Box<super::join::JoinSelect>,
        param_start: usize,
        param_count: usize,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct Subquery {
    pub(crate) id: u64,
    pub(crate) offset: usize,
    pub(crate) body: SubqueryBody,
}

impl PartialEq for Subquery {
    fn eq(&self, other: &Self) -> bool {
        self.body == other.body
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SelectExpr {
    Column {
        name: String,
        offset: usize,
    },
    Aggregate(AggregateCall),
    Literal {
        value: LiteralValue,
        display: String,
    },
    Param {
        idx: usize,
    },
    ScalarSubquery(Box<Subquery>),
    Exists {
        subquery: Box<Subquery>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Subquery>,
        negated: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectItem {
    pub(crate) expr: SelectExpr,
    pub(crate) alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SelectColumns {
    All,
    Items(Vec<SelectItem>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupByItem {
    pub(crate) column: String,
    pub(crate) offset: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NullsOrder {
    First,
    Last,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OrderByTarget {
    Column(String),
    Aggregate(AggregateCall),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OrderByItem {
    pub(crate) target: OrderByTarget,
    pub(crate) offset: usize,
    pub(crate) desc: bool,
    pub(crate) nulls: Option<NullsOrder>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LimitValue {
    Literal(usize),
    Param(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectStmt {
    pub(crate) columns: SelectColumns,
    pub(crate) table: String,
    pub(crate) filter: Option<Expr>,
    pub(crate) group_by: Vec<GroupByItem>,
    pub(crate) having: Option<Expr>,
    pub(crate) order_by: Vec<OrderByItem>,
    pub(crate) limit: Option<LimitValue>,
    pub(crate) offset: Option<LimitValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Assignment {
    pub(crate) column: String,
    pub(crate) offset: usize,
    pub(crate) value: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpdateStmt {
    pub(crate) table: String,
    pub(crate) assignments: Vec<Assignment>,
    pub(crate) filter: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeleteStmt {
    pub(crate) table: String,
    pub(crate) filter: Option<Expr>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LiteralValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Boolean(bool),
}

#[derive(Clone, Copy)]
enum DefaultOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Concat,
}

impl DefaultOp {
    fn precedence(self) -> u8 {
        match self {
            Self::Add | Self::Subtract => 1,
            Self::Multiply | Self::Divide | Self::Remainder => 2,
            Self::Concat => 3,
        }
    }

    fn evaluate(
        self,
        left: LiteralValue,
        right: LiteralValue,
        max_text_bytes: usize,
    ) -> Result<LiteralValue, SqlError> {
        use LiteralValue::{Integer, Null, Real, Text};
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }
        if matches!(self, Self::Concat) {
            let (Text(mut left), Text(right)) = (left, right) else {
                return Err(SqlError::Type(
                    "DEFAULT concatenation requires TEXT operands".into(),
                ));
            };
            if left.len() + right.len() > max_text_bytes {
                return Err(SqlError::Bounds(format!(
                    "DEFAULT text exceeds max_text_bytes ({max_text_bytes})"
                )));
            }
            left.push_str(&right);
            return Ok(Text(left));
        }
        let overflow = || SqlError::Bounds("numeric overflow in DEFAULT expression".into());
        let zero_divisor = || SqlError::Constraint("zero divisor in DEFAULT expression".into());
        if let (Integer(left), Integer(right)) = (&left, &right) {
            if *right == 0 && matches!(self, Self::Divide | Self::Remainder) {
                return Err(zero_divisor());
            }
            return match self {
                Self::Add => left.checked_add(*right),
                Self::Subtract => left.checked_sub(*right),
                Self::Multiply => left.checked_mul(*right),
                Self::Divide => left.checked_div(*right),
                Self::Remainder => Some(if *right == -1 { 0 } else { left % right }),
                Self::Concat => unreachable!(),
            }
            .map(Integer)
            .ok_or_else(overflow);
        }
        let number = |value| match value {
            Integer(n) => super::value::exact_i64_to_f64(n),
            Real(f) => Ok(f),
            _ => Err(SqlError::Type(
                "DEFAULT arithmetic requires numeric operands".into(),
            )),
        };
        let (left, right) = (number(left)?, number(right)?);
        let result = match self {
            Self::Add => left + right,
            Self::Subtract => left - right,
            Self::Multiply => left * right,
            Self::Divide if right != 0.0 => left / right,
            Self::Divide => return Err(zero_divisor()),
            Self::Remainder => {
                return Err(SqlError::Type(
                    "DEFAULT remainder requires INTEGER operands".into(),
                ));
            }
            Self::Concat => unreachable!(),
        };
        result
            .is_finite()
            .then_some(Real(result))
            .ok_or_else(overflow)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Expr {
    Literal(LiteralValue),
    Param(usize),
    Column(String),
    QualifiedColumn {
        table: String,
        column: String,
    },
    Aggregate(AggregateCall),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Not(Box<Expr>),
    Compare {
        op: CompareOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    ScalarSubquery(Box<Subquery>),
    Exists(Box<Subquery>),
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Subquery>,
        negated: bool,
    },
}

impl Expr {
    fn is_local(&self) -> bool {
        match self {
            Self::Literal(_) | Self::Column(_) | Self::Param(_) | Self::Aggregate(_) => true,
            Self::Compare { left, right, .. } => left.is_local() && right.is_local(),
            Self::IsNull { expr, .. } | Self::Not(expr) => expr.is_local(),
            Self::And(items) | Self::Or(items) => items.iter().all(Self::is_local),
            Self::QualifiedColumn { .. }
            | Self::ScalarSubquery(_)
            | Self::Exists(_)
            | Self::InSubquery { .. } => false,
        }
    }
}

fn is_top_level_join_select(tokens: &[Token]) -> bool {
    let is_kw = |token: &Token, word: &str| matches!(&token.kind, TokenKind::Ident { text, quoted: false } if text == word);
    if !tokens.first().is_some_and(|t| is_kw(t, "select")) {
        return false;
    }
    let mut depth = 0usize;
    for token in tokens {
        match &token.kind {
            TokenKind::LParen => depth += 1,
            TokenKind::RParen => depth = depth.saturating_sub(1),
            TokenKind::Ident {
                text,
                quoted: false,
            } if depth == 0 && text == "join" => return true,
            _ => {}
        }
    }
    false
}

pub(crate) fn parse_statement(sql: &str, limits: &SqlLimits) -> Result<SqlStatement, SqlError> {
    let tokens = tokenize(sql, limits)?;
    let is_explain = matches!(
        tokens.first().map(|token| &token.kind),
        Some(TokenKind::Ident { text, quoted: false }) if text == "explain"
    );
    if is_explain {
        let rest = tokens.get(1).map_or(sql.len(), |token| token.offset);
        let inner = parse_statement(&sql[rest..], limits).map_err(|error| match error {
            SqlError::Syntax { offset, message } => SqlError::Syntax {
                offset: offset + rest,
                message,
            },
            other => other,
        })?;
        if !matches!(inner.inner, Statement::Select(_) | Statement::Join(_)) {
            return Err(SqlError::Unsupported {
                feature: "EXPLAIN of a statement other than SELECT".into(),
                offset: Some(0),
            });
        }
        return Ok(SqlStatement {
            param_count: inner.param_count,
            inner: Statement::Explain(Box::new(inner)),
            stmt_id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
        });
    }
    if super::ddl::is_alter_table(&tokens) {
        return Ok(SqlStatement {
            inner: Statement::AlterTable(super::ddl::parse_alter_table(&tokens, limits)?),
            param_count: 0,
            stmt_id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
        });
    }
    if super::ddl::is_drop_table(&tokens) {
        return Ok(SqlStatement {
            inner: Statement::DropTable(super::ddl::parse_drop_table(&tokens)?),
            param_count: 0,
            stmt_id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
        });
    }
    if super::join::is_derived_select(&tokens)
        || (is_top_level_join_select(&tokens) && super::join::is_join_select(&tokens))
    {
        let (join, param_count) = super::join::parse(tokens, sql, limits)?;
        return Ok(SqlStatement {
            inner: Statement::Join(Box::new(join)),
            param_count,
            stmt_id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
        });
    }
    let mut parser = Parser {
        source: sql,
        tokens,
        pos: 0,
        limits,
        param_count: 0,
        subquery_depth: 0,
    };
    let inner = parser.parse_single_statement()?;
    Ok(SqlStatement {
        inner,
        param_count: parser.param_count,
        stmt_id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
    })
}

/// Parses a stored `CHECK` expression. It must be one complete expression
/// without parameters or subqueries, as `CREATE TABLE` accepted it.
pub(crate) fn parse_check_expression(text: &str, limits: &SqlLimits) -> Result<Expr, SqlError> {
    let tokens = tokenize(text, limits)?;
    let mut parser = Parser {
        source: text,
        tokens,
        pos: 0,
        limits,
        param_count: 0,
        subquery_depth: 0,
    };
    let expr = parser.parse_expr(0)?;
    if !matches!(parser.peek().kind, TokenKind::Eof) || parser.param_count != 0 {
        return Err(SqlError::Syntax {
            offset: parser.peek().offset,
            message: "invalid stored CHECK expression".into(),
        });
    }
    Ok(expr)
}

/// Rewrite identifier tokens in a stored CHECK without changing its literals or comments.
pub(super) fn rename_check_column(
    text: &str,
    old: &str,
    new: &str,
    limits: &SqlLimits,
) -> Result<String, SqlError> {
    let quoted_name = format!("\"{}\"", new.replace('"', "\"\""));
    let mut result = String::with_capacity(text.len());
    let mut from = 0;
    for token in tokenize(text, limits)? {
        let TokenKind::Ident { text: name, quoted } = token.kind else {
            continue;
        };
        if name != old || (!quoted && is_reserved_keyword(&name)) {
            continue;
        }
        result.push_str(&text[from..token.offset]);
        result.push_str(&quoted_name);
        from = token.offset + name.len();
        if quoted {
            from += 2 + name.bytes().filter(|&byte| byte == b'"').count();
        }
    }
    result.push_str(&text[from..]);
    Ok(result)
}

struct ParsedReferences {
    ref_table: String,
    ref_columns: Vec<String>,
    on_delete: Option<ForeignKeyAction>,
    on_update: Option<ForeignKeyAction>,
}

struct Parser<'a> {
    /// The statement text, from which `CHECK` expressions are kept verbatim.
    source: &'a str,
    tokens: Vec<Token>,
    pos: usize,
    limits: &'a SqlLimits,
    param_count: usize,
    subquery_depth: usize,
}

impl Parser<'_> {
    fn peek(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .unwrap_or_else(|| self.tokens.last().expect("eof token"))
    }

    fn peek_n(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn advance(&mut self) -> &Token {
        let tok = &self.tokens[self.pos];
        if !matches!(tok.kind, TokenKind::Eof) {
            self.pos += 1;
        }
        tok
    }

    fn take_token(&mut self) -> (TokenKind, usize) {
        let offset = self.tokens[self.pos].offset;
        let kind = std::mem::replace(&mut self.tokens[self.pos].kind, TokenKind::Eof);
        if self.pos + 1 < self.tokens.len() && !matches!(kind, TokenKind::Eof) {
            self.pos += 1;
        }
        (kind, offset)
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Eof)
    }

    fn at_kw(&self, kw: &str) -> bool {
        matches!(&self.peek().kind, TokenKind::Ident { text, quoted: false } if text == kw)
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.at_kw(kw) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<usize, SqlError> {
        let offset = self.peek().offset;
        if self.eat_kw(kw) {
            Ok(offset)
        } else {
            Err(SqlError::Syntax {
                offset,
                message: format!("expected keyword {}", kw.to_ascii_uppercase()),
            })
        }
    }

    fn expect_kind(&mut self, expected: &TokenKind, label: &str) -> Result<usize, SqlError> {
        let offset = self.peek().offset;
        if &self.peek().kind == expected {
            self.advance();
            Ok(offset)
        } else {
            Err(SqlError::Syntax {
                offset,
                message: format!("expected {label}"),
            })
        }
    }

    fn expect_ident(&mut self, label: &str) -> Result<(String, usize), SqlError> {
        let offset = self.peek().offset;
        match &self.peek().kind {
            TokenKind::Ident { text, quoted } => {
                if !quoted && is_reserved_keyword(text) {
                    return Err(SqlError::Syntax {
                        offset,
                        message: format!(
                            "reserved keyword '{}' cannot be used as {label} without double quotes",
                            text.to_ascii_uppercase()
                        ),
                    });
                }
                let (TokenKind::Ident { text, .. }, offset) = self.take_token() else {
                    unreachable!()
                };
                Ok((text, offset))
            }
            _ => Err(SqlError::Syntax {
                offset,
                message: format!("expected {label}"),
            }),
        }
    }

    fn parse_single_statement(&mut self) -> Result<Statement, SqlError> {
        if self.at_eof() {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "empty SQL statement".into(),
            });
        }
        let stmt = match &self.peek().kind {
            TokenKind::Ident {
                text,
                quoted: false,
            } => match text.as_str() {
                "create" => self.parse_create()?,
                "insert" => Statement::Insert(self.parse_insert()?),
                "select" => Statement::Select(self.parse_select()?),
                "update" => Statement::Update(self.parse_update()?),
                "delete" => Statement::Delete(self.parse_delete()?),
                "begin" | "start" => {
                    return Err(SqlError::Unsupported {
                        feature: "explicit SQL transactions (BEGIN/START); use SqlDatabase::transaction or SqlDatabase::transaction_with_retry".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                "commit" | "end" => {
                    return Err(SqlError::Unsupported {
                        feature: "explicit SQL transactions (COMMIT/END); use SqlDatabase::transaction or SqlDatabase::transaction_with_retry".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                "rollback" => {
                    return Err(SqlError::Unsupported {
                        feature: "explicit SQL transactions (ROLLBACK); return Err from SqlDatabase::transaction to roll back".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                "drop" => self.parse_drop()?,
                "alter" => {
                    return Err(SqlError::Unsupported {
                        feature: "ALTER".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                "with" => {
                    return Err(SqlError::Unsupported {
                        feature: "WITH (CTE)".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                "explain" | "pragma" | "replace" | "truncate" | "vacuum" => {
                    let upper = text.to_ascii_uppercase();
                    return Err(SqlError::Unsupported {
                        feature: upper,
                        offset: Some(self.peek().offset),
                    });
                }
                _ => {
                    return Err(SqlError::Syntax {
                        offset: self.peek().offset,
                        message: format!("unexpected statement keyword '{text}'"),
                    });
                }
            },
            _ => {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "expected SQL statement".into(),
                });
            }
        };

        // Optional single trailing semicolon.
        if matches!(self.peek().kind, TokenKind::Semicolon) {
            self.advance();
        }
        if !self.at_eof() {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "multiple statements or trailing tokens are not supported".into(),
            });
        }
        Ok(stmt)
    }

    fn check_trailing_unsupported(&self) -> Result<(), SqlError> {
        let offset = self.peek().offset;
        if let TokenKind::Ident {
            text,
            quoted: false,
        } = &self.peek().kind
        {
            let feature = match text.as_str() {
                "join" | "inner" | "left" | "right" | "full" | "cross" | "natural" => Some("JOIN"),
                "group" => Some("GROUP BY"),
                "having" => Some("HAVING"),
                "order" => Some("ORDER BY"),
                "limit" | "offset" => Some("LIMIT / OFFSET"),
                "union" | "intersect" | "except" => Some("set operations (UNION/INTERSECT/EXCEPT)"),
                "returning" => Some("RETURNING"),
                "on" => Some("ON CONFLICT"),
                "for" => Some("FOR locking clause"),
                "from" => Some("UPDATE ... FROM"),
                _ => None,
            };
            if let Some(feature) = feature {
                return Err(SqlError::Unsupported {
                    feature: feature.into(),
                    offset: Some(offset),
                });
            }
        }
        Ok(())
    }

    fn parse_create(&mut self) -> Result<Statement, SqlError> {
        self.expect_kw("create")?;
        if self.eat_kw("unique") {
            if self.eat_kw("index") {
                return Ok(Statement::CreateIndex(self.parse_create_index(true)?));
            }
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "expected INDEX after CREATE UNIQUE".into(),
            });
        }
        if self.eat_kw("index") {
            return Ok(Statement::CreateIndex(self.parse_create_index(false)?));
        }
        if !self.at_kw("table") {
            if let TokenKind::Ident {
                text,
                quoted: false,
            } = &self.peek().kind
            {
                return Err(SqlError::Unsupported {
                    feature: format!("CREATE {}", text.to_ascii_uppercase()),
                    offset: Some(self.peek().offset),
                });
            }
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "expected TABLE or INDEX after CREATE".into(),
            });
        }
        Ok(Statement::CreateTable(self.parse_create_table_body()?))
    }

    fn parse_create_index(&mut self, unique: bool) -> Result<CreateIndexStmt, SqlError> {
        let if_not_exists = if self.eat_kw("if") {
            self.expect_kw("not")?;
            self.expect_kw("exists")?;
            true
        } else {
            false
        };
        let (name, _) = self.expect_ident("index name")?;
        self.expect_kw("on")?;
        let (table, _) = self.expect_ident("table name")?;
        if self.at_kw("using") {
            return Err(SqlError::Unsupported {
                feature: "CREATE INDEX ... USING".into(),
                offset: Some(self.peek().offset),
            });
        }
        self.expect_kind(&TokenKind::LParen, "'('")?;
        if matches!(self.peek().kind, TokenKind::RParen) {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "CREATE INDEX must declare at least one column".into(),
            });
        }
        let mut columns = Vec::new();
        let mut seen = HashSet::new();
        loop {
            if columns.len() >= MAX_SQL_INDEX_COLUMNS {
                return Err(SqlError::Bounds(format!(
                    "index column count exceeds maximum ({MAX_SQL_INDEX_COLUMNS})"
                )));
            }
            let (col_name, col_offset) = self.expect_ident("column name")?;
            if !seen.insert(col_name.clone()) {
                return Err(SqlError::Syntax {
                    offset: col_offset,
                    message: format!("duplicate column '{col_name}' in CREATE INDEX"),
                });
            }
            if self.at_kw("asc")
                || self.at_kw("desc")
                || self.at_kw("nulls")
                || self.at_kw("collate")
            {
                return Err(SqlError::Unsupported {
                    feature: "index column sort order or collation".into(),
                    offset: Some(self.peek().offset),
                });
            }
            columns.push(col_name);
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.advance();
                continue;
            }
            break;
        }
        self.expect_kind(&TokenKind::RParen, "')'")?;
        if self.at_kw("where") {
            return Err(SqlError::Unsupported {
                feature: "partial index WHERE clause".into(),
                offset: Some(self.peek().offset),
            });
        }
        Ok(CreateIndexStmt {
            unique,
            if_not_exists,
            name,
            table,
            columns,
        })
    }

    fn parse_drop(&mut self) -> Result<Statement, SqlError> {
        let drop_offset = self.expect_kw("drop")?;
        if self.eat_kw("index") {
            return Ok(Statement::DropIndex(self.parse_drop_index()?));
        }
        if let TokenKind::Ident {
            text,
            quoted: false,
        } = &self.peek().kind
        {
            return Err(SqlError::Unsupported {
                feature: format!("DROP {}", text.to_ascii_uppercase()),
                offset: Some(drop_offset),
            });
        }
        Err(SqlError::Unsupported {
            feature: "DROP".into(),
            offset: Some(drop_offset),
        })
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexStmt, SqlError> {
        if self.at_kw("concurrently") {
            return Err(SqlError::Unsupported {
                feature: "DROP INDEX CONCURRENTLY".into(),
                offset: Some(self.peek().offset),
            });
        }
        let if_exists = if self.eat_kw("if") {
            self.expect_kw("exists")?;
            true
        } else {
            false
        };
        let (name, _) = self.expect_ident("index name")?;
        if !self.eat_kw("on") {
            return Err(SqlError::Unsupported {
                feature: "DROP INDEX without ON <table>".into(),
                offset: Some(self.peek().offset),
            });
        }
        let (table, _) = self.expect_ident("table name")?;
        if self.at_kw("cascade") || self.at_kw("restrict") {
            let clause = match &self.peek().kind {
                TokenKind::Ident { text, .. } => text.to_ascii_uppercase(),
                _ => unreachable!(),
            };
            return Err(SqlError::Unsupported {
                feature: format!("DROP INDEX ... {clause}"),
                offset: Some(self.peek().offset),
            });
        }
        Ok(DropIndexStmt {
            if_exists,
            name,
            table,
        })
    }

    /// `"(" expr ")"` after `CHECK`, returned as the expression's text. The
    /// expression may not use parameters or subqueries.
    fn parse_check_clause(&mut self) -> Result<String, SqlError> {
        self.expect_kind(&TokenKind::LParen, "'(' after CHECK")?;
        let start = self.peek().offset;
        let params = self.param_count;
        self.parse_expr(0)?;
        let end = self.peek().offset;
        self.expect_kind(&TokenKind::RParen, "')'")?;
        if self.param_count != params {
            return Err(SqlError::Unsupported {
                feature: "parameters in CHECK".into(),
                offset: Some(start),
            });
        }
        Ok(self.source[start..end].trim().to_owned())
    }

    /// A `DEFAULT` value: a possibly negative number, a string, `TRUE`,
    /// `FALSE` or `NULL`. Expressions and functions are not accepted.
    fn parse_default_literal(&mut self) -> Result<LiteralValue, SqlError> {
        let offset = self.peek().offset;
        let negative = matches!(self.peek().kind, TokenKind::Minus);
        if negative {
            self.advance();
        }
        let value = match self.peek().kind.clone() {
            TokenKind::Integer(raw) => {
                self.advance();
                LiteralValue::Integer(parse_signed_integer(&raw, negative, offset)?)
            }
            TokenKind::Real(raw) => {
                self.advance();
                LiteralValue::Real(parse_real(&raw, negative, offset)?)
            }
            TokenKind::String(text) if !negative => {
                self.advance();
                LiteralValue::Text(text)
            }
            TokenKind::Ident {
                text,
                quoted: false,
            } if !negative
                && matches!(
                    text.to_ascii_lowercase().as_str(),
                    "true" | "false" | "null"
                ) =>
            {
                self.advance();
                match text.to_ascii_lowercase().as_str() {
                    "true" => LiteralValue::Boolean(true),
                    "false" => LiteralValue::Boolean(false),
                    _ => LiteralValue::Null,
                }
            }
            _ => {
                return Err(SqlError::Unsupported {
                    feature:
                        "DEFAULT value other than a literal or parenthesized constant expression"
                            .into(),
                    offset: Some(offset),
                });
            }
        };
        Ok(value)
    }

    fn parse_default_value(&mut self) -> Result<LiteralValue, SqlError> {
        if !matches!(self.peek().kind, TokenKind::LParen) {
            return self.parse_default_literal();
        }
        self.advance();
        let value = self.parse_default_expr(1, 0)?;
        self.expect_kind(&TokenKind::RParen, "')' after DEFAULT expression")?;
        Ok(value)
    }

    fn parse_default_expr(
        &mut self,
        depth: usize,
        precedence: u8,
    ) -> Result<LiteralValue, SqlError> {
        self.check_depth(depth)?;
        let mut left = if matches!(self.peek().kind, TokenKind::LParen) {
            self.advance();
            let inner = self.parse_default_expr(depth + 1, 0)?;
            self.expect_kind(&TokenKind::RParen, "')'")?;
            inner
        } else {
            self.parse_default_literal()?
        };
        loop {
            let op = match self.peek().kind {
                TokenKind::Plus => DefaultOp::Add,
                TokenKind::Minus => DefaultOp::Subtract,
                TokenKind::Star => DefaultOp::Multiply,
                TokenKind::Slash => DefaultOp::Divide,
                TokenKind::Percent => DefaultOp::Remainder,
                TokenKind::Concat => DefaultOp::Concat,
                _ => break,
            };
            if op.precedence() < precedence {
                break;
            }
            self.advance();
            let right = self.parse_default_expr(depth, op.precedence() + 1)?;
            left = op.evaluate(left, right, self.limits.max_text_bytes)?;
        }
        Ok(left)
    }

    fn parse_create_table_body(&mut self) -> Result<CreateTableStmt, SqlError> {
        self.advance();

        let if_not_exists = if self.eat_kw("if") {
            self.expect_kw("not")?;
            self.expect_kw("exists")?;
            true
        } else {
            false
        };

        let (name, _) = self.expect_ident("table name")?;
        self.expect_kind(&TokenKind::LParen, "'('")?;

        let mut columns = Vec::new();
        let mut seen_cols = HashSet::new();
        let mut pk_col_index: Option<usize> = None;
        let mut table_pk_names = Vec::new();
        let mut col_uniques: Vec<usize> = Vec::new();
        let mut table_uniques: Vec<Vec<(String, usize)>> = Vec::new();
        let mut col_fks: Vec<(usize, ParsedReferences)> = Vec::new();
        let mut table_fks = Vec::new();
        let mut checks: Vec<String> = Vec::new();

        if matches!(self.peek().kind, TokenKind::RParen) {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "CREATE TABLE must declare at least one column".into(),
            });
        }

        loop {
            if self.at_kw("primary") {
                self.advance();
                self.expect_kw("key")?;
                self.expect_kind(&TokenKind::LParen, "'('")?;
                if !table_pk_names.is_empty() || pk_col_index.is_some() {
                    return Err(SqlError::Constraint(
                        "table cannot declare multiple PRIMARY KEY constraints".into(),
                    ));
                }
                loop {
                    let key = self.expect_ident("primary key column")?;
                    if table_pk_names.iter().any(|(name, _)| *name == key.0) {
                        return Err(SqlError::Constraint(format!(
                            "PRIMARY KEY repeats column '{}'",
                            key.0
                        )));
                    }
                    table_pk_names.push(key);
                    if table_pk_names.len() > MAX_SQL_INDEX_COLUMNS {
                        return Err(SqlError::Bounds(format!(
                            "PRIMARY KEY has more than {MAX_SQL_INDEX_COLUMNS} columns"
                        )));
                    }
                    if !matches!(self.peek().kind, TokenKind::Comma) {
                        break;
                    }
                    self.advance();
                }
                self.expect_kind(&TokenKind::RParen, "')'")?;
            } else if self.eat_kw("unique") {
                self.expect_kind(&TokenKind::LParen, "'('")?;
                if matches!(self.peek().kind, TokenKind::RParen) {
                    return Err(SqlError::Syntax {
                        offset: self.peek().offset,
                        message: "UNIQUE constraint must declare at least one column".into(),
                    });
                }
                let mut u_cols = Vec::new();
                let mut seen_u = HashSet::new();
                loop {
                    if u_cols.len() >= MAX_SQL_INDEX_COLUMNS {
                        return Err(SqlError::Bounds(format!(
                            "UNIQUE constraint column count exceeds maximum ({MAX_SQL_INDEX_COLUMNS})"
                        )));
                    }
                    let (u_col, u_offset) =
                        self.expect_ident("column name in UNIQUE constraint")?;
                    if !seen_u.insert(u_col.clone()) {
                        return Err(SqlError::Syntax {
                            offset: u_offset,
                            message: format!("duplicate column '{u_col}' in UNIQUE constraint"),
                        });
                    }
                    if self.at_kw("asc")
                        || self.at_kw("desc")
                        || self.at_kw("nulls")
                        || self.at_kw("collate")
                    {
                        return Err(SqlError::Unsupported {
                            feature: "UNIQUE constraint column sort order or collation".into(),
                            offset: Some(self.peek().offset),
                        });
                    }
                    u_cols.push((u_col, u_offset));
                    if matches!(self.peek().kind, TokenKind::Comma) {
                        self.advance();
                        continue;
                    }
                    break;
                }
                self.expect_kind(&TokenKind::RParen, "')'")?;
                table_uniques.push(u_cols);
            } else if self.eat_kw("foreign") {
                self.expect_kw("key")?;
                let columns = self.parse_fk_columns("FOREIGN KEY column")?;
                self.expect_kw("references")?;
                let parsed_ref = self.parse_references_tail()?;
                table_fks.push((columns, parsed_ref));
            } else if self.eat_kw("check") {
                checks.push(self.parse_check_clause()?);
            } else if self.at_kw("constraint") {
                let feature = match &self.peek().kind {
                    TokenKind::Ident { text, .. } => {
                        format!("{} constraint", text.to_ascii_uppercase())
                    }
                    _ => "table constraint".into(),
                };
                return Err(SqlError::Unsupported {
                    feature,
                    offset: Some(self.peek().offset),
                });
            } else {
                if columns.len() >= self.limits.max_columns {
                    return Err(SqlError::Bounds(format!(
                        "column count exceeds max_columns ({})",
                        self.limits.max_columns
                    )));
                }
                let (col_name, col_offset) = self.expect_ident("column name")?;
                if !seen_cols.insert(col_name.clone()) {
                    return Err(SqlError::Syntax {
                        offset: col_offset,
                        message: format!("duplicate column name '{col_name}'"),
                    });
                }
                let col_type = self.parse_column_type()?;
                let mut not_null = false;
                let mut is_pk = false;
                let mut is_unique = false;
                let mut default = None;
                let col_idx = columns.len();
                loop {
                    if self.eat_kw("not") {
                        self.expect_kw("null")?;
                        not_null = true;
                    } else if self.eat_kw("primary") {
                        self.expect_kw("key")?;
                        if is_pk || pk_col_index.is_some() || !table_pk_names.is_empty() {
                            return Err(SqlError::Constraint(
                                "table cannot declare multiple PRIMARY KEY constraints".into(),
                            ));
                        }
                        is_pk = true;
                        not_null = true;
                    } else if self.eat_kw("unique") {
                        is_unique = true;
                    } else if self.eat_kw("references") {
                        let parsed_ref = self.parse_references_tail()?;
                        col_fks.push((col_idx, parsed_ref));
                    } else if self.at_kw("default") {
                        let offset = self.peek().offset;
                        self.advance();
                        if default.is_some() {
                            return Err(SqlError::Syntax {
                                offset,
                                message: format!("column '{col_name}' declares DEFAULT twice"),
                            });
                        }
                        default = Some((self.parse_default_value()?, offset));
                    } else if self.eat_kw("check") {
                        checks.push(self.parse_check_clause()?);
                    } else if self.at_kw("autoincrement")
                        || self.at_kw("generated")
                        || self.at_kw("collate")
                    {
                        let feature = match &self.peek().kind {
                            TokenKind::Ident { text, .. } => {
                                format!("{} column constraint", text.to_ascii_uppercase())
                            }
                            _ => "column constraint".into(),
                        };
                        return Err(SqlError::Unsupported {
                            feature,
                            offset: Some(self.peek().offset),
                        });
                    } else {
                        break;
                    }
                }
                if is_pk {
                    pk_col_index = Some(col_idx);
                }
                if is_unique {
                    col_uniques.push(col_idx);
                }
                columns.push(ColumnSpec {
                    name: col_name,
                    col_type,
                    not_null,
                    default,
                });
            }

            if matches!(self.peek().kind, TokenKind::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        self.expect_kind(&TokenKind::RParen, "')'")?;
        if self.at_kw("without") || self.at_kw("strict") || self.at_kw("as") {
            return Err(SqlError::Unsupported {
                feature: "table options / CREATE TABLE ... AS".into(),
                offset: Some(self.peek().offset),
            });
        }

        let mut primary_key_columns = Vec::new();
        if let Some(index) = pk_col_index {
            primary_key_columns.push(index);
        }
        for (name, offset) in table_pk_names {
            let idx =
                columns
                    .iter()
                    .position(|c| c.name == name)
                    .ok_or_else(|| SqlError::Syntax {
                        offset,
                        message: format!("PRIMARY KEY column '{name}' is not declared in table"),
                    })?;
            columns[idx].not_null = true;
            primary_key_columns.push(idx);
        }
        if primary_key_columns.is_empty() {
            return Err(SqlError::Constraint(
                "CREATE TABLE requires a PRIMARY KEY".into(),
            ));
        }
        for &index in &primary_key_columns {
            let column = &columns[index];
            if !matches!(column.col_type, SqlType::Integer | SqlType::Text { .. }) {
                return Err(SqlError::Type(format!(
                    "PRIMARY KEY column '{}' must be INTEGER or TEXT, got {}",
                    column.name,
                    column.col_type.display_name()
                )));
            }
        }

        let mut unique_constraints: Vec<Vec<usize>> = Vec::new();
        for col_idx in col_uniques {
            if primary_key_columns.as_slice() != [col_idx] {
                let single = vec![col_idx];
                if !unique_constraints.contains(&single) {
                    unique_constraints.push(single);
                }
            }
        }
        for u_cols in table_uniques {
            let mut indices = Vec::with_capacity(u_cols.len());
            for (u_name, u_offset) in u_cols {
                let idx = columns
                    .iter()
                    .position(|c| c.name == u_name)
                    .ok_or_else(|| SqlError::Syntax {
                        offset: u_offset,
                        message: format!("UNIQUE column '{u_name}' is not declared in table"),
                    })?;
                indices.push(idx);
            }
            // The primary key is already unique; skip it and repeats.
            let is_primary_key = indices == primary_key_columns;
            if !(is_primary_key || unique_constraints.contains(&indices)) {
                unique_constraints.push(indices);
            }
        }
        if unique_constraints.len() > MAX_SQL_INDEXES_PER_TABLE {
            return Err(SqlError::Bounds(format!(
                "UNIQUE constraint count exceeds maximum ({MAX_SQL_INDEXES_PER_TABLE})"
            )));
        }
        for cols in &unique_constraints {
            for &col_idx in cols {
                if !matches!(
                    columns[col_idx].col_type,
                    SqlType::Integer | SqlType::Text { .. } | SqlType::Boolean
                ) {
                    return Err(SqlError::Unsupported {
                        feature: format!(
                            "UNIQUE constraint on {} column '{}'",
                            columns[col_idx].col_type.display_name(),
                            columns[col_idx].name
                        ),
                        offset: None,
                    });
                }
            }
        }

        let mut foreign_keys = Vec::with_capacity(col_fks.len() + table_fks.len());
        let mut tuple_foreign_keys = Vec::new();
        for (names, parsed) in table_fks {
            let indices = names
                .into_iter()
                .map(|(name, offset)| {
                    columns
                        .iter()
                        .position(|column| column.name == name)
                        .ok_or_else(|| SqlError::Syntax {
                            offset,
                            message: format!(
                                "FOREIGN KEY column '{name}' is not declared in table"
                            ),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if indices.len() == 1 {
                col_fks.push((indices[0], parsed));
                continue;
            }
            if !parsed.ref_columns.is_empty() && parsed.ref_columns.len() != indices.len() {
                return Err(SqlError::Constraint(
                    "foreign key column counts differ".into(),
                ));
            }
            tuple_foreign_keys.push(TupleForeignKeySpec {
                columns: indices,
                ref_table: parsed.ref_table,
                ref_columns: parsed.ref_columns,
                on_delete: parsed.on_delete.unwrap_or_default(),
                on_update: parsed.on_update.unwrap_or_default(),
            });
        }
        for (column, parsed) in col_fks {
            if parsed.ref_columns.len() > 1 {
                return Err(SqlError::Unsupported {
                    feature: "a single child foreign key column cannot reference a tuple".into(),
                    offset: None,
                });
            }
            foreign_keys.push(ForeignKeySpec {
                column,
                ref_table: parsed.ref_table,
                ref_column: parsed.ref_columns.into_iter().next(),
                on_delete: parsed.on_delete.unwrap_or_default(),
                on_update: parsed.on_update.unwrap_or_default(),
            });
        }
        if foreign_keys.len() + tuple_foreign_keys.len() > super::catalog::MAX_SQL_FOREIGN_KEYS {
            return Err(SqlError::Bounds(format!(
                "FOREIGN KEY constraint count exceeds maximum ({})",
                super::catalog::MAX_SQL_FOREIGN_KEYS
            )));
        }

        Ok(CreateTableStmt {
            if_not_exists,
            name,
            columns,
            primary_key_columns,
            unique_constraints,
            foreign_keys,
            tuple_foreign_keys,
            checks,
        })
    }

    fn parse_fk_columns(&mut self, label: &str) -> Result<Vec<(String, usize)>, SqlError> {
        self.expect_kind(&TokenKind::LParen, "'('")?;
        let mut columns: Vec<(String, usize)> = Vec::new();
        loop {
            let (name, offset) = self.expect_ident(label)?;
            if columns.iter().any(|(column, _)| *column == name) {
                return Err(SqlError::Syntax {
                    offset,
                    message: format!("duplicate foreign key column '{name}'"),
                });
            }
            columns.push((name, offset));
            if columns.len() > MAX_SQL_INDEX_COLUMNS {
                return Err(SqlError::Bounds(format!(
                    "FOREIGN KEY has more than {MAX_SQL_INDEX_COLUMNS} columns"
                )));
            }
            if !matches!(self.peek().kind, TokenKind::Comma) {
                break;
            }
            self.advance();
        }
        self.expect_kind(&TokenKind::RParen, "')'")?;
        Ok(columns)
    }

    fn parse_references_tail(&mut self) -> Result<ParsedReferences, SqlError> {
        let (ref_table, _) = self.expect_ident("referenced table name")?;
        let ref_columns = if matches!(self.peek().kind, TokenKind::LParen) {
            self.parse_fk_columns("referenced column name")?
                .into_iter()
                .map(|(name, _)| name)
                .collect()
        } else {
            Vec::new()
        };

        let mut on_delete = None;
        let mut on_update = None;
        loop {
            if self.eat_kw("on") {
                let is_delete = if self.eat_kw("delete") {
                    true
                } else if self.eat_kw("update") {
                    false
                } else {
                    return Err(SqlError::Syntax {
                        offset: self.peek().offset,
                        message: "expected DELETE or UPDATE after ON".into(),
                    });
                };
                let clause = if is_delete { "DELETE" } else { "UPDATE" };
                let action_offset = self.peek().offset;
                let action = if self.eat_kw("no") {
                    self.expect_kw("action")?;
                    ForeignKeyAction::NoAction
                } else if self.eat_kw("restrict") {
                    ForeignKeyAction::Restrict
                } else if self.eat_kw("cascade") {
                    ForeignKeyAction::Cascade
                } else if self.eat_kw("set") {
                    if self.eat_kw("null") {
                        ForeignKeyAction::SetNull
                    } else if self.eat_kw("default") {
                        return Err(SqlError::Unsupported {
                            feature: format!("FOREIGN KEY ON {clause} SET DEFAULT"),
                            offset: Some(action_offset),
                        });
                    } else {
                        return Err(SqlError::Syntax {
                            offset: self.peek().offset,
                            message: "expected NULL or DEFAULT after SET".into(),
                        });
                    }
                } else {
                    return Err(SqlError::Syntax {
                        offset: action_offset,
                        message: format!("expected referential action after ON {clause}"),
                    });
                };
                let slot = if is_delete {
                    &mut on_delete
                } else {
                    &mut on_update
                };
                if slot.is_some() {
                    return Err(SqlError::Syntax {
                        offset: action_offset,
                        message: format!("duplicate ON {clause} clause in FOREIGN KEY"),
                    });
                }
                *slot = Some(action);
            } else if self.at_kw("match") {
                return Err(SqlError::Unsupported {
                    feature: "FOREIGN KEY MATCH clause".into(),
                    offset: Some(self.peek().offset),
                });
            } else if self.at_kw("deferrable")
                || self.at_kw("initially")
                || (self.at_kw("not")
                    && matches!(
                        self.peek_n(1).map(|t| &t.kind),
                        Some(TokenKind::Ident { text, quoted: false }) if text == "deferrable"
                    ))
            {
                return Err(SqlError::Unsupported {
                    feature: "DEFERRABLE foreign key constraints".into(),
                    offset: Some(self.peek().offset),
                });
            } else {
                break;
            }
        }

        Ok(ParsedReferences {
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        })
    }

    fn parse_column_type(&mut self) -> Result<SqlType, SqlError> {
        let offset = self.peek().offset;
        if !matches!(&self.peek().kind, TokenKind::Ident { quoted: false, .. }) {
            return Err(SqlError::Syntax {
                offset,
                message: "expected column data type".into(),
            });
        }
        let (TokenKind::Ident { text, .. }, _) = self.take_token() else {
            unreachable!()
        };
        match text.as_str() {
            "integer" | "int" | "bigint" => Ok(SqlType::Integer),
            "real" | "double" | "float" => Ok(SqlType::Real),
            "boolean" | "bool" => Ok(SqlType::Boolean),
            "text" => Ok(SqlType::Text { max_chars: None }),
            "varchar" => {
                if matches!(self.peek().kind, TokenKind::LParen) {
                    self.advance();
                    let len_offset = self.peek().offset;
                    let n = if matches!(self.peek().kind, TokenKind::Integer(_)) {
                        let (TokenKind::Integer(raw), _) = self.take_token() else {
                            unreachable!()
                        };
                        raw.parse::<usize>()
                            .map_err(|_| SqlError::Bounds("VARCHAR length out of range".into()))?
                    } else {
                        return Err(SqlError::Syntax {
                            offset: len_offset,
                            message: "expected positive integer length in VARCHAR(n)".into(),
                        });
                    };
                    if n == 0 {
                        return Err(SqlError::Syntax {
                            offset: len_offset,
                            message: "VARCHAR(n) length must be at least 1".into(),
                        });
                    }
                    if n > self.limits.max_text_bytes {
                        return Err(SqlError::Bounds(format!(
                            "VARCHAR({n}) exceeds max_text_bytes ({})",
                            self.limits.max_text_bytes
                        )));
                    }
                    self.expect_kind(&TokenKind::RParen, "')'")?;
                    Ok(SqlType::Text { max_chars: Some(n) })
                } else {
                    Ok(SqlType::Text { max_chars: None })
                }
            }
            _ => Err(SqlError::Syntax {
                offset,
                message: format!("unknown or unsupported column type '{text}'"),
            }),
        }
    }

    fn parse_insert(&mut self) -> Result<InsertStmt, SqlError> {
        self.expect_kw("insert")?;
        self.expect_kw("into")?;
        let (table, _) = self.expect_ident("table name")?;

        let columns = if matches!(self.peek().kind, TokenKind::LParen) {
            self.advance();
            if matches!(self.peek().kind, TokenKind::RParen) {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "INSERT column list cannot be empty".into(),
                });
            }
            let mut cols = Vec::new();
            let mut seen = HashSet::new();
            loop {
                if cols.len() >= self.limits.max_columns {
                    return Err(SqlError::Bounds(format!(
                        "INSERT column list exceeds max_columns ({})",
                        self.limits.max_columns
                    )));
                }
                let (col, col_offset) = self.expect_ident("column name")?;
                if !seen.insert(col.clone()) {
                    return Err(SqlError::Syntax {
                        offset: col_offset,
                        message: format!("duplicate column '{col}' in INSERT column list"),
                    });
                }
                cols.push(col);
                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
            self.expect_kind(&TokenKind::RParen, "')'")?;
            Some(cols)
        } else {
            None
        };

        if self.at_kw("select") || self.at_kw("with") {
            return Err(SqlError::Unsupported {
                feature: "INSERT ... SELECT".into(),
                offset: Some(self.peek().offset),
            });
        }
        if self.at_kw("default") {
            return Err(SqlError::Unsupported {
                feature: "DEFAULT VALUES".into(),
                offset: Some(self.peek().offset),
            });
        }
        self.expect_kw("values")?;

        let mut rows = Vec::new();
        loop {
            if rows.len() >= self.limits.max_insert_rows {
                return Err(SqlError::Bounds(format!(
                    "INSERT row count exceeds max_insert_rows ({})",
                    self.limits.max_insert_rows
                )));
            }
            self.expect_kind(&TokenKind::LParen, "'('")?;
            if matches!(self.peek().kind, TokenKind::RParen) {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "VALUES tuple cannot be empty".into(),
                });
            }
            let mut tuple = Vec::new();
            loop {
                if tuple.len() >= self.limits.max_columns {
                    return Err(SqlError::Bounds(format!(
                        "VALUES tuple exceeds max_columns ({})",
                        self.limits.max_columns
                    )));
                }
                tuple.push(self.parse_expr(1)?);
                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
            self.expect_kind(&TokenKind::RParen, "')'")?;
            if let Some(cols) = &columns {
                if tuple.len() != cols.len() {
                    return Err(SqlError::Syntax {
                        offset: self.peek().offset,
                        message: format!(
                            "INSERT has {} target columns but {} values",
                            cols.len(),
                            tuple.len()
                        ),
                    });
                }
            }
            rows.push(tuple);
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        self.check_trailing_unsupported()?;
        Ok(InsertStmt {
            table,
            columns,
            rows,
        })
    }

    fn parse_select(&mut self) -> Result<SelectStmt, SqlError> {
        self.expect_kw("select")?;
        if self.at_kw("distinct") {
            return Err(SqlError::Unsupported {
                feature: "DISTINCT".into(),
                offset: Some(self.peek().offset),
            });
        }

        let columns = if matches!(self.peek().kind, TokenKind::Star) {
            self.advance();
            if matches!(self.peek().kind, TokenKind::Comma) {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "cannot mix '*' with explicit columns in SELECT".into(),
                });
            }
            if self.at_kw("as") {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "cannot alias '*' in SELECT".into(),
                });
            }
            if is_arithmetic_or_concat(&self.peek().kind) {
                return Err(SqlError::Unsupported {
                    feature: "arithmetic / concatenation operators".into(),
                    offset: Some(self.peek().offset),
                });
            }
            SelectColumns::All
        } else {
            let mut items = Vec::new();
            loop {
                if items.len() >= self.limits.max_columns {
                    return Err(SqlError::Bounds(format!(
                        "SELECT column list exceeds max_columns ({})",
                        self.limits.max_columns
                    )));
                }
                let tok = self.peek().clone();
                let mut expr = self.parse_select_item_base(&tok)?;
                if !matches!(expr, SelectExpr::Aggregate(_) | SelectExpr::Exists { .. })
                    && (self.at_kw("in")
                        || (self.at_kw("not")
                            && matches!(
                                self.peek_n(1).map(|t| &t.kind),
                                Some(TokenKind::Ident { text, quoted: false }) if text == "in"
                            )))
                {
                    let negated = self.eat_kw("not");
                    let in_offset = self.expect_kw("in")?;
                    let subquery = self.parse_in_subquery_rhs(1, in_offset, negated)?;
                    let lhs = select_expr_into_expr(expr);
                    expr = SelectExpr::InSubquery {
                        expr: Box::new(lhs),
                        subquery: Box::new(subquery),
                        negated,
                    };
                }
                if self.at_kw("over") {
                    return Err(SqlError::Unsupported {
                        feature: "window functions (OVER)".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                if self.at_kw("filter") {
                    return Err(SqlError::Unsupported {
                        feature: "FILTER clause on aggregates".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                if is_arithmetic_or_concat(&self.peek().kind) {
                    return Err(SqlError::Unsupported {
                        feature: "arithmetic / concatenation operators".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                let alias = if self.eat_kw("as") {
                    let (alias_name, _) = self.expect_ident("column alias")?;
                    if is_arithmetic_or_concat(&self.peek().kind) {
                        return Err(SqlError::Unsupported {
                            feature: "arithmetic / concatenation operators".into(),
                            offset: Some(self.peek().offset),
                        });
                    }
                    Some(alias_name)
                } else {
                    None
                };
                items.push(SelectItem { expr, alias });
                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }
            SelectColumns::Items(items)
        };

        self.expect_kw("from")?;
        if matches!(self.peek().kind, TokenKind::LParen) {
            return Err(SqlError::Unsupported {
                feature: "subqueries in FROM".into(),
                offset: Some(self.peek().offset),
            });
        }
        let (table, _) = self.expect_ident("table name")?;
        if matches!(self.peek().kind, TokenKind::Comma) {
            return Err(SqlError::Unsupported {
                feature: "JOIN (comma-separated tables)".into(),
                offset: Some(self.peek().offset),
            });
        }
        if self.at_kw("as") {
            return Err(SqlError::Unsupported {
                feature: "table aliases (AS)".into(),
                offset: Some(self.peek().offset),
            });
        }
        if !(self.at_kw("where")
            || self.at_kw("group")
            || self.at_kw("having")
            || self.at_kw("order")
            || self.at_kw("limit")
            || self.at_kw("offset"))
        {
            self.check_trailing_unsupported()?;
        }

        let filter = if self.eat_kw("where") {
            Some(self.parse_expr(1)?)
        } else {
            None
        };
        if !(self.at_kw("group")
            || self.at_kw("having")
            || self.at_kw("order")
            || self.at_kw("limit")
            || self.at_kw("offset"))
        {
            self.check_trailing_unsupported()?;
        }

        let group_by = if self.eat_kw("group") {
            self.parse_group_by()?
        } else {
            Vec::new()
        };
        if !(self.at_kw("having")
            || self.at_kw("order")
            || self.at_kw("limit")
            || self.at_kw("offset"))
        {
            if self.at_kw("where") {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "WHERE must precede GROUP BY".into(),
                });
            }
            self.check_trailing_unsupported()?;
        }

        let having = if self.eat_kw("having") {
            Some(self.parse_expr_with_aggs(1, true)?)
        } else {
            None
        };
        if self.at_kw("group") {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "GROUP BY must precede HAVING".into(),
            });
        }
        if !(self.at_kw("order") || self.at_kw("limit") || self.at_kw("offset")) {
            if self.at_kw("where") {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "WHERE must precede HAVING".into(),
                });
            }
            self.check_trailing_unsupported()?;
        }

        let order_by = if self.eat_kw("order") {
            self.parse_order_by()?
        } else {
            Vec::new()
        };
        if self.at_kw("group") || self.at_kw("having") {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "GROUP BY and HAVING must precede ORDER BY".into(),
            });
        }
        if !(self.at_kw("limit") || self.at_kw("offset")) {
            if self.at_kw("where") {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "WHERE must precede ORDER BY".into(),
                });
            }
            self.check_trailing_unsupported()?;
        }

        let limit = if self.eat_kw("limit") {
            Some(self.parse_limit_value("LIMIT")?)
        } else {
            None
        };
        let offset = if self.eat_kw("offset") {
            if limit.is_none() {
                return Err(SqlError::Unsupported {
                    feature: "OFFSET without LIMIT".into(),
                    offset: Some(self.peek().offset),
                });
            }
            Some(self.parse_limit_value("OFFSET")?)
        } else {
            None
        };

        if self.at_kw("where") {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "WHERE must precede LIMIT".into(),
            });
        }
        if self.at_kw("group") || self.at_kw("having") {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "GROUP BY and HAVING must precede LIMIT".into(),
            });
        }
        if self.at_kw("order") {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "ORDER BY must precede LIMIT".into(),
            });
        }
        if self.at_kw("window") || self.at_kw("qualify") || self.at_kw("fetch") {
            let kw = match &self.peek().kind {
                TokenKind::Ident { text, .. } => text.to_ascii_uppercase(),
                _ => unreachable!(),
            };
            return Err(SqlError::Unsupported {
                feature: format!("{kw} clause"),
                offset: Some(self.peek().offset),
            });
        }
        if is_arithmetic_or_concat(&self.peek().kind) {
            return Err(SqlError::Unsupported {
                feature: "arithmetic / concatenation operators".into(),
                offset: Some(self.peek().offset),
            });
        }
        self.check_trailing_unsupported()?;

        Ok(SelectStmt {
            columns,
            table,
            filter,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_aggregate_call(
        &mut self,
        fn_name: &str,
        fn_offset: usize,
    ) -> Result<AggregateCall, SqlError> {
        self.advance();
        self.expect_kind(&TokenKind::LParen, "'('")?;
        if matches!(self.peek().kind, TokenKind::RParen) {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: format!("{}() requires an argument", fn_name.to_ascii_uppercase()),
            });
        }
        let (kind, column) = match fn_name {
            "count" => {
                if matches!(self.peek().kind, TokenKind::Star) {
                    self.advance();
                    if is_arithmetic_or_concat(&self.peek().kind) {
                        return Err(SqlError::Unsupported {
                            feature: "arithmetic / concatenation operators".into(),
                            offset: Some(self.peek().offset),
                        });
                    }
                    self.expect_kind(&TokenKind::RParen, "')'")?;
                    (AggregateKind::CountStar, None)
                } else if self.eat_kw("distinct") {
                    if matches!(self.peek().kind, TokenKind::Star) {
                        return Err(SqlError::Syntax {
                            offset: self.peek().offset,
                            message: "COUNT(DISTINCT *) is not valid".into(),
                        });
                    }
                    let col = self.parse_aggregate_column_arg("COUNT(DISTINCT ...)")?;
                    self.expect_kind(&TokenKind::RParen, "')'")?;
                    (AggregateKind::CountDistinct, Some(col))
                } else if self.at_kw("all")
                    && !matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::RParen))
                {
                    return Err(SqlError::Unsupported {
                        feature: "ALL modifier in COUNT(...)".into(),
                        offset: Some(self.peek().offset),
                    });
                } else {
                    let col = self.parse_aggregate_column_arg("COUNT")?;
                    self.expect_kind(&TokenKind::RParen, "')'")?;
                    (AggregateKind::Count, Some(col))
                }
            }
            "sum" | "avg" | "min" | "max" => {
                let upper = fn_name.to_ascii_uppercase();
                if matches!(self.peek().kind, TokenKind::Star) {
                    return Err(SqlError::Syntax {
                        offset: self.peek().offset,
                        message: format!("'*' is only valid in COUNT(*), not {upper}"),
                    });
                }
                if self.at_kw("distinct")
                    && !matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::RParen))
                {
                    return Err(SqlError::Unsupported {
                        feature: format!("DISTINCT in {upper}"),
                        offset: Some(self.peek().offset),
                    });
                }
                if self.at_kw("all")
                    && !matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::RParen))
                {
                    return Err(SqlError::Unsupported {
                        feature: format!("ALL modifier in {upper}(...)"),
                        offset: Some(self.peek().offset),
                    });
                }
                let col = self.parse_aggregate_column_arg(&upper)?;
                self.expect_kind(&TokenKind::RParen, "')'")?;
                let kind = match fn_name {
                    "sum" => AggregateKind::Sum,
                    "avg" => AggregateKind::Avg,
                    "min" => AggregateKind::Min,
                    "max" => AggregateKind::Max,
                    _ => unreachable!(),
                };
                (kind, Some(col))
            }
            _ => unreachable!(),
        };
        Ok(AggregateCall {
            kind,
            column,
            offset: fn_offset,
        })
    }

    fn parse_aggregate_column_arg(&mut self, ctx: &str) -> Result<String, SqlError> {
        let tok = self.peek().clone();
        match &tok.kind {
            TokenKind::LParen => Err(SqlError::Unsupported {
                feature: format!("expression argument in {ctx}"),
                offset: Some(tok.offset),
            }),
            TokenKind::Ident { text, quoted } => {
                if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::LParen)) {
                    return Err(SqlError::Unsupported {
                        feature: format!("nested aggregate or function call '{text}(...)'"),
                        offset: Some(tok.offset),
                    });
                }
                if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::Dot)) {
                    return Err(SqlError::Unsupported {
                        feature: "qualified column names (table.column)".into(),
                        offset: Some(tok.offset),
                    });
                }
                if !quoted && matches!(text.as_str(), "null" | "true" | "false") {
                    return Err(SqlError::Unsupported {
                        feature: format!("non-column argument in {ctx}"),
                        offset: Some(tok.offset),
                    });
                }
                let (col, _) = self.expect_ident(&format!("column name in {ctx}"))?;
                if matches!(self.peek().kind, TokenKind::Comma) {
                    return Err(SqlError::Unsupported {
                        feature: format!("multi-argument call in {ctx}"),
                        offset: Some(self.peek().offset),
                    });
                }
                if is_arithmetic_or_concat(&self.peek().kind) {
                    return Err(SqlError::Unsupported {
                        feature: "arithmetic / concatenation operators".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                if matches!(
                    self.peek().kind,
                    TokenKind::Eq
                        | TokenKind::Ne
                        | TokenKind::Lt
                        | TokenKind::Le
                        | TokenKind::Gt
                        | TokenKind::Ge
                ) || self.at_kw("is")
                    || self.at_kw("and")
                    || self.at_kw("or")
                {
                    return Err(SqlError::Unsupported {
                        feature: format!("expression argument in {ctx}"),
                        offset: Some(self.peek().offset),
                    });
                }
                Ok(col)
            }
            TokenKind::Integer(_)
            | TokenKind::Real(_)
            | TokenKind::String(_)
            | TokenKind::Minus
            | TokenKind::Question => Err(SqlError::Unsupported {
                feature: format!("non-column argument in {ctx}"),
                offset: Some(tok.offset),
            }),
            _ => Err(SqlError::Syntax {
                offset: tok.offset,
                message: format!("expected column name in {ctx}"),
            }),
        }
    }

    fn parse_group_by(&mut self) -> Result<Vec<GroupByItem>, SqlError> {
        self.expect_kw("by")?;
        if self.at_kw("rollup") || self.at_kw("cube") || self.at_kw("grouping") {
            let feature = match &self.peek().kind {
                TokenKind::Ident { text, .. } => format!("GROUP BY {}", text.to_ascii_uppercase()),
                _ => "GROUP BY extension".into(),
            };
            return Err(SqlError::Unsupported {
                feature,
                offset: Some(self.peek().offset),
            });
        }
        let mut items = Vec::new();
        loop {
            if items.len() >= self.limits.max_columns {
                return Err(SqlError::Bounds(format!(
                    "GROUP BY column count exceeds max_columns ({})",
                    self.limits.max_columns
                )));
            }
            let tok = self.peek().clone();
            match &tok.kind {
                TokenKind::Integer(_) => {
                    return Err(SqlError::Unsupported {
                        feature: "GROUP BY column position".into(),
                        offset: Some(tok.offset),
                    });
                }
                TokenKind::Minus
                    if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::Integer(_))) =>
                {
                    return Err(SqlError::Unsupported {
                        feature: "GROUP BY column position".into(),
                        offset: Some(tok.offset),
                    });
                }
                TokenKind::LParen => {
                    return Err(SqlError::Unsupported {
                        feature: "expressions in GROUP BY".into(),
                        offset: Some(tok.offset),
                    });
                }
                _ => {}
            }
            let (column, col_offset) = self.expect_ident("column name in GROUP BY")?;
            if matches!(self.peek().kind, TokenKind::Dot) {
                return Err(SqlError::Unsupported {
                    feature: "qualified column names (table.column)".into(),
                    offset: Some(col_offset),
                });
            }
            if matches!(self.peek().kind, TokenKind::LParen) {
                return Err(SqlError::Unsupported {
                    feature: format!("function or aggregate call '{column}(...)' in GROUP BY"),
                    offset: Some(col_offset),
                });
            }
            if is_arithmetic_or_concat(&self.peek().kind) {
                return Err(SqlError::Unsupported {
                    feature: "expressions in GROUP BY".into(),
                    offset: Some(self.peek().offset),
                });
            }
            items.push(GroupByItem {
                column,
                offset: col_offset,
            });
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.advance();
                continue;
            }
            break;
        }
        Ok(items)
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByItem>, SqlError> {
        self.expect_kw("by")?;
        let mut items = Vec::new();
        loop {
            if items.len() >= self.limits.max_columns {
                return Err(SqlError::Bounds(format!(
                    "ORDER BY item count exceeds max_columns ({})",
                    self.limits.max_columns
                )));
            }
            let tok = self.peek().clone();
            match &tok.kind {
                TokenKind::Integer(_) => {
                    return Err(SqlError::Unsupported {
                        feature: "ORDER BY column position".into(),
                        offset: Some(tok.offset),
                    });
                }
                TokenKind::Minus
                    if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::Integer(_))) =>
                {
                    return Err(SqlError::Unsupported {
                        feature: "ORDER BY column position".into(),
                        offset: Some(tok.offset),
                    });
                }
                TokenKind::LParen => {
                    return Err(SqlError::Unsupported {
                        feature: "expressions in ORDER BY".into(),
                        offset: Some(tok.offset),
                    });
                }
                _ => {}
            }
            let (target, item_offset) = if let TokenKind::Ident { text, quoted } = &tok.kind {
                if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::LParen)) {
                    if !quoted && is_supported_aggregate_name(text) {
                        let fn_name = text.clone();
                        let agg = self.parse_aggregate_call(&fn_name, tok.offset)?;
                        (OrderByTarget::Aggregate(agg), tok.offset)
                    } else {
                        return Err(SqlError::Unsupported {
                            feature: format!("function or aggregate call '{text}(...)'"),
                            offset: Some(tok.offset),
                        });
                    }
                } else {
                    let (column, col_offset) = self.expect_ident("column name in ORDER BY")?;
                    if matches!(self.peek().kind, TokenKind::Dot) {
                        return Err(SqlError::Unsupported {
                            feature: "qualified column names (table.column)".into(),
                            offset: Some(col_offset),
                        });
                    }
                    (OrderByTarget::Column(column), col_offset)
                }
            } else {
                return Err(SqlError::Syntax {
                    offset: tok.offset,
                    message: "expected column name or aggregate in ORDER BY".into(),
                });
            };
            if is_arithmetic_or_concat(&self.peek().kind) {
                return Err(SqlError::Unsupported {
                    feature: "expressions in ORDER BY".into(),
                    offset: Some(self.peek().offset),
                });
            }
            let desc = if self.eat_kw("asc") {
                false
            } else {
                self.eat_kw("desc")
            };
            let nulls = if self.eat_kw("nulls") {
                if self.eat_kw("first") {
                    Some(NullsOrder::First)
                } else if self.eat_kw("last") {
                    Some(NullsOrder::Last)
                } else {
                    return Err(SqlError::Syntax {
                        offset: self.peek().offset,
                        message: "expected FIRST or LAST after NULLS".into(),
                    });
                }
            } else {
                None
            };
            items.push(OrderByItem {
                target,
                offset: item_offset,
                desc,
                nulls,
            });
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.advance();
                continue;
            }
            break;
        }
        Ok(items)
    }

    fn parse_limit_value(&mut self, clause: &str) -> Result<LimitValue, SqlError> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::Integer(raw) => {
                self.advance();
                let n = parse_signed_integer(&raw, false, tok.offset)?;
                let val = usize::try_from(n).map_err(|_| {
                    SqlError::Bounds(format!("{clause} value {n} exceeds supported range"))
                })?;
                Ok(LimitValue::Literal(val))
            }
            TokenKind::Minus => {
                self.advance();
                let num = self.peek().clone();
                match num.kind {
                    TokenKind::Integer(raw) => {
                        self.advance();
                        let n = parse_signed_integer(&raw, true, num.offset)?;
                        if n < 0 {
                            Err(SqlError::Bounds(format!(
                                "{clause} must be non-negative, got {n}"
                            )))
                        } else {
                            Ok(LimitValue::Literal(0))
                        }
                    }
                    TokenKind::Real(raw) => {
                        self.advance();
                        parse_real(&raw, true, num.offset)?;
                        Err(SqlError::Type(format!(
                            "{clause} requires an INTEGER value, got REAL"
                        )))
                    }
                    _ => Err(SqlError::Syntax {
                        offset: tok.offset,
                        message: format!("{clause} expects a non-negative INTEGER literal or '?'"),
                    }),
                }
            }
            TokenKind::Real(raw) => {
                self.advance();
                parse_real(&raw, false, tok.offset)?;
                Err(SqlError::Type(format!(
                    "{clause} requires an INTEGER value, got REAL"
                )))
            }
            TokenKind::String(_) => {
                self.advance();
                Err(SqlError::Type(format!(
                    "{clause} requires an INTEGER value, got TEXT"
                )))
            }
            TokenKind::Question => {
                self.advance();
                if self.param_count >= self.limits.max_params {
                    return Err(SqlError::Bounds(format!(
                        "parameter count exceeds max_params ({})",
                        self.limits.max_params
                    )));
                }
                let idx = self.param_count;
                self.param_count += 1;
                Ok(LimitValue::Param(idx))
            }
            TokenKind::Ident {
                ref text,
                quoted: false,
            } if matches!(text.as_str(), "null" | "true" | "false") => {
                let kind = if text == "null" { "NULL" } else { "BOOLEAN" };
                self.advance();
                Err(SqlError::Type(format!(
                    "{clause} requires an INTEGER value, got {kind}"
                )))
            }
            _ => Err(SqlError::Syntax {
                offset: tok.offset,
                message: format!("{clause} expects a non-negative INTEGER literal or '?'"),
            }),
        }
    }

    fn parse_update(&mut self) -> Result<UpdateStmt, SqlError> {
        self.expect_kw("update")?;
        let (table, _) = self.expect_ident("table name")?;
        self.expect_kw("set")?;

        let mut assignments = Vec::new();
        let mut seen = HashSet::new();
        loop {
            if assignments.len() >= self.limits.max_columns {
                return Err(SqlError::Bounds(format!(
                    "UPDATE assignment count exceeds max_columns ({})",
                    self.limits.max_columns
                )));
            }
            let (col, col_offset) = self.expect_ident("column name")?;
            if !seen.insert(col.clone()) {
                return Err(SqlError::Syntax {
                    offset: col_offset,
                    message: format!("duplicate column '{col}' in UPDATE SET"),
                });
            }
            self.expect_kind(&TokenKind::Eq, "'='")?;
            let value = self.parse_expr(1)?;
            assignments.push(Assignment {
                column: col,
                offset: col_offset,
                value,
            });
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.advance();
                continue;
            }
            break;
        }

        self.check_trailing_unsupported()?;
        let filter = if self.eat_kw("where") {
            Some(self.parse_expr(1)?)
        } else {
            None
        };
        self.check_trailing_unsupported()?;

        Ok(UpdateStmt {
            table,
            assignments,
            filter,
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteStmt, SqlError> {
        self.expect_kw("delete")?;
        self.expect_kw("from")?;
        let (table, _) = self.expect_ident("table name")?;
        self.check_trailing_unsupported()?;

        let filter = if self.eat_kw("where") {
            Some(self.parse_expr(1)?)
        } else {
            None
        };
        self.check_trailing_unsupported()?;

        Ok(DeleteStmt { table, filter })
    }

    fn check_depth(&self, depth: usize) -> Result<(), SqlError> {
        if depth > self.limits.max_expr_depth {
            return Err(SqlError::Bounds(format!(
                "expression nesting depth exceeds max_expr_depth ({})",
                self.limits.max_expr_depth
            )));
        }
        Ok(())
    }

    fn parse_expr(&mut self, depth: usize) -> Result<Expr, SqlError> {
        self.parse_expr_with_aggs(depth, false)
    }

    fn parse_expr_with_aggs(&mut self, depth: usize, allow_aggs: bool) -> Result<Expr, SqlError> {
        self.check_depth(depth)?;
        self.parse_or(depth, allow_aggs)
    }

    fn parse_or(&mut self, depth: usize, allow_aggs: bool) -> Result<Expr, SqlError> {
        let first = self.parse_and(depth, allow_aggs)?;
        if !self.at_kw("or") {
            return Ok(first);
        }
        let mut items = vec![first];
        while self.eat_kw("or") {
            self.check_depth(depth + 1)?;
            items.push(self.parse_and(depth + 1, allow_aggs)?);
        }
        Ok(Expr::Or(items))
    }

    fn parse_and(&mut self, depth: usize, allow_aggs: bool) -> Result<Expr, SqlError> {
        let first = self.parse_not(depth, allow_aggs)?;
        if !self.at_kw("and") {
            return Ok(first);
        }
        let mut items = vec![first];
        while self.eat_kw("and") {
            self.check_depth(depth + 1)?;
            items.push(self.parse_not(depth + 1, allow_aggs)?);
        }
        Ok(Expr::And(items))
    }

    fn parse_not(&mut self, depth: usize, allow_aggs: bool) -> Result<Expr, SqlError> {
        if self.eat_kw("not") {
            self.check_depth(depth + 1)?;
            let inner = self.parse_not(depth + 1, allow_aggs)?;
            Ok(Expr::Not(Box::new(inner)))
        } else {
            self.parse_comparison(depth, allow_aggs)
        }
    }

    fn parse_comparison(&mut self, depth: usize, allow_aggs: bool) -> Result<Expr, SqlError> {
        let left = self.parse_primary(depth, allow_aggs)?;
        if self.eat_kw("is") {
            let negated = self.eat_kw("not");
            if !self.eat_kw("null") {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "expected NULL after IS [NOT]".into(),
                });
            }
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }
        if self.at_kw("in")
            || (self.at_kw("not")
                && matches!(
                    self.peek_n(1).map(|t| &t.kind),
                    Some(TokenKind::Ident { text, quoted: false }) if text == "in"
                ))
        {
            let negated = self.eat_kw("not");
            let in_offset = self.expect_kw("in")?;
            let subquery = self.parse_in_subquery_rhs(depth, in_offset, negated)?;
            if matches!(
                self.peek().kind,
                TokenKind::Eq
                    | TokenKind::Ne
                    | TokenKind::Lt
                    | TokenKind::Le
                    | TokenKind::Gt
                    | TokenKind::Ge
            ) {
                return Err(SqlError::Syntax {
                    offset: self.peek().offset,
                    message: "chained comparison operators require parentheses".into(),
                });
            }
            return Ok(Expr::InSubquery {
                expr: Box::new(left),
                subquery: Box::new(subquery),
                negated,
            });
        }
        if let TokenKind::Ident {
            text,
            quoted: false,
        } = &self.peek().kind
        {
            if matches!(text.as_str(), "between" | "like" | "ilike") {
                return Err(SqlError::Unsupported {
                    feature: text.to_ascii_uppercase(),
                    offset: Some(self.peek().offset),
                });
            }
            if text == "not"
                && matches!(
                    self.peek_n(1).map(|t| &t.kind),
                    Some(TokenKind::Ident { text: next, quoted: false })
                        if matches!(next.as_str(), "between" | "like" | "ilike")
                )
            {
                let next_kw = match &self.peek_n(1).expect("peeked").kind {
                    TokenKind::Ident { text, .. } => text.to_ascii_uppercase(),
                    _ => unreachable!(),
                };
                return Err(SqlError::Unsupported {
                    feature: format!("NOT {next_kw}"),
                    offset: Some(self.peek().offset),
                });
            }
        }
        let op = match self.peek().kind {
            TokenKind::Eq => CompareOp::Eq,
            TokenKind::Ne => CompareOp::Ne,
            TokenKind::Lt => CompareOp::Lt,
            TokenKind::Le => CompareOp::Le,
            TokenKind::Gt => CompareOp::Gt,
            TokenKind::Ge => CompareOp::Ge,
            TokenKind::Plus
            | TokenKind::Minus
            | TokenKind::Star
            | TokenKind::Slash
            | TokenKind::Percent
            | TokenKind::Concat => {
                return Err(SqlError::Unsupported {
                    feature: "arithmetic / concatenation operators".into(),
                    offset: Some(self.peek().offset),
                });
            }
            _ => return Ok(left),
        };
        self.advance();
        self.check_depth(depth + 1)?;
        let right = self.parse_primary(depth + 1, allow_aggs)?;
        if matches!(
            self.peek().kind,
            TokenKind::Eq
                | TokenKind::Ne
                | TokenKind::Lt
                | TokenKind::Le
                | TokenKind::Gt
                | TokenKind::Ge
        ) {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "chained comparison operators require parentheses".into(),
            });
        }
        Ok(Expr::Compare {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn parse_primary(&mut self, depth: usize, allow_aggs: bool) -> Result<Expr, SqlError> {
        self.check_depth(depth)?;
        let offset = self.peek().offset;
        match &self.peek().kind {
            TokenKind::LParen => {
                self.advance();
                if self.at_kw("with") {
                    return Err(SqlError::Unsupported {
                        feature: "WITH (CTE) in subquery".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                if self.at_kw("select") {
                    self.check_depth(depth + 1)?;
                    let subquery = self.parse_subquery_body(offset)?;
                    self.expect_kind(&TokenKind::RParen, "')'")?;
                    return Ok(Expr::ScalarSubquery(Box::new(subquery)));
                }
                self.check_depth(depth + 1)?;
                let inner = self.parse_expr_with_aggs(depth + 1, allow_aggs)?;
                if matches!(self.peek().kind, TokenKind::Comma) {
                    return Err(SqlError::Unsupported {
                        feature: "tuple / row expressions".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                self.expect_kind(&TokenKind::RParen, "')'")?;
                Ok(inner)
            }
            TokenKind::Minus => {
                self.advance();
                let num_offset = self.peek().offset;
                match &self.peek().kind {
                    TokenKind::Integer(_) => {
                        let (TokenKind::Integer(raw), _) = self.take_token() else {
                            unreachable!()
                        };
                        let val = parse_signed_integer(&raw, true, num_offset)?;
                        Ok(Expr::Literal(LiteralValue::Integer(val)))
                    }
                    TokenKind::Real(_) => {
                        let (TokenKind::Real(raw), _) = self.take_token() else {
                            unreachable!()
                        };
                        let val = parse_real(&raw, true, num_offset)?;
                        Ok(Expr::Literal(LiteralValue::Real(val)))
                    }
                    _ => Err(SqlError::Unsupported {
                        feature: "unary minus on non-literal expression".into(),
                        offset: Some(offset),
                    }),
                }
            }
            TokenKind::Integer(_) => {
                let (TokenKind::Integer(raw), _) = self.take_token() else {
                    unreachable!()
                };
                let val = parse_signed_integer(&raw, false, offset)?;
                Ok(Expr::Literal(LiteralValue::Integer(val)))
            }
            TokenKind::Real(_) => {
                let (TokenKind::Real(raw), _) = self.take_token() else {
                    unreachable!()
                };
                let val = parse_real(&raw, false, offset)?;
                Ok(Expr::Literal(LiteralValue::Real(val)))
            }
            TokenKind::String(_) => {
                let (TokenKind::String(text), _) = self.take_token() else {
                    unreachable!()
                };
                Ok(Expr::Literal(LiteralValue::Text(text)))
            }
            TokenKind::Question => {
                self.advance();
                if self.param_count >= self.limits.max_params {
                    return Err(SqlError::Bounds(format!(
                        "parameter count exceeds max_params ({})",
                        self.limits.max_params
                    )));
                }
                let idx = self.param_count;
                self.param_count += 1;
                Ok(Expr::Param(idx))
            }
            TokenKind::Ident { text, quoted } => {
                if !quoted {
                    match text.as_str() {
                        "null" => {
                            self.advance();
                            return Ok(Expr::Literal(LiteralValue::Null));
                        }
                        "true" => {
                            self.advance();
                            return Ok(Expr::Literal(LiteralValue::Boolean(true)));
                        }
                        "false" => {
                            self.advance();
                            return Ok(Expr::Literal(LiteralValue::Boolean(false)));
                        }
                        "exists" => {
                            let exists_offset = offset;
                            self.advance();
                            let subquery = self.parse_exists_rhs(depth, exists_offset)?;
                            return Ok(Expr::Exists(Box::new(subquery)));
                        }
                        "case" | "cast" => {
                            return Err(SqlError::Unsupported {
                                feature: text.to_ascii_uppercase(),
                                offset: Some(offset),
                            });
                        }
                        _ if is_reserved_keyword(text) => {
                            return Err(SqlError::Syntax {
                                offset,
                                message: format!(
                                    "unexpected keyword '{}'",
                                    text.to_ascii_uppercase()
                                ),
                            });
                        }
                        _ => {}
                    }
                }
                if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::LParen)) {
                    if allow_aggs && !quoted && is_supported_aggregate_name(text) {
                        let fn_name = text.clone();
                        return Ok(Expr::Aggregate(
                            self.parse_aggregate_call(&fn_name, offset)?,
                        ));
                    }
                    return Err(SqlError::Unsupported {
                        feature: format!("function or aggregate call '{text}(...)'"),
                        offset: Some(offset),
                    });
                }
                if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::Dot)) {
                    if self.subquery_depth > 0 {
                        let table = text.clone();
                        self.advance();
                        self.advance();
                        let (column, _) = self.expect_ident("column name")?;
                        return Ok(Expr::QualifiedColumn { table, column });
                    }
                    return Err(SqlError::Unsupported {
                        feature: "qualified column names (table.column)".into(),
                        offset: Some(offset),
                    });
                }
                let (TokenKind::Ident { text, .. }, _) = self.take_token() else {
                    unreachable!()
                };
                Ok(Expr::Column(text))
            }
            _ => Err(SqlError::Syntax {
                offset,
                message: "expected expression".into(),
            }),
        }
    }

    fn parse_select_item_base(&mut self, tok: &Token) -> Result<SelectExpr, SqlError> {
        match &tok.kind {
            TokenKind::LParen => {
                self.advance();
                if self.at_kw("with") {
                    return Err(SqlError::Unsupported {
                        feature: "WITH (CTE) in subquery".into(),
                        offset: Some(self.peek().offset),
                    });
                }
                if self.at_kw("select") {
                    let subquery = self.parse_subquery_body(tok.offset)?;
                    self.expect_kind(&TokenKind::RParen, "')'")?;
                    return Ok(SelectExpr::ScalarSubquery(Box::new(subquery)));
                }
                Err(SqlError::Unsupported {
                    feature: "parenthesized expressions in SELECT list".into(),
                    offset: Some(tok.offset),
                })
            }
            TokenKind::Minus => {
                self.advance();
                let num_offset = self.peek().offset;
                match &self.peek().kind {
                    TokenKind::Integer(_) => {
                        let (TokenKind::Integer(raw), _) = self.take_token() else {
                            unreachable!()
                        };
                        let val = parse_signed_integer(&raw, true, num_offset)?;
                        Ok(SelectExpr::Literal {
                            value: LiteralValue::Integer(val),
                            display: format!("-{raw}"),
                        })
                    }
                    TokenKind::Real(_) => {
                        let (TokenKind::Real(raw), _) = self.take_token() else {
                            unreachable!()
                        };
                        let val = parse_real(&raw, true, num_offset)?;
                        Ok(SelectExpr::Literal {
                            value: LiteralValue::Real(val),
                            display: format!("-{raw}"),
                        })
                    }
                    _ => Err(SqlError::Unsupported {
                        feature: "unary minus on non-literal expression".into(),
                        offset: Some(tok.offset),
                    }),
                }
            }
            TokenKind::Integer(_) => {
                if self
                    .peek_n(1)
                    .is_some_and(|t| is_arithmetic_or_concat(&t.kind))
                {
                    return Err(SqlError::Unsupported {
                        feature: "arithmetic / concatenation operators".into(),
                        offset: Some(tok.offset),
                    });
                }
                let (TokenKind::Integer(raw), _) = self.take_token() else {
                    unreachable!()
                };
                let val = parse_signed_integer(&raw, false, tok.offset)?;
                Ok(SelectExpr::Literal {
                    value: LiteralValue::Integer(val),
                    display: raw,
                })
            }
            TokenKind::Real(_) => {
                if self
                    .peek_n(1)
                    .is_some_and(|t| is_arithmetic_or_concat(&t.kind))
                {
                    return Err(SqlError::Unsupported {
                        feature: "arithmetic / concatenation operators".into(),
                        offset: Some(tok.offset),
                    });
                }
                let (TokenKind::Real(raw), _) = self.take_token() else {
                    unreachable!()
                };
                let val = parse_real(&raw, false, tok.offset)?;
                Ok(SelectExpr::Literal {
                    value: LiteralValue::Real(val),
                    display: raw,
                })
            }
            TokenKind::String(_) => {
                if self
                    .peek_n(1)
                    .is_some_and(|t| is_arithmetic_or_concat(&t.kind))
                {
                    return Err(SqlError::Unsupported {
                        feature: "arithmetic / concatenation operators".into(),
                        offset: Some(tok.offset),
                    });
                }
                let (TokenKind::String(text), _) = self.take_token() else {
                    unreachable!()
                };
                let display = format!("'{text}'");
                Ok(SelectExpr::Literal {
                    value: LiteralValue::Text(text),
                    display,
                })
            }
            TokenKind::Question => {
                if self
                    .peek_n(1)
                    .is_some_and(|t| is_arithmetic_or_concat(&t.kind))
                {
                    return Err(SqlError::Unsupported {
                        feature: "arithmetic / concatenation operators".into(),
                        offset: Some(tok.offset),
                    });
                }
                self.advance();
                if self.param_count >= self.limits.max_params {
                    return Err(SqlError::Bounds(format!(
                        "parameter count exceeds max_params ({})",
                        self.limits.max_params
                    )));
                }
                let idx = self.param_count;
                self.param_count += 1;
                Ok(SelectExpr::Param { idx })
            }
            TokenKind::Ident { text, quoted } => {
                if !quoted && text == "exists" {
                    self.advance();
                    let subquery = self.parse_exists_rhs(1, tok.offset)?;
                    return Ok(SelectExpr::Exists {
                        subquery: Box::new(subquery),
                        negated: false,
                    });
                }
                if !quoted
                    && text == "not"
                    && matches!(
                        self.peek_n(1).map(|t| &t.kind),
                        Some(TokenKind::Ident { text: next, quoted: false }) if next == "exists"
                    )
                {
                    self.advance();
                    let exists_offset = self.peek().offset;
                    self.advance();
                    let subquery = self.parse_exists_rhs(1, exists_offset)?;
                    return Ok(SelectExpr::Exists {
                        subquery: Box::new(subquery),
                        negated: true,
                    });
                }
                if !quoted && matches!(text.as_str(), "null" | "true" | "false") {
                    let (value, display) = match text.as_str() {
                        "null" => (LiteralValue::Null, "NULL".to_owned()),
                        "true" => (LiteralValue::Boolean(true), "TRUE".to_owned()),
                        _ => (LiteralValue::Boolean(false), "FALSE".to_owned()),
                    };
                    self.advance();
                    return Ok(SelectExpr::Literal { value, display });
                }
                if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::LParen)) {
                    if !quoted && is_supported_aggregate_name(text) {
                        let fn_name = text.clone();
                        return Ok(SelectExpr::Aggregate(
                            self.parse_aggregate_call(&fn_name, tok.offset)?,
                        ));
                    }
                    return Err(SqlError::Unsupported {
                        feature: format!("aggregate or function call '{text}'"),
                        offset: Some(tok.offset),
                    });
                }
                if matches!(self.peek_n(1).map(|t| &t.kind), Some(TokenKind::Dot)) {
                    return Err(SqlError::Unsupported {
                        feature: "qualified column names (table.column)".into(),
                        offset: Some(tok.offset),
                    });
                }
                if !quoted && is_reserved_keyword(text) {
                    return Err(SqlError::Syntax {
                        offset: tok.offset,
                        message: format!(
                            "expected column or aggregate in SELECT list, found keyword '{}'",
                            text.to_ascii_uppercase()
                        ),
                    });
                }
                let col = text.clone();
                self.advance();
                Ok(SelectExpr::Column {
                    name: col,
                    offset: tok.offset,
                })
            }
            _ => Err(SqlError::Syntax {
                offset: tok.offset,
                message: "expected '*', column name, or aggregate in SELECT list".into(),
            }),
        }
    }

    fn parse_exists_rhs(&mut self, depth: usize, offset: usize) -> Result<Subquery, SqlError> {
        if !matches!(self.peek().kind, TokenKind::LParen) {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "expected '(' after EXISTS".into(),
            });
        }
        let lparen_offset = self.peek().offset;
        self.advance();
        if self.at_kw("with") {
            return Err(SqlError::Unsupported {
                feature: "WITH (CTE) in subquery".into(),
                offset: Some(self.peek().offset),
            });
        }
        if !self.at_kw("select") {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "expected SELECT in EXISTS subquery".into(),
            });
        }
        self.check_depth(depth + 1)?;
        let subquery = self.parse_subquery_body(lparen_offset.max(offset))?;
        self.expect_kind(&TokenKind::RParen, "')'")?;
        Ok(subquery)
    }

    fn parse_in_subquery_rhs(
        &mut self,
        depth: usize,
        in_offset: usize,
        negated: bool,
    ) -> Result<Subquery, SqlError> {
        if !matches!(self.peek().kind, TokenKind::LParen) {
            return Err(SqlError::Syntax {
                offset: self.peek().offset,
                message: "expected '(' after IN".into(),
            });
        }
        let lparen_offset = self.peek().offset;
        self.advance();
        if self.at_kw("with") {
            return Err(SqlError::Unsupported {
                feature: "WITH (CTE) in subquery".into(),
                offset: Some(self.peek().offset),
            });
        }
        if !self.at_kw("select") {
            return Err(SqlError::Unsupported {
                feature: if negated {
                    "NOT IN (value list)"
                } else {
                    "IN (value list)"
                }
                .into(),
                offset: Some(in_offset),
            });
        }
        self.check_depth(depth + 1)?;
        let subquery = self.parse_subquery_body(lparen_offset)?;
        self.expect_kind(&TokenKind::RParen, "')'")?;
        Ok(subquery)
    }

    fn subquery_has_top_level_join(&self) -> bool {
        let mut depth = 0usize;
        let mut i = self.pos;
        while i < self.tokens.len() {
            match &self.tokens[i].kind {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                TokenKind::Eof => break,
                TokenKind::Ident {
                    text,
                    quoted: false,
                } if depth == 0 && text == "join" => return true,
                _ => {}
            }
            i += 1;
        }
        false
    }

    fn parse_subquery_body(&mut self, offset: usize) -> Result<Subquery, SqlError> {
        if self.subquery_depth >= MAX_SQL_SUBQUERY_DEPTH {
            return Err(SqlError::Bounds(format!(
                "subquery nesting depth exceeds maximum ({MAX_SQL_SUBQUERY_DEPTH})"
            )));
        }
        self.subquery_depth += 1;
        let res = if self.subquery_has_top_level_join() {
            self.parse_join_subquery()
        } else {
            self.parse_select()
                .map(|select| SubqueryBody::Select(Box::new(select)))
        };
        self.subquery_depth -= 1;
        let body = res?;
        Ok(Subquery {
            id: NEXT_STMT_ID.fetch_add(1, Ordering::Relaxed),
            offset,
            body,
        })
    }

    fn parse_join_subquery(&mut self) -> Result<SubqueryBody, SqlError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut end = start;
        let mut closed = false;
        while end < self.tokens.len() {
            match &self.tokens[end].kind {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => {
                    if depth == 0 {
                        closed = true;
                        break;
                    }
                    depth -= 1;
                }
                TokenKind::Eof => break,
                _ => {}
            }
            end += 1;
        }
        if !closed {
            return Err(SqlError::Syntax {
                offset: self.tokens[end.min(self.tokens.len() - 1)].offset,
                message: "expected ')' closing subquery".into(),
            });
        }
        if end > start && matches!(self.tokens[end - 1].kind, TokenKind::Semicolon) {
            return Err(SqlError::Syntax {
                offset: self.tokens[end - 1].offset,
                message: "unexpected ';' inside subquery".into(),
            });
        }
        let mut sub_tokens = self.tokens[start..end].to_vec();
        sub_tokens.push(Token {
            kind: TokenKind::Eof,
            offset: self.tokens[end].offset,
        });
        let (join, inner_params) = super::join::parse(sub_tokens, self.source, self.limits)?;
        if self.param_count + inner_params > self.limits.max_params {
            return Err(SqlError::Bounds(format!(
                "parameter count exceeds max_params ({})",
                self.limits.max_params
            )));
        }
        let param_start = self.param_count;
        self.param_count += inner_params;
        self.pos = end;
        Ok(SubqueryBody::Join {
            join: Box::new(join),
            param_start,
            param_count: inner_params,
        })
    }
}

fn select_expr_into_expr(expr: SelectExpr) -> Expr {
    match expr {
        SelectExpr::Column { name, .. } => Expr::Column(name),
        SelectExpr::Literal { value, .. } => Expr::Literal(value),
        SelectExpr::Param { idx } => Expr::Param(idx),
        SelectExpr::ScalarSubquery(sub) => Expr::ScalarSubquery(sub),
        SelectExpr::Aggregate(_) | SelectExpr::Exists { .. } | SelectExpr::InSubquery { .. } => {
            unreachable!("validated non-aggregate LHS in SELECT IN")
        }
    }
}

fn is_supported_aggregate_name(word: &str) -> bool {
    matches!(word, "count" | "sum" | "avg" | "min" | "max")
}

fn is_arithmetic_or_concat(kind: &TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Plus
            | TokenKind::Minus
            | TokenKind::Star
            | TokenKind::Slash
            | TokenKind::Percent
            | TokenKind::Concat
    )
}

fn parse_signed_integer(raw: &str, negated: bool, offset: usize) -> Result<i64, SqlError> {
    if negated {
        let with_sign = format!("-{raw}");
        with_sign.parse::<i64>().map_err(|_| SqlError::Syntax {
            offset,
            message: format!("integer literal '-{raw}' out of signed 64-bit range"),
        })
    } else {
        raw.parse::<i64>().map_err(|_| SqlError::Syntax {
            offset,
            message: format!("integer literal '{raw}' out of signed 64-bit range"),
        })
    }
}

fn parse_real(raw: &str, negated: bool, offset: usize) -> Result<f64, SqlError> {
    let parsed = if negated {
        format!("-{raw}").parse::<f64>()
    } else {
        raw.parse::<f64>()
    };
    match parsed {
        Ok(val) if val.is_finite() => Ok(val),
        _ => Err(SqlError::Syntax {
            offset,
            message: format!("real literal '{raw}' is not a finite 64-bit float"),
        }),
    }
}

fn is_reserved_keyword(word: &str) -> bool {
    matches!(
        word,
        "select"
            | "from"
            | "where"
            | "insert"
            | "into"
            | "values"
            | "update"
            | "set"
            | "delete"
            | "create"
            | "table"
            | "if"
            | "exists"
            | "primary"
            | "key"
            | "not"
            | "null"
            | "true"
            | "false"
            | "and"
            | "or"
            | "is"
            | "join"
            | "inner"
            | "left"
            | "right"
            | "full"
            | "cross"
            | "natural"
            | "on"
            | "group"
            | "order"
            | "by"
            | "having"
            | "limit"
            | "offset"
            | "begin"
            | "start"
            | "commit"
            | "end"
            | "rollback"
            | "drop"
            | "alter"
            | "index"
            | "returning"
            | "conflict"
            | "default"
            | "check"
            | "foreign"
            | "references"
            | "unique"
            | "union"
            | "intersect"
            | "except"
            | "with"
            | "as"
            | "distinct"
            | "in"
            | "between"
            | "like"
            | "ilike"
            | "case"
            | "when"
            | "then"
            | "else"
            | "autoincrement"
            | "generated"
            | "constraint"
            | "collate"
            | "without"
            | "strict"
            | "for"
    )
}
