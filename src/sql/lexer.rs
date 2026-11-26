//! Hand-written SQL lexer with bounded statement bytes, tokens, and identifier/text lengths.
use super::value::{MAX_SQL_IDENTIFIER_BYTES, SqlError, SqlLimits};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TokenKind {
    Ident { text: String, quoted: bool },
    Integer(String),
    Real(String),
    String(String),
    Question,
    Star,
    Comma,
    Semicolon,
    LParen,
    RParen,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Minus,
    Plus,
    Slash,
    Percent,
    Concat,
    Dot,
    Eof,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) offset: usize,
}

pub(crate) fn tokenize(sql: &str, limits: &SqlLimits) -> Result<Vec<Token>, SqlError> {
    if sql.len() > limits.max_sql_bytes {
        return Err(SqlError::Bounds(format!(
            "SQL statement length {} exceeds max_sql_bytes ({})",
            sql.len(),
            limits.max_sql_bytes
        )));
    }
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut tokens = Vec::new();

    let push_token =
        |tokens: &mut Vec<Token>, kind: TokenKind, offset: usize| -> Result<(), SqlError> {
            if tokens.len() >= limits.max_tokens {
                return Err(SqlError::Bounds(format!(
                    "token count exceeds max_tokens ({})",
                    limits.max_tokens
                )));
            }
            tokens.push(Token { kind, offset });
            Ok(())
        };

    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // `--` line comments
        if b == b'-' && bytes.get(i + 1) == Some(&b'-') {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && bytes.get(i + 1) == Some(&b'*') {
            return Err(SqlError::Syntax {
                offset: i,
                message: "block comments (/* ... */) are not supported".into(),
            });
        }

        let start = i;
        match b {
            b'\'' => {
                i += 1;
                let mut text = String::new();
                let mut closed = false;
                while i < sql.len() {
                    let ch = sql[i..].chars().next().expect("valid utf-8 boundary");
                    i += ch.len_utf8();
                    if ch == '\'' {
                        if bytes.get(i) == Some(&b'\'') {
                            i += 1;
                            text.push('\'');
                        } else {
                            closed = true;
                            break;
                        }
                    } else {
                        text.push(ch);
                    }
                    if text.len() > limits.max_text_bytes {
                        return Err(SqlError::Bounds(format!(
                            "string literal exceeds max_text_bytes ({})",
                            limits.max_text_bytes
                        )));
                    }
                }
                if !closed {
                    return Err(SqlError::Syntax {
                        offset: start,
                        message: "unterminated string literal".into(),
                    });
                }
                push_token(&mut tokens, TokenKind::String(text), start)?;
            }
            b'"' => {
                i += 1;
                let mut ident = String::new();
                let mut closed = false;
                while i < sql.len() {
                    let ch = sql[i..].chars().next().expect("valid utf-8 boundary");
                    i += ch.len_utf8();
                    if ch == '"' {
                        if bytes.get(i) == Some(&b'"') {
                            i += 1;
                            ident.push('"');
                        } else {
                            closed = true;
                            break;
                        }
                    } else {
                        ident.push(ch);
                    }
                    if ident.len() > MAX_SQL_IDENTIFIER_BYTES {
                        return Err(SqlError::Bounds(format!(
                            "identifier exceeds {MAX_SQL_IDENTIFIER_BYTES} bytes"
                        )));
                    }
                }
                if !closed {
                    return Err(SqlError::Syntax {
                        offset: start,
                        message: "unterminated quoted identifier".into(),
                    });
                }
                if ident.is_empty() {
                    return Err(SqlError::Syntax {
                        offset: start,
                        message: "quoted identifier cannot be empty".into(),
                    });
                }
                push_token(
                    &mut tokens,
                    TokenKind::Ident {
                        text: ident,
                        quoted: true,
                    },
                    start,
                )?;
            }
            b'?' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Question, start)?;
            }
            b'*' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Star, start)?;
            }
            b',' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Comma, start)?;
            }
            b';' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Semicolon, start)?;
            }
            b'(' => {
                i += 1;
                push_token(&mut tokens, TokenKind::LParen, start)?;
            }
            b')' => {
                i += 1;
                push_token(&mut tokens, TokenKind::RParen, start)?;
            }
            b'=' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Eq, start)?;
            }
            b'+' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Plus, start)?;
            }
            b'-' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Minus, start)?;
            }
            b'/' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Slash, start)?;
            }
            b'%' => {
                i += 1;
                push_token(&mut tokens, TokenKind::Percent, start)?;
            }
            b'|' => {
                if bytes.get(i + 1) == Some(&b'|') {
                    i += 2;
                    push_token(&mut tokens, TokenKind::Concat, start)?;
                } else {
                    return Err(SqlError::Syntax {
                        offset: start,
                        message: "unexpected character '|'".into(),
                    });
                }
            }
            b'!' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    i += 2;
                    push_token(&mut tokens, TokenKind::Ne, start)?;
                } else {
                    return Err(SqlError::Syntax {
                        offset: start,
                        message: "unexpected character '!'".into(),
                    });
                }
            }
            b'<' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    i += 2;
                    push_token(&mut tokens, TokenKind::Le, start)?;
                } else if bytes.get(i + 1) == Some(&b'>') {
                    i += 2;
                    push_token(&mut tokens, TokenKind::Ne, start)?;
                } else {
                    i += 1;
                    push_token(&mut tokens, TokenKind::Lt, start)?;
                }
            }
            b'>' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    i += 2;
                    push_token(&mut tokens, TokenKind::Ge, start)?;
                } else {
                    i += 1;
                    push_token(&mut tokens, TokenKind::Gt, start)?;
                }
            }
            b'.' => {
                if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    let (raw, next_i) = scan_number(sql, i)?;
                    i = next_i;
                    push_token(&mut tokens, TokenKind::Real(raw), start)?;
                } else {
                    i += 1;
                    push_token(&mut tokens, TokenKind::Dot, start)?;
                }
            }
            b'0'..=b'9' => {
                let (raw, next_i) = scan_number(sql, i)?;
                i = next_i;
                let kind = if raw.contains('.') || raw.contains('e') || raw.contains('E') {
                    TokenKind::Real(raw)
                } else {
                    TokenKind::Integer(raw)
                };
                push_token(&mut tokens, kind, start)?;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if i - start > MAX_SQL_IDENTIFIER_BYTES {
                    return Err(SqlError::Bounds(format!(
                        "identifier exceeds {MAX_SQL_IDENTIFIER_BYTES} bytes"
                    )));
                }
                let text = sql[start..i].to_ascii_lowercase();
                push_token(
                    &mut tokens,
                    TokenKind::Ident {
                        text,
                        quoted: false,
                    },
                    start,
                )?;
            }
            _ => {
                let ch = sql[start..].chars().next().expect("valid utf-8 boundary");
                return Err(SqlError::Syntax {
                    offset: start,
                    message: format!("unexpected character {ch:?}"),
                });
            }
        }
    }

    tokens.push(Token {
        kind: TokenKind::Eof,
        offset: sql.len(),
    });
    Ok(tokens)
}

fn scan_number(sql: &str, start: usize) -> Result<(String, usize), SqlError> {
    let bytes = sql.as_bytes();
    let mut i = start;
    let mut saw_dot = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        saw_dot = true;
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    if bytes.get(i) == Some(&b'.') {
        return Err(SqlError::Syntax {
            offset: i,
            message: "unexpected '.' in numeric literal".into(),
        });
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(bytes.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let exp_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return Err(SqlError::Syntax {
                offset: start,
                message: "malformed exponent in numeric literal".into(),
            });
        }
    }
    if let Some(&next) = bytes.get(i) {
        if next.is_ascii_alphabetic() || next == b'_' {
            return Err(SqlError::Syntax {
                offset: i,
                message: "invalid trailing character in numeric literal".into(),
            });
        }
    }
    let raw = &sql[start..i];
    if saw_dot && raw == "." {
        return Err(SqlError::Syntax {
            offset: start,
            message: "invalid numeric literal '.'".into(),
        });
    }
    Ok((raw.to_owned(), i))
}
