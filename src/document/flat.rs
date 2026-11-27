//! An allocation-light reader for flat stored documents.
//!
//! SQL rows are JSON objects whose members are scalars. For those, building a
//! `serde_json::Value` with an owned map per row dominates scan cost. This
//! module accepts only a strict subset of JSON and returns borrowed fields:
//! a top-level object whose members are `null`, booleans, numbers, or strings
//! without escapes, with unique member names without escapes and at most
//! [`MAX_FLAT_BYTES`] encoded bytes. Anything else, including any syntax
//! error, is declined so that the caller uses the full decoder, which then
//! accepts or rejects it exactly as before.
//!
//! For an accepted document, `serde_json` would produce the same members and
//! values: integers that fit `i64` or `u64` are kept exactly, and floats are
//! parsed by `serde_json` itself so the bits match. Tree limits cannot be
//! exceeded: depth is one, members are fewer than [`MAX_FLAT_BYTES`] / 5,
//! below the node limit, and string bytes are below the encoded length.
use serde_json::{Map, Number, Value};

/// Documents larger than this always use the full decoder, which also runs
/// the complete tree validation for them.
pub(crate) const MAX_FLAT_BYTES: usize = 262_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum FlatScalar<'a> {
    Null,
    Bool(bool),
    /// An integer that fits `i64`.
    Int(i64),
    /// An integer above `i64::MAX` that fits `u64`.
    UInt(u64),
    Float(f64),
    Str(&'a str),
}

impl FlatScalar<'_> {
    /// As `serde_json::Value::as_i64`.
    pub(crate) fn as_i64(&self) -> Option<i64> {
        match *self {
            Self::Int(n) => Some(n),
            _ => None,
        }
    }

    /// As `serde_json::Value::as_f64`.
    pub(crate) fn as_f64(&self) -> Option<f64> {
        match *self {
            Self::Int(n) => Some(n as f64),
            Self::UInt(n) => Some(n as f64),
            Self::Float(f) => Some(f),
            _ => None,
        }
    }

    fn to_value(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(b) => Value::Bool(b),
            Self::Int(n) => Value::Number(Number::from(n)),
            Self::UInt(n) => Value::Number(Number::from(n)),
            Self::Float(f) => Number::from_f64(f).map_or(Value::Null, Value::Number),
            Self::Str(s) => Value::String(s.to_owned()),
        }
    }
}

/// Documents with at most this many members keep them inline, without
/// allocating; larger ones move them to a vector.
const INLINE_FIELDS: usize = 8;

type Field<'a> = (&'a str, FlatScalar<'a>);

/// The members of an accepted flat document, in encoded order.
#[derive(Debug)]
pub(crate) struct FlatDocument<'a> {
    inline: [Field<'a>; INLINE_FIELDS],
    len: usize,
    spill: Vec<Field<'a>>,
}

impl Default for FlatDocument<'_> {
    fn default() -> Self {
        Self {
            inline: [("", FlatScalar::Null); INLINE_FIELDS],
            len: 0,
            spill: Vec::new(),
        }
    }
}

impl<'a> FlatDocument<'a> {
    fn fields(&self) -> &[Field<'a>] {
        if self.len <= INLINE_FIELDS {
            &self.inline[..self.len]
        } else {
            &self.spill
        }
    }

    fn push(&mut self, field: Field<'a>) {
        if self.len < INLINE_FIELDS {
            self.inline[self.len] = field;
        } else {
            if self.len == INLINE_FIELDS {
                self.spill.extend_from_slice(&self.inline);
            }
            self.spill.push(field);
        }
        self.len += 1;
    }

    pub(crate) fn get(&self, name: &str) -> Option<FlatScalar<'a>> {
        self.fields()
            .iter()
            .find(|(field, _)| *field == name)
            .map(|(_, value)| *value)
    }

    /// The members `keep` selects, as `serde_json` would decode them.
    pub(crate) fn to_map_where(&self, keep: impl Fn(&str) -> bool) -> Map<String, Value> {
        self.fields()
            .iter()
            .filter(|(name, _)| keep(name))
            .map(|(name, value)| ((*name).to_owned(), value.to_value()))
            .collect()
    }

    /// The `serde_json` object this document decodes to.
    pub(crate) fn to_map(&self) -> Map<String, Value> {
        self.fields()
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.to_value()))
            .collect()
    }
}

/// The input was checked to be UTF-8 once, so every slice between ASCII
/// delimiters is a valid `str`.
struct Cursor<'a> {
    text: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn skip_ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.bytes.get(self.pos) {
            self.pos += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> Option<()> {
        (self.bytes.get(self.pos) == Some(&byte)).then(|| self.pos += 1)
    }

    fn literal(&mut self, text: &[u8]) -> Option<()> {
        self.bytes
            .get(self.pos..self.pos + text.len())
            .filter(|slice| *slice == text)
            .map(|_| self.pos += text.len())
    }

    /// A string without escapes or control characters, as valid UTF-8.
    fn string(&mut self) -> Option<&'a str> {
        self.eat(b'"')?;
        let start = self.pos;
        let length = self.bytes[start..]
            .iter()
            .position(|&byte| byte == b'"' || byte == b'\\' || byte < 0x20)?;
        self.pos += length;
        self.eat(b'"')?;
        Some(&self.text[start..start + length])
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
            self.pos += 1;
        }
        self.pos - start
    }

    /// A number in strict JSON grammar.
    fn number(&mut self) -> Option<FlatScalar<'a>> {
        let start = self.pos;
        let _ = self.eat(b'-');
        match self.bytes.get(self.pos)? {
            b'0' => self.pos += 1,
            b'1'..=b'9' => {
                self.digits();
            }
            _ => return None,
        }
        let mut integer = true;
        if self.eat(b'.').is_some() {
            integer = false;
            if self.digits() == 0 {
                return None;
            }
        }
        if let Some(b'e' | b'E') = self.bytes.get(self.pos) {
            integer = false;
            self.pos += 1;
            if let Some(b'+' | b'-') = self.bytes.get(self.pos) {
                self.pos += 1;
            }
            if self.digits() == 0 {
                return None;
            }
        }
        let text = &self.text[start..self.pos];
        if integer {
            // serde_json reads "-0" as the float -0.0, not the integer 0.
            if text == "-0" {
                return Some(FlatScalar::Float(-0.0));
            }
            // At most 18 digits always fit i64; sum them directly.
            let digits = text.strip_prefix('-').unwrap_or(text);
            if digits.len() <= 18 {
                let magnitude = digits
                    .bytes()
                    .fold(0_i64, |sum, digit| sum * 10 + i64::from(digit - b'0'));
                return Some(FlatScalar::Int(if text.len() == digits.len() {
                    magnitude
                } else {
                    -magnitude
                }));
            }
            if let Ok(n) = text.parse::<i64>() {
                return Some(FlatScalar::Int(n));
            }
            // serde_json turns larger integers into floats; decline them.
            return text.parse::<u64>().ok().map(FlatScalar::UInt);
        }
        if let Some(value) = exact_float(text) {
            return Some(FlatScalar::Float(value));
        }
        // Parse with serde_json so the value has exactly its bits; it
        // rejects floats outside the f64 range, as the full decoder would.
        let value: f64 = serde_json::from_str(text).ok()?;
        value.is_finite().then_some(FlatScalar::Float(value))
    }

    fn scalar(&mut self) -> Option<FlatScalar<'a>> {
        match *self.bytes.get(self.pos)? {
            b'"' => self.string().map(FlatScalar::Str),
            b't' => self.literal(b"true").map(|()| FlatScalar::Bool(true)),
            b'f' => self.literal(b"false").map(|()| FlatScalar::Bool(false)),
            b'n' => self.literal(b"null").map(|()| FlatScalar::Null),
            b'-' | b'0'..=b'9' => self.number(),
            // Nested objects and arrays use the full decoder.
            _ => None,
        }
    }
}

/// Reads a flat document, or returns `None` to send the caller to the full
/// decoder. `None` is never a verdict that the document is invalid.
/// Powers of ten that are exact in `f64`.
const EXACT_POWERS: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// The value of a JSON float when it is exactly representable as an
/// integer of at most 15 digits scaled by an exact power of ten of at most
/// 22. One correctly rounded multiplication or division then gives the
/// correctly rounded result, which every correct parser, `serde_json`
/// included, must return. Other floats return `None`.
fn exact_float(text: &str) -> Option<f64> {
    let bytes = text.as_bytes();
    let (negative, mut pos) = match bytes.first()? {
        b'-' => (true, 1),
        _ => (false, 0),
    };
    let mut mantissa: u64 = 0;
    let mut digits = 0_u32;
    let mut scale: i32 = 0;
    let mut seen_point = false;
    while let Some(&byte) = bytes.get(pos) {
        match byte {
            b'0'..=b'9' => {
                if mantissa != 0 || byte != b'0' {
                    digits += 1;
                }
                mantissa = mantissa
                    .checked_mul(10)?
                    .checked_add(u64::from(byte - b'0'))?;
                if seen_point {
                    scale -= 1;
                }
            }
            b'.' => seen_point = true,
            b'e' | b'E' => break,
            _ => return None,
        }
        pos += 1;
    }
    if bytes.get(pos).is_some() {
        pos += 1;
        let (sign, start) = match bytes.get(pos)? {
            b'-' => (-1, pos + 1),
            b'+' => (1, pos + 1),
            _ => (1, pos),
        };
        let exponent: i32 = std::str::from_utf8(&bytes[start..]).ok()?.parse().ok()?;
        scale = scale.checked_add(sign * exponent)?;
    }
    if digits > 15 || mantissa >= 1 << 53 {
        return None;
    }
    let magnitude = mantissa as f64;
    let value = if scale >= 0 {
        magnitude * *EXACT_POWERS.get(usize::try_from(scale).ok()?)?
    } else {
        magnitude / *EXACT_POWERS.get(usize::try_from(-scale).ok()?)?
    };
    // serde_json reads "-0.0" as negative zero as well.
    Some(if negative { -value } else { value })
}

pub(crate) fn parse(bytes: &[u8]) -> Option<FlatDocument<'_>> {
    if bytes.len() > MAX_FLAT_BYTES {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let mut cursor = Cursor {
        text,
        bytes,
        pos: 0,
    };
    let mut document = FlatDocument::default();
    cursor.skip_ws();
    cursor.eat(b'{')?;
    cursor.skip_ws();
    if cursor.eat(b'}').is_none() {
        loop {
            cursor.skip_ws();
            let name = cursor.string()?;
            cursor.skip_ws();
            cursor.eat(b':')?;
            cursor.skip_ws();
            let value = cursor.scalar()?;
            if document.get(name).is_some() {
                // serde_json keeps the last duplicate; leave that to it.
                return None;
            }
            document.push((name, value));
            cursor.skip_ws();
            if cursor.eat(b',').is_some() {
                continue;
            }
            cursor.eat(b'}')?;
            break;
        }
    }
    cursor.skip_ws();
    (cursor.pos == bytes.len()).then_some(document)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every accepted input decodes to exactly what serde_json produces;
    /// every input serde_json rejects is declined.
    fn check(input: &str) {
        let full: Result<Value, _> = serde_json::from_str(input);
        if let Some(flat) = parse(input.as_bytes()) {
            let full = full.unwrap_or_else(|e| panic!("accepted invalid {input:?}: {e}"));
            let flat = Value::Object(flat.to_map());
            assert_eq!(flat, full, "{input:?}");
            // Value equality treats -0.0 and 0.0 as equal; text does not.
            assert_eq!(flat.to_string(), full.to_string(), "{input:?}");
        }
    }

    #[test]
    fn accepted_documents_match_serde_json_exactly() {
        for input in [
            "{}",
            " { } ",
            r#"{"a":1}"#,
            r#"{"a":-0,"b":0,"c":-9223372036854775808,"d":9223372036854775807}"#,
            r#"{"big":18446744073709551615,"over":18446744073709551616}"#,
            r#"{"f":1.5,"g":-2.25e-3,"h":1E10,"i":0.1,"j":123456789.123456789}"#,
            r#"{"f":2.2250738585072014e-308,"g":1.7976931348623157e308,"h":1e400}"#,
            r#"{"t":true,"f":false,"n":null,"s":"héllo","e":""}"#,
            "{\"a\" : 1 ,\n\t\"b\":\"x\" }",
            r#"{"esc":"a\nb"}"#,
            r#"{"a\"b":1}"#,
            r#"{"nested":{"x":1}}"#,
            r#"{"arr":[1,2]}"#,
            r#"{"dup":1,"dup":2}"#,
            r#"{"a":01}"#,
            r#"{"a":1.}"#,
            r#"{"a":.5}"#,
            r#"{"a":+1}"#,
            r#"{"a":1e}"#,
            r#"{"a":NaN}"#,
            r#"{"a":tru}"#,
            r#"{"a":1,}"#,
            r#"{"a":1} x"#,
            r#"{"a" 1}"#,
            "[1]",
            "\"s\"",
            "",
            "{",
            "{\"a\":\"\u{1}\"}",
        ] {
            check(input);
        }
    }

    /// Documents past the inline capacity keep every member, in order.
    #[test]
    fn documents_with_many_members_are_read_whole() {
        for members in [7, 8, 9, 16, 40] {
            let text = format!(
                "{{{}}}",
                (0..members)
                    .map(|i| format!(
                        "\"m{i}\":{}",
                        match i % 4 {
                            0 => format!("{}", i * 1_000_003),
                            1 => format!("\"s{i}\""),
                            2 => "-1.5".to_owned(),
                            _ => "null".to_owned(),
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let document = parse(text.as_bytes()).expect("flat");
            let expected: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(Value::Object(document.to_map()), expected, "{members}");
            assert_eq!(document.fields().len(), members);
            let names: Vec<_> = document.fields().iter().map(|(name, _)| *name).collect();
            assert_eq!(
                names,
                (0..members).map(|i| format!("m{i}")).collect::<Vec<_>>()
            );
            assert!(document.get(&format!("m{}", members - 1)).is_some());
            // A duplicate past the inline members still sends the caller away.
            let duplicate = format!("{},\"m0\":1}}", &text[..text.len() - 1]);
            assert!(parse(duplicate.as_bytes()).is_none(), "{members}");
        }
    }

    #[test]
    fn random_flat_documents_match_serde_json() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let pieces = [
            "1",
            "-1",
            "0",
            "-0",
            "1.5",
            "-0.0",
            "1e5",
            "1E-5",
            "123456789012345678901",
            "9223372036854775808",
            "-9223372036854775809",
            "true",
            "false",
            "null",
            "\"\"",
            "\"x\"",
            "\"é\"",
            "\"a\\\"b\"",
            "{}",
            "[]",
            "0.1",
            "1.7976931348623157e309",
            "00",
            "1.",
            "-",
            "\"\\u0041\"",
            "999999999999999999",
            "-999999999999999999",
            "1000000000000000000",
            "9223372036854775807",
            "-9223372036854775808",
            "18446744073709551615",
            "18446744073709551616",
        ];
        // Up to 14 members, beyond the inline capacity of 8.
        for _ in 0..40_000 {
            let members = next() % 15;
            let mut text = String::from("{");
            for index in 0..members {
                if index > 0 {
                    text.push(',');
                }
                let name = ["a", "b", "c", "a"][(next() % 4) as usize];
                text.push_str(&format!("\"{name}{}\":", next() % 8));
                text.push_str(pieces[(next() % pieces.len() as u64) as usize]);
            }
            text.push('}');
            check(&text);
        }
    }

    #[test]
    fn exact_float_fast_path_matches_serde_json_bits() {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut checked = 0;
        for _ in 0..200_000 {
            let digits = 1 + next() % 17;
            let mut text = String::new();
            if next() % 2 == 0 {
                text.push('-');
            }
            let point = next() % (digits + 1);
            for index in 0..digits {
                if index == point && index > 0 {
                    text.push('.');
                }
                let digit = if index == 0 {
                    1 + next() % 9
                } else {
                    next() % 10
                };
                text.push(char::from(b'0' + digit as u8));
            }
            if next() % 3 == 0 {
                text.push_str(&format!("e{}", (next() % 50) as i64 - 25));
            }
            let Some(fast) = exact_float(&text) else {
                continue;
            };
            let expected: f64 = serde_json::from_str(&text).unwrap();
            assert_eq!(fast.to_bits(), expected.to_bits(), "{text}");
            checked += 1;
        }
        assert!(checked > 50_000, "{checked}");
    }

    #[test]
    fn large_documents_use_the_full_decoder() {
        let text = format!("{{\"a\":\"{}\"}}", "x".repeat(MAX_FLAT_BYTES));
        assert!(parse(text.as_bytes()).is_none());
    }
}

#[cfg(test)]
mod schema_tests {
    use crate::document::schema::{CollectionSchema, FieldRule, FieldType};
    use crate::document::{DocumentTransaction, StoredDocument};

    fn rule(name: &str, field_type: FieldType, required: bool, nullable: bool) -> FieldRule {
        FieldRule {
            path: vec![name.into()],
            field_type,
            required,
            nullable,
        }
    }

    /// decode_stored accepts and rejects exactly what the full decoder does,
    /// and decodes accepted documents to the same value.
    #[test]
    fn flat_validation_matches_full_validation() {
        let schema = CollectionSchema {
            fields: vec![
                rule("id", FieldType::Integer, true, false),
                rule("r", FieldType::Number, false, true),
                rule("s", FieldType::StringBytes { min: 0, max: 3 }, false, true),
                rule("b", FieldType::Boolean, true, true),
                rule(
                    "n",
                    FieldType::IntegerRange { min: -5, max: 5 },
                    false,
                    false,
                ),
            ],
        };
        let values = [
            "1",
            "-7",
            "2.5",
            "-0",
            "9223372036854775808",
            "true",
            "false",
            "null",
            "\"\"",
            "\"abc\"",
            "\"abcd\"",
            "\"é\"",
            "\"éé\"",
        ];
        let names = ["id", "r", "s", "b", "n", "extra"];
        let mut state = 0x1234_5678_9abc_def1_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let (mut accepted, mut rejected) = (0, 0);
        for _ in 0..30_000 {
            let mut members: Vec<String> = Vec::new();
            for name in names {
                if next() % 4 != 0 {
                    // Mostly type-correct values, so both outcomes occur.
                    let valid = match name {
                        "id" => "3",
                        "r" => "1.25",
                        "s" => "\"ab\"",
                        "b" => "true",
                        "n" => "-2",
                        _ => "\"x\"",
                    };
                    let value = if next() % 3 == 0 {
                        values[(next() % values.len() as u64) as usize]
                    } else {
                        valid
                    };
                    members.push(format!("\"{name}\":{value}"));
                }
            }
            let text = format!("{{{}}}", members.join(","));
            let bytes = text.as_bytes();
            let full = DocumentTransaction::decode_document(bytes, &schema);
            let fast = DocumentTransaction::decode_stored(bytes, &schema);
            match (&full, &fast) {
                (Ok(value), Ok(StoredDocument::Flat(flat))) => {
                    assert_eq!(&serde_json::Value::Object(flat.to_map()), value, "{text}");
                    accepted += 1;
                }
                (Ok(value), Ok(StoredDocument::Full(other))) => assert_eq!(value, other),
                (Err(a), Err(b)) => {
                    assert_eq!(a.to_string(), b.to_string(), "{text}");
                    rejected += 1;
                }
                _ => panic!("{text}: full {full:?}, fast {:?}", fast.is_ok()),
            }
        }
        assert!(
            accepted > 1_000 && rejected > 1_000,
            "{accepted} {rejected}"
        );
    }
}
