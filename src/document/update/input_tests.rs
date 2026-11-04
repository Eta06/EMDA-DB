use super::*;

fn verify(value: &str) {
    let expected = serde_json::to_vec(value).unwrap().len();
    assert_eq!(
        json_string_length(value, expected).unwrap(),
        expected,
        "{value:?}"
    );
    assert!(
        json_string_length(value, expected - 1).is_err(),
        "{value:?}"
    );
}

#[test]
fn every_ascii_byte_is_counted_at_each_word_offset() {
    verify("");
    for length in 1..=48 {
        for position in 0..length {
            for byte in 0..=127 {
                let mut input = vec![b'a'; length];
                input[position] = byte;
                verify(&String::from_utf8(input).unwrap());
            }
        }
    }
}

#[test]
fn unicode_and_dense_escapes_match_json_encoding() {
    for fragment in [
        "é", "語", "🦀", "\u{2028}", "\u{2029}", "\0", "\n", "\\", "\"",
    ] {
        for prefix in 0..33 {
            verify(&format!(
                "{}{}{}",
                "x".repeat(prefix),
                fragment.repeat(40),
                "z".repeat(33 - prefix)
            ));
        }
    }
    let alphabet = [
        'a', '"', '\\', '\0', '\t', '\n', '\u{1f}', 'é', '語', '🦀', '\u{2028}',
    ];
    let mut state = 0x91e1_0da5_u64;
    for _ in 0..10_000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let count = (state % 128) as usize;
        let mut value = String::new();
        for _ in 0..count {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            value.push(alphabet[(state as usize) % alphabet.len()]);
        }
        verify(&value);
    }
}

fn verify_value(value: &Value) {
    let expected = serde_json::to_vec(value).unwrap().len();
    assert!(expected <= MAX_DOCUMENT_UPDATE_INPUT_BYTES);
    let mut exact = InputBudget(MAX_DOCUMENT_UPDATE_INPUT_BYTES - expected);
    exact.value(value).unwrap();
    assert_eq!(exact.0, MAX_DOCUMENT_UPDATE_INPUT_BYTES, "{value:?}");
    let mut over = InputBudget(MAX_DOCUMENT_UPDATE_INPUT_BYTES - expected + 1);
    assert!(over.value(value).is_err(), "{value:?}");
}

fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn generated(state: &mut u64, depth: usize) -> Value {
    let kind = next(state) % if depth >= 4 { 6 } else { 8 };
    match kind {
        0 => Value::Null,
        1 => Value::Bool(next(state) & 1 != 0),
        2 => Value::from(next(state) as i64),
        3 => Value::from(next(state)),
        4 => Value::from((next(state) as i32 as f64) / 7.0),
        5 => Value::String(
            ["", "\0\"\\", "é語🦀", "\u{2028}"][(next(state) % 4) as usize].to_owned(),
        ),
        6 => Value::Array(
            (0..next(state) % 4)
                .map(|_| generated(state, depth + 1))
                .collect(),
        ),
        _ => {
            let mut object = serde_json::Map::new();
            for index in 0..next(state) % 4 {
                let prefix = ["", "\0\"\\", "é語🦀", "\u{2028}"][(next(state) % 4) as usize];
                object.insert(format!("{prefix}{index}"), generated(state, depth + 1));
            }
            Value::Object(object)
        }
    }
}

#[test]
fn nested_values_match_serde_json_at_the_exact_boundary() {
    for value in [
        serde_json::json!([]),
        serde_json::json!({}),
        serde_json::json!({"\0\"\\":[null, true, false, -1, 0, 1.25, "é語🦀", {"z":"\n"}]}),
        serde_json::json!(u64::MAX),
        serde_json::json!(i64::MIN),
        serde_json::json!(-0.0),
    ] {
        verify_value(&value);
    }
    let mut state = 0xd0c0_1234_aa55_u64;
    for _ in 0..10_000 {
        verify_value(&generated(&mut state, 0));
    }
    let mut deepest = Value::Null;
    for _ in 0..64 {
        deepest = Value::Array(vec![deepest]);
    }
    verify_value(&deepest);
    let too_deep = Value::Array(vec![deepest]);
    assert!(InputBudget::default().value(&too_deep).is_err());
    verify_value(&Value::Array(vec![Value::Null; 131_071]));
}
