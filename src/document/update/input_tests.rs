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
