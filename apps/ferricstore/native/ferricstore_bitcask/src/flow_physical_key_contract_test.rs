use crate::flow_physical_key::physical_state_key;
use std::borrow::Cow;

const VECTORS: &str = include_str!("../../../test/fixtures/flow_lmdb_physical_key_vectors.txt");

#[test]
fn physical_state_keys_match_the_shared_contract_vectors() {
    for row in VECTORS
        .lines()
        .filter(|row| !row.is_empty() && !row.starts_with('#'))
    {
        let columns = row.split('|').collect::<Vec<_>>();
        assert_eq!(columns.len(), 5);

        let name = columns[0];
        let prefix = columns[1].as_bytes();
        let fill = columns[2].as_bytes();
        let logical_bytes = columns[3].parse::<usize>().unwrap();
        let expected = columns[4];
        assert_eq!(fill.len(), 1);
        assert!(logical_bytes >= prefix.len());

        let mut logical_key = Vec::with_capacity(logical_bytes);
        logical_key.extend_from_slice(prefix);
        logical_key.extend(std::iter::repeat(fill[0]).take(logical_bytes - prefix.len()));

        let physical_key = physical_state_key(&logical_key);

        if expected == "direct" {
            assert!(matches!(physical_key, Cow::Borrowed(_)), "{name}");
            assert_eq!(physical_key.as_ref(), logical_key.as_slice(), "{name}");
        } else {
            assert!(matches!(physical_key, Cow::Owned(_)), "{name}");
            assert_eq!(physical_key.as_ref(), decode_hex(expected), "{name}");
        }
    }
}

fn decode_hex(encoded: &str) -> Vec<u8> {
    assert_eq!(encoded.len() % 2, 0);

    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("invalid hexadecimal fixture"),
    }
}
