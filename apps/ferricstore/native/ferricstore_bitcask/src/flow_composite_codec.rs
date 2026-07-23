use sha2::{Digest, Sha256};

const HEADER_BYTES: usize = 21;
const MAX_KEY_BYTES: usize = 511;
const MAX_COMPONENT_BYTES: usize = 65_535;
// Reserve the longest `f:{f:<43-byte digest>}:s:` state-key envelope.
const MAX_RUN_ID_BYTES: usize = MAX_COMPONENT_BYTES - 52;
const MAX_EXACT_INTEGER: u64 = 9_007_199_254_740_991;

#[inline]
pub(crate) fn decode_entry<'a>(
    key: &[u8],
    value: &'a [u8],
    hasher: &mut Sha256,
) -> Option<(&'a [u8], &'a [u8], u64, u64)> {
    if key.len() > MAX_KEY_BYTES || value.len() <= HEADER_BYTES || value[0] != 1 {
        return None;
    }
    let id_len = usize::try_from(u32::from_be_bytes(value[1..5].try_into().ok()?)).ok()?;
    if id_len == 0 || id_len > MAX_RUN_ID_BYTES || HEADER_BYTES + id_len >= value.len() {
        return None;
    }
    let record_version = u64::from_be_bytes(value[5..13].try_into().ok()?);
    if record_version > MAX_EXACT_INTEGER {
        return None;
    }
    let expire_at_ms = u64::from_be_bytes(value[13..21].try_into().ok()?);
    let id = &value[HEADER_BYTES..HEADER_BYTES + id_len];
    let state_key = &value[HEADER_BYTES + id_len..];
    if state_key.len() > MAX_COMPONENT_BYTES || !state_key_owns_id(state_key, id) {
        return None;
    }
    if key.len() < 33 || key[key.len() - 33] != 0x60 {
        return None;
    }
    hasher.update(id);
    let digest = hasher.finalize_reset();
    if key[key.len() - 32..] != digest[..] {
        return None;
    }
    Some((id, state_key, record_version, expire_at_ms))
}

#[inline]
fn state_key_owns_id(state_key: &[u8], id: &[u8]) -> bool {
    const MARKER: &[u8] = b"}:s:";
    if !state_key.starts_with(b"f:{") {
        return false;
    }
    state_key[3..]
        .windows(MARKER.len())
        .position(|window| window == MARKER)
        .is_some_and(|position| {
            let tag = &state_key[3..3 + position];
            valid_flow_tag(tag) && &state_key[3 + position + MARKER.len()..] == id
        })
}

#[inline]
fn valid_flow_tag(tag: &[u8]) -> bool {
    if tag == b"f" {
        return true;
    }
    if let Some(bucket) = tag.strip_prefix(b"fa:") {
        return !bucket.is_empty()
            && (bucket.len() == 1 || bucket[0] != b'0')
            && bucket.iter().all(u8::is_ascii_digit)
            && bucket.iter().fold(0u16, |value, digit| {
                value
                    .saturating_mul(10)
                    .saturating_add(u16::from(*digit - b'0'))
            }) <= 255;
    }
    let Some(digest) = tag.strip_prefix(b"f:") else {
        return false;
    };
    digest.len() == 43
        && digest[..42]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_'))
        && matches!(
            digest[42],
            b'A' | b'E'
                | b'I'
                | b'M'
                | b'Q'
                | b'U'
                | b'Y'
                | b'c'
                | b'g'
                | b'k'
                | b'o'
                | b's'
                | b'w'
                | b'0'
                | b'4'
                | b'8'
        )
}

#[cfg(test)]
mod composite_entry_codec_tests {
    use super::*;

    fn entry_key(id: &[u8]) -> Vec<u8> {
        let mut key = vec![0x60];
        key.extend_from_slice(&Sha256::digest(id));
        key
    }

    fn entry_value(id: &[u8], state_key: &[u8], record_version: u64) -> Vec<u8> {
        let mut value = Vec::with_capacity(HEADER_BYTES + id.len() + state_key.len());
        value.push(1);
        value.extend_from_slice(&u32::try_from(id.len()).unwrap().to_be_bytes());
        value.extend_from_slice(&record_version.to_be_bytes());
        value.extend_from_slice(&5_000_u64.to_be_bytes());
        value.extend_from_slice(id);
        value.extend_from_slice(state_key);
        value
    }

    #[test]
    fn flow_tag_validation_matches_the_canonical_elixir_grammar() {
        assert!(valid_flow_tag(b"f"));
        assert!(valid_flow_tag(b"fa:0"));
        assert!(valid_flow_tag(b"fa:255"));
        assert!(valid_flow_tag(
            &(*b"f:").into_iter().chain([b'A'; 43]).collect::<Vec<_>>()
        ));

        assert!(!valid_flow_tag(b"invalid"));
        assert!(!valid_flow_tag(b"fa:00"));
        assert!(!valid_flow_tag(b"fa:256"));
        assert!(!valid_flow_tag(
            &(*b"f:").into_iter().chain([b'A'; 42]).collect::<Vec<_>>()
        ));

        let mut invalid_final = [b'A'; 43];
        invalid_final[42] = b'B';
        assert!(!valid_flow_tag(
            &(*b"f:")
                .into_iter()
                .chain(invalid_final)
                .collect::<Vec<_>>()
        ));
    }

    #[test]
    fn compact_entry_decoder_validates_owner_version_and_key_digest() {
        let id = b"run-1";
        let state_key = b"f:{fa:17}:s:run-1";
        let key = entry_key(id);
        let value = entry_value(id, state_key, 3);
        let mut hasher = Sha256::new();

        assert_eq!(
            decode_entry(&key, &value, &mut hasher),
            Some((id.as_slice(), state_key.as_slice(), 3, 5_000))
        );

        let wrong_owner = entry_value(id, b"f:{fa:17}:s:other", 3);
        assert!(decode_entry(&key, &wrong_owner, &mut hasher).is_none());

        let invalid_tag = entry_value(id, b"f:{fa:017}:s:run-1", 3);
        assert!(decode_entry(&key, &invalid_tag, &mut hasher).is_none());

        let oversized_version = entry_value(id, state_key, MAX_EXACT_INTEGER + 1);
        assert!(decode_entry(&key, &oversized_version, &mut hasher).is_none());

        let mut wrong_key = key.clone();
        *wrong_key.last_mut().unwrap() ^= 1;
        assert!(decode_entry(&wrong_key, &value, &mut hasher).is_none());
        assert!(decode_entry(&key, &value[..value.len() - 1], &mut hasher).is_none());
    }

    #[test]
    fn compact_entry_decoder_enforces_the_canonical_run_id_ceiling() {
        let id = vec![b'r'; MAX_RUN_ID_BYTES + 1];
        let mut state_key = b"f:{f}:s:".to_vec();
        state_key.extend_from_slice(&id);
        let key = entry_key(&id);
        let value = entry_value(&id, &state_key, 1);

        assert!(decode_entry(&key, &value, &mut Sha256::new()).is_none());
    }
}
