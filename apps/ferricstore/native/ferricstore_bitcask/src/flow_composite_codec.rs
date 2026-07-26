use sha2::{Digest, Sha256};

const CHECKED_PREFIX_BYTES: usize = 5;
const PLAIN_BODY_HEADER_BYTES: usize = 20;
const COVERING_BODY_HEADER_BYTES: usize = 28;
const MAX_COVERING_BYTES: usize = 64 * 1024;
const MAX_KEY_BYTES: usize = 511;
const MAX_COMPONENT_BYTES: usize = 65_535;
// Reserve the longest `f:{f:<43-byte digest>}:s:` state-key envelope.
const MAX_RUN_ID_BYTES: usize = MAX_COMPONENT_BYTES - 52;
const MAX_EXACT_INTEGER: u64 = 9_007_199_254_740_991;

pub(crate) type DecodedEntry<'a> = (&'a [u8], &'a [u8], u64, u64, Option<&'a [u8]>);

#[inline]
pub(crate) fn decode_entry<'a>(
    key: &[u8],
    value: &'a [u8],
    hasher: &mut Sha256,
) -> Option<DecodedEntry<'a>> {
    if key.len() > MAX_KEY_BYTES || value.is_empty() {
        return None;
    }

    let (id, state_key, record_version, expire_at_ms, covering) = match value[0] {
        1 => decode_plain_value(value)?,
        2 => decode_covering_value(value)?,
        _ => return None,
    };

    if record_version > MAX_EXACT_INTEGER {
        return None;
    }
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
    Some((id, state_key, record_version, expire_at_ms, covering))
}

#[inline]
fn decode_plain_value(value: &[u8]) -> Option<DecodedEntry<'_>> {
    let body = checked_body(value)?;
    if body.len() <= PLAIN_BODY_HEADER_BYTES {
        return None;
    }
    let id_len = usize::try_from(u32::from_be_bytes(body[0..4].try_into().ok()?)).ok()?;
    if id_len == 0 || id_len > MAX_RUN_ID_BYTES || PLAIN_BODY_HEADER_BYTES + id_len >= body.len() {
        return None;
    }
    let record_version = u64::from_be_bytes(body[4..12].try_into().ok()?);
    let expire_at_ms = u64::from_be_bytes(body[12..20].try_into().ok()?);
    let id = &body[PLAIN_BODY_HEADER_BYTES..PLAIN_BODY_HEADER_BYTES + id_len];
    let state_key = &body[PLAIN_BODY_HEADER_BYTES + id_len..];
    Some((id, state_key, record_version, expire_at_ms, None))
}

#[inline]
fn decode_covering_value(value: &[u8]) -> Option<DecodedEntry<'_>> {
    let body = checked_body(value)?;
    if body.len() <= COVERING_BODY_HEADER_BYTES {
        return None;
    }
    let id_len = usize::try_from(u32::from_be_bytes(body[0..4].try_into().ok()?)).ok()?;
    let state_key_len = usize::try_from(u32::from_be_bytes(body[4..8].try_into().ok()?)).ok()?;
    let record_version = u64::from_be_bytes(body[8..16].try_into().ok()?);
    let expire_at_ms = u64::from_be_bytes(body[16..24].try_into().ok()?);
    let covering_len = usize::try_from(u32::from_be_bytes(body[24..28].try_into().ok()?)).ok()?;
    let payload_len = id_len
        .checked_add(state_key_len)?
        .checked_add(covering_len)?;

    if id_len == 0
        || id_len > MAX_RUN_ID_BYTES
        || state_key_len == 0
        || state_key_len > MAX_COMPONENT_BYTES
        || covering_len == 0
        || covering_len > MAX_COVERING_BYTES
        || body.len() != COVERING_BODY_HEADER_BYTES.checked_add(payload_len)?
    {
        return None;
    }

    let id_start = COVERING_BODY_HEADER_BYTES;
    let state_key_start = id_start + id_len;
    let covering_start = state_key_start + state_key_len;
    let id = &body[id_start..state_key_start];
    let state_key = &body[state_key_start..covering_start];
    let covering = &body[covering_start..];
    Some((id, state_key, record_version, expire_at_ms, Some(covering)))
}

#[inline]
fn checked_body(value: &[u8]) -> Option<&[u8]> {
    if value.len() <= CHECKED_PREFIX_BYTES {
        return None;
    }
    let expected = u32::from_be_bytes(value[1..CHECKED_PREFIX_BYTES].try_into().ok()?);
    let body = &value[CHECKED_PREFIX_BYTES..];
    (crc32fast::hash(body) == expected).then_some(body)
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
        let mut body = Vec::with_capacity(PLAIN_BODY_HEADER_BYTES + id.len() + state_key.len());
        body.extend_from_slice(&u32::try_from(id.len()).unwrap().to_be_bytes());
        body.extend_from_slice(&record_version.to_be_bytes());
        body.extend_from_slice(&5_000_u64.to_be_bytes());
        body.extend_from_slice(id);
        body.extend_from_slice(state_key);
        checked_value(1, body)
    }

    fn covering_entry_value(
        id: &[u8],
        state_key: &[u8],
        record_version: u64,
        covering: &[u8],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&u32::try_from(id.len()).unwrap().to_be_bytes());
        body.extend_from_slice(&u32::try_from(state_key.len()).unwrap().to_be_bytes());
        body.extend_from_slice(&record_version.to_be_bytes());
        body.extend_from_slice(&5_000_u64.to_be_bytes());
        body.extend_from_slice(&u32::try_from(covering.len()).unwrap().to_be_bytes());
        body.extend_from_slice(id);
        body.extend_from_slice(state_key);
        body.extend_from_slice(covering);
        checked_value(2, body)
    }

    fn checked_value(tag: u8, body: Vec<u8>) -> Vec<u8> {
        let mut value = Vec::with_capacity(CHECKED_PREFIX_BYTES + body.len());
        value.push(tag);
        value.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
        value.extend_from_slice(&body);
        value
    }

    fn rewrite_checksum(value: &mut [u8]) {
        let checksum = crc32fast::hash(&value[CHECKED_PREFIX_BYTES..]);
        value[1..CHECKED_PREFIX_BYTES].copy_from_slice(&checksum.to_be_bytes());
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
            Some((id.as_slice(), state_key.as_slice(), 3, 5_000, None))
        );

        let covering = b"bounded-covering-record";
        let covering_value = covering_entry_value(id, state_key, 3, covering);

        assert_eq!(
            decode_entry(&key, &covering_value, &mut hasher),
            Some((
                id.as_slice(),
                state_key.as_slice(),
                3,
                5_000,
                Some(covering.as_slice())
            ))
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

        for valid in [&value, &covering_value] {
            for offset in 0..valid.len() {
                let mut corrupted = valid.clone();
                corrupted[offset] ^= 1;
                assert!(decode_entry(&key, &corrupted, &mut hasher).is_none());
            }
        }
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

    #[test]
    fn compact_entry_decoder_rejects_malformed_or_oversized_covering_payloads() {
        let id = b"run-1";
        let state_key = b"f:{fa:17}:s:run-1";
        let key = entry_key(id);
        let valid = covering_entry_value(id, state_key, 3, b"cover");

        assert!(decode_entry(&key, &valid[..valid.len() - 1], &mut Sha256::new()).is_none());

        let mut zero_cover = valid.clone();
        zero_cover[29..33].copy_from_slice(&0_u32.to_be_bytes());
        rewrite_checksum(&mut zero_cover);
        assert!(decode_entry(&key, &zero_cover, &mut Sha256::new()).is_none());

        let mut wrong_state_length = valid.clone();
        wrong_state_length[9..13]
            .copy_from_slice(&u32::try_from(state_key.len() + 1).unwrap().to_be_bytes());
        rewrite_checksum(&mut wrong_state_length);
        assert!(decode_entry(&key, &wrong_state_length, &mut Sha256::new()).is_none());

        let oversized = covering_entry_value(id, state_key, 3, &vec![0; MAX_COVERING_BYTES + 1]);
        assert!(decode_entry(&key, &oversized, &mut Sha256::new()).is_none());
    }
}
