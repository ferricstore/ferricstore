use sha2::{Digest, Sha256};
use std::borrow::Cow;

const MAX_DIRECT_STATE_KEY_BYTES: usize = 511;
const PHYSICAL_STATE_KEY_PREFIX: &[u8] = b"\0flsk:1:";

pub(crate) fn physical_state_key(logical_state_key: &[u8]) -> Cow<'_, [u8]> {
    if logical_state_key.len() <= MAX_DIRECT_STATE_KEY_BYTES {
        Cow::Borrowed(logical_state_key)
    } else {
        let digest = Sha256::digest(logical_state_key);
        let mut physical_key = Vec::with_capacity(PHYSICAL_STATE_KEY_PREFIX.len() + digest.len());
        physical_key.extend_from_slice(PHYSICAL_STATE_KEY_PREFIX);
        physical_key.extend_from_slice(&digest);
        Cow::Owned(physical_key)
    }
}
