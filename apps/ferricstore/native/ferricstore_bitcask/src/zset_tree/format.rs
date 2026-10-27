//! On-disk layout of a sorted-set tree file.
//!
//! ```text
//! [ header region: slot A | slot B | padding ][ frame ][ frame ] ...
//! frame = [ frame header | pages | logical ops | trailer ]
//! ```
//!
//! Frames are appended and never rewritten. A frame's trailer names the roots
//! of both trees as of that frame, so any valid trailer describes a whole,
//! consistent tree. The header slots only point at the latest frame and at the
//! last frame known to be on disk; recovery trusts the frames, not the slots.

use super::Result;

pub const PAGE_SIZE: usize = 4096;
pub const HEADER_REGION: u64 = 4096;
pub const SLOT_SIZE: usize = 256;
pub const FORMAT_VERSION: u32 = 1;
pub const FRAME_HEADER_SIZE: usize = 40;
pub const TRAILER_SIZE: usize = 72;
/// Keys longer than this keep a prefix in the node and the rest in overflow
/// pages.
pub const INLINE_KEY_MAX: usize = 512;
pub const OVERFLOW_PREFIX: usize = 64;
pub const MAX_MEMBER_LEN: usize = 16 * 1024 * 1024;

const SLOT_MAGIC: &[u8; 8] = b"FZSTHDR1";
const FRAME_MAGIC: u32 = 0x4652_4d31;
const TRAILER_MAGIC: u32 = 0x5452_4c31;

/// The Raft position a frame was written for. Frames carry it so a replayed
/// entry is recognized: a token at or below the file's latest is stale.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Token {
    pub index: u64,
    pub ordinal: u64,
}

impl Token {
    #[must_use]
    pub const fn new(index: u64, ordinal: u64) -> Self {
        Self { index, ordinal }
    }
}

/// -0.0 sorts and compares as 0.0, as in Redis; NaN is not a score.
pub fn normalize_score(score: f64) -> Result<f64> {
    if score.is_nan() {
        Err("zset tree: score is not a number".into())
    } else if score == 0.0 {
        Ok(0.0)
    } else {
        Ok(score)
    }
}

/// Big-endian bytes whose order is the order of the scores.
pub fn score_prefix(score: f64) -> Result<[u8; 8]> {
    let bits = normalize_score(score)?.to_bits();
    let sortable = if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    };
    Ok(sortable.to_be_bytes())
}

/// The prefix right after every key with this score's prefix, or `None` when
/// no prefix follows.
pub fn next_score_prefix(prefix: [u8; 8]) -> Option<[u8; 8]> {
    u64::from_be_bytes(prefix)
        .checked_add(1)
        .map(u64::to_be_bytes)
}

#[must_use]
pub fn crc(parts: &[&[u8]]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize()
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub fn get_u16(bytes: &[u8], at: usize) -> Result<u16> {
    bytes
        .get(at..at + 2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]))
        .ok_or_else(|| "zset tree: truncated field".to_string())
}

pub fn get_u32(bytes: &[u8], at: usize) -> Result<u32> {
    bytes
        .get(at..at + 4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| "zset tree: truncated field".to_string())
}

pub fn get_u64(bytes: &[u8], at: usize) -> Result<u64> {
    bytes
        .get(at..at + 8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| "zset tree: truncated field".to_string())
}

/// One of the two header slots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderSlot {
    pub generation: u64,
    /// End of the latest frame; `HEADER_REGION` when there is none.
    pub latest_frame_end: u64,
    /// End of the last frame known to be on disk.
    pub synced_frame_end: u64,
}

impl HeaderSlot {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            generation: 1,
            latest_frame_end: HEADER_REGION,
            synced_frame_end: HEADER_REGION,
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(SLOT_SIZE);
        out.extend_from_slice(SLOT_MAGIC);
        put_u32(&mut out, FORMAT_VERSION);
        put_u32(&mut out, PAGE_SIZE as u32);
        put_u64(&mut out, self.generation);
        put_u64(&mut out, self.latest_frame_end);
        put_u64(&mut out, self.synced_frame_end);
        let checksum = crc(&[&out]);
        put_u32(&mut out, checksum);
        out.resize(SLOT_SIZE, 0);
        out
    }

    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 44 || &bytes[0..8] != SLOT_MAGIC {
            return None;
        }
        let checksum = get_u32(bytes, 40).ok()?;
        if crc(&[&bytes[0..40]]) != checksum
            || get_u32(bytes, 8).ok()? != FORMAT_VERSION
            || get_u32(bytes, 12).ok()? as usize != PAGE_SIZE
        {
            return None;
        }
        let slot = Self {
            generation: get_u64(bytes, 16).ok()?,
            latest_frame_end: get_u64(bytes, 24).ok()?,
            synced_frame_end: get_u64(bytes, 32).ok()?,
        };
        (slot.latest_frame_end >= HEADER_REGION
            && slot.synced_frame_end >= HEADER_REGION
            && slot.synced_frame_end <= slot.latest_frame_end)
            .then_some(slot)
    }

    /// Slot A holds odd generations, slot B even ones.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        if self.generation % 2 == 1 {
            0
        } else {
            SLOT_SIZE as u64
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub page_count: u32,
    pub ops_len: u32,
    pub token: Token,
    /// A compacted file's frame: the whole tree, with no earlier frame to
    /// rewind to.
    pub base: bool,
}

impl FrameHeader {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(FRAME_HEADER_SIZE);
        put_u32(&mut out, FRAME_MAGIC);
        put_u32(&mut out, self.page_count);
        put_u32(&mut out, self.ops_len);
        put_u32(&mut out, u32::from(self.base));
        put_u64(&mut out, self.token.index);
        put_u64(&mut out, self.token.ordinal);
        let checksum = crc(&[&out]);
        put_u32(&mut out, checksum);
        put_u32(&mut out, 0);
        out
    }

    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < FRAME_HEADER_SIZE || get_u32(bytes, 0).ok()? != FRAME_MAGIC {
            return None;
        }
        if crc(&[&bytes[0..32]]) != get_u32(bytes, 32).ok()? {
            return None;
        }
        Some(Self {
            page_count: get_u32(bytes, 4).ok()?,
            ops_len: get_u32(bytes, 8).ok()?,
            token: Token::new(get_u64(bytes, 16).ok()?, get_u64(bytes, 24).ok()?),
            base: get_u32(bytes, 12).ok()? & 1 == 1,
        })
    }

    #[must_use]
    pub fn frame_len(&self) -> u64 {
        FRAME_HEADER_SIZE as u64
            + u64::from(self.page_count) * PAGE_SIZE as u64
            + u64::from(self.ops_len)
            + TRAILER_SIZE as u64
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Trailer {
    pub score_root: u64,
    pub member_root: u64,
    pub count: u64,
    pub frame_start: u64,
    pub generation: u64,
    pub token: Token,
    /// CRC of the frame header, pages and ops.
    pub body_crc: u32,
}

impl Trailer {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(TRAILER_SIZE);
        put_u64(&mut out, self.score_root);
        put_u64(&mut out, self.member_root);
        put_u64(&mut out, self.count);
        put_u64(&mut out, self.frame_start);
        put_u64(&mut out, self.generation);
        put_u64(&mut out, self.token.index);
        put_u64(&mut out, self.token.ordinal);
        put_u32(&mut out, self.body_crc);
        put_u32(&mut out, TRAILER_MAGIC);
        let checksum = crc(&[&out]);
        put_u32(&mut out, checksum);
        put_u32(&mut out, 0);
        out
    }

    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < TRAILER_SIZE || get_u32(bytes, 60).ok()? != TRAILER_MAGIC {
            return None;
        }
        if crc(&[&bytes[0..64]]) != get_u32(bytes, 64).ok()? {
            return None;
        }
        Some(Self {
            score_root: get_u64(bytes, 0).ok()?,
            member_root: get_u64(bytes, 8).ok()?,
            count: get_u64(bytes, 16).ok()?,
            frame_start: get_u64(bytes, 24).ok()?,
            generation: get_u64(bytes, 32).ok()?,
            token: Token::new(get_u64(bytes, 40).ok()?, get_u64(bytes, 48).ok()?),
            body_crc: get_u32(bytes, 56).ok()?,
        })
    }
}

/// What a frame changed, member by member, with what each member had before.
/// Compaction replays these onto a rewritten file to catch up with frames
/// written while it copied, and keeps them, newest last, as the undo log a
/// replayed entry rewinds through.
#[derive(Clone, Debug, PartialEq)]
pub enum LoggedOp {
    /// `member` now has `score`; it had `prev` (`None`: it was not in the set).
    Set {
        member: Vec<u8>,
        score: f64,
        prev: Option<f64>,
    },
    /// `member` left the set; it had `prev`.
    Remove { member: Vec<u8>, prev: f64 },
}

const OP_SET_NEW: u8 = 1;
const OP_REMOVE: u8 = 2;
const OP_SET_CHANGED: u8 = 3;

pub fn encode_ops(ops: &[LoggedOp]) -> Vec<u8> {
    let mut out = Vec::new();
    for op in ops {
        let (kind, member) = match op {
            LoggedOp::Set {
                member, prev: None, ..
            } => (OP_SET_NEW, member),
            LoggedOp::Set { member, .. } => (OP_SET_CHANGED, member),
            LoggedOp::Remove { member, .. } => (OP_REMOVE, member),
        };
        out.push(kind);
        put_u32(&mut out, member.len() as u32);
        out.extend_from_slice(member);
        match op {
            LoggedOp::Set { score, prev, .. } => {
                put_u64(&mut out, score.to_bits());
                if let Some(prev) = prev {
                    put_u64(&mut out, prev.to_bits());
                }
            }
            LoggedOp::Remove { prev, .. } => put_u64(&mut out, prev.to_bits()),
        }
    }
    out
}

pub fn decode_ops(mut bytes: &[u8]) -> Result<Vec<LoggedOp>> {
    let mut ops = Vec::new();
    while !bytes.is_empty() {
        let kind = bytes[0];
        let len = get_u32(bytes, 1)? as usize;
        let member = bytes
            .get(5..5 + len)
            .ok_or_else(|| "zset tree: truncated logged op".to_string())?
            .to_vec();
        bytes = &bytes[5 + len..];
        let mut next = || -> Result<f64> {
            let value = f64::from_bits(get_u64(bytes, 0)?);
            bytes = &bytes[8..];
            Ok(value)
        };
        ops.push(match kind {
            OP_SET_NEW => LoggedOp::Set {
                member,
                score: next()?,
                prev: None,
            },
            OP_SET_CHANGED => {
                let score = next()?;
                LoggedOp::Set {
                    member,
                    score,
                    prev: Some(next()?),
                }
            }
            OP_REMOVE => LoggedOp::Remove {
                member,
                prev: next()?,
            },
            _ => return Err("zset tree: unknown logged op".into()),
        });
    }
    Ok(ops)
}

/// A compacted file's record of the changes it folded in after `floor` (an
/// index Raft never replays at or below), one group per frame, oldest first.
/// A replayed entry after the floor rewinds the file by undoing the groups at
/// and after it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UndoLog {
    pub floor: u64,
    pub groups: Vec<(Token, Vec<LoggedOp>)>,
}

impl UndoLog {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u64(&mut out, self.floor);
        for (token, ops) in &self.groups {
            let ops = encode_ops(ops);
            put_u64(&mut out, token.index);
            put_u64(&mut out, token.ordinal);
            put_u32(&mut out, ops.len() as u32);
            out.extend_from_slice(&ops);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::default());
        }
        let floor = get_u64(bytes, 0)?;
        let mut rest = &bytes[8..];
        let mut groups = Vec::new();
        while !rest.is_empty() {
            let token = Token::new(get_u64(rest, 0)?, get_u64(rest, 8)?);
            let len = get_u32(rest, 16)? as usize;
            let ops = rest
                .get(20..20 + len)
                .ok_or_else(|| "zset tree: truncated undo log".to_string())?;
            groups.push((token, decode_ops(ops)?));
            rest = &rest[20 + len..];
        }
        Ok(Self { floor, groups })
    }
}
