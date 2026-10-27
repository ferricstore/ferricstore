//! Disk-native sorted sets.
//!
//! One file per sorted set holds two copy-on-write B+trees: members by
//! `(score, member)`, whose branches count the members under each child, and
//! scores by member. Positions are found in one descent, so ZRANK and ZRANGE
//! by index cost O(log N) like every other operation.
//!
//! Every call opens the file, reads the pages it needs, appends a frame for
//! its changes, and closes it: nothing stays in memory between calls, and the
//! kernel page cache keeps hot pages. Writes come from one writer (Raft
//! apply); readers run alongside it, since a frame is never rewritten.

use std::path::Path;

mod format;
mod maintain;
mod nif;
mod node;
mod ops;
mod store;
mod tree;

#[cfg(test)]
mod tests;

pub type Result<T> = std::result::Result<T, String>;

pub use format::{LoggedOp, Token, MAX_MEMBER_LEN};
pub use maintain::{compact, recover, sync, Compacted, Compaction, Recovery, TEMP_SUFFIXES};

use maintain::Rewound;
pub use ops::{Conditions, Entry, LexBound, Op, Outcome, ScoreBound};
pub use store::TreeState;
pub use tree::Tx;

use store::TreeFile;

#[derive(Clone, Debug, PartialEq)]
pub struct Applied {
    pub outcomes: Vec<Outcome>,
    pub state: TreeState,
}

fn op_member(op: &Op) -> Option<&[u8]> {
    match op {
        Op::Add { member, .. } | Op::Incr { member, .. } | Op::Remove { member } => Some(member),
        Op::PopMin { .. } | Op::PopMax { .. } => None,
    }
}

/// Applies `ops` in order as one frame, all or nothing. A token at or below
/// the file's latest is a replayed Raft entry: the file is first cut back to
/// before it, so it gets the outcomes it had the first time. A token of index
/// 0 (no Raft log) takes the next index after the file's.
pub fn apply(path: &Path, token: Token, ops: &[Op]) -> Result<Applied> {
    if ops
        .iter()
        .filter_map(op_member)
        .any(|member| member.len() > MAX_MEMBER_LEN)
    {
        return Err("zset tree: member too long".into());
    }
    let mut file = TreeFile::open_write(path)?;
    let mut state = file.state()?;
    let token = if token.index == 0 {
        Token::new(state.token.index + 1, token.ordinal.max(1))
    } else {
        token
    };
    if token <= state.token {
        match maintain::rewind(path, &file, state, token)? {
            Rewound::InPlace(rewound) => state = rewound,
            Rewound::Rewritten => {
                drop(file);
                file = TreeFile::open_write(path)?;
                state = file.state()?;
            }
        }
    }
    let mut tx = Tx::begin(&file, state);
    let outcomes = ops
        .iter()
        .map(|op| tx.apply(op))
        .collect::<Result<Vec<_>>>()?;
    let state = tx.commit(token)?;
    Ok(Applied { outcomes, state })
}

/// Cuts the file back to the last frame before `token` (a failed transaction's
/// first write). A file with nothing at or past the token, or no file, is left
/// as it is.
pub fn rewind_to(path: &Path, token: Token) -> Result<()> {
    let Some(file) = TreeFile::open_existing(path)? else {
        return Ok(());
    };
    let state = file.state()?;
    if token <= state.token {
        maintain::rewind(path, &file, state, token)?;
    }
    Ok(())
}

/// The file's length and its first frame's (the tree as conversion or the
/// last compaction built it): how far writes have grown the file. `None` when
/// the file does not exist.
pub fn file_stats(path: &Path) -> Result<Option<(u64, u64)>> {
    let Some(file) = TreeFile::open_read(path)? else {
        return Ok(None);
    };
    let first = if file.len() >= format::HEADER_REGION + format::FRAME_HEADER_SIZE as u64 {
        format::FrameHeader::decode(
            &file.read_exact(format::HEADER_REGION, format::FRAME_HEADER_SIZE)?,
        )
        .map_or(0, |header| header.frame_len())
    } else {
        0
    };
    Ok(Some((file.len(), first)))
}

/// A read that saw its frame cut away is run again, this many times at most.
const READ_ATTEMPTS: usize = 16;

/// Runs `f` on the latest tree; `None` when the file does not exist.
///
/// Readers take no lock. A failed transaction cuts its frames off while
/// others read, and a later frame can reuse their bytes; so once `f` is done
/// the frame it read must still end the same way, or `f` runs again on the
/// file as it is now.
pub fn read<R>(path: &Path, f: impl Fn(&Tx<'_>) -> Result<R>) -> Result<Option<R>> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match read_once(path, &f) {
            Ok(Some(result)) => return Ok(result),
            Ok(None) if attempt < READ_ATTEMPTS => {}
            Err(_reason) if attempt < READ_ATTEMPTS => {}
            Ok(None) => return Err("zset tree: the file kept changing under a read".into()),
            Err(reason) => return Err(reason),
        }
    }
}

/// `Some(result)` when the frame read is still in place after `f`.
fn read_once<R>(path: &Path, f: &impl Fn(&Tx<'_>) -> Result<R>) -> Result<Option<Option<R>>> {
    let Some(file) = TreeFile::open_read(path)? else {
        return Ok(Some(None));
    };
    let state = file.state()?;
    let result = f(&Tx::begin(&file, state))?;
    let unchanged = state.frame_end == format::HEADER_REGION
        || file
            .trailer_ending_at(state.frame_end)?
            .is_some_and(|trailer| {
                trailer.token == state.token
                    && trailer.score_root == state.score_root
                    && trailer.member_root == state.member_root
            });
    Ok(unchanged.then_some(Some(result)))
}
