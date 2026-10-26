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
pub use maintain::{compact, recover, sync, Compacted, Compaction, Recovery};
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
    let file = TreeFile::open_write(path)?;
    let state = file.state()?;
    let token = if token.index == 0 {
        Token::new(state.token.index + 1, token.ordinal.max(1))
    } else {
        token
    };
    let state = if token <= state.token {
        maintain::rewind(&file, state, token)?
    } else {
        state
    };
    let mut tx = Tx::begin(&file, state);
    let outcomes = ops
        .iter()
        .map(|op| tx.apply(op))
        .collect::<Result<Vec<_>>>()?;
    let state = tx.commit(token)?;
    Ok(Applied { outcomes, state })
}

/// Runs `f` on the latest tree; `None` when the file does not exist.
pub fn read<R>(path: &Path, f: impl FnOnce(&Tx<'_>) -> Result<R>) -> Result<Option<R>> {
    let Some(file) = TreeFile::open_read(path)? else {
        return Ok(None);
    };
    let state = file.state()?;
    let tx = Tx::begin(&file, state);
    f(&tx).map(Some)
}
