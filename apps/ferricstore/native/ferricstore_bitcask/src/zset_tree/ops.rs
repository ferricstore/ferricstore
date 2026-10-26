//! Sorted-set semantics over a transaction.
//!
//! Every read turns its bounds into a range of positions in one of the two
//! trees, then reads that range: O(log N) to find it plus one step per
//! returned member.

use super::format::{next_score_prefix, normalize_score, score_prefix, LoggedOp};
use super::tree::{Tree, Tx};
use super::Result;

/// ZADD / ZINCRBY conditions, as Redis spells them.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Conditions {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Add {
        member: Vec<u8>,
        score: f64,
        conditions: Conditions,
    },
    Incr {
        member: Vec<u8>,
        delta: f64,
        conditions: Conditions,
    },
    Remove {
        member: Vec<u8>,
    },
    PopMin {
        count: u64,
    },
    PopMax {
        count: u64,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Add { added: bool, changed: bool },
    Incr(Option<f64>),
    Removed(bool),
    Popped(Vec<(Vec<u8>, f64)>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScoreBound {
    NegInf,
    PosInf,
    Incl(f64),
    Excl(f64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LexBound {
    Min,
    Max,
    Incl(Vec<u8>),
    Excl(Vec<u8>),
}

pub type Entry = (Vec<u8>, f64);

pub fn score_key(score: f64, member: &[u8]) -> Result<Vec<u8>> {
    let mut key = score_prefix(score)?.to_vec();
    key.extend_from_slice(member);
    Ok(key)
}

fn score_value(score: f64) -> Vec<u8> {
    score.to_bits().to_le_bytes().to_vec()
}

fn decode_score_value(value: &[u8]) -> Result<f64> {
    let bits: [u8; 8] = value
        .try_into()
        .map_err(|_| "zset tree: member value is not a score".to_string())?;
    Ok(f64::from_bits(u64::from_le_bytes(bits)))
}

/// Score-tree entries carry the score as stored: the key's prefix orders -0
/// with 0, the value keeps its sign.
fn score_entry((key, value): (Vec<u8>, Vec<u8>)) -> Result<Entry> {
    if key.len() < 8 {
        return Err("zset tree: short score key".into());
    }
    Ok((key[8..].to_vec(), decode_score_value(&value)?))
}

fn member_entry((member, value): (Vec<u8>, Vec<u8>)) -> Result<Entry> {
    Ok((member, decode_score_value(&value)?))
}

fn with_zero(mut key: Vec<u8>) -> Vec<u8> {
    key.push(0);
    key
}

impl Tx<'_> {
    pub fn score(&self, member: &[u8]) -> Result<Option<f64>> {
        self.get(Tree::Member, member)?
            .map(|value| decode_score_value(&value))
            .transpose()
    }

    /// Sets `member`'s score; returns whether the member was added. The score
    /// is stored as given; only its sort key folds -0 into 0.
    fn set(&mut self, member: &[u8], score: f64) -> Result<bool> {
        normalize_score(score)?;
        let previous = self.score(member)?;
        if previous == Some(score) {
            return Ok(false);
        }
        if let Some(old) = previous {
            self.remove(Tree::Score, &score_key(old, member)?)?;
        }
        self.insert(Tree::Score, score_key(score, member)?, score_value(score))?;
        self.insert(Tree::Member, member.to_vec(), score_value(score))?;
        self.log(LoggedOp::Set(member.to_vec(), score));
        if previous.is_none() {
            self.count += 1;
        }
        Ok(previous.is_none())
    }

    fn delete(&mut self, member: &[u8]) -> Result<Option<f64>> {
        let Some(score) = self.score(member)? else {
            return Ok(None);
        };
        self.remove(Tree::Score, &score_key(score, member)?)?;
        self.remove(Tree::Member, member)?;
        self.log(LoggedOp::Remove(member.to_vec()));
        self.count -= 1;
        Ok(Some(score))
    }

    /// Replays one logged change (compaction catch-up).
    pub fn replay(&mut self, op: &LoggedOp) -> Result<()> {
        match op {
            LoggedOp::Set(member, score) => self.set(member, *score).map(|_| ()),
            LoggedOp::Remove(member) => self.delete(member).map(|_| ()),
        }
    }

    pub fn apply(&mut self, op: &Op) -> Result<Outcome> {
        match op {
            Op::Add {
                member,
                score,
                conditions,
            } => {
                let score = *score;
                normalize_score(score)?;
                let outcome = match self.score(member)? {
                    None if conditions.xx => Outcome::Add {
                        added: false,
                        changed: false,
                    },
                    None => {
                        self.set(member, score)?;
                        Outcome::Add {
                            added: true,
                            changed: true,
                        }
                    }
                    Some(old)
                        if conditions.nx
                            || (conditions.gt && score <= old)
                            || (conditions.lt && score >= old)
                            || score == old =>
                    {
                        Outcome::Add {
                            added: false,
                            changed: false,
                        }
                    }
                    Some(_) => {
                        self.set(member, score)?;
                        Outcome::Add {
                            added: false,
                            changed: true,
                        }
                    }
                };
                Ok(outcome)
            }
            Op::Incr {
                member,
                delta,
                conditions,
            } => {
                let current = self.score(member)?;
                if (current.is_none() && conditions.xx) || (current.is_some() && conditions.nx) {
                    return Ok(Outcome::Incr(None));
                }
                let old = current.unwrap_or(0.0);
                let new = old + delta;
                if new.is_nan() {
                    return Err("resulting score is not a number (NaN)".into());
                }
                if new.is_infinite() && old.is_finite() && delta.is_finite() {
                    return Err("increment would produce NaN or Infinity".into());
                }
                if current.is_some()
                    && ((conditions.gt && new <= old) || (conditions.lt && new >= old))
                {
                    return Ok(Outcome::Incr(None));
                }
                self.set(member, new)?;
                Ok(Outcome::Incr(Some(new)))
            }
            Op::Remove { member } => Ok(Outcome::Removed(self.delete(member)?.is_some())),
            Op::PopMin { count } => {
                let take = (*count).min(self.count);
                let popped = self.entries(Tree::Score, 0, take, false)?;
                for (member, _score) in &popped {
                    self.delete(member)?;
                }
                Ok(Outcome::Popped(popped))
            }
            Op::PopMax { count } => {
                let take = (*count).min(self.count);
                let popped = self.entries(Tree::Score, self.count - take, self.count, true)?;
                for (member, _score) in &popped {
                    self.delete(member)?;
                }
                Ok(Outcome::Popped(popped))
            }
        }
    }

    /// Members at positions `[start, end)` of a tree, reversed when `rev`.
    fn entries(&self, tree: Tree, start: u64, end: u64, rev: bool) -> Result<Vec<Entry>> {
        let raw = self.range(tree, start, end)?;
        let mut out = raw
            .into_iter()
            .map(match tree {
                Tree::Score => score_entry,
                Tree::Member => member_entry,
            })
            .collect::<Result<Vec<_>>>()?;
        if rev {
            out.reverse();
        }
        Ok(out)
    }

    /// The member's position, counted from the highest score when `rev`.
    pub fn member_rank(&self, member: &[u8], rev: bool) -> Result<Option<u64>> {
        let Some(score) = self.score(member)? else {
            return Ok(None);
        };
        let rank = self.rank(Tree::Score, &score_key(score, member)?)?;
        Ok(Some(if rev { self.count - 1 - rank } else { rank }))
    }

    /// ZRANGE by index, with Redis's negative indices.
    pub fn range_by_index(&self, start: i64, stop: i64, rev: bool) -> Result<Vec<Entry>> {
        let n = self.count as i64;
        let start = if start < 0 { start + n } else { start }.max(0);
        let stop = if stop < 0 { stop + n } else { stop }.min(n - 1);
        if start > stop || start >= n {
            return Ok(Vec::new());
        }
        let (start, stop) = (start as u64, stop as u64);
        if rev {
            let n = n as u64;
            self.entries(Tree::Score, n - 1 - stop, n - start, true)
        } else {
            self.entries(Tree::Score, start, stop + 1, false)
        }
    }

    fn score_lower(&self, bound: &ScoreBound) -> Result<u64> {
        match bound {
            ScoreBound::NegInf => Ok(0),
            ScoreBound::PosInf => Ok(self.count),
            ScoreBound::Incl(score) => self.rank(Tree::Score, &score_prefix(*score)?),
            ScoreBound::Excl(score) => match next_score_prefix(score_prefix(*score)?) {
                Some(next) => self.rank(Tree::Score, &next),
                None => Ok(self.count),
            },
        }
    }

    fn score_upper(&self, bound: &ScoreBound) -> Result<u64> {
        match bound {
            ScoreBound::NegInf => Ok(0),
            ScoreBound::PosInf => Ok(self.count),
            ScoreBound::Incl(score) => match next_score_prefix(score_prefix(*score)?) {
                Some(next) => self.rank(Tree::Score, &next),
                None => Ok(self.count),
            },
            ScoreBound::Excl(score) => self.rank(Tree::Score, &score_prefix(*score)?),
        }
    }

    fn lex_lower(&self, bound: &LexBound) -> Result<u64> {
        match bound {
            LexBound::Min => Ok(0),
            LexBound::Max => Ok(self.count),
            LexBound::Incl(member) => self.rank(Tree::Member, member),
            LexBound::Excl(member) => self.rank(Tree::Member, &with_zero(member.clone())),
        }
    }

    fn lex_upper(&self, bound: &LexBound) -> Result<u64> {
        match bound {
            LexBound::Min => Ok(0),
            LexBound::Max => Ok(self.count),
            LexBound::Incl(member) => self.rank(Tree::Member, &with_zero(member.clone())),
            LexBound::Excl(member) => self.rank(Tree::Member, member),
        }
    }

    /// Applies an offset and limit to the positions `[low, high)`, from the
    /// top when `rev`.
    fn window(low: u64, high: u64, rev: bool, offset: u64, limit: Option<u64>) -> (u64, u64) {
        if low >= high || offset >= high - low {
            return (0, 0);
        }
        if rev {
            let end = high - offset;
            let start = limit.map_or(low, |limit| end.saturating_sub(limit).max(low));
            (start, end)
        } else {
            let start = low + offset;
            let end = limit.map_or(high, |limit| start.saturating_add(limit).min(high));
            (start, end)
        }
    }

    pub fn range_by_score(
        &self,
        min: &ScoreBound,
        max: &ScoreBound,
        rev: bool,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Vec<Entry>> {
        let (start, end) = Self::window(
            self.score_lower(min)?,
            self.score_upper(max)?,
            rev,
            offset,
            limit,
        );
        self.entries(Tree::Score, start, end, rev)
    }

    /// ZRANGE BYLEX, ordered by member bytes.
    pub fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        rev: bool,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Vec<Entry>> {
        let (start, end) = Self::window(
            self.lex_lower(min)?,
            self.lex_upper(max)?,
            rev,
            offset,
            limit,
        );
        self.entries(Tree::Member, start, end, rev)
    }

    pub fn count_by_score(&self, min: &ScoreBound, max: &ScoreBound) -> Result<u64> {
        Ok(self
            .score_upper(max)?
            .saturating_sub(self.score_lower(min)?))
    }

    pub fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> Result<u64> {
        Ok(self.lex_upper(max)?.saturating_sub(self.lex_lower(min)?))
    }

    /// Members at the given positions (ZRANDMEMBER draws the positions).
    pub fn at_positions(&self, positions: &[u64]) -> Result<Vec<Entry>> {
        positions
            .iter()
            .map(|&position| {
                self.entries(Tree::Score, position, position + 1, false)?
                    .pop()
                    .ok_or_else(|| format!("zset tree: no member at position {position}"))
            })
            .collect()
    }

    /// Up to `count` members after `after` in member order, and the cursor
    /// to continue from when more remain.
    pub fn scan(&self, after: Option<&[u8]>, count: u64) -> Result<(Vec<Entry>, Option<Vec<u8>>)> {
        let start = match after {
            Some(member) => self.rank(Tree::Member, &with_zero(member.to_vec()))?,
            None => 0,
        };
        let end = start.saturating_add(count).min(self.count);
        let entries = self.entries(Tree::Member, start, end, false)?;
        let next = (end < self.count)
            .then(|| entries.last().map(|(member, _)| member.clone()))
            .flatten();
        Ok((entries, next))
    }
}
