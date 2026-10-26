//! NIFs over the tree engine. A missing file reads as an empty sorted set.
//!
//! Terms:
//! - ops: `{:add, member, score, flags}`, `{:incr, member, delta, flags}`,
//!   `{:remove, member}`, `{:pop_min, count}`, `{:pop_max, count}`; flags are
//!   bits nx = 1, xx = 2, gt = 4, lt = 8;
//! - score bounds: `:neg_inf | :inf | {:incl, float} | {:excl, float}`;
//! - lex bounds: `:min | :max | {:incl, binary} | {:excl, binary}`;
//! - entries come back as `{member, score}`.

use std::path::Path;

use rustler::types::tuple::get_tuple;
use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, Term};

use super::{Applied, Compacted, Conditions, Entry, LexBound, Op, Outcome, ScoreBound, Token};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        add,
        incr,
        remove,
        pop_min,
        pop_max,
        neg_inf,
        inf,
        incl,
        excl,
        min,
        max,
        missing,
        uncovered,
    }
}

fn binary<'a>(env: Env<'a>, bytes: &[u8]) -> NifResult<Term<'a>> {
    let mut owned = OwnedBinary::new(bytes.len())
        .ok_or_else(|| rustler::Error::Term(Box::new("failed to allocate binary")))?;
    owned.as_mut_slice().copy_from_slice(bytes);
    Ok(owned.release(env).encode(env))
}

fn score_term<'a>(env: Env<'a>, score: f64) -> Term<'a> {
    if score == f64::INFINITY {
        atoms::inf().encode(env)
    } else if score == f64::NEG_INFINITY {
        atoms::neg_inf().encode(env)
    } else {
        score.encode(env)
    }
}

fn entries_term<'a>(env: Env<'a>, entries: &[Entry]) -> NifResult<Term<'a>> {
    let mut out = Vec::with_capacity(entries.len());
    for (member, score) in entries {
        out.push((binary(env, member)?, score_term(env, *score)).encode(env));
    }
    Ok(out.encode(env))
}

fn error<'a>(env: Env<'a>, reason: &str) -> NifResult<Term<'a>> {
    Ok((atoms::error(), reason).encode(env))
}

fn bad_arg<T>() -> NifResult<T> {
    Err(rustler::Error::BadArg)
}

fn is_atom(term: Term<'_>, atom: rustler::Atom) -> bool {
    term.decode::<rustler::Atom>()
        .is_ok_and(|decoded| decoded == atom)
}

fn conditions(flags: u32) -> NifResult<Conditions> {
    if flags > 15 {
        return bad_arg();
    }
    Ok(Conditions {
        nx: flags & 1 != 0,
        xx: flags & 2 != 0,
        gt: flags & 4 != 0,
        lt: flags & 8 != 0,
    })
}

fn decode_op(term: Term<'_>) -> NifResult<Op> {
    let parts = get_tuple(term)?;
    let tag = parts.first().copied().ok_or(rustler::Error::BadArg)?;
    let member = |i: usize| -> NifResult<Vec<u8>> {
        Ok(parts
            .get(i)
            .ok_or(rustler::Error::BadArg)?
            .decode::<Binary>()?
            .as_slice()
            .to_vec())
    };
    match parts.len() {
        4 if is_atom(tag, atoms::add()) => Ok(Op::Add {
            member: member(1)?,
            score: parts[2].decode()?,
            conditions: conditions(parts[3].decode()?)?,
        }),
        4 if is_atom(tag, atoms::incr()) => Ok(Op::Incr {
            member: member(1)?,
            delta: parts[2].decode()?,
            conditions: conditions(parts[3].decode()?)?,
        }),
        2 if is_atom(tag, atoms::remove()) => Ok(Op::Remove { member: member(1)? }),
        2 if is_atom(tag, atoms::pop_min()) => Ok(Op::PopMin {
            count: parts[1].decode()?,
        }),
        2 if is_atom(tag, atoms::pop_max()) => Ok(Op::PopMax {
            count: parts[1].decode()?,
        }),
        _ => bad_arg(),
    }
}

fn outcome_term<'a>(env: Env<'a>, outcome: &Outcome) -> NifResult<Term<'a>> {
    Ok(match outcome {
        Outcome::Add { added, changed } => (*added, *changed).encode(env),
        Outcome::Incr(None) => atoms::nil().encode(env),
        Outcome::Incr(Some(score)) => score_term(env, *score),
        Outcome::Removed(removed) => removed.encode(env),
        Outcome::Popped(entries) => entries_term(env, entries)?,
    })
}

fn decode_score_bound(term: Term<'_>) -> NifResult<ScoreBound> {
    if is_atom(term, atoms::neg_inf()) {
        return Ok(ScoreBound::NegInf);
    }
    if is_atom(term, atoms::inf()) {
        return Ok(ScoreBound::PosInf);
    }
    let parts = get_tuple(term)?;
    match parts.as_slice() {
        [tag, score] if is_atom(*tag, atoms::incl()) => Ok(ScoreBound::Incl(score.decode()?)),
        [tag, score] if is_atom(*tag, atoms::excl()) => Ok(ScoreBound::Excl(score.decode()?)),
        _ => bad_arg(),
    }
}

fn decode_lex_bound(term: Term<'_>) -> NifResult<LexBound> {
    if is_atom(term, atoms::min()) {
        return Ok(LexBound::Min);
    }
    if is_atom(term, atoms::max()) {
        return Ok(LexBound::Max);
    }
    let parts = get_tuple(term)?;
    let bytes = |term: &Term<'_>| -> NifResult<Vec<u8>> {
        Ok(term.decode::<Binary>()?.as_slice().to_vec())
    };
    match parts.as_slice() {
        [tag, member] if is_atom(*tag, atoms::incl()) => Ok(LexBound::Incl(bytes(member)?)),
        [tag, member] if is_atom(*tag, atoms::excl()) => Ok(LexBound::Excl(bytes(member)?)),
        _ => bad_arg(),
    }
}

fn decode_limit(term: Term<'_>) -> NifResult<Option<u64>> {
    if is_atom(term, atoms::nil()) {
        Ok(None)
    } else {
        Ok(Some(term.decode()?))
    }
}

/// Runs a read; a missing file reads as an empty set.
fn read_term<'a, R>(
    env: Env<'a>,
    path: &str,
    read: impl FnOnce(&super::Tx<'_>) -> super::Result<R>,
    empty: impl FnOnce() -> R,
    encode: impl FnOnce(Env<'a>, R) -> NifResult<Term<'a>>,
) -> NifResult<Term<'a>> {
    match super::read(Path::new(path), read) {
        Ok(Some(value)) => Ok((atoms::ok(), encode(env, value)?).encode(env)),
        Ok(None) => Ok((atoms::ok(), encode(env, empty())?).encode(env)),
        Err(reason) => error(env, &reason),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_apply<'a>(
    env: Env<'a>,
    path: String,
    token_index: u64,
    token_ordinal: u64,
    ops: Vec<Term<'a>>,
) -> NifResult<Term<'a>> {
    let ops = ops
        .into_iter()
        .map(decode_op)
        .collect::<NifResult<Vec<_>>>()?;
    match super::apply(
        Path::new(&path),
        Token::new(token_index, token_ordinal),
        &ops,
    ) {
        Ok(Applied { outcomes, .. }) => {
            let terms = outcomes
                .iter()
                .map(|outcome| outcome_term(env, outcome))
                .collect::<NifResult<Vec<_>>>()?;
            Ok((atoms::ok(), terms).encode(env))
        }
        Err(reason) => error(env, &reason),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_card(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    read_term(
        env,
        &path,
        |tx| Ok(tx.count),
        || 0,
        |env, n| Ok(n.encode(env)),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_scores<'a>(
    env: Env<'a>,
    path: String,
    members: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let count = members.len();
    read_term(
        env,
        &path,
        |tx| {
            members
                .iter()
                .map(|member| tx.score(member.as_slice()))
                .collect()
        },
        || vec![None; count],
        |env, scores: Vec<Option<f64>>| {
            Ok(scores
                .into_iter()
                .map(|score| score.map_or_else(|| atoms::nil().encode(env), |s| score_term(env, s)))
                .collect::<Vec<_>>()
                .encode(env))
        },
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_rank<'a>(
    env: Env<'a>,
    path: String,
    member: Binary<'a>,
    rev: bool,
) -> NifResult<Term<'a>> {
    read_term(
        env,
        &path,
        |tx| tx.member_rank(member.as_slice(), rev),
        || None,
        |env, rank| Ok(rank.map_or_else(|| atoms::nil().encode(env), |r| r.encode(env))),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_range_by_index(
    env: Env<'_>,
    path: String,
    start: i64,
    stop: i64,
    rev: bool,
) -> NifResult<Term<'_>> {
    read_term(
        env,
        &path,
        |tx| tx.range_by_index(start, stop, rev),
        Vec::new,
        |env, e| entries_term(env, &e),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_range_by_score<'a>(
    env: Env<'a>,
    path: String,
    min: Term<'a>,
    max: Term<'a>,
    rev: bool,
    offset: u64,
    limit: Term<'a>,
) -> NifResult<Term<'a>> {
    let (min, max, limit) = (
        decode_score_bound(min)?,
        decode_score_bound(max)?,
        decode_limit(limit)?,
    );
    read_term(
        env,
        &path,
        |tx| tx.range_by_score(&min, &max, rev, offset, limit),
        Vec::new,
        |env, e| entries_term(env, &e),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_range_by_lex<'a>(
    env: Env<'a>,
    path: String,
    min: Term<'a>,
    max: Term<'a>,
    rev: bool,
    offset: u64,
    limit: Term<'a>,
) -> NifResult<Term<'a>> {
    let (min, max, limit) = (
        decode_lex_bound(min)?,
        decode_lex_bound(max)?,
        decode_limit(limit)?,
    );
    read_term(
        env,
        &path,
        |tx| tx.range_by_lex(&min, &max, rev, offset, limit),
        Vec::new,
        |env, e| entries_term(env, &e),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_count_by_score<'a>(
    env: Env<'a>,
    path: String,
    min: Term<'a>,
    max: Term<'a>,
) -> NifResult<Term<'a>> {
    let (min, max) = (decode_score_bound(min)?, decode_score_bound(max)?);
    read_term(
        env,
        &path,
        |tx| tx.count_by_score(&min, &max),
        || 0,
        |env, n| Ok(n.encode(env)),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_count_by_lex<'a>(
    env: Env<'a>,
    path: String,
    min: Term<'a>,
    max: Term<'a>,
) -> NifResult<Term<'a>> {
    let (min, max) = (decode_lex_bound(min)?, decode_lex_bound(max)?);
    read_term(
        env,
        &path,
        |tx| tx.count_by_lex(&min, &max),
        || 0,
        |env, n| Ok(n.encode(env)),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_at_positions(
    env: Env<'_>,
    path: String,
    positions: Vec<u64>,
) -> NifResult<Term<'_>> {
    read_term(
        env,
        &path,
        |tx| tx.at_positions(&positions),
        Vec::new,
        |env, e| entries_term(env, &e),
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_scan<'a>(
    env: Env<'a>,
    path: String,
    after: Term<'a>,
    count: u64,
) -> NifResult<Term<'a>> {
    let after = if is_atom(after, atoms::nil()) {
        None
    } else {
        Some(after.decode::<Binary>()?.as_slice().to_vec())
    };
    read_term(
        env,
        &path,
        |tx| tx.scan(after.as_deref(), count),
        || (Vec::new(), None),
        |env, (entries, next)| {
            let next = match next {
                Some(member) => binary(env, &member)?,
                None => atoms::nil().encode(env),
            };
            Ok((entries_term(env, &entries)?, next).encode(env))
        },
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_recover(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match super::recover(Path::new(&path)) {
        Ok(None) => Ok((atoms::ok(), atoms::missing()).encode(env)),
        Ok(Some(recovery)) => Ok((
            atoms::ok(),
            (
                recovery.frames_checked,
                recovery.truncated_bytes,
                recovery.state.count,
            ),
        )
            .encode(env)),
        Err(reason) => error(env, &reason),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_sync(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match super::sync(Path::new(&path)) {
        Ok(_) => Ok(atoms::ok().encode(env)),
        Err(reason) => error(env, &reason),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn zset_tree_compact(env: Env<'_>, path: String, covered_index: u64) -> NifResult<Term<'_>> {
    match super::compact(Path::new(&path), covered_index) {
        Ok(Compacted::Missing) => Ok((atoms::ok(), atoms::missing()).encode(env)),
        Ok(Compacted::Uncovered) => Ok((atoms::ok(), atoms::uncovered()).encode(env)),
        Ok(Compacted::Done(compaction)) => Ok((
            atoms::ok(),
            (compaction.bytes_before, compaction.bytes_after),
        )
            .encode(env)),
        Err(reason) => error(env, &reason),
    }
}
