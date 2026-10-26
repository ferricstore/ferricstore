use std::collections::BTreeMap;
use std::path::Path;

use super::format::{score_prefix, HeaderSlot, SLOT_SIZE};
use super::store::TreeFile;
use super::tree::{Tree, Tx};
use super::*;

/// xorshift64*: deterministic, no dependencies.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// The reference: Redis semantics on a map, written independently of the tree.
#[derive(Clone, Default)]
struct Model {
    scores: BTreeMap<Vec<u8>, f64>,
}

impl Model {
    fn by_score(&self) -> Vec<(Vec<u8>, f64)> {
        let mut entries: Vec<_> = self.scores.iter().map(|(m, s)| (m.clone(), *s)).collect();
        entries.sort_by(|a, b| {
            (score_prefix(a.1).unwrap(), &a.0).cmp(&(score_prefix(b.1).unwrap(), &b.0))
        });
        entries
    }

    fn by_member(&self) -> Vec<(Vec<u8>, f64)> {
        self.scores.iter().map(|(m, s)| (m.clone(), *s)).collect()
    }

    fn apply(&mut self, op: &Op) -> Result<Outcome> {
        let norm = |s: f64| if s == 0.0 { 0.0 } else { s };
        Ok(match op {
            Op::Add {
                member,
                score,
                conditions: c,
            } => {
                let score = norm(*score);
                match self.scores.get(member).copied() {
                    None if c.xx => Outcome::Add {
                        added: false,
                        changed: false,
                    },
                    None => {
                        self.scores.insert(member.clone(), score);
                        Outcome::Add {
                            added: true,
                            changed: true,
                        }
                    }
                    Some(old) => {
                        let blocked = c.nx || (c.gt && score <= old) || (c.lt && score >= old);
                        let allowed = !blocked;
                        if allowed && score != old {
                            self.scores.insert(member.clone(), score);
                            Outcome::Add {
                                added: false,
                                changed: true,
                            }
                        } else {
                            Outcome::Add {
                                added: false,
                                changed: false,
                            }
                        }
                    }
                }
            }
            Op::Incr {
                member,
                delta,
                conditions: c,
            } => {
                let current = self.scores.get(member).copied();
                if (current.is_none() && c.xx) || (current.is_some() && c.nx) {
                    return Ok(Outcome::Incr(None));
                }
                let new = current.unwrap_or(0.0) + delta;
                if new.is_nan()
                    || (new.is_infinite()
                        && current.unwrap_or(0.0).is_finite()
                        && delta.is_finite())
                {
                    return Err("nan".into());
                }
                if let Some(old) = current {
                    if (c.gt && new <= old) || (c.lt && new >= old) {
                        return Ok(Outcome::Incr(None));
                    }
                }
                self.scores.insert(member.clone(), norm(new));
                Outcome::Incr(Some(norm(new)))
            }
            Op::Remove { member } => Outcome::Removed(self.scores.remove(member).is_some()),
            Op::PopMin { count } => {
                let popped: Vec<_> = self.by_score().into_iter().take(*count as usize).collect();
                for (member, _) in &popped {
                    self.scores.remove(member);
                }
                Outcome::Popped(popped)
            }
            Op::PopMax { count } => {
                let popped: Vec<_> = self
                    .by_score()
                    .into_iter()
                    .rev()
                    .take(*count as usize)
                    .collect();
                for (member, _) in &popped {
                    self.scores.remove(member);
                }
                Outcome::Popped(popped)
            }
        })
    }
}

const SCORES: [f64; 9] = [
    f64::NEG_INFINITY,
    -2.5,
    -0.0,
    0.0,
    1.0,
    1.5,
    3.0,
    1e300,
    f64::INFINITY,
];

fn member(rng: &mut Rng) -> Vec<u8> {
    match rng.below(100) {
        0 => Vec::new(),
        1..=3 => (0..(600 + rng.below(9000)))
            .map(|i| b'a' + (i % 26) as u8)
            .collect(),
        _ => format!("m{:03}", rng.below(400)).into_bytes(),
    }
}

fn score(rng: &mut Rng) -> f64 {
    if rng.chance(50) {
        SCORES[rng.below(SCORES.len() as u64) as usize]
    } else {
        (rng.below(2000) as f64 - 1000.0) / 8.0
    }
}

fn conditions(rng: &mut Rng) -> Conditions {
    match rng.below(8) {
        0 => Conditions {
            nx: true,
            ..Conditions::default()
        },
        1 => Conditions {
            xx: true,
            ..Conditions::default()
        },
        2 => Conditions {
            gt: true,
            ..Conditions::default()
        },
        3 => Conditions {
            lt: true,
            ..Conditions::default()
        },
        4 => Conditions {
            xx: true,
            gt: true,
            ..Conditions::default()
        },
        _ => Conditions::default(),
    }
}

fn random_op(rng: &mut Rng) -> Op {
    match rng.below(20) {
        0..=10 => Op::Add {
            member: member(rng),
            score: score(rng),
            conditions: conditions(rng),
        },
        11..=12 => Op::Incr {
            member: member(rng),
            delta: (rng.below(40) as f64 - 20.0) / 4.0,
            conditions: conditions(rng),
        },
        13..=17 => Op::Remove {
            member: member(rng),
        },
        18 => Op::PopMin {
            count: rng.below(4),
        },
        _ => Op::PopMax {
            count: rng.below(4),
        },
    }
}

fn score_bound(rng: &mut Rng) -> ScoreBound {
    match rng.below(6) {
        0 => ScoreBound::NegInf,
        1 => ScoreBound::PosInf,
        2 | 3 => ScoreBound::Incl(score(rng)),
        _ => ScoreBound::Excl(score(rng)),
    }
}

fn lex_bound(rng: &mut Rng) -> LexBound {
    match rng.below(6) {
        0 => LexBound::Min,
        1 => LexBound::Max,
        2 | 3 => LexBound::Incl(member(rng)),
        _ => LexBound::Excl(member(rng)),
    }
}

fn in_score(bound_low: &ScoreBound, bound_high: &ScoreBound, s: f64) -> bool {
    let low = match bound_low {
        ScoreBound::NegInf => true,
        ScoreBound::PosInf => false,
        ScoreBound::Incl(b) => s >= *b,
        ScoreBound::Excl(b) => s > *b,
    };
    let high = match bound_high {
        ScoreBound::NegInf => false,
        ScoreBound::PosInf => true,
        ScoreBound::Incl(b) => s <= *b,
        ScoreBound::Excl(b) => s < *b,
    };
    low && high
}

fn in_lex(low: &LexBound, high: &LexBound, m: &[u8]) -> bool {
    let low = match low {
        LexBound::Min => true,
        LexBound::Max => false,
        LexBound::Incl(b) => m >= b.as_slice(),
        LexBound::Excl(b) => m > b.as_slice(),
    };
    let high = match high {
        LexBound::Min => false,
        LexBound::Max => true,
        LexBound::Incl(b) => m <= b.as_slice(),
        LexBound::Excl(b) => m < b.as_slice(),
    };
    low && high
}

fn window(
    mut items: Vec<(Vec<u8>, f64)>,
    rev: bool,
    offset: u64,
    limit: Option<u64>,
) -> Vec<(Vec<u8>, f64)> {
    if rev {
        items.reverse();
    }
    let take = limit.map_or(usize::MAX, |l| l as usize);
    items.into_iter().skip(offset as usize).take(take).collect()
}

fn redis_index_range(n: i64, start: i64, stop: i64) -> Option<(usize, usize)> {
    let start = if start < 0 { start + n } else { start }.max(0);
    let stop = if stop < 0 { stop + n } else { stop }.min(n - 1);
    (start <= stop && start < n).then_some((start as usize, stop as usize))
}

/// Compares every read path of the file with the model.
fn check_reads(path: &Path, model: &Model, rng: &mut Rng) {
    let by_score = model.by_score();
    let by_member = model.by_member();
    let n = by_score.len() as i64;
    read(path, |tx| {
        assert_eq!(tx.count, n as u64, "count");
        assert_eq!(tx.check(Tree::Score)?, n as u64, "score tree");
        assert_eq!(tx.check(Tree::Member)?, n as u64, "member tree");

        for _ in 0..6 {
            let m = member(rng);
            let expected = by_score.iter().position(|(x, _)| *x == m).map(|i| i as u64);
            assert_eq!(tx.member_rank(&m, false)?, expected, "rank");
            assert_eq!(
                tx.member_rank(&m, true)?,
                expected.map(|r| n as u64 - 1 - r),
                "revrank"
            );
            assert_eq!(tx.score(&m)?, model.scores.get(&m).copied(), "score");
        }

        for _ in 0..4 {
            let start = rng.below(2 * n as u64 + 3) as i64 - n - 1;
            let stop = rng.below(2 * n as u64 + 3) as i64 - n - 1;
            let rev = rng.chance(50);
            let expected = match redis_index_range(n, start, stop) {
                None => Vec::new(),
                Some((a, b)) if rev => by_score
                    .iter()
                    .rev()
                    .skip(a)
                    .take(b - a + 1)
                    .cloned()
                    .collect(),
                Some((a, b)) => by_score[a..=b].to_vec(),
            };
            assert_eq!(
                tx.range_by_index(start, stop, rev)?,
                expected,
                "range by index"
            );
        }

        for _ in 0..4 {
            let (low, high) = (score_bound(rng), score_bound(rng));
            let rev = rng.chance(50);
            let offset = rng.below(5);
            let limit = rng.chance(50).then(|| rng.below(6));
            let matching: Vec<_> = by_score
                .iter()
                .filter(|(_, s)| in_score(&low, &high, *s))
                .cloned()
                .collect();
            assert_eq!(
                tx.count_by_score(&low, &high)?,
                matching.len() as u64,
                "zcount"
            );
            assert_eq!(
                tx.range_by_score(&low, &high, rev, offset, limit)?,
                window(matching, rev, offset, limit),
                "range by score"
            );
        }

        for _ in 0..4 {
            let (low, high) = (lex_bound(rng), lex_bound(rng));
            let rev = rng.chance(50);
            let offset = rng.below(5);
            let limit = rng.chance(50).then(|| rng.below(6));
            let matching: Vec<_> = by_member
                .iter()
                .filter(|(m, _)| in_lex(&low, &high, m))
                .cloned()
                .collect();
            assert_eq!(
                tx.count_by_lex(&low, &high)?,
                matching.len() as u64,
                "zlexcount"
            );
            assert_eq!(
                tx.range_by_lex(&low, &high, rev, offset, limit)?,
                window(matching, rev, offset, limit),
                "range by lex"
            );
        }

        let mut scanned = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let (page, next) = tx.scan(cursor.as_deref(), 1 + rng.below(40))?;
            scanned.extend(page);
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(scanned, by_member, "scan");

        if n > 0 {
            let positions: Vec<u64> = (0..3).map(|_| rng.below(n as u64)).collect();
            let expected: Vec<_> = positions
                .iter()
                .map(|&p| by_score[p as usize].clone())
                .collect();
            assert_eq!(tx.at_positions(&positions)?, expected, "at positions");
        }
        Ok(())
    })
    .unwrap()
    .unwrap_or_else(|| {
        assert!(
            model.scores.is_empty(),
            "missing file for a non-empty model"
        );
    });
}

fn temp_tree() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("tree");
    (dir, path)
}

#[test]
fn matches_the_model_through_writes_compactions_and_syncs() {
    for seed in 1..=6_u64 {
        let (_dir, path) = temp_tree();
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ seed);
        let mut model = Model::default();
        let mut index = 0;
        for round in 0..700 {
            let ops: Vec<Op> = (0..=rng.below(8)).map(|_| random_op(&mut rng)).collect();
            let mut trial = model.clone();
            let expected: Result<Vec<Outcome>> = ops.iter().map(|op| trial.apply(op)).collect();
            index += 1;
            match (apply(&path, Token::new(index, 1), &ops), expected) {
                (Ok(Applied::Done { outcomes, state }), Ok(expected)) => {
                    assert_eq!(outcomes, expected, "seed {seed} round {round}");
                    assert_eq!(state.count, trial.scores.len() as u64);
                    model = trial;
                }
                (Err(_), Err(_)) => {}
                (got, want) => panic!("seed {seed} round {round}: {got:?} vs {want:?}"),
            }
            if round % 25 == 0 {
                check_reads(&path, &model, &mut rng);
            }
            if round % 97 == 96 {
                sync(&path).unwrap();
            }
            if round % 211 == 210 {
                compact(&path).unwrap();
                check_reads(&path, &model, &mut rng);
            }
        }
        check_reads(&path, &model, &mut rng);
    }
}

#[test]
fn a_replayed_token_changes_nothing() {
    let (_dir, path) = temp_tree();
    let add = |member: &str, score| Op::Add {
        member: member.as_bytes().to_vec(),
        score,
        conditions: Conditions::default(),
    };
    assert!(matches!(
        apply(&path, Token::new(5, 1), &[add("a", 1.0)]).unwrap(),
        Applied::Done { .. }
    ));
    assert_eq!(
        apply(&path, Token::new(5, 1), &[add("b", 2.0)]).unwrap(),
        Applied::Stale
    );
    assert_eq!(
        apply(&path, Token::new(4, 9), &[add("c", 3.0)]).unwrap(),
        Applied::Stale
    );
    assert!(matches!(
        apply(&path, Token::new(5, 2), &[add("d", 4.0)]).unwrap(),
        Applied::Done { .. }
    ));
    let members = read(&path, |tx| tx.range_by_index(0, -1, false))
        .unwrap()
        .unwrap();
    assert_eq!(members, vec![(b"a".to_vec(), 1.0), (b"d".to_vec(), 4.0)]);
}

#[test]
fn a_failed_operation_leaves_the_whole_batch_unapplied() {
    let (_dir, path) = temp_tree();
    let ops = vec![
        Op::Add {
            member: b"x".to_vec(),
            score: f64::INFINITY,
            conditions: Conditions::default(),
        },
        Op::Incr {
            member: b"x".to_vec(),
            delta: f64::NEG_INFINITY,
            conditions: Conditions::default(),
        },
    ];
    assert!(apply(&path, Token::new(1, 1), &ops).is_err());
    assert_eq!(read(&path, |tx| Ok(tx.count)).unwrap(), Some(0));
}

#[test]
fn positions_cost_a_descent_not_a_walk() {
    let (_dir, path) = temp_tree();
    let total = 60_000_u64;
    let mut index = 0;
    for chunk in (0..total).collect::<Vec<_>>().chunks(2_000) {
        index += 1;
        let ops: Vec<Op> = chunk
            .iter()
            .map(|i| Op::Add {
                member: format!("event-{i:08}").into_bytes(),
                score: 0.0,
                conditions: Conditions::default(),
            })
            .collect();
        apply(&path, Token::new(index, 1), &ops).unwrap();
    }
    compact(&path).unwrap();
    let file = TreeFile::open_read(&path).unwrap().unwrap();
    let tx = Tx::begin(&file, file.state().unwrap());
    // Height bounds the pages a position lookup reads.
    let height = |root: u64| {
        let mut depth = 1;
        let mut offset = root;
        loop {
            let page = file.read_page(offset).unwrap();
            match super::node::decode_node(&page, &mut |h, l| file.read_overflow(h, l)).unwrap() {
                super::node::Node::Leaf(_) => return depth,
                super::node::Node::Branch(b) => {
                    let super::node::Child::Disk(next) = b.children[0] else {
                        unreachable!()
                    };
                    offset = next;
                    depth += 1;
                }
            }
        }
    };
    let state = file.state().unwrap();
    assert!(height(state.score_root) <= 3, "score tree height");
    let last = format!("event-{:08}", total - 1).into_bytes();
    assert_eq!(tx.member_rank(&last, false).unwrap(), Some(total - 1));
    let tail = tx.range_by_index(-100, -1, false).unwrap();
    assert_eq!(tail.len(), 100);
    assert_eq!(tail[99].0, last);
}

/// Frame ends and the model after each frame.
fn build_history(path: &Path, rng: &mut Rng, rounds: u64) -> Vec<(u64, Model)> {
    let mut model = Model::default();
    let mut history = vec![(super::format::HEADER_REGION, Model::default())];
    for round in 0..rounds {
        let ops: Vec<Op> = (0..=rng.below(6)).map(|_| random_op(rng)).collect();
        let mut trial = model.clone();
        if ops
            .iter()
            .map(|op| trial.apply(op))
            .collect::<Result<Vec<_>>>()
            .is_err()
        {
            continue;
        }
        if let Applied::Done { state, .. } = apply(path, Token::new(round + 1, 1), &ops).unwrap() {
            model = trial;
            if history.last().unwrap().0 != state.frame_end {
                history.push((state.frame_end, model.clone()));
            }
        }
    }
    history
}

fn model_at(history: &[(u64, Model)], end: u64) -> Model {
    history
        .iter()
        .rev()
        .find(|(frame_end, _)| *frame_end <= end)
        .unwrap()
        .1
        .clone()
}

#[test]
fn recovery_keeps_exactly_the_whole_frames() {
    let (dir, path) = temp_tree();
    let mut rng = Rng(42);
    let mut history = build_history(&path, &mut rng, 60);
    sync(&path).unwrap();
    let synced_end = history.last().unwrap().0;
    history.extend(build_history_continue(&path, &mut rng, &history, 120));
    let pristine = std::fs::read(&path).unwrap();
    let copy = dir.path().join("copy");

    for trial in 0..60 {
        let mut bytes = pristine.clone();
        let damage = synced_end + rng.below(bytes.len() as u64 - synced_end);
        if trial % 2 == 0 {
            // Lost tail: everything from `damage` on never reached disk.
            bytes.truncate(damage as usize);
        } else {
            // A page that reached disk with the wrong bytes.
            bytes[damage as usize] ^= 0x5a;
        }
        std::fs::write(&copy, &bytes).unwrap();
        let recovery = recover(&copy).unwrap().unwrap();
        let expected = model_at(&history, damage);
        check_reads(&copy, &expected, &mut rng);
        assert!(recovery.state.frame_end <= damage.max(synced_end));

        // The recovered file takes new writes.
        let op = Op::Add {
            member: b"after-recovery".to_vec(),
            score: 7.0,
            conditions: Conditions::default(),
        };
        let mut model = expected;
        model.apply(&op).unwrap();
        apply(&copy, Token::new(1_000_000, 1), &[op]).unwrap();
        check_reads(&copy, &model, &mut rng);
    }
}

fn build_history_continue(
    path: &Path,
    rng: &mut Rng,
    history: &[(u64, Model)],
    rounds: u64,
) -> Vec<(u64, Model)> {
    let mut model = history.last().unwrap().1.clone();
    let mut out = Vec::new();
    let mut last_end = history.last().unwrap().0;
    for round in 0..rounds {
        let ops: Vec<Op> = (0..=rng.below(6)).map(|_| random_op(rng)).collect();
        let mut trial = model.clone();
        if ops
            .iter()
            .map(|op| trial.apply(op))
            .collect::<Result<Vec<_>>>()
            .is_err()
        {
            continue;
        }
        if let Applied::Done { state, .. } =
            apply(path, Token::new(10_000 + round, 1), &ops).unwrap()
        {
            model = trial;
            if state.frame_end != last_end {
                last_end = state.frame_end;
                out.push((state.frame_end, model.clone()));
            }
        }
    }
    out
}

#[test]
fn a_torn_header_slot_falls_back_to_the_other() {
    let (_dir, path) = temp_tree();
    let mut rng = Rng(7);
    let history = build_history(&path, &mut rng, 30);
    let file = TreeFile::open_read(&path).unwrap().unwrap();
    let slots = file.slots().unwrap();
    let newest = slots
        .iter()
        .flatten()
        .max_by_key(|slot| slot.generation)
        .copied()
        .unwrap();
    let older = slots
        .iter()
        .flatten()
        .min_by_key(|slot| slot.generation)
        .copied()
        .unwrap();
    drop(file);

    let mut bytes = std::fs::read(&path).unwrap();
    let at = newest.offset() as usize;
    bytes[at + 20] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    assert!(HeaderSlot::decode(&bytes[at..at + SLOT_SIZE]).is_none());
    check_reads(&path, &model_at(&history, older.latest_frame_end), &mut rng);
}

#[test]
fn large_trees_rebalance_through_mass_removal() {
    let (_dir, path) = temp_tree();
    let mut rng = Rng(0xfeed);
    let mut model = Model::default();
    let mut members: Vec<Vec<u8>> = (0..40_000_u64)
        .map(|i| {
            let len = if i % 500 == 0 {
                700
            } else {
                6 + (i % 23) as usize
            };
            let mut m = format!("{:016x}", rng.next()).into_bytes();
            m.resize(len, b'z');
            m
        })
        .collect();
    let mut index = 0;
    for chunk in members.chunks(1_500) {
        let ops: Vec<Op> = chunk
            .iter()
            .map(|m| Op::Add {
                member: m.clone(),
                score: (rng.below(64) as f64) / 2.0,
                conditions: Conditions::default(),
            })
            .collect();
        for op in &ops {
            model.apply(op).unwrap();
        }
        index += 1;
        apply(&path, Token::new(index, 1), &ops).unwrap();
    }
    check_reads(&path, &model, &mut rng);

    // Remove in random order until a few hundred remain, checking as it shrinks.
    for i in (1..members.len()).rev() {
        members.swap(i, rng.below(i as u64 + 1) as usize);
    }
    let mut removed = 0;
    for chunk in members[..39_700].chunks(700) {
        let ops: Vec<Op> = chunk
            .iter()
            .map(|m| Op::Remove { member: m.clone() })
            .collect();
        for op in &ops {
            model.apply(op).unwrap();
        }
        index += 1;
        apply(&path, Token::new(index, 1), &ops).unwrap();
        removed += chunk.len();
        if removed % 7_000 < 700 {
            check_reads(&path, &model, &mut rng);
        }
    }
    check_reads(&path, &model, &mut rng);
}

#[test]
fn an_increment_that_overflows_to_infinity_is_refused() {
    let (_dir, path) = temp_tree();
    let add = Op::Add {
        member: b"big".to_vec(),
        score: f64::MAX,
        conditions: Conditions::default(),
    };
    apply(&path, Token::new(1, 1), &[add]).unwrap();
    let incr = Op::Incr {
        member: b"big".to_vec(),
        delta: f64::MAX,
        conditions: Conditions::default(),
    };
    assert!(apply(&path, Token::new(2, 1), &[incr]).is_err());
    assert_eq!(
        read(&path, |tx| tx.score(b"big")).unwrap(),
        Some(Some(f64::MAX))
    );
}
