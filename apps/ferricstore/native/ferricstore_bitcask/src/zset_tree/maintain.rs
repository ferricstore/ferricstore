//! Recovery, sync and compaction of a tree file.

use std::path::{Path, PathBuf};

use super::format::{
    decode_ops, FrameHeader, HeaderSlot, Token, Trailer, FRAME_HEADER_SIZE, HEADER_REGION,
    INLINE_KEY_MAX, PAGE_SIZE, TRAILER_SIZE,
};
use super::node::{decode_node, encode_node, encode_overflow, Branch, Child, Key, Leaf, Node};
use super::store::{TreeFile, TreeState};
use super::tree::Tx;
use super::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Recovery {
    pub frames_checked: u64,
    pub truncated_bytes: u64,
    pub state: TreeState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Compaction {
    pub bytes_before: u64,
    pub bytes_after: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compacted {
    Missing,
    /// Raft could still replay an entry the file holds; the compacted frame
    /// would leave nothing to rewind to.
    Uncovered,
    Done(Compaction),
}

/// Cuts the file back to the last frame before `token`, so a replayed entry
/// applies again from the state it first saw. Fails when that would go below
/// a compacted base.
pub fn rewind(file: &TreeFile, state: TreeState, token: Token) -> Result<TreeState> {
    let mut end = state.frame_end;
    while end > HEADER_REGION {
        let trailer = file
            .trailer_ending_at(end)?
            .ok_or_else(|| "zset tree: rewind met a damaged frame".to_string())?;
        if trailer.token < token {
            break;
        }
        let header = FrameHeader::decode(&file.read_exact(trailer.frame_start, FRAME_HEADER_SIZE)?)
            .ok_or_else(|| "zset tree: rewind met a damaged frame".to_string())?;
        if header.base {
            return Err(format!(
                "zset tree: entry {}.{} is older than the compacted file ({}.{})",
                token.index, token.ordinal, header.token.index, header.token.ordinal
            ));
        }
        end = trailer.frame_start;
    }
    file.set_len(end)?;
    file.write_slot(&HeaderSlot {
        generation: state.generation + 1,
        latest_frame_end: end,
        synced_frame_end: state.synced_frame_end.min(end),
    })?;
    file.state()
}

const READ_CHUNK: usize = 1 << 20;

fn frame_valid(file: &TreeFile, start: u64) -> Result<Option<u64>> {
    if start + FRAME_HEADER_SIZE as u64 > file.len() {
        return Ok(None);
    }
    let Some(header) = FrameHeader::decode(&file.read_exact(start, FRAME_HEADER_SIZE)?) else {
        return Ok(None);
    };
    let end = start.saturating_add(header.frame_len());
    if end > file.len() {
        return Ok(None);
    }
    let trailer_at = end - TRAILER_SIZE as u64;
    let Some(trailer) = Trailer::decode(&file.read_exact(trailer_at, TRAILER_SIZE)?) else {
        return Ok(None);
    };
    if trailer.frame_start != start || trailer.token != header.token {
        return Ok(None);
    }
    let mut hasher = crc32fast::Hasher::new();
    let mut at = start;
    while at < trailer_at {
        let len = ((trailer_at - at) as usize).min(READ_CHUNK);
        hasher.update(&file.read_exact(at, len)?);
        at += len as u64;
    }
    if hasher.finalize() != trailer.body_crc {
        return Ok(None);
    }
    let ops_len = header.ops_len as usize;
    let ops = file.read_exact(trailer_at - ops_len as u64, ops_len)?;
    Ok(decode_ops(&ops).is_ok().then_some(end))
}

/// Run once when a node starts, before the file is used. Frames written since
/// the last sync may have reached disk out of order or in part; each one is
/// checked in turn, the file is cut after the last whole one, and Raft replay
/// restores what came after it. Returns `None` when the file does not exist.
pub fn recover(path: &Path) -> Result<Option<Recovery>> {
    let Some(file) = TreeFile::open_existing(path)? else {
        return Ok(None);
    };
    if file.len() < HEADER_REGION {
        // Interrupted while the file was created: it never held a frame.
        file.set_len(0)?;
        drop(file);
        let file = TreeFile::open_write(path)?;
        return Ok(Some(Recovery {
            frames_checked: 0,
            truncated_bytes: 0,
            state: file.state()?,
        }));
    }
    let slots: Vec<HeaderSlot> = file.slots()?.into_iter().flatten().collect();
    let generation = slots.iter().map(|slot| slot.generation).max().unwrap_or(0);
    let synced = slots
        .iter()
        .max_by_key(|slot| slot.generation)
        .map_or(HEADER_REGION, |slot| slot.synced_frame_end);
    let start = if synced == HEADER_REGION || file.trailer_ending_at(synced)?.is_some() {
        synced
    } else {
        HEADER_REGION
    };

    let mut end = start;
    let mut frames_checked = 0;
    while let Some(frame_end) = frame_valid(&file, end)? {
        end = frame_end;
        frames_checked += 1;
    }
    let truncated_bytes = file.len() - end;
    if truncated_bytes > 0 {
        file.set_len(end)?;
    }
    file.write_slot(&HeaderSlot {
        generation: generation + 1,
        latest_frame_end: end,
        synced_frame_end: start,
    })?;
    Ok(Some(Recovery {
        frames_checked,
        truncated_bytes,
        state: file.state()?,
    }))
}

/// Makes every frame durable and records that in a header slot. Returns false
/// when the file does not exist.
pub fn sync(path: &Path) -> Result<bool> {
    let Some(file) = TreeFile::open_existing(path)? else {
        return Ok(false);
    };
    let state = file.state()?;
    if state.synced_frame_end == state.frame_end {
        return Ok(true);
    }
    file.sync()?;
    file.write_slot(&HeaderSlot {
        generation: state.generation + 1,
        latest_frame_end: state.frame_end,
        synced_frame_end: state.frame_end,
    })?;
    Ok(true)
}

fn compacting_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(".compacting");
    path.with_file_name(name)
}

/// One frame of a file: where it starts and ends, and its trailer.
#[derive(Clone, Copy)]
struct FrameAt {
    start: u64,
    end: u64,
    trailer: Trailer,
}

/// The last frame at or below `floor_index`, and the frames after it, oldest
/// first; `None` when every frame is after the floor.
fn split_at_floor(
    file: &TreeFile,
    latest_end: u64,
    floor_index: u64,
) -> Result<Option<(FrameAt, Vec<FrameAt>)>> {
    let mut tail = Vec::new();
    let mut end = latest_end;
    while end > HEADER_REGION {
        let trailer = file
            .trailer_ending_at(end)?
            .ok_or_else(|| "zset tree: compaction met a damaged frame".to_string())?;
        let frame = FrameAt {
            start: trailer.frame_start,
            end,
            trailer,
        };
        if trailer.token.index <= floor_index {
            tail.reverse();
            return Ok(Some((frame, tail)));
        }
        tail.push(frame);
        end = trailer.frame_start;
    }
    Ok(None)
}

/// The frames after `base`, oldest first; `None` when `base` is not one of
/// the file's frames.
fn frames_after(file: &TreeFile, latest_end: u64, base: &FrameAt) -> Result<Option<Vec<FrameAt>>> {
    let mut tail = Vec::new();
    let mut end = latest_end;
    while end > base.end {
        let Some(trailer) = file.trailer_ending_at(end)? else {
            return Ok(None);
        };
        tail.push(FrameAt {
            start: trailer.frame_start,
            end,
            trailer,
        });
        end = trailer.frame_start;
    }
    if end != base.end || file.trailer_ending_at(end)? != Some(base.trailer) {
        return Ok(None);
    }
    tail.reverse();
    Ok(Some(tail))
}

/// Rewrites the tree as of the last frame at or below `floor_index` into a
/// fresh file, pages about 7/8 full, as one base frame. Then copies each later
/// frame onto it by replaying its logged changes under the frame's own token,
/// and puts the new file in place.
///
/// Raft replays nothing at or below `floor_index`, so no rewind needs a frame
/// the base folded away, and every later frame is still there to rewind to.
/// Runs beside writers: the base is built without the file's lock; the copy of
/// the later frames, and the rename, hold it.
pub fn compact(path: &Path, floor_index: u64) -> Result<Compacted> {
    let Some(source) = TreeFile::open_read(path)? else {
        return Ok(Compacted::Missing);
    };
    let latest = source.state()?;
    let Some((base, _tail)) = split_at_floor(&source, latest.frame_end, floor_index)? else {
        return Ok(Compacted::Uncovered);
    };
    let target_path = compacting_path(path);
    match crate::path_open::remove_file_nofollow(&target_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("zset tree: remove stale compaction: {error}")),
    }
    let target = TreeFile::open_write(&target_path)?;
    let mut state = write_base(&source, &target, &base.trailer)?;

    // Held until the new file is in place: no writer adds a frame meanwhile.
    let Some(locked) = TreeFile::open_read_locked(path)? else {
        let _ = crate::path_open::remove_file_nofollow(&target_path);
        return Ok(Compacted::Missing);
    };
    let latest = locked.state()?;
    let Some(tail) = frames_after(&locked, latest.frame_end, &base)? else {
        // Not the file the base came from.
        let _ = crate::path_open::remove_file_nofollow(&target_path);
        return Ok(Compacted::Uncovered);
    };
    for frame in tail {
        let header = FrameHeader::decode(&locked.read_exact(frame.start, FRAME_HEADER_SIZE)?)
            .filter(|header| !header.base)
            .ok_or_else(|| "zset tree: compaction met a damaged frame".to_string())?;
        let ops_len = header.ops_len as usize;
        let ops_at = frame.end - TRAILER_SIZE as u64 - ops_len as u64;
        let ops = decode_ops(&locked.read_exact(ops_at, ops_len)?)?;
        let mut tx = Tx::begin(&target, state);
        for op in &ops {
            tx.replay(op)?;
        }
        state = tx.commit(frame.trailer.token)?;
    }
    target.sync()?;
    target.write_slot(&HeaderSlot {
        generation: state.generation + 1,
        latest_frame_end: state.frame_end,
        synced_frame_end: state.frame_end,
    })?;
    target.sync()?;
    crate::path_open::rename_nofollow(&target_path, path)
        .map_err(|error| format!("zset tree: install compacted file: {error}"))?;
    if let Some(parent) = path.parent() {
        crate::path_open::open_directory_nofollow(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|error| format!("zset tree: sync directory: {error}"))?;
    }
    Ok(Compacted::Done(Compaction {
        bytes_before: locked.len(),
        bytes_after: state.frame_end,
    }))
}

/// Writes the tree `base` names into the empty `target` as one base frame.
fn write_base(source: &TreeFile, target: &TreeFile, base: &Trailer) -> Result<TreeState> {
    let frame_start = HEADER_REGION;
    let mut bulk = Bulk::new(target, frame_start + FRAME_HEADER_SIZE as u64);
    for root in [base.score_root, base.member_root] {
        visit(source, root, &mut |key, value| bulk.push_entry(key, value))?;
        bulk.finish_tree()?;
    }
    let (roots, page_count, pages_crc, pages_end) = bulk.into_parts();

    let header = FrameHeader {
        page_count,
        ops_len: 0,
        token: base.token,
        base: true,
    }
    .encode();
    target.write_at(frame_start, &header)?;
    let mut body = crc32fast::Hasher::new();
    body.update(&header);
    body.combine(&pages_crc);
    let generation = 2;
    let trailer = Trailer {
        score_root: roots[0],
        member_root: roots[1],
        count: base.count,
        frame_start,
        generation,
        token: base.token,
        body_crc: body.finalize(),
    };
    target.write_at(pages_end, &trailer.encode())?;
    let frame_end = pages_end + TRAILER_SIZE as u64;
    target.note_appended(frame_end);
    target.write_slot(&HeaderSlot {
        generation,
        latest_frame_end: frame_end,
        synced_frame_end: HEADER_REGION,
    })?;
    target.state()
}

/// Calls `f` with every key and value of a tree, in order.
fn visit(file: &TreeFile, root: u64, f: &mut dyn FnMut(Key, Vec<u8>) -> Result<()>) -> Result<()> {
    if root == 0 {
        return Ok(());
    }
    let page = file.read_page(root)?;
    match decode_node(&page, &mut |head, len| file.read_overflow(head, len))? {
        Node::Leaf(leaf) => {
            for (key, value) in leaf.keys.into_iter().zip(leaf.values) {
                f(Key::new(key.bytes), value)?;
            }
        }
        Node::Branch(branch) => {
            for child in branch.children {
                let Child::Disk(offset) = child else {
                    return Err("zset tree: unwritten child on disk".into());
                };
                visit(file, offset, f)?;
            }
        }
    }
    Ok(())
}

/// A node being filled, and the first key of everything under it.
struct Open {
    first: Key,
    node: Node,
}

#[derive(Default)]
struct Level {
    /// The last full node, kept back so the final one can be balanced with it.
    held: Option<Open>,
    pending: Option<Open>,
}

const FILL: usize = PAGE_SIZE * 7 / 8;

/// Builds packed trees bottom-up from keys in order, writing pages as they
/// fill. Holds one or two nodes per level.
struct Bulk<'f> {
    file: &'f TreeFile,
    next: u64,
    page_count: u32,
    crc: crc32fast::Hasher,
    levels: Vec<Level>,
    roots: Vec<u64>,
}

impl<'f> Bulk<'f> {
    fn new(file: &'f TreeFile, first_page: u64) -> Self {
        Self {
            file,
            next: first_page,
            page_count: 0,
            crc: crc32fast::Hasher::new(),
            levels: Vec::new(),
            roots: Vec::new(),
        }
    }

    fn into_parts(self) -> ([u64; 2], u32, crc32fast::Hasher, u64) {
        (
            [self.roots[0], self.roots[1]],
            self.page_count,
            self.crc,
            self.next,
        )
    }

    fn write_page(&mut self, page: &[u8]) -> Result<u64> {
        let offset = self.next;
        self.file.write_at(offset, page)?;
        self.crc.update(page);
        self.next += PAGE_SIZE as u64;
        self.page_count += 1;
        Ok(offset)
    }

    /// Writes a node; long keys get fresh overflow pages in this file.
    fn write_node(&mut self, mut node: Node) -> Result<(u64, u64)> {
        let keys = match &mut node {
            Node::Leaf(leaf) => &mut leaf.keys,
            Node::Branch(branch) => &mut branch.keys,
        };
        let mut chains = Vec::new();
        for key in keys.iter_mut() {
            key.overflow = None;
            if key.bytes.len() > INLINE_KEY_MAX {
                chains.push(key);
            }
        }
        for key in chains {
            let head = self.next;
            for page in encode_overflow(&key.bytes, head) {
                self.write_page(&page)?;
            }
            key.overflow = Some(head);
        }
        let count = node.count();
        let page = encode_node(&node, &|child| match child {
            Child::Disk(offset) => Ok(offset),
            Child::Dirty(_) => Err("zset tree: bulk node with an unwritten child".into()),
        })?;
        Ok((self.write_page(&page)?, count))
    }

    fn level(&mut self, index: usize) -> &mut Level {
        while self.levels.len() <= index {
            self.levels.push(Level::default());
        }
        &mut self.levels[index]
    }

    fn push_entry(&mut self, key: Key, value: Vec<u8>) -> Result<()> {
        let grows = key.encoded_len() + 2 + value.len();
        let level = self.level(0);
        if let Some(open) = &mut level.pending {
            let Node::Leaf(leaf) = &mut open.node else {
                unreachable!()
            };
            if leaf.encoded_len() + grows <= FILL {
                leaf.keys.push(key);
                leaf.values.push(value);
                return Ok(());
            }
        }
        let first = key.clone();
        self.rotate(
            0,
            Open {
                first,
                node: Node::Leaf(Leaf {
                    keys: vec![key],
                    values: vec![value],
                }),
            },
        )
    }

    fn push_child(&mut self, index: usize, first: Key, offset: u64, count: u64) -> Result<()> {
        let grows = first.encoded_len() + 16;
        let level = self.level(index);
        if let Some(open) = &mut level.pending {
            let Node::Branch(branch) = &mut open.node else {
                unreachable!()
            };
            if branch.encoded_len() + grows <= FILL {
                branch.keys.push(first);
                branch.children.push(Child::Disk(offset));
                branch.counts.push(count);
                return Ok(());
            }
        }
        self.rotate(
            index,
            Open {
                first,
                node: Node::Branch(Branch {
                    keys: Vec::new(),
                    children: vec![Child::Disk(offset)],
                    counts: vec![count],
                }),
            },
        )
    }

    /// Starts a new pending node; the old pending is held and the old held
    /// node is written and passed up.
    fn rotate(&mut self, index: usize, fresh: Open) -> Result<()> {
        let level = self.level(index);
        let full = level.pending.replace(fresh);
        let written = match full {
            Some(full) => level.held.replace(full),
            None => None,
        };
        if let Some(open) = written {
            let (offset, count) = self.write_node(open.node)?;
            self.push_child(index + 1, open.first, offset, count)?;
        }
        Ok(())
    }

    /// Ends the tree: each level's last two nodes are merged, and split
    /// again by bytes when they do not fit one page.
    fn finish_tree(&mut self) -> Result<()> {
        let mut index = 0;
        let root = loop {
            if index >= self.levels.len() {
                break 0;
            }
            let level = std::mem::take(&mut self.levels[index]);
            let mut nodes = balance_last(level.held, level.pending)?;
            let top = self.levels[index + 1..]
                .iter()
                .all(|level| level.held.is_none() && level.pending.is_none());
            if top && nodes.len() == 1 {
                let open = nodes.pop().unwrap();
                match open.node {
                    Node::Branch(branch) if branch.children.len() == 1 => {
                        let Child::Disk(offset) = branch.children[0] else {
                            unreachable!()
                        };
                        break offset;
                    }
                    node => break self.write_node(node)?.0,
                }
            }
            for open in nodes {
                let (offset, count) = self.write_node(open.node)?;
                self.push_child(index + 1, open.first, offset, count)?;
            }
            index += 1;
        };
        self.levels.clear();
        self.roots.push(root);
        Ok(())
    }
}

fn balance_last(held: Option<Open>, pending: Option<Open>) -> Result<Vec<Open>> {
    let (held, pending) = match (held, pending) {
        (None, None) => return Ok(Vec::new()),
        (Some(only), None) | (None, Some(only)) => return Ok(vec![only]),
        (Some(held), Some(pending)) => (held, pending),
    };
    let first = held.first;
    let merged = match (held.node, pending.node) {
        (Node::Leaf(mut into), Node::Leaf(from)) => {
            into.keys.extend(from.keys);
            into.values.extend(from.values);
            Node::Leaf(into)
        }
        (Node::Branch(mut into), Node::Branch(from)) => {
            into.keys.push(pending.first);
            into.keys.extend(from.keys);
            into.children.extend(from.children);
            into.counts.extend(from.counts);
            Node::Branch(into)
        }
        _ => return Err("zset tree: bulk nodes at different depths".into()),
    };
    if merged.encoded_len() <= PAGE_SIZE {
        return Ok(vec![Open {
            first,
            node: merged,
        }]);
    }
    Ok(match merged {
        Node::Leaf(mut leaf) => {
            let at = split_point(
                &leaf
                    .keys
                    .iter()
                    .zip(&leaf.values)
                    .map(|(key, value)| key.encoded_len() + 2 + value.len())
                    .collect::<Vec<_>>(),
                1,
            );
            let right = Leaf {
                keys: leaf.keys.split_off(at),
                values: leaf.values.split_off(at),
            };
            let right_first = right.keys[0].clone();
            vec![
                Open {
                    first,
                    node: Node::Leaf(leaf),
                },
                Open {
                    first: right_first,
                    node: Node::Leaf(right),
                },
            ]
        }
        Node::Branch(mut branch) => {
            let sizes: Vec<usize> = branch
                .keys
                .iter()
                .map(|key| key.encoded_len() + 16)
                .collect();
            let mid = split_point(&sizes, 1).min(sizes.len() - 2);
            let right_keys = branch.keys.split_off(mid + 1);
            let right_first = branch.keys.pop().expect("separator");
            let right = Branch {
                keys: right_keys,
                children: branch.children.split_off(mid + 1),
                counts: branch.counts.split_off(mid + 1),
            };
            vec![
                Open {
                    first,
                    node: Node::Branch(branch),
                },
                Open {
                    first: right_first,
                    node: Node::Branch(right),
                },
            ]
        }
    })
}

/// The index where the bytes before it first reach half, at least `min`.
fn split_point(sizes: &[usize], min: usize) -> usize {
    let half = sizes.iter().sum::<usize>() / 2;
    let mut at = min;
    let mut acc: usize = sizes[..min].iter().sum();
    while at < sizes.len() - 1 && acc + sizes[at] <= half {
        acc += sizes[at];
        at += 1;
    }
    at
}
