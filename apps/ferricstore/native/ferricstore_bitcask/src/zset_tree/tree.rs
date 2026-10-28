//! A copy-on-write transaction over the two trees of a file.
//!
//! The score tree is keyed by `score prefix ++ member`, the member tree by the
//! member with the score as value. Both keep per-child counts. A transaction
//! reads pages as it needs them, copies the nodes it changes into memory, and
//! on commit appends them as one frame; nothing it read is rewritten.

use std::borrow::Cow;
use std::collections::HashMap;

use super::format::{
    crc, encode_ops, FrameHeader, HeaderSlot, LoggedOp, Token, Trailer, FRAME_HEADER_SIZE,
    INLINE_KEY_MAX, PAGE_SIZE,
};
use super::node::{
    decode_node, encode_node, encode_overflow, Branch, Child, Key, Leaf, Node, PageView, EMPTY,
};
use super::store::{TreeFile, TreeState};
use super::Result;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Tree {
    Score = 0,
    Member = 1,
}

struct Split {
    sep: Key,
    right: Child,
    left_count: u64,
    right_count: u64,
}

pub struct Tx<'f> {
    file: &'f TreeFile,
    base: TreeState,
    nodes: Vec<Node>,
    roots: [Child; 2],
    pub count: u64,
    log: Vec<LoggedOp>,
}

/// The child that holds `key`: the number of separators at or below it.
fn child_index(seps: &[Key], key: &[u8]) -> usize {
    seps.partition_point(|sep| sep.bytes.as_slice() <= key)
}

fn leaf_search(leaf: &Leaf, key: &[u8]) -> std::result::Result<usize, usize> {
    leaf.keys
        .binary_search_by(|probe| probe.bytes.as_slice().cmp(key))
}

impl<'f> Tx<'f> {
    #[must_use]
    pub const fn begin(file: &'f TreeFile, base: TreeState) -> Self {
        Self {
            file,
            base,
            nodes: Vec::new(),
            roots: [Child::Disk(base.score_root), Child::Disk(base.member_root)],
            count: base.count,
            log: Vec::new(),
        }
    }

    #[must_use]
    pub const fn base(&self) -> &TreeState {
        &self.base
    }

    pub fn log(&mut self, op: LoggedOp) {
        self.log.push(op);
    }

    fn load(&self, offset: u64) -> Result<Node> {
        let page = self.file.read_page(offset)?;
        decode_node(&page, &mut |head, len| self.file.read_overflow(head, len))
    }

    fn node(&self, child: Child) -> Result<Cow<'_, Node>> {
        match child {
            Child::Disk(0) => Err("zset tree: read of the empty tree's root".into()),
            Child::Disk(offset) => self.load(offset).map(Cow::Owned),
            Child::Dirty(index) => Ok(Cow::Borrowed(&self.nodes[index])),
        }
    }

    fn make_dirty(&mut self, child: Child) -> Result<usize> {
        match child {
            Child::Dirty(index) => Ok(index),
            Child::Disk(0) => {
                self.nodes.push(Node::Leaf(Leaf::default()));
                Ok(self.nodes.len() - 1)
            }
            Child::Disk(offset) => {
                let node = self.load(offset)?;
                self.nodes.push(node);
                Ok(self.nodes.len() - 1)
            }
        }
    }

    fn push(&mut self, node: Node) -> Child {
        self.nodes.push(node);
        Child::Dirty(self.nodes.len() - 1)
    }

    /// The page at `offset`, read in place.
    fn page(&self, offset: u64) -> Result<Vec<u8>> {
        if offset == 0 {
            return Err("zset tree: read of the empty tree's root".into());
        }
        self.file.read_page(offset)
    }

    fn read_overflow(&self) -> impl FnMut(u64, usize) -> Result<Vec<u8>> + '_ {
        |head, len| self.file.read_overflow(head, len)
    }

    pub fn get(&self, tree: Tree, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut child = self.roots[tree as usize];
        if child == EMPTY {
            return Ok(None);
        }
        loop {
            match child {
                Child::Dirty(index) => match &self.nodes[index] {
                    Node::Leaf(leaf) => {
                        return Ok(leaf_search(leaf, key).ok().map(|i| leaf.values[i].clone()))
                    }
                    Node::Branch(branch) => {
                        child = branch.children[child_index(&branch.keys, key)];
                    }
                },
                Child::Disk(offset) => {
                    let page = self.page(offset)?;
                    let view = PageView::read(&page)?;
                    let read_overflow = &mut self.read_overflow();
                    if view.is_leaf() {
                        let found = view.search(key, read_overflow)?;
                        return Ok(found.ok().map(|i| view.value(i).to_vec()));
                    }
                    child = Child::Disk(view.child(view.child_index(key, read_overflow)?)?.0);
                }
            }
        }
    }

    /// The number of keys below `key`.
    pub fn rank(&self, tree: Tree, key: &[u8]) -> Result<u64> {
        let mut child = self.roots[tree as usize];
        if child == EMPTY {
            return Ok(0);
        }
        let mut below = 0;
        loop {
            match child {
                Child::Dirty(index) => match &self.nodes[index] {
                    Node::Leaf(leaf) => {
                        let position = match leaf_search(leaf, key) {
                            Ok(i) | Err(i) => i,
                        };
                        return Ok(below + position as u64);
                    }
                    Node::Branch(branch) => {
                        let i = child_index(&branch.keys, key);
                        below += branch.counts[..i].iter().sum::<u64>();
                        child = branch.children[i];
                    }
                },
                Child::Disk(offset) => {
                    let page = self.page(offset)?;
                    let view = PageView::read(&page)?;
                    let read_overflow = &mut self.read_overflow();
                    if view.is_leaf() {
                        let position = match view.search(key, read_overflow)? {
                            Ok(i) | Err(i) => i,
                        };
                        return Ok(below + position as u64);
                    }
                    let i = view.child_index(key, read_overflow)?;
                    for j in 0..i {
                        below += view.child(j)?.1;
                    }
                    child = Child::Disk(view.child(i)?.0);
                }
            }
        }
    }

    /// Keys and values at positions `[start, end)`, in order.
    pub fn range(&self, tree: Tree, start: u64, end: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let root = self.roots[tree as usize];
        if root != EMPTY && start < end {
            self.range_rec(root, start, end, &mut out)?;
        }
        Ok(out)
    }

    fn range_rec(
        &self,
        child: Child,
        start: u64,
        end: u64,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let Child::Disk(page_offset) = child else {
            return self.range_dirty(child, start, end, out);
        };
        let page = self.page(page_offset)?;
        let view = PageView::read(&page)?;
        if view.is_leaf() {
            let read_overflow = &mut self.read_overflow();
            let stop = (end as usize).min(view.len());
            for i in (start as usize)..stop {
                out.push((view.key(i, read_overflow)?, view.value(i).to_vec()));
            }
            return Ok(());
        }
        let mut offset = 0;
        for i in 0..view.children() {
            if offset >= end {
                break;
            }
            let (below, count) = view.child(i)?;
            if offset + count > start {
                self.range_rec(
                    Child::Disk(below),
                    start.saturating_sub(offset),
                    (end - offset).min(count),
                    out,
                )?;
            }
            offset += count;
        }
        Ok(())
    }

    fn range_dirty(
        &self,
        child: Child,
        start: u64,
        end: u64,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let node = self.node(child)?;
        match node.as_ref() {
            Node::Leaf(leaf) => {
                let stop = (end as usize).min(leaf.keys.len());
                for i in (start as usize)..stop {
                    out.push((leaf.keys[i].bytes.clone(), leaf.values[i].clone()));
                }
            }
            Node::Branch(branch) => {
                let mut offset = 0;
                for (i, &count) in branch.counts.iter().enumerate() {
                    if offset >= end {
                        break;
                    }
                    if offset + count > start {
                        self.range_rec(
                            branch.children[i],
                            start.saturating_sub(offset),
                            (end - offset).min(count),
                            out,
                        )?;
                    }
                    offset += count;
                }
            }
        }
        Ok(())
    }

    /// Inserts or replaces; returns the replaced value.
    pub fn insert(&mut self, tree: Tree, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let key = Key::new(key);
        let root = self.roots[tree as usize];
        let (node, replaced, split) = self.insert_rec(root, &key, &value)?;
        self.roots[tree as usize] = match split {
            None => node,
            Some(split) => self.push(Node::Branch(Branch {
                keys: vec![split.sep],
                children: vec![node, split.right],
                counts: vec![split.left_count, split.right_count],
            })),
        };
        Ok(replaced)
    }

    fn insert_rec(
        &mut self,
        child: Child,
        key: &Key,
        value: &[u8],
    ) -> Result<(Child, Option<Vec<u8>>, Option<Split>)> {
        let index = self.make_dirty(child)?;
        if let Node::Leaf(leaf) = &mut self.nodes[index] {
            match leaf_search(leaf, &key.bytes) {
                Ok(i) => {
                    let old = std::mem::replace(&mut leaf.values[i], value.to_vec());
                    return Ok((Child::Dirty(index), Some(old), None));
                }
                Err(i) => {
                    leaf.keys.insert(i, key.clone());
                    leaf.values.insert(i, value.to_vec());
                }
            }
            let split = (leaf.encoded_len() > PAGE_SIZE).then(|| self.split_leaf(index));
            return Ok((Child::Dirty(index), None, split));
        }

        let (position, below) = match &self.nodes[index] {
            Node::Branch(branch) => {
                let i = child_index(&branch.keys, &key.bytes);
                (i, branch.children[i])
            }
            Node::Leaf(_) => unreachable!(),
        };
        let (node, replaced, split) = self.insert_rec(below, key, value)?;
        let Node::Branch(branch) = &mut self.nodes[index] else {
            unreachable!()
        };
        branch.children[position] = node;
        if replaced.is_none() {
            branch.counts[position] += 1;
        }
        if let Some(split) = split {
            branch.counts[position] = split.left_count;
            branch.keys.insert(position, split.sep);
            branch.children.insert(position + 1, split.right);
            branch.counts.insert(position + 1, split.right_count);
        }
        let split = (branch.encoded_len() > PAGE_SIZE).then(|| self.split_branch(index));
        Ok((Child::Dirty(index), replaced, split))
    }

    /// Splits by bytes, so each half fits a page whatever its key sizes.
    fn split_leaf(&mut self, index: usize) -> Split {
        let Node::Leaf(leaf) = &mut self.nodes[index] else {
            unreachable!()
        };
        let sizes: Vec<usize> = leaf
            .keys
            .iter()
            .zip(&leaf.values)
            .map(|(key, value)| key.encoded_len() + 2 + value.len())
            .collect();
        let half = sizes.iter().sum::<usize>() / 2;
        let mut at = 1;
        let mut acc = sizes[0];
        while at < sizes.len() - 1 && acc + sizes[at] <= half {
            acc += sizes[at];
            at += 1;
        }
        let right = Leaf {
            keys: leaf.keys.split_off(at),
            values: leaf.values.split_off(at),
        };
        let left_count = leaf.keys.len() as u64;
        let right_count = right.keys.len() as u64;
        let sep = right.keys[0].clone();
        let right = self.push(Node::Leaf(right));
        Split {
            sep,
            right,
            left_count,
            right_count,
        }
    }

    fn split_branch(&mut self, index: usize) -> Split {
        let Node::Branch(branch) = &mut self.nodes[index] else {
            unreachable!()
        };
        let sizes: Vec<usize> = branch
            .keys
            .iter()
            .map(|key| key.encoded_len() + 16)
            .collect();
        let half = sizes.iter().sum::<usize>() / 2;
        // The promoted separator leaves at least two children on each side.
        let mut mid = 1;
        let mut acc = sizes[0];
        while mid < sizes.len() - 2 && acc + sizes[mid] <= half {
            acc += sizes[mid];
            mid += 1;
        }
        let right_keys = branch.keys.split_off(mid + 1);
        let sep = branch.keys.pop().expect("separator to promote");
        let right = Branch {
            keys: right_keys,
            children: branch.children.split_off(mid + 1),
            counts: branch.counts.split_off(mid + 1),
        };
        let left_count = branch.total();
        let right_count = right.total();
        let right = self.push(Node::Branch(right));
        Split {
            sep,
            right,
            left_count,
            right_count,
        }
    }

    /// Removes `key`; returns its value, or `None` when it is absent.
    pub fn remove(&mut self, tree: Tree, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.get(tree, key)?.is_none() {
            return Ok(None);
        }
        let root = self.roots[tree as usize];
        let (mut node, value) = self.remove_rec(root, key)?;
        // A root with one child hands over to it; an empty leaf root is the
        // empty tree.
        loop {
            match self.node(node)?.as_ref() {
                Node::Branch(branch) if branch.children.len() == 1 => node = branch.children[0],
                Node::Leaf(leaf) if leaf.keys.is_empty() => {
                    node = EMPTY;
                    break;
                }
                _ => break,
            }
        }
        self.roots[tree as usize] = node;
        Ok(Some(value))
    }

    fn remove_rec(&mut self, child: Child, key: &[u8]) -> Result<(Child, Vec<u8>)> {
        let index = self.make_dirty(child)?;
        if let Node::Leaf(leaf) = &mut self.nodes[index] {
            let i = leaf_search(leaf, key)
                .map_err(|_| "zset tree: removed key vanished".to_string())?;
            leaf.keys.remove(i);
            return Ok((Child::Dirty(index), leaf.values.remove(i)));
        }
        let (position, below) = match &self.nodes[index] {
            Node::Branch(branch) => {
                let i = child_index(&branch.keys, key);
                (i, branch.children[i])
            }
            Node::Leaf(_) => unreachable!(),
        };
        let (node, value) = self.remove_rec(below, key)?;
        if let Node::Branch(branch) = &mut self.nodes[index] {
            branch.children[position] = node;
            branch.counts[position] -= 1;
        }
        self.rebalance(index, position)?;
        Ok((Child::Dirty(index), value))
    }

    /// After a removal under `position`: drops an emptied child, and merges an
    /// underfull one with a neighbor, splitting the result again when it does
    /// not fit a page. Either way both sides end at least a quarter full and a
    /// branch keeps at least two children.
    fn rebalance(&mut self, index: usize, position: usize) -> Result<()> {
        let Node::Branch(branch) = &self.nodes[index] else {
            unreachable!()
        };
        if branch.counts[position] == 0 {
            let Node::Branch(branch) = &mut self.nodes[index] else {
                unreachable!()
            };
            branch.children.remove(position);
            branch.counts.remove(position);
            branch.keys.remove(
                position
                    .saturating_sub(1)
                    .min(branch.keys.len().saturating_sub(1)),
            );
            return Ok(());
        }
        if branch.children.len() < 2
            || self.node(branch.children[position])?.encoded_len() >= PAGE_SIZE / 4
        {
            return Ok(());
        }
        let left = if position > 0 { position - 1 } else { position };
        let (left_child, right_child, sep) = (
            branch.children[left],
            branch.children[left + 1],
            branch.keys[left].clone(),
        );
        let left_index = self.make_dirty(left_child)?;
        let right_node = self.node(right_child)?.into_owned();
        match (&mut self.nodes[left_index], right_node) {
            (Node::Leaf(into), Node::Leaf(from)) => {
                into.keys.extend(from.keys);
                into.values.extend(from.values);
            }
            (Node::Branch(into), Node::Branch(from)) => {
                into.keys.push(sep);
                into.keys.extend(from.keys);
                into.children.extend(from.children);
                into.counts.extend(from.counts);
            }
            _ => return Err("zset tree: siblings at different depths".into()),
        }
        let split = match &self.nodes[left_index] {
            Node::Leaf(leaf) if leaf.encoded_len() > PAGE_SIZE => Some(self.split_leaf(left_index)),
            Node::Branch(merged) if merged.encoded_len() > PAGE_SIZE => {
                Some(self.split_branch(left_index))
            }
            _ => None,
        };
        let Node::Branch(branch) = &mut self.nodes[index] else {
            unreachable!()
        };
        branch.children[left] = Child::Dirty(left_index);
        if let Some(split) = split {
            branch.keys[left] = split.sep;
            branch.children[left + 1] = split.right;
            branch.counts[left] = split.left_count;
            branch.counts[left + 1] = split.right_count;
        } else {
            branch.counts[left] += branch.counts[left + 1];
            branch.children.remove(left + 1);
            branch.counts.remove(left + 1);
            branch.keys.remove(left);
        }
        Ok(())
    }

    /// Appends the changed nodes as one frame and points a header slot at it.
    /// A transaction that changed nothing writes nothing.
    pub fn commit(mut self, token: Token) -> Result<TreeState> {
        let file = self.file;
        if self.log.is_empty() {
            return Ok(self.base);
        }
        let frame_start = self.base.frame_end;
        let pages_base = frame_start + FRAME_HEADER_SIZE as u64;
        let mut pages: Vec<Vec<u8>> = Vec::new();
        let mut written: HashMap<usize, u64> = HashMap::new();
        let mut overflow: HashMap<Vec<u8>, u64> = HashMap::new();
        let roots = self.roots;
        let score_root = self.write_child(
            roots[0],
            pages_base,
            &mut pages,
            &mut written,
            &mut overflow,
        )?;
        let member_root = self.write_child(
            roots[1],
            pages_base,
            &mut pages,
            &mut written,
            &mut overflow,
        )?;

        let ops = encode_ops(&self.log);
        let header = FrameHeader {
            page_count: pages.len() as u32,
            ops_len: ops.len() as u32,
            token,
            base: false,
        }
        .encode();
        let mut frame = Vec::with_capacity(
            header.len() + pages.len() * PAGE_SIZE + ops.len() + super::format::TRAILER_SIZE,
        );
        frame.extend_from_slice(&header);
        for page in &pages {
            frame.extend_from_slice(page);
        }
        frame.extend_from_slice(&ops);
        let generation = self.base.generation + 1;
        let trailer = Trailer {
            score_root,
            member_root,
            count: self.count,
            frame_start,
            generation,
            token,
            body_crc: crc(&[&frame]),
        };
        frame.extend_from_slice(&trailer.encode());
        let frame_end = frame_start + frame.len() as u64;
        file.write_at(frame_start, &frame)?;
        file.note_appended(frame_end);
        let slot = HeaderSlot {
            generation,
            latest_frame_end: frame_end,
            synced_frame_end: self.base.synced_frame_end,
        };
        file.write_slot(&slot)?;
        Ok(TreeState {
            score_root,
            member_root,
            count: self.count,
            token,
            frame_end,
            generation,
            synced_frame_end: slot.synced_frame_end,
        })
    }

    /// Writes a dirty subtree children first, so every parent knows its
    /// children's offsets. Long keys get their overflow pages before the node
    /// that names them; a key written twice in one frame shares one chain.
    fn write_child(
        &mut self,
        child: Child,
        base: u64,
        pages: &mut Vec<Vec<u8>>,
        written: &mut HashMap<usize, u64>,
        overflow: &mut HashMap<Vec<u8>, u64>,
    ) -> Result<u64> {
        let index = match child {
            Child::Disk(offset) => return Ok(offset),
            Child::Dirty(index) => index,
        };
        if let Some(&offset) = written.get(&index) {
            return Ok(offset);
        }
        if let Node::Branch(branch) = &self.nodes[index] {
            for below in branch.children.clone() {
                self.write_child(below, base, pages, written, overflow)?;
            }
        }
        let keys = match &mut self.nodes[index] {
            Node::Leaf(leaf) => &mut leaf.keys,
            Node::Branch(branch) => &mut branch.keys,
        };
        for key in keys.iter_mut() {
            if key.bytes.len() > INLINE_KEY_MAX && key.overflow.is_none() {
                let head = if let Some(&head) = overflow.get(&key.bytes) {
                    head
                } else {
                    let head = base + (pages.len() * PAGE_SIZE) as u64;
                    pages.extend(encode_overflow(&key.bytes, head));
                    overflow.insert(key.bytes.clone(), head);
                    head
                };
                key.overflow = Some(head);
            }
        }
        let page = encode_node(&self.nodes[index], &|below| match below {
            Child::Disk(offset) => Ok(offset),
            Child::Dirty(i) => written
                .get(&i)
                .copied()
                .ok_or_else(|| "zset tree: child written after its parent".to_string()),
        })?;
        let offset = base + (pages.len() * PAGE_SIZE) as u64;
        pages.push(page);
        written.insert(index, offset);
        Ok(offset)
    }

    /// Checks every structural invariant: order, separator bounds, counts,
    /// page fit, equal depth. Returns the number of keys.
    #[cfg(test)]
    pub fn check(&self, tree: Tree) -> Result<u64> {
        let root = self.roots[tree as usize];
        if root == EMPTY {
            return Ok(0);
        }
        let mut depth = None;
        self.check_rec(root, None, None, 0, &mut depth)
    }

    #[cfg(test)]
    fn check_rec(
        &self,
        child: Child,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        level: usize,
        leaf_depth: &mut Option<usize>,
    ) -> Result<u64> {
        let node = self.node(child)?;
        if node.encoded_len() > PAGE_SIZE {
            return Err("node exceeds a page".into());
        }
        let in_bounds =
            |key: &[u8]| low.map_or(true, |low| key >= low) && high.map_or(true, |high| key < high);
        match node.as_ref() {
            Node::Leaf(leaf) => {
                if leaf.keys.is_empty() {
                    return Err("empty leaf in a non-empty tree".into());
                }
                if leaf_depth.replace(level).is_some_and(|d| d != level) {
                    return Err("leaves at different depths".into());
                }
                for pair in leaf.keys.windows(2) {
                    if pair[0].bytes >= pair[1].bytes {
                        return Err("leaf keys out of order".into());
                    }
                }
                if !leaf.keys.iter().all(|key| in_bounds(&key.bytes)) {
                    return Err("leaf key outside its separators".into());
                }
                Ok(leaf.keys.len() as u64)
            }
            Node::Branch(branch) => {
                if branch.children.len() < 2 || branch.keys.len() + 1 != branch.children.len() {
                    return Err("malformed branch".into());
                }
                let mut total = 0;
                for (i, &below) in branch.children.iter().enumerate() {
                    let child_low = if i == 0 {
                        low
                    } else {
                        Some(branch.keys[i - 1].bytes.as_slice())
                    };
                    let child_high = branch.keys.get(i).map(|key| key.bytes.as_slice()).or(high);
                    let count =
                        self.check_rec(below, child_low, child_high, level + 1, leaf_depth)?;
                    if count != branch.counts[i] {
                        return Err(format!(
                            "branch count {} != subtree {count}",
                            branch.counts[i]
                        ));
                    }
                    total += count;
                }
                Ok(total)
            }
        }
    }
}
