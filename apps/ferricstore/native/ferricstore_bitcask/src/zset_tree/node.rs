//! Tree nodes and their page encoding.
//!
//! ```text
//! page = [ kind u8 | 0 u8 | n u16 | used u32 | crc u32 | 0 u32 | payload ... ]
//! key  = [ 0 | len u16 | bytes ]                                  inline
//!      | [ 1 | len u32 | first 64 bytes | overflow page offset u64 ] long
//! leaf    payload = n x ( key | value len u16 | value )
//! branch  payload = child u64 | count u64 | (n - 1) x ( key | child u64 | count u64 )
//! overflow payload = next page offset u64 | bytes
//! ```
//!
//! Every branch entry carries the number of keys under its child, so a key's
//! position and the key at a position are found in one descent.

use super::format::{crc, get_u16, get_u32, get_u64, INLINE_KEY_MAX, OVERFLOW_PREFIX, PAGE_SIZE};
use super::Result;

pub const PAGE_HEADER: usize = 16;
const KIND_LEAF: u8 = 1;
const KIND_BRANCH: u8 = 2;
const KIND_OVERFLOW: u8 = 3;
const OVERFLOW_KEY_SIZE: usize = 1 + 4 + OVERFLOW_PREFIX + 8;
pub const OVERFLOW_PAYLOAD: usize = PAGE_SIZE - PAGE_HEADER - 8;

/// A key, always held whole in memory. `overflow` names the page chain that
/// already holds a long key on disk, so a rewritten node reuses it.
#[derive(Clone, Debug)]
pub struct Key {
    pub bytes: Vec<u8>,
    pub overflow: Option<u64>,
}

impl Key {
    #[must_use]
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            overflow: None,
        }
    }

    #[must_use]
    pub fn encoded_len(&self) -> usize {
        if self.bytes.len() > INLINE_KEY_MAX {
            OVERFLOW_KEY_SIZE
        } else {
            3 + self.bytes.len()
        }
    }
}

/// A child pointer: a page already on disk, or a node changed in this
/// transaction and not yet written. `Disk(0)` is the empty tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Child {
    Disk(u64),
    Dirty(usize),
}

pub const EMPTY: Child = Child::Disk(0);

#[derive(Clone, Debug, Default)]
pub struct Leaf {
    pub keys: Vec<Key>,
    pub values: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
pub struct Branch {
    /// Separators: child `i` holds keys in `[keys[i - 1], keys[i])`.
    pub keys: Vec<Key>,
    pub children: Vec<Child>,
    pub counts: Vec<u64>,
}

#[derive(Clone, Debug)]
pub enum Node {
    Leaf(Leaf),
    Branch(Branch),
}

impl Leaf {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        PAGE_HEADER
            + self
                .keys
                .iter()
                .zip(&self.values)
                .map(|(key, value)| key.encoded_len() + 2 + value.len())
                .sum::<usize>()
    }
}

impl Branch {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        PAGE_HEADER
            + 16
            + self
                .keys
                .iter()
                .map(|key| key.encoded_len() + 16)
                .sum::<usize>()
    }

    #[must_use]
    pub fn total(&self) -> u64 {
        self.counts.iter().sum()
    }
}

impl Node {
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        match self {
            Self::Leaf(leaf) => leaf.encoded_len(),
            Self::Branch(branch) => branch.encoded_len(),
        }
    }

    #[must_use]
    pub fn count(&self) -> u64 {
        match self {
            Self::Leaf(leaf) => leaf.keys.len() as u64,
            Self::Branch(branch) => branch.total(),
        }
    }
}

fn page_header(kind: u8, n: usize, used: usize) -> Vec<u8> {
    let mut page = Vec::with_capacity(PAGE_SIZE);
    page.push(kind);
    page.push(0);
    page.extend_from_slice(&(n as u16).to_le_bytes());
    page.extend_from_slice(&(used as u32).to_le_bytes());
    page.extend_from_slice(&[0; 8]);
    page
}

fn seal(mut page: Vec<u8>) -> Vec<u8> {
    let used = page.len();
    let checksum = crc(&[&page[0..8], &page[12..used]]);
    page[8..12].copy_from_slice(&checksum.to_le_bytes());
    page.resize(PAGE_SIZE, 0);
    page
}

fn encode_key(out: &mut Vec<u8>, key: &Key) -> Result<()> {
    if key.bytes.len() > INLINE_KEY_MAX {
        let head = key
            .overflow
            .ok_or_else(|| "zset tree: long key written before its overflow pages".to_string())?;
        out.push(1);
        out.extend_from_slice(&(key.bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&key.bytes[..OVERFLOW_PREFIX]);
        out.extend_from_slice(&head.to_le_bytes());
    } else {
        out.push(0);
        out.extend_from_slice(&(key.bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(&key.bytes);
    }
    Ok(())
}

/// Encodes a node whose children are all on disk. `child_offset` maps each
/// child to its page offset.
pub fn encode_node(node: &Node, child_offset: &dyn Fn(Child) -> Result<u64>) -> Result<Vec<u8>> {
    let used = node.encoded_len();
    if used > PAGE_SIZE {
        return Err(format!(
            "zset tree: node of {used} bytes does not fit a page"
        ));
    }
    let page = match node {
        Node::Leaf(leaf) => {
            let mut page = page_header(KIND_LEAF, leaf.keys.len(), used);
            for (key, value) in leaf.keys.iter().zip(&leaf.values) {
                encode_key(&mut page, key)?;
                page.extend_from_slice(&(value.len() as u16).to_le_bytes());
                page.extend_from_slice(value);
            }
            page
        }
        Node::Branch(branch) => {
            let mut page = page_header(KIND_BRANCH, branch.children.len(), used);
            page.extend_from_slice(&child_offset(branch.children[0])?.to_le_bytes());
            page.extend_from_slice(&branch.counts[0].to_le_bytes());
            for (i, key) in branch.keys.iter().enumerate() {
                encode_key(&mut page, key)?;
                page.extend_from_slice(&child_offset(branch.children[i + 1])?.to_le_bytes());
                page.extend_from_slice(&branch.counts[i + 1].to_le_bytes());
            }
            page
        }
    };
    debug_assert_eq!(page.len(), used);
    Ok(seal(page))
}

/// Splits a long key's bytes into overflow pages. Page `i` is written at
/// `first_offset + i * PAGE_SIZE`.
#[must_use]
pub fn encode_overflow(bytes: &[u8], first_offset: u64) -> Vec<Vec<u8>> {
    let chunks: Vec<&[u8]> = bytes.chunks(OVERFLOW_PAYLOAD).collect();
    chunks
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            let next = if i + 1 < chunks.len() {
                first_offset + ((i + 1) * PAGE_SIZE) as u64
            } else {
                0
            };
            let used = PAGE_HEADER + 8 + chunk.len();
            let mut page = page_header(KIND_OVERFLOW, 0, used);
            page.extend_from_slice(&next.to_le_bytes());
            page.extend_from_slice(chunk);
            seal(page)
        })
        .collect()
}

/// Checks a page's checksum and returns its kind, entry count and used bytes.
pub fn verify_page(page: &[u8]) -> Result<(u8, usize, usize)> {
    if page.len() != PAGE_SIZE {
        return Err("zset tree: short page".into());
    }
    let n = get_u16(page, 2)? as usize;
    let used = get_u32(page, 4)? as usize;
    if !(PAGE_HEADER..=PAGE_SIZE).contains(&used) {
        return Err("zset tree: page length out of range".into());
    }
    if crc(&[&page[0..8], &page[12..used]]) != get_u32(page, 8)? {
        return Err("zset tree: page checksum mismatch".into());
    }
    Ok((page[0], n, used))
}

/// The next page and bytes of one overflow page.
pub fn decode_overflow(page: &[u8]) -> Result<(u64, &[u8])> {
    let (kind, _n, used) = verify_page(page)?;
    if kind != KIND_OVERFLOW {
        return Err("zset tree: expected an overflow page".into());
    }
    Ok((get_u64(page, PAGE_HEADER)?, &page[PAGE_HEADER + 8..used]))
}

#[cfg(test)]
thread_local! {
    /// How many keys this thread copied out of pages by decoding whole nodes.
    pub static DECODED_KEYS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn decode_key(
    page: &[u8],
    at: &mut usize,
    read_overflow: &mut dyn FnMut(u64, usize) -> Result<Vec<u8>>,
) -> Result<Key> {
    #[cfg(test)]
    DECODED_KEYS.with(|keys| keys.set(keys.get() + 1));
    let stored = stored_key(page, at)?;
    Ok(Key {
        bytes: stored.bytes(read_overflow)?,
        overflow: match stored {
            StoredKey::Inline(_) => None,
            StoredKey::Overflow { head, .. } => Some(head),
        },
    })
}

/// A key as its page stores it: the bytes of an inline key, or the length,
/// first bytes and overflow chain of a long one.
#[derive(Clone, Copy)]
enum StoredKey<'p> {
    Inline(&'p [u8]),
    Overflow {
        len: usize,
        prefix: &'p [u8],
        head: u64,
    },
}

fn stored_key<'p>(page: &'p [u8], at: &mut usize) -> Result<StoredKey<'p>> {
    let truncated = || "zset tree: truncated key".to_string();
    match page.get(*at) {
        Some(0) => {
            let len = get_u16(page, *at + 1)? as usize;
            let bytes = page.get(*at + 3..*at + 3 + len).ok_or_else(truncated)?;
            *at += 3 + len;
            Ok(StoredKey::Inline(bytes))
        }
        Some(1) => {
            let len = get_u32(page, *at + 1)? as usize;
            let prefix = page
                .get(*at + 5..*at + 5 + OVERFLOW_PREFIX)
                .ok_or_else(truncated)?;
            let head = get_u64(page, *at + 5 + OVERFLOW_PREFIX)?;
            *at += OVERFLOW_KEY_SIZE;
            Ok(StoredKey::Overflow { len, prefix, head })
        }
        _ => Err("zset tree: unknown key encoding".into()),
    }
}

impl StoredKey<'_> {
    fn bytes(
        self,
        read_overflow: &mut dyn FnMut(u64, usize) -> Result<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        match self {
            Self::Inline(bytes) => Ok(bytes.to_vec()),
            Self::Overflow { len, prefix, head } => {
                let bytes = read_overflow(head, len)?;
                if bytes.len() != len || &bytes[..OVERFLOW_PREFIX] != prefix {
                    return Err("zset tree: overflow key does not match its prefix".into());
                }
                Ok(bytes)
            }
        }
    }

    /// Orders the stored key against `key`; a long key's overflow pages are
    /// read only when its first bytes tie.
    fn cmp(
        self,
        key: &[u8],
        read_overflow: &mut dyn FnMut(u64, usize) -> Result<Vec<u8>>,
    ) -> Result<std::cmp::Ordering> {
        match self {
            Self::Inline(bytes) => Ok(bytes.cmp(key)),
            Self::Overflow { prefix, .. } => {
                match prefix.cmp(&key[..key.len().min(OVERFLOW_PREFIX)]) {
                    std::cmp::Ordering::Equal => Ok(self.bytes(read_overflow)?.as_slice().cmp(key)),
                    order => Ok(order),
                }
            }
        }
    }
}

/// A leaf or branch read where it lies in its page: entries are compared in
/// place and only what a read returns is copied out.
pub struct PageView<'p> {
    page: &'p [u8],
    leaf: bool,
    /// A leaf's keys, each with where its value starts; a branch's
    /// separators, each with where the child after it starts.
    entries: Vec<(StoredKey<'p>, usize)>,
}

impl<'p> PageView<'p> {
    pub fn read(page: &'p [u8]) -> Result<Self> {
        let (kind, n, used) = verify_page(page)?;
        let page = &page[..used];
        let truncated = || "zset tree: truncated entry".to_string();
        let mut at = PAGE_HEADER;
        let mut entries = Vec::with_capacity(n);
        match kind {
            KIND_LEAF => {
                for _ in 0..n {
                    let key = stored_key(page, &mut at)?;
                    let len = get_u16(page, at)? as usize;
                    page.get(at + 2..at + 2 + len).ok_or_else(truncated)?;
                    entries.push((key, at));
                    at += 2 + len;
                }
            }
            KIND_BRANCH => {
                if n < 2 {
                    return Err("zset tree: branch with fewer than two children".into());
                }
                page.get(at..at + 16).ok_or_else(truncated)?;
                at += 16;
                for _ in 1..n {
                    let key = stored_key(page, &mut at)?;
                    page.get(at..at + 16).ok_or_else(truncated)?;
                    entries.push((key, at));
                    at += 16;
                }
            }
            _ => return Err("zset tree: expected a leaf or branch page".into()),
        }
        Ok(Self {
            page,
            leaf: kind == KIND_LEAF,
            entries,
        })
    }

    #[must_use]
    pub const fn is_leaf(&self) -> bool {
        self.leaf
    }

    /// A leaf's number of keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// A leaf's key at `i`, whole.
    pub fn key(
        &self,
        i: usize,
        read_overflow: &mut dyn FnMut(u64, usize) -> Result<Vec<u8>>,
    ) -> Result<Vec<u8>> {
        self.entries[i].0.bytes(read_overflow)
    }

    /// A leaf's value at `i`.
    #[must_use]
    pub fn value(&self, i: usize) -> &'p [u8] {
        let at = self.entries[i].1;
        let len = usize::from(u16::from_le_bytes([self.page[at], self.page[at + 1]]));
        &self.page[at + 2..at + 2 + len]
    }

    /// Where `key` is in a leaf, or where it would go.
    pub fn search(
        &self,
        key: &[u8],
        read_overflow: &mut dyn FnMut(u64, usize) -> Result<Vec<u8>>,
    ) -> Result<std::result::Result<usize, usize>> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            let mid = low + (high - low) / 2;
            match self.entries[mid].0.cmp(key, read_overflow)? {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(Ok(mid)),
            }
        }
        Ok(Err(low))
    }

    /// A branch's number of children.
    #[must_use]
    pub fn children(&self) -> usize {
        self.entries.len() + 1
    }

    /// The branch child that holds `key`: the number of separators at or
    /// below it.
    pub fn child_index(
        &self,
        key: &[u8],
        read_overflow: &mut dyn FnMut(u64, usize) -> Result<Vec<u8>>,
    ) -> Result<usize> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            let mid = low + (high - low) / 2;
            if self.entries[mid].0.cmp(key, read_overflow)? == std::cmp::Ordering::Greater {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        Ok(low)
    }

    /// A branch child's page offset and the number of keys under it.
    pub fn child(&self, i: usize) -> Result<(u64, u64)> {
        let at = if i == 0 {
            PAGE_HEADER
        } else {
            self.entries[i - 1].1
        };
        Ok((get_u64(self.page, at)?, get_u64(self.page, at + 8)?))
    }
}

pub fn decode_node(
    page: &[u8],
    read_overflow: &mut dyn FnMut(u64, usize) -> Result<Vec<u8>>,
) -> Result<Node> {
    let (kind, n, used) = verify_page(page)?;
    let page = &page[..used];
    let mut at = PAGE_HEADER;
    match kind {
        KIND_LEAF => {
            let mut leaf = Leaf {
                keys: Vec::with_capacity(n),
                values: Vec::with_capacity(n),
            };
            for _ in 0..n {
                leaf.keys.push(decode_key(page, &mut at, read_overflow)?);
                let len = get_u16(page, at)? as usize;
                let value = page
                    .get(at + 2..at + 2 + len)
                    .ok_or_else(|| "zset tree: truncated value".to_string())?;
                leaf.values.push(value.to_vec());
                at += 2 + len;
            }
            Ok(Node::Leaf(leaf))
        }
        KIND_BRANCH => {
            if n < 2 {
                return Err("zset tree: branch with fewer than two children".into());
            }
            let mut branch = Branch {
                keys: Vec::with_capacity(n - 1),
                children: Vec::with_capacity(n),
                counts: Vec::with_capacity(n),
            };
            branch.children.push(Child::Disk(get_u64(page, at)?));
            branch.counts.push(get_u64(page, at + 8)?);
            at += 16;
            for _ in 1..n {
                branch.keys.push(decode_key(page, &mut at, read_overflow)?);
                branch.children.push(Child::Disk(get_u64(page, at)?));
                branch.counts.push(get_u64(page, at + 8)?);
                at += 16;
            }
            Ok(Node::Branch(branch))
        }
        _ => Err("zset tree: expected a leaf or branch page".into()),
    }
}
