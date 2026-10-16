//! Compact form of Raft log commands, used by data format 2.
//!
//! A command is stored in the Erlang external term format. Most of its bytes
//! are atom names (command tags, attribute keys, wrapper tags) and the apply
//! context tuple, which repeat in every entry. `compact` replaces atoms and
//! whole terms found in a frozen dictionary with short references (two bytes,
//! three past the first 256 atoms), and `expand` restores the original bytes
//! exactly.
//!
//! Every dictionary is frozen once released: replicas and replay must expand
//! old entries to the same bytes forever. A new dictionary takes a new version
//! byte and the old one stays, and writing it needs a cluster feature level,
//! since members on an older release cannot expand it.

const MAGIC: u8 = 0xFC;
const DICTIONARY_V1: u8 = 1;
const ATOM_REF: u8 = 1;
const TERM_REF: u8 = 2;
const WIDE_ATOM_REF: u8 = 3;
// Commands nest a few levels; deeper user values stay in the plain form.
const MAX_DEPTH: usize = 128;

const VERSION_MAGIC: u8 = 131;
const NEW_FLOAT_EXT: u8 = 70;
const BIT_BINARY_EXT: u8 = 77;
const SMALL_INTEGER_EXT: u8 = 97;
const INTEGER_EXT: u8 = 98;
const SMALL_TUPLE_EXT: u8 = 104;
const LARGE_TUPLE_EXT: u8 = 105;
const NIL_EXT: u8 = 106;
const STRING_EXT: u8 = 107;
const LIST_EXT: u8 = 108;
const BINARY_EXT: u8 = 109;
const SMALL_BIG_EXT: u8 = 110;
const LARGE_BIG_EXT: u8 = 111;
const MAP_EXT: u8 = 116;
const ATOM_UTF8_EXT: u8 = 118;
const SMALL_ATOM_UTF8_EXT: u8 = 119;

include!("raft_command_dictionary_v1.rs");

use std::collections::HashMap;
use std::sync::OnceLock;

struct Dictionary {
    atoms: &'static [&'static str],
    terms: &'static [&'static [u8]],
    atom_index: OnceLock<HashMap<&'static [u8], u16>>,
}

impl Dictionary {
    fn atom_ref(&self, name: &[u8]) -> Option<u16> {
        self.atom_index
            .get_or_init(|| {
                self.atoms
                    .iter()
                    .enumerate()
                    .map(|(index, atom)| (atom.as_bytes(), index as u16))
                    .collect()
            })
            .get(name)
            .copied()
    }

    fn atom(&self, index: usize) -> Option<&'static [u8]> {
        self.atoms.get(index).map(|atom| atom.as_bytes())
    }
}

static V1: Dictionary = Dictionary {
    atoms: &ATOMS_V1,
    terms: &TERMS_V1,
    atom_index: OnceLock::new(),
};

fn dictionary(version: u8) -> Option<&'static Dictionary> {
    match version {
        DICTIONARY_V1 => Some(&V1),
        _ => None,
    }
}

/// The compact form of an external-term-format binary, or `None` when the
/// input uses a term the codec does not cover or the result is not smaller.
/// The result is checked to expand back to the input byte for byte.
pub fn compact(input: &[u8]) -> Option<Vec<u8>> {
    let dict = &V1;
    let body = input.strip_prefix(&[VERSION_MAGIC])?;
    let mut out = Vec::with_capacity(input.len());
    out.extend_from_slice(&[MAGIC, DICTIONARY_V1]);
    let used = compact_term(body, 0, dict, &mut out, 0)?;

    if used != body.len() || out.len() >= input.len() {
        return None;
    }

    match expand(&out) {
        Some(expanded) if expanded == input => Some(out),
        _ => None,
    }
}

/// The external-term-format bytes of a compact binary, or `None` when it is
/// malformed or names an unknown dictionary.
pub fn expand(input: &[u8]) -> Option<Vec<u8>> {
    let (&magic, rest) = input.split_first()?;
    let (&version, body) = rest.split_first()?;

    if magic != MAGIC {
        return None;
    }

    let dict = dictionary(version)?;
    let mut out = Vec::with_capacity(input.len() * 2);
    out.push(VERSION_MAGIC);
    let used = expand_term(body, 0, dict, &mut out, 0)?;

    if used == body.len() {
        Some(out)
    } else {
        None
    }
}

fn compact_term(
    input: &[u8],
    pos: usize,
    dict: &Dictionary,
    out: &mut Vec<u8>,
    depth: usize,
) -> Option<usize> {
    if depth > MAX_DEPTH {
        return None;
    }

    let rest = input.get(pos..)?;

    // A dictionary term is a complete term, so a prefix match at a term start
    // is the whole term.
    for (index, term) in dict.terms.iter().enumerate() {
        if rest.starts_with(term) {
            out.extend_from_slice(&[TERM_REF, index as u8]);
            return Some(pos + term.len());
        }
    }

    let tag = *rest.first()?;

    match tag {
        SMALL_ATOM_UTF8_EXT => {
            let len = *rest.get(1)? as usize;
            let name = rest.get(2..2 + len)?;

            match dict.atom_ref(name) {
                Some(index) if index < 256 => out.extend_from_slice(&[ATOM_REF, index as u8]),
                Some(index) => {
                    out.push(WIDE_ATOM_REF);
                    out.extend_from_slice(&index.to_be_bytes());
                }
                None => out.extend_from_slice(rest.get(..2 + len)?),
            }

            Some(pos + 2 + len)
        }
        SMALL_TUPLE_EXT => {
            let arity = *rest.get(1)? as u64;
            out.extend_from_slice(rest.get(..2)?);
            compact_many(input, pos + 2, arity, dict, out, depth)
        }
        LARGE_TUPLE_EXT => {
            let arity = u32_at(rest, 1)? as u64;
            out.extend_from_slice(rest.get(..5)?);
            compact_many(input, pos + 5, arity, dict, out, depth)
        }
        MAP_EXT => {
            let arity = u32_at(rest, 1)? as u64;
            out.extend_from_slice(rest.get(..5)?);
            compact_many(input, pos + 5, arity * 2, dict, out, depth)
        }
        LIST_EXT => {
            // The elements, then the tail.
            let len = u32_at(rest, 1)? as u64;
            out.extend_from_slice(rest.get(..5)?);
            compact_many(input, pos + 5, len + 1, dict, out, depth)
        }
        _ => {
            let size = leaf_size(rest)?;
            out.extend_from_slice(rest.get(..size)?);
            Some(pos + size)
        }
    }
}

fn compact_many(
    input: &[u8],
    mut pos: usize,
    count: u64,
    dict: &Dictionary,
    out: &mut Vec<u8>,
    depth: usize,
) -> Option<usize> {
    for _ in 0..count {
        pos = compact_term(input, pos, dict, out, depth + 1)?;
    }

    Some(pos)
}

fn expand_term(
    input: &[u8],
    pos: usize,
    dict: &Dictionary,
    out: &mut Vec<u8>,
    depth: usize,
) -> Option<usize> {
    if depth > MAX_DEPTH {
        return None;
    }

    let rest = input.get(pos..)?;
    let tag = *rest.first()?;

    match tag {
        ATOM_REF | WIDE_ATOM_REF => {
            let (index, size) = if tag == ATOM_REF {
                (*rest.get(1)? as usize, 2)
            } else {
                (u16_at(rest, 1)? as usize, 3)
            };
            let name = dict.atom(index)?;
            out.extend_from_slice(&[SMALL_ATOM_UTF8_EXT, name.len() as u8]);
            out.extend_from_slice(name);
            Some(pos + size)
        }
        TERM_REF => {
            out.extend_from_slice(dict.terms.get(*rest.get(1)? as usize)?);
            Some(pos + 2)
        }
        SMALL_ATOM_UTF8_EXT => {
            let size = 2 + *rest.get(1)? as usize;
            out.extend_from_slice(rest.get(..size)?);
            Some(pos + size)
        }
        SMALL_TUPLE_EXT => {
            let arity = *rest.get(1)? as u64;
            out.extend_from_slice(rest.get(..2)?);
            expand_many(input, pos + 2, arity, dict, out, depth)
        }
        LARGE_TUPLE_EXT => {
            let arity = u32_at(rest, 1)? as u64;
            out.extend_from_slice(rest.get(..5)?);
            expand_many(input, pos + 5, arity, dict, out, depth)
        }
        MAP_EXT => {
            let arity = u32_at(rest, 1)? as u64;
            out.extend_from_slice(rest.get(..5)?);
            expand_many(input, pos + 5, arity * 2, dict, out, depth)
        }
        LIST_EXT => {
            let len = u32_at(rest, 1)? as u64;
            out.extend_from_slice(rest.get(..5)?);
            expand_many(input, pos + 5, len + 1, dict, out, depth)
        }
        _ => {
            let size = leaf_size(rest)?;
            out.extend_from_slice(rest.get(..size)?);
            Some(pos + size)
        }
    }
}

fn expand_many(
    input: &[u8],
    mut pos: usize,
    count: u64,
    dict: &Dictionary,
    out: &mut Vec<u8>,
    depth: usize,
) -> Option<usize> {
    for _ in 0..count {
        pos = expand_term(input, pos, dict, out, depth + 1)?;
    }

    Some(pos)
}

/// The size of a term that holds no nested terms, or `None` for a tag the
/// codec does not cover (pids, ports, references, funs, old atom forms).
fn leaf_size(rest: &[u8]) -> Option<usize> {
    let size = match *rest.first()? {
        SMALL_INTEGER_EXT => 2,
        INTEGER_EXT => 5,
        NEW_FLOAT_EXT => 9,
        NIL_EXT => 1,
        SMALL_BIG_EXT => 3 + *rest.get(1)? as usize,
        LARGE_BIG_EXT => 6 + u32_at(rest, 1)? as usize,
        STRING_EXT => 3 + u16_at(rest, 1)? as usize,
        BINARY_EXT => 5 + u32_at(rest, 1)? as usize,
        BIT_BINARY_EXT => 6 + u32_at(rest, 1)? as usize,
        ATOM_UTF8_EXT => 3 + u16_at(rest, 1)? as usize,
        _ => return None,
    };

    if size <= rest.len() {
        Some(size)
    } else {
        None
    }
}

// Larger commands are mostly value bytes, and these NIFs run on normal
// schedulers, so they leave them alone.
const MAX_NIF_INPUT: usize = 64 * 1024;

#[rustler::nif]
fn raft_command_compact<'a>(
    env: rustler::Env<'a>,
    input: rustler::Binary<'a>,
) -> rustler::NifResult<rustler::Term<'a>> {
    let compacted = if input.len() <= MAX_NIF_INPUT {
        compact(input.as_slice())
    } else {
        None
    };

    nif_result(env, compacted)
}

#[rustler::nif]
fn raft_command_expand<'a>(
    env: rustler::Env<'a>,
    input: rustler::Binary<'a>,
) -> rustler::NifResult<rustler::Term<'a>> {
    let expanded = if input.len() <= MAX_NIF_INPUT {
        expand(input.as_slice())
    } else {
        None
    };

    nif_result(env, expanded)
}

fn nif_result<'a>(
    env: rustler::Env<'a>,
    bytes: Option<Vec<u8>>,
) -> rustler::NifResult<rustler::Term<'a>> {
    use rustler::Encoder;

    match bytes.and_then(|bytes| {
        let mut binary = rustler::OwnedBinary::new(bytes.len())?;
        binary.as_mut_slice().copy_from_slice(&bytes);
        Some(binary)
    }) {
        Some(binary) => Ok((crate::atoms::ok(), binary.release(env)).encode(env)),
        None => Ok(crate::atoms::error().encode(env)),
    }
}

fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn atom(name: &str) -> Vec<u8> {
        let mut bytes = vec![SMALL_ATOM_UTF8_EXT, name.len() as u8];
        bytes.extend_from_slice(name.as_bytes());
        bytes
    }

    fn binary(data: &[u8]) -> Vec<u8> {
        let mut bytes = vec![BINARY_EXT];
        bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
        bytes.extend_from_slice(data);
        bytes
    }

    fn tuple(items: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = vec![SMALL_TUPLE_EXT, items.len() as u8];
        for item in items {
            bytes.extend_from_slice(item);
        }
        bytes
    }

    fn etf(term: Vec<u8>) -> Vec<u8> {
        let mut bytes = vec![VERSION_MAGIC];
        bytes.extend(term);
        bytes
    }

    #[test]
    fn dictionary_atoms_and_terms_become_references_and_expand_exactly() {
        let input = etf(tuple(&[
            atom(ATOMS_V1[0]),
            atom("not_in_the_dictionary"),
            binary(b"key"),
            TERMS_V1[0].to_vec(),
        ]));

        let compacted = compact(&input).expect("smaller");
        assert_eq!(&compacted[..2], &[MAGIC, DICTIONARY_V1]);
        assert!(compacted.len() < input.len() - TERMS_V1[0].len());
        assert_eq!(expand(&compacted).as_deref(), Some(&input[..]));
    }

    #[test]
    fn terms_the_codec_does_not_cover_stay_plain() {
        // A pid (NEW_PID_EXT) inside a tuple.
        let pid = vec![88, 119, 1, b'n', 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(compact(&etf(tuple(&[atom(ATOMS_V1[0]), pid]))), None);
        // No dictionary hit leaves nothing to gain.
        assert_eq!(compact(&etf(tuple(&[binary(b"only data")]))), None);
        // Not an external term.
        assert_eq!(compact(b"plain bytes"), None);
    }

    #[test]
    fn deep_nesting_stays_plain() {
        let mut term = atom(ATOMS_V1[0]);
        for _ in 0..=MAX_DEPTH {
            term = tuple(&[term]);
        }
        assert_eq!(compact(&etf(term)), None);
    }

    #[test]
    fn malformed_compact_input_is_refused() {
        let input = etf(tuple(&[atom(ATOMS_V1[0]), binary(b"value")]));
        let compacted = compact(&input).expect("smaller");

        // Truncated, trailing bytes, unknown dictionary, references out of range.
        assert_eq!(expand(&compacted[..compacted.len() - 1]), None);
        assert_eq!(expand(&[compacted.clone(), vec![NIL_EXT]].concat()), None);
        assert_eq!(expand(&[MAGIC, 99, ATOM_REF, 0]), None);
        assert_eq!(
            expand(&[MAGIC, DICTIONARY_V1, WIDE_ATOM_REF, 255, 255]),
            None
        );
        assert_eq!(expand(&[MAGIC, DICTIONARY_V1, TERM_REF, 255]), None);
        assert_eq!(
            expand(&[MAGIC, DICTIONARY_V1, LIST_EXT, 255, 255, 255, 255]),
            None
        );
        assert_eq!(expand(&input), None);
    }

    #[test]
    fn atoms_past_the_first_256_take_three_byte_references() {
        let input = etf(tuple(&[atom(ATOMS_V1[300]), atom(ATOMS_V1[1])]));
        let compacted = compact(&input).expect("smaller");

        assert_eq!(
            &compacted[2..],
            &[SMALL_TUPLE_EXT, 2, WIDE_ATOM_REF, 1, 44, ATOM_REF, 1]
        );
        assert_eq!(expand(&compacted).as_deref(), Some(&input[..]));
        assert_eq!(
            expand(&[MAGIC, DICTIONARY_V1, WIDE_ATOM_REF, 255, 255]),
            None
        );
    }

    #[test]
    fn dictionary_v1_is_frozen() {
        // Replicas and replay expand old entries with this dictionary forever.
        // If this fails, the dictionary changed: restore it and add a version.
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        for atom in ATOMS_V1 {
            hasher.update([atom.as_bytes(), b"\0"].concat());
        }
        for term in TERMS_V1 {
            hasher.update([b"\x01", term].concat());
        }
        let digest = hasher
            .finalize()
            .iter()
            .fold(String::new(), |mut hex, byte| {
                use std::fmt::Write;
                let _ = write!(hex, "{byte:02x}");
                hex
            });

        assert_eq!(
            digest,
            "a4da21dcf1c6befbdc98a6456eb1c1f195f3a7907c137c9dc7fa4b455ddddb3d"
        );
    }

    #[test]
    fn dictionary_v1_fits_its_references_and_has_no_duplicates() {
        assert!(ATOMS_V1.len() > 300 && ATOMS_V1.len() <= 65_536 && TERMS_V1.len() <= 256);
        let mut names: Vec<&str> = ATOMS_V1.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ATOMS_V1.len());
        assert!(ATOMS_V1
            .iter()
            .all(|name| !name.is_empty() && name.len() < 256));
    }
}
