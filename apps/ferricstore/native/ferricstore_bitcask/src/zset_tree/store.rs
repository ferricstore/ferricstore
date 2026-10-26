//! The tree file: opens without following symlinks, header slots, page reads.
//! Each call opens the file, uses it and drops it; nothing is cached.
//!
//! Writers hold the file's exclusive lock (`flock`) while they write. A
//! compaction builds the new file beside the old one without it, then takes it
//! to copy the frames written meanwhile and put the new file in place; a
//! writer that waited finds the path names another file and opens that one.
//! Readers take no lock: frames are only appended, and a reader of a file
//! that was replaced keeps reading the old one.

use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::os::unix::io::AsRawFd;
use std::path::Path;

use super::format::{
    HeaderSlot, Token, Trailer, HEADER_REGION, PAGE_SIZE, SLOT_SIZE, TRAILER_SIZE,
};
use super::node::{decode_overflow, OVERFLOW_PAYLOAD};
use super::Result;

/// The tree as of one frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreeState {
    pub score_root: u64,
    pub member_root: u64,
    pub count: u64,
    pub token: Token,
    pub frame_end: u64,
    pub generation: u64,
    pub synced_frame_end: u64,
}

impl TreeState {
    fn empty(slot: HeaderSlot) -> Self {
        Self {
            score_root: 0,
            member_root: 0,
            count: 0,
            token: Token::default(),
            frame_end: HEADER_REGION,
            generation: slot.generation,
            synced_frame_end: slot.synced_frame_end,
        }
    }
}

pub struct TreeFile {
    file: File,
    len: std::cell::Cell<u64>,
}

fn io_error(context: &str, error: &std::io::Error) -> String {
    format!("zset tree: {context}: {error}")
}

/// Takes the exclusive lock of `file`, released when it is closed.
fn lock(file: &File) -> Result<()> {
    loop {
        // SAFETY: flock on a descriptor this function borrows for the call.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(io_error("lock", &error));
        }
    }
}

/// Whether `path` names the open `file`.
fn names(path: &Path, file: &File) -> Result<bool> {
    let open = file.metadata().map_err(|error| io_error("stat", &error))?;
    match std::fs::symlink_metadata(path) {
        Ok(at_path) => Ok(at_path.dev() == open.dev() && at_path.ino() == open.ino()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("stat", &error)),
    }
}

impl TreeFile {
    /// Opens the file for reading; `None` when it does not exist.
    pub fn open_read(path: &Path) -> Result<Option<Self>> {
        match crate::path_open::open_file_nofollow(path, libc::O_RDONLY, 0) {
            Ok(file) => Self::from_file(file).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error("open", &error)),
        }
    }

    /// Opens an existing file for writing, holding its lock; `None` when it
    /// does not exist.
    pub fn open_existing(path: &Path) -> Result<Option<Self>> {
        Self::open_locked(path, libc::O_RDWR)
    }

    /// Opens the file for reading, holding its lock, so no writer changes it
    /// meanwhile; `None` when it does not exist.
    pub fn open_read_locked(path: &Path) -> Result<Option<Self>> {
        Self::open_locked(path, libc::O_RDONLY)
    }

    fn open_locked(path: &Path, flags: libc::c_int) -> Result<Option<Self>> {
        loop {
            let file = match crate::path_open::open_file_nofollow(path, flags, 0o644) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(io_error("open", &error)),
            };
            lock(&file)?;
            if names(path, &file)? {
                return Self::from_file(file).map(Some);
            }
        }
    }

    /// Opens the file for writing, holding its lock, and creates it with an
    /// empty tree.
    pub fn open_write(path: &Path) -> Result<Self> {
        let tree = loop {
            let file =
                crate::path_open::open_file_nofollow(path, libc::O_RDWR | libc::O_CREAT, 0o644)
                    .map_err(|error| io_error("open", &error))?;
            lock(&file)?;
            if names(path, &file)? {
                break Self::from_file(file)?;
            }
        };
        if tree.len() == 0 {
            let mut header = vec![0_u8; HEADER_REGION as usize];
            header[..SLOT_SIZE].copy_from_slice(&HeaderSlot::empty().encode());
            tree.write_at(0, &header)?;
            tree.note_appended(HEADER_REGION);
        }
        Ok(tree)
    }

    /// Whether `path` still names this file (a compaction may have replaced it).
    pub fn is_at(&self, path: &Path) -> Result<bool> {
        names(path, &self.file)
    }

    fn from_file(file: File) -> Result<Self> {
        let len = file
            .metadata()
            .map_err(|error| io_error("stat", &error))?
            .len();
        Ok(Self {
            file,
            len: std::cell::Cell::new(len),
        })
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.len.get()
    }

    pub fn read_exact(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if offset
            .checked_add(len as u64)
            .map_or(true, |end| end > self.len())
        {
            return Err(format!(
                "zset tree: read of {len} bytes at {offset} past the end ({})",
                self.len()
            ));
        }
        let mut buffer = vec![0_u8; len];
        self.file
            .read_exact_at(&mut buffer, offset)
            .map_err(|error| io_error("read", &error))?;
        Ok(buffer)
    }

    pub fn read_page(&self, offset: u64) -> Result<Vec<u8>> {
        if offset < HEADER_REGION {
            return Err(format!("zset tree: page offset {offset} inside the header"));
        }
        self.read_exact(offset, PAGE_SIZE)
    }

    pub fn read_overflow(&self, head: u64, len: usize) -> Result<Vec<u8>> {
        let mut bytes = Vec::with_capacity(len);
        let mut next = head;
        let max_pages = len.div_ceil(OVERFLOW_PAYLOAD);
        for _ in 0..max_pages {
            let page = self.read_page(next)?;
            let (following, chunk) = decode_overflow(&page)?;
            bytes.extend_from_slice(chunk);
            next = following;
            if next == 0 {
                break;
            }
        }
        if bytes.len() != len || next != 0 {
            return Err("zset tree: overflow chain length mismatch".into());
        }
        Ok(bytes)
    }

    pub fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        self.file
            .write_all_at(bytes, offset)
            .map_err(|error| io_error("write", &error))
    }

    pub fn set_len(&self, len: u64) -> Result<()> {
        self.file
            .set_len(len)
            .map_err(|error| io_error("truncate", &error))?;
        self.len.set(len);
        Ok(())
    }

    pub fn note_appended(&self, end: u64) {
        self.len.set(self.len.get().max(end));
    }

    pub fn sync(&self) -> Result<()> {
        self.file
            .sync_data()
            .map_err(|error| io_error("fsync", &error))
    }

    /// Both slots, each `None` when torn, stale-format or never written.
    pub fn slots(&self) -> Result<[Option<HeaderSlot>; 2]> {
        let bytes = self.read_exact(0, 2 * SLOT_SIZE)?;
        Ok([
            HeaderSlot::decode(&bytes[..SLOT_SIZE]),
            HeaderSlot::decode(&bytes[SLOT_SIZE..]),
        ])
    }

    pub fn write_slot(&self, slot: &HeaderSlot) -> Result<()> {
        self.write_at(slot.offset(), &slot.encode())
    }

    /// The latest consistent tree: the newest valid slot whose latest frame
    /// has a valid trailer. A slot being rewritten fails its checksum and the
    /// other slot, one frame older, is used.
    pub fn state(&self) -> Result<TreeState> {
        let mut slots: Vec<HeaderSlot> = self.slots()?.into_iter().flatten().collect();
        slots.sort_by_key(|slot| std::cmp::Reverse(slot.generation));
        for slot in slots {
            if slot.latest_frame_end == HEADER_REGION {
                return Ok(TreeState::empty(slot));
            }
            if let Some(trailer) = self.trailer_ending_at(slot.latest_frame_end)? {
                return Ok(TreeState {
                    score_root: trailer.score_root,
                    member_root: trailer.member_root,
                    count: trailer.count,
                    token: trailer.token,
                    frame_end: slot.latest_frame_end,
                    generation: slot.generation,
                    synced_frame_end: slot.synced_frame_end,
                });
            }
        }
        Err("zset tree: no header names a valid frame; the file needs recovery".into())
    }

    pub fn trailer_ending_at(&self, frame_end: u64) -> Result<Option<Trailer>> {
        if frame_end > self.len() || frame_end < HEADER_REGION + TRAILER_SIZE as u64 {
            return Ok(None);
        }
        let bytes = self.read_exact(frame_end - TRAILER_SIZE as u64, TRAILER_SIZE)?;
        Ok(Trailer::decode(&bytes).filter(|trailer| trailer.frame_start < frame_end))
    }
}
