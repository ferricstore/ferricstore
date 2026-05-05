# Ra Fork WAL NIF Design Review

Review of `ra-fork-wal-nif-design.md`. Focus: correctness, crash safety, race conditions, and production readiness under extreme load.

---

## 1. Correctness Issues

### 1.1 Merged Flush Drops Caller Notifications (CRITICAL)

In the drain loop inside the `Flush` handler:

```rust
WalMsg::Flush { .. } => { /* merge: this flush covers everything */ }
```

The second `Flush` message's `caller` and `ref_term` are silently discarded. The Erlang process that sent that flush will never receive `{wal_flush_complete, Ref}`. The `ra_log_wal` gen_batch_server will hang forever waiting for that notification, blocking all subsequent writer notifications for that batch and all future batches.

**Fix:** Collect all Flush callers into a `Vec<(LocalPid, OwnedTerm)>` and notify every one of them after the single fdatasync. Alternatively, only the *last* Flush caller needs to be kept since earlier writes are covered by the later fsync, but all callers must be notified.

### 1.2 The Same Flush-Merging Bug Exists in Commit Delay Loop

```rust
_ => { /* handle flush/close */ }
```

Any `Flush` or `Close` messages received during the spin-wait are silently dropped with a comment placeholder. A `Close` arriving during commit delay would be ignored entirely, meaning `wal_close` blocks forever on the oneshot receiver.

### 1.3 O_DIRECT + O_DSYNC Together Is Redundant and Possibly Harmful

The design opens files with `O_DIRECT | O_DSYNC`:

```rust
.custom_flags(libc::O_DIRECT | libc::O_DSYNC)
```

`O_DSYNC` makes every `write()` call implicitly durable (equivalent to write + fdatasync). This means the explicit `fdatasync()` call afterward is redundant. Worse, it means the commit delay spin-wait is useless because each `write_all()` already blocks for the full disk flush. The entire batching architecture collapses into synchronous write-per-batch.

**Fix:** Use `O_DIRECT` alone (no `O_DSYNC`). The explicit `fdatasync()` after the batched write provides the durability guarantee. This is the whole point of the background thread: batch writes, then one fsync.

### 1.4 `write_all` With O_DIRECT May Silently Corrupt

With `O_DIRECT`, the kernel requires:
- Buffer memory must be aligned to 512 bytes (or filesystem block size, typically 4096)
- Write offset must be aligned to 512 bytes
- Write length must be a multiple of 512 bytes

`Vec<u8>` from a standard allocator is **not** guaranteed to be aligned. `write_all` with an unaligned buffer and `O_DIRECT` will return `EINVAL` on Linux. The `.unwrap()` will panic the background thread, which is an unrecoverable crash (see section 4.2).

**Fix:** Use `posix_memalign` or a custom aligned allocator for the write buffer. Pad writes to 512-byte (or 4096-byte) boundaries. Track the logical file size separately from the physical (padded) file size. On recovery, use a length prefix or sentinel to distinguish real data from padding.

### 1.5 `file_size` AtomicU64 Uses Relaxed Ordering

```rust
wal.file_size.fetch_add(wal.buffer.len() as u64, Ordering::Relaxed);
```

`Relaxed` ordering means the Erlang thread calling `wal_file_size` may see a stale value. Since `fill_ratio` drives rollover decisions, a stale file size could delay rollover, allowing the WAL to grow beyond `max_wal_size_bytes`. In practice the race window is small and the consequence is just a slightly oversized WAL file, so this is low severity. But `Release` on the store and `Acquire` on the load would be correct and cost nothing measurable on x86.

---

## 2. Race Conditions

### 2.1 Write-then-Flush Ordering Across Channel

The Erlang side does:
```erlang
wal_write_batch(Fd, Pend),    %% sends Write to channel
wal_flush(Fd, self(), Ref),   %% sends Flush to channel
```

These are two separate NIF calls. If `wal_write_batch` sends a `Write` message and then the BEAM scheduler preempts the Erlang process before `wal_flush` runs, the background thread could receive the `Write`, see no `Flush`, process other messages, and then eventually get the `Flush`. This is actually fine because crossbeam channels are FIFO and only one Erlang process (the gen_batch_server) sends to a given handle. But this depends on the invariant that only one process ever calls these NIFs on a given handle. **Document this invariant and assert it**, because if two ra systems accidentally share a handle, the interleaving would corrupt data.

### 2.2 No Backpressure on Channel

The crossbeam channel is unbounded (the design doesn't specify bounded capacity). Under extreme load (200+ connections, pipeline=100), the `gen_batch_server` can enqueue writes faster than the background thread drains them. The channel buffer grows without limit, consuming memory. If `fdatasync` stalls (e.g., NVMe thermal throttling, 50-100ms), the channel accumulates all writes from that stall window.

At 100K entries/sec with 256B entries average, a 100ms stall queues ~10K messages = ~2.5MB of data plus Vec overhead. This is manageable. But at larger value sizes (1MB values, common for caching workloads), a 100ms stall could buffer hundreds of MB.

**Fix:** Use a bounded channel. When the channel is full, `wal_write_batch` should return `{error, backpressure}` and the gen_batch_server should back-pressure callers. This is how you get flow control from disk to client.

### 2.3 Rollover Race: Close + Open Sequence

```erlang
ferricstore_wal_nif:wal_close(OldFd),   %% blocks until drain + fsync + close
NewFd = ferricstore_wal_nif:wal_open(NewFilename, PreAlloc),
```

`wal_close` is documented as blocking. This means the gen_batch_server (which runs on a normal BEAM scheduler) is blocked for the entire drain + fsync + close duration. During this time, all other ra servers writing to the WAL are stalled because the gen_batch_server cannot process their messages. This could be hundreds of milliseconds.

**Fix:** Either (a) make close asynchronous too (send `Close` and get a completion notification, like `Flush`), or (b) accept the stall and document it as an expected latency spike during rollover.

### 2.4 Thread-per-Handle: Resource Leak on Abnormal Exit

Each `wal_open` spawns a background thread. If the Erlang process crashes and the `ResourceArc<WalHandle>` is garbage collected, the `Drop` impl on `WalHandle` must send a `Close` to the background thread and join it. If `Drop` is not implemented, the background thread leaks and the file descriptor leaks. The design does not mention a `Drop` implementation.

---

## 3. Edge Cases Under Load

### 3.1 Spin-Wait Burns a Full CPU Core

```rust
while Instant::now() < deadline {
    if let Ok(msg) = rx.try_recv() { ... }
    std::hint::spin_loop();
}
```

The `spin_loop` hint tells the CPU to enter a low-power spin state, but it still burns 100% of one core for `commit_delay_us` microseconds on every flush cycle. At 3,300 flushes/sec with 200us delay, the thread spins for 660ms out of every 1000ms. On a 4-core machine, that is 25% of total CPU capacity dedicated to spinning.

**Alternative:** Use `crossbeam::channel::recv_timeout` with the commit delay as timeout. This yields the CPU to the OS scheduler. The latency cost is ~1-5us from the context switch, which is negligible compared to the 200us delay.

### 3.2 Buffer Growth Under Sustained Load

`wal.buffer` is a `Vec<u8>` that grows via `extend_from_slice` and is cleared (not shrunk) via `.clear()`. Under steady-state load, the Vec will grow to the peak batch size and stay there. This is fine and intentional. But if there is a transient spike (e.g., a large pipeline burst), the Vec allocates to accommodate it and never shrinks. Consider periodically calling `shrink_to` or reallocating if `capacity >> len * 4`.

### 3.3 OOM During Buffer Accumulation

If the system is under memory pressure and `wal.buffer.extend_from_slice(&data)` triggers an allocation failure, Rust's default allocator will abort the process (not the BEAM, the entire OS process). There is no way to handle this gracefully from Erlang. This is inherent to any NIF that allocates Rust heap memory.

**Mitigation:** Use the bounded channel (section 2.2) to limit how much data can be in-flight. Monitor `wal.buffer.len()` via a metric and alert if it grows beyond a threshold.

---

## 4. Crash Safety

### 4.1 BEAM Crash Mid-Write

If the BEAM crashes after `wal_write_batch` enqueues data but before `wal_flush`:
- The background thread has data in its buffer but no `Flush` arrives.
- The thread blocks on `rx.recv()` waiting for the next message.
- Since the BEAM is dead, the channel sender is dropped, `rx.recv()` returns `Err(RecvError)`, and the thread exits via the `Err(_) => return` arm.
- **Data in the buffer is lost.** This is correct behavior: unflushed data was never durable, so callers were never notified of success.

However: the `Err(_) => return` path does **not** flush the buffer or close the file. The OS will close the fd on process exit, but the buffered data is silently dropped. If the data was `write()`-ed to the kernel buffer but not yet `fdatasync()`-ed, the kernel may or may not flush it on process exit. This creates an ambiguous state on recovery: some entries may be partially written to disk without being fsynced.

**Fix:** In the `Err(_)` arm, call `write_all` + `sync_data` for any remaining buffer data before returning. This is best-effort (the BEAM is already dead) but makes recovery deterministic.

### 4.2 Rust Thread Panic

If `write_all` or `sync_data` returns an error (disk full, I/O error), the `.unwrap()` panics the background thread. When the thread panics:
- The channel receiver is dropped.
- The next `wal_write_batch` NIF call will `tx.send(...)` which returns `Err(SendError)` because the receiver is gone.
- If the NIF ignores this error (the design shows returning `:ok` always), the Erlang side silently loses writes with no error propagation.
- `wal_flush` will also silently fail: the caller never receives `{wal_flush_complete, Ref}`, causing the gen_batch_server to hang.

This is a cascading failure: a single disk error kills the background thread, which silently blocks the entire WAL pipeline forever.

**Fix:**
1. Replace `.unwrap()` with proper error handling. On write error, send an error notification to the Flush callers.
2. Store an `AtomicBool` "thread_alive" flag in the `WalHandle`. The background thread sets it to `false` on exit. All NIF functions check it before sending to the channel and return `{error, wal_thread_dead}` if it is false.
3. Consider restarting the background thread on recoverable errors (ENOSPC is not recoverable, but EIO on a single write might be).

### 4.3 NVMe Write Error Semantics

On NVMe, `write()` with `O_DIRECT` can fail with `EIO` if the device rejects the command (media error, etc.). The current design's `.unwrap()` panics on any error. Even with proper error handling, the question is: what state is the file in after a failed `write_all`? With `O_DIRECT`, a failed write may have partially transferred data. The file offset is advanced by however much was written before the error. On recovery, this partial write would be present in the file.

**Mitigation:** The WAL record format must include a length prefix and checksum per entry (or per batch) so that recovery can detect and discard partial writes. The design's `wal_recover` function must handle this, but the design does not specify the on-disk record format at all (see section 11).

### 4.4 File Descriptor Leak on Failed Open

`open_wal_file` calls `fallocate` after opening the file. If `fallocate` fails (ENOSPC), the file is open but the error is not handled (return type uses `?` but the NIF function signature returns `ResourceArc<WalHandle>`, not `Result`). This needs error handling that closes the fd on `fallocate` failure.

---

## 5. Recovery Correctness

### 5.1 No On-Disk Format Specified

The design specifies `wal_recover` returns `Vec<(u64, u64, Binary)>` (index, term, entry binary), but does not define:
- How entries are framed on disk (length prefix? sentinel? fixed-size headers?)
- How checksums are stored per entry
- How the end of valid data is detected after a crash
- How partial writes are distinguished from valid data

This is the most important part of a WAL design and it is completely absent. Without a defined format, recovery is guesswork.

**Required:** Define a binary record format. Minimum viable: `[u32 length][u64 index][u64 term][u32 crc32][payload bytes]`. Recovery reads records sequentially, validates CRC, and stops at the first invalid/incomplete record.

### 5.2 O_DIRECT Torn Write Boundaries

With `O_DIRECT`, writes bypass the page cache and go directly to the device. NVMe drives guarantee atomicity at the 512-byte sector level. A write of N sectors where power fails mid-write can result in the first K sectors being written and the remaining N-K sectors containing old data (or zeros if the file was freshly fallocated).

This means a single `write_all` of a large buffer can be torn at any 512-byte boundary. Recovery must handle:
- A record whose header was written but payload was not
- A record whose payload was partially written
- A record whose CRC was not written (appears valid but has wrong checksum)

The design does not address any of these scenarios.

### 5.3 fallocate Creates a File Full of Zeros

After `fallocate(fd, 0, 0, 256MB)`, the file contains 256MB of zeros. On recovery, `wal_recover` must distinguish between:
- Valid entry data
- Zero-filled preallocated space
- Partially written data from a crash

A zero-byte or zero-length-prefix would naturally terminate parsing, but this must be explicitly handled and tested.

### 5.4 Recovery Yielding via `enif_schedule_nif`

The design mentions using `enif_schedule_nif` for yielding during recovery of large files. This is correct for long-running NIFs, but Rustler does not expose `enif_schedule_nif` in its safe API. Implementing this requires unsafe FFI calls to the raw `enif_*` functions. This is non-trivial and error-prone. The alternative is to recover in chunks, returning control to Erlang between chunks (e.g., return `{:more, partial_results, continuation}` and let Erlang call back).

---

## 6. O_DIRECT Pitfalls

### 6.1 Buffer Alignment (CRITICAL, repeated from 1.4)

`O_DIRECT` on Linux requires the user-space buffer address to be aligned to the filesystem's logical block size (typically 512 or 4096 bytes). `Vec<u8>::new()` uses the global allocator which provides 8-byte alignment (or 16 on some platforms). This is insufficient.

**Fix:** Use `std::alloc::Layout::from_size_align(capacity, 4096)` with `std::alloc::alloc` to get a 4096-aligned buffer, or use a crate like `aligned-vec`.

### 6.2 Write Size Alignment

Write size must be a multiple of the logical block size. The accumulated buffer from `extend_from_slice` will generally not be a multiple of 512. The `write_all` call will fail with `EINVAL`.

**Fix:** Pad the buffer to the next 512-byte (or 4096-byte) boundary before writing. Use a sentinel or length field in the record format to indicate actual data length.

### 6.3 File Offset Alignment

With `O_DIRECT`, the file offset for each write must also be aligned. Since writes are append-only and the first write starts at offset 0 (aligned), subsequent writes start at offset = sum of previous write sizes. If all write sizes are padded to 4096-byte alignment, offsets remain aligned. But if any write has an unaligned size, all subsequent writes fail.

### 6.4 Interaction With fallocate

`fallocate` with `O_DIRECT` is fine, but `ftruncate` is not a substitute. The design uses `fallocate`, which is correct.

### 6.5 macOS Fallback Path

macOS has no `O_DIRECT`. The `fcntl(F_NOCACHE)` flag provides similar behavior but with different semantics (it is advisory, not mandatory). The macOS fallback opens a plain file without `F_NOCACHE`, which means page cache pollution on large WAL files. This is acceptable for development but should be documented as a performance difference.

---

## 7. io_uring Pitfalls

### 7.1 Write-then-Fsync Ordering

```rust
ring.submission().push(&write_e).unwrap();
ring.submission().push(&sync_e).unwrap();
ring.submit_and_wait(2).unwrap();
```

io_uring does **not** guarantee ordering of completions. The write and fsync are submitted together, but the fsync could complete before the write if the kernel reorders them. However, since `IORING_OP_FSYNC` syncs all previously submitted writes to the same fd, and both are submitted in the same `submit_and_wait` call, the kernel will process them in order for the same fd. This is correct as written, but fragile.

**Safer alternative:** Use `IOSQE_IO_LINK` flag on the write SQE to chain write -> fsync, guaranteeing sequential execution:

```rust
let write_e = opcode::Write::new(...)
    .build()
    .flags(squeue::Flags::IO_LINK)  // chain to next SQE
    .user_data(1);
```

### 7.2 `submit_and_wait(2)` Blocks the Thread

`submit_and_wait(2)` blocks until both completions arrive. This is fine for the background thread (it has nothing else to do while waiting for fsync), but it means the thread cannot process new `Write` messages during the fsync. With the current non-io_uring path, this is also true (sync `sync_data()` blocks), so there is no regression. But io_uring's value is in async completion. A more advanced design would submit the fsync, then continue draining writes from the channel, and poll for completion separately.

### 7.3 Kernel Version Requirements

`io_uring` was introduced in Linux 5.1. `IORING_OP_FSYNC` was added in 5.1. But there are known bugs in early io_uring implementations:
- Linux 5.1-5.3: various io_uring bugs
- Linux 5.4: first stable release
- Linux 5.6: adds `io_uring_register` for registered buffers

**Minimum:** Require Linux 5.10+ (LTS) for io_uring support. Check kernel version at runtime before enabling.

### 7.4 Error Handling on Completion

The design's io_uring code does `.unwrap()` on `submit_and_wait`. If the kernel returns an error on the completion queue entry (CQE), the `res` field is negative (errno). The code must check `cqe.result()` for each completion and handle errors. A negative result on the write CQE means the write failed; a negative result on the fsync CQE means the sync failed (data may not be durable).

### 7.5 Registered Buffers for O_DIRECT

io_uring supports pre-registered buffers (`io_uring_register_buffers`) that avoid per-I/O buffer mapping in the kernel. For `O_DIRECT` writes, this eliminates the page pinning overhead. The design should use registered buffers for the write buffer once io_uring is implemented.

---

## 8. Memory Management

### 8.1 Binary Ownership at NIF Boundary

`wal_write_batch` receives `entries: Term` which is iodata (list of binaries). The NIF must extract the binary data within the NIF call's lifetime because `Term` references are only valid for the duration of the NIF call. The design says "The NIF copies the iodata into a contiguous buffer," which is correct. But the copy is expensive for large values.

For ra's WAL, entries are pre-serialized via `term_to_iovec` which produces iodata containing refc binaries. These binaries live on the BEAM's shared binary heap. The NIF copies from the refc binary into a Rust `Vec<u8>`, then sends that Vec to the background thread. The refc binary's refcount was incremented when the Erlang process received it and is decremented when the NIF returns (the Term goes out of scope). This is correct but means every WAL write involves a full memcpy of the entry data.

**Optimization opportunity:** Instead of copying the binary data, the NIF could increment the refc binary's refcount (via `enif_keep_resource` or by creating a Rustler `Binary` that holds a reference) and send the reference to the background thread. The background thread would use `writev` (scatter-gather I/O) to write directly from the binary data's memory location. This avoids the copy entirely. However, this is complex and requires careful lifecycle management to ensure the binary is not garbage collected while the background thread holds a pointer to it. May not be worth the complexity for sub-1KB entries, but significant for large values.

### 8.2 OwnedTerm for Ref

`wal_flush` receives `ref_term: Term` which is the Erlang reference used for the completion notification. This `Term` is only valid during the NIF call. To send it from the background thread later, it must be copied into an `OwnedEnv`. The design shows `ref_term: OwnedTerm` in `WalMsg::Flush`, which implies this conversion happens, but the code does not show it explicitly.

The correct pattern:
```rust
fn wal_flush(env: Env, handle: ResourceArc<WalHandle>, caller_pid: LocalPid, ref_term: Term) {
    let owned_env = OwnedEnv::new();
    let saved_ref = owned_env.save(ref_term);
    handle.tx.send(WalMsg::Flush { caller: caller_pid, ref_env: owned_env, ref_term: saved_ref });
}
```

Then in the background thread:
```rust
owned_env.send_and_clear(&caller, |env| {
    let ref_term = saved_ref.load(env);
    (atoms::wal_flush_complete(), ref_term).encode(env)
});
```

If this lifecycle is wrong (e.g., using `ref_term` after the NIF returns without `OwnedEnv`), the background thread reads freed memory. This is a use-after-free. Rustler's type system should prevent this at compile time if `Term<'a>` lifetimes are respected, but `OwnedTerm` (if that is a type being used) may have different guarantees depending on the Rustler version.

### 8.3 ResourceArc Lifecycle

`ResourceArc<WalHandle>` is reference-counted across the NIF boundary. When the Erlang term holding the resource is garbage collected, the Rust `Drop` implementation runs. But `Drop` runs on the BEAM's GC thread (a normal scheduler), and it must not block. If `Drop` sends a `Close` message and waits for the oneshot response (like `wal_close` does), it blocks a BEAM scheduler during GC. This is a violation of the "no blocking on normal schedulers" constraint.

**Fix:** `Drop` should send the `Close` message but **not** wait for the response. The background thread will close the file and exit; the `Drop` handler just fires and forgets. Alternatively, require explicit `wal_close` calls and make `Drop` a no-op (or a best-effort non-blocking shutdown).

### 8.4 Channel Message Size

Each `WalMsg::Write { data: Vec<u8> }` is at minimum 24 bytes (Vec header) plus the heap allocation for data. With thousands of small writes per second, the allocation pressure from creating and dropping Vecs is significant. Consider a slab allocator or reusable buffer pool for the `data` field.

---

## 9. Rust-Specific Concerns

### 9.1 `send_flush_complete` Must Use `OwnedEnv`

The background thread is not a BEAM-managed thread. It cannot call `enif_send` directly without an environment. Rustler provides `OwnedEnv::send_and_clear` for this purpose. The design's `send_flush_complete(caller, ref_term)` is pseudocode; the actual implementation must use `OwnedEnv`. If implemented incorrectly (e.g., trying to create an `Env` on a non-scheduler thread), it will segfault.

### 9.2 Panic Safety

If the background thread panics (e.g., from `.unwrap()`), the `std::thread::JoinHandle` captures the panic. But the NIF side does not check for panics. The channel's `Sender` is still held in the `WalHandle`, so `tx.send()` will return `Ok(())` until the receiver is dropped (which happens when the thread's stack unwinds). After that, `tx.send()` returns `Err`, but the NIF ignores the return value.

**Fix:** Use `catch_unwind` at the top of `wal_thread_loop` to catch panics. On panic, set an error flag in shared state, close the file, and exit cleanly.

### 9.3 Thread Name

Set the background thread's name via `std::thread::Builder::new().name("ferricstore-wal".into()).spawn(...)` for debuggability. Unnamed threads are hard to identify in `top`, `htop`, and crash dumps.

### 9.4 `LocalPid` Send Safety

`LocalPid` in Rustler implements `Send`, which is required to move it to the background thread. This is correct as of Rustler 0.37. Verify this does not change in future versions.

---

## 10. Performance Concerns

### 10.1 Double Copy of Entry Data

The current path:
1. Erlang `term_to_iovec` produces iodata (refc binary on shared heap)
2. NIF `wal_write_batch` copies iodata into a Rust `Vec<u8>`
3. Background thread copies from `Vec<u8>` into `wal.buffer` via `extend_from_slice`
4. `write_all` copies from `wal.buffer` to kernel (or DMA with O_DIRECT)

Steps 2 and 3 are two copies in Rust land. Step 2 could be eliminated if the NIF directly appends to a shared buffer (but that requires locking). Step 3 could be eliminated by sending a `&[u8]` reference instead of an owned `Vec<u8>`, but that requires lifetime management across threads.

**Practical fix:** In `wal_write_batch`, instead of creating a new `Vec<u8>` per call and sending it through the channel, write directly into a shared buffer protected by a mutex. The flush path locks the mutex, takes ownership of the buffer, and replaces it with a fresh one (swap pattern). This eliminates the channel overhead and one copy.

Alternatively, accept the two copies. For 256-byte entries at 100K entries/sec, the total copy bandwidth is ~50MB/s, well within memory bandwidth. Only revisit if profiling shows memcpy as a bottleneck.

### 10.2 Channel Overhead vs Mutex

Crossbeam unbounded channel allocates one heap node per message. At 100K writes/sec, that is 100K allocations/sec. A mutex-protected buffer swap would be one lock/unlock per flush cycle (~3,300/sec). The mutex approach has ~30x fewer allocations. But the channel is wait-free for the sender (the NIF), while a mutex could block the NIF if the background thread holds it. In practice the mutex is held for a `swap()` (nanoseconds), so contention is negligible.

### 10.3 Commit Delay Value

200us default commit delay with ~100us NVMe fdatasync = 300us per flush cycle = ~3,300 flushes/sec. This is the throughput ceiling per WAL. With 50 entries per flush, that is 165K entries/sec (matches the design's estimate). But if entry arrival rate drops below ~3,300/sec, every flush waits the full 200us for nothing. The commit delay should be adaptive: skip the delay if the channel has no pending messages after draining (i.e., only delay if there is reason to believe more writes are coming).

### 10.4 The gen_batch_server Already Batches

`ra_log_wal` is a `gen_batch_server` which collects multiple messages into one `handle_batch` call. The `flush_pending` + `complete_batch` sequence already represents a batch of entries. Adding a 200us commit delay in Rust on top of gen_batch_server's inherent batching may over-batch, increasing latency without proportional throughput gain. Measure the actual entries-per-flush with and without commit delay to validate.

---

## 11. Missing NIF Functions / Design Gaps

### 11.1 No WAL Record Format

The most critical gap. The design specifies NIF functions but not the on-disk byte layout. Without a record format, `wal_recover` cannot be implemented. The format must specify:
- Record header: magic byte, length, index, term, CRC
- Alignment/padding rules for O_DIRECT
- How to detect end-of-data vs. corruption vs. preallocated zeros
- Byte order (little-endian recommended for NVMe/x86)

### 11.2 No `wal_truncate` or `wal_trim`

Ra performs WAL truncation after segments are written. The current Erlang WAL just deletes the file. If the Rust WAL needs to support partial truncation (trim entries before an index), there is no NIF for it. This may not be needed if ra's existing file-deletion-based cleanup is preserved.

### 11.3 No Error Propagation

All NIF functions return `:ok` or a value. There is no error return path. Disk errors, channel disconnection, thread death: all are silent. Every NIF should return `{:ok, result} | {:error, reason}` so the Erlang side can crash/recover appropriately.

### 11.4 No Metrics Export

For production operation, the WAL should expose:
- Current buffer size (bytes pending write)
- Channel depth (messages pending)
- Flush count, bytes written, fdatasync count
- Fdatasync latency histogram
- Entries per flush histogram

These could be a single `wal_stats` NIF returning a map, or individual counters via atomics.

### 11.5 No `wal_sync_close` vs `wal_async_close`

The design has one `wal_close` that blocks. During normal rollover, blocking is acceptable. But during BEAM shutdown, if multiple WAL handles need closing, they close sequentially. Consider an async close variant for shutdown.

### 11.6 Missing `commit_delay_us` in Open

The design says "The commit_delay is passed to the NIF at `wal_open` time" but the `wal_open` signature is `fn wal_open(path: String, pre_allocate_bytes: u64) -> ResourceArc<WalHandle>` with no commit_delay parameter.

### 11.7 Pre-serialization Pass-Through Not Shown for Write Path

The design in section 1 shows the `{ttb, _}` pass-through in `ra_log.erl`, and the optimization plan describes pre-serialization in the Batcher. But the design does not show how the pre-serialized binary flows through `write_data` in `ra_log_wal.erl` to the NIF. Specifically, `write_data` computes checksums and formats records. If the data is `{ttb, Bin}`, it must preserve the binary as-is (no re-serialization), but the record framing (header, length, checksum) must still be applied. Make sure the Erlang-side record formatting is compatible with the Rust-side recovery parser.

---

## 12. Test Plan

### 12.1 Correctness Tests

| Test | What it validates |
|------|-------------------|
| **Write-read round-trip** | Write entries via NIF, close, recover via `wal_recover`, verify all entries match |
| **Flush notification delivery** | Every `wal_flush` caller receives exactly one `{wal_flush_complete, Ref}` message |
| **Multiple concurrent flushes** | Two `wal_flush` calls in sequence: both callers get notified (tests the merged-flush bug from 1.1) |
| **Close drains and syncs** | Write entries, close without explicit flush, recover: all entries present |
| **Rollover** | Write to fill ratio, rollover to new file, verify old file is durable and new file accepts writes |
| **File size tracking** | `wal_file_size` returns correct value after writes (within eventual-consistency window) |
| **Empty WAL recovery** | Recover from a freshly-created WAL file: returns empty list |
| **Large entry** | Write a single entry larger than 1MB, recover it correctly |

### 12.2 Crash Recovery Tests

| Test | What it validates |
|------|-------------------|
| **Kill during write** | Start writing a large batch, kill the BEAM process (`:erlang.halt(1)`), recover: only fully-fsynced entries are present |
| **Kill during fsync** | Requires simulation: inject a delay in fdatasync, kill during the delay. Recovery should contain data from all prior synced batches but none from the in-flight batch. Hard to test without fault injection. |
| **Partial write on disk** | Manually craft a WAL file with a valid entry followed by a truncated entry (simulating torn write). `wal_recover` returns only the valid entry. |
| **Zero-filled tail** | WAL file with valid entries followed by zeros (from fallocate). Recovery stops at the zero boundary. |
| **Corrupt checksum** | Valid-looking entry with wrong CRC. Recovery stops before the corrupt entry. |
| **Power-loss simulation** | On Linux: use `dm-flakey` device mapper target to simulate power loss during write. Verify recovery correctness. This is the gold standard for WAL testing. |

### 12.3 Concurrency / Stress Tests

| Test | What it validates |
|------|-------------------|
| **High-rate writes** | 100K writes/sec sustained for 60s, verify no data loss (all flushed entries recoverable) |
| **Backpressure behavior** | Inject a slow fdatasync (mock), verify channel does not grow unboundedly, verify callers are eventually unblocked |
| **Rollover under load** | Continuous writes while rollover happens, verify no entries lost during transition |
| **Thread panic recovery** | Force a panic in the background thread (e.g., mock I/O error), verify the NIF returns errors and the gen_batch_server handles them |
| **Multiple concurrent readers** | `wal_file_size` called from multiple processes while writes are happening: no crashes, reasonable values |
| **Handle leak** | Open a WAL, drop the Erlang reference (let GC collect it), verify the background thread exits and the fd is closed (check with `/proc/PID/fd` on Linux) |

### 12.4 O_DIRECT-Specific Tests (Linux Only)

| Test | What it validates |
|------|-------------------|
| **Alignment enforcement** | Verify all writes are aligned (buffer address, write size, file offset) |
| **Unaligned write rejection** | If alignment is wrong, verify a clear error (not a panic or silent corruption) |
| **ext4 vs XFS** | Run the full suite on both filesystems: behavior may differ for O_DIRECT edge cases |
| **fallocate on full disk** | Open WAL on a nearly-full filesystem, verify graceful error |

### 12.5 io_uring-Specific Tests (Linux Only)

| Test | What it validates |
|------|-------------------|
| **Kernel version check** | On kernels < 5.10, io_uring is disabled and fallback is used |
| **Write + fsync correctness** | Same as non-io_uring tests, but exercising the io_uring code path |
| **CQE error handling** | Inject an error (e.g., bad fd), verify the error is propagated, not panicked |
| **Registered buffers** | If used: verify buffer registration/deregistration lifecycle |

### 12.6 Integration Tests With ra

| Test | What it validates |
|------|-------------------|
| **ra smoke test** | Start a 3-node ra cluster with the NIF WAL, write 10K entries, read them back, kill a node, restart, verify consistency |
| **Shard rollover under Raft load** | Write continuously to all shards, trigger WAL rollover, verify no Raft leadership elections or term bumps caused by WAL stalls |
| **Concurrent quorum writes** | Write continuously across multiple key prefixes, verify quorum writes are durable and no prefix blocks unrelated quorum batches |

### 12.7 Performance Benchmarks

| Benchmark | What it measures |
|-----------|-----------------|
| **NIF overhead** | Time `wal_write_batch` and `wal_flush` NIF calls in isolation (should be <1us for write, <1us for flush submission) |
| **Entries per fsync** | Histogram of how many entries each fdatasync covers (target: 10-50) |
| **fdatasync latency** | Histogram of fdatasync wall-clock time (target: <200us on NVMe) |
| **End-to-end throughput** | memtier benchmark with pipeline=100, 200 connections, quorum writes (target: 50K-100K ops/sec on NVMe) |
| **Commit delay tuning** | Sweep commit_delay from 0 to 1000us, plot throughput and p99 latency |
| **Recovery time** | Time to recover a full 256MB WAL file (target: <1s) |

---

## Summary of Severity

| # | Issue | Severity |
|---|-------|----------|
| 1.1 | Merged flush drops caller notifications | **CRITICAL** - causes gen_batch_server hang |
| 1.2 | Commit delay loop drops flush/close | **CRITICAL** - same hang + close deadlock |
| 1.3 | O_DIRECT + O_DSYNC eliminates batching benefit | **HIGH** - defeats the entire design's purpose |
| 1.4 | O_DIRECT buffer alignment | **HIGH** - immediate EINVAL panic on Linux |
| 4.2 | Thread panic cascading failure | **HIGH** - unrecoverable silent data loss |
| 6.2 | Write size alignment for O_DIRECT | **HIGH** - immediate EINVAL panic on Linux |
| 5.1 | No on-disk record format | **HIGH** - recovery cannot be implemented |
| 11.3 | No error propagation from NIFs | **HIGH** - all failures are silent |
| 8.3 | ResourceArc Drop blocks BEAM scheduler | **MEDIUM** - violates scheduler constraint |
| 2.2 | Unbounded channel, no backpressure | **MEDIUM** - OOM under sustained load spike |
| 2.3 | Blocking close during rollover | **MEDIUM** - latency spike for all shards |
| 3.1 | Spin-wait burns full CPU core | **MEDIUM** - 25% CPU wasted on spinning |
| 11.6 | commit_delay_us missing from wal_open signature | **LOW** - obvious oversight |
| 1.5 | Relaxed ordering on file_size atomic | **LOW** - slightly stale rollover decisions |
