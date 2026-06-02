defmodule :ferricstore_wal_nif do
  @moduledoc """
  Rust NIF append-log I/O module.

  Replaces file:write and file:datasync with a Rust background thread
  that handles O_DIRECT, commit_delay batching, and fdatasync.

  All NIF functions run on normal BEAM schedulers (<1μs each).
  The blocking I/O runs on a dedicated Rust OS thread.

  This module is registered as an Erlang atom `:ferricstore_wal_nif`
  so the WARaft segment log can call it from Erlang.
  """

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :ferricstore,
    crate: "ferricstore_wal_nif",
    base_url: "https://github.com/ferricstore/ferricstore/releases/download/v#{version}",
    version: version,
    nif_versions: ["2.16"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-musl
    )

  @doc "Open a WAL file. Spawns background I/O thread."
  def open(_path, _commit_delay_us, _pre_allocate_bytes, _max_buffer_bytes),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc "Open a raw append file with no header offset. Used by WARaft segment logs."
  def open_raw_append(_path, _commit_delay_us, _max_buffer_bytes, _start_offset),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc "Write pre-formatted iodata to the WAL buffer. Does NOT write to disk."
  def write(_handle, _iodata), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Request async fdatasync. Sends {wal_sync_complete, Ref, synced_position} on completion."
  def sync(_handle, _caller_pid, _ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Request async fdatasync with a per-sync adaptive delay."
  def sync_with_delay(_handle, _caller_pid, _ref, _delay_us),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc "Close the WAL file. Blocks until drain + sync + close."
  def close(_handle), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Returns current logical file size in bytes."
  def position(_handle), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Read bytes from WAL at offset. Used during recovery."
  def pread(_handle, _offset, _len), do: :erlang.nif_error(:nif_not_loaded)

  @doc "Reserve file space without extending logical length. Used by WARaft segment logs."
  def preallocate_keep_size(_path, _bytes), do: :erlang.nif_error(:nif_not_loaded)
end
