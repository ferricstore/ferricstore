defmodule Ferricstore.Bitcask.NIF do
  @moduledoc "Rustler NIF bindings for Bitcask record I/O, hint files, and mmap-backed probabilistic data structure file operations."

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :ferricstore,
    crate: "ferricstore_bitcask",
    base_url: "https://github.com/yoavgeva/ferricstore/releases/download/v#{version}",
    version: version,
    nif_versions: ["2.16"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-musl
    ),
    force_build: System.get_env("FERRICSTORE_BUILD") in ["1", "true"]

  # -- Tracking allocator --
  @spec rust_allocated_bytes() :: {:ok, non_neg_integer()} | {:error, term()}
  def rust_allocated_bytes, do: :erlang.nif_error(:nif_not_loaded)

  # -- v2 Pure stateless NIFs (no Store resource, no Mutex) --
  @spec v2_append_record(binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def v2_append_record(_path, _key, _value, _expire_at_ms), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_tombstone(binary(), binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def v2_append_tombstone(_path, _key), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_batch(binary(), [{binary(), binary(), non_neg_integer()}]) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()}
  def v2_append_batch(_path, _records), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_at(binary(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def v2_pread_at(_path, _offset), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_file(binary()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()}]}
          | {:error, term()}
  def v2_scan_file(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_file_from_offset(binary(), non_neg_integer()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()}]}
          | {:error, term()}
  def v2_scan_file_from_offset(_path, _start_offset), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_tombstones(binary()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()}]}
          | {:error, term()}
  def v2_scan_tombstones(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch(binary(), [non_neg_integer()]) :: {:ok, [binary()]} | {:error, term()}
  def v2_pread_batch(_path, _locations), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_fsync(binary()) :: :ok | {:error, term()}
  def v2_fsync(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Fsyncs a directory so that recent `File.rename/2`, `File.rm/1`,
  `File.touch!/1`, or `File.create/1` operations on files inside it are
  durable. Use after any namespace mutation that must survive a kernel
  panic — rotation, compaction rename, hint-file creation, prob-file
  create/delete, shard init.

  Returns `:ok` on success or `{:error, reason}` where reason is a
  short string suitable for logging.

  POSIX: file-data fsync does NOT make the filename durable; only a
  directory fsync does.
  """
  @spec v2_fsync_dir(binary()) :: :ok | {:error, term()}
  def v2_fsync_dir(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_available_disk_space(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def v2_available_disk_space(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_write_hint_file(binary(), [
          {binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
        ]) :: :ok | {:error, term()}
  def v2_write_hint_file(_path, _entries), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_read_hint_file(binary()) ::
          {:ok,
           [
             {binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
              non_neg_integer()}
           ]}
          | {:error, term()}
  def v2_read_hint_file(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_copy_records(binary(), binary(), [non_neg_integer()]) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()}
  def v2_copy_records(_source_path, _dest_path, _offsets), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_copy_records_preserve_tombstones(
          binary(),
          binary(),
          [non_neg_integer()],
          [non_neg_integer()]
        ) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()}
  def v2_copy_records_preserve_tombstones(
        _source_path,
        _dest_path,
        _live_offsets,
        _tombstone_offsets
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_batch_nosync(binary(), [{binary(), binary(), non_neg_integer()}]) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()}
  def v2_append_batch_nosync(_path, _records), do: :erlang.nif_error(:nif_not_loaded)

  @type append_op :: {:put, binary(), binary(), non_neg_integer()} | {:delete, binary()}
  @type append_op_location ::
          {:put, non_neg_integer(), non_neg_integer()}
          | {:delete, non_neg_integer(), non_neg_integer()}
  @spec v2_append_ops_batch_nosync(binary(), [append_op()]) ::
          {:ok, [append_op_location()]} | {:error, term()}
  def v2_append_ops_batch_nosync(_path, _records), do: :erlang.nif_error(:nif_not_loaded)

  # -- v2 Tokio async IO NIFs --
  @spec v2_pread_at_async(pid(), term(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def v2_pread_at_async(_caller_pid, _correlation_id, _path, _offset),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_at_key_async(pid(), term(), binary(), non_neg_integer(), binary()) ::
          :ok | {:error, term()}
  def v2_pread_at_key_async(_caller_pid, _correlation_id, _path, _offset, _expected_key),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch_async(pid(), term(), [{binary(), non_neg_integer()}]) ::
          :ok | {:error, term()}
  def v2_pread_batch_async(_caller_pid, _correlation_id, _locations),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch_path_async(pid(), term(), binary(), [non_neg_integer()]) ::
          :ok | {:error, term()}
  def v2_pread_batch_path_async(_caller_pid, _correlation_id, _path, _offsets),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch_path_key_async(
          pid(),
          term(),
          binary(),
          [{non_neg_integer(), binary()}]
        ) :: :ok | {:error, term()}
  def v2_pread_batch_path_key_async(_caller_pid, _correlation_id, _path, _reads),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch_grouped_async(
          pid(),
          term(),
          [{binary(), [{non_neg_integer(), non_neg_integer()}]}]
        ) :: :ok | {:error, term()}
  def v2_pread_batch_grouped_async(_caller_pid, _correlation_id, _groups),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch_grouped_key_async(
          pid(),
          term(),
          [{binary(), [{non_neg_integer(), non_neg_integer(), binary()}]}]
        ) :: :ok | {:error, term()}
  def v2_pread_batch_grouped_key_async(_caller_pid, _correlation_id, _groups),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_fsync_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def v2_fsync_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_append_batch_async(pid(), term(), binary(), [{binary(), binary(), non_neg_integer()}]) ::
          :ok | {:error, term()}
  def v2_append_batch_async(_caller_pid, _correlation_id, _path, _records),
    do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite Bloom NIFs --
  @spec bloom_file_create(binary(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, term()}
  def bloom_file_create(_path, _num_bits, _num_hashes), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_add(binary(), binary()) :: :ok | {:error, term()}
  def bloom_file_add(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_madd(binary(), [binary()]) :: :ok | {:error, term()}
  def bloom_file_madd(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_exists(binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def bloom_file_exists(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_mexists(binary(), [binary()]) :: {:ok, [boolean()]} | {:error, term()}
  def bloom_file_mexists(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_card(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def bloom_file_card(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_info(binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def bloom_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async Bloom read NIFs (Tokio spawn_blocking) --
  @spec bloom_file_exists_async(pid(), term(), binary(), binary()) :: :ok | {:error, term()}
  def bloom_file_exists_async(_caller_pid, _correlation_id, _path, _element),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_mexists_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def bloom_file_mexists_async(_caller_pid, _correlation_id, _path, _elements),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_card_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def bloom_file_card_async(_caller_pid, _correlation_id, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec bloom_file_info_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def bloom_file_info_async(_caller_pid, _correlation_id, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite CMS NIFs --
  @spec cms_file_create(binary(), non_neg_integer(), non_neg_integer()) :: :ok | {:error, term()}
  def cms_file_create(_path, _width, _depth), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_incrby(binary(), [{binary(), non_neg_integer()}]) :: :ok | {:error, term()}
  def cms_file_incrby(_path, _items), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_query(binary(), [binary()]) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def cms_file_query(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_info(binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | {:error, term()}
  def cms_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_merge(binary(), [binary()], [number()]) :: :ok | {:error, term()}
  def cms_file_merge(_dst_path, _src_paths, _weights), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async CMS read NIFs (Tokio spawn_blocking) --
  @spec cms_file_query_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def cms_file_query_async(_caller_pid, _correlation_id, _path, _elements),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec cms_file_info_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def cms_file_info_async(_caller_pid, _correlation_id, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite Cuckoo NIFs --
  @spec cuckoo_file_create(binary(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, term()}
  def cuckoo_file_create(_path, _capacity, _bucket_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_add(binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_add(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_addnx(binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def cuckoo_file_addnx(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_del(binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_del(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_exists(binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def cuckoo_file_exists(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_mexists(binary(), [binary()]) :: {:ok, [boolean()]} | {:error, term()}
  def cuckoo_file_mexists(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_count(binary(), binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def cuckoo_file_count(_path, _element), do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_info(binary()) :: {:ok, tuple()} | {:error, term()}
  def cuckoo_file_info(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async Cuckoo read NIFs (Tokio spawn_blocking) --
  @spec cuckoo_file_exists_async(pid(), term(), binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_exists_async(_caller_pid, _correlation_id, _path, _element),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_mexists_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def cuckoo_file_mexists_async(_caller_pid, _correlation_id, _path, _elements),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_count_async(pid(), term(), binary(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_count_async(_caller_pid, _correlation_id, _path, _element),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec cuckoo_file_info_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def cuckoo_file_info_async(_caller_pid, _correlation_id, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  # -- Stateless pread/pwrite TopK v2 NIFs --
  @spec topk_file_create_v2(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          float()
        ) :: :ok | {:error, term()}
  def topk_file_create_v2(_path, _k, _width, _depth, _decay),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_add_v2(binary(), [binary()]) :: {:ok, [binary() | nil]} | {:error, term()}
  def topk_file_add_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_incrby_v2(binary(), [{binary(), non_neg_integer()}]) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def topk_file_incrby_v2(_path, _pairs), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_query_v2(binary(), [binary()]) :: {:ok, [boolean()]} | {:error, term()}
  def topk_file_query_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_list_v2(binary()) :: {:ok, [{binary(), non_neg_integer()}]} | {:error, term()}
  def topk_file_list_v2(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_count_v2(binary(), [binary()]) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def topk_file_count_v2(_path, _elements), do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_info_v2(binary()) :: {:ok, tuple()} | {:error, term()}
  def topk_file_info_v2(_path), do: :erlang.nif_error(:nif_not_loaded)

  # -- Async TopK v2 read NIFs (Tokio spawn_blocking) --
  @spec topk_file_query_v2_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def topk_file_query_v2_async(_caller_pid, _correlation_id, _path, _elements),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_list_v2_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def topk_file_list_v2_async(_caller_pid, _correlation_id, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_count_v2_async(pid(), term(), binary(), [binary()]) :: :ok | {:error, term()}
  def topk_file_count_v2_async(_caller_pid, _correlation_id, _path, _elements),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec topk_file_info_v2_async(pid(), term(), binary()) :: :ok | {:error, term()}
  def topk_file_info_v2_async(_caller_pid, _correlation_id, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  # -------------------------------------------------------------------
  # Filesystem metadata NIFs (Normal scheduler; replace :prim_file)
  #
  # Motivation: every `File.touch!`, `File.mkdir_p!`, `File.rename`,
  # `File.rm`, `File.exists?`, `File.dir?`, `File.ls` call in Elixir
  # dispatches to the `:prim_file` async-thread pool and appears as
  # `erts_internal:dirty_nif_finalizer/1` in BEAM crash dumps, breaking
  # scheduler-utilization observability. These replacements run on the
  # Normal scheduler with `consume_timeslice` so BEAM accounting stays
  # accurate. For potentially long operations (rm_rf on a big tree),
  # use the `_async` variants which spawn onto the Tokio blocking pool.
  #
  # Error shape: `{:error, {atom_kind, message_binary}}` where `kind` is
  # one of: `:not_found`, `:already_exists`, `:permission_denied`,
  # `:not_a_directory`, `:is_a_directory`, `:directory_not_empty`,
  # `:invalid_path`, `:other`.
  # -------------------------------------------------------------------

  @type fs_error :: {:error, {atom(), binary()}}

  @doc """
  Creates an empty file if it does not exist. Idempotent on an existing
  file (does not truncate). Equivalent to `File.touch!/1` without the
  timestamp update (matches our callers' usage).
  """
  @spec fs_touch(binary()) :: :ok | fs_error()
  def fs_touch(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Recursive mkdir -p. Idempotent when the directory already exists.
  Equivalent to `File.mkdir_p/1`.
  """
  @spec fs_mkdir_p(binary()) :: :ok | fs_error()
  def fs_mkdir_p(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Atomic rename. On POSIX, `rename` replaces the target atomically.
  Cross-device renames return `{:error, {:other, _}}` — caller must
  handle with copy+remove.
  """
  @spec fs_rename(binary(), binary()) :: :ok | fs_error()
  def fs_rename(_old_path, _new_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Remove a single file. Use `fs_rm_rf_async/3` for directory trees.
  """
  @spec fs_rm(binary()) :: :ok | fs_error()
  def fs_rm(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Does the path exist? Follows symlinks (broken symlink → false).
  Never returns an error for the common kinds (missing, no-permission).
  """
  @spec fs_exists(binary()) :: boolean()
  def fs_exists(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Is the path a directory? Follows symlinks. Returns `false` for
  missing paths (matches `File.dir?/1`).
  """
  @spec fs_is_dir(binary()) :: boolean()
  def fs_is_dir(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  List the entries in a directory (names only, no path prefix).
  Yields every 256 entries to keep BEAM reductions accurate on huge
  directories.
  """
  @spec fs_ls(binary()) :: {:ok, [binary()]} | fs_error()
  def fs_ls(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Async recursive remove. Runs on the Tokio blocking pool; sends
  `{:tokio_complete, correlation_id, :ok}` or
  `{:tokio_complete, correlation_id, :error, {kind, msg}}` to the
  caller on completion. Idempotent: removing a non-existent path sends
  `:ok`.
  """
  @spec fs_rm_rf_async(pid(), term(), binary()) :: :ok | fs_error()
  def fs_rm_rf_async(_caller_pid, _correlation_id, _path), do: :erlang.nif_error(:nif_not_loaded)
end
