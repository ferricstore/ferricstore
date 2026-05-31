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
    )

  # -- Tracking allocator --
  @spec rust_allocated_bytes() :: {:ok, non_neg_integer()} | {:error, term()}
  def rust_allocated_bytes, do: :erlang.nif_error(:nif_not_loaded)

  # -- Flow native ordered index resource --
  @type flow_index_resource :: reference()

  @spec flow_index_new() :: flow_index_resource()
  def flow_index_new, do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_put_entries(flow_index_resource(), [{binary(), binary(), float()}]) :: :ok
  def flow_index_put_entries(_resource, _entries), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_put_new_entries(flow_index_resource(), [{binary(), binary(), float()}]) :: :ok
  def flow_index_put_new_entries(_resource, _entries), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_move_entries(flow_index_resource(), [
          {binary(), binary(), binary(), float()}
        ]) :: :ok
  def flow_index_move_entries(_resource, _entries), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_delete_members(flow_index_resource(), binary(), [binary()]) :: :ok
  def flow_index_delete_members(_resource, _key, _members), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_delete_entries(flow_index_resource(), [{binary(), binary()}]) :: :ok
  def flow_index_delete_entries(_resource, _entries), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_apply_batch(
          flow_index_resource(),
          [{binary(), binary(), float()}],
          [{binary(), binary(), float()}],
          [{binary(), binary(), binary(), float()}],
          [{binary(), [binary()]}],
          [flow_index_claim_entry()]
        ) :: :ok
  def flow_index_apply_batch(
        _resource,
        _put_entries,
        _put_new_entries,
        _move_entries,
        _delete_entries,
        _claim_entries
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_score_of(flow_index_resource(), binary(), binary()) ::
          {:ok, float()} | :miss
  def flow_index_score_of(_resource, _key, _member), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_range_slice(
          flow_index_resource(),
          binary(),
          non_neg_integer(),
          float(),
          non_neg_integer(),
          float(),
          boolean(),
          non_neg_integer(),
          integer()
        ) :: [{binary(), float()}]
  def flow_index_range_slice(
        _resource,
        _key,
        _min_kind,
        _min_score,
        _max_kind,
        _max_score,
        _reverse?,
        _offset,
        _count
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_take_due(
          flow_index_resource(),
          binary(),
          float(),
          non_neg_integer()
        ) :: [{binary(), float()}]
  def flow_index_take_due(_resource, _key, _max_score, _count),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_claim_due_candidates(
          flow_index_resource(),
          [binary()],
          float(),
          non_neg_integer(),
          non_neg_integer()
        ) :: [{binary(), [{binary(), float()}]}]
  def flow_index_claim_due_candidates(_resource, _keys, _max_score, _limit, _max_scan),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_due_keys_present(flow_index_resource(), [binary()], float()) :: [binary()]
  def flow_index_due_keys_present(_resource, _keys, _max_score),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_count_all(flow_index_resource(), binary()) :: non_neg_integer()
  def flow_index_count_all(_resource, _key), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_count_many(flow_index_resource(), [binary()]) :: [non_neg_integer()]
  def flow_index_count_many(_resource, _keys), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_count_keys(flow_index_resource()) :: [binary()]
  def flow_index_count_keys(_resource), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_due_count_keys(flow_index_resource()) :: [binary()]
  def flow_index_due_count_keys(_resource), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_restore_count(flow_index_resource(), binary(), integer()) :: :ok
  def flow_index_restore_count(_resource, _key, _count), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_delete_count(flow_index_resource(), binary()) :: :ok
  def flow_index_delete_count(_resource, _key), do: :erlang.nif_error(:nif_not_loaded)

  @type flow_index_claim_entry ::
          {binary(), binary(), float(), binary(), float(), binary(), float(), binary(), float(),
           binary(), binary(), float()}

  @spec flow_index_apply_claim_entries(flow_index_resource(), [flow_index_claim_entry()]) :: :ok
  def flow_index_apply_claim_entries(_resource, _entries),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_index_rollback_claim_entries(flow_index_resource(), [flow_index_claim_entry()]) ::
          :ok
  def flow_index_rollback_claim_entries(_resource, _entries),
    do: :erlang.nif_error(:nif_not_loaded)

  @type flow_record_claim_plan ::
          {binary(), flow_index_claim_entry(), binary(), non_neg_integer() | nil}

  @type flow_record_claim_history_entry ::
          {binary(), binary(), non_neg_integer(), non_neg_integer(), binary(), binary(),
           non_neg_integer() | nil, non_neg_integer() | nil, boolean()}

  @type flow_record_claim_history_plan ::
          {binary(), flow_index_claim_entry(), binary(), non_neg_integer() | nil,
           flow_record_claim_history_entry()}

  @spec flow_record_plan_claims(
          [{binary(), float()}],
          [binary() | nil],
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary()
        ) :: {:ok, [flow_record_claim_plan()], [binary()], non_neg_integer()} | :fallback
  def flow_record_plan_claims(
        _candidates,
        _values,
        _type,
        _expected_state,
        _worker,
        _lease_ms,
        _now_ms,
        _remaining,
        _from_due_key,
        _to_due_key,
        _from_state_key,
        _to_state_key,
        _inflight_key,
        _worker_key,
        _state_key_prefix
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_record_plan_claims_with_history(
          [{binary(), float()}],
          [binary() | nil],
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary()
        ) ::
          {:ok, [flow_record_claim_history_plan()], [binary()], non_neg_integer()} | :fallback
  def flow_record_plan_claims_with_history(
        _candidates,
        _values,
        _type,
        _expected_state,
        _worker,
        _lease_ms,
        _now_ms,
        _remaining,
        _from_due_key,
        _to_due_key,
        _from_state_key,
        _to_state_key,
        _inflight_key,
        _worker_key,
        _state_key_prefix,
        _history_key_prefix
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_record_encode(
          binary() | nil,
          binary() | nil,
          binary() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          non_neg_integer() | nil,
          binary() | nil,
          binary() | nil,
          binary()
        ) :: binary()
  def flow_record_encode(
        _id,
        _type,
        _state,
        _version,
        _attempts,
        _fencing_token,
        _created_at_ms,
        _updated_at_ms,
        _next_run_at_ms,
        _priority,
        _ttl_ms,
        _history_hot_max_events,
        _history_max_events,
        _retention_ttl_ms,
        _terminal_retention_until_ms,
        _partition_key,
        _payload_ref,
        _parent_flow_id,
        _parent_partition_key,
        _root_flow_id,
        _correlation_id,
        _result_ref,
        _error_ref,
        _lease_owner,
        _lease_token,
        _lease_deadline_ms,
        _run_state,
        _rewound_to_event_id,
        _child_groups_encoded
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_record_decode(binary()) :: {:ok, list()} | :error
  def flow_record_decode(_value), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_records_terminal_after_noop([binary()]) :: [boolean()]
  def flow_records_terminal_after_noop(_values), do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_history_encode(
          binary(),
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary() | nil,
          binary()
        ) :: binary()
  def flow_history_encode(
        _event,
        _version,
        _now_ms,
        _id,
        _type,
        _state,
        _priority,
        _attempts,
        _fencing_token,
        _created_at_ms,
        _updated_at_ms,
        _next_run_at_ms,
        _lease_deadline_ms,
        _lease_owner,
        _payload_ref,
        _parent_flow_id,
        _root_flow_id,
        _correlation_id,
        _result_ref,
        _error_ref,
        _rewound_to_event_id,
        _meta_encoded
      ),
      do: :erlang.nif_error(:nif_not_loaded)

  @spec flow_history_decode(binary()) :: {:ok, list()} | :error
  def flow_history_decode(_value), do: :erlang.nif_error(:nif_not_loaded)

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

  @spec v2_validate_value_ref(binary(), non_neg_integer(), binary(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | :mismatch | {:error, term()}
  def v2_validate_value_ref(_path, _offset, _expected_key, _expected_value_size),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_file(binary()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()}]}
          | {:error, term()}
  def v2_scan_file(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_file_from_offset(binary(), non_neg_integer()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()}]}
          | {:error, term()}
  def v2_scan_file_from_offset(_path, _start_offset), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_file_page(binary(), non_neg_integer(), pos_integer()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()}],
           non_neg_integer(), boolean()}
          | {:error, term()}
  def v2_scan_file_page(_path, _start_offset, _limit), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_tombstones(binary()) ::
          {:ok, [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()}]}
          | {:error, term()}
  def v2_scan_tombstones(_path), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_scan_key_states(binary(), [binary()]) ::
          {:ok, [{binary(), non_neg_integer(), boolean()}]} | {:error, term()}
  def v2_scan_key_states(_path, _keys), do: :erlang.nif_error(:nif_not_loaded)

  @spec v2_pread_batch(binary(), [non_neg_integer()]) ::
          {:ok, [binary() | nil]} | {:error, term()}
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

  # -- LMDB Flow state backend --
  @type lmdb_op ::
          {:put, binary(), binary()} | {:put_new, binary(), binary()} | {:delete, binary()}
  @type lmdb_original :: {binary(), :missing | {:value, binary()}}

  @spec lmdb_get(binary(), binary(), non_neg_integer()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def lmdb_get(_path, _key, _map_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_get_many(binary(), [binary()], non_neg_integer()) ::
          {:ok, [{:ok, binary()} | :not_found]} | {:error, term()}
  def lmdb_get_many(_path, _keys, _map_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_put(binary(), binary(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def lmdb_put(_path, _key, _value, _map_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_delete(binary(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def lmdb_delete(_path, _key, _map_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_write_batch(binary(), [lmdb_op()], non_neg_integer()) :: :ok | {:error, term()}
  def lmdb_write_batch(_path, _ops, _map_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_write_batch_with_originals(binary(), [lmdb_op()], non_neg_integer()) ::
          {:ok, [lmdb_original()]} | {:error, term()}
  def lmdb_write_batch_with_originals(_path, _ops, _map_size),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_clear(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def lmdb_clear(_path, _map_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_prefix_entries(binary(), binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def lmdb_prefix_entries(_path, _prefix, _limit, _map_size),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_prefix_entries_after(
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def lmdb_prefix_entries_after(_path, _prefix, _after_key, _limit, _map_size),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_prefix_entries_reverse(binary(), binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def lmdb_prefix_entries_reverse(_path, _prefix, _limit, _map_size),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_prefix_entries_reverse_before(
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def lmdb_prefix_entries_reverse_before(_path, _prefix, _before_key, _limit, _map_size),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec lmdb_prefix_count(binary(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def lmdb_prefix_count(_path, _prefix, _map_size), do: :erlang.nif_error(:nif_not_loaded)

  # -- v2 Tokio async IO NIFs --
  @type pread_batch_value :: binary() | nil | {:error, binary()}
  @type pread_batch_result :: [pread_batch_value()]

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
