defmodule Ferricstore.Store.Promotion do
  @moduledoc """
  Collection promotion: migrates large compound-key collections from the
  shared shard Bitcask into a dedicated per-key Bitcask instance.

  ## Background (spec section 2B.4b)

  Small collections (hashes, sets, sorted sets) are stored as compound keys
  in the shared shard Bitcask (`H:key\\0field`, `S:key\\0member`,
  `Z:key\\0member`). When any compound-key collection exceeds the
  configurable promotion threshold (default: 100 entries), it is promoted
  to a dedicated Bitcask instance stored under:

      dedicated/shard_N/{type}:{sha256_of_key}/

  where `{type}` is `hash`, `set`, or `zset`.

  Promotion is **one-way** -- once promoted, a collection stays in its
  dedicated instance even if entries are later deleted below the threshold.
  The dedicated instance is only removed when the entire key is deleted
  via `DEL` / `UNLINK`.

  ## Lists are not promoted

  Lists store all elements as a single serialized Erlang term in one
  Bitcask entry (via `ListOps`). Since there is no compound key fan-out,
  a list with 1000 elements is still a single Bitcask entry and does not
  benefit from promotion. List promotion is intentionally skipped.

  ## Promotion marker

  When a key is promoted, a marker entry `PM:redis_key` is written to the
  shared Bitcask with the type as its value (`"hash"`, `"set"`, or
  `"zset"`). This allows the shard to rediscover promoted keys on restart
  by scanning for `PM:` prefixed keys during initialization.

  ## Configuration

      config :ferricstore, :promotion_threshold, 100

  The value is captured in the instance apply context and replicated to every
  shard before it affects promotion decisions.

  Set to `0` to disable automatic promotion entirely (no collections will
  ever be promoted).
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.ExpiryContext
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.{ActiveFile, AppendResult, BlobRef, BlobValue, CompoundKey, LFU}
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  require Logger

  @cold_read_timeout_ms 10_000
  @compaction_latch_sleep_ms 1
  @default_compaction_latch_timeout_ms 30_000
  @default_recovery_scan_page_size 8_192
  @log_header_size 26
  @max_log_value_size 512 * 1024 * 1024
  @tombstone_value_size 0xFFFFFFFF

  @doc "Returns the immutable promotion threshold captured by an instance."
  @spec threshold(FerricStore.Instance.t()) :: non_neg_integer()
  def threshold(%{apply_context: %{promotion_threshold: threshold}})
      when is_integer(threshold) and threshold >= 0,
      do: threshold

  @spec dedicated_path(binary(), non_neg_integer(), atom(), binary()) :: binary()
  def dedicated_path(data_dir, shard_index, type, redis_key) do
    hash = :crypto.hash(:sha256, redis_key) |> Base.encode16(case: :lower)
    type_str = Atom.to_string(type)
    Path.join([data_dir, "dedicated", "shard_#{shard_index}", "#{type_str}:#{hash}"])
  end

  @spec marker_key(binary()) :: binary()
  def marker_key(redis_key), do: CompoundKey.promotion_marker_key(redis_key)

  @doc false
  @spec flush_marker_tombstones(map(), pos_integer()) ::
          {:ok,
           %{
             marker_count: non_neg_integer(),
             appended_bytes: non_neg_integer(),
             active_file_id: non_neg_integer() | nil,
             active_file_path: binary() | nil
           }}
          | {:error, term()}
  def flush_marker_tombstones(owner, page_size \\ 512)
      when is_map(owner) and is_integer(page_size) and page_size > 0 do
    keydir = Map.get(owner, :ets, Map.get(owner, :keydir))

    try do
      :ets.safe_fixtable(keydir, true)

      try do
        match_spec = [
          {{:"$1", :_, :_, :_, :_, :_, :_}, [{:is_binary, :"$1"}], [:"$1"]}
        ]

        initial = %{
          marker_count: 0,
          appended_bytes: 0,
          active_file_id: nil,
          active_file_path: nil
        }

        with {:ok, result} <-
               flush_marker_tombstone_pages(
                 owner,
                 :ets.select(keydir, match_spec, page_size),
                 initial
               ),
             :ok <- fsync_flushed_marker_tombstones(result) do
          {:ok, result}
        end
      after
        :ets.safe_fixtable(keydir, false)
      end
    rescue
      error in ArgumentError -> {:error, {:promotion_marker_keydir_unavailable, error}}
      error -> {:error, {:promotion_marker_flush_failed, error}}
    catch
      kind, reason -> {:error, {:promotion_marker_flush_failed, kind, reason}}
    end
  end

  defp flush_marker_tombstone_pages(_owner, :"$end_of_table", result), do: {:ok, result}

  defp flush_marker_tombstone_pages(owner, {keys, continuation}, result) do
    marker_keys = Enum.filter(keys, &promotion_marker?/1)

    with {:ok, next_result} <- append_marker_tombstone_page(owner, marker_keys, result) do
      flush_marker_tombstone_pages(owner, :ets.select(continuation), next_result)
    end
  end

  defp append_marker_tombstone_page(_owner, [], result), do: {:ok, result}

  defp append_marker_tombstone_page(owner, marker_keys, result) do
    with {:ok, file_id, active_path} <- marker_flush_active_file(owner, result),
         ops = Enum.map(marker_keys, &{:delete, &1}),
         {:ok, locations} <- append_marker_tombstone_ops(active_path, ops),
         :ok <- AppendResult.validate_operation_locations(locations, ops),
         {:ok, appended_bytes} <- marker_tombstone_location_bytes(locations) do
      {:ok,
       %{
         result
         | marker_count: result.marker_count + length(marker_keys),
           appended_bytes: result.appended_bytes + appended_bytes,
           active_file_id: file_id,
           active_file_path: active_path
       }}
    end
  end

  defp marker_flush_active_file(
         _owner,
         %{active_file_id: file_id, active_file_path: path}
       )
       when is_integer(file_id) and file_id >= 0 and is_binary(path),
       do: {:ok, file_id, path}

  defp marker_flush_active_file(owner, _result) do
    case {Map.get(owner, :instance_ctx), Map.get(owner, :shard_index, Map.get(owner, :index))} do
      {%FerricStore.Instance{} = ctx, shard_index}
      when is_integer(shard_index) and shard_index >= 0 ->
        case ActiveFile.get(ctx, shard_index) do
          {file_id, path, _shard_path}
          when is_integer(file_id) and file_id >= 0 and is_binary(path) ->
            {:ok, file_id, path}

          other ->
            {:error, {:promotion_marker_active_file_unavailable, other}}
        end

      _missing_context ->
        case {Map.get(owner, :active_file_id), Map.get(owner, :active_file_path)} do
          {file_id, path} when is_integer(file_id) and file_id >= 0 and is_binary(path) ->
            {:ok, file_id, path}

          other ->
            {:error, {:promotion_marker_active_file_unavailable, other}}
        end
    end
  rescue
    error -> {:error, {:promotion_marker_active_file_unavailable, error}}
  end

  defp append_marker_tombstone_ops(active_path, ops) do
    result =
      case Process.get(:ferricstore_promotion_marker_append_hook) do
        hook when is_function(hook, 2) ->
          case hook.(active_path, ops) do
            :passthrough -> NIF.v2_append_ops_batch(active_path, ops)
            result -> result
          end

        _missing ->
          NIF.v2_append_ops_batch(active_path, ops)
      end

    case result do
      {:ok, locations} -> {:ok, locations}
      {:error, reason} -> {:error, {:append_promotion_marker_tombstones_failed, reason}}
      other -> {:error, {:append_promotion_marker_tombstones_failed, other}}
    end
  end

  defp marker_tombstone_location_bytes(locations) do
    Enum.reduce_while(locations, {:ok, 0}, fn
      {:delete, _offset, record_size}, {:ok, total}
      when is_integer(record_size) and record_size >= 0 ->
        {:cont, {:ok, total + record_size}}

      invalid, {:ok, _total} ->
        {:halt, {:error, {:invalid_promotion_marker_tombstone_location, invalid}}}
    end)
  end

  defp fsync_flushed_marker_tombstones(%{marker_count: 0}), do: :ok

  defp fsync_flushed_marker_tombstones(%{active_file_path: active_path})
       when is_binary(active_path) do
    case NIF.v2_fsync(active_path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_promotion_marker_tombstones_failed, reason}}
      other -> {:error, {:fsync_promotion_marker_tombstones_failed, other}}
    end
  end

  defp promotion_marker?(<<"PM:", _::binary>>), do: true
  defp promotion_marker?(_key), do: false

  @doc false
  @spec remove_shard_dedicated_storage(map()) :: :ok | {:error, term()}
  def remove_shard_dedicated_storage(owner) when is_map(owner) do
    with data_dir when is_binary(data_dir) <- Map.get(owner, :data_dir),
         shard_index when is_integer(shard_index) and shard_index >= 0 <-
           Map.get(owner, :shard_index, Map.get(owner, :index)) do
      dedicated_parent = Path.join(data_dir, "dedicated")
      shard_root = Path.join(dedicated_parent, "shard_#{shard_index}")

      with :ok <- maybe_remove_dedicated_shard_root(shard_root),
           :ok <- fsync_dedicated_parent(data_dir, dedicated_parent) do
        :ok
      end
    else
      invalid -> {:error, {:invalid_dedicated_storage_owner, invalid}}
    end
  end

  defp maybe_remove_dedicated_shard_root(shard_root) do
    if Ferricstore.FS.exists?(shard_root),
      do: remove_dedicated_shard_root(shard_root),
      else: :ok
  end

  defp remove_dedicated_shard_root(shard_root) do
    case Ferricstore.FS.rm_rf(shard_root) do
      :ok -> :ok
      {:error, reason} -> {:error, {:remove_dedicated_shard_root_failed, shard_root, reason}}
    end
  end

  defp fsync_dedicated_parent(data_dir, dedicated_parent) do
    sync_dir =
      if Ferricstore.FS.dir?(dedicated_parent),
        do: dedicated_parent,
        else: data_dir

    result =
      case Process.get(:ferricstore_promotion_fsync_dir_hook) do
        hook when is_function(hook, 1) -> hook.(sync_dir)
        _missing -> NIF.v2_fsync_dir(sync_dir)
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_dedicated_parent_failed, sync_dir, reason}}
      other -> {:error, {:fsync_dedicated_parent_failed, sync_dir, other}}
    end
  end

  @doc """
  Runs `fun` while holding the per-promoted-key compaction latch.

  Promoted dedicated compaction rewrites member records into a newer log file.
  Raft-applied promoted writes must not append to that same per-key log while
  the rewrite snapshot is in progress, or disk replay order can resurrect stale
  compacted records. The latch is only consulted on promoted dedicated paths.
  """
  @spec with_compaction_latch(map(), binary(), (-> term())) :: term()
  def with_compaction_latch(owner, redis_key, fun) when is_function(fun, 0) do
    case acquire_compaction_latch(owner, redis_key) do
      :none ->
        fun.()

      token ->
        try do
          fun.()
        after
          release_compaction_latch(token)
        end
    end
  end

  @doc false
  @spec acquire_compaction_latch(map(), binary()) :: :none | {term(), term()}
  def acquire_compaction_latch(owner, redis_key) do
    case compaction_latch(owner, redis_key) do
      nil ->
        :none

      {tab, latch_key, shard_index} ->
        acquire_compaction_latch(tab, latch_key, shard_index)
        {tab, latch_key}
    end
  end

  @doc false
  @spec acquire_shared_log_latch(map()) :: :none | {term(), term()}
  def acquire_shared_log_latch(owner) do
    case shared_log_latch(owner) do
      nil ->
        :none

      {tab, latch_key, shard_index} ->
        acquire_compaction_latch(tab, latch_key, shard_index)
        {tab, latch_key}
    end
  end

  @doc false
  @spec try_acquire_shared_log_latch(map()) :: :none | :busy | {:ok, {term(), term()}}
  def try_acquire_shared_log_latch(owner) do
    case shared_log_latch(owner) do
      nil ->
        :none

      {tab, latch_key, _shard_index} ->
        try_acquire_latch(tab, latch_key)
    end
  rescue
    ArgumentError -> :none
  end

  @doc false
  @spec release_compaction_latch(:none | {term(), term()}) :: :ok
  def release_compaction_latch(:none), do: :ok

  def release_compaction_latch({tab, latch_key}) do
    :ets.delete(tab, latch_key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc """
  Waits until no promoted compaction latch is held for `redis_key`.

  Called by Raft apply before appending promoted dedicated writes/tombstones.
  """
  @spec await_compaction_latch(map(), binary()) :: :ok
  def await_compaction_latch(owner, redis_key) do
    owner
    |> await_compaction_latch_clear(redis_key)
    |> raise_if_compound_promotion_failed(owner, redis_key)
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec record_compound_promotion_failure(map(), binary(), term()) :: :ok
  def record_compound_promotion_failure(owner, redis_key, reason) do
    record_compound_promotion_fence(owner, redis_key, {:error, reason})
  end

  @doc false
  @spec record_compound_promotion_running(map(), binary()) :: :ok
  def record_compound_promotion_running(owner, redis_key) do
    record_compound_promotion_fence(owner, redis_key, :running)
  end

  @doc false
  @spec record_compound_promotion_success(map(), binary()) :: :ok
  def record_compound_promotion_success(owner, redis_key) do
    record_compound_promotion_fence(owner, redis_key, :ok)
  end

  defp record_compound_promotion_fence(owner, redis_key, result) do
    case failure_fence(owner, redis_key) do
      nil -> :ok
      {tab, fence_key} -> true = :ets.insert(tab, {fence_key, result})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec clear_compound_promotion_fence(map(), binary()) :: :ok
  def clear_compound_promotion_fence(owner, redis_key) do
    case failure_fence(owner, redis_key) do
      nil -> :ok
      {tab, fence_key} -> :ets.delete(tab, fence_key)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc false
  @spec clear_compound_promotion_fences(map()) :: :ok
  def clear_compound_promotion_fences(owner) do
    case latch_table(owner) do
      nil -> :ok
      tab -> :ets.match_delete(tab, {{:compound_promotion_failure, :_}, :_})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp await_compaction_latch_clear(owner, redis_key) do
    case compaction_latch(owner, redis_key) do
      nil ->
        :ok

      {tab, latch_key, shard_index} ->
        case :ets.lookup(tab, latch_key) do
          [{^latch_key, latch_owner}] when latch_owner == self() ->
            :ok

          _other ->
            wait_compaction_latch_clear!(tab, latch_key, shard_index)
        end
    end
  end

  defp raise_if_compound_promotion_failed(:ok, owner, redis_key) do
    case failure_fence(owner, redis_key) do
      nil ->
        :ok

      {tab, fence_key} ->
        case :ets.lookup(tab, fence_key) do
          [{^fence_key, :ok}] ->
            :ok

          [{^fence_key, {:error, reason}}] ->
            raise "compound promotion failed for #{inspect(redis_key)}: #{inspect(reason)}"

          [{^fence_key, :running}] ->
            raise "compound promotion failed for #{inspect(redis_key)}: worker exited before completion"

          [] ->
            :ok
        end
    end
  end

  @spec open_dedicated(binary(), non_neg_integer(), atom(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def open_dedicated(data_dir, shard_index, type, redis_key) do
    path = dedicated_path(data_dir, shard_index, type, redis_key)
    created_dir? = not Ferricstore.FS.dir?(path)
    Ferricstore.FS.mkdir_p!(path)

    with :ok <- maybe_fsync_dir(created_dir?, Path.dirname(path), :create_dedicated_dir) do
      active_file = Path.join(path, "00000.log")

      created_file? =
        if Ferricstore.FS.exists?(active_file) do
          false
        else
          Ferricstore.FS.touch!(active_file)
          true
        end

      with :ok <- maybe_fsync_dir(created_dir? or created_file?, path, :create_active_file) do
        {:ok, path}
      end
    end
  end

  defp maybe_fsync_dir(false, _path, _phase), do: :ok
  defp maybe_fsync_dir(true, path, phase), do: fsync_dir(path, phase)

  defp fsync_dir(path, phase) do
    result =
      case Process.get(:ferricstore_promotion_fsync_dir_hook) do
        fun when is_function(fun, 1) -> fun.(path)
        _ -> NIF.v2_fsync_dir(path)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Promotion: directory fsync failed during #{phase} for #{path}: #{inspect(reason)}"
        )

        {:error, {:fsync_dir_failed, phase, reason}}
    end
  end

  @spec promote_collection!(
          atom(),
          binary(),
          binary(),
          atom(),
          binary(),
          non_neg_integer(),
          term(),
          CompoundMemberIndex.table_ref()
        ) ::
          {:ok, reference()} | {:error, term()}
  def promote_collection!(
        _type,
        _redis_key,
        _shard_data_path,
        _keydir,
        _data_dir,
        _shard_index,
        _instance_ctx,
        nil
      ) do
    raise ArgumentError, "compound member catalog is required for promotion"
  end

  def promote_collection!(
        type,
        redis_key,
        shard_data_path,
        keydir,
        data_dir,
        shard_index,
        instance_ctx,
        member_index
      ) do
    prefix = compound_prefix_for(type, redis_key)
    type_key = CompoundKey.type_key(redis_key)
    type_str = CompoundKey.encode_type(type)
    type_label = type_label(type)

    now =
      ExpiryContext.capture()
      |> ExpiryContext.safe_expiry_cutoff_ms()

    entries_result =
      collect_promotion_entries(
        member_index,
        shard_data_path,
        keydir,
        type_key,
        prefix,
        now,
        instance_ctx,
        shard_index
      )

    entries =
      case entries_result do
        {:ok, entries} ->
          entries

        {:error, {key, reason}} ->
          raise "promotion copy read failed for #{inspect(key)}: #{inspect(reason)}"
      end

    validate_promotion_type!(entries, type_key, type_str)

    active_path = find_active(shard_data_path)
    mk = marker_key(redis_key)

    # --------------------------------------------------------------------
    # Crash-safe promotion order (the crash-safe promotion ordering):
    #
    #   1. Write the marker FIRST. If we crash here, recover_promoted
    #      sees the marker and falls back to compound keys still in the
    #      shared log (not yet tombstoned) — no data loss.
    #   2. Open/create dedicated dir + write the batch. Dedicated data is
    #      fsynced by v2_append_batch. If we crash here, recover_promoted
    #      sees the marker, opens the dedicated dir, finds either data
    #      (good) or empty (falls back to shared log compound keys).
    #   3. Tombstone compound keys in shared log LAST. If we crash
    #      partway through, the un-tombstoned keys are still in the
    #      shared log — recover_promoted reads them via the fallback.
    #
    # Old order (dedicated-first → tombstones → marker) lost data when
    # crashed between tombstones and marker: compound keys gone, no
    # marker to locate dedicated dir, collection silently vanished.
    # --------------------------------------------------------------------

    # Step 1: marker
    case NIF.v2_append_record(active_path, mk, type_str, 0) do
      {:ok, {moffset, mvsize}} ->
        marker_fid = file_id_from_path(active_path)

        track_binary_insert(keydir, shard_index, mk, type_str, instance_ctx)
        :ets.insert(keydir, {mk, type_str, 0, LFU.initial(), marker_fid, moffset, mvsize})

      {:error, reason} ->
        Logger.error(
          "Promotion: marker write failed for #{inspect(redis_key)}: #{inspect(reason)}"
        )

        raise "promotion marker write failed: #{inspect(reason)}"
    end

    # Step 2: open dedicated + write batch
    dedicated_path =
      case open_dedicated(data_dir, shard_index, type, redis_key) do
        {:ok, path} ->
          path

        {:error, reason} ->
          Logger.error(
            "Promotion: open dedicated failed for #{inspect(redis_key)}: #{inspect(reason)}"
          )

          raise "promotion open dedicated failed: #{inspect(reason)}"
      end

    if entries != [] do
      batch = Enum.map(entries, fn {k, v, exp} -> {k, v, exp} end)
      dedicated_active = find_active(dedicated_path)

      dedicated_fid = file_id_from_path(dedicated_active)

      case NIF.v2_append_batch(dedicated_active, batch) do
        {:ok, locations} ->
          case AppendResult.validate_locations(locations, length(entries)) do
            :ok ->
              Enum.zip(entries, locations)
              |> Enum.each(fn {{k, _v, _exp}, {offset, value_size}} ->
                :ets.update_element(keydir, k, [
                  {5, dedicated_fid},
                  {6, offset},
                  {7, value_size}
                ])
              end)

            {:error, reason} ->
              raise "promotion dedicated write failed: #{inspect(reason)}"
          end

        {:error, reason} ->
          Logger.error(
            "Promotion: v2_append_batch failed for #{inspect(redis_key)}: #{inspect(reason)}"
          )

          raise "promotion dedicated write failed: #{inspect(reason)}"
      end
    end

    # Step 3: tombstone compound keys in shared log (LAST step).
    # Batch tombstones and fsync once. If this crashes before the fsync,
    # recovery is still safe because marker + dedicated data are already
    # durable, and any un-tombstoned shared keys remain valid fallback copies.
    # If the write/fsync returns an error, fail closed so the caller does not
    # observe a successful promotion whose shared-log cleanup may be missing.
    tombstone_ops = Enum.map(entries, fn {key, _value, _exp} -> {:delete, key} end)

    if tombstone_ops != [] do
      run_before_shared_tombstones_hook()

      case NIF.v2_append_ops_batch(active_path, tombstone_ops) do
        {:ok, locations} ->
          case AppendResult.validate_operation_locations(locations, tombstone_ops) do
            :ok ->
              :ok

            {:error, reason} ->
              raise "promotion shared tombstone batch failed: #{inspect(reason)}"
          end

        {:error, reason} ->
          Logger.error(
            "Promotion: durable tombstone batch failed for #{inspect(redis_key)}: #{inspect(reason)}"
          )

          raise "promotion shared tombstone batch failed: #{inspect(reason)}"
      end
    end

    Logger.info(
      "Promoted #{type_label} #{inspect(redis_key)} to dedicated Bitcask " <>
        "(#{length(entries)} entries, shard #{shard_index})"
    )

    {:ok, dedicated_path}
  end

  defp collect_promotion_entries(
         member_index,
         shard_data_path,
         keydir,
         type_key,
         prefix,
         now,
         instance_ctx,
         shard_index
       ) do
    location_ctx = {shard_data_path, instance_ctx, shard_index}

    with {:ok, type_row} <- promotion_type_row(keydir, type_key),
         {:ok, initial} <- collect_promotion_row(type_row, {:ok, []}, location_ctx, now) do
      case CompoundMemberIndex.reduce_rows_while(
             member_index,
             %{keydir: keydir},
             prefix,
             initial,
             fn row, acc ->
               case collect_promotion_row(row, {:ok, acc}, location_ctx, now) do
                 {:ok, next_acc} -> {:cont, next_acc}
                 {:error, _reason} = error -> {:halt, error}
               end
             end
           ) do
        {:ok, entries} -> {:ok, entries}
        {:halt, {:error, _reason} = error} -> error
        {:error, reason} -> {:error, {prefix, {:member_index_failed, reason}}}
        :unavailable -> {:error, {prefix, :compound_member_index_unavailable}}
      end
    else
      {:error, reason} -> {:error, {type_key, reason}}
    end
  end

  defp promotion_type_row(keydir, type_key) do
    case :ets.lookup(keydir, type_key) do
      [{^type_key, _value, _exp, _lfu, _fid, _off, _vsize} = row] -> {:ok, row}
      [] -> {:error, :missing_type_metadata}
      invalid -> {:error, {:invalid_type_metadata, invalid}}
    end
  rescue
    ArgumentError -> {:error, :keydir_unavailable}
  end

  defp validate_promotion_type!(entries, type_key, expected_type) do
    case Enum.find(entries, fn {key, _value, _expire_at_ms} -> key == type_key end) do
      {^type_key, ^expected_type, _expire_at_ms} ->
        :ok

      {^type_key, actual_type, _expire_at_ms} ->
        raise "promotion type metadata mismatch for #{inspect(type_key)}: " <>
                "expected #{inspect(expected_type)}, got #{inspect(actual_type)}"

      nil ->
        raise "promotion copy read failed for #{inspect(type_key)}: :missing_type_metadata"
    end
  end

  defp collect_promotion_row(
         {key, value, exp, _lfu, fid, off, _vsize},
         {:ok, acc},
         location_ctx,
         now
       )
       when is_binary(key) and is_integer(exp) and exp >= 0 do
    if exp == 0 or exp > now do
      case promotion_entry_value(location_ctx, key, value, fid, off) do
        {:ok, live_value} -> {:ok, [{key, live_value, exp} | acc]}
        {:error, reason} -> {:error, {key, reason}}
      end
    else
      {:ok, acc}
    end
  end

  defp collect_promotion_row(row, {:ok, _acc}, _location_ctx, _now),
    do: {:error, {:invalid_keydir_row, row}}

  defp run_before_shared_tombstones_hook do
    case Process.get(:ferricstore_promotion_before_shared_tombstones_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  @spec recover_promoted(binary(), atom(), binary(), non_neg_integer(), term()) :: map()
  def recover_promoted(shard_data_path, keydir, data_dir, shard_index, instance_ctx \\ nil) do
    recovery_plan = :ets.new(:promotion_recovery_plan, [:private, :ordered_set])
    shared_index = :ets.new(:promotion_recovery_shared_index, [:private, :ordered_set])

    try do
      {all_markers, marker_actions} =
        plan_recovery_markers!(shard_data_path, keydir, shard_index)

      now =
        ExpiryContext.capture()
        |> ExpiryContext.safe_expiry_cutoff_ms()

      build_shared_recovery_index!(shared_index, keydir, shard_index, all_markers != [])

      marker_actions_by_key = Enum.group_by(marker_actions, &elem(&1, 1))

      {recovered, applied_marker_keys} =
        Enum.reduce(all_markers, {%{}, MapSet.new()}, fn
          {redis_key, type, marker_key, marker_state}, {recovered, applied_marker_keys} ->
            # A plan can contain every live value in one collection. Release it
            # before decoding the next collection so startup memory is bounded
            # by the largest collection, rather than the whole shard.
            :ets.delete_all_objects(recovery_plan)

            shared_state =
              shared_compound_recovery_state!(
                shared_index,
                keydir,
                redis_key,
                type,
                now,
                shard_index
              )

            collection_plan =
              case marker_state do
                :promoted ->
                  plan_promoted_collection!(
                    redis_key,
                    type,
                    marker_key,
                    shared_state,
                    shard_data_path,
                    data_dir,
                    shard_index,
                    instance_ctx,
                    now
                  )

                {:intent, :fallback} ->
                  {:fallback, marker_key, redis_key, type,
                   dedicated_path(data_dir, shard_index, type, redis_key)}

                {:intent, :cleanup} ->
                  {:cleanup, marker_key, redis_key, type,
                   dedicated_path(data_dir, shard_index, type, redis_key), shared_state.all_keys}
              end

            true = :ets.insert(recovery_plan, {0, collection_plan})

            apply_recovery_plan!(
              Map.get(marker_actions_by_key, marker_key, []),
              recovery_plan,
              shard_data_path,
              keydir,
              shard_index,
              instance_ctx
            )

            recovered =
              case collection_plan do
                {:promoted, _marker_key, ^redis_key, promoted, _row_actions} ->
                  Map.put(recovered, redis_key, promoted)

                _fallback_or_cleanup ->
                  recovered
              end

            {recovered, MapSet.put(applied_marker_keys, marker_key)}
        end)

      remaining_marker_actions =
        Enum.reject(marker_actions, fn action ->
          MapSet.member?(applied_marker_keys, elem(action, 1))
        end)

      :ets.delete_all_objects(recovery_plan)

      apply_recovery_plan!(
        remaining_marker_actions,
        recovery_plan,
        shard_data_path,
        keydir,
        shard_index,
        instance_ctx
      )

      recovered
    after
      :ets.delete(recovery_plan)
      :ets.delete(shared_index)
    end
  end

  defp plan_recovery_markers!(shard_data_path, keydir, shard_index) do
    pm_prefix = "PM:"
    pm_len = byte_size(pm_prefix)

    match_spec = [
      {{:"$1", :"$2", :_, :_, :"$3", :"$4", :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, pm_len},
           {:==, {:binary_part, :"$1", 0, pm_len}, pm_prefix}}}
       ], [{{:"$1", :"$2", :"$3", :"$4"}}]}
    ]

    {markers, actions} =
      keydir
      |> :ets.select(match_spec)
      |> Enum.reduce({[], []}, fn {full_key, value, fid, offset}, {markers, actions} ->
        type_str =
          if is_binary(value) do
            value
          else
            file_path =
              Path.join(
                shard_data_path,
                "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
              )

            read_recovery_value!(file_path, offset, full_key, :read_marker, shard_index)
          end

        case decode_recovery_marker(type_str) do
          {:ok, type, marker_state} ->
            redis_key = CompoundKey.extract_redis_key(full_key)

            actions =
              if not is_binary(value) do
                [{:hydrate_marker, full_key, type_str} | actions]
              else
                actions
              end

            {[{redis_key, type, full_key, marker_state} | markers], actions}

          :error ->
            {markers, [{:delete_invalid_marker, full_key, type_str} | actions]}
        end
      end)

    unique_markers =
      markers
      |> Enum.reverse()
      |> Enum.uniq_by(fn {redis_key, _type, _marker_key, _marker_state} -> redis_key end)

    {unique_markers, Enum.reverse(actions)}
  end

  defp plan_promoted_collection!(
         redis_key,
         type,
         marker_key,
         shared_state,
         shard_data_path,
         data_dir,
         shard_index,
         instance_ctx,
         now
       ) do
    expected_path = dedicated_path(data_dir, shard_index, type, redis_key)

    if not Ferricstore.FS.dir?(expected_path) and MapSet.size(shared_state.all_keys) == 0 do
      fail_recovery!(:open_dedicated, redis_key, shard_index, :dedicated_directory_missing)
    end

    dedicated_path = open_recovery_dedicated!(data_dir, shard_index, type, redis_key)
    log_files = recovery_log_files!(dedicated_path, shard_index)
    active_fid = log_files |> List.last() |> elem(0)

    final_state =
      Enum.reduce(log_files, %{}, fn {fid, file_path}, state ->
        recover_dedicated_log!(
          file_path,
          fid,
          state,
          shard_index,
          redis_key,
          type,
          fid == active_fid,
          shared_state.all_keys,
          now
        )
      end)

    dedicated_keys = final_state |> Map.keys() |> MapSet.new()

    partial_dedicated? =
      shared_uncovered_live_compound?(shared_state.live_keys, dedicated_keys)

    dedicated_empty? = map_size(final_state) == 0
    shared_live? = shared_collection_live?(shared_state)
    shared_dead? = shared_collection_dead?(shared_state)
    dedicated_live? = dedicated_collection_live?(final_state, redis_key, now)
    dedicated_dead? = dedicated_collection_dead?(final_state, redis_key, now)

    cond do
      shared_live? and (dedicated_empty? or partial_dedicated? or not dedicated_live?) ->
        validate_shared_recovery_type!(
          shared_state,
          redis_key,
          type,
          shard_data_path,
          shard_index
        )

        {:fallback, marker_key, redis_key, type, dedicated_path}

      dedicated_dead? or (dedicated_empty? and shared_dead?) ->
        {:cleanup, marker_key, redis_key, type, dedicated_path, shared_state.all_keys}

      dedicated_empty? ->
        fail_recovery!(:scan_dedicated_log, dedicated_path, shard_index, :dedicated_state_missing)

      partial_dedicated? ->
        fail_recovery!(
          :scan_dedicated_log,
          dedicated_path,
          shard_index,
          :incomplete_without_live_shared_source
        )

      true ->
        require_live_recovered_type!(final_state, redis_key, dedicated_path, shard_index, now)
        total_bytes = recovery_log_bytes!(log_files, shard_index)

        row_actions =
          plan_recovered_rows!(final_state, redis_key, type, shard_index, instance_ctx, now)

        live_bytes =
          Enum.reduce(final_state, 0, fn
            {key, {:live, _fid, _path, _offset, value_size, expire_at_ms}}, total
            when value_size > 0 and (expire_at_ms == 0 or expire_at_ms > now) ->
              total + @log_header_size + byte_size(key) + value_size

            {_key, _entry}, total ->
              total
          end)

        promoted = %{
          path: dedicated_path,
          writes: 0,
          total_bytes: total_bytes,
          dead_bytes: max(total_bytes - live_bytes, 0),
          last_compacted_at: nil
        }

        {:promoted, marker_key, redis_key, promoted, row_actions}
    end
  end

  defp require_live_recovered_type!(
         final_state,
         redis_key,
         dedicated_path,
         shard_index,
         now
       ) do
    type_key = CompoundKey.type_key(redis_key)

    case Map.get(final_state, type_key) do
      {:live, _fid, _file_path, _offset, _value_size, expire_at_ms}
      when expire_at_ms == 0 or expire_at_ms > now ->
        :ok

      _missing_or_tombstoned ->
        fail_recovery!(
          :scan_dedicated_log,
          dedicated_path,
          shard_index,
          {:dedicated_type_missing, type_key}
        )
    end
  end

  defp dedicated_collection_live?(final_state, redis_key, now) do
    type_key = CompoundKey.type_key(redis_key)

    recovery_entry_live?(Map.get(final_state, type_key), now) and
      Enum.any?(final_state, fn
        {^type_key, _entry} -> false
        {_key, entry} -> recovery_entry_live?(entry, now)
      end)
  end

  defp dedicated_collection_dead?(final_state, redis_key, now) when map_size(final_state) > 0 do
    type_key = CompoundKey.type_key(redis_key)
    type_entry = Map.get(final_state, type_key)

    live_member? =
      Enum.any?(final_state, fn
        {^type_key, _entry} -> false
        {_key, entry} -> recovery_entry_live?(entry, now)
      end)

    cond do
      recovery_entry_expired?(type_entry, now) -> true
      recovery_entry_live?(type_entry, now) and not live_member? -> true
      type_entry == :tombstone and not live_member? -> true
      is_nil(type_entry) and not live_member? -> true
      true -> false
    end
  end

  defp dedicated_collection_dead?(_final_state, _redis_key, _now), do: false

  defp recovery_entry_live?(
         {:live, _fid, _file_path, _offset, _value_size, expire_at_ms},
         now
       ),
       do: expire_at_ms == 0 or expire_at_ms > now

  defp recovery_entry_live?(_entry, _now), do: false

  defp recovery_entry_expired?(
         {:live, _fid, _file_path, _offset, _value_size, expire_at_ms},
         now
       ),
       do: expire_at_ms > 0 and expire_at_ms <= now

  defp recovery_entry_expired?(_entry, _now), do: false

  defp open_recovery_dedicated!(data_dir, shard_index, type, redis_key) do
    case open_dedicated(data_dir, shard_index, type, redis_key) do
      {:ok, path} ->
        path

      {:error, reason} ->
        fail_recovery!(:open_dedicated, redis_key, shard_index, reason)

      other ->
        fail_recovery!(:open_dedicated, redis_key, shard_index, {:unexpected, other})
    end
  end

  defp recover_dedicated_log!(
         file_path,
         fid,
         state,
         shard_index,
         redis_key,
         type,
         active?,
         shared_keys,
         now
       ) do
    recover_dedicated_log_pages!(
      file_path,
      fid,
      0,
      recovery_scan_page_size(),
      state,
      shard_index,
      redis_key,
      type,
      active?,
      shared_keys,
      now
    )
  end

  defp recover_dedicated_log_pages!(
         file_path,
         fid,
         offset,
         page_size,
         state,
         shard_index,
         redis_key,
         type,
         active?,
         shared_keys,
         now
       ) do
    case NIF.v2_scan_file_page(file_path, offset, page_size) do
      {:ok, records, next_offset, done?}
      when is_list(records) and is_integer(next_offset) and next_offset >= offset and
             is_boolean(done?) ->
        next_state =
          Enum.reduce(records, state, fn
            {key, _record_offset, _value_size, _expire_at_ms, true}, acc ->
              validate_recovered_key!(key, redis_key, type, file_path, shard_index)

              if key == CompoundKey.type_key(redis_key) or MapSet.member?(shared_keys, key) do
                Map.put(acc, key, :tombstone)
              else
                Map.delete(acc, key)
              end

            {key, record_offset, value_size, expire_at_ms, false}, acc ->
              validate_recovered_key!(key, redis_key, type, file_path, shard_index)

              if expire_at_ms > 0 and expire_at_ms <= now and
                   key != CompoundKey.type_key(redis_key) and
                   not MapSet.member?(shared_keys, key) do
                Map.delete(acc, key)
              else
                Map.put(
                  acc,
                  key,
                  {:live, fid, file_path, record_offset, value_size, expire_at_ms}
                )
              end

            invalid_record, _acc ->
              fail_recovery!(
                :scan_dedicated_log,
                file_path,
                shard_index,
                {:invalid_record, invalid_record}
              )
          end)

        run_recovery_state_hook(map_size(next_state))

        cond do
          done? ->
            validate_scan_stop!(file_path, next_offset, active?, shard_index)
            next_state

          next_offset > offset ->
            recover_dedicated_log_pages!(
              file_path,
              fid,
              next_offset,
              page_size,
              next_state,
              shard_index,
              redis_key,
              type,
              active?,
              shared_keys,
              now
            )

          true ->
            fail_recovery!(
              :scan_dedicated_log,
              file_path,
              shard_index,
              {:non_advancing_scan, offset}
            )
        end

      {:error, reason} ->
        fail_recovery!(:scan_dedicated_log, file_path, shard_index, reason)

      other ->
        fail_recovery!(
          :scan_dedicated_log,
          file_path,
          shard_index,
          {:unexpected, other}
        )
    end
  end

  defp run_recovery_state_hook(retained_rows) do
    case Process.get(:ferricstore_promotion_recovery_state_hook) do
      fun when is_function(fun, 1) -> fun.(retained_rows)
      _ -> :ok
    end
  end

  defp validate_recovered_key!(key, redis_key, type, file_path, shard_index) do
    type_key = CompoundKey.type_key(redis_key)
    prefix = compound_prefix_for(type, redis_key)

    unless key == type_key or (is_binary(key) and String.starts_with?(key, prefix)) do
      fail_recovery!(
        :scan_dedicated_log,
        file_path,
        shard_index,
        {:foreign_record, key, redis_key, type}
      )
    end
  end

  defp validate_scan_stop!(file_path, valid_end, active?, shard_index) do
    case open_recovery_tail_file(file_path, active?) do
      {:ok, file} ->
        try do
          validate_scan_stop_file!(file, file_path, valid_end, active?, shard_index)
        after
          :file.close(file)
        end

      {:error, reason} ->
        fail_recovery!(:open_dedicated_tail, file_path, shard_index, reason)
    end
  end

  defp open_recovery_tail_file(file_path, active?) do
    modes = if active?, do: [:read, :write, :raw, :binary], else: [:read, :raw, :binary]

    with {:ok, %File.Stat{type: :regular} = expected_stat} <- File.lstat(file_path),
         :ok <- run_recovery_tail_open_hook(),
         {:ok, file} <- :file.open(file_path, modes) do
      case verify_recovery_file_identity(file, expected_stat) do
        :ok ->
          {:ok, file}

        {:error, _reason} = error ->
          :file.close(file)
          error
      end
    else
      {:ok, %File.Stat{type: type}} -> {:error, {:unsafe_file_type, type}}
      {:error, _reason} = error -> error
    end
  end

  defp run_recovery_tail_open_hook do
    case Process.get(:ferricstore_promotion_recovery_tail_open_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  defp verify_recovery_file_identity(
         file,
         %File.Stat{major_device: major_device, minor_device: minor_device, inode: inode}
       ) do
    case :file.read_file_info(file) do
      {:ok, info}
      when elem(info, 2) == :regular and elem(info, 9) == major_device and
             elem(info, 10) == minor_device and elem(info, 11) == inode ->
        :ok

      _ ->
        {:error, :file_identity_changed}
    end
  end

  defp validate_scan_stop_file!(file, file_path, valid_end, active?, shard_index) do
    case :file.read_file_info(file) do
      {:ok, info} when elem(info, 1) == valid_end ->
        :ok

      {:ok, info} when elem(info, 1) > valid_end ->
        file_size = elem(info, 1)

        case classify_recovery_tail!(file, file_path, valid_end, file_size, shard_index) do
          :torn when active? ->
            repair_recovery_tail!(file, file_path, valid_end, shard_index)

          :torn ->
            fail_recovery!(
              :scan_dedicated_log,
              file_path,
              shard_index,
              {:torn_tail_in_sealed_log, valid_end, file_size}
            )

          :complete_record ->
            fail_recovery!(
              :scan_dedicated_log,
              file_path,
              shard_index,
              {:complete_record_rejected, valid_end, file_size}
            )
        end

      {:ok, info} ->
        fail_recovery!(
          :scan_dedicated_log,
          file_path,
          shard_index,
          {:scan_past_file_end, valid_end, elem(info, 1)}
        )

      {:error, reason} ->
        fail_recovery!(:stat_dedicated_log, file_path, shard_index, reason)
    end
  end

  defp classify_recovery_tail!(file, file_path, valid_end, file_size, shard_index) do
    remaining = file_size - valid_end

    if remaining < @log_header_size do
      :torn
    else
      header = read_recovery_header!(file, valid_end, shard_index)

      <<_crc::little-unsigned-32, _timestamp::little-unsigned-64,
        _expire_at_ms::little-unsigned-64, key_size::little-unsigned-16,
        value_size::little-unsigned-32>> = header

      record_size =
        cond do
          value_size == @tombstone_value_size ->
            @log_header_size + key_size

          value_size <= @max_log_value_size ->
            @log_header_size + key_size + value_size

          true ->
            fail_recovery!(
              :scan_dedicated_log,
              file_path,
              shard_index,
              {:invalid_tail_value_size, value_size, valid_end}
            )
        end

      if remaining < record_size, do: :torn, else: :complete_record
    end
  end

  defp read_recovery_header!(file, offset, shard_index) do
    case :file.pread(file, offset, @log_header_size) do
      {:ok, header} when byte_size(header) == @log_header_size ->
        header

      other ->
        fail_recovery!(
          :read_dedicated_tail,
          offset,
          shard_index,
          {:unexpected, other}
        )
    end
  end

  defp repair_recovery_tail!(file, file_path, valid_end, shard_index) do
    truncate_result =
      with {:ok, ^valid_end} <- :file.position(file, valid_end),
           :ok <- :file.truncate(file) do
        :file.sync(file)
      end

    case truncate_result do
      :ok ->
        :ok

      {:error, reason} ->
        fail_recovery!(:repair_dedicated_tail, file_path, shard_index, reason)

      other ->
        fail_recovery!(
          :repair_dedicated_tail,
          file_path,
          shard_index,
          {:unexpected, other}
        )
    end
  end

  defp plan_recovered_rows!(final_state, redis_key, type, shard_index, instance_ctx, now) do
    hot_cache_threshold = recover_hot_cache_threshold(instance_ctx)
    expected_type_key = CompoundKey.type_key(redis_key)
    expected_type_value = CompoundKey.encode_type(type)

    Enum.map(final_state, fn
      {key, :tombstone} ->
        {:delete, key}

      {key, {:live, _fid, _file_path, _offset, _value_size, expire_at_ms}}
      when expire_at_ms > 0 and expire_at_ms <= now ->
        {:delete, key}

      {key, {:live, fid, file_path, offset, value_size, expire_at_ms}} ->
        disk_value =
          read_recovery_value!(
            file_path,
            offset,
            key,
            :read_dedicated_record,
            shard_index
          )

        if key == expected_type_key and disk_value != expected_type_value do
          fail_recovery!(
            :read_dedicated_record,
            {file_path, offset, key},
            shard_index,
            {:type_value_mismatch, expected_type_value, disk_value}
          )
        end

        value = recovered_value_for_ets(disk_value, hot_cache_threshold, instance_ctx)
        {:put, key, value, expire_at_ms, fid, offset, value_size}
    end)
  end

  defp apply_recovery_plan!(
         marker_actions,
         recovery_plan,
         shard_data_path,
         keydir,
         shard_index,
         instance_ctx
       ) do
    rollback_fallbacks!(recovery_plan, shard_data_path, shard_index)

    rolled_back_markers =
      :ets.foldl(
        fn
          {_sequence, {:fallback, marker_key, _redis_key, _type, _dedicated_path}}, markers ->
            MapSet.put(markers, marker_key)

          {_sequence, {:cleanup, marker_key, _redis_key, _type, _dedicated_path, _shared_keys}},
          markers ->
            MapSet.put(markers, marker_key)

          {_sequence, {:promoted, _marker_key, _redis_key, _promoted, _row_actions}}, markers ->
            markers
        end,
        MapSet.new(),
        recovery_plan
      )

    Enum.each(marker_actions, fn
      {:hydrate_marker, marker_key, type_str} ->
        unless MapSet.member?(rolled_back_markers, marker_key) do
          track_binary_insert(keydir, shard_index, marker_key, type_str, instance_ctx)

          unless :ets.update_element(keydir, marker_key, {2, type_str}) do
            fail_recovery!(
              :apply_recovery_plan,
              marker_key,
              shard_index,
              :marker_disappeared
            )
          end
        end

      {:delete_invalid_marker, marker_key, type_str} ->
        Logger.warning(
          "Promotion recovery: ignoring invalid marker #{inspect(marker_key)} with type #{inspect(type_str)}"
        )

        track_binary_delete(keydir, shard_index, marker_key, instance_ctx)
        :ets.delete(keydir, marker_key)
    end)

    :ets.foldl(
      fn
        {_sequence, {:fallback, marker_key, redis_key, _type, _dedicated_path}}, :ok ->
          track_binary_delete(keydir, shard_index, marker_key, instance_ctx)
          :ets.delete(keydir, marker_key)

          Logger.info(
            "Promotion recovery: marker for #{inspect(redis_key)} exists but dedicated " <>
              "dir is incomplete; falling back to compound keys in shared log."
          )

          :ok

        {_sequence, {:cleanup, marker_key, redis_key, _type, _dedicated_path, shared_keys}},
        :ok ->
          Enum.each(shared_keys, fn key ->
            track_binary_delete(keydir, shard_index, key, instance_ctx)
            :ets.delete(keydir, key)
          end)

          track_binary_delete(keydir, shard_index, marker_key, instance_ctx)
          :ets.delete(keydir, marker_key)

          Logger.info(
            "Promotion recovery: removing expired or empty promoted collection " <>
              "#{inspect(redis_key)}."
          )

          :ok

        {_sequence, {:promoted, _marker_key, _redis_key, _promoted, row_actions}}, :ok ->
          Enum.each(row_actions, fn
            {:delete, key} ->
              track_binary_delete(keydir, shard_index, key, instance_ctx)
              :ets.delete(keydir, key)

            {:put, key, value, expire_at_ms, fid, offset, value_size} ->
              track_binary_insert(keydir, shard_index, key, value, instance_ctx)

              :ets.insert(
                keydir,
                {key, value, expire_at_ms, LFU.initial(), fid, offset, value_size}
              )
          end)

          :ok
      end,
      :ok,
      recovery_plan
    )
  end

  defp rollback_fallbacks!(recovery_plan, shard_data_path, shard_index) do
    :ets.foldl(
      fn
        {_sequence, {:fallback, marker_key, redis_key, type, dedicated_path}}, :ok ->
          rollback_fallback!(
            marker_key,
            redis_key,
            type,
            dedicated_path,
            shard_data_path,
            shard_index
          )

        {_sequence, {:cleanup, marker_key, redis_key, type, dedicated_path, shared_keys}}, :ok ->
          rollback_cleanup!(
            marker_key,
            redis_key,
            type,
            dedicated_path,
            shared_keys,
            shard_data_path,
            shard_index
          )

        {_sequence, {:promoted, _marker_key, _redis_key, _promoted, _row_actions}}, :ok ->
          :ok
      end,
      :ok,
      recovery_plan
    )
  end

  defp rollback_fallback!(
         marker_key,
         redis_key,
         type,
         dedicated_path,
         shard_data_path,
         shard_index
       ) do
    active_path = recovery_active_shared_path!(shard_data_path, shard_index)
    persist_recovery_intent!(active_path, marker_key, redis_key, type, :fallback, shard_index)
    remove_recovery_dedicated!(dedicated_path, redis_key, shard_index)

    case NIF.v2_append_tombstone(active_path, marker_key) do
      {:ok, _offset} ->
        :ok

      {:error, reason} ->
        fail_recovery!(
          :rollback_incomplete_dedicated,
          redis_key,
          shard_index,
          {:tombstone_marker, reason}
        )

      other ->
        fail_recovery!(
          :rollback_incomplete_dedicated,
          redis_key,
          shard_index,
          {:unexpected_tombstone_result, other}
        )
    end
  end

  defp rollback_cleanup!(
         marker_key,
         redis_key,
         type,
         dedicated_path,
         shared_keys,
         shard_data_path,
         shard_index
       ) do
    active_path = recovery_active_shared_path!(shard_data_path, shard_index)
    persist_recovery_intent!(active_path, marker_key, redis_key, type, :cleanup, shard_index)
    remove_recovery_dedicated!(dedicated_path, redis_key, shard_index)

    cleanup_ops =
      shared_keys
      |> Enum.sort()
      |> Enum.map(&{:delete, &1})
      |> Kernel.++([{:delete, marker_key}])

    case append_recovery_cleanup_ops(active_path, cleanup_ops) do
      {:ok, locations} ->
        case AppendResult.validate_operation_locations(locations, cleanup_ops) do
          :ok ->
            :ok

          {:error, reason} ->
            fail_recovery!(
              :cleanup_expired_promoted,
              redis_key,
              shard_index,
              {:tombstone_records, reason}
            )
        end

      {:error, reason} ->
        fail_recovery!(
          :cleanup_expired_promoted,
          redis_key,
          shard_index,
          {:tombstone_records, reason}
        )

      other ->
        fail_recovery!(
          :cleanup_expired_promoted,
          redis_key,
          shard_index,
          {:unexpected_tombstone_result, other}
        )
    end
  end

  defp append_recovery_cleanup_ops(active_path, cleanup_ops) do
    case Process.get(:ferricstore_promotion_recovery_cleanup_append_hook) do
      hook when is_function(hook, 2) ->
        case hook.(active_path, cleanup_ops) do
          :passthrough -> NIF.v2_append_ops_batch(active_path, cleanup_ops)
          result -> result
        end

      _missing ->
        NIF.v2_append_ops_batch(active_path, cleanup_ops)
    end
  end

  defp persist_recovery_intent!(
         active_path,
         marker_key,
         redis_key,
         type,
         intent,
         shard_index
       ) do
    intent_value = recovery_intent_value(intent, type)

    case NIF.v2_append_record(active_path, marker_key, intent_value, 0) do
      {:ok, {_offset, _value_size}} ->
        :ok

      {:error, reason} ->
        fail_recovery!(
          :persist_cleanup_intent,
          redis_key,
          shard_index,
          {intent, reason}
        )

      other ->
        fail_recovery!(
          :persist_cleanup_intent,
          redis_key,
          shard_index,
          {intent, {:unexpected, other}}
        )
    end
  end

  defp remove_recovery_dedicated!(dedicated_path, redis_key, shard_index) do
    case Ferricstore.FS.rm_rf(dedicated_path) do
      :ok ->
        :ok

      {:error, reason} ->
        fail_recovery!(
          :rollback_incomplete_dedicated,
          redis_key,
          shard_index,
          {:remove_directory, reason}
        )
    end

    case fsync_dir(Path.dirname(dedicated_path), :rollback_incomplete_dedicated) do
      :ok ->
        :ok

      {:error, reason} ->
        fail_recovery!(
          :rollback_incomplete_dedicated,
          redis_key,
          shard_index,
          reason
        )
    end

    :ok
  end

  defp recovery_active_shared_path!(shard_data_path, shard_index) do
    case list_log_files_result(shard_data_path) do
      {:ok, []} ->
        fail_recovery!(:list_shared_logs, shard_data_path, shard_index, :no_log_files)

      {:ok, log_files} ->
        log_files |> List.last() |> elem(1)

      {:error, reason} ->
        fail_recovery!(:list_shared_logs, shard_data_path, shard_index, reason)
    end
  end

  defp recovery_scan_page_size do
    case Application.get_env(
           :ferricstore,
           :recovery_scan_page_size,
           @default_recovery_scan_page_size
         ) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_recovery_scan_page_size
    end
  end

  defp read_recovery_value!(path, offset, key, operation, shard_index) do
    result =
      case Process.get(:ferricstore_promotion_recovery_read_hook) do
        fun when is_function(fun, 3) -> fun.(path, offset, key)
        _ -> read_cold_async(path, offset, key)
      end

    case result do
      {:ok, value} when is_binary(value) ->
        value

      {:error, reason} ->
        fail_recovery!(operation, {path, offset, key}, shard_index, reason)

      {:ok, nil} ->
        fail_recovery!(operation, {path, offset, key}, shard_index, :record_not_found)

      other ->
        fail_recovery!(operation, {path, offset, key}, shard_index, {:unexpected, other})
    end
  end

  defp recovery_log_files!(dir, shard_index) do
    case list_log_files_result(dir) do
      {:ok, []} ->
        fail_recovery!(:list_dedicated_logs, dir, shard_index, :no_log_files)

      {:ok, files} ->
        files

      {:error, reason} ->
        fail_recovery!(:list_dedicated_logs, dir, shard_index, reason)
    end
  end

  defp recovery_log_bytes!(log_files, shard_index) do
    Enum.reduce(log_files, 0, fn {_fid, path}, total ->
      case File.lstat(path) do
        {:ok, %File.Stat{type: :regular, size: size}} ->
          total + size

        {:ok, %File.Stat{type: type}} ->
          fail_recovery!(:stat_dedicated_log, path, shard_index, {:unsafe_file_type, type})

        {:error, reason} ->
          fail_recovery!(:stat_dedicated_log, path, shard_index, reason)
      end
    end)
  end

  defp fail_recovery!(operation, subject, shard_index, reason) do
    message =
      "promotion recovery #{operation} failed for #{inspect(subject)} on shard " <>
        "#{shard_index}: #{inspect(reason)}"

    Logger.error(message)
    raise message
  end

  defp recover_hot_cache_threshold(%{hot_cache_max_value_size: threshold})
       when is_integer(threshold) and threshold >= 0,
       do: threshold

  defp recover_hot_cache_threshold(_instance_ctx), do: 65_536

  defp recovered_value_for_ets(disk_value, hot_cache_threshold, instance_ctx) do
    if BlobValue.threshold(instance_ctx) > 0 and BlobRef.ref?(disk_value) do
      nil
    else
      ShardETS.value_for_ets(disk_value, hot_cache_threshold)
    end
  end

  defp build_shared_recovery_index!(_index, _keydir, _shard_index, false), do: :ok

  defp build_shared_recovery_index!(index, keydir, shard_index, true) do
    CompoundMemberIndex.reset(index)

    :ets.foldl(
      fn
        {key, _value, expire_at_ms, _lfu, _fid, _offset, _value_size}, :ok
        when is_binary(key) and is_integer(expire_at_ms) and expire_at_ms >= 0 ->
          CompoundMemberIndex.put(index, key)

        row, :ok ->
          fail_recovery!(:index_shared_compound, keydir, shard_index, {:invalid_keydir_row, row})
      end,
      :ok,
      keydir
    )
  rescue
    ArgumentError ->
      fail_recovery!(:index_shared_compound, keydir, shard_index, :keydir_unavailable)
  end

  defp shared_compound_recovery_state!(index, keydir, redis_key, type, now, shard_index) do
    type_key = CompoundKey.type_key(redis_key)

    state =
      case :ets.lookup(keydir, type_key) do
        [{^type_key, value, expire_at_ms, _lfu, fid, offset, _value_size}]
        when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
          add_shared_recovery_key(
            empty_shared_recovery_state(),
            type_key,
            expire_at_ms,
            redis_key,
            value,
            fid,
            offset,
            now
          )

        [] ->
          empty_shared_recovery_state()

        invalid ->
          fail_recovery!(
            :index_shared_compound,
            type_key,
            shard_index,
            {:invalid_keydir_entry, invalid}
          )
      end

    prefix = compound_prefix_for(type, redis_key)

    case CompoundMemberIndex.reduce_rows_while(
           index,
           %{keydir: keydir},
           prefix,
           state,
           fn
             {key, value, expire_at_ms, _lfu, fid, offset, _value_size}, acc
             when is_binary(key) and is_integer(expire_at_ms) and expire_at_ms >= 0 ->
               {:cont,
                add_shared_recovery_key(
                  acc,
                  key,
                  expire_at_ms,
                  redis_key,
                  value,
                  fid,
                  offset,
                  now
                )}

             row, _acc ->
               fail_recovery!(
                 :index_shared_compound,
                 redis_key,
                 shard_index,
                 {:invalid_keydir_row, row}
               )
           end
         ) do
      {:ok, shared_state} ->
        shared_state

      {:error, reason} ->
        fail_recovery!(:index_shared_compound, redis_key, shard_index, reason)

      :unavailable ->
        fail_recovery!(:index_shared_compound, redis_key, shard_index, :index_unavailable)

      other ->
        fail_recovery!(:index_shared_compound, redis_key, shard_index, {:unexpected, other})
    end
  rescue
    ArgumentError ->
      fail_recovery!(:index_shared_compound, redis_key, shard_index, :keydir_unavailable)
  end

  defp empty_shared_recovery_state do
    %{
      all_keys: MapSet.new(),
      live_keys: MapSet.new(),
      live_member_keys: MapSet.new(),
      type_status: :missing,
      type_record: nil
    }
  end

  defp add_shared_recovery_key(
         state,
         key,
         expire_at_ms,
         redis_key,
         value,
         fid,
         offset,
         now
       ) do
    live? = expire_at_ms == 0 or expire_at_ms > now
    state = %{state | all_keys: MapSet.put(state.all_keys, key)}

    state =
      if live? do
        %{state | live_keys: MapSet.put(state.live_keys, key)}
      else
        state
      end

    if key == CompoundKey.type_key(redis_key) do
      %{
        state
        | type_status: if(live?, do: :live, else: :expired),
          type_record: {key, value, fid, offset}
      }
    else
      if live? do
        %{state | live_member_keys: MapSet.put(state.live_member_keys, key)}
      else
        state
      end
    end
  end

  defp validate_shared_recovery_type!(
         shared_state,
         redis_key,
         type,
         shard_data_path,
         shard_index
       ) do
    expected_type = CompoundKey.encode_type(type)

    actual_type =
      case shared_state.type_record do
        {_key, value, _fid, _offset} when is_binary(value) ->
          value

        {type_key, _value, fid, offset} ->
          file_path =
            Path.join(
              shard_data_path,
              "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
            )

          read_recovery_value!(
            file_path,
            offset,
            type_key,
            :read_shared_type,
            shard_index
          )

        nil ->
          fail_recovery!(
            :validate_shared_type,
            redis_key,
            shard_index,
            :shared_type_missing
          )
      end

    if actual_type != expected_type do
      fail_recovery!(
        :validate_shared_type,
        redis_key,
        shard_index,
        {:shared_type_mismatch, expected_type, actual_type}
      )
    end
  end

  defp shared_collection_live?(state) do
    state.type_status == :live and MapSet.size(state.live_member_keys) > 0
  end

  defp shared_collection_dead?(state) do
    MapSet.size(state.all_keys) > 0 and
      (state.type_status == :expired or MapSet.size(state.live_member_keys) == 0)
  end

  defp decode_recovery_marker("hash"), do: {:ok, :hash, :promoted}
  defp decode_recovery_marker("set"), do: {:ok, :set, :promoted}
  defp decode_recovery_marker("zset"), do: {:ok, :zset, :promoted}
  defp decode_recovery_marker("fallback:hash"), do: {:ok, :hash, {:intent, :fallback}}
  defp decode_recovery_marker("fallback:set"), do: {:ok, :set, {:intent, :fallback}}
  defp decode_recovery_marker("fallback:zset"), do: {:ok, :zset, {:intent, :fallback}}
  defp decode_recovery_marker("cleanup:hash"), do: {:ok, :hash, {:intent, :cleanup}}
  defp decode_recovery_marker("cleanup:set"), do: {:ok, :set, {:intent, :cleanup}}
  defp decode_recovery_marker("cleanup:zset"), do: {:ok, :zset, {:intent, :cleanup}}
  defp decode_recovery_marker(_type_str), do: :error

  defp recovery_intent_value(intent, type) when intent in [:fallback, :cleanup] do
    Atom.to_string(intent) <> ":" <> CompoundKey.encode_type(type)
  end

  defp shared_uncovered_live_compound?(shared_keys, dedicated_keys) do
    Enum.any?(shared_keys, fn key -> not MapSet.member?(dedicated_keys, key) end)
  end

  @spec cleanup_promoted!(
          binary(),
          :hash | :set | :zset,
          binary(),
          binary(),
          atom(),
          binary(),
          non_neg_integer(),
          term()
        ) :: :ok
  def cleanup_promoted!(
        redis_key,
        type,
        resolved_path,
        shard_data_path,
        keydir,
        data_dir,
        shard_index,
        instance_ctx \\ nil
      )
      when type in [:hash, :set, :zset] and is_binary(resolved_path) do
    expected_path = dedicated_path(data_dir, shard_index, type, redis_key)

    if Path.expand(resolved_path) != Path.expand(expected_path) do
      raise ArgumentError,
            "resolved promotion path mismatch: expected #{inspect(expected_path)}, got #{inspect(resolved_path)}"
    end

    mk = marker_key(redis_key)
    intent_value = recovery_intent_value(:cleanup, type)
    type_label = type_label(type)
    active_path = recovery_active_shared_path!(shard_data_path, shard_index)

    # The intent makes directory-first cleanup retryable if the final marker
    # tombstone fails after the dedicated state has already been removed.
    case NIF.v2_append_record(active_path, mk, intent_value, 0) do
      {:ok, {offset, value_size}} ->
        marker_fid = file_id_from_path(active_path)
        track_binary_insert(keydir, shard_index, mk, intent_value, instance_ctx)

        :ets.insert(
          keydir,
          {mk, intent_value, 0, LFU.initial(), marker_fid, offset, value_size}
        )

      {:error, reason} ->
        Logger.error(
          "Promotion cleanup: marker fence failed for #{inspect(mk)}: #{inspect(reason)}"
        )

        raise "promotion cleanup marker fence failed: #{inspect(reason)}"
    end

    case Ferricstore.FS.rm_rf(resolved_path) do
      :ok -> :ok
      {:error, reason} -> raise "promotion cleanup directory removal failed: #{inspect(reason)}"
    end

    case fsync_dir(Path.dirname(resolved_path), :remove_dedicated_dir) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "promotion cleanup directory fsync failed: #{inspect(reason)}"
    end

    case NIF.v2_append_tombstone(active_path, mk) do
      {:ok, _offset} ->
        track_binary_delete(keydir, shard_index, mk, instance_ctx)
        :ets.delete(keydir, mk)

      {:error, reason} ->
        Logger.error(
          "Promotion cleanup: marker tombstone failed for #{inspect(mk)}: #{inspect(reason)}"
        )

        raise "promotion cleanup marker tombstone failed: #{inspect(reason)}"
    end

    Logger.debug("Cleaned up promoted #{type_label} #{inspect(redis_key)} (shard #{shard_index})")

    :ok
  end

  @spec compound_prefix_for(atom(), binary()) :: binary()
  defp compound_prefix_for(:hash, redis_key), do: CompoundKey.hash_prefix(redis_key)
  defp compound_prefix_for(:set, redis_key), do: CompoundKey.set_prefix(redis_key)
  defp compound_prefix_for(:zset, redis_key), do: CompoundKey.zset_prefix(redis_key)

  defp promotion_entry_value(_location_ctx, _key, value, _fid, _off) when value != nil,
    do: {:ok, value}

  defp promotion_entry_value(
         {_shard_data_path, instance_ctx, shard_index},
         key,
         nil,
         {tag, index} = file_id,
         off
       )
       when is_map(instance_ctx) and is_integer(shard_index) and shard_index >= 0 and
              tag in [:waraft_segment, :waraft_projection, :waraft_apply_projection] and
              is_integer(index) and index > 0 and is_integer(off) and off >= 0 do
    case WARaftSegmentReader.read_value_from_location(instance_ctx, shard_index, file_id, key) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      :not_found -> {:error, :record_not_found}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_waraft_segment_read_result, other}}
    end
  end

  defp promotion_entry_value({shard_data_path, _instance_ctx, _shard_index}, key, nil, fid, off)
       when is_integer(fid) and fid >= 0 and is_integer(off) and off >= 0 do
    file_path =
      Path.join(shard_data_path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")

    case read_cold_async(file_path, off, key) do
      {:ok, value} when value != nil -> {:ok, value}
      {:ok, nil} -> {:error, :record_not_found}
      {:error, reason} -> {:error, reason}
      nil -> {:error, :record_not_found}
      other -> {:error, {:unexpected_cold_read_result, other}}
    end
  end

  defp promotion_entry_value(_location_ctx, _key, _value, fid, off),
    do: {:error, {:invalid_cold_location, fid, off}}

  defp read_cold_async(path, offset, key) do
    Ferricstore.Store.ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms)
  end

  @spec type_label(atom()) :: binary()
  defp type_label(:hash), do: "hash"
  defp type_label(:set), do: "set"
  defp type_label(:zset), do: "zset"

  # Finds the active (highest numbered) .log file in a shard data directory.
  @doc "Returns the active (highest file_id) log file path in a dedicated directory."
  @spec find_active(binary()) :: binary()
  def find_active(path) do
    case list_log_files_result(path) do
      {:ok, []} ->
        Path.join(path, "00000.log")

      {:ok, files} ->
        files |> List.last() |> elem(1)

      {:error, reason} ->
        raise "promotion active-file discovery failed for #{inspect(path)}: #{inspect(reason)}"
    end
  end

  defp compaction_latch(owner, redis_key) do
    latch(owner, {:promoted_compaction, redis_key})
  end

  defp shared_log_latch(owner) do
    latch(owner, :compound_promotion_shared_log)
  end

  defp failure_fence(owner, redis_key) do
    case latch_table(owner) do
      nil -> nil
      tab -> {tab, {:compound_promotion_failure, redis_key}}
    end
  end

  defp latch_table(owner) do
    with %FerricStore.Instance{} = ctx <- latch_context(owner),
         idx when is_integer(idx) and idx >= 0 <- latch_index(owner),
         true <- idx < tuple_size(ctx.latch_refs) do
      elem(ctx.latch_refs, idx)
    else
      _ -> nil
    end
  end

  defp latch(owner, latch_key) do
    case {latch_table(owner), latch_index(owner)} do
      {tab, idx} when tab != nil and is_integer(idx) -> {tab, latch_key, idx}
      _missing -> nil
    end
  end

  defp latch_context(%FerricStore.Instance{} = ctx), do: ctx
  defp latch_context(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx

  defp latch_context(%{instance_name: name}) when is_atom(name) do
    FerricStore.Instance.get(name)
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp latch_context(_owner), do: nil

  defp latch_index(%{index: index}), do: index
  defp latch_index(%{shard_index: index}), do: index
  defp latch_index(_owner), do: nil

  defp acquire_compaction_latch(tab, latch_key, shard_index) do
    case :ets.insert_new(tab, {latch_key, self()}) do
      true ->
        :ok

      false ->
        wait_compaction_latch_clear!(tab, latch_key, shard_index)
        acquire_compaction_latch(tab, latch_key, shard_index)
    end
  end

  defp try_acquire_latch(tab, latch_key) do
    case :ets.insert_new(tab, {latch_key, self()}) do
      true ->
        {:ok, {tab, latch_key}}

      false ->
        case :ets.lookup(tab, latch_key) do
          [{^latch_key, owner}] when is_pid(owner) ->
            if Process.alive?(owner) do
              :busy
            else
              :ets.delete_object(tab, {latch_key, owner})

              case :ets.insert_new(tab, {latch_key, self()}) do
                true -> {:ok, {tab, latch_key}}
                false -> :busy
              end
            end

          _other ->
            :busy
        end
    end
  end

  defp wait_compaction_latch_clear!(tab, latch_key, shard_index) do
    emit_compaction_latch_event(:blocked, shard_index, latch_key, 0)

    started_ms = System.monotonic_time(:millisecond)
    timeout_ms = compaction_latch_timeout_ms()

    case wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms) do
      :ok ->
        :ok

      {:error, {:timeout, wait_ms}} ->
        Logger.error(
          "Promoted compaction latch timeout after #{wait_ms}ms for #{inspect(latch_key)} " <>
            "on shard #{inspect(shard_index)}"
        )

        raise "compaction latch timeout after #{wait_ms}ms for #{inspect(latch_key)}"
    end
  end

  defp wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms) do
    wait_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)

    if wait_ms >= timeout_ms do
      emit_compaction_latch_event(:timeout, shard_index, latch_key, wait_ms)
      {:error, {:timeout, wait_ms}}
    else
      do_wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms)
    end
  end

  defp do_wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms) do
    case :ets.lookup(tab, latch_key) do
      [] ->
        :ok

      [{^latch_key, owner}] ->
        wait_for_latch_owner(tab, latch_key, owner, shard_index, started_ms, timeout_ms)
    end
  end

  defp wait_for_latch_owner(tab, latch_key, owner, shard_index, started_ms, timeout_ms)
       when is_pid(owner) do
    if Process.alive?(owner) do
      Process.sleep(@compaction_latch_sleep_ms)
    else
      :ets.delete_object(tab, {latch_key, owner})
    end

    wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms)
  end

  defp wait_for_latch_owner(tab, latch_key, _owner, shard_index, started_ms, timeout_ms) do
    Process.sleep(@compaction_latch_sleep_ms)
    wait_compaction_latch_clear(tab, latch_key, shard_index, started_ms, timeout_ms)
  end

  defp compaction_latch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :promotion_compaction_latch_timeout_ms,
      @default_compaction_latch_timeout_ms
    )
  end

  defp emit_compaction_latch_event(status, shard_index, latch_key, wait_ms) do
    :telemetry.execute(
      [:ferricstore, :promotion, :compaction_latch],
      %{count: 1, wait_ms: wait_ms},
      %{
        status: status,
        shard_index: shard_index,
        redis_key_hash: compaction_latch_key_hash(latch_key)
      }
    )
  end

  defp compaction_latch_key_hash({:promoted_compaction, redis_key}) when is_binary(redis_key),
    do: :erlang.phash2(redis_key)

  defp compaction_latch_key_hash(_latch_key), do: :unknown

  defp file_id_from_path(path) do
    path
    |> Path.basename()
    |> String.trim_trailing(".log")
    |> String.to_integer()
  end

  defp list_log_files_result(dir) do
    case Ferricstore.FS.ls(dir) do
      {:ok, files} ->
        cleanup_leftover_compaction_temp_files(dir, files)

        log_files =
          files
          |> Enum.flat_map(fn name ->
            case numeric_log_file_id(dir, name) do
              {:ok, fid} -> [{fid, Path.join(dir, name)}]
              :skip -> []
            end
          end)
          |> Enum.sort_by(fn {fid, _} -> fid end)

        {:ok, log_files}

      {:error, _reason} = error ->
        error
    end
  end

  defp numeric_log_file_id(dir, name) do
    with true <- String.ends_with?(name, ".log"),
         false <- String.starts_with?(name, "compact_"),
         stem <- String.trim_trailing(name, ".log"),
         {fid, ""} <- Integer.parse(stem),
         true <- fid >= 0,
         {:ok, %File.Stat{type: :regular}} <- File.lstat(Path.join(dir, name)) do
      {:ok, fid}
    else
      _ -> :skip
    end
  end

  defp cleanup_leftover_compaction_temp_files(dir, files) do
    Enum.each(files, fn name ->
      if String.starts_with?(name, "compact_") and String.ends_with?(name, ".log") do
        path = Path.join(dir, name)

        case Ferricstore.FS.rm(path) do
          :ok ->
            :ok

          {:error, reason} ->
            :telemetry.execute(
              [:ferricstore, :promotion, :compact_temp_cleanup_failed],
              %{count: 1},
              %{path: path, name: name, reason: reason}
            )

            Logger.warning(
              "Promotion recovery: failed to remove leftover compact temp file #{name} " <>
                "at #{path}: #{inspect(reason)}"
            )
        end
      end
    end)
  end

  # -- Off-heap binary byte tracking --

  defp keydir_binary_ref(%{keydir_binary_bytes: ref, shard_count: count}, shard_index)
       when ref != nil do
    if shard_index < count, do: ref, else: nil
  end

  defp keydir_binary_ref(name, shard_index) when is_atom(name) do
    keydir_binary_ref_for_instance(name, shard_index)
  end

  defp keydir_binary_ref(_instance_ctx, shard_index) do
    keydir_binary_ref_for_instance(:default, shard_index)
  end

  defp keydir_binary_ref_for_instance(name, shard_index) do
    try do
      %{keydir_binary_bytes: ref, shard_count: count} = FerricStore.Instance.get(name)
      if ref != nil and shard_index < count, do: ref, else: nil
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  defp track_binary_insert(keydir, shard_index, key, new_val, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      new_bytes = offheap_size(key) + offheap_size(new_val)

      old_bytes =
        case :ets.lookup(keydir, key) do
          [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
          _ -> 0
        end

      delta = new_bytes - old_bytes
      if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
    end
  end

  defp track_binary_delete(keydir, shard_index, key, instance_ctx) do
    ref = keydir_binary_ref(instance_ctx, shard_index)

    if ref do
      bytes =
        case :ets.lookup(keydir, key) do
          [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
          _ -> 0
        end

      if bytes > 0, do: :atomics.sub(ref, shard_index + 1, bytes)
    end
  end

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0
end
