defmodule Ferricstore.Raft.StateMachine.FlushDerivedState do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.Strings.Delete

  alias Ferricstore.Flow.{
    HistoryProjector,
    LMDB,
    LMDBReplaySafeIndex,
    LMDBWriter,
    NativeOrderedIndex,
    SharedRefBackfill
  }

  alias Ferricstore.Flow.LMDBWriter.Telemetry, as: LMDBWriterTelemetry
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  @spec stream_roots(map(), [binary()]) :: [binary()]
  def stream_roots(state, keys) when is_map(state) and is_list(keys) do
    Enum.reduce(keys, [], fn
      <<"T:", _::binary>> = type_key, roots ->
        case :ets.lookup(state.ets, type_key) do
          [{^type_key, "stream", _expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
            [CompoundKey.extract_redis_key(type_key) | roots]

          _not_a_stream ->
            roots
        end

      _other_key, roots ->
        roots
    end)
  rescue
    error in ArgumentError ->
      raise ArgumentError, "flush stream-root scan failed: #{Exception.message(error)}"
  end

  @spec clear_stream_roots(map(), [binary()]) :: :ok | {:error, term()}
  def clear_stream_roots(_state, []), do: :ok

  def clear_stream_roots(state, roots) when is_map(state) and is_list(roots) do
    result =
      case Process.get(:ferricstore_flush_stream_cleanup_hook) do
        hook when is_function(hook, 2) ->
          hook.(state, roots)

        _missing ->
          store = %{cache_scope: Map.get(state, :instance_name, :default)}
          Enum.each(roots, &Delete.cleanup_stream_metadata(&1, store))
          :ok
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:stream_cache_cleanup_failed, reason}}
      other -> {:error, {:stream_cache_cleanup_failed, {:unexpected_result, other}}}
    end
  rescue
    error -> {:error, {:stream_cache_cleanup_failed, error}}
  catch
    kind, reason -> {:error, {:stream_cache_cleanup_failed, kind, reason}}
  end

  @spec clear(map(), non_neg_integer() | nil) :: {:ok, map()} | {:error, term()}
  def clear(state, ra_index) when is_map(state) do
    instance_name = Map.get(state, :instance_name, :default)
    instance_ctx = Map.get(state, :instance_ctx)
    shard_index = Map.fetch!(state, :shard_index)
    shard_data_path = Map.fetch!(state, :shard_data_path)
    flow_lmdb_path = Map.fetch!(state, :flow_lmdb_path)
    history_ctx = instance_ctx || %{name: instance_name}

    result =
      with :ok <- discard_lmdb_writer(instance_name, shard_index),
           :ok <- clear_lmdb_projection_outbox(instance_name, shard_index),
           :ok <- discard_history_projector(history_ctx, shard_index),
           :ok <- clear_lmdb(flow_lmdb_path),
           :ok <- clear_history(shard_data_path),
           :ok <- reset_projection_watermarks(state, history_ctx, ra_index),
           :ok <- invalidate_shared_ref_verification(instance_name, shard_index),
           {:ok, finalized_state} <- finalize_shared_ref_backfill(state, history_ctx),
           :ok <- clear_mirror_degraded(state),
           :ok <- reset_native_flow_index(state) do
        {:ok, finalized_state}
      end

    case result do
      {:ok, finalized_state} -> {:ok, finalized_state}
      {:error, reason} -> {:error, {:flush_derived_state_cleanup_failed, reason}}
      other -> {:error, {:flush_derived_state_cleanup_failed, {:unexpected_result, other}}}
    end
  rescue
    error -> {:error, {:flush_derived_state_cleanup_failed, error}}
  catch
    kind, reason -> {:error, {:flush_derived_state_cleanup_failed, kind, reason}}
  end

  defp discard_lmdb_writer(instance_name, shard_index) do
    case LMDBWriter.discard(instance_name, shard_index) do
      :ok -> :ok
      {:error, reason} -> {:error, {:lmdb_writer_discard_failed, reason}}
      other -> {:error, {:lmdb_writer_discard_failed, other}}
    end
  end

  defp clear_lmdb_projection_outbox(instance_name, shard_index) do
    table = LMDBWriter.projection_outbox_name(instance_name, shard_index)

    case :ets.whereis(table) do
      :undefined ->
        :ok

      _table ->
        :ets.delete_all_objects(table)
        :ok
    end
  rescue
    error -> {:error, {:lmdb_projection_outbox_clear_failed, error}}
  end

  defp discard_history_projector(instance_ctx, shard_index) do
    case HistoryProjector.discard(instance_ctx, shard_index, 5_000) do
      :ok -> :ok
      {:error, reason} -> {:error, {:history_projector_discard_failed, reason}}
      other -> {:error, {:history_projector_discard_failed, other}}
    end
  end

  defp clear_lmdb(path) do
    result =
      case Application.get_env(:ferricstore, :flush_derived_lmdb_clear_hook) do
        hook when is_function(hook, 1) -> hook.(path)
        _missing -> LMDB.clear(path)
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:lmdb_clear_failed, reason}}
      other -> {:error, {:lmdb_clear_failed, other}}
    end
  end

  defp clear_history(shard_data_path) do
    history_dir = HistoryProjector.history_dir(shard_data_path)

    with :ok <- remove_history_dir(history_dir),
         :ok <- fsync_shard_dir(shard_data_path) do
      :ok
    end
  end

  defp remove_history_dir(history_dir) do
    case Ferricstore.FS.rm_rf(history_dir) do
      :ok -> :ok
      {:error, reason} -> {:error, {:history_dir_remove_failed, history_dir, reason}}
    end
  end

  defp fsync_shard_dir(shard_data_path) do
    case NIF.v2_fsync_dir(shard_data_path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:history_dir_fsync_failed, shard_data_path, reason}}
      other -> {:error, {:history_dir_fsync_failed, shard_data_path, other}}
    end
  end

  defp reset_projection_watermarks(_state, _history_ctx, nil), do: :ok

  defp reset_projection_watermarks(state, history_ctx, ra_index)
       when is_integer(ra_index) and ra_index >= 0 do
    with :ok <- reset_lmdb_watermark(state, ra_index),
         :ok <-
           HistoryProjector.reset_projected_index(
             history_ctx,
             state.shard_index,
             state.shard_data_path,
             ra_index
           ),
         :ok <- HistoryProjector.reset_after_flush(history_ctx, state.shard_index, ra_index),
         :ok <- reset_lmdb_writer(state, ra_index) do
      :ok
    end
  end

  defp reset_lmdb_watermark(state, ra_index) do
    with :ok <- LMDBReplaySafeIndex.reset(state.shard_data_path, ra_index) do
      LMDBWriterTelemetry.reset_replay_safe(
        Map.get(state, :instance_ctx),
        state.shard_index,
        ra_index
      )
    end
  end

  defp reset_lmdb_writer(state, _ra_index) do
    case LMDBWriter.resume_after_snapshot_install(state.instance_name, state.shard_index) do
      :ok -> :ok
      {:error, reason} -> {:error, {:lmdb_writer_reset_failed, reason}}
      other -> {:error, {:lmdb_writer_reset_failed, other}}
    end
  end

  defp invalidate_shared_ref_verification(instance_name, shard_index) do
    SharedRefBackfill.invalidate_verified_shard!(instance_name, shard_index)
  end

  defp finalize_shared_ref_backfill(state, instance_ctx) do
    SharedRefBackfill.finalize_empty_shard!(
      state.shard_data_path,
      state.ets,
      state.shard_index,
      instance_ctx,
      active_file_id: state.active_file_id,
      active_file_path: state.active_file_path
    )

    reconcile_active_file(state)
  rescue
    error -> {:error, {:shared_ref_backfill_finalize_failed, error}}
  catch
    kind, reason -> {:error, {:shared_ref_backfill_finalize_failed, kind, reason}}
  end

  defp reconcile_active_file(state) do
    file_stats = ShardFlush.compute_file_stats(state.shard_data_path, state.ets)

    case Map.fetch(file_stats, state.active_file_id) do
      {:ok, {active_file_size, _dead_bytes}}
      when is_integer(active_file_size) and active_file_size >= 0 ->
        {:ok, %{state | active_file_size: active_file_size, file_stats: file_stats}}

      :error ->
        {:error, {:active_file_stats_missing, state.active_file_id, state.active_file_path}}

      {:ok, invalid} ->
        {:error,
         {:invalid_active_file_stats, state.active_file_id, state.active_file_path, invalid}}
    end
  end

  defp clear_mirror_degraded(state) do
    LMDBWriterTelemetry.clear_mirror_degraded(
      Map.get(state, :instance_ctx),
      state.shard_index
    )
  end

  defp reset_native_flow_index(state) do
    _resource =
      NativeOrderedIndex.reset(
        Map.fetch!(state, :flow_index_name),
        Map.fetch!(state, :flow_lookup_name)
      )

    :ok
  rescue
    error -> {:error, {:native_flow_index_reset_failed, error}}
  end
end
