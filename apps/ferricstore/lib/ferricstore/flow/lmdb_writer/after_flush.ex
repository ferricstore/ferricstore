defmodule Ferricstore.Flow.LMDBWriter.AfterFlush do
  @moduledoc false

  alias Ferricstore.Flow.Hibernation
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.Flow.Locator
  alias Ferricstore.Store.{BlobRef, BlobStore, ColdRead, Keydir}

  @max_timer_ms 4_294_967_295

  def apply_actions(actions), do: apply_actions(actions, %{})

  def apply_actions(actions, publication_ctx) when is_list(actions) do
    Enum.reduce_while(actions, :ok, fn action, :ok ->
      case apply_after_flush(action, publication_ctx) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  rescue
    _ -> {:error, :after_flush_action_failed}
  catch
    _, _ -> {:error, :after_flush_action_failed}
  end

  def apply_actions(_actions, _publication_ctx), do: {:error, :invalid_after_flush_actions}

  def apply_after_flush(action), do: apply_after_flush(action, %{})

  def apply_after_flush(
        {:prune_terminal_flow, data_dir, shard_index, ets, zset_index, zset_lookup, flow_index,
         flow_lookup, state_key, type, terminal_state, partition_key, parent_flow_id,
         root_flow_id, correlation_id, id, version, incarnation},
        publication_ctx
      ) do
    state_index_key = Keys.state_index_key(type, terminal_state, partition_key)

    metadata_index_keys =
      ProjectionOps.terminal_project_metadata_index_keys(
        id,
        partition_key,
        parent_flow_id,
        root_flow_id,
        correlation_id
      )

    cleanup = fn ->
      with :ok <- safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id),
           :ok <- safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id),
           :ok <- delete_flow_index_members(flow_index, flow_lookup, metadata_index_keys, id) do
        :ok
      end
    end

    identity = %{version: version, state: terminal_state, incarnation: incarnation}

    case prune_terminal_state_key(
           data_dir,
           shard_index,
           ets,
           state_key,
           identity,
           publication_ctx,
           cleanup
         ) do
      :ok -> :ok
      :stale -> :ok
      {:error, _reason} = error -> error
    end
  end

  def apply_after_flush(
        {:prune_terminal_flow_from_source, data_dir, shard_index, ets, zset_index, zset_lookup,
         flow_index, flow_lookup, state_key, version},
        publication_ctx
      ) do
    case flow_record_with_keydir_row(data_dir, shard_index, ets, state_key) do
      {:ok, record, source_row} ->
        with ^version <- Map.get(record, :version),
             terminal_state when is_binary(terminal_state) <- Map.get(record, :state),
             true <- Ferricstore.Flow.LMDB.terminal_state?(terminal_state),
             type when is_binary(type) <- Map.get(record, :type),
             id when is_binary(id) <- Map.get(record, :id),
             :ok <- terminal_source_projected(data_dir, shard_index, state_key, record) do
          partition_key = Map.get(record, :partition_key)
          state_index_key = Keys.state_index_key(type, terminal_state, partition_key)

          metadata_index_keys =
            ProjectionOps.terminal_project_metadata_index_keys(
              id,
              partition_key,
              Map.get(record, :parent_flow_id),
              Map.get(record, :root_flow_id),
              Map.get(record, :correlation_id)
            )

          cleanup = fn ->
            with :ok <- safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id),
                 :ok <-
                   safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id),
                 :ok <-
                   delete_flow_index_members(flow_index, flow_lookup, metadata_index_keys, id) do
              :ok
            end
          end

          identity = %{
            version: version,
            state: terminal_state,
            incarnation: Map.get(record, :incarnation),
            record: record
          }

          case prune_terminal_state_key(
                 data_dir,
                 shard_index,
                 ets,
                 state_key,
                 identity,
                 publication_ctx,
                 cleanup,
                 source_row
               ) do
            :ok -> :ok
            :stale -> :ok
            {:error, _reason} = error -> error
          end
        else
          {:error, _reason} = error -> error
          _stale_or_nonterminal -> :ok
        end

      :not_found ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  def apply_after_flush(
        {:hibernate_flow_evict_hot,
         %{
           data_dir: data_dir,
           shard_index: shard_index,
           ets: ets,
           flow_index: flow_index,
           flow_lookup: flow_lookup,
           state_key: state_key,
           record: record,
           locator: %Locator{} = locator
         } = attrs},
        publication_ctx
      ) do
    zset_index = Map.get(attrs, :zset_index)
    zset_lookup = Map.get(attrs, :zset_lookup)
    id = Map.fetch!(record, :id)
    index_keys = Hibernation.hot_index_keys(record, due_any?: true)

    cleanup = fn ->
      delete_hot_index_members(
        zset_index,
        zset_lookup,
        flow_index,
        flow_lookup,
        index_keys,
        id
      )
    end

    case hibernate_delete_hot_state_key(
           data_dir,
           shard_index,
           ets,
           state_key,
           locator,
           publication_ctx,
           cleanup
         ) do
      {:ok, true} ->
        Hibernation.maybe_schedule_claim_waiter(record)
        emit_hibernation_evict(:evicted, shard_index)
        :ok

      {:ok, false} ->
        emit_hibernation_evict(:stale, shard_index)
        :ok

      {:error, _reason} = error ->
        emit_hibernation_evict(:error, shard_index)
        error
    end
  end

  def apply_after_flush({:cleanup_stale_cold_due, path, batches}, _publication_ctx) do
    Hibernation.cleanup_stale_due_batches(path, batches)
  end

  def apply_after_flush({:defer_after_flush, delay_ms, action}, publication_ctx) do
    delay_ms = normalize_delay_ms(delay_ms)

    if delay_ms > 0 do
      Process.send_after(self(), {:apply_after_flush, action}, delay_ms)
      :ok
    else
      apply_after_flush(action, publication_ctx)
    end
  end

  def apply_after_flush({:delete_flow_tombstone, ets, key}, _publication_ctx) do
    case :ets.lookup(ets, key) do
      [{^key, nil, 0, :flow_state_deleted, :deleted, 0, 0}] ->
        :ets.delete(ets, key)
        :ok

      _ ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  def apply_after_flush(_action, _publication_ctx), do: {:error, :invalid_after_flush_action}

  def hot_flow_record_from_ets(ets, state_key) do
    case :ets.lookup(ets, state_key) do
      [{^state_key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}]
      when is_binary(value) ->
        decode_raw_flow_record(value)

      _ ->
        :not_found
    end
  rescue
    ArgumentError -> :not_found
  end

  def decode_raw_flow_record(value) when is_binary(value) do
    case Ferricstore.Flow.decode_record(value) do
      record when is_map(record) -> {:ok, record}
      _invalid -> {:error, :invalid_source_flow_record}
    end
  rescue
    _ -> {:error, :invalid_source_flow_record}
  end

  def hibernate_delete_hot_state_key(data_dir, shard_index, ets, state_key, %Locator{} = locator) do
    hibernate_delete_hot_state_key(
      data_dir,
      shard_index,
      ets,
      state_key,
      locator,
      %{},
      fn -> :ok end
    )
  end

  defp hibernate_delete_hot_state_key(
         data_dir,
         shard_index,
         ets,
         state_key,
         %Locator{} = locator,
         publication_ctx,
         cleanup
       )
       when is_function(cleanup, 0) do
    case :ets.lookup(ets, state_key) do
      [{^state_key, _value, _expire_at_ms, _lfu, file_id, offset, value_size} = row] ->
        if file_id == locator.file_id and offset == locator.offset and
             value_size == locator.value_size do
          with :ok <- ensure_apply_projection_row_durable(data_dir, shard_index, row) do
            with_publication_write(publication_ctx, shard_index, fn ->
              if Keydir.delete_unchanged_source(ets, row) do
                after_hot_delete_hook(:hibernate, ets, row)

                case cleanup.() do
                  :ok -> {:ok, true}
                  {:error, _reason} = error -> error
                end
              else
                {:ok, false}
              end
            end)
          end
        else
          {:ok, false}
        end

      _ ->
        {:ok, false}
    end
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  def normalize_delay_ms(delay_ms) when is_integer(delay_ms) and delay_ms >= 0,
    do: min(delay_ms, @max_timer_ms)

  def normalize_delay_ms(_delay_ms), do: 0

  def normalize_non_negative_integer(value, _default) when is_integer(value) and value >= 0,
    do: value

  def normalize_non_negative_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> parsed
      _ -> default
    end
  end

  def normalize_non_negative_integer(_value, default), do: default

  def terminal_hot_ttl_ms do
    :ferricstore
    |> Application.get_env(:flow_terminal_hot_ttl_ms, 0)
    |> normalize_non_negative_integer(0)
    |> normalize_delay_ms()
  end

  def safe_zset_delete_member(nil, _zset_lookup, _state_index_key, _id), do: :ok
  def safe_zset_delete_member(_zset_index, nil, _state_index_key, _id), do: :ok

  def safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
      when is_atom(zset_index) and is_atom(zset_lookup) do
    case {:ets.whereis(zset_index), :ets.whereis(zset_lookup)} do
      {:undefined, :undefined} -> :ok
      {:undefined, _lookup} -> {:error, :zset_index_unavailable}
      {_index, :undefined} -> {:error, :zset_index_unavailable}
      {_index, _lookup} -> do_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
    end
  end

  def safe_zset_delete_member(zset_index, zset_lookup, state_index_key, id) do
    do_zset_delete_member(zset_index, zset_lookup, state_index_key, id)
  end

  defp do_zset_delete_member(zset_index, zset_lookup, state_index_key, id) do
    Ferricstore.Store.Shard.ZSetIndex.delete_member(
      zset_index,
      zset_lookup,
      state_index_key,
      id
    )
  rescue
    ArgumentError -> {:error, :zset_index_unavailable}
    _ -> {:error, :zset_index_delete_failed}
  end

  def safe_flow_index_delete_member(nil, _flow_lookup, _state_index_key, _id), do: :ok
  def safe_flow_index_delete_member(_flow_index, nil, _state_index_key, _id), do: :ok

  def safe_flow_index_delete_member(flow_index, flow_lookup, state_index_key, id) do
    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil -> :ok
      native -> Ferricstore.Flow.NativeOrderedIndex.delete_member(native, state_index_key, id)
    end
  rescue
    ArgumentError -> {:error, :flow_index_unavailable}
    _ -> {:error, :flow_index_delete_failed}
  end

  defp delete_flow_index_members(flow_index, flow_lookup, index_keys, id) do
    Enum.reduce_while(index_keys, :ok, fn index_key, :ok ->
      case safe_flow_index_delete_member(flow_index, flow_lookup, index_key, id) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp delete_hot_index_members(
         zset_index,
         zset_lookup,
         flow_index,
         flow_lookup,
         index_keys,
         id
       ) do
    Enum.reduce_while(index_keys, :ok, fn index_key, :ok ->
      with :ok <- safe_zset_delete_member(zset_index, zset_lookup, index_key, id),
           :ok <- safe_flow_index_delete_member(flow_index, flow_lookup, index_key, id) do
        {:cont, :ok}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp emit_hibernation_evict(result, shard_index) do
    :telemetry.execute(
      [:ferricstore, :flow, :hibernation, :evict_hot],
      %{count: 1},
      %{result: result, shard_index: shard_index}
    )
  end

  defp terminal_source_projected(data_dir, shard_index, state_key, record) do
    path =
      data_dir |> Ferricstore.DataDir.shard_data_path(shard_index) |> Ferricstore.Flow.LMDB.path()

    with {:ok, encoded} <- Ferricstore.Flow.LMDB.get(path, state_key),
         {:ok, %{record: projected}} <-
           Ferricstore.Flow.Query.QueryRowCodec.decode(encoded, state_key) do
      # State-entry sequences are monotonic across incarnations and retained in
      # QueryRows. Version alone can repeat when an expired Flow ID is reused.
      seq = Map.get(record, :state_enter_seq)

      if is_integer(seq) and Map.get(projected, :state_enter_seq) == seq and
           Map.get(projected, :version) == Map.get(record, :version) and
           Map.get(projected, :state) == Map.get(record, :state),
         do: :ok,
         else: :stale
    else
      :not_found -> :stale
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_terminal_projection}
    end
  end

  def prune_terminal_state_key(data_dir, shard_index, ets, state_key, version) do
    prune_terminal_state_key(
      data_dir,
      shard_index,
      ets,
      state_key,
      %{version: version},
      %{},
      fn -> :ok end
    )
  end

  defp prune_terminal_state_key(
         data_dir,
         shard_index,
         ets,
         state_key,
         identity,
         publication_ctx,
         cleanup,
         expected_row \\ nil
       )
       when is_function(cleanup, 0) do
    rows = if expected_row, do: [expected_row], else: :ets.lookup(ets, state_key)

    case rows do
      [row] ->
        with :ok <- ensure_apply_projection_row_durable(data_dir, shard_index, row),
             :ok <- terminal_prune_row_matches?(data_dir, shard_index, state_key, row, identity) do
          with_publication_write(publication_ctx, shard_index, fn ->
            case delete_pruned_keydir_entry(ets, row) do
              :ok -> cleanup.()
              :stale -> :stale
            end
          end)
        end

      [] ->
        before_publication_write_hook(:prune_absent, ets, state_key)

        with_publication_write(publication_ctx, shard_index, fn ->
          case :ets.lookup(ets, state_key) do
            [] -> cleanup.()
            _newer_row -> :stale
          end
        end)
    end
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp with_publication_write(publication_ctx, shard_index, fun)
       when is_function(fun, 0) do
    Ferricstore.Store.PublicationEpoch.with_write(publication_ctx, shard_index, fun)
  end

  defp delete_pruned_keydir_entry(ets, row) do
    if Keydir.delete_unchanged_source(ets, row) do
      after_hot_delete_hook(:terminal, ets, row)
      :ok
    else
      :stale
    end
  end

  defp terminal_prune_row_matches?(
         _data_dir,
         _shard_index,
         _state_key,
         _row,
         %{record: record} = identity
       ) do
    terminal_prune_record_matches?(record, identity)
  end

  defp terminal_prune_row_matches?(data_dir, shard_index, state_key, row, identity) do
    case flow_record_from_keydir_row(data_dir, shard_index, state_key, row) do
      {:ok, record} ->
        terminal_prune_record_matches?(record, identity)

      :not_found ->
        :stale

      {:error, _reason} = error ->
        error
    end
  end

  defp terminal_prune_record_matches?(record, identity) do
    expected_version = Map.get(identity, :version)
    expected_state = Map.get(identity, :state)
    expected_incarnation = Map.get(identity, :incarnation)

    cond do
      Map.get(record, :version) != expected_version ->
        :stale

      not Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
        :stale

      is_binary(expected_state) and Map.get(record, :state) != expected_state ->
        :stale

      is_integer(expected_incarnation) and
          Map.get(record, :incarnation) != expected_incarnation ->
        :stale

      true ->
        :ok
    end
  end

  if Mix.env() == :test do
    defp before_publication_write_hook(kind, ets, state_key) do
      case Process.get(:ferricstore_after_flush_before_publication_write_hook) do
        hook when is_function(hook, 3) -> hook.(kind, ets, state_key)
        _ -> :ok
      end
    end

    defp after_hot_delete_hook(kind, ets, row) do
      case Process.get(:ferricstore_after_flush_hot_delete_hook) do
        hook when is_function(hook, 3) -> hook.(kind, ets, row)
        _ -> :ok
      end
    end
  else
    defp before_publication_write_hook(_kind, _ets, _state_key), do: :ok
    defp after_hot_delete_hook(_kind, _ets, _row), do: :ok
  end

  def delete_apply_projection_cache_for_row(
        data_dir,
        shard_index,
        {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
         _value_size}
      )
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(key) and is_integer(index) and index > 0 do
    Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(data_dir, shard_index, [
      {index, key}
    ])

    :ok
  rescue
    _ -> :ok
  end

  def delete_apply_projection_cache_for_row(_data_dir, _shard_index, _row), do: :ok

  defp ensure_apply_projection_row_durable(
         data_dir,
         shard_index,
         {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
          _value_size}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_integer(index) and index > 0 do
    case Ferricstore.Raft.WARaftSegmentReader.ensure_apply_projection_entries_durable(
           data_dir,
           shard_index,
           [{index, key}]
         ) do
      {:ok, _removed} -> :ok
      {:error, reason} -> {:error, {:source_not_durable, reason}}
    end
  end

  defp ensure_apply_projection_row_durable(_data_dir, _shard_index, _row), do: :ok

  def flow_record_from_keydir(data_dir, shard_index, ets, state_key) do
    case flow_record_with_keydir_row(data_dir, shard_index, ets, state_key) do
      {:ok, record, _row} -> {:ok, record}
      result -> result
    end
  end

  defp flow_record_with_keydir_row(data_dir, shard_index, ets, state_key) do
    do_flow_record_with_keydir_row(
      data_dir,
      shard_index,
      ets,
      state_key,
      ProjectionOps.source_pending_retries(),
      ProjectionOps.source_pending_sleep_ms()
    )
  end

  defp do_flow_record_with_keydir_row(
         data_dir,
         shard_index,
         ets,
         state_key,
         pending_retries,
         pending_sleep_ms
       ) do
    case :ets.lookup(ets, state_key) do
      [row] ->
        case flow_record_from_keydir_row(data_dir, shard_index, state_key, row) do
          {:ok, record} ->
            {:ok, record, row}

          {:error, {:source_pending, ^state_key}} when pending_retries > 0 ->
            Process.sleep(pending_sleep_ms)

            do_flow_record_with_keydir_row(
              data_dir,
              shard_index,
              ets,
              state_key,
              pending_retries - 1,
              pending_sleep_ms
            )

          result ->
            result
        end

      _missing ->
        :not_found
    end
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  def flow_record_from_keydir_row(
        data_dir,
        shard_index,
        state_key,
        {state_key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}
      )
      when is_binary(value) do
    decode_keydir_flow_value(data_dir, shard_index, value)
  end

  def flow_record_from_keydir_row(
        _data_dir,
        _shard_index,
        state_key,
        {state_key, _value, _expire_at_ms, _lfu, :pending, _offset, _value_size}
      ),
      do: {:error, {:source_pending, state_key}}

  def flow_record_from_keydir_row(
        data_dir,
        shard_index,
        state_key,
        {state_key, nil, _expire_at_ms, _lfu, {kind, _index} = file_id, _offset, _value_size}
      )
      when kind in [:waraft_segment, :waraft_projection, :waraft_apply_projection] do
    ctx = %{data_dir: data_dir}

    with {:ok, value} <-
           Ferricstore.Raft.WARaftSegmentReader.read_value_from_location_including_expired(
             ctx,
             shard_index,
             file_id,
             state_key
           ) do
      decode_keydir_flow_value(data_dir, shard_index, value)
    end
  end

  def flow_record_from_keydir_row(
        data_dir,
        shard_index,
        state_key,
        {state_key, nil, _expire_at_ms, _lfu, file_id, offset, value_size}
      ) do
    with {:ok, value} <-
           read_keydir_flow_value(data_dir, shard_index, state_key, file_id, offset, value_size) do
      decode_keydir_flow_value(data_dir, shard_index, value)
    end
  end

  def flow_record_from_keydir_row(_data_dir, _shard_index, _state_key, _row), do: :not_found

  defp decode_keydir_flow_value(data_dir, shard_index, value) when is_binary(value) do
    with {:ok, materialized} <- maybe_materialize_blob_ref(data_dir, shard_index, value) do
      decode_raw_flow_record(materialized)
    end
  end

  defp maybe_materialize_blob_ref(data_dir, shard_index, value) when is_binary(value) do
    case BlobRef.decode(value) do
      {:ok, %BlobRef{} = ref} -> BlobStore.get(data_dir, shard_index, ref)
      :error -> {:ok, value}
    end
  end

  defp read_keydir_flow_value(data_dir, shard_index, state_key, file_id, offset, value_size)
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(state_key) and is_integer(file_id) and file_id >= 0 and
              is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 do
    data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Store.Shard.ETS.file_path(file_id)
    |> ColdRead.pread_keyed(offset, state_key, 10_000)
  end

  defp read_keydir_flow_value(
         _data_dir,
         _shard_index,
         _state_key,
         _file_id,
         _offset,
         _value_size
       ),
       do: :not_found
end
