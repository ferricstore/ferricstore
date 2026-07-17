defmodule Ferricstore.Store.Router.BlobGC do
  @moduledoc false

  @blob_ref_encoded_size Ferricstore.Store.BlobRef.encoded_size()

  def sweep_blob_garbage(ctx) do
    initial = blob_gc_empty_stats()

    result =
      Enum.reduce_while(
        Range.new(0, :erlang.-(effective_shard_count(ctx), 1)),
        {:ok, initial},
        fn idx, {:ok, acc} ->
          case sweep_blob_garbage_shard(ctx, idx) do
            {:ok, %{skipped: true} = stats} ->
              {:cont, {:ok, blob_gc_merge_stats(acc, :maps.merge(blob_gc_empty_stats(), stats))}}

            {:ok, stats} ->
              {:cont, {:ok, blob_gc_merge_stats(acc, stats)}}

            {:error, reason} ->
              emit_blob_gc_failed(ctx, idx, reason)
              {:halt, {:error, {idx, reason}}}
          end
        end
      )

    case result do
      {:ok, stats} ->
        emit_blob_gc(ctx, stats)
        {:ok, stats}

      {:error, _reason} = error ->
        error
    end
  end

  defp sweep_blob_garbage_shard(ctx, idx) do
    try do
      with {:ok, hardened_ids, hardened_stats} <- blob_gc_reconcile_hardened_protections(ctx, idx),
           state <- :sys.get_state(resolve_shard(ctx, idx)),
           :ok <- blob_gc_prepare_replay_safe(ctx, state, idx),
           :ok <- blob_gc_replay_safe?(ctx, state, idx),
           :ok <- blob_gc_fsync_active_file(state),
           {:ok, stats} <-
             Ferricstore.Store.BlobStore.sweep_unreferenced_releasing_hardened_with_live_refs(
               ctx.data_dir,
               idx,
               hardened_ids,
               fn ->
                 with {:ok, live_refs} <- blob_gc_live_refs(ctx, idx, state),
                      :ok <- blob_gc_after_live_refs_hook(ctx, idx, live_refs) do
                   {:ok, live_refs}
                 end
               end
             ) do
        {:ok, blob_gc_merge_stats(:maps.merge(blob_gc_empty_stats(), hardened_stats), stats)}
      end
    catch
      :exit, reason -> {:error, {:blob_gc_shard_unavailable, reason}}
    end
  end

  defp blob_gc_reconcile_hardened_protections(ctx, idx) do
    case :erlang.==(
           Application.get_env(:ferricstore, :blob_protection_reconcile_enabled, true),
           false
         ) do
      false ->
        limit = blob_gc_reconcile_limit()
        ids = Ferricstore.Store.BlobStore.hardened_protection_ids(ctx.data_dir, idx, limit)

        case ids do
          [] ->
            {:ok, [], %{hardened_protections_seen: 0, hardened_protections_blocked: 0}}

          [_ | _] ->
            timeout_ms = blob_gc_reconcile_barrier_timeout_ms()

            with :ok <- Ferricstore.Raft.WARaftBackend.blob_protection_barrier(idx, timeout_ms),
                 :ok <- blob_gc_wait_replay_safe(ctx, idx, timeout_ms) do
              emit_blob_protection_reconcile(ctx, idx, :erlang.length(ids), 0)

              {:ok, ids,
               %{hardened_protections_seen: :erlang.length(ids), hardened_protections_blocked: 0}}
            else
              {:ok, %{skipped: true, reason: reason}} ->
                emit_blob_protection_reconcile_failed(ctx, idx, :erlang.length(ids), reason)

                {:ok, [],
                 %{
                   hardened_protections_seen: :erlang.length(ids),
                   hardened_protections_blocked: :erlang.length(ids)
                 }}

              {:error, reason} ->
                emit_blob_protection_reconcile_failed(ctx, idx, :erlang.length(ids), reason)

                {:ok, [],
                 %{
                   hardened_protections_seen: :erlang.length(ids),
                   hardened_protections_blocked: :erlang.length(ids)
                 }}
            end
        end

      true ->
        {:ok, [], %{hardened_protections_blocked: 0}}
    end
  end

  defp blob_gc_wait_replay_safe(ctx, idx, timeout_ms) do
    try do
      state = :sys.get_state(resolve_shard(ctx, idx))

      with :ok <- blob_gc_prepare_replay_safe(ctx, state, idx, timeout_ms) do
        blob_gc_replay_safe?(ctx, state, idx)
      end
    catch
      :exit, reason -> {:error, {:blob_gc_replay_wait_shard_unavailable, reason}}
    end
  end

  defp blob_gc_reconcile_limit() do
    case Application.get_env(:ferricstore, :blob_protection_reconcile_max_records, 1000) do
      value when :erlang.andalso(:erlang.is_integer(value), :erlang.>(value, 0)) -> value
      _other -> 1000
    end
  end

  defp blob_gc_reconcile_barrier_timeout_ms() do
    case Application.get_env(:ferricstore, :blob_protection_reconcile_barrier_timeout_ms, 30_000) do
      value when :erlang.andalso(:erlang.is_integer(value), :erlang.>=(value, 0)) -> value
      _other -> 30_000
    end
  end

  defp blob_gc_replay_safe?(ctx, state, idx) do
    cond do
      blob_gc_active_waraft_ctx?(ctx) ->
        blob_gc_waraft_replay_safe?(idx)

      Map.get(ctx, :name) == :default or Map.get(state, :raft?) == true ->
        blob_gc_skipped(:missing_waraft_storage_metrics)

      true ->
        :ok
    end
  end

  defp blob_gc_prepare_replay_safe(ctx, state, idx) do
    blob_gc_prepare_replay_safe(ctx, state, idx, blob_gc_reconcile_barrier_timeout_ms())
  end

  defp blob_gc_prepare_replay_safe(ctx, state, idx, timeout_ms) do
    cond do
      blob_gc_active_waraft_ctx?(ctx) ->
        case blob_gc_waraft_replay_safe?(idx) do
          :ok ->
            :ok

          {:ok, %{reason: {:waraft_storage_replay_gap, _applied, _durable}}} ->
            case Ferricstore.Raft.WARaftBackend.flush_storage_replay_dependencies(idx, timeout_ms) do
              :ok -> :ok
              {:error, reason} -> blob_gc_skipped({:waraft_storage_durability_failed, reason})
            end

          {:ok, %{skipped: true} = skipped} ->
            {:ok, skipped}
        end

      Map.get(ctx, :name) == :default or Map.get(state, :raft?) == true ->
        blob_gc_skipped(:missing_waraft_storage_metrics)

      true ->
        :ok
    end
  end

  defp blob_gc_waraft_replay_safe?(idx) do
    case Ferricstore.Raft.WARaftBackend.storage_status(idx) do
      {:ok, status} when is_list(status) ->
        blob_gc_waraft_status_replay_safe?(status)

      {:error, reason} ->
        blob_gc_skipped({:waraft_storage_unavailable, reason})

      _invalid ->
        blob_gc_skipped(:missing_waraft_storage_metrics)
    end
  end

  defp blob_gc_waraft_status_replay_safe?(status) do
    applied = Keyword.get(status, :applied_position)
    durable = Keyword.get(status, :durable_position)

    cond do
      Keyword.get(status, :blocked?, true) != false ->
        blob_gc_skipped({:waraft_storage_blocked, Keyword.get(status, :blocked_error)})

      Keyword.get(status, :payload_dirty?, true) != false ->
        blob_gc_skipped({:waraft_storage_payload_dirty, applied, durable})

      not blob_gc_valid_waraft_position?(applied) or
          not blob_gc_valid_waraft_position?(durable) ->
        blob_gc_skipped(:missing_waraft_storage_metrics)

      applied == durable ->
        :ok

      true ->
        blob_gc_skipped({:waraft_storage_replay_gap, applied, durable})
    end
  end

  defp blob_gc_valid_waraft_position?({:raft_log_pos, index, term})
       when is_integer(index) and index >= 0 and is_integer(term) and term >= 0,
       do: true

  defp blob_gc_valid_waraft_position?(_position), do: false

  defp blob_gc_active_waraft_ctx?(%{name: name, data_dir: data_dir})
       when is_atom(name) and is_binary(data_dir) do
    active_ctx = Ferricstore.Raft.WARaftBackend.context!(:ferricstore_waraft_backend)

    active_ctx.name == name and
      Path.expand(active_ctx.data_dir) == Path.expand(data_dir)
  catch
    _kind, _reason -> false
  end

  defp blob_gc_active_waraft_ctx?(_ctx), do: false

  defp blob_gc_skipped(reason) do
    {:ok,
     %{
       deleted_files: 0,
       deleted_bytes: 0,
       kept_files: 0,
       deleted_tmp_files: 0,
       deleted_tmp_bytes: 0,
       skipped: true,
       reason: reason
     }}
  end

  defp blob_gc_fsync_active_file(%{active_file_path: path}) when :erlang.is_binary(path) do
    case Ferricstore.Bitcask.NIF.v2_fsync(path) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:blob_gc_active_fsync_failed, path, reason}}
      other -> {:error, {:blob_gc_active_fsync_failed, path, other}}
    end
  end

  defp blob_gc_fsync_active_file(_state) do
    {:error, {:blob_gc_active_fsync_failed, nil, :missing}}
  end

  defp blob_gc_live_refs(ctx, idx, state) do
    keydir =
      case Map.get(state, :ets) do
        x when :erlang.orelse(:erlang."=:="(x, false), :erlang."=:="(x, nil)) ->
          resolve_keydir(ctx, idx)

        x ->
          x
      end

    now =
      Ferricstore.ExpiryContext.capture()
      |> Ferricstore.ExpiryContext.safe_expiry_cutoff_ms()

    with {:ok, keydir_refs} <- blob_gc_keydir_live_refs(ctx, idx, state, keydir, now),
         {:ok, lmdb_refs} <- blob_gc_flow_lmdb_live_refs(ctx, idx, now) do
      {:ok, MapSet.union(keydir_refs, lmdb_refs)}
    end
  end

  defp blob_gc_keydir_live_refs(ctx, idx, state, keydir, now) do
    try do
      refs =
        :ets.foldl(
          fn entry, refs ->
            case blob_gc_entry_ref(ctx, idx, state, entry, now) do
              {:ok, nil} -> refs
              {:ok, %Ferricstore.Store.BlobRef{} = ref} -> MapSet.put(refs, ref)
              {:error, _reason} = error -> :erlang.throw({:blob_gc_keydir_live_refs_error, error})
            end
          end,
          MapSet.new(),
          keydir
        )

      {:ok, refs}
    rescue
      _ in [ArgumentError] -> {:error, {:blob_gc_keydir_scan_failed, idx}}
    catch
      :throw, {:blob_gc_keydir_live_refs_error, {:error, _reason} = error} -> error
    end
  end

  defp blob_gc_flow_lmdb_live_refs(ctx, idx, now) do
    blob_gc_scan_flow_lmdb_refs(
      Ferricstore.Flow.LMDB.path(
        Ferricstore.DataDir.shard_data_path(
          ctx.data_dir,
          idx
        )
      ),
      "",
      MapSet.new(),
      now
    )
  end

  defp blob_gc_scan_flow_lmdb_refs(path, after_key, refs, now) do
    page_size = blob_gc_flow_lmdb_page_size()

    case Ferricstore.Flow.LMDB.prefix_entries_after(path, "f:", after_key, page_size) do
      {:ok, []} ->
        {:ok, refs}

      {:ok, entries} ->
        with {:ok, refs} <- blob_gc_collect_lmdb_value_refs(entries, refs, now) do
          {last_key, _} = List.last(entries)

          case (case :erlang.<(:erlang.length(entries), page_size) do
                  false -> :erlang.==(last_key, after_key)
                  true -> true
                end) do
            false -> blob_gc_scan_flow_lmdb_refs(path, last_key, refs, now)
            true -> {:ok, refs}
          end
        end

      {:error, reason} ->
        {:error, {:blob_gc_flow_lmdb_scan_failed, reason}}
    end
  end

  defp blob_gc_collect_lmdb_value_refs(entries, refs, now) do
    Enum.reduce_while(entries, {:ok, refs}, fn {key, blob}, {:ok, refs} ->
      case blob_gc_lmdb_value_ref(blob, now) do
        {:ok, nil} -> {:cont, {:ok, refs}}
        {:ok, %Ferricstore.Store.BlobRef{} = ref} -> {:cont, {:ok, MapSet.put(refs, ref)}}
        {:error, reason} -> {:halt, {:error, {:blob_gc_flow_lmdb_ref_decode_failed, key, reason}}}
      end
    end)
  end

  defp blob_gc_lmdb_value_ref(blob, now) do
    case Ferricstore.Flow.LMDB.decode_value_locator(blob, now) do
      {:ok, _locator} ->
        {:ok, nil}

      :expired ->
        {:ok, nil}

      :not_locator ->
        case Ferricstore.Flow.LMDB.decode_value(blob, now) do
          {:ok, value} -> blob_gc_decode_ref(value)
          :expired -> {:ok, nil}
          :error -> {:error, :invalid_value_wrapper}
        end

      :error ->
        {:error, :invalid_value_locator}
    end
  end

  defp blob_gc_flow_lmdb_page_size() do
    case Application.get_env(:ferricstore, :blob_gc_flow_lmdb_page_size, 1024) do
      value when :erlang.andalso(:erlang.is_integer(value), :erlang.>(value, 0)) -> value
      _ -> 1024
    end
  end

  defp blob_gc_entry_ref(ctx, idx, state, {key, value, exp, lfu, fid, off, size}, now)
       when :erlang.andalso(
              :erlang.andalso(
                :erlang.andalso(:erlang.is_binary(key), :erlang.is_integer(exp)),
                :erlang."/="(exp, 0)
              ),
              :erlang."=<"(exp, now)
            ) do
    case blob_gc_retention_metadata_key?(key) do
      x when :erlang.orelse(:erlang."=:="(x, false), :erlang."=:="(x, nil)) -> {:ok, nil}
      _ -> blob_gc_live_entry_ref(ctx, idx, state, {key, value, exp, lfu, fid, off, size})
    end
  end

  defp blob_gc_entry_ref(ctx, idx, state, entry, _now) do
    blob_gc_live_entry_ref(ctx, idx, state, entry)
  end

  defp blob_gc_live_entry_ref(
         _ctx,
         _idx,
         _state,
         {key, value, _exp, _lfu, _fid, _off, _size}
       )
       when :erlang.andalso(:erlang.is_binary(key), :erlang.is_binary(value)) do
    blob_gc_decode_ref(value)
  end

  defp blob_gc_live_entry_ref(
         ctx,
         idx,
         state,
         {key, _value, _exp, _lfu, fid, off, value_size}
       )
       when :erlang.andalso(
              :erlang.andalso(
                :erlang.andalso(
                  :erlang.andalso(
                    :erlang.andalso(:erlang.is_binary(key), :erlang.is_integer(fid)),
                    :erlang.>=(fid, 0)
                  ),
                  :erlang.is_integer(off)
                ),
                :erlang.>=(off, 0)
              ),
              :erlang.==(value_size, @blob_ref_encoded_size)
            ) do
    path = blob_gc_entry_file_path(ctx, idx, state, key, fid)

    case Ferricstore.Store.ColdRead.pread_keyed(path, off, key, 10_000) do
      {:ok, value} -> blob_gc_decode_ref(value)
      {:error, reason} -> {:error, {:blob_gc_live_ref_scan_failed, key, reason}}
    end
  end

  defp blob_gc_live_entry_ref(ctx, idx, _state, {key, _value, _exp, _lfu, fid, off, value_size})
       when :erlang.andalso(
              :erlang.is_binary(key),
              :erlang.andalso(
                :erlang.andalso(
                  :erlang.andalso(
                    :erlang.andalso(
                      :erlang.andalso(
                        :erlang.andalso(
                          :erlang.andalso(
                            :erlang.andalso(
                              :erlang.is_tuple(fid),
                              :erlang.==(:erlang.tuple_size(fid), 2)
                            ),
                            :erlang.orelse(
                              :erlang.orelse(
                                :erlang.==(:erlang.element(1, fid), :waraft_segment),
                                :erlang.==(:erlang.element(1, fid), :waraft_projection)
                              ),
                              :erlang.==(:erlang.element(1, fid), :waraft_apply_projection)
                            )
                          ),
                          :erlang.is_integer(:erlang.element(2, fid))
                        ),
                        :erlang.>(:erlang.element(2, fid), 0)
                      ),
                      :erlang.is_integer(off)
                    ),
                    :erlang.>=(off, 0)
                  ),
                  :erlang.is_integer(value_size)
                ),
                :erlang.>=(value_size, 0)
              )
            ) do
    case blob_gc_waraft_ref_candidate?(ctx, value_size) do
      x when :erlang.orelse(:erlang."=:="(x, false), :erlang."=:="(x, nil)) ->
        {:ok, nil}

      _ ->
        case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, idx, fid, key) do
          {:ok, value} -> blob_gc_decode_ref(value)
          :not_found -> {:error, {:blob_gc_live_ref_scan_failed, key, :not_found}}
          {:error, reason} -> {:error, {:blob_gc_live_ref_scan_failed, key, reason}}
        end
    end
  end

  defp blob_gc_live_entry_ref(_ctx, _idx, _state, _entry) do
    {:ok, nil}
  end

  defp blob_gc_waraft_ref_candidate?(ctx, value_size) do
    threshold = Ferricstore.Store.BlobValue.threshold(ctx)

    case :erlang.>(threshold, 0) do
      false ->
        false

      true ->
        case Ferricstore.Store.BlobRef.encoded_size?(value_size) do
          false ->
            case :erlang.is_integer(value_size) do
              false -> false
              true -> :erlang.>=(value_size, threshold)
            end

          true ->
            true

          other ->
            :erlang.error({:badbool, :or, other})
        end
    end
  end

  defp blob_gc_after_live_refs_hook(ctx, idx, live_refs) do
    with :ok <-
           Ferricstore.FaultInjection.maybe_pause(:after_blob_gc_live_refs, %{
             shard_index: idx,
             live_ref_count: Enum.count(live_refs)
           }) do
      case Process.get(:ferricstore_blob_gc_after_live_refs_hook) do
        fun when :erlang.is_function(fun, 3) -> fun.(ctx, idx, live_refs)
        _other -> :ok
      end
    end
  end

  defp blob_gc_decode_ref(value) when :erlang.is_binary(value) do
    case Ferricstore.Store.BlobRef.decode(value) do
      {:ok, %Ferricstore.Store.BlobRef{} = ref} -> {:ok, ref}
      _ -> {:ok, nil}
    end
  end

  defp blob_gc_retention_metadata_key?(key) do
    case (case Ferricstore.Flow.Keys.state_key?(key) do
            false -> Ferricstore.Flow.Keys.value_key?(key)
            true -> true
            other -> :erlang.error({:badbool, :or, other})
          end) do
      false -> blob_gc_flow_history_entry_key?(key)
      true -> true
      other -> :erlang.error({:badbool, :or, other})
    end
  end

  defp blob_gc_flow_history_entry_key?(<<"X:f:{", rest::binary>>) do
    :erlang."/="(:binary.match(rest, "}:h:"), :nomatch)
  end

  defp blob_gc_flow_history_entry_key?(_key) do
    false
  end

  defp blob_gc_entry_file_path(ctx, idx, state, key, fid) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case Map.get(Map.get(state, :promoted_instances, %{}), redis_key) do
      %{path: dedicated_path} when :erlang.is_binary(dedicated_path) ->
        Ferricstore.Store.Shard.Compound.dedicated_file_path(dedicated_path, fid)

      _ ->
        cold_file_path(ctx, idx, fid)
    end
  end

  defp blob_gc_empty_stats() do
    %{
      deleted_files: 0,
      deleted_bytes: 0,
      kept_files: 0,
      deleted_tmp_files: 0,
      deleted_tmp_bytes: 0,
      hardened_protections_seen: 0,
      hardened_protections_released: 0,
      hardened_protections_blocked: 0
    }
  end

  defp blob_gc_merge_stats(acc, stats) do
    Map.merge(acc, stats, fn
      _key, a, b when :erlang.andalso(:erlang.is_integer(a), :erlang.is_integer(b)) ->
        :erlang.+(a, b)

      _key, _a, b ->
        b
    end)
  end

  defp emit_blob_protection_reconcile(ctx, idx, released_count, blocked_count) do
    :telemetry.execute(
      [:ferricstore, :blob, :protection, :reconcile],
      %{released: released_count, blocked: blocked_count},
      %{instance: ctx.name, shard_index: idx}
    )
  end

  defp emit_blob_protection_reconcile_failed(ctx, idx, blocked_count, reason) do
    :telemetry.execute(
      [:ferricstore, :blob, :protection, :reconcile, :failed],
      %{blocked: blocked_count},
      %{instance: ctx.name, shard_index: idx, reason: reason}
    )
  end

  defp emit_blob_gc(ctx, stats) do
    :telemetry.execute([:ferricstore, :blob, :gc], stats, %{
      instance: ctx.name,
      shard_count: effective_shard_count(ctx),
      result:
        case Map.get(stats, :skipped) do
          x when :erlang.orelse(:erlang."=:="(x, false), :erlang."=:="(x, nil)) -> :ok
          _ -> :skipped
        end,
      reason: Map.get(stats, :reason)
    })
  end

  defp emit_blob_gc_failed(ctx, idx, reason) do
    :telemetry.execute(
      [:ferricstore, :blob, :gc, :failed],
      %{count: 1},
      %{instance: ctx.name, shard_index: idx, reason: reason}
    )
  end

  defp effective_shard_count(ctx) do
    ctx.shard_count
  end

  defp resolve_shard(ctx, idx) do
    :erlang.element(:erlang.+(idx, 1), ctx.shard_names)
  end

  defp resolve_keydir(ctx, idx) do
    :erlang.element(:erlang.+(idx, 1), ctx.keydir_refs)
  end

  defp cold_file_path(ctx, idx, {:flow_history, file_id}) do
    Ferricstore.Flow.HistoryProjector.history_file_path(
      Ferricstore.DataDir.shard_data_path(
        ctx.data_dir,
        idx
      ),
      file_id
    )
  end

  defp cold_file_path(ctx, idx, file_id) do
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

    Path.join(
      shard_path,
      <<String.Chars.to_string(String.pad_leading(:erlang.integer_to_binary(file_id), 5, "0"))::binary,
        ".log">>
    )
  end
end
