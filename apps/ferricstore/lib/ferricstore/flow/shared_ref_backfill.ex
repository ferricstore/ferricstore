defmodule Ferricstore.Flow.SharedRefBackfill do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.HistoryProjector.ValueProjection
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.RetentionCleanupMember
  alias Ferricstore.Flow.RetentionGuard
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle

  @version 2
  @default_batch_size 512
  @default_batch_bytes 4 * 1_024 * 1_024
  @cold_read_timeout_ms 30_000
  @staging_root "__ferricstore:shared-ref-backfill:v2:"

  defguardp valid_waraft_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                   is_integer(offset) and offset >= 0 and is_integer(value_size) and
                   value_size >= 0

  @doc false
  def progress_key(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    "f:{f}:svbp:2:" <> Integer.to_string(shard_index)
  end

  @doc false
  def completion_key(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    "__ferricstore:shared-ref-backfill:complete:v2:" <> Integer.to_string(shard_index)
  end

  @doc false
  def verified_complete?(instance_name, shard_index)
      when is_integer(shard_index) and shard_index >= 0 do
    :persistent_term.get(verified_key(instance_name, shard_index), false) == true
  end

  @doc false
  def invalidate_verified!(_instance_name, 0), do: :ok

  def invalidate_verified!(instance_name, shard_count)
      when is_integer(shard_count) and shard_count > 0 do
    Enum.each(0..(shard_count - 1), fn shard_index ->
      :persistent_term.erase(verified_key(instance_name, shard_index))
    end)

    :ok
  end

  def run!(shard_path, keydir, shard_index, instance_ctx, flow_index, flow_lookup, opts \\ []) do
    native = NativeOrderedIndex.get(flow_index, flow_lookup)

    ctx = %{
      shard_path: shard_path,
      keydir: keydir,
      shard_index: shard_index,
      instance_name: instance_name(instance_ctx),
      instance_ctx: instance_ctx,
      native: native,
      lmdb_path: LMDB.path(shard_path),
      batch_size: positive_opt(opts, :batch_size, @default_batch_size),
      batch_bytes: positive_opt(opts, :batch_bytes, @default_batch_bytes),
      active_file_id: Keyword.get(opts, :active_file_id),
      active_file_path: Keyword.get(opts, :active_file_path)
    }

    clear_verified!(ctx)

    watermark_key = Keys.shared_value_ref_backfill_key(shard_index)

    case lookup_primary_value!(ctx, watermark_key) do
      {:ok, <<1>>} ->
        case completion_certificate_run_id(ctx) do
          {:ok, _run_id} ->
            rebuild_cleanup_index!(ctx)
            mark_verified!(ctx)
            :ok

          :missing_or_invalid ->
            persist_deletes!(ctx, [watermark_key])
            migrate!(ctx, initialize_progress!(ctx))
        end

      :not_found ->
        ctx
        |> load_or_initialize_progress!()
        |> then(&migrate!(ctx, &1))

      {:ok, _invalid} ->
        raise "shared-ref backfill found corrupt final watermark"
    end
  end

  @doc false
  def finalize_empty_shard!(shard_path, keydir, shard_index, instance_ctx, opts \\ []) do
    ctx = %{
      shard_path: shard_path,
      keydir: keydir,
      shard_index: shard_index,
      instance_name: instance_name(instance_ctx),
      instance_ctx: instance_ctx,
      native: nil,
      lmdb_path: LMDB.path(shard_path),
      batch_size: positive_opt(opts, :batch_size, @default_batch_size),
      batch_bytes: positive_opt(opts, :batch_bytes, @default_batch_bytes),
      active_file_id: Keyword.get(opts, :active_file_id),
      active_file_path: Keyword.get(opts, :active_file_path)
    }

    clear_verified!(ctx)
    require_empty_keydir!(keydir, shard_index)

    run_id = new_run_id()
    complete = progress(run_id, :complete, <<>>, 0)

    lmdb_write_batch!(ctx, [
      {:put, ready_key(shard_index), ready_proof(ctx, run_id)},
      {:put, cleanup_proof_key(shard_index), cleanup_proof(ctx, run_id)},
      {:put, completion_key(shard_index), completion_certificate(shard_index, run_id)}
    ])

    persist_puts!(ctx, %{
      Keys.shared_value_ref_backfill_key(shard_index) => <<1>>,
      progress_key(shard_index) => encode_progress(complete)
    })

    fsync_primary!(ctx)
    publish_progress_proof!(ctx, complete)
    mark_verified!(ctx)
    :ok
  end

  defp require_empty_keydir!(keydir, shard_index) do
    case :ets.info(keydir, :size) do
      0 ->
        :ok

      size when is_integer(size) and size <= 2 ->
        allowed = [
          Keys.shared_value_ref_backfill_key(shard_index),
          progress_key(shard_index)
        ]

        if Enum.count(allowed, &:ets.member(keydir, &1)) == size do
          :ok
        else
          raise "shared-ref backfill empty-shard finalization requires an empty keydir"
        end

      :undefined ->
        raise "shared-ref backfill empty-shard finalization requires an available keydir"

      _nonempty ->
        raise "shared-ref backfill empty-shard finalization requires an empty keydir"
    end
  rescue
    error in ArgumentError ->
      raise "shared-ref backfill empty-shard finalization requires an available keydir: #{Exception.message(error)}"
  end

  defp positive_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end

  defp instance_name(%{name: name}), do: name
  defp instance_name(_instance_ctx), do: :default

  defp load_or_initialize_progress!(ctx) do
    primary = lookup_primary_value!(ctx, progress_key(ctx.shard_index))

    proof =
      case lmdb_get!(ctx, progress_proof_key(ctx.shard_index)) do
        {:error, reason} ->
          raise "shared-ref backfill progress proof read failed: #{inspect(reason)}"

        other ->
          other
      end

    case {primary, proof} do
      {{:ok, encoded}, {:ok, encoded}} ->
        encoded |> decode_progress!() |> ensure_staging_run!(ctx)

      {:not_found, :not_found} ->
        initialize_progress!(ctx)

      {_missing_or_mismatched, _proof} ->
        initialize_progress!(ctx)
    end
  end

  defp initialize_progress!(ctx) do
    progress = progress(new_run_id(), :cleanup_stale, <<>>, 0)
    save_progress!(ctx, progress)
  end

  defp ensure_staging_run!(%{phase: :cleanup_staging} = progress, ctx) do
    ensure_run_proof!(
      ctx,
      ready_key(ctx.shard_index),
      ready_proof(ctx, progress.run_id),
      progress
    )
  end

  defp ensure_staging_run!(%{phase: :finalize} = progress, ctx) do
    ensure_run_proof!(
      ctx,
      cleanup_proof_key(ctx.shard_index),
      cleanup_proof(ctx, progress.run_id),
      progress
    )
  end

  defp ensure_staging_run!(%{phase: :complete} = progress, ctx) do
    case completion_certificate_run_id(ctx) do
      {:ok, run_id} when run_id == progress.run_id ->
        %{progress | phase: :finalize}

      _missing_or_wrong ->
        initialize_progress!(ctx)
    end
  end

  defp ensure_staging_run!(progress, ctx) do
    case lmdb_get!(ctx, manifest_key(progress.run_id)) do
      {:ok, encoded_run_id} when encoded_run_id == progress.run_id ->
        progress

      :not_found ->
        initialize_progress!(ctx)

      {:ok, _wrong_run_id} ->
        initialize_progress!(ctx)

      {:error, reason} ->
        raise "shared-ref backfill LMDB manifest read failed: #{inspect(reason)}"

      other ->
        raise "shared-ref backfill LMDB manifest read returned #{inspect(other)}"
    end
  end

  defp ensure_run_proof!(ctx, key, expected, progress) do
    case lmdb_get!(ctx, key) do
      {:ok, ^expected} -> progress
      :not_found -> initialize_progress!(ctx)
      {:ok, _wrong} -> initialize_progress!(ctx)
      {:error, reason} -> raise "shared-ref backfill run proof read failed: #{inspect(reason)}"
      other -> raise "shared-ref backfill run proof read returned #{inspect(other)}"
    end
  end

  defp migrate!(ctx, %{phase: :cleanup_stale} = progress) do
    case delete_staging_page!(ctx, progress.cursor) do
      {:more, cursor, deleted} ->
        next = %{progress | cursor: cursor, processed: progress.processed + deleted}
        migrate!(ctx, save_progress!(ctx, next))

      :done ->
        lmdb_write_batch!(ctx, [{:put, manifest_key(progress.run_id), progress.run_id}])
        next = progress(progress.run_id, :snapshot_keydir, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))
    end
  end

  defp migrate!(ctx, %{phase: :snapshot_keydir} = progress) do
    next = snapshot_keydir!(ctx, progress)
    migrate!(ctx, save_progress!(ctx, next))
  end

  defp migrate!(ctx, %{phase: :scan_lmdb_states} = progress) do
    case lmdb_prefix_entries_after_bounded!(
           ctx,
           "f:{",
           progress.cursor,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        next = progress(progress.run_id, :scan_work, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      entries ->
        emit_read_batch(:scan_lmdb_states, entries)
        Enum.each(entries, &process_lmdb_state_entry!(ctx, &1, progress.run_id))
        {cursor, _value} = List.last(entries)

        next = %{
          progress
          | cursor: cursor,
            processed: progress.processed + length(entries)
        }

        migrate!(ctx, save_page_progress!(ctx, next))
    end
  end

  defp migrate!(ctx, %{phase: :scan_work} = progress) do
    prefix = work_prefix(progress.run_id)

    case lmdb_prefix_entries_after_bounded!(
           ctx,
           prefix,
           progress.cursor,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        next = progress(progress.run_id, :count_refs, encode_count_cursor(<<>>, nil, 0), 0)
        migrate!(ctx, save_progress!(ctx, next))

      entries ->
        emit_read_batch(:scan_work, entries)

        Enum.each(entries, fn {_stage_key, encoded_key} ->
          process_work_key!(ctx, decode_work_key!(encoded_key), progress.run_id)
        end)

        {cursor, _value} = List.last(entries)

        next = %{
          progress
          | cursor: cursor,
            processed: progress.processed + length(entries)
        }

        migrate!(ctx, save_page_progress!(ctx, next))
    end
  end

  defp migrate!(ctx, %{phase: :count_refs} = progress) do
    {after_key, group_ref, group_count} = decode_count_cursor!(progress.cursor)
    prefix = contribution_prefix(progress.run_id)

    case lmdb_prefix_entries_after_bounded!(
           ctx,
           prefix,
           after_key,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        stage_exact_count!(ctx, progress.run_id, group_ref, group_count)
        next = progress(progress.run_id, :validate_current_registries, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      entries ->
        emit_read_batch(:count_refs, entries)

        case count_contribution_page!(ctx, progress.run_id, entries, group_ref, group_count) do
          {:ok, next_ref, next_count} ->
            {cursor, _value} = List.last(entries)

            next = %{
              progress
              | cursor: encode_count_cursor(cursor, next_ref, next_count),
                processed: progress.processed + length(entries)
            }

            migrate!(ctx, save_page_progress!(ctx, next))

          :registry_changed ->
            # Contributions are immutable snapshots. A fresh run removes stale
            # rows instead of allowing them to invalidate every subsequent pass.
            migrate!(ctx, initialize_progress!(ctx))
        end
    end
  end

  defp migrate!(ctx, %{phase: :commit_counts} = progress) do
    prefix = count_result_prefix(progress.run_id)

    case lmdb_prefix_entries_after_bounded!(
           ctx,
           prefix,
           progress.cursor,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        next = progress(progress.run_id, :delete_orphan_counts, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      entries ->
        emit_read_batch(:commit_counts, entries)

        Enum.each(entries, fn {_key, encoded} -> commit_count_result!(ctx, encoded) end)

        {cursor, _value} = List.last(entries)

        next = %{
          progress
          | cursor: cursor,
            processed: progress.processed + length(entries)
        }

        migrate!(ctx, save_page_progress!(ctx, next))
    end
  end

  defp migrate!(ctx, %{phase: :delete_orphan_counts} = progress) do
    prefix = existing_count_prefix(progress.run_id)

    case lmdb_prefix_entries_after_bounded!(
           ctx,
           prefix,
           progress.cursor,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        next = progress(progress.run_id, :validate_committed_current, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      entries ->
        emit_read_batch(:delete_orphan_counts, entries)

        orphan_keys =
          Enum.flat_map(entries, fn {_key, encoded} ->
            case orphan_count_key!(ctx, progress.run_id, encoded) do
              nil -> []
              count_key -> [count_key]
            end
          end)

        persist_deletes!(ctx, orphan_keys)

        {cursor, _value} = List.last(entries)

        next = %{
          progress
          | cursor: cursor,
            processed: progress.processed + length(entries)
        }

        migrate!(ctx, save_page_progress!(ctx, next))
    end
  end

  defp migrate!(ctx, %{phase: :validate_current_registries} = progress) do
    case validate_current_registries!(ctx, progress) do
      {:ok, validated} ->
        next = progress(validated.run_id, :validate_staged_registries, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      :registry_changed ->
        migrate!(ctx, initialize_progress!(ctx))
    end
  end

  defp migrate!(ctx, %{phase: :validate_staged_registries} = progress) do
    prefix = registry_snapshot_prefix(progress.run_id)

    case lmdb_prefix_entries_after_bounded!(
           ctx,
           prefix,
           progress.cursor,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        next = progress(progress.run_id, :commit_counts, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      entries ->
        emit_read_batch(:validate_staged_registries, entries)

        if Enum.all?(entries, fn {_key, encoded} -> registry_snapshot_current?(ctx, encoded) end) do
          {cursor, _value} = List.last(entries)

          next = %{
            progress
            | cursor: cursor,
              processed: progress.processed + length(entries)
          }

          migrate!(ctx, save_page_progress!(ctx, next))
        else
          migrate!(ctx, initialize_progress!(ctx))
        end
    end
  end

  defp migrate!(ctx, %{phase: :validate_committed_current} = progress) do
    case validate_current_registries!(ctx, progress) do
      {:ok, validated} ->
        next = progress(validated.run_id, :validate_committed_staged, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      :registry_changed ->
        migrate!(ctx, initialize_progress!(ctx))
    end
  end

  defp migrate!(ctx, %{phase: :validate_committed_staged} = progress) do
    prefix = registry_snapshot_prefix(progress.run_id)

    case lmdb_prefix_entries_after_bounded!(
           ctx,
           prefix,
           progress.cursor,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        lmdb_write_batch!(ctx, [
          {:put, ready_key(ctx.shard_index), ready_proof(ctx, progress.run_id)}
        ])

        next = progress(progress.run_id, :cleanup_staging, <<>>, 0)
        migrate!(ctx, save_progress!(ctx, next))

      entries ->
        emit_read_batch(:validate_committed_staged, entries)

        if Enum.all?(entries, fn {_key, encoded} -> registry_snapshot_current?(ctx, encoded) end) do
          {cursor, _value} = List.last(entries)

          next = %{
            progress
            | cursor: cursor,
              processed: progress.processed + length(entries)
          }

          migrate!(ctx, save_page_progress!(ctx, next))
        else
          migrate!(ctx, initialize_progress!(ctx))
        end
    end
  end

  defp migrate!(ctx, %{phase: :cleanup_staging} = progress) do
    case delete_staging_page!(ctx, progress.cursor) do
      {:more, cursor, deleted} ->
        next = %{progress | cursor: cursor, processed: progress.processed + deleted}
        migrate!(ctx, save_page_progress!(ctx, next))

      :done ->
        lmdb_write_batch!(ctx, [
          {:put, cleanup_proof_key(ctx.shard_index), cleanup_proof(ctx, progress.run_id)}
        ])

        next = progress(progress.run_id, :finalize, <<>>, progress.processed)
        migrate!(ctx, save_progress!(ctx, next))
    end
  end

  defp migrate!(ctx, %{phase: :finalize} = progress) do
    fsync_primary!(ctx)
    rebuild_cleanup_index!(ctx)
    watermark_key = Keys.shared_value_ref_backfill_key(ctx.shard_index)
    complete = progress(progress.run_id, :complete, <<>>, progress.processed)

    lmdb_write_batch!(ctx, [
      {:put, completion_key(ctx.shard_index),
       completion_certificate(ctx.shard_index, progress.run_id)}
    ])

    persist_puts!(ctx, %{
      watermark_key => <<1>>,
      progress_key(ctx.shard_index) => encode_progress(complete)
    })

    fsync_primary!(ctx)
    publish_progress_proof!(ctx, complete)
    mark_verified!(ctx)
    :ok
  end

  defp migrate!(_ctx, %{phase: :complete}), do: :ok

  defp migrate!(_ctx, progress) do
    raise "shared-ref backfill has invalid phase: #{inspect(progress)}"
  end

  defp completion_certificate_run_id(ctx) do
    case lmdb_get!(ctx, completion_key(ctx.shard_index)) do
      {:ok, certificate} ->
        case safe_binary_to_term(certificate) do
          {:shared_ref_backfill_complete, @version, shard_index, run_id}
          when shard_index == ctx.shard_index and is_binary(run_id) and run_id != "" ->
            {:ok, run_id}

          _invalid ->
            :missing_or_invalid
        end

      :not_found ->
        :missing_or_invalid

      {:error, reason} ->
        raise "shared-ref backfill completion certificate read failed: #{inspect(reason)}"

      other ->
        raise "shared-ref backfill completion certificate read returned #{inspect(other)}"
    end
  end

  defp completion_certificate(shard_index, run_id) do
    :erlang.term_to_binary({:shared_ref_backfill_complete, @version, shard_index, run_id})
  end

  defp ready_key(shard_index),
    do: "__ferricstore:shared-ref-backfill:ready:v2:" <> Integer.to_string(shard_index)

  defp cleanup_proof_key(shard_index),
    do: "__ferricstore:shared-ref-backfill:clean:v2:" <> Integer.to_string(shard_index)

  defp progress_proof_key(shard_index),
    do: "__ferricstore:shared-ref-backfill:progress:v2:" <> Integer.to_string(shard_index)

  defp ready_proof(ctx, run_id),
    do: :erlang.term_to_binary({:shared_ref_backfill_ready, @version, ctx.shard_index, run_id})

  defp cleanup_proof(ctx, run_id),
    do: :erlang.term_to_binary({:shared_ref_backfill_clean, @version, ctx.shard_index, run_id})

  defp verified_key(instance_name, shard_index),
    do: {__MODULE__, :verified_complete, instance_name, shard_index}

  defp clear_verified!(ctx),
    do: :persistent_term.erase(verified_key(ctx.instance_name, ctx.shard_index))

  defp mark_verified!(ctx),
    do: :persistent_term.put(verified_key(ctx.instance_name, ctx.shard_index), true)

  defp snapshot_keydir!(ctx, progress) do
    with_fixed_keydir!(ctx.keydir, fn ->
      snapshot_keydir_pages!(
        ctx,
        progress,
        :ets.select(ctx.keydir, keydir_match_spec(), ctx.batch_size)
      )
    end)
  end

  defp snapshot_keydir_pages!(_ctx, progress, :"$end_of_table") do
    progress(progress.run_id, :scan_lmdb_states, <<>>, 0)
  end

  defp snapshot_keydir_pages!(ctx, progress, {entries, continuation}) do
    stage_work_entries!(ctx, progress.run_id, entries)
    register_cleanup_entries!(ctx, entries)

    next = %{progress | processed: progress.processed + length(entries)}
    saved = save_page_progress!(ctx, next)

    snapshot_keydir_pages!(ctx, saved, :ets.select(continuation))
  rescue
    error in ArgumentError ->
      raise "shared-ref backfill keydir scan failed: #{Exception.message(error)}"
  end

  defp stage_work_entries!(ctx, run_id, entries) do
    ops =
      entries
      |> Enum.flat_map(fn
        {:key, key} when is_binary(key) ->
          if migration_primary_key?(ctx, key) or not migration_candidate_key?(key) do
            []
          else
            [{:put, work_key(run_id, key), :erlang.term_to_binary({:key, key})}]
          end

        {:invalid} ->
          raise "shared-ref backfill encountered an invalid keydir row"

        _invalid ->
          raise "shared-ref backfill encountered an invalid keydir key"
      end)

    lmdb_write_ops_bounded!(ctx, :snapshot_keydir, ops)
  end

  defp process_lmdb_state_entry!(ctx, {state_key, blob}, run_id) do
    if flow_state_key?(state_key) do
      encoded =
        case lookup_primary_value!(ctx, state_key) do
          {:ok, current} -> current
          :not_found -> decode_lmdb_state_value!(blob, state_key)
        end

      state_key
      |> decode_record!(encoded)
      |> process_record!(ctx, state_key, run_id)
    end
  end

  defp process_work_key!(ctx, key, run_id) do
    cond do
      flow_state_key?(key) ->
        with_primary_value!(ctx, key, fn value ->
          key |> decode_record!(value) |> process_record!(ctx, key, run_id)
        end)

      cleanup_member_key?(key) ->
        with_primary_value!(ctx, key, &register_cleanup_member!(ctx, key, &1))

      history_entry_key?(key) ->
        with_primary_value!(ctx, key, &process_history_entry!(ctx, key, &1, run_id))

      shared_registry_key?(key) ->
        with_primary_value!(ctx, key, fn value ->
          refs = decode_registry!(value, key)
          canonical = encode_registry(refs)
          if canonical != value, do: persist_puts!(ctx, %{key => canonical})
          stage_contributions!(ctx, run_id, key, refs)
        end)

      shared_count_key?(key) ->
        with_primary_value!(ctx, key, fn value ->
          _count = decode_count!(value, key)
          stage_existing_count!(ctx, run_id, key)
        end)

      retention_guard_key?(key) ->
        with_primary_value!(ctx, key, &validate_guard!(&1, key))

      true ->
        case key_marker_and_remainder(key) do
          :other ->
            :ok

          {kind, _tag, _remainder}
          when kind in [:shared_link, :governance_effect, :governance_ledger, :ledger_index] ->
            with_primary_value!(ctx, key, &process_owned_key!(ctx, key, &1))

          _owned_candidate ->
            if key_present!(ctx.keydir, key), do: process_owned_key!(ctx, key, nil)
        end
    end
  end

  defp with_primary_value!(ctx, key, fun) do
    case lookup_primary_value!(ctx, key) do
      {:ok, value} -> fun.(value)
      :not_found -> :ok
    end
  end

  defp process_record!(record, ctx, state_key, run_id) do
    validate_record_key!(record, state_key)
    maybe_put_guard!(ctx, record)

    refs = shared_refs(record)
    merge_registry!(ctx, run_id, record_info(record), refs)

    record
    |> all_record_refs()
    |> Enum.filter(&owned_value_ref?(&1, record))
    |> Enum.each(&track_cleanup_key!(ctx, record_info(record), &1))
  end

  defp process_history_entry!(ctx, entry_key, value, run_id) do
    with {:ok, state_key} <- history_state_key(entry_key),
         {:ok, record} <- lookup_record!(ctx, state_key) do
      info = record_info(record)
      refs = decode_history_refs!(value, entry_key)
      merge_registry!(ctx, run_id, info, refs)

      refs
      |> Enum.filter(&owned_value_ref?(&1, record))
      |> Enum.each(&track_cleanup_key!(ctx, info, &1))
    else
      :not_found ->
        :ok

      :error ->
        raise "shared-ref backfill could not resolve history owner for #{inspect(entry_key)}"
    end
  end

  defp process_owned_key!(ctx, key, value) do
    case cleanup_owner_info(ctx, key, value) do
      {:ok, info, extra_keys} ->
        Enum.each([key | extra_keys], &track_cleanup_key!(ctx, info, &1))

      :not_owned ->
        :ok
    end
  end

  defp merge_registry!(_ctx, _run_id, _info, refs) when map_size(refs) == 0, do: :ok

  defp merge_registry!(ctx, run_id, info, refs) do
    {existing, existing_value} =
      case lookup_primary_value!(ctx, info.registry_key) do
        :not_found -> {MapSet.new(), nil}
        {:ok, value} -> {decode_registry!(value, info.registry_key), value}
      end

    merged = MapSet.union(existing, refs)
    encoded = encode_registry(merged)

    if encoded != existing_value do
      persist_puts!(ctx, %{info.registry_key => encoded})
    end

    stage_contributions!(ctx, run_id, info.registry_key, merged)
  end

  defp stage_contributions!(ctx, run_id, registry_key, refs) do
    registry = encode_registry(refs)
    registry_digest = :crypto.hash(:sha256, registry)

    lmdb_write_batch!(ctx, [
      {:put, registry_snapshot_key(run_id, registry_key),
       :erlang.term_to_binary({:registry_snapshot, registry_key, registry_digest})}
    ])

    lmdb_write_stream_bounded!(ctx, :contributions, refs, fn ref ->
      value = :erlang.term_to_binary({:shared_ref, ref, registry_key, registry_digest})
      {:put, contribution_key(run_id, ref, registry_key), value}
    end)
  end

  defp validate_current_registries!(ctx, progress) do
    with_fixed_keydir!(ctx.keydir, fn ->
      validate_current_registry_pages!(
        ctx,
        progress,
        :ets.select(ctx.keydir, keydir_match_spec(), ctx.batch_size)
      )
    end)
  end

  defp validate_current_registry_pages!(_ctx, progress, :"$end_of_table"),
    do: {:ok, progress}

  defp validate_current_registry_pages!(ctx, progress, {keys, continuation}) do
    stable? =
      Enum.all?(keys, fn
        {:key, key} when is_binary(key) ->
          if shared_registry_key?(key) do
            current_registry_staged?(ctx, progress.run_id, key)
          else
            true
          end

        {:invalid} ->
          raise "shared-ref backfill encountered an invalid keydir row"

        _invalid ->
          raise "shared-ref backfill encountered an invalid keydir key"
      end)

    if stable? do
      next = %{progress | processed: progress.processed + length(keys)}
      saved = save_page_progress!(ctx, next)
      validate_current_registry_pages!(ctx, saved, :ets.select(continuation))
    else
      :registry_changed
    end
  end

  defp current_registry_staged?(ctx, run_id, registry_key) do
    with {:ok, value} <- lookup_primary_value!(ctx, registry_key),
         refs <- decode_registry!(value, registry_key),
         canonical = encode_registry(refs),
         true <- canonical == value,
         {:ok, snapshot} <- lmdb_get!(ctx, registry_snapshot_key(run_id, registry_key)) do
      snapshot ==
        :erlang.term_to_binary(
          {:registry_snapshot, registry_key, :crypto.hash(:sha256, canonical)}
        )
    else
      :not_found ->
        false

      false ->
        false

      {:error, reason} ->
        raise "shared-ref backfill registry snapshot read failed: #{inspect(reason)}"

      other ->
        raise "shared-ref backfill registry snapshot read returned #{inspect(other)}"
    end
  end

  defp registry_snapshot_current?(ctx, encoded) do
    case safe_binary_to_term(encoded) do
      {:registry_snapshot, registry_key, expected_digest}
      when is_binary(registry_key) and is_binary(expected_digest) ->
        case lookup_primary_value!(ctx, registry_key) do
          {:ok, registry} ->
            _refs = decode_registry!(registry, registry_key)
            :crypto.hash(:sha256, registry) == expected_digest

          :not_found ->
            false
        end

      _invalid ->
        raise "shared-ref backfill found corrupt staged registry snapshot"
    end
  end

  defp count_contribution_page!(ctx, run_id, entries, group_ref, group_count) do
    Enum.reduce_while(entries, {:ok, group_ref, group_count}, fn {_key, encoded},
                                                                 {:ok, current_ref, count} ->
      {ref, registry_key, expected_digest} = decode_contribution!(encoded)

      case lookup_primary_value!(ctx, registry_key) do
        {:ok, registry} ->
          refs = decode_registry!(registry, registry_key)

          if :crypto.hash(:sha256, registry) == expected_digest and MapSet.member?(refs, ref) do
            cond do
              is_nil(current_ref) ->
                {:cont, {:ok, ref, 1}}

              current_ref == ref ->
                {:cont, {:ok, ref, count + 1}}

              ref_digest(current_ref) == ref_digest(ref) ->
                raise "shared-ref backfill detected a shared-ref digest collision"

              true ->
                stage_exact_count!(ctx, run_id, current_ref, count)
                {:cont, {:ok, ref, 1}}
            end
          else
            {:halt, :registry_changed}
          end

        :not_found ->
          {:halt, :registry_changed}
      end
    end)
  end

  defp stage_exact_count!(_ctx, _run_id, nil, _count), do: :ok

  defp stage_exact_count!(ctx, run_id, ref, count)
       when is_integer(count) and count > 0 do
    count_key = Keys.shared_value_ref_count_key(ref, ctx.shard_index)

    lmdb_write_batch!(ctx, [
      {:put, count_result_key(run_id, count_key),
       :erlang.term_to_binary({:count_result, count_key, count})}
    ])
  end

  defp stage_existing_count!(ctx, run_id, count_key) do
    lmdb_write_batch!(ctx, [
      {:put, existing_count_key(run_id, count_key),
       :erlang.term_to_binary({:existing_count, count_key})}
    ])
  end

  defp commit_count_result!(ctx, encoded) do
    {count_key, count} =
      case safe_binary_to_term(encoded) do
        {:count_result, key, value}
        when is_binary(key) and is_integer(value) and value > 0 ->
          {key, value}

        _invalid ->
          raise "shared-ref backfill found corrupt staged count result"
      end

    case lookup_primary_value!(ctx, count_key) do
      {:ok, existing} -> _existing_count = decode_count!(existing, count_key)
      :not_found -> :ok
    end

    persist_puts!(ctx, %{count_key => :erlang.term_to_binary(count)})
  end

  defp orphan_count_key!(ctx, run_id, encoded) do
    count_key =
      case safe_binary_to_term(encoded) do
        {:existing_count, key} when is_binary(key) -> key
        _invalid -> raise "shared-ref backfill found corrupt staged count snapshot"
      end

    case lmdb_get!(ctx, count_result_key(run_id, count_key)) do
      {:ok, result} when is_binary(result) ->
        case safe_binary_to_term(result) do
          {:count_result, ^count_key, count} when is_integer(count) and count > 0 -> :ok
          _invalid -> raise "shared-ref backfill found corrupt staged count result"
        end

      :not_found ->
        count_key

      {:error, reason} ->
        raise "shared-ref backfill count result read failed: #{inspect(reason)}"

      other ->
        raise "shared-ref backfill count result read returned #{inspect(other)}"
    end
    |> case do
      :ok -> nil
      key when is_binary(key) -> key
    end
  end

  defp maybe_put_guard!(ctx, record) do
    info = record_info(record)
    expected = RetentionGuard.encode(record)
    {expected_version, _expected_identity} = decode_guard!(expected, info.guard_key)

    case lookup_primary_value!(ctx, info.guard_key) do
      :not_found ->
        persist_puts!(ctx, %{info.guard_key => expected})

      {:ok, guard} ->
        {guard_version, _guard_identity} = decode_guard!(guard, info.guard_key)

        cond do
          guard_version < expected_version ->
            persist_puts!(ctx, %{info.guard_key => expected})

          guard_version > expected_version ->
            :ok

          guard == expected ->
            :ok

          true ->
            raise "shared-ref backfill found conflicting retention guard #{inspect(info.guard_key)}"
        end
    end
  end

  defp validate_guard!(guard, key) do
    _decoded = decode_guard!(guard, key)
    :ok
  end

  defp decode_guard!(guard, key) when is_binary(guard) do
    case safe_binary_to_term(guard) do
      {version, {:state_enter_seq, sequence}}
      when is_integer(version) and version >= 0 and is_integer(sequence) and sequence >= 0 ->
        {version, {:state_enter_seq, sequence}}

      _invalid ->
        raise "shared-ref backfill found corrupt retention guard #{inspect(key)}"
    end
  end

  defp decode_guard!(_guard, key) do
    raise "shared-ref backfill found corrupt retention guard #{inspect(key)}"
  end

  defp track_cleanup_key!(ctx, info, owned_key) do
    member_key = Keys.retention_cleanup_member_key(info.id, owned_key, info.partition_key)
    member = RetentionCleanupMember.encode(info.cleanup_index_key, owned_key)

    case lookup_primary_value!(ctx, member_key) do
      :not_found -> persist_puts!(ctx, %{member_key => member})
      {:ok, ^member} -> :ok
      {:ok, _corrupt_or_colliding} -> raise "shared-ref backfill found corrupt cleanup member"
    end

    if ctx.native do
      NativeOrderedIndex.put_member(ctx.native, info.cleanup_index_key, member_key, 0)
    end
  end

  defp cleanup_owner_info(ctx, key, value) do
    case key_marker_and_remainder(key) do
      {:private_value, tag, remainder} ->
        with {:ok, id, _version} <- split_versioned_ref(remainder),
             {:ok, info} <- find_owner_info(ctx, tag, [id]) do
          {:ok, info, []}
        else
          _missing -> :not_owned
        end

      {:shared_value, tag, remainder} ->
        with {:ok, ref_id, version} <- split_versioned_ref(remainder),
             {:ok, info} <-
               find_shared_value_owner_info(ctx, tag, ref_id, version, key) do
          {:ok, info, []}
        else
          _missing -> :not_owned
        end

      {:shared_link, tag, remainder} ->
        shared_link_owner_info(ctx, tag, remainder, key, value)

      {:governance_effect, _tag, _remainder} ->
        governance_owner_info(ctx, :effect, key, value)

      {:governance_ledger, _tag, _remainder} ->
        governance_owner_info(ctx, :ledger, key, value)

      {:ledger_index, _tag, _id} ->
        governance_owner_info(ctx, :ledger_index, key, value)

      :other ->
        :not_owned
    end
  end

  defp find_shared_value_owner_info(ctx, tag, ref_id, version, ref) do
    Enum.reduce_while(owner_candidates(ref_id), :not_found, fn id, :not_found ->
      state_key = "f:" <> tag <> ":s:" <> id

      case lookup_record!(ctx, state_key) do
        {:ok, %{id: ^id} = record} ->
          if shared_value_link_matches?(ctx, tag, id, ref_id, version, ref) do
            {:halt, {:ok, record_info(record)}}
          else
            {:cont, :not_found}
          end

        {:ok, _mismatched} ->
          raise "shared-ref backfill found mismatched state identity"

        :not_found ->
          {:cont, :not_found}
      end
    end)
  end

  defp shared_link_owner_info(ctx, tag, remainder, key, value) do
    unless is_binary(value) and Keys.shared_value_ref?(value) do
      raise "shared-ref backfill found corrupt shared-value link #{inspect(key)}"
    end

    result =
      Enum.reduce_while(owner_candidates(remainder), :not_found, fn id, :not_found ->
        state_key = "f:" <> tag <> ":s:" <> id

        case lookup_record!(ctx, state_key) do
          {:ok, %{id: ^id} = record} ->
            if shared_link_matches_owner?(key, value, remainder, record) do
              {:halt, {:ok, record_info(record)}}
            else
              {:cont, :not_found}
            end

          {:ok, _mismatched} ->
            raise "shared-ref backfill found mismatched state identity"

          :not_found ->
            {:cont, :not_found}
        end
      end)

    case result do
      {:ok, info} -> {:ok, info, [value]}
      :not_found -> :not_owned
    end
  end

  defp shared_link_matches_owner?(key, value, remainder, record) do
    id = Map.fetch!(record, :id)
    prefix = id <> ":"

    with true <- String.starts_with?(remainder, prefix),
         name_version <-
           binary_part(remainder, byte_size(prefix), byte_size(remainder) - byte_size(prefix)),
         {:ok, name, version} <- split_versioned_ref(name_version),
         expected_link <-
           Keys.shared_value_link_prefix(id, Map.get(record, :partition_key)) <>
             name <> ":" <> version,
         {version_number, ""} <- Integer.parse(version),
         expected_ref <-
           Keys.value_key(
             id <> ":" <> name,
             :shared,
             version_number,
             Map.get(record, :partition_key)
           ) do
      key == expected_link and value == expected_ref
    else
      _invalid -> false
    end
  end

  defp governance_owner_info(ctx, kind, key, value) do
    case decode_governance_owner!(kind, value) do
      :not_owned ->
        :not_owned

      {flow_id, partition_key, expected_key} ->
        do_governance_owner_info(ctx, key, flow_id, partition_key, expected_key)
    end
  end

  defp do_governance_owner_info(ctx, key, flow_id, partition_key, expected_key) do
    if key != expected_key do
      raise "shared-ref backfill governance owner does not match key #{inspect(key)}"
    end

    state_key = Keys.state_key(flow_id, partition_key)

    case lookup_record!(ctx, state_key) do
      {:ok, %{id: ^flow_id} = record} ->
        if Map.get(record, :partition_key) == partition_key do
          {:ok, record_info(record), []}
        else
          raise "shared-ref backfill governance partition does not match owner"
        end

      {:ok, _mismatched} ->
        raise "shared-ref backfill governance flow id does not match owner"

      :not_found ->
        :not_owned
    end
  end

  defp decode_governance_owner!(:effect, value) do
    case safe_binary_to_term(value) do
      {:flow_governance_effect_v1,
       %{flow_id: flow_id, partition_key: partition_key, effect_key: effect_key}}
      when is_binary(flow_id) and (is_binary(partition_key) or is_nil(partition_key)) and
             is_binary(effect_key) ->
        {flow_id, partition_key, Keys.governance_effect_key(flow_id, effect_key, partition_key)}

      _invalid ->
        raise "shared-ref backfill found corrupt governance effect"
    end
  end

  defp decode_governance_owner!(:ledger, value) do
    case safe_binary_to_term(value) do
      {:flow_governance_ledger_v1,
       %{id: event_id, flow_id: flow_id, partition_key: partition_key}}
      when is_binary(event_id) and is_binary(flow_id) and
             (is_binary(partition_key) or is_nil(partition_key)) ->
        {flow_id, partition_key, Keys.governance_ledger_key(flow_id, event_id, partition_key)}

      _invalid ->
        raise "shared-ref backfill found corrupt governance ledger event"
    end
  end

  defp decode_governance_owner!(:ledger_index, value) do
    case safe_binary_to_term(value) do
      {:flow_governance_ledger_index_v1, [first | rest]}
      when is_map(first) and is_list(rest) ->
        with %{flow_id: flow_id, partition_key: partition_key} <- first,
             true <- is_binary(flow_id) and (is_binary(partition_key) or is_nil(partition_key)),
             true <-
               Enum.all?(rest, fn
                 %{flow_id: ^flow_id, partition_key: ^partition_key} -> true
                 _other -> false
               end) do
          {flow_id, partition_key, Keys.governance_ledger_index_key(flow_id, partition_key)}
        else
          _invalid -> :not_owned
        end

      _empty_or_invalid ->
        :not_owned
    end
  end

  defp shared_value_link_matches?(ctx, tag, owner_id, ref_id, version, ref) do
    prefix = owner_id <> ":"

    if String.starts_with?(ref_id, prefix) do
      name = binary_part(ref_id, byte_size(prefix), byte_size(ref_id) - byte_size(prefix))
      link_key = "f:" <> tag <> ":svl:" <> owner_id <> ":" <> name <> ":" <> version

      case lookup_primary_value!(ctx, link_key) do
        {:ok, ^ref} -> true
        _missing_or_changed -> false
      end
    else
      false
    end
  end

  defp find_owner_info(ctx, tag, candidates) do
    Enum.reduce_while(candidates, :not_found, fn id, :not_found ->
      state_key = "f:" <> tag <> ":s:" <> id

      case lookup_record!(ctx, state_key) do
        {:ok, %{id: ^id} = record} -> {:halt, {:ok, record_info(record)}}
        {:ok, _mismatched} -> raise "shared-ref backfill found mismatched state identity"
        :not_found -> {:cont, :not_found}
      end
    end)
  end

  defp lookup_record!(ctx, state_key) do
    case lookup_primary_value!(ctx, state_key) do
      {:ok, encoded} -> {:ok, decode_record!(state_key, encoded)}
      :not_found -> lookup_lmdb_record!(ctx, state_key)
    end
  end

  defp lookup_lmdb_record!(ctx, state_key) do
    case lmdb_get!(ctx, state_key) do
      {:ok, blob} ->
        encoded = decode_lmdb_state_value!(blob, state_key)
        {:ok, decode_record!(state_key, encoded)}

      :not_found ->
        :not_found

      {:error, reason} ->
        raise "shared-ref backfill LMDB state read failed: #{inspect(reason)}"
    end
  end

  defp decode_lmdb_state_value!(blob, state_key) do
    case LMDB.decode_value(blob, 0) do
      {:ok, encoded} when is_binary(encoded) ->
        encoded

      other ->
        raise "shared-ref backfill failed to decode LMDB state #{inspect(state_key)}: #{inspect(other)}"
    end
  end

  defp decode_record!(state_key, encoded) when is_binary(encoded) do
    case Flow.decode_record(encoded) do
      %{id: id, type: type, state: state} = record
      when is_binary(id) and is_binary(type) and is_binary(state) ->
        record

      _invalid ->
        raise "shared-ref backfill failed to decode state #{inspect(state_key)}"
    end
  rescue
    error ->
      raise "shared-ref backfill failed to decode state #{inspect(state_key)}: #{Exception.message(error)}"
  end

  defp decode_record!(state_key, _encoded) do
    raise "shared-ref backfill failed to decode state #{inspect(state_key)}"
  end

  defp validate_record_key!(record, state_key) do
    expected = Keys.state_key(Map.fetch!(record, :id), Map.get(record, :partition_key))

    if expected != state_key do
      raise "shared-ref backfill state key does not match decoded record"
    end
  end

  defp decode_history_refs!(value, entry_key) when is_binary(value) do
    fields = HistoryProjector.flow_call(:decode_history_fields, [value])

    if is_list(fields) do
      fields
      |> ValueProjection.history_fields_to_map()
      |> ValueProjection.history_fields_value_refs()
      |> Enum.filter(&Keys.shared_value_ref?/1)
      |> MapSet.new()
    else
      raise "shared-ref backfill failed to decode history #{inspect(entry_key)}"
    end
  rescue
    error ->
      raise "shared-ref backfill failed to decode history #{inspect(entry_key)}: #{Exception.message(error)}"
  end

  defp decode_history_refs!(_value, entry_key) do
    raise "shared-ref backfill failed to decode history #{inspect(entry_key)}"
  end

  defp decode_registry!(value, key) when is_binary(value) do
    case safe_binary_to_term(value) do
      refs when is_list(refs) ->
        if Enum.all?(refs, &Keys.shared_value_ref?/1) do
          MapSet.new(refs)
        else
          raise "shared-ref backfill found corrupt registry #{inspect(key)}"
        end

      _invalid ->
        raise "shared-ref backfill found corrupt registry #{inspect(key)}"
    end
  end

  defp decode_registry!(_value, key) do
    raise "shared-ref backfill found corrupt registry #{inspect(key)}"
  end

  defp encode_registry(refs), do: refs |> Enum.sort() |> :erlang.term_to_binary()

  defp decode_count!(value, key) when is_binary(value) do
    case safe_binary_to_term(value) do
      count when is_integer(count) and count > 0 -> count
      _invalid -> raise "shared-ref backfill found corrupt ref count #{inspect(key)}"
    end
  end

  defp decode_count!(_value, key) do
    raise "shared-ref backfill found corrupt ref count #{inspect(key)}"
  end

  defp decode_contribution!(encoded) do
    case safe_binary_to_term(encoded) do
      {:shared_ref, ref, registry_key, digest}
      when is_binary(ref) and is_binary(registry_key) and is_binary(digest) ->
        {ref, registry_key, digest}

      _invalid ->
        raise "shared-ref backfill found corrupt staged contribution"
    end
  end

  defp decode_work_key!(encoded) do
    case safe_binary_to_term(encoded) do
      {:key, key} when is_binary(key) -> key
      _invalid -> raise "shared-ref backfill found corrupt staged keydir row"
    end
  end

  defp safe_binary_to_term(value) do
    :erlang.binary_to_term(value, [:safe])
  rescue
    _ -> :invalid
  end

  defp record_info(record) do
    id = Map.fetch!(record, :id)
    partition_key = Map.get(record, :partition_key)

    %{
      id: id,
      partition_key: partition_key,
      registry_key: Keys.shared_value_ref_registry_key(id, partition_key),
      guard_key: Keys.retention_guard_key(id, partition_key),
      cleanup_index_key: Keys.retention_cleanup_index_key(id, partition_key)
    }
  end

  defp shared_refs(record) do
    record
    |> all_record_refs()
    |> Enum.filter(&Keys.shared_value_ref?/1)
    |> MapSet.new()
  end

  defp all_record_refs(record) do
    direct = Enum.map([:payload_ref, :result_ref, :error_ref], &Map.get(record, &1))

    named =
      record
      |> Flow.flow_record_value_refs()
      |> Map.values()
      |> Enum.map(&Map.get(&1, :ref))

    Enum.filter(direct ++ named, &is_binary/1)
  end

  defp owned_value_ref?(ref, record) do
    id = Map.fetch!(record, :id)
    partition_key = Map.get(record, :partition_key)

    Enum.any?([:payload, :result, :error, :shared], fn kind ->
      prefix = owned_value_prefix(id, kind, partition_key)

      if String.starts_with?(ref, prefix) do
        suffix = binary_part(ref, byte_size(prefix), byte_size(ref) - byte_size(prefix))
        owned_value_suffix?(suffix, kind)
      else
        false
      end
    end)
  end

  defp owned_value_prefix(id, kind, partition_key) do
    key = Keys.value_key(id, kind, 0, partition_key)
    {position, 1} = key |> :binary.matches(":") |> List.last()
    binary_part(key, 0, position + 1)
  end

  defp owned_value_suffix?(suffix, :shared) do
    value_version?(suffix) or
      case :binary.matches(suffix, ":") do
        [] ->
          false

        matches ->
          {position, 1} = List.last(matches)
          value_version?(binary_part(suffix, position + 1, byte_size(suffix) - position - 1))
      end
  end

  defp owned_value_suffix?(suffix, _kind), do: value_version?(suffix)

  defp value_version?(value) do
    case Integer.parse(value) do
      {version, ""} when version >= 0 -> true
      _invalid -> false
    end
  end

  defp key_marker_and_remainder(key) do
    with {:ok, tag, suffix} <- split_flow_key(key) do
      Enum.find_value(
        [
          {":v:p:", :private_value},
          {":v:r:", :private_value},
          {":v:e:", :private_value},
          {":v:s:", :shared_value},
          {":svl:", :shared_link},
          {":gov:e:", :governance_effect},
          {":gov:l:", :governance_ledger},
          {":gov:li:", :ledger_index}
        ],
        :other,
        fn {marker, kind} ->
          if String.starts_with?(suffix, marker) do
            remainder =
              binary_part(suffix, byte_size(marker), byte_size(suffix) - byte_size(marker))

            if remainder == "", do: :other, else: {kind, tag, remainder}
          end
        end
      )
    else
      :error -> :other
    end
  end

  defp split_flow_key(<<"f:{", rest::binary>>) do
    case :binary.match(rest, "}") do
      {position, 1} when position > 0 ->
        tag = "{" <> binary_part(rest, 0, position + 1)
        suffix_start = position + 1
        suffix = binary_part(rest, suffix_start, byte_size(rest) - suffix_start)

        if valid_flow_tag?(tag), do: {:ok, tag, suffix}, else: :error

      _invalid ->
        :error
    end
  end

  defp split_flow_key(_key), do: :error

  defp valid_flow_tag?("{f}"), do: true

  defp valid_flow_tag?("{fa:" <> bucket_with_brace) do
    with true <- String.ends_with?(bucket_with_brace, "}"),
         bucket <- String.trim_trailing(bucket_with_brace, "}"),
         {number, ""} when number in 0..255 <- Integer.parse(bucket),
         true <- bucket == Integer.to_string(number) do
      true
    else
      _invalid -> false
    end
  end

  defp valid_flow_tag?("{f:" <> digest_with_brace) do
    with true <- String.ends_with?(digest_with_brace, "}"),
         digest <- String.trim_trailing(digest_with_brace, "}"),
         true <- byte_size(digest) == 43,
         {:ok, decoded} <- Base.url_decode64(digest, padding: false),
         true <- byte_size(decoded) == 32 do
      true
    else
      _invalid -> false
    end
  end

  defp valid_flow_tag?(_tag), do: false

  defp split_versioned_ref(remainder) do
    case :binary.matches(remainder, ":") do
      [] ->
        :error

      matches ->
        {position, 1} = List.last(matches)
        id = binary_part(remainder, 0, position)
        version = binary_part(remainder, position + 1, byte_size(remainder) - position - 1)
        if id != "" and value_version?(version), do: {:ok, id, version}, else: :error
    end
  end

  defp owner_candidates(remainder) do
    prefixes =
      remainder
      |> :binary.matches(":")
      |> Enum.reverse()
      |> Enum.map(fn {position, 1} -> binary_part(remainder, 0, position) end)

    [remainder | prefixes]
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp history_entry_key?(key), do: match?({:ok, _state_key}, history_state_key(key))

  defp history_state_key(<<"X:", rest::binary>>) do
    with {separator, 1} <- :binary.match(rest, <<0>>),
         history_key <- binary_part(rest, 0, separator),
         true <- separator > 0 and separator < byte_size(rest) - 1,
         event_id <- binary_part(rest, separator + 1, byte_size(rest) - separator - 1),
         true <- valid_history_event_id?(event_id),
         {:ok, tag, ":h:" <> id} <- split_flow_key(history_key),
         true <- id != "" do
      {:ok, "f:" <> tag <> ":s:" <> id}
    else
      _invalid -> :error
    end
  end

  defp history_state_key(_key), do: :error

  defp valid_history_event_id?(event_id) do
    case String.split(event_id, "-", parts: 2) do
      [milliseconds, version] ->
        nonnegative_decimal?(milliseconds) and nonnegative_decimal?(version)

      _invalid ->
        false
    end
  end

  defp nonnegative_decimal?(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 -> value == Integer.to_string(number)
      _invalid -> false
    end
  end

  defp shared_registry_key?(key), do: flow_suffix?(key, ":svr:")
  defp shared_count_key?(key), do: flow_suffix?(key, ":svc:")
  defp retention_guard_key?(key), do: flow_suffix?(key, ":rtg:")
  defp flow_state_key?(key), do: flow_suffix?(key, ":s:")
  defp cleanup_member_key?(key), do: flow_suffix?(key, ":rtm:")

  defp flow_suffix?(key, prefix) do
    case split_flow_key(key) do
      {:ok, _tag, suffix} ->
        String.starts_with?(suffix, prefix) and byte_size(suffix) > byte_size(prefix)

      :error ->
        false
    end
  end

  defp migration_primary_key?(ctx, key) do
    key == progress_key(ctx.shard_index) or
      key == Keys.shared_value_ref_backfill_key(ctx.shard_index)
  end

  defp migration_candidate_key?(key) do
    flow_state_key?(key) or cleanup_member_key?(key) or history_entry_key?(key) or
      shared_registry_key?(key) or shared_count_key?(key) or retention_guard_key?(key) or
      key_marker_and_remainder(key) != :other
  end

  defp rebuild_cleanup_index!(%{native: nil}), do: :ok

  defp rebuild_cleanup_index!(ctx) do
    with_fixed_keydir!(ctx.keydir, fn ->
      rebuild_cleanup_pages!(ctx, :ets.select(ctx.keydir, keydir_match_spec(), ctx.batch_size))
    end)
  end

  defp rebuild_cleanup_pages!(_ctx, :"$end_of_table"), do: :ok

  defp rebuild_cleanup_pages!(ctx, {entries, continuation}) do
    register_cleanup_entries!(ctx, entries)
    rebuild_cleanup_pages!(ctx, :ets.select(continuation))
  end

  defp register_cleanup_entries!(ctx, entries) do
    Enum.each(entries, fn
      {:key, key} when is_binary(key) ->
        if cleanup_member_key?(key) do
          case lookup_primary_value!(ctx, key) do
            {:ok, value} -> register_cleanup_member!(ctx, key, value)
            :not_found -> :ok
          end
        end

      {:invalid} ->
        raise "shared-ref backfill encountered an invalid keydir row"

      _invalid ->
        raise "shared-ref backfill encountered an invalid keydir key"
    end)
  end

  defp register_cleanup_member!(ctx, key, value) do
    case validate_cleanup_member!(ctx, key, value) do
      {:ok, index_key, _owned_key} ->
        if ctx.native do
          NativeOrderedIndex.put_member(ctx.native, index_key, key, 0)
        end

      {:stale, index_key, _owned_key} ->
        persist_deletes!(ctx, [key])

        if ctx.native do
          NativeOrderedIndex.delete_member(ctx.native, index_key, key)
        end
    end

    :ok
  end

  defp validate_cleanup_member!(ctx, member_key, value) do
    with {:ok, {index_key, owned_key}} <- RetentionCleanupMember.decode(value),
         {:ok, tag, ":i:rtc:" <> owner_id} when owner_id != "" <- split_flow_key(index_key),
         true <- cleanup_member_key(tag, owner_id, owned_key) == member_key do
      validate_cleanup_member_owner!(ctx, member_key, index_key, owned_key, tag, owner_id)
    else
      _invalid -> raise "shared-ref backfill found forged cleanup member owner"
    end
  end

  defp validate_cleanup_member_owner!(
         ctx,
         member_key,
         index_key,
         owned_key,
         tag,
         owner_id
       ) do
    state_key = "f:" <> tag <> ":s:" <> owner_id

    case lookup_record!(ctx, state_key) do
      :not_found ->
        {:stale, index_key, owned_key}

      {:ok, %{id: ^owner_id} = record} ->
        expected_index =
          Keys.retention_cleanup_index_key(owner_id, Map.get(record, :partition_key))

        expected_member =
          Keys.retention_cleanup_member_key(
            owner_id,
            owned_key,
            Map.get(record, :partition_key)
          )

        if expected_index == index_key and expected_member == member_key and
             cleanup_owned_key?(owned_key, record) do
          {:ok, index_key, owned_key}
        else
          raise "shared-ref backfill found forged cleanup member owner"
        end

      _mismatched ->
        raise "shared-ref backfill found forged cleanup member owner"
    end
  end

  defp cleanup_member_key(tag, owner_id, owned_key) do
    digest = :crypto.hash(:sha256, owned_key) |> Base.url_encode64(padding: false)
    "f:" <> tag <> ":rtm:" <> owner_id <> ":" <> digest
  end

  defp cleanup_owned_key?(owned_key, record) do
    id = Map.fetch!(record, :id)
    partition_key = Map.get(record, :partition_key)

    owned_value_ref?(owned_key, record) or
      String.starts_with?(owned_key, Keys.shared_value_link_prefix(id, partition_key)) or
      String.starts_with?(owned_key, Keys.governance_effect_key_prefix(id, partition_key)) or
      String.starts_with?(owned_key, Keys.governance_ledger_key_prefix(id, partition_key)) or
      owned_key == Keys.governance_ledger_index_key(id, partition_key)
  end

  defp lookup_primary_value!(ctx, key) do
    case :ets.lookup(ctx.keydir, key) do
      [entry] ->
        case read_entry_value!(ctx, entry) do
          :__missing_shared_ref_primary_value__ -> :not_found
          value -> {:ok, value}
        end

      [] ->
        :not_found

      _invalid ->
        raise "shared-ref backfill keydir contains duplicate rows"
    end
  rescue
    error in ArgumentError ->
      raise "shared-ref backfill keydir read failed: #{Exception.message(error)}"
  end

  defp read_entry_value!(
         ctx,
         {_key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}
       )
       when is_binary(value) do
    materialize!(ctx, value)
  end

  defp read_entry_value!(
         ctx,
         {key, nil, _expire_at_ms, _lfu, file_id, offset, value_size}
       )
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0 do
    path = ShardETS.file_path(ctx.shard_path, file_id)

    case ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms) do
      {:ok, value} when is_binary(value) -> materialize!(ctx, value)
      other -> raise "shared-ref backfill primary read failed: #{inspect(other)}"
    end
  end

  defp read_entry_value!(
         ctx,
         {key, nil, _expire_at_ms, _lfu, file_id, offset, value_size}
       )
       when valid_waraft_location(file_id, offset, value_size) do
    case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location_including_expired(
           ctx.instance_ctx,
           ctx.shard_index,
           file_id,
           key
         ) do
      {:ok, value} when is_binary(value) -> materialize!(ctx, value)
      :not_found -> :__missing_shared_ref_primary_value__
      other -> raise "shared-ref backfill WARaft read failed: #{inspect(other)}"
    end
  end

  defp read_entry_value!(_ctx, _entry) do
    raise "shared-ref backfill encountered an unreadable keydir row"
  end

  defp materialize!(%{instance_ctx: %{data_dir: data_dir}} = ctx, value) do
    case BlobValue.maybe_materialize(
           data_dir,
           ctx.shard_index,
           BlobValue.threshold(ctx.instance_ctx),
           value
         ) do
      {:ok, materialized} -> materialized
      other -> raise "shared-ref backfill blob materialization failed: #{inspect(other)}"
    end
  end

  defp materialize!(_ctx, value), do: value

  defp persist_puts!(_ctx, puts) when map_size(puts) == 0, do: :ok

  defp persist_puts!(ctx, puts) do
    puts
    |> Enum.sort_by(fn {key, _value} -> key end)
    |> bounded_chunks(ctx.batch_size, ctx.batch_bytes, fn {key, value} ->
      byte_size(key) + byte_size(value)
    end)
    |> Enum.each(&persist_primary_chunk!(ctx, &1))
  end

  defp persist_primary_chunk!(ctx, chunk) do
    {file_id, file_path} = active_file(ctx)
    records = Enum.map(chunk, fn {key, value} -> {key, value, 0} end)

    locations =
      case Application.get_env(:ferricstore, :flow_shared_ref_backfill_write_hook) do
        fun when is_function(fun, 2) -> fun.(file_path, records)
        _missing -> NIF.v2_append_batch_nosync(file_path, records)
      end

    case locations do
      {:ok, locations} when length(locations) == length(chunk) ->
        if Enum.all?(locations, &valid_append_location?/1) do
          Enum.zip(chunk, locations)
          |> Enum.each(fn {{key, value}, {offset, _record_size}} ->
            :ets.insert(
              ctx.keydir,
              {key, value, 0, LFU.initial(), file_id, offset, byte_size(value)}
            )
          end)

          emit_batch(:primary_write, chunk, fn {key, value} ->
            byte_size(key) + byte_size(value)
          end)
        else
          raise "shared-ref backfill persistence returned invalid locations"
        end

      {:error, reason} ->
        raise "shared-ref backfill persistence failed: #{inspect(reason)}"

      other ->
        raise "shared-ref backfill persistence returned invalid locations: #{inspect(other)}"
    end
  end

  defp valid_append_location?({offset, record_size})
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size >= 0,
       do: true

  defp valid_append_location?(_location), do: false

  defp persist_deletes!(_ctx, []), do: :ok

  defp persist_deletes!(ctx, keys) do
    {_file_id, file_path} = active_file(ctx)
    ops = Enum.map(keys, &{:delete, &1})

    case NIF.v2_append_ops_batch_nosync(file_path, ops) do
      {:ok, locations} when length(locations) == length(keys) ->
        if Enum.all?(locations, &valid_delete_location?/1) do
          Enum.each(keys, &:ets.delete(ctx.keydir, &1))

          emit_phase(
            {:batch, :primary_delete,
             %{items: length(keys), bytes: Enum.reduce(keys, 0, &(&2 + byte_size(&1)))}}
          )

          :ok
        else
          raise "shared-ref backfill tombstone persistence returned invalid locations"
        end

      {:ok, _wrong_length} ->
        raise "shared-ref backfill tombstone persistence returned invalid locations"

      {:error, reason} ->
        raise "shared-ref backfill tombstone persistence failed: #{inspect(reason)}"

      other ->
        raise "shared-ref backfill tombstone persistence returned #{inspect(other)}"
    end
  end

  defp valid_delete_location?({:delete, offset, record_size})
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size >= 0,
       do: true

  defp valid_delete_location?(_location), do: false

  defp active_file(%{active_file_id: file_id, active_file_path: file_path})
       when is_integer(file_id) and is_binary(file_path),
       do: {file_id, file_path}

  defp active_file(ctx) do
    {file_id, _current_size} = ShardLifecycle.discover_active_file(ctx.shard_path)
    {file_id, ShardETS.file_path(ctx.shard_path, file_id)}
  end

  defp fsync_primary!(ctx) do
    {_file_id, file_path} = active_file(ctx)
    correlation_id = System.unique_integer([:positive])

    case NIF.v2_fsync_async(self(), correlation_id, file_path) do
      :ok ->
        receive do
          {:tokio_complete, ^correlation_id, :ok, _result} ->
            :ok

          {:tokio_complete, ^correlation_id, :error, reason} ->
            raise "shared-ref backfill fsync failed: #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "shared-ref backfill fsync submission failed: #{inspect(reason)}"

      other ->
        raise "shared-ref backfill fsync submission returned #{inspect(other)}"
    end
  end

  defp lmdb_write_ops_bounded!(_ctx, _phase, []), do: :ok

  defp lmdb_write_ops_bounded!(ctx, phase, ops) do
    ops
    |> bounded_chunks(ctx.batch_size, ctx.batch_bytes, &lmdb_op_bytes/1)
    |> Enum.each(fn chunk ->
      lmdb_write_batch!(ctx, chunk)
      emit_batch(phase, chunk, &lmdb_op_bytes/1)
    end)
  end

  defp lmdb_write_stream_bounded!(ctx, phase, enumerable, mapper) do
    {chunk, _count, _bytes} =
      Enum.reduce(enumerable, {[], 0, 0}, fn item, {chunk, count, bytes} ->
        op = mapper.(item)
        size = lmdb_op_bytes(op)

        if count > 0 and (count >= ctx.batch_size or bytes + size > ctx.batch_bytes) do
          flush_lmdb_chunk!(ctx, phase, chunk)
          {[op], 1, size}
        else
          {[op | chunk], count + 1, bytes + size}
        end
      end)

    flush_lmdb_chunk!(ctx, phase, chunk)
  end

  defp flush_lmdb_chunk!(_ctx, _phase, []), do: :ok

  defp flush_lmdb_chunk!(ctx, phase, reversed_chunk) do
    chunk = Enum.reverse(reversed_chunk)
    lmdb_write_batch!(ctx, chunk)
    emit_batch(phase, chunk, &lmdb_op_bytes/1)
  end

  defp lmdb_op_bytes({:put, key, value}), do: byte_size(key) + byte_size(value)
  defp lmdb_op_bytes({:delete, key}), do: byte_size(key)

  defp bounded_chunks(entries, max_items, max_bytes, size_fun) do
    {chunks, current, _count, _bytes} =
      Enum.reduce(entries, {[], [], 0, 0}, fn entry, {chunks, current, count, bytes} ->
        size = size_fun.(entry)

        if count > 0 and (count >= max_items or bytes + size > max_bytes) do
          {[Enum.reverse(current) | chunks], [entry], 1, size}
        else
          {chunks, [entry | current], count + 1, bytes + size}
        end
      end)

    case current do
      [] -> Enum.reverse(chunks)
      _entries -> Enum.reverse([Enum.reverse(current) | chunks])
    end
  end

  defp lmdb_get!(ctx, key) do
    lmdb_call!(:get, [ctx.lmdb_path, key], fn -> LMDB.get(ctx.lmdb_path, key) end)
  end

  defp lmdb_prefix_entries_after_bounded!(ctx, prefix, cursor, max_items, max_bytes) do
    case lmdb_call!(
           :prefix_entries_after,
           [ctx.lmdb_path, prefix, cursor, max_items, max_bytes],
           fn ->
             LMDB.prefix_entries_after_bounded(
               ctx.lmdb_path,
               prefix,
               cursor,
               max_items,
               max_bytes
             )
           end
         ) do
      {:ok, entries} when is_list(entries) -> entries
      {:error, reason} -> raise "shared-ref backfill LMDB scan failed: #{inspect(reason)}"
      other -> raise "shared-ref backfill LMDB scan returned #{inspect(other)}"
    end
  end

  defp lmdb_write_batch!(ctx, ops) do
    case lmdb_call!(:write_batch, [ctx.lmdb_path, ops], fn ->
           LMDB.write_batch(ctx.lmdb_path, ops)
         end) do
      :ok -> :ok
      {:error, reason} -> raise "shared-ref backfill LMDB write failed: #{inspect(reason)}"
      other -> raise "shared-ref backfill LMDB write returned #{inspect(other)}"
    end
  end

  defp lmdb_call!(operation, args, fallback) do
    case Application.get_env(:ferricstore, :flow_shared_ref_backfill_lmdb_hook) do
      fun when is_function(fun, 2) ->
        case fun.(operation, args) do
          :passthrough -> fallback.()
          result -> result
        end

      _missing ->
        fallback.()
    end
  end

  defp delete_staging_page!(ctx, cursor) do
    case lmdb_prefix_entries_after_bounded!(
           ctx,
           @staging_root,
           cursor,
           ctx.batch_size,
           ctx.batch_bytes
         ) do
      [] ->
        :done

      entries ->
        ops = Enum.map(entries, fn {key, _value} -> {:delete, key} end)
        lmdb_write_ops_bounded!(ctx, :cleanup_staging, ops)
        {next_cursor, _value} = List.last(entries)
        {:more, next_cursor, length(entries)}
    end
  end

  defp save_progress!(ctx, progress) do
    persist_puts!(ctx, %{progress_key(ctx.shard_index) => encode_progress(progress)})
    fsync_primary!(ctx)
    publish_progress_proof!(ctx, progress)
    progress
  end

  defp publish_progress_proof!(ctx, progress) do
    lmdb_write_batch!(ctx, [
      {:put, progress_proof_key(ctx.shard_index), encode_progress(progress)}
    ])
  end

  defp save_page_progress!(ctx, progress) do
    saved = save_progress!(ctx, progress)
    emit_phase({:page_persisted, progress.phase, %{processed: progress.processed}})
    saved
  end

  defp emit_batch(phase, entries, size_fun) do
    emit_phase(
      {:batch, phase,
       %{items: length(entries), bytes: Enum.reduce(entries, 0, &(&2 + size_fun.(&1)))}}
    )
  end

  defp emit_read_batch(phase, entries) do
    emit_phase(
      {:read_batch, phase,
       %{
         items: length(entries),
         bytes:
           Enum.reduce(entries, 0, fn {key, value}, total ->
             total + byte_size(key) + byte_size(value)
           end)
       }}
    )
  end

  defp emit_phase(event) do
    case Application.get_env(:ferricstore, :flow_shared_ref_backfill_phase_hook) do
      fun when is_function(fun, 1) -> fun.(event)
      _missing -> :ok
    end
  end

  defp progress(run_id, phase, cursor, processed) do
    %{version: @version, run_id: run_id, phase: phase, cursor: cursor, processed: processed}
  end

  defp encode_progress(progress) do
    :erlang.term_to_binary(
      {:shared_ref_backfill_progress, @version, progress.run_id, progress.phase, progress.cursor,
       progress.processed}
    )
  end

  defp decode_progress!(encoded) do
    case safe_binary_to_term(encoded) do
      {:shared_ref_backfill_progress, @version, run_id, phase, cursor, processed}
      when is_binary(run_id) and is_atom(phase) and is_binary(cursor) and is_integer(processed) and
             processed >= 0 ->
        progress(run_id, phase, cursor, processed)

      _invalid ->
        raise "shared-ref backfill found corrupt migration progress"
    end
  end

  defp encode_count_cursor(after_key, ref, count) do
    :erlang.term_to_binary({after_key, ref, count})
  end

  defp decode_count_cursor!(encoded) do
    case safe_binary_to_term(encoded) do
      {after_key, ref, count}
      when is_binary(after_key) and (is_binary(ref) or is_nil(ref)) and is_integer(count) and
             count >= 0 ->
        {after_key, ref, count}

      _invalid ->
        raise "shared-ref backfill found corrupt count cursor"
    end
  end

  defp new_run_id, do: :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
  defp run_prefix(run_id), do: @staging_root <> run_id <> ":"
  defp manifest_key(run_id), do: run_prefix(run_id) <> "manifest"
  defp work_prefix(run_id), do: run_prefix(run_id) <> "work:"
  defp contribution_prefix(run_id), do: run_prefix(run_id) <> "refs:"
  defp registry_snapshot_prefix(run_id), do: run_prefix(run_id) <> "registries:"
  defp existing_count_prefix(run_id), do: run_prefix(run_id) <> "existing-counts:"
  defp count_result_prefix(run_id), do: run_prefix(run_id) <> "count-results:"

  defp work_key(run_id, key) do
    work_prefix(run_id) <> Base.url_encode64(:crypto.hash(:sha256, key), padding: false)
  end

  defp contribution_key(run_id, ref, registry_key) do
    contribution_prefix(run_id) <>
      ref_digest(ref) <>
      ":" <> Base.url_encode64(:crypto.hash(:sha256, registry_key), padding: false)
  end

  defp registry_snapshot_key(run_id, registry_key) do
    registry_snapshot_prefix(run_id) <>
      Base.url_encode64(:crypto.hash(:sha256, registry_key), padding: false)
  end

  defp existing_count_key(run_id, count_key) do
    existing_count_prefix(run_id) <>
      Base.url_encode64(:crypto.hash(:sha256, count_key), padding: false)
  end

  defp count_result_key(run_id, count_key) do
    count_result_prefix(run_id) <>
      Base.url_encode64(:crypto.hash(:sha256, count_key), padding: false)
  end

  defp ref_digest(ref), do: Base.url_encode64(:crypto.hash(:sha256, ref), padding: false)

  defp key_present!(keydir, key) do
    :ets.member(keydir, key)
  rescue
    error in ArgumentError ->
      raise "shared-ref backfill keydir read failed: #{Exception.message(error)}"
  end

  defp with_fixed_keydir!(keydir, fun) do
    :ets.safe_fixtable(keydir, true)

    try do
      fun.()
    after
      :ets.safe_fixtable(keydir, false)
    end
  rescue
    error in ArgumentError ->
      raise "shared-ref backfill keydir scan failed: #{Exception.message(error)}"
  end

  defp keydir_match_spec do
    [
      {{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"}, [], [{{:key, :"$1"}}]},
      {:"$1", [{:"/=", {:tuple_size, :"$1"}, 7}], [{{:invalid}}]}
    ]
  end
end
