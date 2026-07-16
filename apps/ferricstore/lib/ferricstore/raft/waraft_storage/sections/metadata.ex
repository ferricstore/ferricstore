defmodule Ferricstore.Raft.WARaftStorage.Sections.Metadata do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Promotion
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.ZSetIndex

      defp recovery_storage_metadata_candidate(path, candidates) do
        journal_path = metadata_journal_path(path)

        case Enum.find(candidates, fn
               {^journal_path, metadata} -> snapshot_boundary_metadata?(metadata)
               _candidate -> false
             end) do
          nil -> newest_storage_metadata_candidate(candidates)
          boundary_candidate -> boundary_candidate
        end
      end

      defp read_recovery_metadata_candidates(path) do
        previous_path = metadata_previous_path(path)
        journal_path = metadata_journal_path(path)

        [
          {previous_path, read_previous_storage_metadata(path), :previous},
          {journal_path, read_latest_storage_metadata_journal(path), :journal}
        ]
        |> Enum.reduce({[], %{}}, fn
          {source_path, {:ok, metadata}, source_key}, {candidates, errors} ->
            {[{source_path, metadata} | candidates], Map.delete(errors, source_key)}

          {_source_path, {:error, reason}, source_key}, {candidates, errors} ->
            {candidates, Map.put(errors, source_key, reason)}
        end)
      end

      defp newest_storage_metadata_candidate(candidates) do
        Enum.max_by(candidates, fn {_source_path, metadata} ->
          storage_metadata_position_key(metadata)
        end)
      end

      defp storage_metadata_position_key(%{position: {:raft_log_pos, index, term}})
           when is_integer(index) and is_integer(term),
           do: {index, term}

      defp read_metadata_if_present(path) do
        case read_storage_metadata_file(path, :storage_metadata_file_too_large) do
          {:ok, binary} ->
            case persisted_binary_to_term(binary) do
              {:ok, %{version: @version} = metadata} ->
                case validate_storage_metadata(metadata) do
                  {:ok, validated} -> validated
                  {:error, reason} -> {:error, reason}
                end

              {:ok, other} ->
                {:error, {:bad_storage_metadata, other}}

              {:error, reason} ->
                {:error, {:decode_storage_metadata, reason}}
            end

          {:error, :enoent} ->
            %{}

          {:error, reason} ->
            {:error, {:read_storage_metadata, reason}}
        end
      end

      defp read_storage_metadata_file(path, too_large_reason) do
        read_bounded_metadata_file(path, @max_storage_metadata_bytes, too_large_reason)
      end

      defp validate_storage_metadata(%{position: position, config: config} = metadata)
           when is_map(metadata) do
        with :ok <- validate_raft_position(position),
             :ok <- validate_storage_config(config),
             :ok <- validate_storage_snapshot_boundary(metadata),
             :ok <- validate_apply_context_metadata(metadata) do
          {:ok, metadata}
        else
          {:error, reason} -> {:error, {:bad_storage_metadata, reason}}
        end
      end

      defp validate_storage_metadata(%{position: position} = metadata) when is_map(metadata) do
        with :ok <- validate_raft_position(position),
             :ok <- validate_storage_snapshot_boundary(metadata),
             :ok <- validate_apply_context_metadata(metadata) do
          {:ok, metadata}
        else
          {:error, reason} -> {:error, {:bad_storage_metadata, reason}}
        end
      end

      defp validate_storage_metadata(metadata) when is_map(metadata),
        do: {:error, {:bad_storage_metadata, :missing_position}}

      defp validate_storage_metadata(other), do: {:error, {:bad_storage_metadata, other}}

      defp validate_storage_config(nil), do: :ok

      defp validate_storage_config({position, config}) when is_map(config) do
        validate_raft_position(position)
      end

      defp validate_storage_config(other), do: {:error, {:bad_config, other}}

      defp validate_storage_snapshot_boundary(%{
             position: position,
             snapshot_boundary_position: position
           }) do
        validate_raft_position(position)
      end

      defp validate_storage_snapshot_boundary(%{
             position: position,
             snapshot_boundary_position: boundary
           }) do
        with :ok <- validate_raft_position(boundary) do
          {:error, {:snapshot_boundary_position_mismatch, position, boundary}}
        end
      end

      defp validate_storage_snapshot_boundary(_metadata), do: :ok

      defp validate_apply_context_metadata(%{apply_context: context}) do
        if Ferricstore.Raft.ApplyContext.valid?(context) do
          :ok
        else
          {:error, :invalid_apply_context}
        end
      end

      defp validate_apply_context_metadata(_metadata), do: {:error, :missing_apply_context}

      defp validate_raft_position({:raft_log_pos, index, term})
           when is_integer(index) and index >= 0 and is_integer(term) and term >= 0,
           do: :ok

      defp validate_raft_position(other), do: {:error, {:bad_position, other}}

      defp persisted_binary_to_term(binary) do
        with {:ok, term} <- Ferricstore.TermCodec.decode(binary) do
          {:ok, decode_persisted_metadata_term(term)}
        end
      rescue
        error -> {:error, error}
      end

      defp encode_persisted_metadata_term(%{} = metadata) do
        metadata
        |> Map.update(:config, nil, &encode_persisted_storage_config/1)
        |> Map.update(:apply_context, nil, &encode_persisted_apply_context/1)
      end

      defp encode_persisted_metadata_term(other), do: other

      defp decode_persisted_metadata_term(%{} = metadata) do
        metadata = Map.update(metadata, :config, nil, &decode_persisted_storage_config/1)

        case Map.fetch(metadata, :apply_context) do
          {:ok, context} ->
            Map.put(metadata, :apply_context, decode_persisted_apply_context(context))

          :error ->
            metadata
        end
      end

      defp decode_persisted_metadata_term(other), do: other

      defp encode_persisted_storage_config({position, config}) when is_map(config),
        do: {position, Ferricstore.Raft.WARaftStorage.PersistedConfig.encode!(config)}

      defp encode_persisted_storage_config(other), do: other

      defp decode_persisted_storage_config({position, config}) when is_map(config),
        do: {position, Ferricstore.Raft.WARaftStorage.PersistedConfig.decode!(config)}

      defp decode_persisted_storage_config(other), do: other

      defp encode_persisted_apply_context(%Ferricstore.Raft.ApplyContext{} = context),
        do: Ferricstore.Raft.ApplyContext.encode(context)

      defp encode_persisted_apply_context(other), do: other

      defp decode_persisted_apply_context(
             {:flow_apply_context_v1, _retention_ttl_ms, _history_hot, _history_max,
              _max_history_hot, _max_history, _cleanup_keys, _cleanup_bytes, _history_scan,
              _value_scan, _hibernation_enabled, _hot_window_ms, _safety_margin_ms,
              _promote_window_ms, _late_promote_window_ms, _flow_max_batch_items,
              _promotion_threshold, _compound_delete_member_budget, _max_value_size} = encoded
           ) do
        case Ferricstore.Raft.ApplyContext.decode(encoded) do
          {:ok, context} -> context
          {:error, :invalid_apply_context} -> encoded
        end
      end

      defp decode_persisted_apply_context(other), do: other

      @doc false
      def __decode_persisted_waraft_config_for_test__(config) do
        Ferricstore.Raft.WARaftStorage.PersistedConfig.decode(config)
      end

      defp metadata_path(root_dir), do: Path.join(root_dir, @metadata_file)

      defp metadata_previous_path(path), do: path <> @metadata_previous_suffix

      defp metadata_journal_path(path), do: path <> @metadata_journal_suffix

      defp segment_projection_root(root_dir), do: Path.join(root_dir, @segment_projection_dir)

      defp segment_projection_checkpoint_root(root_dir),
        do: Path.join(root_dir, @segment_projection_checkpoint_dir)

      defp segment_projection_files_present?(root_dir) do
        Ferricstore.FS.exists?(segment_projection_root(root_dir)) or
          Ferricstore.FS.exists?(segment_projection_checkpoint_root(root_dir))
      end

      defp apply_projection_root(root_dir), do: Path.join(root_dir, @apply_projection_dir)

      defp ensure_apply_projection_segment_log_ready!(root_dir) do
        case ensure_apply_projection_segment_log_ready(root_dir) do
          :ok ->
            :ok

          {:error, reason} ->
            raise "failed to initialize WARaft apply-projection segment log: #{inspect(reason)}"
        end
      end

      defp ensure_apply_projection_segment_log_ready(root_dir) do
        case :ferricstore_waraft_spike_segment_log.ensure_segment_config(
               root_dir
               |> apply_projection_root()
               |> to_charlist()
             ) do
          :ok ->
            :ok

          {:error, reason} ->
            {:error, reason}

          other ->
            {:error, other}
        end
      end

      defp maybe_write_snapshot_segment_projection(snapshot_path, handle) do
        with {:ok, entries} <- collect_segment_projected_entries_strict(handle.sm_state) do
          case entries do
            [] ->
              {:ok, nil}

            _ ->
              projection_root = Path.join(snapshot_path, @segment_projection_dir)

              case write_segment_projection(projection_root, handle.position, entries) do
                :ok ->
                  {:ok,
                   %{
                     dir: @segment_projection_dir,
                     format: :segment_log,
                     count: length(entries)
                   }}

                {:error, reason} ->
                  {:error, {:write_segment_projection_snapshot, reason}}
              end
          end
        else
          {:error, reason} -> {:error, {:collect_segment_projection_snapshot, reason}}
        end
      end

      defp write_segment_projection(projection_root, position, entries) do
        case :ferricstore_waraft_spike_segment_log.write_projection(
               to_charlist(projection_root),
               position,
               entries
             ) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
          other -> {:error, {:write_segment_projection_log, other}}
        end
      end

      defp collect_segment_projected_entries_strict(%{
             ets: keydir,
             instance_ctx: ctx,
             shard_index: shard_index
           }) do
        now = HLC.now_ms()

        case segment_projection_entries_from_keydir(keydir, ctx, shard_index, now) do
          :unavailable ->
            {:error, {:segment_keydir_unavailable, shard_index}}

          {:ok, {entries, _count}} ->
            {:ok, entries}

          {:error, _reason} = error ->
            error
        end
      rescue
        error -> {:error, {:collect_segment_projection_entries_failed, error}}
      end

      defp collect_segment_projected_entries_strict(_sm_state),
        do: {:error, :bad_segment_projection_state}

      defp collect_segment_projection_relocations(ctx, shard_index) do
        keydir = elem(ctx.keydir_refs, shard_index)

        collect_segment_projection_relocations(%{
          ets: keydir,
          instance_ctx: ctx,
          shard_index: shard_index
        })
      end

      defp collect_segment_projection_relocations(%{
             ets: keydir,
             instance_ctx: ctx,
             shard_index: shard_index
           }) do
        now = HLC.now_ms()

        case segment_projection_relocations_from_keydir(keydir, ctx, shard_index, now) do
          :unavailable ->
            {:error, {:segment_keydir_unavailable, shard_index}}

          {:ok, relocations} ->
            {:ok, relocations}

          {:error, _reason} = error ->
            error
        end
      rescue
        error -> {:error, {:collect_segment_projection_relocations_failed, error}}
      end

      defp collect_segment_projection_relocations(_sm_state),
        do: {:error, :bad_segment_projection_state}

      defp segment_projection_relocations_from_keydir(keydir, ctx, shard_index, now) do
        keydir
        |> reduce_keydir_rows_while([], fn row, acc ->
          case segment_projection_entry_from_keydir_row(row, ctx, shard_index, now) do
            {:ok, entry} -> {:cont, [{entry, row} | acc]}
            :skip -> {:cont, acc}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, relocations} ->
            {:ok, Enum.sort_by(relocations, fn {{key, _value, _expire_at_ms}, _row} -> key end)}

          {:error, _reason} = error ->
            error

          :unavailable ->
            :unavailable
        end
      end

      defp segment_projection_entries_from_relocations(relocations) do
        Enum.map(relocations, fn {entry, _row} -> entry end)
      end

      defp segment_projection_checkpoint_relocations(ctx, shard_index, entries, trim_index) do
        keydir = elem(ctx.keydir_refs, shard_index)
        now = HLC.now_ms()

        entry_by_key =
          entries
          |> Enum.with_index(1)
          |> Map.new(fn {{key, value, expire_at_ms}, projection_index} ->
            {key, {projection_index, value, expire_at_ms}}
          end)

        case reduce_keydir_rows_while(keydir, [], fn row, acc ->
               case segment_projection_checkpoint_relocation(row, entry_by_key, trim_index, now) do
                 {:ok, relocation} -> {:cont, [relocation | acc]}
                 :skip -> {:cont, acc}
                 {:error, reason} -> {:halt, {:error, reason}}
               end
             end) do
          :unavailable ->
            {:error, {:segment_keydir_unavailable, shard_index}}

          {:ok, relocations} ->
            {:ok, Enum.reverse(relocations)}

          {:error, _reason} = error ->
            error
        end
      rescue
        error -> {:error, {:segment_projection_checkpoint_relocations_failed, error}}
      end

      defp segment_projection_checkpoint_relocation(
             {key, _value, expire_at_ms, _lfu, file_id, _offset, _value_size} = row,
             entry_by_key,
             trim_index,
             now
           )
           when is_binary(key) do
        cond do
          not live_expire_at?(expire_at_ms, now) ->
            :skip

          not segment_projection_relocatable_file_id?(file_id, trim_index) ->
            :skip

          true ->
            case Map.fetch(entry_by_key, key) do
              {:ok, {projection_index, projected_value, ^expire_at_ms}} ->
                {:ok, {projection_index, {{key, projected_value, expire_at_ms}, row}}}

              {:ok, {_projection_index, _projected_value, projected_expire_at_ms}} ->
                {:error,
                 {:segment_projection_checkpoint_expire_mismatch, key, expire_at_ms,
                  projected_expire_at_ms}}

              :error ->
                {:error, {:segment_projection_checkpoint_missing_key, key, file_id}}
            end
        end
      end

      defp segment_projection_checkpoint_relocation(_row, _entry_by_key, _trim_index, _now),
        do: :skip

      defp segment_projection_relocatable_file_id?({:waraft_segment, index}, trim_index)
           when is_integer(index),
           do: index < trim_index

      defp segment_projection_relocatable_file_id?({:waraft_apply_projection, index}, trim_index)
           when is_integer(index),
           do: index < trim_index

      defp segment_projection_relocatable_file_id?({:waraft_projection, index}, _trim_index)
           when is_integer(index),
           do: true

      defp segment_projection_relocatable_file_id?(_file_id, _trim_index), do: false

      defp prepare_segment_value_pins_for_trim(root_dir, ctx, shard_index, trim_index) do
        prepare_segment_value_pins_for_trim(
          root_dir,
          ctx,
          shard_index,
          trim_index,
          @segment_value_pin_scan_limit
        )
      end

      defp prepare_segment_value_pins_for_trim(root_dir, ctx, shard_index, trim_index, page_limit) do
        lmdb_path = flow_lmdb_path(ctx, shard_index)

        do_prepare_segment_value_pins_for_trim(
          root_dir,
          ctx,
          shard_index,
          lmdb_path,
          trim_index,
          <<>>,
          page_limit,
          0
        )
      rescue
        error -> {:error, {:prepare_segment_value_pins_for_trim_failed, error}}
      end

      defp do_prepare_segment_value_pins_for_trim(
             root_dir,
             ctx,
             shard_index,
             lmdb_path,
             trim_index,
             after_key,
             page_limit,
             count
           ) do
        case FlowLMDB.segment_value_pin_entries_before_page(
               lmdb_path,
               trim_index,
               after_key,
               page_limit
             ) do
          {:ok, pins, next_after_key, done?} ->
            with {:ok, relocations} <-
                   segment_value_pin_relocations_from_pins(ctx, shard_index, pins),
                 :ok <- write_apply_projection_value_pins(root_dir, relocations),
                 :ok <- relocate_segment_value_pins(ctx, shard_index, relocations) do
              next_count = count + length(relocations)

              if done? do
                {:ok, next_count}
              else
                do_prepare_segment_value_pins_for_trim(
                  root_dir,
                  ctx,
                  shard_index,
                  lmdb_path,
                  trim_index,
                  next_after_key,
                  page_limit,
                  next_count
                )
              end
            end

          {:error, reason} ->
            {:error, {:collect_segment_value_pin_relocations_failed, reason}}
        end
      end

      defp segment_value_pin_relocations_from_pins(ctx, shard_index, pins) do
        pins
        |> Enum.reduce_while({:ok, []}, fn pin, {:ok, acc} ->
          case segment_value_pin_relocation_from_pin(ctx, shard_index, pin) do
            {:ok, relocation} -> {:cont, {:ok, [relocation | acc]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, relocations} -> {:ok, Enum.reverse(relocations)}
          {:error, _reason} = error -> error
        end
      end

      defp segment_value_pin_relocation_from_pin(
             ctx,
             shard_index,
             %{
               key: key,
               expire_at_ms: expire_at_ms,
               file_id: file_id,
               offset: offset,
               value_size: value_size,
               pin_key: pin_key
             }
           )
           when is_binary(key) and valid_segment_backed_file_id(file_id) and is_integer(offset) and
                  offset >= 0 and is_integer(value_size) and value_size >= 0 and
                  is_binary(pin_key) do
        if expired_segment_value_pin?(expire_at_ms) do
          {:ok,
           %{
             key: key,
             expire_at_ms: expire_at_ms,
             source_file_id: file_id,
             source_offset: offset,
             source_value_size: value_size,
             source_pin_key: pin_key,
             stale?: true
           }}
        else
          segment_value_pin_relocation_from_live_pin(
            ctx,
            shard_index,
            key,
            expire_at_ms,
            file_id,
            offset,
            value_size,
            pin_key
          )
        end
      end

      defp segment_value_pin_relocation_from_pin(_ctx, _shard_index, pin),
        do: {:error, {:bad_segment_value_pin, pin}}

      defp segment_value_pin_relocation_from_live_pin(
             ctx,
             shard_index,
             key,
             expire_at_ms,
             file_id,
             offset,
             value_size,
             pin_key
           ) do
        case WARaftSegmentReader.read_value_from_location(ctx, shard_index, file_id, key) do
          {:ok, value} when is_binary(value) ->
            {:ok,
             %{
               key: key,
               value: value,
               expire_at_ms: expire_at_ms,
               source_file_id: file_id,
               source_offset: offset,
               source_value_size: value_size,
               source_pin_key: pin_key
             }}

          :not_found ->
            {:error, {:segment_value_pin_missing_live_value, key, file_id}}

          {:error, reason} ->
            {:error, {:segment_value_pin_read_failed, key, file_id, reason}}
        end
      end

      defp expired_segment_value_pin?(expire_at_ms)
           when is_integer(expire_at_ms) and expire_at_ms > 0,
           do: not live_expire_at?(expire_at_ms, HLC.now_ms())

      defp expired_segment_value_pin?(_expire_at_ms), do: false

      defp write_apply_projection_value_pins(_root_dir, []), do: :ok

      defp write_apply_projection_value_pins(root_dir, relocations) do
        relocations =
          Enum.filter(relocations, fn
            %{stale?: true} ->
              false

            %{source_file_id: {:waraft_segment, index}} when is_integer(index) and index > 0 ->
              true

            _other ->
              false
          end)

        if relocations == [] do
          :ok
        else
          batches =
            relocations
            |> Enum.group_by(fn %{source_file_id: {_tag, index}} -> index end)
            |> Enum.map(fn {index, index_relocations} ->
              entries =
                Enum.map(index_relocations, fn %{
                                                 key: key,
                                                 value: value,
                                                 expire_at_ms: expire_at_ms
                                               } ->
                  {key, value, expire_at_ms}
                end)

              {{:raft_log_pos, index, 0}, entries}
            end)

          case :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
                 root_dir
                 |> apply_projection_root()
                 |> to_charlist(),
                 batches
               ) do
            :ok -> :ok
            {:error, reason} -> {:error, {:write_apply_projection_value_pins_failed, reason}}
            other -> {:error, {:write_apply_projection_value_pins_failed, other}}
          end
        end
      end

      defp compact_apply_projection_log(root_dir, ctx, shard_index, trim_index) do
        compact_apply_projection_log(
          root_dir,
          ctx,
          shard_index,
          trim_index,
          flow_lmdb_path(ctx, shard_index)
        )
      end

      defp compact_apply_projection_log(
             root_dir,
             ctx,
             shard_index,
             trim_index,
             retention_lmdb_path
           ) do
        with {:ok, retained_batches} <-
               collect_apply_projection_retention_batches(
                 ctx,
                 shard_index,
                 trim_index,
                 retention_lmdb_path
               ) do
          case :ferricstore_waraft_spike_segment_log.compact_apply_projection(
                 root_dir
                 |> apply_projection_root()
                 |> to_charlist(),
                 trim_index,
                 retained_batches
               ) do
            :ok -> :ok
            {:error, reason} -> {:error, {:compact_apply_projection_log_failed, reason}}
            other -> {:error, {:compact_apply_projection_log_failed, other}}
          end
        end
      rescue
        error -> {:error, {:compact_apply_projection_log_failed, error}}
      end

      defp collect_apply_projection_retention_batches(
             ctx,
             shard_index,
             trim_index,
             retention_lmdb_path
           ) do
        with {:ok, keydir_entries} <-
               collect_apply_projection_keydir_retention(ctx, shard_index, trim_index),
             {:ok, pin_entries} <-
               collect_apply_projection_pin_retention(
                 ctx,
                 shard_index,
                 trim_index,
                 retention_lmdb_path
               ),
             {:ok, entries_by_ref} <-
               merge_apply_projection_retention_entries(keydir_entries, pin_entries) do
          batches =
            entries_by_ref
            |> Enum.group_by(fn {{index, _key}, _entry} -> index end)
            |> Enum.sort_by(fn {index, _entries} -> index end)
            |> Enum.map(fn {index, entries} ->
              retained_entries =
                entries
                |> Enum.map(fn {_ref, entry} -> entry end)
                |> Enum.sort_by(&elem(&1, 0))

              {{:raft_log_pos, index, 0}, retained_entries}
            end)

          {:ok, batches}
        end
      end

      defp collect_apply_projection_keydir_retention(ctx, shard_index, trim_index) do
        keydir = elem(ctx.keydir_refs, shard_index)
        now = HLC.now_ms()

        case reduce_keydir_rows_while(keydir, [], fn
               {key, value, expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
                _value_size},
               acc
               when is_binary(key) and is_integer(index) and index > 0 and index < trim_index ->
                 if live_expire_at?(expire_at_ms, now) do
                   case apply_projection_retention_entry(
                          ctx,
                          shard_index,
                          index,
                          key,
                          value,
                          expire_at_ms
                        ) do
                     {:ok, entry} -> {:cont, [{{index, key}, entry} | acc]}
                     {:error, reason} -> {:halt, {:error, reason}}
                   end
                 else
                   {:cont, acc}
                 end

               _row, acc ->
                 {:cont, acc}
             end) do
          {:ok, entries} -> {:ok, Enum.reverse(entries)}
          :unavailable -> {:error, {:segment_keydir_unavailable, shard_index}}
          {:error, _reason} = error -> error
        end
      end

      defp collect_apply_projection_pin_retention(
             ctx,
             shard_index,
             trim_index,
             lmdb_path
           ) do
        do_collect_apply_projection_pin_retention(
          ctx,
          shard_index,
          lmdb_path,
          trim_index,
          <<>>,
          []
        )
      end

      defp do_collect_apply_projection_pin_retention(
             ctx,
             shard_index,
             lmdb_path,
             trim_index,
             after_key,
             acc
           ) do
        case FlowLMDB.segment_value_pin_entries_before_page(
               lmdb_path,
               trim_index,
               after_key,
               @segment_value_pin_scan_limit
             ) do
          {:ok, pins, next_after_key, done?} ->
            case collect_apply_projection_pin_retention_page(
                   ctx,
                   shard_index,
                   lmdb_path,
                   pins,
                   acc
                 ) do
              {:ok, next_acc} when done? ->
                {:ok, Enum.reverse(next_acc)}

              {:ok, next_acc} ->
                do_collect_apply_projection_pin_retention(
                  ctx,
                  shard_index,
                  lmdb_path,
                  trim_index,
                  next_after_key,
                  next_acc
                )

              {:error, _reason} = error ->
                error
            end

          {:error, reason} ->
            {:error, {:collect_apply_projection_pin_retention_failed, reason}}
        end
      end

      defp collect_apply_projection_pin_retention_page(
             ctx,
             shard_index,
             lmdb_path,
             pins,
             acc
           ) do
        Enum.reduce_while(pins, {:ok, acc}, fn
          %{
            key: key,
            expire_at_ms: expire_at_ms,
            file_id: {:waraft_apply_projection, index} = file_id,
            offset: offset,
            value_size: value_size
          },
          {:ok, entries}
          when is_binary(key) and is_integer(index) and index > 0 and is_integer(offset) and
                 offset >= 0 and is_integer(value_size) and value_size >= 0 ->
            case current_segment_value_pin_locator(
                   lmdb_path,
                   key,
                   file_id,
                   offset,
                   value_size
                 ) do
              :current ->
                case apply_projection_retention_entry(
                       ctx,
                       shard_index,
                       index,
                       key,
                       nil,
                       expire_at_ms
                     ) do
                  {:ok, entry} -> {:cont, {:ok, [{{index, key}, entry} | entries]}}
                  {:error, reason} -> {:halt, {:error, reason}}
                end

              :changed_or_deleted ->
                {:cont, {:ok, entries}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end

          %{file_id: {:waraft_segment, _index}} = pin, _entries ->
            {:halt, {:error, {:unrelocated_segment_value_pin, pin}}}

          _invalid, entries ->
            {:cont, entries}
        end)
      end

      defp apply_projection_retention_entry(
             _ctx,
             _shard_index,
             _index,
             key,
             value,
             expire_at_ms
           )
           when is_binary(key) and is_binary(value) and is_integer(expire_at_ms),
           do: {:ok, {key, value, expire_at_ms}}

      defp apply_projection_retention_entry(
             ctx,
             shard_index,
             index,
             key,
             _value,
             expire_at_ms
           )
           when is_binary(key) and is_integer(index) and index > 0 and
                  is_integer(expire_at_ms) do
        case WARaftSegmentReader.read_value_from_location_including_expired(
               ctx,
               shard_index,
               {:waraft_apply_projection, index},
               key
             ) do
          {:ok, value} when is_binary(value) ->
            {:ok, {key, value, expire_at_ms}}

          :not_found ->
            {:error, {:apply_projection_retention_value_missing, key, index}}

          {:error, reason} ->
            {:error, {:apply_projection_retention_read_failed, key, index, reason}}
        end
      end

      defp merge_apply_projection_retention_entries(left, right) do
        Enum.reduce_while(left ++ right, {:ok, %{}}, fn
          {ref, entry}, {:ok, acc} ->
            case Map.fetch(acc, ref) do
              :error ->
                {:cont, {:ok, Map.put(acc, ref, entry)}}

              {:ok, ^entry} ->
                {:cont, {:ok, acc}}

              {:ok, existing} ->
                {:halt, {:error, {:conflicting_apply_projection_retention, ref, existing, entry}}}
            end

          invalid, _acc ->
            {:halt, {:error, {:bad_apply_projection_retention_entry, invalid}}}
        end)
      end

      defp relocate_segment_projection_keydir(_ctx, _shard_index, _projection_root, []), do: :ok

      defp relocate_segment_projection_keydir(ctx, shard_index, projection_root, relocations) do
        keydir = elem(ctx.keydir_refs, shard_index)

        with :ok <-
               maybe_run_segment_projection_before_relocate_hook(
                 shard_index,
                 projection_root,
                 relocations
               ) do
          relocations
          |> Enum.with_index(1)
          |> Enum.reduce_while(:ok, fn {relocation, projection_index}, :ok ->
            case relocate_segment_projection_row(
                   keydir,
                   projection_root,
                   projection_index,
                   relocation
                 ) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
        end
      rescue
        error -> {:error, {:relocate_segment_projection_keydir_failed, error}}
      end

      defp relocate_segment_projection_keydir_from_checkpoint(
             _ctx,
             _shard_index,
             _projection_root,
             []
           ),
           do: :ok

      defp relocate_segment_projection_keydir_from_checkpoint(
             ctx,
             shard_index,
             projection_root,
             relocations
           ) do
        keydir = elem(ctx.keydir_refs, shard_index)

        with :ok <-
               maybe_run_segment_projection_before_relocate_hook(
                 shard_index,
                 projection_root,
                 relocations
               ) do
          Enum.reduce_while(relocations, :ok, fn {projection_index, relocation}, :ok ->
            case relocate_segment_projection_row(
                   keydir,
                   projection_root,
                   projection_index,
                   relocation
                 ) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
        end
      rescue
        error -> {:error, {:relocate_segment_projection_keydir_from_checkpoint_failed, error}}
      end

      defp relocate_segment_value_pins(_ctx, _shard_index, []), do: :ok

      defp relocate_segment_value_pins(
             ctx,
             shard_index,
             relocations
           ) do
        lmdb_path = flow_lmdb_path(ctx, shard_index)

        ops =
          relocations
          |> Enum.reduce_while({:ok, []}, fn relocation, {:ok, acc} ->
            case segment_value_pin_relocation_ops(lmdb_path, relocation) do
              {:ok, relocation_ops} -> {:cont, {:ok, [relocation_ops | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)

        with {:ok, op_groups} <- ops do
          lmdb_ops =
            op_groups
            |> Enum.reverse()
            |> List.flatten()
            |> Enum.uniq()

          FlowLMDB.write_batch(lmdb_path, lmdb_ops)
        end
      rescue
        error -> {:error, {:relocate_segment_value_pins_failed, error}}
      end

      defp segment_value_pin_relocation_ops(
             lmdb_path,
             %{
               key: key,
               expire_at_ms: expire_at_ms,
               source_file_id: source_file_id,
               source_offset: source_offset,
               source_value_size: source_value_size,
               source_pin_key: source_pin_key
             }
           ) do
        with :current <-
               current_segment_value_pin_locator(
                 lmdb_path,
                 key,
                 source_file_id,
                 source_offset,
                 source_value_size
               ) do
          case source_file_id do
            {:waraft_apply_projection, index} when is_integer(index) and index > 0 ->
              {:ok, []}

            {:waraft_segment, index} when is_integer(index) and index > 0 ->
              target =
                {key, expire_at_ms, {:waraft_apply_projection, index},
                 apply_projection_pin_target_offset(source_file_id, source_offset),
                 source_value_size}

              {:ok,
               FlowLMDB.segment_value_pin_batch_put_ops([target]) ++ [{:delete, source_pin_key}]}

            _other ->
              {:error, {:unsupported_segment_value_pin_source, source_file_id}}
          end
        else
          :changed_or_deleted ->
            {:ok, [{:delete, source_pin_key}]}

          {:error, _reason} = error ->
            error
        end
      end

      defp apply_projection_pin_target_offset({:waraft_segment, _index}, _source_offset), do: 0

      defp apply_projection_pin_target_offset({:waraft_apply_projection, _index}, source_offset),
        do: source_offset

      defp apply_projection_pin_target_offset(_file_id, source_offset), do: source_offset

      defp current_segment_value_pin_locator(
             lmdb_path,
             key,
             source_file_id,
             source_offset,
             source_value_size
           ) do
        case FlowLMDB.get(lmdb_path, key) do
          {:ok, blob} ->
            case FlowLMDB.decode_value_locator(blob, HLC.now_ms()) do
              {:ok, {^source_file_id, ^source_offset, ^source_value_size}} -> :current
              _expired_or_changed -> :changed_or_deleted
            end

          :not_found ->
            :changed_or_deleted

          {:error, reason} ->
            {:error, {:read_segment_value_pin_locator_failed, key, reason}}
        end
      end
    end
  end
end
