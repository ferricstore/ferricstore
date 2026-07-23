defmodule Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

      defp build_ctx(root, opts \\ []) do
        shard_count = Keyword.get(opts, :shard_count, 1)

        FerricStore.Instance.build(
          :"waraft_backend_test_#{System.unique_integer([:positive])}",
          instance_opts(root, shard_count: shard_count)
        )
      end

      defp assert_pending_keydir_rows(ctx, expected) do
        actual =
          0..(ctx.shard_count - 1)
          |> Enum.reduce(0, fn shard_index, acc ->
            keydir = elem(ctx.keydir_refs, shard_index)

            acc +
              :ets.select_count(keydir, [
                {{:_, :_, :_, :_, :pending, :_, :_}, [], [true]}
              ])
          end)

        assert actual == expected
      end

      defp blob_regular_files(data_dir, shard_index) do
        data_dir
        |> Ferricstore.DataDir.blob_shard_path(shard_index)
        |> Path.join("**/*")
        |> Path.wildcard()
        |> Enum.filter(&File.regular?/1)
      end

      defp instance_opts(root, opts) do
        [
          data_dir: root,
          shard_count: Keyword.get(opts, :shard_count, 1),
          query_index_provider: FerricStore.Flow.QueryIndexProvider.Disabled,
          max_memory_bytes: 256 * 1024 * 1024,
          keydir_max_ram: 64 * 1024 * 1024,
          hot_cache_max_value_size: 65_536,
          blob_side_channel_threshold_bytes: 256 * 1024,
          max_active_file_size: 64 * 1024 * 1024,
          read_sample_rate: 100,
          lfu_decay_time: 1,
          lfu_log_factor: 10
        ]
      end

      defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
      defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

      defp assert_receive_snapshot_payload_fsync(predicate) do
        deadline = System.monotonic_time(:millisecond) + 1_000
        assert_receive_snapshot_payload_fsync(predicate, deadline, [])
      end

      defp assert_receive_snapshot_payload_fsync(predicate, deadline, seen) do
        timeout = max(deadline - System.monotonic_time(:millisecond), 0)

        receive do
          {:snapshot_payload_fsync, path} ->
            if predicate.(path) do
              path
            else
              assert_receive_snapshot_payload_fsync(predicate, deadline, [path | seen])
            end
        after
          timeout ->
            flunk("expected matching snapshot payload fsync; saw: #{inspect(Enum.reverse(seen))}")
        end
      end

      defp assert_receive_apply_projection_sync do
        deadline = System.monotonic_time(:millisecond) + 1_000
        assert_receive_apply_projection_sync(deadline, [])
      end

      defp assert_receive_apply_projection_sync(deadline, seen) do
        timeout = max(deadline - System.monotonic_time(:millisecond), 0)

        receive do
          {:waraft_segment_log_file_sync, path, :datasync} ->
            path_string = to_string(path)

            if String.contains?(path_string, "apply_projection_log") do
              :ok
            else
              assert_receive_apply_projection_sync(deadline, [path_string | seen])
            end
        after
          timeout ->
            flunk(
              "trim-time Flow value pin materialization must be fdatasynced before the old segment can be removed; saw syncs: #{inspect(Enum.reverse(seen))}"
            )
        end
      end

      defp restore_waraft_app_env(key, nil),
        do: Application.delete_env(:ferricstore_waraft_backend, key)

      defp restore_waraft_app_env(key, value),
        do: Application.put_env(:ferricstore_waraft_backend, key, value)

      defp restore_ra_env(key, nil), do: Application.delete_env(:ra, key)
      defp restore_ra_env(key, value), do: Application.put_env(:ra, key, value)

      defp waraft_storage_label(shard_index) do
        :ferricstore_waraft_backend
        |> :wa_raft_storage.registered_name(shard_index + 1)
        |> :wa_raft_storage.label()
      end

      defp waraft_storage_status(shard_index) do
        :ferricstore_waraft_backend
        |> :wa_raft_storage.registered_name(shard_index + 1)
        |> :wa_raft_storage.status()
      end

      defp waraft_log_table(shard_index) do
        :"raft_log_ferricstore_waraft_backend_#{shard_index + 1}"
      end

      defp waraft_storage_metadata(root, shard_index) do
        root
        |> waraft_storage_metadata_path(shard_index)
        |> File.read!()
        |> :erlang.binary_to_term([:safe])
      end

      defp waraft_latest_storage_metadata(root, shard_index) do
        path = waraft_storage_metadata_path(root, shard_index)
        current = File.read!(path) |> :erlang.binary_to_term([:safe])

        case latest_storage_metadata_journal(path <> ".journal") do
          nil ->
            current

          journal ->
            Enum.max_by([current, journal], &storage_metadata_position_key/1)
        end
      end

      defp latest_storage_metadata_journal(path) do
        case File.read(path) do
          {:ok, binary} -> scan_storage_metadata_journal(binary, nil)
          {:error, :enoent} -> nil
        end
      end

      defp write_storage_metadata_journal!(path, metadata) do
        payload = :erlang.term_to_binary(metadata)
        record = <<"FSMJ1", byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
        File.write!(path, record)
      end

      defp scan_storage_metadata_journal(<<>>, latest), do: latest

      defp scan_storage_metadata_journal(<<"FSMJ1", size::32, crc::32, rest::binary>>, latest)
           when byte_size(rest) >= size do
        <<payload::binary-size(size), tail::binary>> = rest

        if :erlang.crc32(payload) == crc do
          scan_storage_metadata_journal(tail, :erlang.binary_to_term(payload, [:safe]))
        else
          latest
        end
      end

      defp scan_storage_metadata_journal(_partial_or_corrupt_tail, latest), do: latest

      defp storage_metadata_position_key(%{position: {:raft_log_pos, index, term}}),
        do: {index, term}

      defp waraft_storage_metadata_path(root, shard_index) do
        Path.join([
          root,
          "waraft",
          "ferricstore_waraft_backend.#{shard_index + 1}",
          "ferricstore_storage.term"
        ])
      end

      defp waraft_storage_metadata_previous_path(root, shard_index) do
        waraft_storage_metadata_path(root, shard_index) <> ".previous"
      end

      defp waraft_storage_metadata_journal_path(root, shard_index) do
        waraft_storage_metadata_path(root, shard_index) <> ".journal"
      end

      defp write_waraft_storage_metadata!(root, shard_index, metadata) do
        path = waraft_storage_metadata_path(root, shard_index)

        File.mkdir_p!(Path.dirname(path))
        File.write!(path, :erlang.term_to_binary(metadata))
        File.rm(waraft_storage_metadata_previous_path(root, shard_index))
        File.rm(waraft_storage_metadata_journal_path(root, shard_index))
      end

      defp read_segment_projection_header(root, shard_index) do
        projection_root =
          Path.join([
            root,
            "waraft",
            "ferricstore_waraft_backend.#{shard_index + 1}",
            "segment_projection_log"
          ])

        fold_fun = fn
          _index, {0, {:ferricstore_segment_projection_header, position, count}}, acc ->
            Map.put(acc, :header, {position, count})

          _index, {_term, {:ferricstore_segment_projection_header, position, count}}, acc ->
            Map.put(acc, :header, {position, count})

          _index, _entry, acc ->
            acc
        end

        case :ferricstore_waraft_spike_segment_log.fold_disk(
               to_charlist(projection_root),
               fold_fun,
               %{}
             ) do
          {:ok, %{header: header}} -> {:ok, header}
          {:ok, %{}} -> :not_found
          {:error, _reason} = error -> error
        end
      end

      defp read_snapshot_segment_projection_header(snapshot_path) do
        projection_root = Path.join(snapshot_path, "segment_projection_log")

        fold_fun = fn
          _index, {0, {:ferricstore_segment_projection_header, position, count}}, acc ->
            Map.put(acc, :header, {position, count})

          _index, {_term, {:ferricstore_segment_projection_header, position, count}}, acc ->
            Map.put(acc, :header, {position, count})

          _index, _entry, acc ->
            acc
        end

        case :ferricstore_waraft_spike_segment_log.fold_disk(
               to_charlist(projection_root),
               fold_fun,
               %{}
             ) do
          {:ok, %{header: header}} -> {:ok, header}
          {:ok, %{}} -> :not_found
          {:error, _reason} = error -> error
        end
      end

      defp waraft_segment_log_dir(root, shard_index) do
        Path.join([
          root,
          "waraft",
          "ferricstore_waraft_backend.#{shard_index + 1}",
          "segment_log"
        ])
      end

      defp append_raw_waraft_segment_record!(root, shard_index, {index, _entry} = record)
           when is_integer(index) and index > 0 do
        # Test-only helper for disk states that can happen after a leader election:
        # Raft may have a no-op tail entry that is not application storage work.
        path = Path.join(waraft_segment_log_dir(root, shard_index), "0.seg")
        File.write!(path, encode_segment_record(record), [:append])
      end

      defp encode_segment_record(record) do
        payload = :erlang.term_to_binary(record)
        <<byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
      end

      defp waraft_storage_root(root, shard_index) do
        Path.join([
          root,
          "waraft",
          "ferricstore_waraft_backend.#{shard_index + 1}"
        ])
      end

      defp waraft_apply_projection_root(root, shard_index) do
        Path.join([waraft_storage_root(root, shard_index), "apply_projection_log"])
      end

      defp apply_projection_segment_files(apply_projection_root) do
        apply_projection_root
        |> Path.join("segment_log/*.seg")
        |> Path.wildcard()
      end

      defp apply_projection_segment_bytes(apply_projection_root) do
        apply_projection_root
        |> apply_projection_segment_files()
        |> Enum.reduce(0, fn path, bytes -> bytes + File.stat!(path).size end)
      end

      defp apply_projection_disk_record_counts(apply_projection_root) do
        assert {:ok, counts} =
                 :ferricstore_waraft_spike_segment_log.fold_disk(
                   to_charlist(apply_projection_root),
                   fn index,
                      {0, {:ferricstore_segment_apply_projection_batch, _position, _entries}},
                      acc ->
                     Map.update(acc, index, 1, &(&1 + 1))
                   end,
                   %{}
                 )

        counts
      end

      defp clear_apply_projection_cache! do
        case :ets.whereis(:ferricstore_waraft_apply_projection_cache) do
          :undefined -> :ok
          table -> :ets.delete_all_objects(table)
        end
      rescue
        ArgumentError -> :ok
      end

      defp apply_projection_cache_contains?(root, shard_index, index, key) do
        case :ets.whereis(:ferricstore_waraft_apply_projection_cache) do
          :undefined ->
            false

          table ->
            case :ets.lookup(table, {waraft_storage_root(root, shard_index), index, key}) do
              [] -> false
              [_entry] -> true
            end
        end
      rescue
        ArgumentError -> false
      end

      defp apply_projection_cache_contains_key?(root, shard_index, key) do
        case :ets.whereis(:ferricstore_waraft_apply_projection_cache) do
          :undefined ->
            false

          table ->
            root = waraft_storage_root(root, shard_index)

            :ets.select_count(table, [
              {{{root, :_, key}, :_, :_}, [], [true]}
            ]) > 0
        end
      rescue
        ArgumentError -> false
      end

      defp apply_projection_cache_value_bytes(root, shard_index, index) do
        case :ets.whereis(:ferricstore_waraft_apply_projection_cache) do
          :undefined ->
            0

          table ->
            root = waraft_storage_root(root, shard_index)

            table
            |> :ets.match_object({{root, index, :_}, :_, :_})
            |> Enum.reduce(0, fn
              {{^root, ^index, _key}, value, _expire_at_ms}, acc when is_binary(value) ->
                acc + byte_size(value)

              _entry, acc ->
                acc
            end)
        end
      rescue
        ArgumentError -> 0
      end

      defp apply_projection_cache_rows(root, shard_index) do
        case :ets.whereis(:ferricstore_waraft_apply_projection_cache) do
          :undefined ->
            0

          table ->
            root = waraft_storage_root(root, shard_index)

            :ets.select_count(table, [
              {{{root, :_, :_}, :_, :_}, [], [true]}
            ])
        end
      rescue
        ArgumentError -> 0
      end

      defp read_segment_config(segment_dir) do
        segment_dir
        |> Path.join("segment_config.term")
        |> File.read!()
        |> :erlang.binary_to_term([:safe])
      end

      defp missing_blob_ref(payload) when is_binary(payload) do
        <<segment_id::unsigned-big-64, _rest::binary>> = :crypto.hash(:sha256, payload)
        ref = BlobRef.from_segment(payload, segment_id, 48)
        {BlobRef.encode!(ref), ref}
      end

      defp write_blob_segment!(ctx, shard_index, ref, payload) do
        path = BlobRef.path(ctx.data_dir, shard_index, ref)
        File.mkdir_p!(Path.dirname(path))

        File.write!(
          path,
          <<0, "FSBLOG", 1, ref.size::unsigned-big-64, ref.checksum::binary, payload::binary>>
        )
      end

      defp waraft_segment_log_record(shard_index) do
        partition = shard_index + 1

        {:raft_log, :"raft_log_ferricstore_waraft_backend_#{partition}",
         :ferricstore_waraft_backend, :ferricstore_waraft_backend, partition,
         :ferricstore_waraft_spike_segment_log}
      end

      defp append_waraft_fence!(key, value) do
        assert :ok = WARaftBackend.write(0, {:put, key, value, 0})
        log = waraft_segment_log_record(0)
        index = :ferricstore_waraft_spike_segment_log.last_index(log)
        assert is_integer(index) and index > 0
        {log, index}
      end

      defp insert_apply_projection_ref!(root, ctx, index, key, value) do
        lfu = Ferricstore.Store.LFU.initial()
        value_size = byte_size(value)

        :ets.insert(
          elem(ctx.keydir_refs, 0),
          {key, nil, 0, lfu, {:waraft_apply_projection, index}, 0, value_size}
        )

        assert :ok =
                 Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(root, 0, index, [
                   {key, value, 0}
                 ])

        {lfu, 0, value_size}
      end

      defp key_for_shard(ctx, shard_idx) do
        Stream.iterate(0, &(&1 + 1))
        |> Enum.find_value(fn n ->
          key = "shard:#{shard_idx}:#{n}"
          if Router.shard_for(ctx, key) == shard_idx, do: key
        end)
      end

      defp parse_stream_ms(id) do
        [ms, "0"] = String.split(id, "-", parts: 2)
        String.to_integer(ms)
      end

      defp flow_partition_for_shard(ctx, id, shard_idx) do
        Stream.iterate(0, &(&1 + 1))
        |> Enum.find_value(fn n ->
          partition = "flow-partition:#{shard_idx}:#{n}"
          key = Ferricstore.Flow.Keys.state_key(id, partition)
          if Router.shard_for(ctx, key) == shard_idx, do: partition
        end)
      end

      defp setup_flow_child(
             ctx,
             parent_id,
             child_id,
             parent_partition,
             child_partition,
             opts
           ) do
        group_id = Keyword.fetch!(opts, :group_id)
        on_child_failed = Keyword.get(opts, :on_child_failed, :ignore)
        on_parent_closed = Keyword.get(opts, :on_parent_closed, :abandon_children)

        with :ok <-
               Ferricstore.Flow.create(ctx, parent_id,
                 type: "parent",
                 state: "dispatch",
                 partition_key: parent_partition,
                 now_ms: 1_000
               ),
             {:ok, created_parent} <-
               Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition),
             :ok <-
               Ferricstore.Flow.spawn_children(
                 ctx,
                 parent_id,
                 [%{id: child_id, type: "child", partition_key: child_partition}],
                 group_id: group_id,
                 wait: :all,
                 wait_state: "waiting_children",
                 on_child_failed: on_child_failed,
                 on_parent_closed: on_parent_closed,
                 exhaust_to: %{success: "children_done", failure: "children_failed"},
                 partition_key: parent_partition,
                 from_state: "dispatch",
                 fencing_token: created_parent.fencing_token,
                 now_ms: 1_010
               ) do
          Ferricstore.Flow.get(ctx, parent_id, partition_key: parent_partition)
        end
      end

      defp claim_flow_child!(ctx, id, partition_key, worker) do
        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, "child",
                   partition_key: partition_key,
                   worker: worker,
                   limit: 1,
                   now_ms: 9_000_000_000_000
                 )

        assert claimed.id == id
        claimed
      end

      defp setup_colocated_child_for_many!(ctx, parent_id, child_id, group_id, opts \\ []) do
        parent_partition = flow_partition_for_shard(ctx, parent_id, 0)
        child_partition = parent_partition

        assert {:ok, _waiting_parent} =
                 setup_flow_child(
                   ctx,
                   parent_id,
                   child_id,
                   parent_partition,
                   child_partition,
                   Keyword.merge(opts, group_id: group_id)
                 )

        {parent_partition, child_partition}
      end

      defp rewind_waraft_storage_position!(root, shard_index, position) do
        metadata_path =
          Path.join([
            root,
            "waraft",
            "ferricstore_waraft_backend.#{shard_index + 1}",
            "ferricstore_storage.term"
          ])

        metadata =
          metadata_path
          |> File.read!()
          |> :erlang.binary_to_term([:safe])
          |> Map.put(:position, position)

        File.write!(metadata_path, :erlang.term_to_binary(metadata))
      end

      defp waraft_server_name(shard_index) do
        :wa_raft_server.registered_name(:ferricstore_waraft_backend, shard_index + 1)
      end

      defp kill_waraft_server!(shard_index) do
        server = waraft_server_name(shard_index)

        pid = Process.whereis(server)
        assert is_pid(pid), "expected live WARaft server #{inspect(server)}"

        Process.exit(pid, :kill)
      end

      defp kill_waraft_server!(node, shard_index) do
        server =
          :ferricstore_waraft_backend
          |> :wa_raft_server.registered_name(shard_index + 1)

        pid = :rpc.call(node, Process, :whereis, [server])
        assert is_pid(pid), "expected live WARaft server #{inspect(server)} on #{inspect(node)}"

        :rpc.call(node, Process, :exit, [pid, :kill])
      end

      defp wait_for_kill_load_acks(0, acked), do: acked

      defp wait_for_kill_load_acks(remaining, acked) do
        receive do
          {:waraft_kill_load_result, key, value, :ok} ->
            wait_for_kill_load_acks(remaining - 1, [{key, value} | acked])

          {:waraft_kill_load_result, _key, _value, _error} ->
            wait_for_kill_load_acks(remaining, acked)
        after
          5_000 ->
            flunk("expected #{remaining} more acknowledged writes before killing WARaft server")
        end
      end

      defp drain_kill_load_results(acked) do
        receive do
          {:waraft_kill_load_result, key, value, :ok} ->
            drain_kill_load_results([{key, value} | acked])

          {:waraft_kill_load_result, _key, _value, _error} ->
            drain_kill_load_results(acked)
        after
          0 ->
            acked
        end
      end

      defp wait_for_multi_kill_shard_acks(_shard_index, 0, acked), do: acked

      defp wait_for_multi_kill_shard_acks(shard_index, remaining, acked) do
        receive do
          {:waraft_multi_kill_result, ^shard_index, key, value, :ok} ->
            wait_for_multi_kill_shard_acks(shard_index, remaining - 1, [
              {shard_index, key, value} | acked
            ])

          {:waraft_multi_kill_result, other_shard_index, key, value, :ok} ->
            wait_for_multi_kill_shard_acks(shard_index, remaining, [
              {other_shard_index, key, value} | acked
            ])

          {:waraft_multi_kill_result, _shard_index, _key, _value, _error} ->
            wait_for_multi_kill_shard_acks(shard_index, remaining, acked)
        after
          5_000 ->
            flunk("expected #{remaining} more acknowledged writes on shard #{shard_index}")
        end
      end

      defp drain_multi_kill_results(acked) do
        receive do
          {:waraft_multi_kill_result, shard_index, key, value, :ok} ->
            drain_multi_kill_results([{shard_index, key, value} | acked])

          {:waraft_multi_kill_result, _shard_index, _key, _value, _error} ->
            drain_multi_kill_results(acked)
        after
          0 ->
            Enum.uniq(acked)
        end
      end

      defp wait_for_cluster_kill_acks(0, acked), do: acked

      defp wait_for_cluster_kill_acks(remaining, acked) do
        receive do
          {:waraft_cluster_kill_result, key, value, :ok} ->
            wait_for_cluster_kill_acks(remaining - 1, [{key, value} | acked])

          {:waraft_cluster_kill_result, _key, _value, _error} ->
            wait_for_cluster_kill_acks(remaining, acked)
        after
          10_000 ->
            flunk("expected #{remaining} more acknowledged cluster writes before kill")
        end
      end

      defp drain_cluster_kill_results(acked) do
        receive do
          {:waraft_cluster_kill_result, key, value, :ok} ->
            drain_cluster_kill_results([{key, value} | acked])

          {:waraft_cluster_kill_result, _key, _value, _error} ->
            drain_cluster_kill_results(acked)
        after
          0 ->
            Enum.uniq(acked)
        end
      end

      defp shard_dir_specs(ctx, shard_index) do
        [
          data: Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index),
          blob: Ferricstore.DataDir.blob_shard_path(ctx.data_dir, shard_index),
          dedicated: Path.join([ctx.data_dir, "dedicated", "shard_#{shard_index}"]),
          prob: Path.join([ctx.data_dir, "prob", "shard_#{shard_index}"])
        ]
      end

      defp shard_payload_present?(ctx, shard_index) do
        shard_bitcask_payload_present?(ctx, shard_index) or
          waraft_segment_payload_present?(ctx.data_dir, shard_index)
      end
    end
  end
end
