defmodule Ferricstore.Flow.LMDBTest.Sections.DefaultStartupRepairsActiveProjectionClearsStaleLmdbFlushMarker do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

      test "default startup repairs active projection and clears stale LMDB flush marker" do
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)

        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_default_startup_partial_#{System.unique_integer([:positive])}"
          )

        shard_index = 88
        keydir = :ets.new(:flow_lmdb_default_startup_partial_keydir, [:set])

        on_exit(fn ->
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        File.mkdir_p!(shard_path)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        queued =
          active_lmdb_record("flow-default-partial", "default-partial", "queued",
            partition_key: "tenant-default-partial",
            updated_at_ms: 2,
            next_run_at_ms: 10
          )

        completed =
          queued
          |> Map.merge(%{
            state: "completed",
            version: 2,
            updated_at_ms: 20,
            next_run_at_ms: nil
          })

        state_key = Ferricstore.Flow.Keys.state_key(completed.id, completed.partition_key)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   lmdb_path,
                   [
                     Ferricstore.Flow.LMDB.flush_in_progress_put_op()
                     | elem(
                         Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(
                           state_key,
                           queued,
                           0
                         ),
                         0
                       )
                   ]
                 )

        encoded_completed = Ferricstore.Flow.encode_record(completed)

        :ets.insert(
          keydir,
          {state_key, encoded_completed, 0, 0, :hot, 0, byte_size(encoded_completed)}
        )

        assert Ferricstore.Flow.LMDB.flush_in_progress?(lmdb_path)

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(:default, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{name: :default},
                   nil,
                   nil,
                   flow_index,
                   flow_lookup
                 )

        refute Ferricstore.Flow.LMDB.flush_in_progress?(lmdb_path)

        queued_state_key =
          Ferricstore.Flow.Keys.state_index_key(queued.type, queued.state, queued.partition_key)

        completed_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            completed.type,
            completed.state,
            completed.partition_key
          )

        assert [] =
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   queued_state_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_state_key)
                 )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_index_prefix(completed_state_key)
                 )
      end

      test "default WARaft segment replay reconcile repairs stale existing LMDB projection" do
        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_default_segment_replay_reconcile_#{System.unique_integer([:positive])}"
          )

        shard_index = 89
        keydir = :ets.new(:flow_lmdb_default_segment_replay_reconcile_keydir, [:set])

        on_exit(fn ->
          if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
          File.rm_rf!(data_dir)
        end)

        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
        File.mkdir_p!(shard_path)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        queued =
          active_lmdb_record("flow-default-segment-replay", "default-segment-replay", "queued",
            partition_key: "tenant-default-segment-replay",
            updated_at_ms: 2,
            next_run_at_ms: 10
          )

        completed =
          queued
          |> Map.merge(%{
            state: "completed",
            version: 2,
            updated_at_ms: 20,
            next_run_at_ms: nil
          })

        state_key = Ferricstore.Flow.Keys.state_key(completed.id, completed.partition_key)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(
                   lmdb_path,
                   elem(
                     Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(
                       state_key,
                       queued,
                       0
                     ),
                     0
                   )
                 )

        assert Ferricstore.Flow.LMDB.env_present?(lmdb_path)

        encoded_completed = Ferricstore.Flow.encode_record(completed)

        :ets.insert(
          keydir,
          {state_key, encoded_completed, 0, 0, :hot, 0, byte_size(encoded_completed)}
        )

        {flow_index, flow_lookup} =
          Ferricstore.Flow.OrderedIndex.table_names(:default, shard_index)

        Ferricstore.Flow.NativeOrderedIndex.reset(flow_index, flow_lookup)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
                   shard_path,
                   keydir,
                   shard_index,
                   %{name: :default},
                   nil,
                   nil,
                   flow_index,
                   flow_lookup,
                   force_full_reconcile?: true,
                   reason: :segment_replay
                 )

        assert {:ok, lmdb_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
        assert {:ok, ^encoded_completed} = Ferricstore.Flow.LMDB.decode_value(lmdb_blob, 30)

        queued_state_key =
          Ferricstore.Flow.Keys.state_index_key(queued.type, queued.state, queued.partition_key)

        completed_state_key =
          Ferricstore.Flow.Keys.state_index_key(
            completed.type,
            completed.state,
            completed.partition_key
          )

        assert [] =
                 Ferricstore.Flow.OrderedIndex.range_slice(
                   flow_index,
                   queued_state_key,
                   :neg_inf,
                   :inf,
                   false,
                   0,
                   :all
                 )

        assert {:ok, 0} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.active_index_prefix(queued_state_key)
                 )

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.prefix_count(
                   lmdb_path,
                   Ferricstore.Flow.LMDB.terminal_index_prefix(completed_state_key)
                 )
      end

      test "stores, reads, overwrites, and deletes raw flow state blobs" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        key = "flow:{flow:test}:state:a"

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, key, "v1"}])
        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, key, "v2"},
                   {:put, key <> ":other", "v3"}
                 ])

        assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)
        assert {:ok, "v3"} = Ferricstore.Flow.LMDB.get(path, key <> ":other")

        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:delete, key}])
        assert :not_found = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "get_many returns ordered values and misses" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, "a", "1"},
                   {:put, "c", "3"}
                 ])

        assert {:ok, [{:ok, "1"}, :not_found, {:ok, "3"}]} =
                 Ferricstore.Flow.LMDB.get_many(path, ["a", "b", "c"])
      end

      test "query index values require the current state-key encoding" do
        state_key = Ferricstore.Flow.Keys.state_key("flow-query-value", "tenant-query-value")

        value =
          Ferricstore.Flow.LMDB.encode_query_index_value(
            "query-index",
            "flow-query-value",
            42,
            1_000,
            state_key
          )

        assert {:ok,
                {_family_digest, _index_digest, nil, "flow-query-value", 42, 1_000,
                 ^state_key}} =
                 Ferricstore.Flow.LMDB.decode_query_index_value(value)

        obsolete_expiring = :erlang.term_to_binary({"flow-query-value", 43, 2_000})
        obsolete_permanent = :erlang.term_to_binary({"flow-query-value", 44})

        assert :error = Ferricstore.Flow.LMDB.decode_query_index_value(obsolete_expiring)
        assert :error = Ferricstore.Flow.LMDB.decode_query_index_value(obsolete_permanent)
      end

      test "terminal and history indexes reject shortened obsolete encodings" do
        terminal = :erlang.term_to_binary({"flow-terminal-value", 42, 1_000})
        history = :erlang.term_to_binary({"event", 42, "compound-key"})
        history_expiry = :erlang.term_to_binary("history-key")

        assert :error = Ferricstore.Flow.LMDB.decode_terminal_index_value(terminal)
        assert :error = Ferricstore.Flow.LMDB.decode_history_index_value(history)
        assert :error = Ferricstore.Flow.LMDB.decode_history_index_location(history)
        assert :error = Ferricstore.Flow.LMDB.decode_history_flow_expire_value(history_expiry)
      end

      test "terminal_counts batches exact reads without process-local caching" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        completed_key = "flow:{flow:test}:idx:completed"
        failed_key = "flow:{flow:test}:idx:failed"

        on_exit(fn -> File.rm_rf!(path) end)

        assert {:ok, [0, 0]} =
                 Ferricstore.Flow.LMDB.terminal_counts(path, [completed_key, failed_key])

        assert :ets.whereis(:ferricstore_flow_lmdb_terminal_count_cache) == :undefined

        assert :ok = Ferricstore.Flow.LMDB.put_terminal_count(path, completed_key, 7)

        assert {:ok, [7, 0]} =
                 Ferricstore.Flow.LMDB.terminal_counts(path, [completed_key, failed_key])

        assert :ets.whereis(:ferricstore_flow_lmdb_terminal_count_cache) == :undefined
      end

      test "terminal index keys preserve numeric timestamp order in bounded prefix reads" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        state_index_key = "flow:{flow:test}:idx:completed"
        prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(state_index_key)
        older_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "older", 999)
        newer_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "newer", 1_000)
        count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

        older_value =
          Ferricstore.Flow.LMDB.encode_terminal_index_value("older", 999, 0, nil, count_key)

        newer_value =
          Ferricstore.Flow.LMDB.encode_terminal_index_value("newer", 1_000, 0, nil, count_key)

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, newer_key, newer_value},
                   {:put, older_key, older_value}
                 ])

        assert {:ok, [{^older_key, ^older_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1)

        assert {:ok, [{^newer_key, ^newer_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1, true)
      end

      test "history index keys preserve numeric timestamp order in bounded prefix reads" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        history_key = Ferricstore.Flow.Keys.history_key("history-order", "tenant-history-order")
        prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
        older_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "999-1", 999)
        newer_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1000-2", 1_000)
        older_value = Ferricstore.Flow.LMDB.encode_history_index_value("999-1", 999, "X:older")
        newer_value = Ferricstore.Flow.LMDB.encode_history_index_value("1000-2", 1_000, "X:newer")

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, newer_key, newer_value},
                   {:put, older_key, older_value}
                 ])

        assert {:ok, [{^older_key, ^older_value}]} =
                 Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1)
      end

      test "sortable flow LMDB keys use fixed-width decimal timestamp bytes" do
        history_key = Ferricstore.Flow.Keys.history_key("history-pad", "tenant-history-pad")
        history_prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
        history_index_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "event-1", 42)

        terminal_key = "flow:{terminal-pad}:idx:done"
        terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(terminal_key)
        terminal_index_key = Ferricstore.Flow.LMDB.terminal_index_key(terminal_key, "id-1", 42)

        assert history_index_key == history_prefix <> "00000000000000000042" <> <<0>> <> "event-1"
        assert terminal_index_key == terminal_prefix <> "00000000000000000042" <> <<0>> <> "id-1"
      end

      test "terminal expire sweep deletes orphan metadata markers without a state key" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        metadata_index_key = Ferricstore.Flow.Keys.root_index_key("root-orphan", "tenant-orphan")

        terminal_key =
          Ferricstore.Flow.LMDB.terminal_index_key(metadata_index_key, "flow-orphan", 10)

        count_key = Ferricstore.Flow.LMDB.terminal_count_key(metadata_index_key)
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(10, terminal_key)

        expire_value =
          Ferricstore.Flow.LMDB.encode_terminal_expire_value(terminal_key, nil, count_key)

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, expire_key, expire_value}])

        assert {:ok, 0} = Ferricstore.Flow.LMDB.sweep_expired_terminal(path, 11, 100)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
      end

      test "terminal expire sweep deletes expired live terminal marker" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_live_terminal_expire_#{System.unique_integer([:positive])}"
          )

        state_index_key =
          Ferricstore.Flow.Keys.state_index_key("kind", "completed", "tenant-live")

        state_key = Ferricstore.Flow.Keys.state_key("flow-live-expire", "tenant-live")

        terminal_key =
          Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, "flow-live-expire", 10)

        reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
        count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(10, terminal_key)

        terminal_value =
          Ferricstore.Flow.LMDB.encode_terminal_index_value(
            "flow-live-expire",
            10,
            10,
            state_key,
            count_key
          )

        expire_value =
          Ferricstore.Flow.LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, terminal_key, terminal_value},
                   {:put, reverse_key, terminal_key},
                   {:put, expire_key, expire_value},
                   {:put, count_key, Ferricstore.Flow.LMDB.encode_count(1)}
                 ])

        assert {:ok, 1} = Ferricstore.Flow.LMDB.sweep_expired_terminal(path, 11, 100)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, terminal_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, reverse_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
        assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
      end

      test "history expire sweep removes expired history index entries" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        history_key = Ferricstore.Flow.Keys.history_key("history-expire", "tenant-history-expire")
        history_index_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1000-1", 1_000)
        expire_key = Ferricstore.Flow.LMDB.history_expire_key(10, history_index_key)

        history_value =
          Ferricstore.Flow.LMDB.encode_history_index_value("1000-1", 1_000, "X:history", 10)

        expire_value = Ferricstore.Flow.LMDB.encode_history_expire_value(history_index_key)

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, history_index_key, history_value},
                   {:put, expire_key, expire_value}
                 ])

        assert {:ok, 1} = Ferricstore.Flow.LMDB.sweep_expired_history(path, 11, 100)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, history_index_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, expire_key)
      end

      test "history flow expire sweep removes all projection entries for the flow" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        history_key =
          Ferricstore.Flow.Keys.history_key("history-flow-expire", "tenant-history-flow-expire")

        prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)
        older_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1000-1", 1_000)
        newer_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "1001-2", 1_001)
        reused_key = Ferricstore.Flow.LMDB.history_index_key(history_key, "3000-1", 3_000)
        newer_expire_key = Ferricstore.Flow.LMDB.history_expire_key(2_000, newer_key)
        flow_expire_key = Ferricstore.Flow.LMDB.history_flow_expire_key(2_000, history_key)

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, older_key,
                    Ferricstore.Flow.LMDB.encode_history_index_value("1000-1", 1_000, "X:older")},
                   {:put, newer_key,
                    Ferricstore.Flow.LMDB.encode_history_index_value(
                      "1001-2",
                      1_001,
                      "X:newer",
                      2_000
                    )},
                   {:put, newer_expire_key,
                    Ferricstore.Flow.LMDB.encode_history_expire_value(newer_key)},
                   {:put, reused_key,
                    Ferricstore.Flow.LMDB.encode_history_index_value("3000-1", 3_000, "X:reused")},
                   {:put, flow_expire_key,
                    Ferricstore.Flow.LMDB.encode_history_flow_expire_value(history_key, 2_000)}
                 ])

        assert {:ok, 3} = Ferricstore.Flow.LMDB.prefix_count(path, prefix)
        assert {:ok, 2} = Ferricstore.Flow.LMDB.sweep_expired_history(path, 2_001, 100)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(path, prefix)
        assert :not_found = Ferricstore.Flow.LMDB.get(path, flow_expire_key)
        assert {:ok, _} = Ferricstore.Flow.LMDB.get(path, reused_key)
      end

      test "flow LMDB mode is always lagged" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_enabled = Application.get_env(:ferricstore, :flow_lmdb_enabled)

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_enabled, old_enabled)
        end)

        Application.delete_env(:ferricstore, :flow_lmdb_mode)
        Application.delete_env(:ferricstore, :flow_lmdb_enabled)
        assert Ferricstore.Flow.LMDB.enabled?()
        assert Ferricstore.Flow.LMDB.mode() == :lagged
        assert Ferricstore.Flow.LMDB.projection_enabled?()

        Application.put_env(:ferricstore, :flow_lmdb_enabled, true)
        assert Ferricstore.Flow.LMDB.enabled?()
        assert Ferricstore.Flow.LMDB.mode() == :lagged

        for ignored <- [
              :off,
              :lagged,
              :async,
              :mirror,
              :write_through,
              false,
              true,
              "false",
              "TRUE",
              "0",
              "1",
              "off",
              "lagged",
              "async",
              "batched",
              "mirror",
              "write_through",
              "on",
              nil
            ] do
          Application.put_env(:ferricstore, :flow_lmdb_mode, ignored)
          assert Ferricstore.Flow.LMDB.enabled?()
          assert Ferricstore.Flow.LMDB.mode() == :lagged
          assert Ferricstore.Flow.LMDB.projection_enabled?()
        end
      end

      test "batch write can return pre-batch originals for rollback" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        key = "flow:{flow:test}:state:rollback"

        on_exit(fn -> File.rm_rf!(path) end)

        assert {:ok, [{^key, :missing}]} =
                 Ferricstore.Flow.LMDB.write_batch_with_originals(path, [{:put_new, key, "v1"}])

        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

        assert {:ok, [{^key, {:value, "v1"}}]} =
                 Ferricstore.Flow.LMDB.write_batch_with_originals(path, [
                   {:put, key, "v2"},
                   {:put, key, "v3"}
                 ])

        assert {:ok, "v3"} = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "put_new preserves existing LMDB values" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        key = "flow:{flow:test}:state:existing"

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put, key, "v1"}])
        assert :ok = Ferricstore.Flow.LMDB.write_batch(path, [{:put_new, key, "v2"}])
        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)

        assert {:ok, [{^key, {:value, "v1"}}]} =
                 Ferricstore.Flow.LMDB.write_batch_with_originals(path, [{:put_new, key, "v3"}])

        assert {:ok, "v1"} = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "batch write keeps duplicate key order but may sort unique keys internally" do
        path =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_#{System.unique_integer([:positive])}"
          )

        key = "flow:{flow:test}:state:dup"
        low = "flow:{flow:test}:state:a"
        high = "flow:{flow:test}:state:z"

        on_exit(fn -> File.rm_rf!(path) end)

        assert :ok =
                 Ferricstore.Flow.LMDB.write_batch(path, [
                   {:put, key, "v1"},
                   {:put, key, "v2"}
                 ])

        assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)

        assert {:ok, originals} =
                 Ferricstore.Flow.LMDB.write_batch_with_originals(path, [
                   {:put_new, high, "high"},
                   {:put_new, low, "low"}
                 ])

        assert %{^high => :missing, ^low => :missing} = Map.new(originals)
        assert {:ok, "high"} = Ferricstore.Flow.LMDB.get(path, high)
        assert {:ok, "low"} = Ferricstore.Flow.LMDB.get(path, low)
      end

      test "mirror writer batches idempotent full-record puts off the caller path" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_writer_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:state:mirror"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        path =
          data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"},
                   {:put, key, "v1"},
                   {:put, key, "v2"}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)

        assert :ok =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v2"}
                 ])

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(instance_name, shard_index)
        assert {:ok, "v2"} = Ferricstore.Flow.LMDB.get(path, key)
      end

      test "mirror writer rejects enqueue above mailbox guardrail" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

        old_max_mailbox =
          Application.get_env(:ferricstore, :flow_lmdb_writer_max_mailbox_messages)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_writer_max_mailbox_messages, 0)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_queue_full_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_writer_queue_full_#{System.unique_integer([:positive])}"
        key = "flow:{flow:test}:state:queue-full"
        handler_id = {:flow_lmdb_writer_queue_full, self(), make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :lmdb_writer, :unavailable],
          &__MODULE__.handle_lmdb_writer_unavailable/4,
          self()
        )

        on_exit(fn ->
          :telemetry.detach(handler_id)
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_writer_max_mailbox_messages, old_max_mailbox)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        assert {:error, :queue_full} =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, key, "v1"}
                 ])

        assert_receive {:flow_lmdb_writer_unavailable,
                        [:ferricstore, :flow, :lmdb_writer, :unavailable], %{op_count: 1},
                        %{
                          operation: :enqueue,
                          instance_name: ^instance_name,
                          shard_index: ^shard_index,
                          reason: :queue_full
                        }}
      end

      test "mirror writer rejects a single projection batch above configured op guardrail" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_max_enqueue_ops = Application.get_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops, 1)

        data_dir =
          Path.join(
            System.tmp_dir!(),
            "ferricstore_flow_lmdb_writer_op_guard_#{System.unique_integer([:positive])}"
          )

        shard_index = 0
        instance_name = :"flow_lmdb_writer_op_guard_#{System.unique_integer([:positive])}"

        on_exit(fn ->
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_writer_max_enqueue_ops, old_max_enqueue_ops)
          File.rm_rf!(data_dir)
        end)

        Ferricstore.DataDir.ensure_layout!(data_dir, 1)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter,
           shard_index: shard_index, data_dir: data_dir, instance_name: instance_name}
        )

        assert {:error, :queue_full} =
                 Ferricstore.Flow.LMDBWriter.enqueue(instance_name, shard_index, [
                   {:put, "flow:{flow:test}:state:op-guard-1", "v1"},
                   {:put, "flow:{flow:test}:state:op-guard-2", "v2"}
                 ])
      end

      test "active mirror writes do not initialize terminal counters" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)

        on_exit(fn ->
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
          restore_env(:flow_lmdb_mode, old_mode)
        end)

        type = "writer-zero-counts"
        partition_key = "tenant-zero-counts"

        assert :ok =
                 Ferricstore.Store.Router.flow_create(ctx, %{
                   id: "flow-zero-counts",
                   type: type,
                   state: "queued",
                   run_at_ms: 1,
                   partition_key: partition_key,
                   now_ms: 1
                 })

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

        path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(0)
          |> Ferricstore.Flow.LMDB.path()

        for terminal_state <- ["completed", "failed", "cancelled"] do
          state_index_key =
            Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

          assert :not_found = Ferricstore.Flow.LMDB.terminal_count(path, state_index_key)
        end
      end
    end
  end
end
