defmodule Ferricstore.Flow.LMDBTest.Sections.StartupKeepsTerminalHistoryColdWhileDurableHistoryRemainsQuerya do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.LMDBTest.FlushProbeWriter

      test "startup keeps terminal history cold while durable history remains queryable" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        ctx =
          Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

        on_exit(fn ->
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
          restore_env(:flow_lmdb_mode, old_mode)
        end)

        id = "history-restart"
        partition_key = "tenant-history"

        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: "history-restart",
                   run_at_ms: 1,
                   partition_key: partition_key,
                   history_max_events: 5,
                   now_ms: 1
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, "history-restart",
                   worker: "worker-history",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 1,
                   partition_key: partition_key
                 )

        assert {:ok, _record} =
                 Ferricstore.Flow.extend_lease(ctx, id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   lease_ms: 30_000,
                   partition_key: partition_key,
                   now_ms: 2
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   partition_key: partition_key,
                   now_ms: 3
                 )

        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

        assert {:ok, before_restart} =
                 Ferricstore.Flow.history(ctx, id,
                   partition_key: partition_key,
                   include_cold: false
                 )

        assert before_restart == []

        restart_isolated_shard!(ctx, 0)

        assert {:ok, after_restart} =
                 Ferricstore.Flow.history(ctx, id,
                   partition_key: partition_key,
                   include_cold: false
                 )

        assert after_restart == []

        history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
        {_flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, 0)

        assert Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key) == 0

        assert {:ok, cold_history} =
                 Ferricstore.Flow.history(ctx, id, partition_key: partition_key, count: 10)

        assert Enum.map(cold_history, fn {_event_id, fields} -> fields["event"] end) == [
                 "created",
                 "claimed",
                 "lease_extended",
                 "completed"
               ]
      end

      test "startup rebuild recovers terminal LMDB mirror when writer dies before flush" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
        old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
        old_max_batch_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)
        old_trap = Process.flag(:trap_exit, true)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
        Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
        Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 1_000_000)

        ctx =
          Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

        on_exit(fn ->
          Process.flag(:trap_exit, old_trap)
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
          restore_env(:flow_lmdb_mode, old_mode)
          restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
          restore_env(:flow_lmdb_max_batch_ops, old_max_batch_ops)
        end)

        id = "flow-rebuild-terminal"
        partition_key = "tenant-rebuild-terminal"
        flow_type = "rebuild-terminal"

        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: flow_type,
                   run_at_ms: 1,
                   partition_key: partition_key,
                   root_flow_id: "root-rebuild-terminal",
                   correlation_id: "corr-rebuild-terminal",
                   history_hot_max_events: 1,
                   now_ms: 1
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, flow_type,
                   worker: "worker-rebuild-terminal",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 2,
                   partition_key: partition_key
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   partition_key: partition_key,
                   ttl_ms: 60_000,
                   now_ms: 3
                 )

        assert {:ok, completed} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)

        writer_name = Ferricstore.Flow.LMDBWriter.name(ctx.name, 0)
        writer_pid = Process.whereis(writer_name)
        assert is_pid(writer_pid)
        ref = Process.monitor(writer_pid)
        Process.exit(writer_pid, :kill)
        assert_receive {:DOWN, ^ref, :process, ^writer_pid, :killed}

        lmdb_path =
          ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

        completed_index_key =
          Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

        failed_index_key =
          Ferricstore.Flow.Keys.state_index_key(flow_type, "failed", partition_key)

        assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
        assert :ok = Ferricstore.Flow.LMDB.put_terminal_count(lmdb_path, completed_index_key, 7)
        assert :ok = Ferricstore.Flow.LMDB.put_terminal_count(lmdb_path, failed_index_key, 5)

        restart_isolated_shard!(ctx, 0)

        terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)

        root_prefix =
          Ferricstore.Flow.LMDB.query_index_prefix(
            Ferricstore.Flow.Keys.root_index_key("root-rebuild-terminal", partition_key)
          )

        correlation_prefix =
          Ferricstore.Flow.LMDB.query_index_prefix(
            Ferricstore.Flow.Keys.correlation_index_key("corr-rebuild-terminal", partition_key)
          )

        assert [{^state_key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}] =
                 :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

        assert {:ok, %{id: ^id, state: "completed"}} =
                 Ferricstore.Flow.get(ctx, id, partition_key: partition_key)

        assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, completed_index_key)
        assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, failed_index_key)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, root_prefix)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, correlation_prefix)

        assert {:ok, cold_history} =
                 Ferricstore.Flow.history(ctx, id,
                   partition_key: partition_key,
                   include_cold: true,
                   count: 10
                 )

        assert Enum.map(cold_history, fn {_event_id, fields} -> fields["event"] end) == [
                 "created",
                 "claimed",
                 "completed"
               ]

        assert {:ok, info} =
                 Ferricstore.Flow.info(ctx, flow_type,
                   partition_key: partition_key,
                   include_cold: true
                 )

        assert info.completed == 1
        assert completed.version == 3
      end

      test "mirror flow TTL removes expired LMDB state and terminal index on read" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        ctx =
          Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

        on_exit(fn ->
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
          restore_env(:flow_lmdb_mode, old_mode)
        end)

        partition_key = "tenant-ttl"
        flow_type = "ttl"

        assert :ok =
                 Ferricstore.Store.Router.flow_create(ctx, %{
                   id: "flow-ttl",
                   type: flow_type,
                   state: "queued",
                   run_at_ms: 1,
                   partition_key: partition_key,
                   now_ms: 1
                 })

        assert {:ok, [claimed]} =
                 Ferricstore.Store.Router.flow_claim_due(ctx, %{
                   type: flow_type,
                   state: "queued",
                   priority: nil,
                   worker: "worker-ttl",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 2,
                   partition_key: partition_key
                 })

        assert :ok =
                 Ferricstore.Store.Router.flow_complete(ctx, %{
                   id: claimed.id,
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   ttl_ms: 20,
                   now_ms: 3,
                   partition_key: partition_key
                 })

        assert {:ok, completed} =
                 Ferricstore.Flow.get(ctx, claimed.id, partition_key: partition_key)

        assert completed.state == "completed"
        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

        lmdb_path =
          ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

        state_key = Ferricstore.Flow.Keys.state_key(completed.id, partition_key)
        reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

        completed_index_key =
          Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

        terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)

        assert {:ok, _blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)

        Process.sleep(40)

        assert {:ok, nil} = Ferricstore.Flow.get(ctx, completed.id, partition_key: partition_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
        assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, reverse_key)
        assert {:ok, 0} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
      end

      test "mirror flow TTL sweep removes expired terminal index without flow get" do
        old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)

        Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)

        ctx =
          Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1)

        on_exit(fn ->
          Ferricstore.Test.IsolatedInstance.checkin(ctx)
          restore_env(:flow_lmdb_mode, old_mode)
        end)

        partition_key = "tenant-ttl-sweep"
        flow_type = "ttl-sweep"

        assert :ok =
                 Ferricstore.Store.Router.flow_create(ctx, %{
                   id: "flow-ttl-sweep",
                   type: flow_type,
                   state: "queued",
                   run_at_ms: 1,
                   partition_key: partition_key,
                   now_ms: 1
                 })

        assert {:ok, [claimed]} =
                 Ferricstore.Store.Router.flow_claim_due(ctx, %{
                   type: flow_type,
                   state: "queued",
                   priority: nil,
                   worker: "worker-ttl-sweep",
                   lease_ms: 30_000,
                   limit: 1,
                   now_ms: 2,
                   partition_key: partition_key
                 })

        assert :ok =
                 Ferricstore.Store.Router.flow_complete(ctx, %{
                   id: claimed.id,
                   lease_token: claimed.lease_token,
                   fencing_token: claimed.fencing_token,
                   ttl_ms: 20,
                   now_ms: 3,
                   partition_key: partition_key
                 })

        assert {:ok, completed} =
                 Ferricstore.Flow.get(ctx, claimed.id, partition_key: partition_key)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

        lmdb_path =
          ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> Ferricstore.Flow.LMDB.path()

        completed_index_key =
          Ferricstore.Flow.Keys.state_index_key(flow_type, "completed", partition_key)

        terminal_prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(completed_index_key)
        state_key = Ferricstore.Flow.Keys.state_key(completed.id, partition_key)
        reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
        history_key = Ferricstore.Flow.Keys.history_key(completed.id, partition_key)
        history_prefix = Ferricstore.Flow.LMDB.history_index_prefix(history_key)

        assert {:ok, 1} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
        assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, completed_index_key)

        assert {:ok, history_count_before} =
                 Ferricstore.Flow.LMDB.prefix_count(lmdb_path, history_prefix)

        assert history_count_before >= 3

        Process.sleep(40)

        assert {:ok, 1} =
                 Ferricstore.Flow.LMDB.sweep_expired_terminal(
                   lmdb_path,
                   System.os_time(:millisecond),
                   100
                 )

        assert {:ok, 0} = Ferricstore.Flow.LMDB.prefix_count(lmdb_path, terminal_prefix)
        assert {:ok, 0} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, completed_index_key)

        assert {:ok, ^history_count_before} =
                 Ferricstore.Flow.LMDB.prefix_count(lmdb_path, history_prefix)

        assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, reverse_key)

        assert {:ok, _state_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)

        assert {:ok, [^state_key]} =
                 Ferricstore.Flow.LMDB.expired_terminal_state_keys(
                   lmdb_path,
                   System.os_time(:millisecond),
                   100
                 )
      end
    end
  end
end
