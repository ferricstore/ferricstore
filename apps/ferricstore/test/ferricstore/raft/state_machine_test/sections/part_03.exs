defmodule Ferricstore.Raft.StateMachineTest.Sections.Part03 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Raft.{BlobCommand, StateMachine}
      alias Ferricstore.Store.BitcaskWriter
      alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

  describe "Flow command time" do
    test "Flow create does not return projection failure after committing state", %{state: state} do
      setup_flow_indexes(state)
      state = %{state | release_cursor_interval: 1}

      old_hook = Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

      Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _path,
                                                                                      _file_id,
                                                                                      _entries ->
        {:error, :forced_history_projection_failure}
      end)

      on_exit(fn -> restore_env(:flow_history_projector_lmdb_publish_hook, old_hook) end)

      id = "flow-projection-after-commit"
      partition_key = "tenant-projection-after-commit"
      state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

      {new_state, {:applied_at, 1, result}, effects} =
        StateMachine.apply(
          %{index: 1, term: 1, system_time: 1_000},
          {:flow_create, state_key,
           %{id: id, type: "projection-flow", state: "queued", partition_key: partition_key}},
          state
        )

      assert result == :ok
      assert %{id: ^id, state: "queued"} = flow_record!(new_state, state_key)
      refute Enum.any?(effects, &match?({:release_cursor, _index}, &1))
    end

    test "uses stamped apply time when Flow attrs omit now_ms", %{state: state} do
      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      on_exit(fn ->
        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
      end)

      id = "flow-command-time"
      type = "command-time"
      partition_key = "tenant-command-time"
      state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create, state_key,
           %{id: id, type: type, state: "queued", partition_key: partition_key}},
          state
        )

      created = flow_record!(state, state_key)
      assert created.created_at_ms == 1_000
      assert created.updated_at_ms == 1_000
      assert created.next_run_at_ms == 1_000

      due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

      {state, {:ok, [claimed]}} =
        StateMachine.apply(
          %{system_time: 1_250},
          {:flow_claim_due, due_key,
           %{
             type: type,
             state: "queued",
             worker: "worker-command-time",
             lease_ms: 500,
             limit: 1,
             priority: nil,
             partition_key: partition_key
           }},
          state
        )

      assert claimed.updated_at_ms == 1_250
      assert claimed.lease_deadline_ms == 1_750

      running_due_key = Ferricstore.Flow.Keys.due_key(type, "running", 0, partition_key)

      running_state_index_key =
        Ferricstore.Flow.Keys.state_index_key(type, "running", partition_key)

      waiting_due_key = Ferricstore.Flow.Keys.due_key(type, "waiting", 0, partition_key)

      waiting_state_index_key =
        Ferricstore.Flow.Keys.state_index_key(type, "waiting", partition_key)

      inflight_index_key = Ferricstore.Flow.Keys.inflight_index_key(type, partition_key)

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 2_000},
          {:flow_transition, state_key,
           %{
             id: id,
             from_state: "running",
             to_state: "waiting",
             lease_token: claimed.lease_token,
             fencing_token: claimed.fencing_token,
             partition_key: partition_key
           }},
          state
        )

      transitioned = flow_record!(state, state_key)
      assert transitioned.updated_at_ms == 2_000
      assert transitioned.next_run_at_ms == 2_000

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, running_due_key, id) ==
               :miss

      assert Ferricstore.Flow.OrderedIndex.score_of(
               state.flow_lookup_name,
               running_state_index_key,
               id
             ) == :miss

      assert Ferricstore.Flow.OrderedIndex.score_of(
               state.flow_lookup_name,
               inflight_index_key,
               id
             ) == :miss

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, waiting_due_key, id) ==
               {:ok, 2_000.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(
               state.flow_lookup_name,
               waiting_state_index_key,
               id
             ) == {:ok, 2_000.0}
    end

    test "create_many stages Flow state writes into one append batch and projects history", %{
      state: state,
      shard_index: shard_index
    } do
      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      on_exit(fn ->
        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
      end)

      handler_id = {:flow_create_many_append_batch, self(), make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :bitcask, :append],
          &__MODULE__.handle_flow_append_telemetry/4,
          {self(), shard_index}
        )

      partition_key = "tenant-batched-append"

      records =
        for id <- ["flow-batch-a", "flow-batch-b", "flow-batch-c"] do
          %{
            id: id,
            type: "append-batch",
            state: "queued",
            partition_key: partition_key,
            now_ms: 1_000
          }
        end

      try do
        {_state, :ok} =
          StateMachine.apply(
            %{system_time: 1_000},
            {:flow_create_many, nil, %{records: records}},
            state
          )

        assert Enum.all?(records, fn %{id: id, partition_key: partition_key} ->
                 state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
                 flow_record!(state, state_key).id == id
               end)

        assert_receive {:flow_bitcask_append, measurements,
                        %{shard_index: ^shard_index, status: :ok}},
                       500

        assert measurements.batch_size == 6
        assert measurements.delete_count == 0
        assert measurements.batch_bytes > 0

        Enum.each(records, fn %{id: id, partition_key: partition_key} ->
          assert_flow_history_event!(state, id, partition_key, "1000-1", "created")
        end)

        refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
      after
        :telemetry.detach(handler_id)
      end
    end

    test "Ra-batched Flow commands share state append batch but keep semantic results", %{
      state: state,
      shard_index: shard_index
    } do
      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      on_exit(fn ->
        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
      end)

      handler_id = {:flow_batch_append_per_command_results, self(), make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :bitcask, :append],
          &__MODULE__.handle_flow_append_telemetry/4,
          {self(), shard_index}
        )

      partition_key = "tenant-ra-batched-flow"

      create_a = %{
        id: "flow-ra-batch-a",
        type: "ra-batch",
        state: "queued",
        partition_key: partition_key,
        now_ms: 1_000
      }

      create_b = %{
        id: "flow-ra-batch-b",
        type: "ra-batch",
        state: "queued",
        partition_key: partition_key,
        now_ms: 1_000
      }

      try do
        {_state, {:ok, [result_a, duplicate_result, result_b]}} =
          StateMachine.apply(
            %{system_time: 1_000},
            {:batch,
             [
               {:flow_create, nil, create_a},
               {:flow_create, nil, create_a},
               {:flow_create, nil, create_b}
             ]},
            state
          )

        assert :ok = result_a
        assert {:error, "ERR flow already exists"} = duplicate_result
        assert :ok = result_b

        assert flow_record!(
                 state,
                 Ferricstore.Flow.Keys.state_key("flow-ra-batch-a", partition_key)
               ).id ==
                 "flow-ra-batch-a"

        assert flow_record!(
                 state,
                 Ferricstore.Flow.Keys.state_key("flow-ra-batch-b", partition_key)
               ).id ==
                 "flow-ra-batch-b"

        assert_receive {:flow_bitcask_append, measurements,
                        %{shard_index: ^shard_index, status: :ok}},
                       500

        assert measurements.batch_size == 4
        assert measurements.delete_count == 0
        assert measurements.batch_bytes > 0

        assert_flow_history_event!(state, "flow-ra-batch-a", partition_key, "1000-1", "created")
        assert_flow_history_event!(state, "flow-ra-batch-b", partition_key, "1000-1", "created")

        refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
      after
        :telemetry.detach(handler_id)
      end
    end

    test "claim_due stages claimed state records into one append batch", %{
      state: state,
      shard_index: shard_index
    } do
      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      on_exit(fn ->
        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
      end)

      partition_key = "tenant-claim-append"
      type = "claim-append"

      records =
        for id <- ["flow-claim-a", "flow-claim-b", "flow-claim-c"] do
          %{
            id: id,
            type: type,
            state: "queued",
            partition_key: partition_key,
            now_ms: 1_000,
            run_at_ms: 1_000,
            history_hot_max_events: 1
          }
        end

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create_many, nil, %{records: records}},
          state
        )

      assert Enum.all?(records, fn %{id: id, partition_key: partition_key} ->
               state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
               flow_record!(state, state_key).id == id
             end)

      handler_id = {:flow_claim_due_append_batch, self(), make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :bitcask, :append],
          &__MODULE__.handle_flow_append_telemetry/4,
          {self(), shard_index}
        )

      due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

      try do
        {_state, {:ok, claimed}} =
          StateMachine.apply(
            %{system_time: 2_000},
            {:flow_claim_due, due_key,
             %{
               type: type,
               state: "queued",
               worker: "worker-claim-append",
               lease_ms: 30_000,
               limit: 3,
               priority: nil,
               partition_key: partition_key
             }},
            state
          )

        assert Enum.map(claimed, & &1.id) == ["flow-claim-a", "flow-claim-b", "flow-claim-c"]

        assert_receive {:flow_bitcask_append, measurements,
                        %{shard_index: ^shard_index, status: :ok}},
                       500

        assert measurements.batch_size == 3
        assert measurements.delete_count == 0
        assert measurements.batch_bytes > 0

        refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
      after
        :telemetry.detach(handler_id)
      end
    end

    test "claim_due apply uses a claim-specific bulk index plan" do
      source = Ferricstore.Test.SourceFiles.state_machine_source()

      [_, body] =
        Regex.run(
          ~r/defp flow_apply_claim_batch\(state, due_key, plans, stale_due_ids, now_ms\) do(.*?)\n  end\n\n  defp flow_claim_move_indexes/s,
          source
        )

      assert body =~ "flow_claim_move_indexes(state, plans)"
      refute body =~ "flow_transition_move_indexes(state, plans)"
    end

    test "claim_due bulk index plan keeps metadata and reclaimed running indexes correct", %{
      state: state
    } do
      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      on_exit(fn ->
        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
      end)

      id = "flow-claim-bulk-index"
      type = "claim-bulk-index"
      partition_key = "tenant-claim-bulk-index"
      parent_id = "parent-claim-bulk-index"
      correlation_id = "corr-claim-bulk-index"
      state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create, state_key,
           %{
             id: id,
             type: type,
             state: "queued",
             partition_key: partition_key,
             parent_flow_id: parent_id,
             correlation_id: correlation_id,
             now_ms: 1_000,
             run_at_ms: 1_000
           }},
          state
        )

      queued_due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

      {state, {:ok, [first_claim]}} =
        StateMachine.apply(
          %{system_time: 2_000},
          {:flow_claim_due, queued_due_key,
           %{
             type: type,
             state: "queued",
             worker: "worker-old",
             lease_ms: 100,
             limit: 1,
             priority: nil,
             partition_key: partition_key
           }},
          state
        )

      running_due_key = Ferricstore.Flow.Keys.due_key(type, "running", 0, partition_key)
      running_state_key = Ferricstore.Flow.Keys.state_index_key(type, "running", partition_key)
      parent_index_key = Ferricstore.Flow.Keys.parent_index_key(parent_id, partition_key)

      correlation_index_key =
        Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key)

      inflight_key = Ferricstore.Flow.Keys.inflight_index_key(type, partition_key)
      old_worker_key = Ferricstore.Flow.Keys.worker_index_key("worker-old", partition_key)

      assert first_claim.lease_deadline_ms == 2_100

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, queued_due_key, id) ==
               :miss

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, running_due_key, id) ==
               {:ok, 2_100.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, running_state_key, id) ==
               {:ok, 2_000.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, parent_index_key, id) ==
               {:ok, 2_000.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(
               state.flow_lookup_name,
               correlation_index_key,
               id
             ) == {:ok, 2_000.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, inflight_key, id) ==
               {:ok, 2_100.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, old_worker_key, id) ==
               {:ok, 2_100.0}

      {_, {:ok, [reclaimed]}} =
        StateMachine.apply(
          %{system_time: 2_200},
          {:flow_claim_due, running_due_key,
           %{
             type: type,
             state: "running",
             worker: "worker-new",
             lease_ms: 300,
             limit: 1,
             priority: nil,
             partition_key: partition_key
           }},
          state
        )

      new_worker_key = Ferricstore.Flow.Keys.worker_index_key("worker-new", partition_key)

      assert reclaimed.lease_deadline_ms == 2_500
      assert reclaimed.lease_owner == "worker-new"

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, running_due_key, id) ==
               {:ok, 2_500.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, running_state_key, id) ==
               {:ok, 2_200.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, parent_index_key, id) ==
               {:ok, 2_200.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(
               state.flow_lookup_name,
               correlation_index_key,
               id
             ) == {:ok, 2_200.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, inflight_key, id) ==
               {:ok, 2_500.0}

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, old_worker_key, id) ==
               :miss

      assert Ferricstore.Flow.OrderedIndex.score_of(state.flow_lookup_name, new_worker_key, id) ==
               {:ok, 2_500.0}
    end

    test "claim_due mirror does not enqueue full active state blobs", %{
      state: state,
      shard_index: shard_index
    } do
      old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
      old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
      old_max_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

      Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
      Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
      Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 10_000)
      state = %{state | flow_lmdb_mirror?: true}

      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      {:ok, writer_pid} =
        Ferricstore.Flow.LMDBWriter.start_link(
          instance_name: state.instance_name,
          shard_index: shard_index,
          data_dir: state.data_dir
        )

      on_exit(fn ->
        try do
          if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end

        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
        restore_env(:flow_lmdb_mode, old_mode)
        restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
        restore_env(:flow_lmdb_max_batch_ops, old_max_ops)
      end)

      partition_key = "tenant-claim-lmdb-enqueue"
      type = "claim-lmdb-enqueue"

      records =
        for idx <- 1..10 do
          %{
            id: "flow-lmdb-claim-#{idx}",
            type: type,
            state: "queued",
            partition_key: partition_key,
            now_ms: 1_000,
            run_at_ms: 1_000
          }
        end

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create_many, nil, %{records: records}},
          state
        )

      assert Enum.all?(records, fn %{id: id, partition_key: partition_key} ->
               state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
               flow_record!(state, state_key).id == id
             end)

      assert :ok = Ferricstore.Flow.LMDBWriter.flush(state.instance_name, shard_index)

      handler_id = {:flow_claim_due_lmdb_enqueue, self(), make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :lmdb_writer, :backlog],
          fn _event, measurements, metadata, test_pid ->
            send(test_pid, {:flow_lmdb_backlog, measurements, metadata})
          end,
          self()
        )

      due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

      try do
        {_state, {:ok, claimed}} =
          StateMachine.apply(
            %{system_time: 2_000},
            {:flow_claim_due, due_key,
             %{
               type: type,
               state: "queued",
               worker: "worker-claim-lmdb",
               lease_ms: 30_000,
               limit: 10,
               priority: nil,
               partition_key: partition_key
             }},
            state
          )

        assert length(claimed) == 10

        refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100
      after
        :telemetry.detach(handler_id)
      end
    end

    test "active Flow writes do not enqueue cold LMDB projection", %{
      state: state,
      shard_index: shard_index
    } do
      old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
      old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
      old_max_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

      Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
      Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
      Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 10_000)
      state = %{state | flow_lmdb_mirror?: true}

      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      {:ok, writer_pid} =
        Ferricstore.Flow.LMDBWriter.start_link(
          instance_name: state.instance_name,
          shard_index: shard_index,
          data_dir: state.data_dir
        )

      handler_id = {:active_flow_lmdb_projection, self(), make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :flow, :lmdb_writer, :backlog],
          fn _event, measurements, metadata, test_pid ->
            send(test_pid, {:flow_lmdb_backlog, measurements, metadata})
          end,
          self()
        )

      on_exit(fn ->
        :telemetry.detach(handler_id)

        try do
          if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end

        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
        restore_env(:flow_lmdb_mode, old_mode)
        restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
        restore_env(:flow_lmdb_max_batch_ops, old_max_ops)
      end)

      partition_key = "tenant-active-lmdb-projection"
      type = "active-lmdb-projection"
      id = "flow-active-lmdb-projection"
      state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create, state_key,
           %{
             id: id,
             type: type,
             state: "queued",
             partition_key: partition_key,
             parent_flow_id: "parent-active-lmdb-projection",
             correlation_id: "correlation-active-lmdb-projection",
             now_ms: 1_000,
             run_at_ms: 1_000
           }},
          state
        )

      refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100

      due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

      {state, {:ok, [claimed]}} =
        StateMachine.apply(
          %{system_time: 2_000},
          {:flow_claim_due, due_key,
           %{
             type: type,
             state: "queued",
             worker: "worker-active-lmdb-projection",
             lease_ms: 30_000,
             limit: 1,
             priority: nil,
             partition_key: partition_key
           }},
          state
        )

      refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 3_000},
          {:flow_complete, state_key,
           %{
             id: claimed.id,
             lease_token: claimed.lease_token,
             fencing_token: claimed.fencing_token,
             partition_key: partition_key,
             now_ms: 3_000
           }},
          state
        )

      completed = flow_record!(state, state_key)
      assert completed.state == "completed"
      refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100
    end

    test "Flow hot path does not depend on LMDB writer availability", %{
      state: state,
      shard_index: shard_index
    } do
      old_mode = Application.get_env(:ferricstore, :flow_lmdb_mode)
      old_flush_interval = Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)
      old_max_ops = Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)

      Application.put_env(:ferricstore, :flow_lmdb_mode, :mirror)
      Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, 60_000)
      Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 10_000)
      state = %{state | flow_lmdb_mirror?: true}

      :ets.new(state.zset_score_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.zset_score_lookup_name, [:set, :public, :named_table])
      :ets.new(state.flow_index_name, [:ordered_set, :public, :named_table])
      :ets.new(state.flow_lookup_name, [:set, :public, :named_table])

      writer_name = Ferricstore.Flow.LMDBWriter.name(state.instance_name, shard_index)
      assert Process.whereis(writer_name) == nil

      backlog_handler_id = {:flow_hot_path_lmdb_backlog, self(), make_ref()}
      degraded_handler_id = {:flow_hot_path_lmdb_degraded, self(), make_ref()}

      :ok =
        :telemetry.attach(
          backlog_handler_id,
          [:ferricstore, :flow, :lmdb_writer, :backlog],
          fn _event, measurements, metadata, test_pid ->
            send(test_pid, {:flow_lmdb_backlog, measurements, metadata})
          end,
          self()
        )

      :ok =
        :telemetry.attach(
          degraded_handler_id,
          [:ferricstore, :flow, :lmdb_mirror, :degraded],
          fn _event, measurements, metadata, test_pid ->
            send(test_pid, {:flow_lmdb_degraded, measurements, metadata})
          end,
          self()
        )

      on_exit(fn ->
        :telemetry.detach(backlog_handler_id)
        :telemetry.detach(degraded_handler_id)
        safe_delete_ets(state.zset_score_index_name)
        safe_delete_ets(state.zset_score_lookup_name)
        safe_delete_ets(state.flow_index_name)
        safe_delete_ets(state.flow_lookup_name)
        restore_env(:flow_lmdb_mode, old_mode)
        restore_env(:flow_lmdb_flush_interval_ms, old_flush_interval)
        restore_env(:flow_lmdb_max_batch_ops, old_max_ops)
      end)

      partition_key = "tenant-flow-hot-path-lmdb"
      type = "flow-hot-path-lmdb"
      id = "flow-hot-path-lmdb"
      state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create, state_key,
           %{
             id: id,
             type: type,
             state: "queued",
             partition_key: partition_key,
             parent_flow_id: "parent-flow-hot-path-lmdb",
             correlation_id: "correlation-flow-hot-path-lmdb",
             now_ms: 1_000,
             run_at_ms: 1_000
           }},
          state
        )

      assert flow_record!(state, state_key).state == "queued"

      due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)

      {state, {:ok, [claimed]}} =
        StateMachine.apply(
          %{system_time: 2_000},
          {:flow_claim_due, due_key,
           %{
             type: type,
             state: "queued",
             worker: "worker-flow-hot-path-lmdb",
             lease_ms: 30_000,
             limit: 1,
             priority: nil,
             partition_key: partition_key
           }},
          state
        )

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 3_000},
          {:flow_transition, state_key,
           %{
             id: id,
             from_state: "running",
             to_state: "waiting",
             lease_token: claimed.lease_token,
             fencing_token: claimed.fencing_token,
             partition_key: partition_key,
             now_ms: 3_000
           }},
          state
        )

      waiting = flow_record!(state, state_key)
      assert waiting.state == "waiting"
      refute_receive {:flow_lmdb_backlog, _measurements, _metadata}, 100
      refute_receive {:flow_lmdb_degraded, _measurements, _metadata}, 100

      waiting_due_key = Ferricstore.Flow.Keys.due_key(type, "waiting", 0, partition_key)

      {state, {:ok, [claimed_again]}} =
        StateMachine.apply(
          %{system_time: 4_000},
          {:flow_claim_due, waiting_due_key,
           %{
             type: type,
             state: "waiting",
             worker: "worker-flow-hot-path-lmdb",
             lease_ms: 30_000,
             limit: 1,
             priority: nil,
             partition_key: partition_key
           }},
          state
        )

      {state, :ok} =
        StateMachine.apply(
          %{system_time: 5_000},
          {:flow_complete, state_key,
           %{
             id: id,
             lease_token: claimed_again.lease_token,
             fencing_token: claimed_again.fencing_token,
             partition_key: partition_key,
             now_ms: 5_000
           }},
          state
        )

      completed = flow_record!(state, state_key)
      assert completed.state == "completed"

      assert_receive {:flow_lmdb_degraded, %{count: 1},
                      %{shard_index: ^shard_index, reason: :writer_not_started}},
                     500
    end
  end
    end
  end
end
