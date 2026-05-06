defmodule Ferricstore.Raft.StateMachineTest do
  @moduledoc """
  Unit tests for `Ferricstore.Raft.StateMachine`.

  These tests exercise the state machine callbacks directly without running
  a full ra server. The state machine is deterministic and its callbacks can
  be tested in isolation by constructing state manually.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.{CompoundKey, LFU, Promotion}

  # ---------------------------------------------------------------------------
  # Setup: create a temporary Bitcask store and ETS table for each test.
  # Also starts a BitcaskWriter for shard 0 so that background writes from
  # StateMachine.apply work in isolation tests.
  # ---------------------------------------------------------------------------

  setup do
    shard_index = 9000 + :rand.uniform(999)
    root = Path.join(System.tmp_dir!(), "sm_test_#{:rand.uniform(9_999_999)}")
    dir = Ferricstore.DataDir.shard_data_path(root, shard_index)
    File.mkdir_p!(dir)

    # v2: create a .log file instead of NIF.new
    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    suffix = :rand.uniform(9_999_999)
    keydir_name = :"sm_test_keydir_#{suffix}"
    :ets.new(keydir_name, [:set, :public, :named_table])

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir_name
      })

    # Start a BitcaskWriter for this shard so deferred writes are processed.
    {:ok, writer_pid} = BitcaskWriter.start_link(shard_index: shard_index)

    on_exit(fn ->
      try do
        if Process.alive?(writer_pid), do: GenServer.stop(writer_pid, :normal, 5000)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end

      try do
        :ets.delete(keydir_name)
      rescue
        ArgumentError -> :ok
      end

      File.rm_rf!(dir)
      File.rm_rf!(root)
    end)

    %{
      state: state,
      ets: keydir_name,
      store: nil,
      dir: dir,
      active_file_path: active_file_path,
      shard_index: shard_index,
      writer_pid: writer_pid
    }
  end

  # ---------------------------------------------------------------------------
  # init/1
  # ---------------------------------------------------------------------------

  describe "init/1" do
    test "creates initial state with expected fields", %{state: state, shard_index: shard_index} do
      assert state.shard_index == shard_index
      assert is_binary(state.shard_data_path)
      assert is_binary(state.active_file_path)
      assert is_atom(state.ets)
      assert state.applied_count == 0
    end

    test "derives canonical data_dir root from data/shard_N path" do
      root = Path.join(System.tmp_dir!(), "sm_data_dir_#{System.unique_integer([:positive])}")
      shard_path = Path.join([root, "data", "shard_0"])
      File.mkdir_p!(shard_path)

      ets = :ets.new(:"sm_data_dir_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: Path.join(shard_path, "00000.log"),
            ets: ets
          })

        assert state.data_dir == root
      after
        :ets.delete(ets)
        File.rm_rf!(root)
      end
    end

    test "rejects legacy shard_N path outside canonical data layout" do
      root = Path.join(System.tmp_dir!(), "sm_legacy_dir_#{System.unique_integer([:positive])}")
      shard_path = Path.join(root, "shard_0")
      File.mkdir_p!(shard_path)

      ets = :ets.new(:"sm_legacy_dir_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        assert_raise ArgumentError, ~r/expected canonical shard data path/, fn ->
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: Path.join(shard_path, "00000.log"),
            ets: ets
          })
        end
      after
        :ets.delete(ets)
        File.rm_rf!(root)
      end
    end

    test "server_command apply resolves hook from checkpoint-safe instance_name" do
      name = :"sm_hook_instance_#{System.unique_integer([:positive])}"
      root = Path.join(System.tmp_dir!(), "sm_hook_#{System.unique_integer([:positive])}")
      shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
      File.mkdir_p!(shard_path)

      ets = :ets.new(:"sm_hook_ets_#{System.unique_integer([:positive])}", [:set, :public])

      ctx =
        FerricStore.Instance.build(name,
          data_dir: root,
          shard_count: 1,
          max_memory_bytes: 256 * 1024 * 1024,
          keydir_max_ram: 64 * 1024 * 1024
        )

      FerricStore.Instance.inject_callbacks(name,
        raft_apply_hook: fn {:echo, value} -> {:custom_instance, value} end
      )

      try do
        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: Path.join(shard_path, "00000.log"),
            ets: ets,
            instance_name: name
          })

        assert {_state, {:custom_instance, "ok"}} =
                 StateMachine.apply(%{}, {:server_command, {:echo, "ok"}}, state)
      after
        FerricStore.Instance.cleanup(name)
        safe_delete_ets(ets)
        safe_delete_ets(elem(ctx.keydir_refs, 0))
        safe_delete_ets(ctx.hotness_table)
        safe_delete_ets(ctx.config_table)
        File.rm_rf!(root)
      end
    end

    test "stamped server_command exposes stamped HLC time to hook" do
      name = :"sm_hook_time_instance_#{System.unique_integer([:positive])}"
      root = Path.join(System.tmp_dir!(), "sm_hook_time_#{System.unique_integer([:positive])}")
      shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
      File.mkdir_p!(shard_path)

      ets = :ets.new(:"sm_hook_time_ets_#{System.unique_integer([:positive])}", [:set, :public])
      stamped_now = Ferricstore.HLC.now_ms() - 30_000

      ctx =
        FerricStore.Instance.build(name,
          data_dir: root,
          shard_count: 1,
          max_memory_bytes: 256 * 1024 * 1024,
          keydir_max_ram: 64 * 1024 * 1024
        )

      FerricStore.Instance.inject_callbacks(name,
        raft_apply_hook: fn :now_ms -> Ferricstore.CommandTime.now_ms() end
      )

      try do
        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: Path.join(shard_path, "00000.log"),
            ets: ets,
            instance_name: name
          })

        assert {_state, ^stamped_now} =
                 StateMachine.apply(
                   %{system_time: stamped_now + 60_000},
                   {{:server_command, :now_ms}, %{hlc_ts: {stamped_now, 0}}},
                   state
                 )
      after
        FerricStore.Instance.cleanup(name)
        safe_delete_ets(ets)
        safe_delete_ets(elem(ctx.keydir_refs, 0))
        safe_delete_ets(ctx.hotness_table)
        safe_delete_ets(ctx.config_table)
        File.rm_rf!(root)
      end
    end
  end

  defp safe_delete_ets(table) do
    :ets.delete(table)
  rescue
    ArgumentError -> :ok
  end

  describe "Flow command time" do
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

      {state, {:ok, created}} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create, state_key,
           %{id: id, type: type, state: "queued", partition_key: partition_key}},
          state
        )

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

      {_, {:ok, transitioned}} =
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

      assert transitioned.updated_at_ms == 2_000
      assert transitioned.next_run_at_ms == 2_000
    end

    test "create_many stages all Flow Bitcask writes into one append batch", %{
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
          fn _event, measurements, metadata, test_pid ->
            send(test_pid, {:flow_bitcask_append, measurements, metadata})
          end,
          self()
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
        {_state, {:ok, created}} =
          StateMachine.apply(
            %{system_time: 1_000},
            {:flow_create_many, nil, %{records: records}},
            state
          )

        assert length(created) == 3

        assert_receive {:flow_bitcask_append, measurements,
                        %{shard_index: ^shard_index, status: :ok}},
                       500

        assert measurements.batch_size == 6
        assert measurements.delete_count == 0
        assert measurements.batch_bytes > 0

        refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
      after
        :telemetry.detach(handler_id)
      end
    end

    test "Ra-batched Flow commands share append batch but keep per-command semantic results", %{
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
          fn _event, measurements, metadata, test_pid ->
            send(test_pid, {:flow_bitcask_append, measurements, metadata})
          end,
          self()
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

        assert {:ok, %{id: "flow-ra-batch-a"}} = result_a
        assert {:error, "ERR flow already exists"} = duplicate_result
        assert {:ok, %{id: "flow-ra-batch-b"}} = result_b

        assert_receive {:flow_bitcask_append, measurements,
                        %{shard_index: ^shard_index, status: :ok}},
                       500

        assert measurements.batch_size == 4
        assert measurements.delete_count == 0
        assert measurements.batch_bytes > 0

        refute_receive {:flow_bitcask_append, _measurements, _metadata}, 100
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  describe "Flow index rollback" do
    test "rolls back Flow.Index mutations when apply append fails", %{state: state, dir: dir} do
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

      id = "flow-index-rollback"
      type = "index-rollback"
      partition_key = "tenant-index-rollback"
      due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)
      state_index_key = Ferricstore.Flow.Keys.state_index_key(type, "queued", partition_key)
      history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
      state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

      bad_state = %{state | active_file_path: Path.join(dir, "missing.log")}

      {_state, {:error, :active_file_unavailable}} =
        StateMachine.apply(
          %{system_time: 1_000},
          {:flow_create, state_key,
           %{id: id, type: type, state: "queued", partition_key: partition_key}},
          bad_state
        )

      assert Ferricstore.Flow.Index.score_of(state.flow_lookup_name, due_key, id) == :miss
      assert Ferricstore.Flow.Index.score_of(state.flow_lookup_name, state_index_key, id) == :miss
      assert Ferricstore.Flow.Index.count_all(state.flow_lookup_name, history_key) == 0
      assert Ferricstore.Flow.Index.count_all(state.flow_lookup_name, due_key) == 0
      assert Ferricstore.Flow.Index.count_all(state.flow_lookup_name, state_index_key) == 0
    end
  end

  describe "promoted compound prefix delete" do
    test "waits for promoted compaction latch before cleanup", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      redis_key = "promoted_prefix_delete_latch_#{System.unique_integer([:positive])}"
      prefix = CompoundKey.hash_prefix(redis_key)
      compound_key = CompoundKey.hash_field(redis_key, "field")
      dedicated_path = Promotion.dedicated_path(state.data_dir, shard_index, :hash, redis_key)

      File.mkdir_p!(dedicated_path)
      File.touch!(Path.join(dedicated_path, "00000.log"))
      :ets.insert(ets, {compound_key, "value", 0, LFU.initial(), 0, 0, 5})

      latch_tab =
        :ets.new(:"sm_promoted_prefix_latch_#{System.unique_integer([:positive])}", [
          :set,
          :public
        ])

      latch_key = {:promoted_compaction, redis_key}
      assert :ets.insert_new(latch_tab, {latch_key, self()})

      latch_refs =
        List.duplicate(latch_tab, shard_index + 1)
        |> List.to_tuple()

      instance_ctx = %FerricStore.Instance{
        name: :state_machine_test,
        data_dir: state.data_dir,
        data_dir_expanded: state.data_dir,
        latch_refs: latch_refs
      }

      state = %{state | instance_ctx: instance_ctx}

      task =
        Task.async(fn ->
          StateMachine.apply(%{}, {:compound_delete_prefix, prefix}, state)
        end)

      try do
        refute Task.yield(task, 50)

        :ets.delete(latch_tab, latch_key)
        assert {%{}, :ok} = Task.await(task, 1_000)
      after
        safe_delete_ets(latch_tab)
      end
    end
  end

  describe "Bitcask rotation/accounting" do
    test "applied writes rotate the active file when they exceed max_active_file_size", %{
      state: state,
      active_file_path: active_file_path
    } do
      state = %{state | max_active_file_size: 80}
      value = String.duplicate("x", 48)

      {state, :ok} = StateMachine.apply(%{}, {:put, "rotate_a", value, 0}, state)
      {state, :ok} = StateMachine.apply(%{}, {:put, "rotate_b", value, 0}, state)

      assert state.active_file_id > 0
      assert state.active_file_size == 0
      assert File.exists?(state.active_file_path)
      assert state.active_file_path != active_file_path
      assert {old_total, 0} = Map.fetch!(state.file_stats, 0)
      assert old_total == File.stat!(active_file_path).size

      assert {:ok, [{_key, _offset, _value_size, _expire_at_ms, false} | _]} =
               NIF.v2_scan_file(active_file_path)
    end
  end

  describe "origin async PUT replay" do
    test "persists the pending origin value when ETS still matches the command", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      :ets.insert(ets, {"origin_put", "old", 0, 1, :pending, 0, 0})

      {_state2, :ok} =
        StateMachine.apply(%{}, {:async, node(), {:put, "origin_put", "old", 0}}, state)

      assert {:ok, [{"origin_put", _off, 3, 0, false}]} = NIF.v2_scan_file(active_file_path)
      assert {:ok, "old"} = NIF.v2_pread_at(active_file_path, 0)
    end

    test "replays origin PUT when recovery has no local pending row", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      # If the origin crashes after Ra accepts the async command but before the
      # local pending write reaches Bitcask, recovery must apply the Ra log entry
      # instead of skipping it as an already-local write.
      {_state2, :ok} =
        StateMachine.apply(%{}, {:async, node(), {:put, "missing_origin_put", "v", 0}}, state)

      assert [{"missing_origin_put", "v", 0, _lfu, 0, 0, 1}] =
               :ets.lookup(ets, "missing_origin_put")

      assert {:ok, [{"missing_origin_put", 0, 1, 0, false}]} = NIF.v2_scan_file(active_file_path)
    end

    test "replays origin large PUT over an older cold value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "origin_large_put"
      old_value = String.duplicate("o", 70_000)
      new_value = String.duplicate("n", 70_000)

      {:ok, [{old_offset, _old_record_size}]} =
        NIF.v2_append_batch(active_file_path, [{key, old_value, 0}])

      :ets.insert(ets, {key, nil, 0, 1, 0, old_offset, byte_size(old_value)})

      {_state2, :ok} =
        StateMachine.apply(%{}, {:async, node(), {:put, key, new_value, 0}}, state)

      assert [{^key, nil, 0, _lfu, 0, new_offset, 70_000}] = :ets.lookup(ets, key)
      refute new_offset == old_offset
      assert {:ok, ^new_value} = NIF.v2_pread_at(active_file_path, new_offset)
    end

    test "does not duplicate an already-applied origin large PUT", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "origin_large_put_applied"
      value = String.duplicate("v", 70_000)

      {:ok, [{offset, _record_size}]} = NIF.v2_append_batch(active_file_path, [{key, value, 0}])
      :ets.insert(ets, {key, nil, 0, 1, 0, offset, byte_size(value)})

      {_state2, :ok} =
        StateMachine.apply(%{}, {:async, node(), {:put, key, value, 0}}, state)

      assert [{^key, nil, 0, _lfu, 0, ^offset, 70_000}] = :ets.lookup(ets, key)
      assert {:ok, [{^key, ^offset, 70_000, 0, false}]} = NIF.v2_scan_file(active_file_path)
    end

    test "persists stale origin PUT for replay without publishing over newer pending ETS", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index
    } do
      # Router may apply a later async RMW locally before the earlier async PUT
      # reaches StateMachine.apply/3. The origin replay must not write the old
      # command value over the newer local value in ETS, but the earlier Ra
      # entry still needs a Bitcask record before its cursor can be released.
      checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
      checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)
      disk_pressure = :atomics.new(shard_index + 1, signed: false)
      last_applied_index = :atomics.new(shard_index + 1, signed: false)
      last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)
      key = "stale_origin_put"

      state = %{
        state
        | release_cursor_interval: 1,
          instance_ctx: %{
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight,
            disk_pressure: disk_pressure,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            hot_cache_max_value_size: 64
          }
      }

      :ets.insert(ets, {key, "new", 0, 1, :pending, 0, 0})

      meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

      {_state2, {:applied_at, 1, :ok}, effects} =
        StateMachine.apply(meta, {:async, node(), {:put, key, "old", 0}}, state)

      assert [{^key, "new", 0, _lfu, :pending, 0, 0}] = :ets.lookup(ets, key)
      assert {:ok, [{^key, old_offset, 3, 0, false}]} = NIF.v2_scan_file(active_file_path)
      assert {:ok, "old"} = NIF.v2_pread_at(active_file_path, old_offset)
      assert :atomics.get(checkpoint_flags, shard_index + 1) == 1
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0

      refute Enum.any?(effects, &match?({:release_cursor, 1}, &1))
    end

    test "replays origin RMW when recovery has no local value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      {_state2, {:ok, 1}} =
        StateMachine.apply(%{}, {:async, node(), {:incr, "missing_origin_incr", 1}}, state)

      assert [{"missing_origin_incr", "1", 0, _lfu, 0, 0, 1}] =
               :ets.lookup(ets, "missing_origin_incr")

      assert {:ok, [{"missing_origin_incr", 0, 1, 0, false}]} =
               NIF.v2_scan_file(active_file_path)
    end

    test "replays origin RMW when recovery has the old pre-command value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      :ets.insert(ets, {"old_origin_incr", "1", 0, 1, 0, 0, 1})

      {_state2, {:ok, 2}} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "old_origin_incr", {:incr, "old_origin_incr", 1}, "2", 0}},
          state
        )

      assert [{"old_origin_incr", "2", 0, _lfu, 0, _off, 1}] =
               :ets.lookup(ets, "old_origin_incr")

      assert {:ok, records} = NIF.v2_scan_file(active_file_path)
      assert {"old_origin_incr", _offset, 1, 0, false} = List.last(records)
    end

    test "replays origin APPEND when cold location changes during apply read", %{
      state: state,
      ets: ets
    } do
      key = "old_origin_append_cold_retry"
      value = :binary.copy("r", 70_000)
      suffix = "tail"
      expected = value <> suffix
      test_pid = self()

      {state, :ok} = StateMachine.apply(%{}, {:put, key, value, 0}, state)

      assert [{^key, nil, 0, lfu, file_id, _offset, value_size} = live_entry] =
               :ets.lookup(ets, key)

      :ets.insert(ets, {key, nil, 0, lfu, file_id + 10_000, 0, value_size})

      Process.put(:ferricstore_state_machine_cold_location_miss_hook, fn ->
        send(test_pid, :state_machine_cold_location_retry_hook)
        :ets.insert(ets, live_entry)
      end)

      try do
        {state, {:ok, expected_size}} =
          StateMachine.apply(
            %{},
            {:async, node(),
             {:origin_checked, key, {:append, key, suffix}, value, 0, expected, 0}},
            state
          )

        assert expected_size == byte_size(expected)
        assert_receive :state_machine_cold_location_retry_hook, 500
        assert [{^key, nil, 0, _lfu, fid, off, ^expected_size}] = :ets.lookup(ets, key)
        assert {:ok, ^expected} = NIF.v2_pread_at(state.active_file_path, off)
        assert fid == state.active_file_id
      after
        Process.delete(:ferricstore_state_machine_cold_location_miss_hook)
      end
    end

    test "replays origin GETSET when recovery has the pre-command value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"old_origin_getset", "old", 0, 1, 0, 0, 3})

      {_state2, "old"} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "old_origin_getset", {:getset, "old_origin_getset", "new"}, "old", 0,
            "new", 0}},
          state
        )

      assert [{"old_origin_getset", "new", 0, _lfu, 0, _off, 3}] =
               :ets.lookup(ets, "old_origin_getset")
    end

    test "replays origin GETSET over an unaccepted pending local value", %{state: state, ets: ets} do
      :ets.insert(ets, {"future_origin_getset", "future", 0, 1, :pending, 0, 0})

      {_state2, "old"} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "future_origin_getset", {:getset, "future_origin_getset", "new"},
            "old", 0, "new", 0}},
          state
        )

      assert [{"future_origin_getset", "new", 0, _lfu, 0, _off, 3}] =
               :ets.lookup(ets, "future_origin_getset")
    end

    test "does not replay origin GETSET over a durable newer local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"durable_future_getset", "future", 0, 1, 0, 0, 6})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "durable_future_getset", {:getset, "durable_future_getset", "new"},
            "old", 0, "new", 0}},
          state
        )

      assert [{"durable_future_getset", "future", 0, _lfu, 0, 0, 6}] =
               :ets.lookup(ets, "durable_future_getset")
    end

    test "does not replay stale origin PUT over a durable newer local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"durable_future_put", "future", 0, 1, 0, 0, 6})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "durable_future_put", {:put, "durable_future_put", "old", 0}, nil, 0,
            "old", 0}},
          state
        )

      assert [{"durable_future_put", "future", 0, _lfu, 0, 0, 6}] =
               :ets.lookup(ets, "durable_future_put")
    end

    test "does not replay stale origin DELETE over a durable newer local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"durable_future_delete", "future", 0, 1, 0, 0, 6})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "durable_future_delete", {:delete, "durable_future_delete"}, "old",
            0, nil, 0}},
          state
        )

      assert [{"durable_future_delete", "future", 0, _lfu, 0, 0, 6}] =
               :ets.lookup(ets, "durable_future_delete")
    end

    test "does not replay stale origin GETDEL over a durable newer local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"durable_future_getdel", "future", 0, 1, 0, 0, 6})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "durable_future_getdel", {:getdel, "durable_future_getdel"}, "old",
            0, nil, 0}},
          state
        )

      assert [{"durable_future_getdel", "future", 0, _lfu, 0, 0, 6}] =
               :ets.lookup(ets, "durable_future_getdel")
    end

    test "does not replay stale origin PUT over a pending newer local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"pending_future_put", "future", 0, 1, :pending, 0, 0})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "pending_future_put", {:put, "pending_future_put", "old", 0}, nil, 0,
            "old", 0}},
          state
        )

      assert [{"pending_future_put", "future", 0, _lfu, :pending, 0, 0}] =
               :ets.lookup(ets, "pending_future_put")
    end

    test "does not replay stale origin DELETE over a pending newer local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"pending_future_delete", "future", 0, 1, :pending, 0, 0})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "pending_future_delete", {:delete, "pending_future_delete"}, "old",
            0, nil, 0}},
          state
        )

      assert [{"pending_future_delete", "future", 0, _lfu, :pending, 0, 0}] =
               :ets.lookup(ets, "pending_future_delete")
    end

    test "does not replay stale origin GETDEL over a pending newer local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"pending_future_getdel", "future", 0, 1, :pending, 0, 0})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "pending_future_getdel", {:getdel, "pending_future_getdel"}, "old",
            0, nil, 0}},
          state
        )

      assert [{"pending_future_getdel", "future", 0, _lfu, :pending, 0, 0}] =
               :ets.lookup(ets, "pending_future_getdel")
    end

    test "replays origin GETDEL tombstone when local delete already removed the key", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "origin_getdel_tombstone", {:getdel, "origin_getdel_tombstone"},
            "old", 0, nil, 0}},
          state
        )

      assert [] == :ets.lookup(ets, "origin_getdel_tombstone")

      assert {:ok, [{"origin_getdel_tombstone", _offset, _record_size, 0, true}]} =
               NIF.v2_scan_file(active_file_path)
    end

    test "materializes pending origin GETSET even when value equals expected", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      :ets.insert(ets, {"pending_origin_getset", "new", 0, 1, :pending, 0, 0})

      {_state2, "old"} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "pending_origin_getset", {:getset, "pending_origin_getset", "new"},
            "old", 0, "new", 0}},
          state
        )

      assert [{"pending_origin_getset", "new", 0, _lfu, 0, 0, 3}] =
               :ets.lookup(ets, "pending_origin_getset")

      assert {:ok, [{"pending_origin_getset", 0, 3, 0, false}]} =
               NIF.v2_scan_file(active_file_path)
    end

    test "does not replay origin INCR over a provably newer pending local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"pending_origin_incr_newer", "10", 0, 1, :pending, 0, 0})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "pending_origin_incr_newer", {:incr, "pending_origin_incr_newer", 1},
            "4", 0, "5", 0}},
          state
        )

      assert [{"pending_origin_incr_newer", "10", 0, _lfu, :pending, 0, 0}] =
               :ets.lookup(ets, "pending_origin_incr_newer")
    end

    test "does not replay origin DECR over a provably newer pending local value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"pending_origin_decr_newer", "-10", 0, 1, :pending, 0, 0})

      {_state2, :ok} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "pending_origin_decr_newer",
            {:incr, "pending_origin_decr_newer", -1}, "-4", 0, "-5", 0}},
          state
        )

      assert [{"pending_origin_decr_newer", "-10", 0, _lfu, :pending, 0, 0}] =
               :ets.lookup(ets, "pending_origin_decr_newer")
    end

    test "replays origin GETEX when recovery has the old expiry", %{state: state, ets: ets} do
      old_expire_at_ms = Ferricstore.HLC.now_ms() + 10_000
      new_expire_at_ms = old_expire_at_ms + 10_000

      :ets.insert(ets, {"old_origin_getex", "value", old_expire_at_ms, 1, 0, 0, 5})

      {_state2, "value"} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "old_origin_getex", {:getex, "old_origin_getex", new_expire_at_ms},
            "value", old_expire_at_ms, "value", new_expire_at_ms}},
          state
        )

      assert [{"old_origin_getex", "value", ^new_expire_at_ms, _lfu, 0, _off, 5}] =
               :ets.lookup(ets, "old_origin_getex")
    end

    test "replays origin SETRANGE when recovery has the pre-command value", %{
      state: state,
      ets: ets
    } do
      :ets.insert(ets, {"old_origin_setrange", "hello", 0, 1, 0, 0, 5})

      {_state2, {:ok, 5}} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "old_origin_setrange", {:setrange, "old_origin_setrange", 2, "X"},
            "hello", 0, "heXlo", 0}},
          state
        )

      assert [{"old_origin_setrange", "heXlo", 0, _lfu, 0, _off, 5}] =
               :ets.lookup(ets, "old_origin_setrange")
    end

    test "replays origin SETRANGE expected value over pending local value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      :ets.insert(ets, {"pending_origin_setrange_newer", "heXlo!", 0, 1, :pending, 0, 0})

      {_state2, {:ok, 5}} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, "pending_origin_setrange_newer",
            {:setrange, "pending_origin_setrange_newer", 2, "X"}, "hello", 0, "heXlo", 0}},
          state
        )

      assert [{"pending_origin_setrange_newer", "heXlo", 0, _lfu, 0, 0, 5}] =
               :ets.lookup(ets, "pending_origin_setrange_newer")

      assert {:ok, [{"pending_origin_setrange_newer", 0, 5, 0, false}]} =
               NIF.v2_scan_file(active_file_path)
    end

    test "replays origin checked large APPEND over an older cold value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "old_origin_large_append"
      old_value = String.duplicate("o", 70_000)
      suffix = String.duplicate("n", 2048)
      expected_value = old_value <> suffix

      {:ok, [{old_offset, _old_record_size}]} =
        NIF.v2_append_batch(active_file_path, [{key, old_value, 0}])

      :ets.insert(ets, {key, nil, 0, 1, 0, old_offset, byte_size(old_value)})

      {_state2, {:ok, expected_size}} =
        StateMachine.apply(
          %{},
          {:async, node(),
           {:origin_checked, key, {:append, key, suffix}, old_value, 0, expected_value, 0}},
          state
        )

      assert expected_size == byte_size(expected_value)
      assert [{^key, nil, 0, _lfu, 0, new_offset, value_size}] = :ets.lookup(ets, key)
      assert value_size == byte_size(expected_value)
      refute new_offset == old_offset
      assert {:ok, ^expected_value} = NIF.v2_pread_at(active_file_path, new_offset)
    end

    test "replays origin async DELETE when recovery still has an older value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      {:ok, [{old_offset, old_size}]} =
        NIF.v2_append_batch(active_file_path, [{"origin_delete", "old", 0}])

      :ets.insert(ets, {"origin_delete", nil, 0, 1, 0, old_offset, old_size})

      {_state2, :ok} =
        StateMachine.apply(%{}, {:async, node(), {:delete, "origin_delete"}}, state)

      assert [] == :ets.lookup(ets, "origin_delete")
      assert {:ok, records} = NIF.v2_scan_file(active_file_path)

      assert Enum.any?(records, fn {"origin_delete", _off, _size, _exp, tombstone?} ->
               tombstone?
             end)
    end

    test "DELETE aborts when pending PUT cannot be flushed before tombstone", %{
      state: state,
      ets: ets,
      dir: dir,
      shard_index: shard_index,
      writer_pid: writer_pid,
      active_file_path: active_file_path
    } do
      key = "pending_delete_ordering"
      missing_path = Path.join([dir, "missing_parent", "00000.log"])
      :ets.insert(ets, {key, "value", 0, 1, :pending, 0, 0})

      :sys.replace_state(writer_pid, fn writer_state ->
        %{
          writer_state
          | pending: [{:write, nil, missing_path, 0, ets, key, "value", 0}],
            pending_count: 1
        }
      end)

      {_state2, {:error, {:bitcask_writer_flush_failed, {:flush_failed, 1}}}} =
        StateMachine.apply(%{}, {:delete, key}, state)

      assert [{^key, "value", 0, _lfu, :pending, 0, 0}] = :ets.lookup(ets, key)
      assert {:ok, records} = NIF.v2_scan_file(active_file_path)

      refute Enum.any?(records, fn {record_key, _off, _size, _exp, tombstone?} ->
               record_key == key and tombstone?
             end)

      assert shard_index == state.shard_index
    end

    test "origin async DELETE persists tombstone even when Router already removed ETS", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      assert [] == :ets.lookup(ets, "origin_delete_missing_ets")

      {_state2, :ok} =
        StateMachine.apply(%{}, {:async, node(), {:delete, "origin_delete_missing_ets"}}, state)

      assert [] == :ets.lookup(ets, "origin_delete_missing_ets")
      assert {:ok, records} = NIF.v2_scan_file(active_file_path)

      assert Enum.any?(records, fn {"origin_delete_missing_ets", _off, _size, _exp, tombstone?} ->
               tombstone?
             end)
    end

    test "replays origin async GETDEL when recovery still has an older value", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      {:ok, [{old_offset, old_size}]} =
        NIF.v2_append_batch(active_file_path, [{"origin_getdel", "old", 0}])

      :ets.insert(ets, {"origin_getdel", nil, 0, 1, 0, old_offset, old_size})

      {_state2, "old"} =
        StateMachine.apply(%{}, {:async, node(), {:getdel, "origin_getdel"}}, state)

      assert [] == :ets.lookup(ets, "origin_getdel")
      assert {:ok, records} = NIF.v2_scan_file(active_file_path)

      assert Enum.any?(records, fn {"origin_getdel", _off, _size, _exp, tombstone?} ->
               tombstone?
             end)
    end

    test "origin async GETDEL persists tombstone even when Router already removed ETS", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      assert [] == :ets.lookup(ets, "origin_getdel_missing_ets")

      {_state2, nil} =
        StateMachine.apply(%{}, {:async, node(), {:getdel, "origin_getdel_missing_ets"}}, state)

      assert [] == :ets.lookup(ets, "origin_getdel_missing_ets")
      assert {:ok, records} = NIF.v2_scan_file(active_file_path)

      assert Enum.any?(records, fn {"origin_getdel_missing_ets", _off, _size, _exp, tombstone?} ->
               tombstone?
             end)
    end

    test "GETDEL returns flush error when pending PUT cannot be ordered before tombstone", %{
      state: state,
      ets: ets,
      dir: dir,
      writer_pid: writer_pid,
      active_file_path: active_file_path
    } do
      key = "pending_getdel_ordering"
      missing_path = Path.join([dir, "missing_parent", "00000.log"])
      :ets.insert(ets, {key, "value", 0, 1, :pending, 0, 0})

      :sys.replace_state(writer_pid, fn writer_state ->
        %{
          writer_state
          | pending: [{:write, nil, missing_path, 0, ets, key, "value", 0}],
            pending_count: 1
        }
      end)

      {_state2, {:error, {:bitcask_writer_flush_failed, {:flush_failed, 1}}}} =
        StateMachine.apply(%{}, {:getdel, key}, state)

      assert [{^key, "value", 0, _lfu, :pending, 0, 0}] = :ets.lookup(ets, key)
      assert {:ok, records} = NIF.v2_scan_file(active_file_path)

      refute Enum.any?(records, fn {record_key, _off, _size, _exp, tombstone?} ->
               record_key == key and tombstone?
             end)
    end
  end

  describe "state-machine compound reads" do
    test "HINCRBY reads cold promoted hash values from dedicated Bitcask", %{
      state: state,
      ets: ets
    } do
      key = "hash_promoted_cold_hincrby_#{System.unique_integer([:positive])}"
      field_key = CompoundKey.hash_field(key, "counter")

      dedicated_path =
        Ferricstore.Store.Promotion.dedicated_path(
          state.data_dir,
          state.shard_index,
          :hash,
          key
        )

      File.mkdir_p!(dedicated_path)
      dedicated_file = Path.join(dedicated_path, "00000.log")
      File.touch!(dedicated_file)

      {:ok, [{cold_offset, cold_size}]} =
        NIF.v2_append_batch(dedicated_file, [{field_key, "41", 0}])

      :ets.insert(ets, {field_key, nil, 0, 1, 0, cold_offset, cold_size})

      {_state2, result} =
        StateMachine.apply(%{}, {:hincrby, key, "counter", 1}, state)

      assert 42 == result
      assert [{^field_key, "42", 0, _lfu, 0, _off, 2}] = :ets.lookup(ets, field_key)
    end

    test "HINCRBYFLOAT reads cold promoted hash values from dedicated Bitcask", %{
      state: state,
      ets: ets
    } do
      key = "hash_promoted_cold_hincrbyfloat_#{System.unique_integer([:positive])}"
      field_key = CompoundKey.hash_field(key, "ratio")

      dedicated_path =
        Ferricstore.Store.Promotion.dedicated_path(
          state.data_dir,
          state.shard_index,
          :hash,
          key
        )

      File.mkdir_p!(dedicated_path)
      dedicated_file = Path.join(dedicated_path, "00000.log")
      File.touch!(dedicated_file)

      {:ok, [{cold_offset, cold_size}]} =
        NIF.v2_append_batch(dedicated_file, [{field_key, "41.5", 0}])

      :ets.insert(ets, {field_key, nil, 0, 1, 0, cold_offset, cold_size})

      {_state2, result} =
        StateMachine.apply(%{}, {:hincrbyfloat, key, "ratio", 1.0}, state)

      assert "42.5" == result
      assert [{^field_key, "42.5", 0, _lfu, 0, _off, 4}] = :ets.lookup(ets, field_key)
    end

    test "ZINCRBY reads cold promoted zset values from dedicated Bitcask", %{
      state: state,
      ets: ets
    } do
      key = "zset_promoted_cold_zincrby_#{System.unique_integer([:positive])}"
      member_key = CompoundKey.zset_member(key, "Palermo")

      dedicated_path =
        Ferricstore.Store.Promotion.dedicated_path(
          state.data_dir,
          state.shard_index,
          :zset,
          key
        )

      File.mkdir_p!(dedicated_path)
      dedicated_file = Path.join(dedicated_path, "00000.log")
      File.touch!(dedicated_file)

      {:ok, [{cold_offset, cold_size}]} =
        NIF.v2_append_batch(dedicated_file, [{member_key, "41.5", 0}])

      :ets.insert(ets, {member_key, nil, 0, 1, 0, cold_offset, cold_size})

      {_state2, result} =
        StateMachine.apply(%{}, {:zincrby, key, 1.0, "Palermo"}, state)

      assert "42.5" == result
      assert [{^member_key, "42.5", 0, _lfu, 0, _off, 4}] = :ets.lookup(ets, member_key)
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :put
  # ---------------------------------------------------------------------------

  describe "apply/3 with {:put, key, value, expire_at_ms}" do
    test "uses raft meta system_time when checking cross-shard lock expiry", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      lock_expires_after_apply_time = apply_now + 10_000

      locked_state = %{
        state
        | cross_shard_locks: %{
            "meta_time_locked" => {make_ref(), lock_expires_after_apply_time}
          }
      }

      {_new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:put, "meta_time_locked", "value", 0},
          locked_state
        )

      assert {:error, :key_locked} = result
      assert [] == :ets.lookup(ets, "meta_time_locked")
    end

    test "prefers stamped command HLC over raft meta system_time", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      meta_now = local_now - 30_000
      hlc_now = meta_now + 20_000
      lock_expires_between_meta_and_hlc = meta_now + 10_000

      locked_state = %{
        state
        | cross_shard_locks: %{
            "hlc_time_locked" => {make_ref(), lock_expires_between_meta_and_hlc}
          }
      }

      {_new_state, result} =
        StateMachine.apply(
          %{system_time: meta_now},
          {{:put, "hlc_time_locked", "value", 0}, %{hlc_ts: {hlc_now, 0}}},
          locked_state
        )

      assert :ok = result
      assert [{"hlc_time_locked", "value", 0, _, _, _, _}] = :ets.lookup(ets, "hlc_time_locked")
    end

    test "cross-shard dispatched SETEX uses stamped HLC time for relative expiry", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000

      {_new_state, %{^shard_index => [:ok]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:cross_shard_tx, [{shard_index, [{"SETEX", ["stamped_setex", "5", "value"]}], nil}]},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + 5_000

      assert [{"stamped_setex", "value", ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "stamped_setex")
    end

    test "cross-shard dispatched RMW commands preserve existing TTL", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      expire_at_ms = Ferricstore.HLC.now_ms() + 60_000

      cases = [
        {"cross_tx_incr_ttl", "5", {"INCR", ["cross_tx_incr_ttl"]}, "6"},
        {"cross_tx_append_ttl", "base", {"APPEND", ["cross_tx_append_ttl", "-tail"]},
         "base-tail"},
        {"cross_tx_setrange_ttl", "abcdef", {"SETRANGE", ["cross_tx_setrange_ttl", "2", "ZZ"]},
         "abZZef"}
      ]

      Enum.each(cases, fn {key, initial, command, expected} ->
        :ets.insert(
          ets,
          {key, initial, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(initial)}
        )

        {_new_state, %{^shard_index => [_result]}} =
          StateMachine.apply(
            %{system_time: Ferricstore.HLC.now_ms()},
            {:cross_shard_tx, [{shard_index, [command], nil}]},
            state
          )

        assert [{^key, ^expected, ^expire_at_ms, _lfu, _fid, _off, _vsize}] =
                 :ets.lookup(ets, key)
      end)
    end

    test "cross-shard dispatched EXISTS uses cold metadata without pread", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      key = "cross_tx_exists_cold"

      :ets.insert(
        ets,
        {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, 999_999, byte_size("large-cold")}
      )

      {_new_state, %{^shard_index => [1]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"EXISTS", [key]}], nil}]},
          state
        )

      assert [{^key, nil, 0, _lfu, 0, 999_999, _vsize}] = :ets.lookup(ets, key)
    end

    test "cross-shard dispatched SET is appended before acknowledgement", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index,
      writer_pid: writer_pid
    } do
      GenServer.stop(writer_pid, :normal, 5_000)

      {_new_state, %{^shard_index => [:ok]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"SET", ["cross_durable", "durable-value"]}], nil}]},
          state
        )

      value_size = byte_size("durable-value")

      assert {:ok, [{"cross_durable", _off, ^value_size, 0, false}]} =
               NIF.v2_scan_file(active_file_path)

      assert [{"cross_durable", "durable-value", 0, _, 0, _off, ^value_size}] =
               :ets.lookup(ets, "cross_durable")
    end

    test "failed cross-shard multi-target append does not leave replayable partial records", %{
      state: state,
      ets: ets,
      active_file_path: shard0_file
    } do
      root =
        Path.join(System.tmp_dir!(), "sm_cross_partial_#{System.unique_integer([:positive])}")

      shard0 = 0
      shard1 = 1
      shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
      shard1_bad_active = Path.join(shard1_path, "active_is_directory.log")
      ets1 = :ets.new(:"sm_cross_partial_#{System.unique_integer([:positive])}", [:set, :public])
      instance_name = :"sm_cross_partial_#{System.unique_integer([:positive])}"

      File.mkdir_p!(shard1_bad_active)
      Ferricstore.Store.ActiveFile.init(2)

      instance_ctx = %{
        name: instance_name,
        data_dir: root,
        shard_count: 2,
        keydir_refs: List.to_tuple([ets, ets1]),
        keydir_binary_bytes: :atomics.new(2, signed: false),
        checkpoint_flags: :atomics.new(shard1 + 1, signed: false),
        checkpoint_in_flight: :atomics.new(shard1 + 1, signed: false),
        disk_pressure: :atomics.new(shard1 + 1, signed: false),
        hot_cache_max_value_size: 64
      }

      Ferricstore.Store.ActiveFile.publish(
        instance_ctx,
        shard0,
        0,
        shard0_file,
        state.shard_data_path
      )

      Ferricstore.Store.ActiveFile.publish(
        instance_ctx,
        shard1,
        0,
        shard1_bad_active,
        shard1_path
      )

      state = %{state | shard_index: shard0, instance_ctx: instance_ctx}

      try do
        {_new_state, {:error, {:bitcask_append_failed, _reason}}} =
          StateMachine.apply(
            %{system_time: Ferricstore.HLC.now_ms()},
            {:cross_shard_tx,
             [
               {shard0, [{"SET", ["partial_success", "must-not-replay"]}], nil},
               {shard1, [{"SET", ["partial_failure", "fail"]}], nil}
             ]},
            state
          )

        assert [] = :ets.lookup(ets, "partial_success")

        recovered =
          :ets.new(:"sm_cross_partial_recovered_#{System.unique_integer([:positive])}", [
            :set,
            :public
          ])

        try do
          Ferricstore.Store.Shard.Lifecycle.recover_keydir(
            state.shard_data_path,
            recovered,
            shard0,
            instance_ctx
          )

          assert [] = :ets.lookup(recovered, "partial_success")
        after
          :ets.delete(recovered)
        end
      after
        :ets.delete(ets1)
        Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
        File.rm_rf!(root)
      end
    end

    test "failed cross-shard multi-target overwrite restores replayable original record", %{
      state: state,
      ets: ets,
      active_file_path: shard0_file
    } do
      root =
        Path.join(System.tmp_dir!(), "sm_cross_partial_#{System.unique_integer([:positive])}")

      shard0 = 0
      shard1 = 1
      shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
      shard1_bad_active = Path.join(shard1_path, "active_is_directory.log")
      ets1 = :ets.new(:"sm_cross_partial_#{System.unique_integer([:positive])}", [:set, :public])
      instance_name = :"sm_cross_partial_#{System.unique_integer([:positive])}"

      {:ok, {old_offset, old_size}} =
        NIF.v2_append_record(shard0_file, "partial_existing", "old", 0)

      :ets.insert(
        ets,
        {"partial_existing", "old", 0, Ferricstore.Store.LFU.initial(), 0, old_offset, old_size}
      )

      File.mkdir_p!(shard1_bad_active)
      Ferricstore.Store.ActiveFile.init(2)

      instance_ctx = %{
        name: instance_name,
        data_dir: root,
        shard_count: 2,
        keydir_refs: List.to_tuple([ets, ets1]),
        keydir_binary_bytes: :atomics.new(2, signed: false),
        checkpoint_flags: :atomics.new(shard1 + 1, signed: false),
        checkpoint_in_flight: :atomics.new(shard1 + 1, signed: false),
        disk_pressure: :atomics.new(shard1 + 1, signed: false),
        hot_cache_max_value_size: 64
      }

      Ferricstore.Store.ActiveFile.publish(
        instance_ctx,
        shard0,
        0,
        shard0_file,
        state.shard_data_path
      )

      Ferricstore.Store.ActiveFile.publish(
        instance_ctx,
        shard1,
        0,
        shard1_bad_active,
        shard1_path
      )

      state = %{state | shard_index: shard0, instance_ctx: instance_ctx}

      try do
        {_new_state, {:error, {:bitcask_append_failed, _reason}}} =
          StateMachine.apply(
            %{system_time: Ferricstore.HLC.now_ms()},
            {:cross_shard_tx,
             [
               {shard0, [{"SET", ["partial_existing", "new"]}], nil},
               {shard1, [{"SET", ["partial_failure", "fail"]}], nil}
             ]},
            state
          )

        assert [{"partial_existing", "old", 0, _, 0, ^old_offset, ^old_size}] =
                 :ets.lookup(ets, "partial_existing")

        recovered =
          :ets.new(:"sm_cross_partial_recovered_#{System.unique_integer([:positive])}", [
            :set,
            :public
          ])

        try do
          Ferricstore.Store.Shard.Lifecycle.recover_keydir(
            state.shard_data_path,
            recovered,
            shard0,
            instance_ctx
          )

          assert [{"partial_existing", nil, 0, _, 0, recovered_offset, 3}] =
                   :ets.lookup(recovered, "partial_existing")

          assert {:ok, "old"} = NIF.v2_pread_at(shard0_file, recovered_offset)
        after
          :ets.delete(recovered)
        end
      after
        :ets.delete(ets1)
        Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
        File.rm_rf!(root)
      end
    end

    test "cross-shard dispatched large SET has a cold location before acknowledgement", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index,
      writer_pid: writer_pid
    } do
      GenServer.stop(writer_pid, :normal, 5_000)
      large_value = String.duplicate("x", 70_000)

      {_new_state, %{^shard_index => [:ok]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"SET", ["cross_large", large_value]}], nil}]},
          state
        )

      value_size = byte_size(large_value)

      assert {:ok, [{"cross_large", offset, ^value_size, 0, false}]} =
               NIF.v2_scan_file(active_file_path)

      assert [{"cross_large", nil, 0, _, 0, ^offset, ^value_size}] =
               :ets.lookup(ets, "cross_large")

      assert {:ok, ^large_value} = NIF.v2_pread_at(active_file_path, offset)
    end

    test "cross-shard dispatched PEXPIRE uses stamped HLC time for relative expiry", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000

      :ets.insert(
        ets,
        {"stamped_pexpire", "value", 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size("value")}
      )

      {_new_state, %{^shard_index => [1]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:cross_shard_tx, [{shard_index, [{"PEXPIRE", ["stamped_pexpire", "5000"]}], nil}]},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + 5_000

      assert [{"stamped_pexpire", "value", ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "stamped_pexpire")
    end

    test "cross-shard dispatched PEXPIREAT compares absolute expiry to stamped HLC time", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      expire_at_ms = stamped_now + 5_000

      :ets.insert(
        ets,
        {"stamped_pexpireat", "value", 0, Ferricstore.Store.LFU.initial(), 0, 0,
         byte_size("value")}
      )

      {_new_state, %{^shard_index => [1]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:cross_shard_tx,
            [
              {shard_index,
               [{"PEXPIREAT", ["stamped_pexpireat", Integer.to_string(expire_at_ms)]}], nil}
            ]}, %{hlc_ts: {stamped_now, 0}}},
          state
        )

      assert [{"stamped_pexpireat", "value", ^expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "stamped_pexpireat")
    end

    test "cross-shard dispatched PTTL reports remaining time from stamped HLC time", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      expire_at_ms = stamped_now + 5_000

      :ets.insert(
        ets,
        {"stamped_pttl", "value", expire_at_ms, Ferricstore.Store.LFU.initial(), 0, 0,
         byte_size("value")}
      )

      {_new_state, %{^shard_index => [5_000]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:cross_shard_tx, [{shard_index, [{"PTTL", ["stamped_pttl"]}], nil}]},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )
    end

    test "cross-shard GET reads cold value from valid file id zero", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index
    } do
      {:ok, {offset, value_size}} =
        NIF.v2_append_record(active_file_path, "cross_cold_fid0", "cold-value", 0)

      :ets.insert(
        ets,
        {"cross_cold_fid0", nil, 0, Ferricstore.Store.LFU.initial(), 0, offset, value_size}
      )

      {_new_state, %{^shard_index => ["cold-value"]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"GET", ["cross_cold_fid0"]}], nil}]},
          state
        )
    end

    test "cross-shard MGET preserves values from hot keydir entries", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      :ets.insert(
        ets,
        {"cross_mget_a", "value-a", 0, Ferricstore.Store.LFU.initial(), 0, 0,
         byte_size("value-a")}
      )

      :ets.insert(
        ets,
        {"cross_mget_b", "value-b", 0, Ferricstore.Store.LFU.initial(), 0, 0,
         byte_size("value-b")}
      )

      {_new_state, %{^shard_index => [["value-a", "value-b"]]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"MGET", ["cross_mget_a", "cross_mget_b"]}], nil}]},
          state
        )
    end

    test "cross-shard GET rejects mismatched cold offsets", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index
    } do
      key = "cross_cold_stale_offset"
      other_key = "cross_cold_other_offset"

      {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
        NIF.v2_append_batch(active_file_path, [
          {other_key, "wrong-value", 0},
          {key, "right-value", 0}
        ])

      :ets.insert(
        ets,
        {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, other_offset, value_size}
      )

      {_new_state, %{^shard_index => [nil]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"GET", [key]}], nil}]},
          state
        )

      assert [{^key, nil, 0, _lfu, 0, ^other_offset, ^value_size}] = :ets.lookup(ets, key)
    end

    test "cross-shard PTTL reads cold metadata from valid file id zero", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path,
      shard_index: shard_index
    } do
      now = Ferricstore.HLC.now_ms()
      expire_at_ms = now + 5_000

      {:ok, {offset, value_size}} =
        NIF.v2_append_record(active_file_path, "cross_cold_meta_fid0", "cold-meta", expire_at_ms)

      :ets.insert(
        ets,
        {"cross_cold_meta_fid0", nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, offset,
         value_size}
      )

      {_new_state, %{^shard_index => [5_000]}} =
        StateMachine.apply(
          %{system_time: now},
          {:cross_shard_tx, [{shard_index, [{"PTTL", ["cross_cold_meta_fid0"]}], nil}]},
          state
        )
    end

    test "cross-shard GET purges malformed cold location instead of retrying exception path", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      :ets.insert(
        ets,
        {"cross_bad_offset", nil, 0, Ferricstore.Store.LFU.initial(), 0, :pending_offset, 5}
      )

      {_new_state, %{^shard_index => [nil]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"GET", ["cross_bad_offset"]}], nil}]},
          state
        )

      assert [] == :ets.lookup(ets, "cross_bad_offset")
    end

    test "cross-shard PTTL purges malformed cold location instead of retrying exception path", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      :ets.insert(
        ets,
        {"cross_bad_meta_offset", nil, Ferricstore.HLC.now_ms() + 5_000,
         Ferricstore.Store.LFU.initial(), 0, :pending_offset, 5}
      )

      {_new_state, %{^shard_index => [-2]}} =
        StateMachine.apply(
          %{system_time: Ferricstore.HLC.now_ms()},
          {:cross_shard_tx, [{shard_index, [{"PTTL", ["cross_bad_meta_offset"]}], nil}]},
          state
        )

      assert [] == :ets.lookup(ets, "cross_bad_meta_offset")
    end

    test "cross-shard read fallbacks report unavailable keydirs", %{
      state: state,
      ets: ets,
      shard_index: shard_index
    } do
      handler_id = {__MODULE__, self(), make_ref()}
      parent = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :store, :shard_unavailable],
          fn event, measurements, metadata, _config ->
            send(parent, {:sm_keydir_unavailable, event, measurements, metadata})
          end,
          nil
        )

      try do
        :ets.delete(ets)

        {_new_state, %{^shard_index => [nil, -2, 0]}} =
          StateMachine.apply(
            %{system_time: Ferricstore.HLC.now_ms()},
            {:cross_shard_tx,
             [
               {shard_index,
                [
                  {"GET", ["missing_keydir_get"]},
                  {"PTTL", ["missing_keydir_pttl"]},
                  {"HLEN", ["missing_keydir_hash"]}
                ], nil}
             ]},
            state
          )

        assert_keydir_unavailable_event(:cross_shard_get)
        assert_keydir_unavailable_event(:cross_shard_get_meta)
        assert_keydir_unavailable_event(:cross_shard_prefix_count)
      after
        :telemetry.detach(handler_id)
      end
    end

    test "cross-shard prefix delete reports unavailable keydirs" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/raft/state_machine.ex", __DIR__))

      [_, body] = String.split(source, "defp cross_shard_delete_prefix", parts: 2)
      body = body |> String.split("defp sm_file_path_from_path", parts: 2) |> hd()

      assert body =~ "emit_cross_shard_keydir_unavailable(ctx, :cross_shard_delete_prefix)",
             "cross_shard_delete_prefix/3 must emit shard_unavailable before returning :ok on a missing ETS keydir"
    end

    test "stamped ratelimit ignores legacy embedded now_ms", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      embedded_now = local_now + 30_000
      window_ms = 10_000

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:ratelimit_add, "stamped_ratelimit", window_ms, 10, 1, embedded_now},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "stamped batch ratelimit ignores legacy embedded now_ms", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      embedded_now = local_now + 30_000
      window_ms = 10_000

      {_new_state, {:ok, [["allowed", 1, 9, ^window_ms]]}} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:batch,
            [{:ratelimit_add, "batch_stamped_ratelimit", window_ms, 10, 1, embedded_now}]},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"batch_stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "batch_stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "stamped ratelimit repairs malformed state with stamped time", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      stamped_now = local_now - 30_000
      window_ms = 10_000

      :ets.insert(
        ets,
        {"malformed_stamped_ratelimit", "bad-state", 0, Ferricstore.Store.LFU.initial(), 0, 0, 0}
      )

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{system_time: local_now},
          {{:ratelimit_add, "malformed_stamped_ratelimit", window_ms, 10, 1},
           %{hlc_ts: {stamped_now, 0}}},
          state
        )

      expected_expire_at_ms = stamped_now + window_ms * 2

      assert [{"malformed_stamped_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "malformed_stamped_ratelimit")

      assert {1, ^stamped_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "legacy unwrapped ratelimit keeps embedded now_ms for replay compatibility", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      embedded_now = local_now - 30_000
      window_ms = 10_000

      {_new_state, ["allowed", 1, 9, ^window_ms]} =
        StateMachine.apply(
          %{},
          {:ratelimit_add, "legacy_ratelimit", window_ms, 10, 1, embedded_now},
          state
        )

      expected_expire_at_ms = embedded_now + window_ms * 2

      assert [{"legacy_ratelimit", encoded, ^expected_expire_at_ms, _, _, _, _}] =
               :ets.lookup(ets, "legacy_ratelimit")

      assert {1, ^embedded_now, 0} = Ferricstore.Store.ValueCodec.decode_ratelimit(encoded)
    end

    test "uses raft meta system_time when acquiring cross-shard locks", %{state: state} do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      existing_lock_expiry = apply_now + 10_000
      existing_owner = make_ref()

      locked_state = %{
        state
        | cross_shard_locks: %{
            "meta_time_lock_conflict" => {existing_owner, existing_lock_expiry}
          }
      }

      {new_state, result} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:lock_keys, ["meta_time_lock_conflict"], make_ref(), apply_now + 30_000},
          locked_state
        )

      assert {:error, :keys_locked} = result

      assert %{"meta_time_lock_conflict" => {^existing_owner, ^existing_lock_expiry}} =
               new_state.cross_shard_locks
    end

    test "cross-shard control commands tolerate legacy state without lock maps", %{
      state: state
    } do
      apply_now = Ferricstore.HLC.now_ms()
      owner = make_ref()
      legacy_state = Map.drop(state, [:cross_shard_locks, :cross_shard_intents])

      {locked_state, :ok} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:lock_keys, ["legacy_lock"], owner, apply_now + 30_000},
          legacy_state
        )

      assert %{"legacy_lock" => {^owner, _expires_at}} = locked_state.cross_shard_locks

      {intent_state, :ok} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:cross_shard_intent, owner, %{0 => ["legacy_lock"]}},
          Map.drop(locked_state, [:cross_shard_intents])
        )

      assert %{^owner => %{0 => ["legacy_lock"]}} = intent_state.cross_shard_intents

      {cleared_state, :ok} =
        StateMachine.apply(%{system_time: apply_now}, {:clear_locks}, legacy_state)

      assert cleared_state.cross_shard_locks == %{}
      assert cleared_state.cross_shard_intents == %{}
    end

    test "uses raft meta system_time for standalone read-modify-write TTL checks", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      expires_after_apply_time = apply_now + 10_000

      :ets.insert(
        ets,
        {"meta_time_incr", "5", expires_after_apply_time, Ferricstore.Store.LFU.initial(), 0, 0,
         byte_size("5")}
      )

      {_new_state, {:ok, 6}} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:incr, "meta_time_incr", 1},
          state
        )

      assert [{"meta_time_incr", "6", ^expires_after_apply_time, _, _, _, _}] =
               :ets.lookup(ets, "meta_time_incr")
    end

    test "missing active file fails put and rolls back new key", %{
      state: state,
      ets: ets
    } do
      missing_state = state_with_missing_active_file(state)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_new", "value", 0}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert [] == :ets.lookup(ets, "missing_active_new")
    end

    test "missing active file fails overwrite and restores old ETS entry", %{
      state: state,
      ets: ets
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "missing_active_existing", "old", 0}, state)
      old_entry = :ets.lookup(ets, "missing_active_existing")
      missing_state = state_with_missing_active_file(state2)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_existing", "new", 0}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert old_entry == :ets.lookup(ets, "missing_active_existing")
    end

    test "missing state active file falls back to live ActiveFile registry", %{
      state: state,
      ets: ets,
      dir: dir,
      shard_index: shard_index
    } do
      file_id = 8_000_000 + :erlang.unique_integer([:positive])
      live_path = Path.join(dir, "#{file_id}.log")
      File.touch!(live_path)
      Ferricstore.Store.ActiveFile.publish(shard_index, file_id, live_path, dir)

      missing_state = state_with_missing_active_file(state, shard_index: shard_index)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "missing_active_fallback", "value", 0}, missing_state)

      assert :ok = result

      assert [{"missing_active_fallback", "value", 0, _, ^file_id, offset, value_size}] =
               :ets.lookup(ets, "missing_active_fallback")

      assert is_integer(offset)
      assert value_size > 0
    end

    test "uses live ActiveFile registry when state active file is stale but still exists", %{
      state: state,
      ets: ets,
      dir: dir,
      shard_index: shard_index
    } do
      live_file_id = 8_100_000 + :erlang.unique_integer([:positive])
      live_path = Path.join(dir, "#{live_file_id}.log")
      File.touch!(live_path)
      Ferricstore.Store.ActiveFile.publish(shard_index, live_file_id, live_path, dir)

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "stale_active_registry", "value", 0}, state)

      assert :ok = result

      assert [{"stale_active_registry", "value", 0, _, ^live_file_id, offset, value_size}] =
               :ets.lookup(ets, "stale_active_registry")

      assert is_integer(offset)
      assert value_size > 0
      assert {:ok, "value"} = NIF.v2_pread_at(live_path, offset)
    end

    test "Bitcask append errors fail quorum apply and roll back pending ETS", %{
      state: state,
      ets: ets
    } do
      file_id = 9_000_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)

      bad_state = %{state | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "append_error_key", "value", 0}, bad_state)

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert [] == :ets.lookup(ets, "append_error_key")
    end

    test "batch Bitcask append errors restore deleted ETS entries and remove new puts", %{
      state: state,
      ets: ets
    } do
      {state2, :ok} =
        StateMachine.apply(%{}, {:put, "delete_failure_existing", "old_value", 0}, state)

      old_entry = :ets.lookup(ets, "delete_failure_existing")

      file_id = 9_100_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)
      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch,
           [
             {:delete, "delete_failure_existing"},
             {:put, "delete_failure_new", "new_value", 0}
           ]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_entry == :ets.lookup(ets, "delete_failure_existing")
      assert [] == :ets.lookup(ets, "delete_failure_new")
    end

    test "emits bounded apply and Bitcask append telemetry", %{
      state: state,
      shard_index: shard_index
    } do
      handler_id = {:state_machine_quorum_telemetry, self(), make_ref()}

      :ok =
        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :raft, :apply],
            [:ferricstore, :bitcask, :append]
          ],
          fn event, measurements, metadata, test_pid ->
            send(test_pid, {:quorum_telemetry, event, measurements, metadata})
          end,
          self()
        )

      try do
        {_new_state, :ok} = StateMachine.apply(%{}, {:put, "telemetry_key", "value", 0}, state)

        assert_receive {:quorum_telemetry, [:ferricstore, :bitcask, :append], append_meas,
                        %{shard_index: ^shard_index, status: :ok}},
                       500

        assert append_meas.batch_size == 1
        assert append_meas.batch_bytes > 0
        assert is_integer(append_meas.duration_us)

        assert_receive {:quorum_telemetry, [:ferricstore, :raft, :apply], apply_meas,
                        %{shard_index: ^shard_index, result: :ok, disk: :ok}},
                       500

        assert is_integer(apply_meas.duration_us)
      after
        :telemetry.detach(handler_id)
      end
    end

    test "writes value to disk and ETS", %{state: state, ets: ets, shard_index: shard_index} do
      {new_state, result} =
        StateMachine.apply(%{}, {:put, "key1", "value1", 0}, state)

      assert result == :ok
      assert new_state.applied_count == 1

      # Verify ETS (v2 7-tuple format) — value is available immediately
      assert [{"key1", "value1", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "key1")

      # Flush background writer so disk location is materialized
      BitcaskWriter.flush(shard_index)

      # Verify disk via pread
      [{_, _, _, _, fid, off, _}] = :ets.lookup(ets, "key1")
      assert is_integer(fid)

      log_path =
        Path.join(
          state.shard_data_path,
          "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
        )

      assert {:ok, "value1"} = NIF.v2_pread_at(log_path, off)
    end

    test "put with expiry stores expire_at_ms", %{state: state, ets: ets} do
      future = System.os_time(:millisecond) + 60_000

      {_new_state, result} =
        StateMachine.apply(%{}, {:put, "expiring", "val", future}, state)

      assert result == :ok

      assert [{"expiring", "val", ^future, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(ets, "expiring")
    end

    test "put overwrites previous value", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "k", "v1", 0}, state)
      {state3, :ok} = StateMachine.apply(%{}, {:put, "k", "v2", 0}, state2)

      assert state3.applied_count == 2
      assert [{"k", "v2", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(ets, "k")
    end

    test "increments applied_count on each put", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
      {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

      assert s3.applied_count == 3
    end
  end

  describe "apply/3 with {:set, key, value, expire_at_ms, opts}" do
    test "SET NX treats a cold keydir entry as existing without warming it", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "set_nx_cold_existing"
      {:ok, {offset, _record_size}} = NIF.v2_append_record(active_file_path, key, "old", 0)
      value_size = byte_size("old")
      :ets.insert(ets, {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, offset, value_size})

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{nx: true})}, state)

      assert result == nil
      assert [{^key, nil, 0, _lfu, 0, ^offset, ^value_size}] = :ets.lookup(ets, key)
    end

    test "SET XX updates a cold key even when the old value is unreadable", %{
      state: state,
      ets: ets
    } do
      key = "set_xx_cold_unreadable"
      :ets.insert(ets, {key, nil, 0, Ferricstore.Store.LFU.initial(), 99, 123, 3})

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{xx: true})}, state)

      assert result == :ok
      assert [{^key, "new", 0, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end

    test "SET KEEPTTL preserves cold key TTL without reading the old value", %{
      state: state,
      ets: ets
    } do
      key = "set_keepttl_cold_unreadable"
      expire_at_ms = System.os_time(:millisecond) + 60_000
      :ets.insert(ets, {key, nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 99, 123, 3})

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{keepttl: true})}, state)

      assert result == :ok
      assert [{^key, "new", ^expire_at_ms, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end

    test "SET KEEPTTL does not preserve TTL from malformed cold rows", %{
      state: state,
      ets: ets
    } do
      key = "set_keepttl_bad_cold_ref"
      expire_at_ms = System.os_time(:millisecond) + 60_000

      :ets.insert(
        ets,
        {key, nil, expire_at_ms, Ferricstore.Store.LFU.initial(), 0, :pending_offset, 3}
      )

      {_new_state, result} =
        StateMachine.apply(%{}, {:set, key, "new", 0, set_opts(%{keepttl: true})}, state)

      assert result == :ok
      assert [{^key, "new", 0, _lfu, _fid, _off, 3}] = :ets.lookup(ets, key)
    end
  end

  describe "apply/3 with {:append, key, suffix}" do
    test "APPEND treats a mismatched cold offset as missing", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "append_stale_cold_offset"
      other_key = "append_other_cold_offset"

      {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
        NIF.v2_append_batch(active_file_path, [
          {other_key, "wrong-value", 0},
          {key, "right-value", 0}
        ])

      :ets.insert(
        ets,
        {key, nil, 0, Ferricstore.Store.LFU.initial(), 0, other_offset, value_size}
      )

      {_new_state, {:ok, 1}} = StateMachine.apply(%{}, {:append, key, "!"}, state)

      assert [{^key, "!", 0, _lfu, _fid, _off, 1}] = :ets.lookup(ets, key)
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :delete
  # ---------------------------------------------------------------------------

  describe "apply/3 with {:delete, key}" do
    test "removes key from ETS", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "del_me", "val", 0}, state)
      {state3, :ok} = StateMachine.apply(%{}, {:delete, "del_me"}, state2)

      assert state3.applied_count == 2
      assert [] == :ets.lookup(ets, "del_me")
    end

    test "delete nonexistent key returns :ok", %{state: state} do
      {_new_state, result} = StateMachine.apply(%{}, {:delete, "nonexistent"}, state)
      assert result == :ok
    end

    test "delete after delete is idempotent", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "k", "v", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:delete, "k"}, s1)
      {_s3, :ok} = StateMachine.apply(%{}, {:delete, "k"}, s2)
    end

    test "missing active file fails delete and keeps ETS entry", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "missing_active_delete", "val", 0}, state)
      old_entry = :ets.lookup(ets, "missing_active_delete")
      missing_state = state_with_missing_active_file(state2)

      {_new_state, result} =
        StateMachine.apply(%{}, {:delete, "missing_active_delete"}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert old_entry == :ets.lookup(ets, "missing_active_delete")
    end

    test "missing active file does not remove prob file during failed delete", %{
      state: state,
      ets: ets,
      dir: dir
    } do
      key = "missing_active_prob_delete"
      prob_dir = Path.join(dir, "prob")
      File.mkdir_p!(prob_dir)
      prob_path = Path.join(prob_dir, "#{Base.url_encode64(key, padding: false)}.cms")
      File.write!(prob_path, "cms")

      meta = :erlang.term_to_binary({:cms_meta, %{width: 1, depth: 1}})
      {state2, :ok} = StateMachine.apply(%{}, {:put, key, meta, 0}, state)
      old_entry = :ets.lookup(ets, key)
      missing_state = state_with_missing_active_file(state2)

      {_new_state, result} = StateMachine.apply(%{}, {:delete, key}, missing_state)

      assert {:error, :active_file_unavailable} = result
      assert old_entry == :ets.lookup(ets, key)
      assert File.exists?(prob_path)
    end

    test "prob sidecar cleanup fsync failure emits telemetry", %{
      state: state,
      dir: dir,
      shard_index: shard_index
    } do
      key = "prob_delete_fsync_telemetry"
      prob_dir = Path.join(dir, "prob")
      File.mkdir_p!(prob_dir)
      prob_path = Path.join(prob_dir, "#{Base.url_encode64(key, padding: false)}.cms")
      File.write!(prob_path, "cms")

      handler_id = {__MODULE__, self(), :prob_sidecar_delete_failed}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :prob, :sidecar_delete_failed],
        fn event, measurements, metadata, pid ->
          send(pid, {:prob_sidecar_delete_failed, event, measurements, metadata})
        end,
        self()
      )

      Process.put(:ferricstore_prob_fsync_dir_hook, fn
        ^prob_dir -> {:error, :eio}
        _path -> :ok
      end)

      on_exit(fn ->
        :telemetry.detach(handler_id)
        Process.delete(:ferricstore_prob_fsync_dir_hook)
      end)

      meta = :erlang.term_to_binary({:cms_meta, %{width: 1, depth: 1}})
      {state2, :ok} = StateMachine.apply(%{}, {:put, key, meta, 0}, state)
      {_state3, :ok} = StateMachine.apply(%{}, {:delete, key}, state2)

      assert_receive {:prob_sidecar_delete_failed, [:ferricstore, :prob, :sidecar_delete_failed],
                      %{count: 1},
                      %{
                        shard_index: ^shard_index,
                        path: ^prob_path,
                        reason: {:fsync_dir_failed, :prob_file_dir, :eio}
                      }}
    end

    test "append failure rolls back deleted entry in a mixed batch", %{state: state, ets: ets} do
      {state2, :ok} =
        StateMachine.apply(%{}, {:put, "delete_append_failure_keep", "old", 0}, state)

      old_entry = :ets.lookup(ets, "delete_append_failure_keep")
      file_id = 9_100_000 + :erlang.unique_integer([:positive])
      bad_active_path = Path.join(state.shard_data_path, "#{file_id}.log")
      File.mkdir_p!(bad_active_path)

      bad_state = %{state2 | active_file_id: file_id, active_file_path: bad_active_path}

      {_new_state, result} =
        StateMachine.apply(
          %{},
          {:batch,
           [
             {:put, "delete_append_failure_new", "new", 0},
             {:delete, "delete_append_failure_keep"}
           ]},
          bad_state
        )

      assert {:error, {:bitcask_append_failed, _reason}} = result
      assert old_entry == :ets.lookup(ets, "delete_append_failure_keep")
      assert [] == :ets.lookup(ets, "delete_append_failure_new")
    end
  end

  # ---------------------------------------------------------------------------
  # apply/3 with :batch
  # ---------------------------------------------------------------------------

  describe "apply/3 with {:batch, commands}" do
    test "uses raft meta system_time for TTL checks inside batch read-modify-write", %{
      state: state,
      ets: ets
    } do
      local_now = Ferricstore.HLC.now_ms()
      apply_now = local_now - 20_000
      expires_after_apply_time = apply_now + 10_000

      :ets.insert(
        ets,
        {"batch_meta_time_incr", "5", expires_after_apply_time, Ferricstore.Store.LFU.initial(),
         0, 0, byte_size("5")}
      )

      {_new_state, {:ok, [{:ok, 6}]}} =
        StateMachine.apply(
          %{system_time: apply_now},
          {:batch, [{:incr, "batch_meta_time_incr", 1}]},
          state
        )

      assert [{"batch_meta_time_incr", "6", ^expires_after_apply_time, _, _, _, _}] =
               :ets.lookup(ets, "batch_meta_time_incr")
    end

    test "processes all commands and returns results list", %{state: state, ets: ets} do
      commands = [
        {:put, "batch_a", "val_a", 0},
        {:put, "batch_b", "val_b", 0},
        {:put, "batch_c", "val_c", 0}
      ]

      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, commands}, state)

      assert results == [:ok, :ok, :ok]
      assert new_state.applied_count == 3

      # All keys in ETS (single-table format)
      assert [{"batch_a", "val_a", 0, _, _, _, _}] = :ets.lookup(ets, "batch_a")
      assert [{"batch_b", "val_b", 0, _, _, _, _}] = :ets.lookup(ets, "batch_b")
      assert [{"batch_c", "val_c", 0, _, _, _, _}] = :ets.lookup(ets, "batch_c")
    end

    test "RMW command in same batch reads prior pending large put", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      key = "batch_large_then_append"
      large = String.duplicate("L", 70_000)
      expected = large <> "!"

      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, [{:put, key, large, 0}, {:append, key, "!"}]}, state)

      assert results == [:ok, {:ok, byte_size(expected)}]
      assert new_state.applied_count == 2

      assert [{^key, nil, 0, _, 0, offset, value_size}] = :ets.lookup(ets, key)
      assert value_size == byte_size(expected)
      assert {:ok, ^expected} = NIF.v2_pread_at(active_file_path, offset)
    end

    test "probabilistic command in batch does not drop earlier pending puts", %{
      state: state,
      ets: ets,
      active_file_path: active_file_path
    } do
      commands = [
        {:put, "batch_before_prob", "keep-me", 0},
        {:cms_create, "batch_cms", 20, 4}
      ]

      {_new_state, {:ok, [:ok, :ok]}} =
        StateMachine.apply(%{}, {:batch, commands}, state)

      value_size = byte_size("keep-me")

      assert [{"batch_before_prob", "keep-me", 0, _, 0, _offset, ^value_size}] =
               :ets.lookup(ets, "batch_before_prob")

      assert {:ok, records} = NIF.v2_scan_file(active_file_path)
      assert Enum.any?(records, &match?({"batch_before_prob", _off, ^value_size, 0, false}, &1))
      assert Enum.any?(records, &match?({"batch_cms", _off, _size, 0, false}, &1))
    end

    test "CMS merge resolves replicated source keys through local shard paths" do
      root = Path.join(System.tmp_dir!(), "sm_cms_merge_#{System.unique_integer([:positive])}")
      instance_name = :"sm_cms_merge_#{System.unique_integer([:positive])}"
      ctx = FerricStore.Instance.build(instance_name, data_dir: root, shard_count: 4)

      src_key = key_for_shard(ctx, "cms_src", 1)
      dst_key = key_for_shard(ctx, "cms_dst", 0)
      src_dir = Path.join(Ferricstore.DataDir.shard_data_path(root, 1), "prob")
      dst_shard_path = Ferricstore.DataDir.shard_data_path(root, 0)
      dst_dir = Path.join(dst_shard_path, "prob")
      src_path = prob_test_path(src_dir, src_key, "cms")
      dst_path = prob_test_path(dst_dir, dst_key, "cms")

      ets = :ets.new(:"sm_cms_merge_#{System.unique_integer([:positive])}", [:set, :public])

      try do
        File.mkdir_p!(src_dir)
        File.mkdir_p!(dst_shard_path)
        File.touch!(Path.join(dst_shard_path, "00000.log"))

        assert {:ok, _} = NIF.cms_file_create(src_path, 64, 4)
        assert {:ok, _} = NIF.cms_file_incrby(src_path, [{"element", 9}])

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: dst_shard_path,
            active_file_id: 0,
            active_file_path: Path.join(dst_shard_path, "00000.log"),
            ets: ets,
            instance_ctx: ctx,
            instance_name: instance_name
          })

        apply_result =
          StateMachine.apply(
            %{},
            {:cms_merge, dst_key, [src_key], [1], %{width: 64, depth: 4}},
            state
          )

        assert :ok = apply_result_value(apply_result)

        assert {:ok, [9]} = NIF.cms_file_query(dst_path, ["element"])
      after
        :ets.delete(ets)
        FerricStore.Instance.cleanup(instance_name)
        File.rm_rf!(root)
      end
    end

    test "probabilistic create failures in batch do not publish metadata", %{
      state: state,
      ets: ets,
      dir: dir
    } do
      state = %{state | shard_index: 0}
      prob_dir = Path.join(dir, "prob")
      File.write!(prob_dir, "not-a-directory")

      commands = [
        {"batch_bloom_create_fail",
         {:bloom_create, "batch_bloom_create_fail", 9586, 7,
          {:bloom_meta, %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}}},
        {"batch_cms_create_fail", {:cms_create, "batch_cms_create_fail", 100, 5}},
        {"batch_cuckoo_create_fail", {:cuckoo_create, "batch_cuckoo_create_fail", 1024, 4}},
        {"batch_topk_create_fail", {:topk_create, "batch_topk_create_fail", 10, 8, 7, 0.9}}
      ]

      {_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, Enum.map(commands, &elem(&1, 1))}, state)

      assert Enum.all?(results, &match?({:error, _}, &1))

      for {key, _cmd} <- commands do
        assert [] == :ets.lookup(ets, key)
      end
    end

    test "mixed put and delete batch", %{state: state, ets: ets} do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "mix_a", "va", 0}, state)

      commands = [
        {:put, "mix_b", "vb", 0},
        {:delete, "mix_a"},
        {:put, "mix_c", "vc", 0}
      ]

      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, commands}, state2)

      assert results == [:ok, :ok, :ok]
      assert new_state.applied_count == 4

      assert [] == :ets.lookup(ets, "mix_a")
      assert [{"mix_b", "vb", 0, _, _, _, _}] = :ets.lookup(ets, "mix_b")
      assert [{"mix_c", "vc", 0, _, _, _, _}] = :ets.lookup(ets, "mix_c")
    end

    test "mixed put and delete batch emits one Bitcask append for all ops", %{
      state: state
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "batched_delete_seed", "old", 0}, state)
      handler_id = {:state_machine_mixed_delete_batch_telemetry, self(), make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :bitcask, :append],
          fn _event, measurements, metadata, test_pid ->
            send(test_pid, {:bitcask_append, measurements, metadata})
          end,
          self()
        )

      try do
        commands = [
          {:put, "batched_delete_put_a", "a", 0},
          {:delete, "batched_delete_seed"},
          {:put, "batched_delete_put_b", "b", 0}
        ]

        {_new_state, {:ok, [:ok, :ok, :ok]}} =
          StateMachine.apply(%{}, {:batch, commands}, state2)

        assert_receive {:bitcask_append, measurements, %{status: :ok}}, 500
        assert measurements.batch_size == 3
        assert measurements.delete_count == 1

        assert measurements.batch_bytes ==
                 byte_size("batched_delete_put_a") + 1 + byte_size("batched_delete_seed") +
                   byte_size("batched_delete_put_b") + 1
      after
        :telemetry.detach(handler_id)
      end
    end

    test "empty batch returns empty results", %{state: state} do
      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, []}, state)

      assert results == []
      assert new_state.applied_count == 0
    end

    test "large batch (100 commands)", %{state: state} do
      commands = for i <- 1..100, do: {:put, "batch_k#{i}", "batch_v#{i}", 0}

      {new_state, {:ok, results}} =
        StateMachine.apply(%{}, {:batch, commands}, state)

      assert length(results) == 100
      assert Enum.all?(results, &(&1 == :ok))
      assert new_state.applied_count == 100
    end
  end

  describe "pending batch location validation" do
    test "rejects a result count that does not match the batch" do
      batch = [
        {:put, "k1", "v1", 0},
        {:delete, "k2", nil}
      ]

      assert {:error, {:bitcask_append_result_mismatch, {:length_mismatch, 2, 1}}} =
               StateMachine.__validate_pending_locations__(batch, [{:put, 0, 2}])
    end

    test "rejects out-of-order operation tags" do
      batch = [
        {:put, "k1", "v1", 0},
        {:delete, "k2", nil}
      ]

      locations = [
        {:delete, 0, 28},
        {:put, 28, 2}
      ]

      assert {:error, {:bitcask_append_result_mismatch, {:op_mismatch, 0, :put, :delete}}} =
               StateMachine.__validate_pending_locations__(batch, locations)
    end

    test "accepts matching put and delete result tags in order" do
      batch = [
        {:put, "k1", "v1", 0},
        {:delete, "k2", nil}
      ]

      locations = [
        {:put, 0, 2},
        {:delete, 30, 28}
      ]

      assert :ok = StateMachine.__validate_pending_locations__(batch, locations)
    end
  end

  describe "pending batch location application" do
    test "does not attach an old append location to a newer pending value", %{
      state: state,
      ets: ets
    } do
      key = "stale-location-key"

      :ets.insert(
        ets,
        {key, "new", 456, Ferricstore.Store.LFU.initial(), :pending, 0, byte_size("new")}
      )

      StateMachine.__apply_pending_locations_for_test__(
        state,
        7,
        [{:put, key, "old", 123}],
        [{:put, 42, byte_size("old")}]
      )

      assert [{^key, "new", 456, _lfu, :pending, 0, 3}] = :ets.lookup(ets, key)
    end

    test "attaches append location when the pending value still matches", %{
      state: state,
      ets: ets
    } do
      key = "matching-location-key"

      :ets.insert(
        ets,
        {key, "new", 456, Ferricstore.Store.LFU.initial(), :pending, 0, byte_size("new")}
      )

      StateMachine.__apply_pending_locations_for_test__(
        state,
        7,
        [{:put, key, "new", 456}],
        [{:put, 42, byte_size("new")}]
      )

      assert [{^key, "new", 456, _lfu, 7, 42, 3}] = :ets.lookup(ets, key)
    end
  end

  describe "apply/3 probabilistic native failures" do
    test "create reports prob directory parent fsync failure", %{state: state, ets: ets, dir: dir} do
      key = "bloom_create_prob_dir_fsync_fail"

      Process.put(:ferricstore_prob_fsync_dir_hook, fn ^dir ->
        {:error, :eio}
      end)

      try do
        {_state2, result} =
          StateMachine.apply(
            %{},
            {:bloom_create, key, 9586, 7,
             {:bloom_meta, %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}},
            state
          )

        assert {:error, {:fsync_dir_failed, :create_prob_dir, :eio}} = result
        assert [] == :ets.lookup(ets, key)
      after
        Process.delete(:ferricstore_prob_fsync_dir_hook)
      end
    end

    test "create failures do not publish metadata", %{state: state, ets: ets, dir: dir} do
      state = %{state | shard_index: 0}
      prob_dir = Path.join(dir, "prob")
      File.write!(prob_dir, "not-a-directory")

      commands = [
        {"bloom_create_fail",
         {:bloom_create, "bloom_create_fail", 9586, 7,
          {:bloom_meta, %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}}},
        {"cms_create_fail", {:cms_create, "cms_create_fail", 100, 5}},
        {"cuckoo_create_fail", {:cuckoo_create, "cuckoo_create_fail", 1024, 4}},
        {"topk_create_fail", {:topk_create, "topk_create_fail", 10, 8, 7, 0.9}}
      ]

      Enum.reduce(commands, state, fn {key, command}, acc_state ->
        {next_state, result} = StateMachine.apply(%{}, command, acc_state)

        assert {:error, _reason} = result
        assert [] == :ets.lookup(ets, key)

        next_state
      end)
    end

    test "auto-create failures do not publish metadata", %{state: state, ets: ets, dir: dir} do
      state = %{state | shard_index: 0}
      prob_dir = Path.join(dir, "prob")
      File.write!(prob_dir, "not-a-directory")

      commands = [
        {"bloom_add_fail",
         {:bloom_add, "bloom_add_fail", "item",
          %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}},
        {"bloom_madd_fail",
         {:bloom_madd, "bloom_madd_fail", ["item"],
          %{num_bits: 9586, num_hashes: 7, capacity: 1000, error_rate: 0.01}}},
        {"cms_merge_create_fail",
         {:cms_merge, "cms_merge_create_fail", [], [], %{width: 100, depth: 5}}},
        {"cuckoo_add_fail",
         {:cuckoo_add, "cuckoo_add_fail", "item", %{capacity: 1024, bucket_size: 4}}},
        {"cuckoo_addnx_fail",
         {:cuckoo_addnx, "cuckoo_addnx_fail", "item", %{capacity: 1024, bucket_size: 4}}}
      ]

      Enum.reduce(commands, state, fn {key, command}, acc_state ->
        {next_state, result} = StateMachine.apply(%{}, command, acc_state)

        assert {:error, _reason} = result
        assert [] == :ets.lookup(ets, key)

        next_state
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # state_enter/2
  # ---------------------------------------------------------------------------

  describe "state_enter/2" do
    test "returns empty effects for all roles", %{state: state} do
      assert StateMachine.state_enter(:leader, state) == []
      assert StateMachine.state_enter(:follower, state) == []
      assert StateMachine.state_enter(:candidate, state) == []
      assert StateMachine.state_enter(:await_condition, state) == []
      assert StateMachine.state_enter(:delete_and_terminate, state) == []
      assert StateMachine.state_enter(:receive_snapshot, state) == []
    end
  end

  # ---------------------------------------------------------------------------
  # tick/2
  # ---------------------------------------------------------------------------

  describe "tick/2" do
    test "returns empty effects", %{state: state} do
      assert StateMachine.tick(System.os_time(:millisecond), state) == []
    end
  end

  # ---------------------------------------------------------------------------
  # init_aux/1
  # ---------------------------------------------------------------------------

  describe "init_aux/1" do
    test "returns initial aux state with empty hot_keys" do
      aux = StateMachine.init_aux(:test_name)
      assert aux == %{hot_keys: %{}}
    end
  end

  # ---------------------------------------------------------------------------
  # handle_aux/5
  # ---------------------------------------------------------------------------

  describe "handle_aux/5" do
    test "key_written increments hot key counter" do
      aux = %{hot_keys: %{}}
      int_state = %{some: :internal_state}

      {:no_reply, new_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 1
      assert returned_state == int_state
    end

    test "key_written accumulates counts" do
      aux = %{hot_keys: %{"hot_key" => 5}}
      int_state = %{}

      {:no_reply, new_aux, _} =
        StateMachine.handle_aux(:leader, :cast, {:key_written, "hot_key"}, aux, int_state)

      assert new_aux.hot_keys["hot_key"] == 6
    end

    test "unknown command returns aux unchanged" do
      aux = %{hot_keys: %{}}
      int_state = %{}

      {:no_reply, returned_aux, returned_state} =
        StateMachine.handle_aux(:leader, :cast, :unknown, aux, int_state)

      assert returned_aux == aux
      assert returned_state == int_state
    end
  end

  # ---------------------------------------------------------------------------
  # overview/1
  # ---------------------------------------------------------------------------

  describe "overview/1" do
    test "returns shard_index, keydir_size, and applied_count", %{
      state: state,
      shard_index: shard_index
    } do
      {state2, :ok} = StateMachine.apply(%{}, {:put, "ov_k", "ov_v", 0}, state)

      overview = StateMachine.overview(state2)
      assert overview.shard_index == shard_index
      assert overview.keydir_size == 1
      assert overview.applied_count == 1
    end

    test "keydir_size reflects ETS size", %{state: state} do
      {s1, :ok} = StateMachine.apply(%{}, {:put, "a", "1", 0}, state)
      {s2, :ok} = StateMachine.apply(%{}, {:put, "b", "2", 0}, s1)
      {s3, :ok} = StateMachine.apply(%{}, {:put, "c", "3", 0}, s2)

      assert StateMachine.overview(s3).keydir_size == 3
    end
  end

  # ---------------------------------------------------------------------------
  # release_cursor for Raft log compaction (spec 2E.5)
  # ---------------------------------------------------------------------------

  describe "release_cursor log compaction" do
    test "init/1 stores release_cursor_interval from app env", %{store: _store, ets: ets} do
      state = init_state_for_release_cursor(ets)

      assert state.release_cursor_interval ==
               Application.fetch_env!(:ferricstore, :release_cursor_interval)
    end

    test "init/1 accepts custom release_cursor_interval", %{store: _store, ets: ets} do
      state = init_state_for_release_cursor(ets, release_cursor_interval: 500)

      assert state.release_cursor_interval == 500
    end

    test "apply does not inherit stale pending state from a previous crashed apply", %{
      store: _store,
      ets: ets
    } do
      state = init_state_for_release_cursor(ets)

      stale_state = %{
        state
        | active_file_id: 99,
          active_file_path: Path.join(state.shard_data_path, "00099.log"),
          active_file_size: 12_345,
          file_stats: %{99 => %{live_bytes: 1, dead_bytes: 0}}
      }

      Process.put(:sm_apply_state, %{pending_state: stale_state})

      try do
        assert {new_state, _result, _effects} =
                 StateMachine.apply(%{index: 1}, {:getdel, "missing_after_stale"}, state)

        assert new_state.active_file_id == state.active_file_id
        assert new_state.active_file_path == state.active_file_path
        assert new_state.active_file_size == state.active_file_size
        assert new_state.file_stats == state.file_stats
      after
        Process.delete(:sm_apply_state)
      end
    end

    test "cross-shard control apply does not inherit stale pending state", %{
      store: _store,
      ets: ets
    } do
      state = init_state_for_release_cursor(ets)

      stale_state = %{
        state
        | active_file_id: 99,
          active_file_path: Path.join(state.shard_data_path, "00099.log"),
          active_file_size: 12_345,
          file_stats: %{99 => %{live_bytes: 1, dead_bytes: 0}}
      }

      Process.put(:sm_apply_state, %{pending_state: stale_state})

      try do
        assert {new_state, _result, _effects} =
                 StateMachine.apply(%{index: 1}, {:cross_shard_intent, make_ref(), %{}}, state)

        assert new_state.active_file_id == state.active_file_id
        assert new_state.active_file_path == state.active_file_path
        assert new_state.active_file_size == state.active_file_size
        assert new_state.file_stats == state.file_stats
      after
        Process.delete(:sm_apply_state)
      end
    end

    test "init/1 caches expanded paths for release cursor checkpoint checks", %{
      store: _store,
      ets: ets
    } do
      state = init_state_for_release_cursor(ets)

      assert state.data_dir_expanded == Path.expand(state.data_dir)
      assert state.shard_data_path_expanded == Path.expand(state.shard_data_path)
    end

    test "checkpoint path ownership check does not expand paths during release cursor checks" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/raft/state_machine.ex", __DIR__))

      [_match, body] =
        Regex.run(
          ~r/(defp instance_data_path\?\(.*?)(?=\n  defp initial_file_stats)/s,
          source
        )

      refute body =~ "Path.expand",
             "release_cursor checkpoint checks must use paths normalized at state-machine init"
    end

    test "no release_cursor emitted before interval is reached", %{store: _store, ets: ets} do
      state = init_state_for_release_cursor(ets, release_cursor_interval: 5)

      # Apply 4 commands (below interval of 5) -- none should emit release_cursor
      result =
        Enum.reduce(1..4, state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, effects} =
            StateMachine.apply(meta, {:put, "rc_key_#{i}", "v#{i}", 0}, acc)

          if Enum.any?(effects, &match?({:release_cursor, _}, &1)) do
            flunk("release_cursor emitted before interval reached at apply #{i}")
          end

          new_state
        end)

      assert result.applied_count == 4
    end

    test "release_cursor emitted exactly at interval boundary for read-only command", %{
      store: _store,
      ets: ets
    } do
      interval = 5

      state =
        init_state_for_release_cursor(ets, release_cursor_interval: interval)

      # Apply (interval - 1) commands without release_cursor
      state_before =
        Enum.reduce(1..(interval - 1), state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, nil}, _effects} =
            StateMachine.apply(meta, {:getdel, "rc_#{i}"}, acc)

          new_state
        end)

      assert state_before.applied_count == interval - 1

      # The N-th apply (index = interval) should emit release_cursor
      meta = %{index: interval, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, _, nil}, effects} =
        StateMachine.apply(meta, {:getdel, "rc_#{interval}"}, state_before)

      assert new_state.applied_count == interval

      # Verify the recovery checkpoint and release_cursor promotion effects.
      checkpoint_effect = Enum.find(effects, &match?({:checkpoint, _, _}, &1))
      assert {:checkpoint, ^interval, checkpoint_state} = checkpoint_effect
      assert checkpoint_state.shard_index == 0
      assert checkpoint_state.applied_count == interval

      cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
      assert {:release_cursor, ra_index} = cursor_effect
      assert ra_index == interval

      assert Ferricstore.Raft.ReplaySafeIndexWriter.durable?(
               new_state.instance_ctx,
               new_state.shard_index,
               new_state.shard_data_path,
               interval
             )
    end

    test "release_cursor waits while the shard has uncheckpointed bitcask data", %{
      state: state,
      shard_index: shard_index
    } do
      checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
      disk_pressure = :atomics.new(shard_index + 1, signed: false)
      last_applied_index = :atomics.new(shard_index + 1, signed: false)
      last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

      state = %{
        state
        | release_cursor_interval: 1,
          instance_ctx: %{
            checkpoint_flags: checkpoint_flags,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            disk_pressure: disk_pressure,
            hot_cache_max_value_size: 64
          }
      }

      meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, 1, :ok}, effects} =
        StateMachine.apply(meta, {:put, "dirty_rc_key", "dirty_rc_value", 0}, state)

      assert new_state.applied_count == 1
      assert :atomics.get(checkpoint_flags, shard_index + 1) == 1
      assert :atomics.get(last_applied_index, shard_index + 1) == 1
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0

      refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
             "Raft cursor must not advance past data that still needs a Bitcask checkpoint"
    end

    test "release_cursor block metric records consecutive blocked applies", %{
      ets: ets
    } do
      shard0 = 0
      shard1 = 1
      root = Path.join(System.tmp_dir!(), "sm_blocked_rc_#{System.unique_integer([:positive])}")
      shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
      bad_active_path = Path.join(shard1_path, "active_as_dir.log")
      ets1 = :ets.new(:"sm_blocked_rc_#{System.unique_integer([:positive])}", [:set, :public])

      File.mkdir_p!(bad_active_path)
      Ferricstore.Store.ActiveFile.init(2)

      checkpoint_flags = :atomics.new(2, signed: false)
      checkpoint_in_flight = :atomics.new(2, signed: false)
      disk_pressure = :atomics.new(2, signed: false)
      last_applied_index = :atomics.new(2, signed: false)
      last_released_cursor_index = :atomics.new(2, signed: false)
      release_cursor_blocked_apply_count = :atomics.new(2, signed: false)

      state =
        init_state_for_release_cursor(ets,
          shard_index: shard0,
          release_cursor_interval: 1
        )

      instance_ctx = %{
        name: :"sm_blocked_rc_#{System.unique_integer([:positive])}",
        data_dir: root,
        shard_count: 2,
        keydir_refs: List.to_tuple([ets, ets1]),
        keydir_binary_bytes: :atomics.new(2, signed: false),
        checkpoint_flags: checkpoint_flags,
        checkpoint_in_flight: checkpoint_in_flight,
        disk_pressure: disk_pressure,
        last_applied_index: last_applied_index,
        last_released_cursor_index: last_released_cursor_index,
        release_cursor_blocked_apply_count: release_cursor_blocked_apply_count,
        hot_cache_max_value_size: 64
      }

      Ferricstore.Store.ActiveFile.publish(
        instance_ctx,
        shard0,
        state.active_file_id,
        state.active_file_path,
        state.shard_data_path
      )

      Ferricstore.Store.ActiveFile.publish(instance_ctx, shard1, 0, bad_active_path, shard1_path)
      state = %{state | instance_ctx: instance_ctx}

      # The old cold location intentionally points at a missing retired file.
      # If shard1 fails after shard0 accepted its write, compensation must fail
      # instead of releasing Ra's cursor past divergent Bitcask state.
      key = "blocked_cold_original"
      :ets.insert(ets, {key, nil, 0, 0, 1, 0, 5})

      try do
        {state, {:applied_at, 1, {:error, {:cross_shard_compensation_failed, _reason}}}, effects1} =
          StateMachine.apply(
            %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
            {:cross_shard_tx,
             [
               {shard0, [{"SET", [key, "new"]}], nil},
               {shard1, [{"SET", ["blocked_remote_fail", "value"]}], nil}
             ]},
            state
          )

        assert :atomics.get(release_cursor_blocked_apply_count, shard0 + 1) == 1
        refute Enum.any?(effects1, &match?({:release_cursor, _}, &1))

        {_state, {:applied_at, 2, nil}, _effects2} =
          StateMachine.apply(
            %{index: 2, term: 1, system_time: System.os_time(:millisecond)},
            {:getdel, "unblocked_missing"},
            state
          )

        assert :atomics.get(release_cursor_blocked_apply_count, shard0 + 1) == 0
      after
        :ets.delete(ets1)
        Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
        File.rm_rf!(root)
      end
    end

    test "cross-shard SET dirties checkpoint state before release_cursor", %{
      ets: ets,
      shard_index: shard_index
    } do
      checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
      checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)
      disk_pressure = :atomics.new(shard_index + 1, signed: false)
      last_applied_index = :atomics.new(shard_index + 1, signed: false)
      last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

      state =
        init_state_for_release_cursor(ets,
          shard_index: shard_index,
          release_cursor_interval: 1
        )

      state = %{
        state
        | instance_ctx: %{
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight,
            disk_pressure: disk_pressure,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            hot_cache_max_value_size: 64,
            data_dir: state.data_dir
          }
      }

      meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, 1, %{^shard_index => [:ok]}}, effects} =
        StateMachine.apply(
          meta,
          {:cross_shard_tx, [{shard_index, [{"SET", ["cross_cursor_dirty", "value"]}], nil}]},
          state
        )

      assert :atomics.get(checkpoint_flags, shard_index + 1) == 1
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0

      refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
             "cross-shard writes append to Bitcask and must not release Ra log before checkpoint fsync"
    end

    test "remote-only cross-shard SET blocks coordinator release_cursor until remote checkpoint is clean",
         %{ets: ets} do
      shard0 = 0
      shard1 = 1
      root = Path.join(System.tmp_dir!(), "sm_remote_rc_#{System.unique_integer([:positive])}")
      shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
      shard1_file = Path.join(shard1_path, "00000.log")
      ets1 = :ets.new(:"sm_remote_rc_#{System.unique_integer([:positive])}", [:set, :public])

      File.mkdir_p!(shard1_path)
      File.touch!(shard1_file)
      Ferricstore.Store.ActiveFile.init(2)

      checkpoint_flags = :atomics.new(2, signed: false)
      checkpoint_in_flight = :atomics.new(2, signed: false)
      disk_pressure = :atomics.new(2, signed: false)
      last_applied_index = :atomics.new(2, signed: false)
      last_released_cursor_index = :atomics.new(2, signed: false)
      pending_release_cursor_checkpoint_count = :atomics.new(2, signed: false)
      replay_safe_index = :atomics.new(2, signed: false)
      flow_lmdb_replay_safe_index = :atomics.new(2, signed: false)

      :atomics.put(replay_safe_index, shard0 + 1, 2)
      :atomics.put(flow_lmdb_replay_safe_index, shard0 + 1, 2)

      state =
        init_state_for_release_cursor(ets,
          shard_index: shard0,
          release_cursor_interval: 1
        )

      instance_ctx = %{
        name: :"sm_remote_rc_#{System.unique_integer([:positive])}",
        data_dir: root,
        shard_count: 2,
        keydir_refs: List.to_tuple([ets, ets1]),
        keydir_binary_bytes: :atomics.new(2, signed: false),
        checkpoint_flags: checkpoint_flags,
        checkpoint_in_flight: checkpoint_in_flight,
        disk_pressure: disk_pressure,
        last_applied_index: last_applied_index,
        last_released_cursor_index: last_released_cursor_index,
        pending_release_cursor_checkpoint_count: pending_release_cursor_checkpoint_count,
        replay_safe_index: replay_safe_index,
        flow_lmdb_replay_safe_index: flow_lmdb_replay_safe_index,
        hot_cache_max_value_size: 64
      }

      Ferricstore.Store.ActiveFile.publish(
        instance_ctx,
        shard0,
        state.active_file_id,
        state.active_file_path,
        state.shard_data_path
      )

      Ferricstore.Store.ActiveFile.publish(instance_ctx, shard1, 0, shard1_file, shard1_path)

      state = %{state | instance_ctx: instance_ctx}

      try do
        {state, {:applied_at, 1, %{^shard1 => [:ok]}}, effects1} =
          StateMachine.apply(
            %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
            {:cross_shard_tx, [{shard1, [{"SET", ["remote_cursor_dirty", "value"]}], nil}]},
            state
          )

        assert :atomics.get(checkpoint_flags, shard1 + 1) == 1
        assert :atomics.get(last_released_cursor_index, shard0 + 1) == 0
        assert :atomics.get(pending_release_cursor_checkpoint_count, shard0 + 1) == 1

        refute Enum.any?(effects1, &match?({:release_cursor, 1}, &1)),
               "coordinator Ra log must wait for remote Bitcask checkpoint durability"

        :atomics.put(checkpoint_flags, shard1 + 1, 0)
        :atomics.put(checkpoint_in_flight, shard1 + 1, 0)

        {_state, {:applied_at, 2, nil}, effects2} =
          StateMachine.apply(
            %{index: 2, term: 1, system_time: System.os_time(:millisecond)},
            {:getdel, "remote_cursor_missing"},
            state
          )

        assert Enum.any?(effects2, &match?({:release_cursor, 2}, &1))
        assert :atomics.get(last_released_cursor_index, shard0 + 1) == 2
        assert :atomics.get(pending_release_cursor_checkpoint_count, shard0 + 1) == 0
      after
        :ets.delete(ets1)
        Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
        File.rm_rf!(root)
      end
    end

    test "remote-only cross-shard SET rotates the remote active file when it grows past threshold",
         %{ets: ets} do
      shard0 = 0
      shard1 = 1

      root =
        Path.join(System.tmp_dir!(), "sm_remote_rotate_#{System.unique_integer([:positive])}")

      shard1_path = Ferricstore.DataDir.shard_data_path(root, shard1)
      shard1_file = Path.join(shard1_path, "00000.log")
      ets1 = :ets.new(:"sm_remote_rotate_#{System.unique_integer([:positive])}", [:set, :public])

      File.mkdir_p!(shard1_path)
      File.touch!(shard1_file)
      Ferricstore.Store.ActiveFile.init(2)

      state =
        init_state_for_release_cursor(ets,
          shard_index: shard0,
          release_cursor_interval: 1
        )

      instance_ctx = %{
        name: :"sm_remote_rotate_#{System.unique_integer([:positive])}",
        data_dir: root,
        shard_count: 2,
        keydir_refs: List.to_tuple([ets, ets1]),
        keydir_binary_bytes: :atomics.new(2, signed: false),
        checkpoint_flags: :atomics.new(2, signed: false),
        checkpoint_in_flight: :atomics.new(2, signed: false),
        disk_pressure: :atomics.new(2, signed: false),
        last_applied_index: :atomics.new(2, signed: false),
        last_released_cursor_index: :atomics.new(2, signed: false),
        hot_cache_max_value_size: 64,
        max_active_file_size: 80
      }

      Ferricstore.Store.ActiveFile.publish(
        instance_ctx,
        shard0,
        state.active_file_id,
        state.active_file_path,
        state.shard_data_path
      )

      Ferricstore.Store.ActiveFile.publish(instance_ctx, shard1, 0, shard1_file, shard1_path)

      state = %{state | instance_ctx: instance_ctx}
      value = :binary.copy("R", 120)

      try do
        {_state, {:applied_at, 1, %{^shard1 => [:ok]}}, _effects} =
          StateMachine.apply(
            %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
            {:cross_shard_tx, [{shard1, [{"SET", ["remote_rotate_key", value]}], nil}]},
            state
          )

        assert {1, rotated_path, ^shard1_path} =
                 Ferricstore.Store.ActiveFile.get(instance_ctx, shard1)

        assert rotated_path == Path.join(shard1_path, "00001.log")
        assert File.exists?(rotated_path)
      after
        :ets.delete(ets1)
        Ferricstore.Store.ActiveFile.cleanup_instance(instance_ctx)
        File.rm_rf!(root)
      end
    end

    test "release_cursor promotes prior checkpoint when shard was clean before next write", %{
      state: state,
      shard_index: shard_index
    } do
      checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
      checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)
      last_applied_index = :atomics.new(shard_index + 1, signed: false)
      last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

      state = %{
        state
        | release_cursor_interval: 2,
          instance_ctx:
            release_cursor_instance_ctx(shard_index, 2,
              checkpoint_flags: checkpoint_flags,
              checkpoint_in_flight: checkpoint_in_flight,
              last_applied_index: last_applied_index,
              last_released_cursor_index: last_released_cursor_index
            )
      }

      {state, {:applied_at, 1, :ok}, effects1} =
        StateMachine.apply(
          %{index: 1, term: 1, system_time: System.os_time(:millisecond)},
          {:put, "cursor_starve_1", "value", 0},
          state
        )

      refute Enum.any?(effects1, &match?({:release_cursor, _}, &1))

      {state, {:applied_at, 2, :ok}, effects2} =
        StateMachine.apply(
          %{index: 2, term: 1, system_time: System.os_time(:millisecond)},
          {:put, "cursor_starve_2", "value", 0},
          state
        )

      assert Enum.any?(effects2, &match?({:checkpoint, 2, _}, &1))
      refute Enum.any?(effects2, &match?({:release_cursor, _}, &1))

      :atomics.put(checkpoint_flags, shard_index + 1, 0)
      :atomics.put(checkpoint_in_flight, shard_index + 1, 0)

      {_state, {:applied_at, 3, :ok}, effects3} =
        StateMachine.apply(
          %{index: 3, term: 1, system_time: System.os_time(:millisecond)},
          {:put, "cursor_starve_3", "value", 0},
          state
        )

      assert Enum.any?(effects3, &match?({:release_cursor, 2}, &1))
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 2
    end

    test "release_cursor records last released cursor index when emitted", %{
      state: state,
      shard_index: shard_index
    } do
      last_applied_index = :atomics.new(shard_index + 1, signed: false)
      last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

      state = %{
        state
        | release_cursor_interval: 1,
          instance_ctx:
            release_cursor_instance_ctx(shard_index, 42,
              last_applied_index: last_applied_index,
              last_released_cursor_index: last_released_cursor_index
            )
      }

      meta = %{index: 42, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, 42, nil}, effects} =
        StateMachine.apply(meta, {:getdel, "released_cursor_metric_missing"}, state)

      assert Enum.any?(effects, &match?({:release_cursor, 42}, &1))
      assert :atomics.get(last_applied_index, shard_index + 1) == 42
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 42
    end

    test "release_cursor tolerates legacy recovered state without pending cursor fields", %{
      state: state,
      shard_index: shard_index
    } do
      last_applied_index = :atomics.new(shard_index + 1, signed: false)
      last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

      state =
        state
        |> Map.merge(%{
          release_cursor_interval: 1,
          instance_ctx:
            release_cursor_instance_ctx(shard_index, 44,
              last_applied_index: last_applied_index,
              last_released_cursor_index: last_released_cursor_index
            )
        })
        |> Map.drop([
          :pending_release_cursor_index,
          :pending_replay_safe_marker_index,
          :pending_release_cursor_checkpoint_indices
        ])

      meta = %{index: 44, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, 44, nil}, effects} =
        StateMachine.apply(meta, {:getdel, "legacy_release_cursor_missing"}, state)

      assert Enum.any?(effects, &match?({:release_cursor, 44}, &1))
      assert :atomics.get(last_applied_index, shard_index + 1) == 44
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 44
    end

    test "release_cursor is not emitted when replay-safe marker cannot persist", %{
      state: state,
      shard_index: shard_index
    } do
      last_applied_index = :atomics.new(shard_index + 1, signed: false)
      last_released_cursor_index = :atomics.new(shard_index + 1, signed: false)

      invalid_marker_dir =
        Path.join(
          System.tmp_dir!(),
          "replay_safe_marker_file_#{System.unique_integer([:positive])}"
        )

      File.write!(invalid_marker_dir, "not a directory")
      on_exit(fn -> File.rm(invalid_marker_dir) end)

      state = %{
        state
        | release_cursor_interval: 1,
          shard_data_path: invalid_marker_dir,
          instance_ctx: %{
            checkpoint_flags: :atomics.new(shard_index + 1, signed: false),
            checkpoint_in_flight: :atomics.new(shard_index + 1, signed: false),
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index
          }
      }

      meta = %{index: 43, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, 43, nil}, effects} =
        StateMachine.apply(meta, {:getdel, "released_cursor_persist_failure"}, state)

      refute Enum.any?(effects, &match?({:release_cursor, 43}, &1))
      refute Enum.any?(effects, &match?({:checkpoint, 43, _}, &1))
      assert :atomics.get(last_applied_index, shard_index + 1) == 43
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0
    end

    test "release cursor poke releases a pending durable marker without user write", %{
      state: state,
      shard_index: shard_index
    } do
      marker_index = 88
      poke_index = 89
      atomics_size = shard_index + 1
      checkpoint_flags = :atomics.new(atomics_size, signed: false)
      checkpoint_in_flight = :atomics.new(atomics_size, signed: false)
      last_applied_index = :atomics.new(atomics_size, signed: false)
      last_released_cursor_index = :atomics.new(atomics_size, signed: false)
      replay_safe_index = :atomics.new(atomics_size, signed: false)
      flow_lmdb_replay_safe_index = :atomics.new(atomics_size, signed: false)

      :atomics.put(replay_safe_index, shard_index + 1, marker_index)
      :atomics.put(flow_lmdb_replay_safe_index, shard_index + 1, marker_index)

      state = %{
        state
        | applied_count: 10,
          release_cursor_interval: 1,
          pending_release_cursor_index: marker_index,
          pending_replay_safe_marker_index: marker_index,
          pending_release_cursor_checkpoint_indices: MapSet.new(),
          instance_ctx: %{
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            replay_safe_index: replay_safe_index,
            flow_lmdb_replay_safe_index: flow_lmdb_replay_safe_index
          }
      }

      {new_state, {:applied_at, ^poke_index, :ok}, effects} =
        StateMachine.apply(
          %{index: poke_index, term: 1, system_time: System.os_time(:millisecond)},
          {:async, node(), {:release_cursor_poke, marker_index}},
          state
        )

      assert Enum.any?(effects, &match?({:release_cursor, ^marker_index}, &1))
      assert new_state.applied_count == 10
      assert :atomics.get(last_applied_index, shard_index + 1) == poke_index
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == marker_index
    end

    test "release cursor waits for LMDB replay-safe marker when Flow LMDB is enabled", %{
      state: state,
      shard_index: shard_index
    } do
      marker_index = 91
      atomics_size = shard_index + 1
      checkpoint_flags = :atomics.new(atomics_size, signed: false)
      checkpoint_in_flight = :atomics.new(atomics_size, signed: false)
      last_applied_index = :atomics.new(atomics_size, signed: false)
      last_released_cursor_index = :atomics.new(atomics_size, signed: false)
      replay_safe_index = :atomics.new(atomics_size, signed: false)
      flow_lmdb_replay_safe_index = :atomics.new(atomics_size, signed: false)

      :atomics.put(replay_safe_index, shard_index + 1, marker_index)

      state = %{
        state
        | applied_count: 10,
          release_cursor_interval: 1,
          pending_release_cursor_index: marker_index,
          pending_replay_safe_marker_index: marker_index,
          pending_release_cursor_checkpoint_indices: MapSet.new(),
          instance_ctx: %{
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            replay_safe_index: replay_safe_index,
            flow_lmdb_replay_safe_index: flow_lmdb_replay_safe_index
          }
      }

      {_new_state, {:applied_at, 92, :ok}, effects} =
        StateMachine.apply(
          %{index: 92, term: 1, system_time: System.os_time(:millisecond)},
          {:async, node(), {:release_cursor_poke, marker_index}},
          state
        )

      refute Enum.any?(effects, &match?({:release_cursor, ^marker_index}, &1))
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == 0
    end

    test "batched release cursor poke does not advance release interval", %{
      state: state,
      shard_index: shard_index
    } do
      marker_index = 88
      poke_index = 89
      atomics_size = shard_index + 1
      checkpoint_flags = :atomics.new(atomics_size, signed: false)
      checkpoint_in_flight = :atomics.new(atomics_size, signed: false)
      last_applied_index = :atomics.new(atomics_size, signed: false)
      last_released_cursor_index = :atomics.new(atomics_size, signed: false)
      replay_safe_index = :atomics.new(atomics_size, signed: false)
      flow_lmdb_replay_safe_index = :atomics.new(atomics_size, signed: false)

      :atomics.put(replay_safe_index, shard_index + 1, marker_index)
      :atomics.put(flow_lmdb_replay_safe_index, shard_index + 1, marker_index)

      state = %{
        state
        | applied_count: 10,
          release_cursor_interval: 1,
          pending_release_cursor_index: marker_index,
          pending_replay_safe_marker_index: marker_index,
          pending_release_cursor_checkpoint_indices: MapSet.new(),
          instance_ctx: %{
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight,
            last_applied_index: last_applied_index,
            last_released_cursor_index: last_released_cursor_index,
            replay_safe_index: replay_safe_index,
            flow_lmdb_replay_safe_index: flow_lmdb_replay_safe_index
          }
      }

      {new_state, {:applied_at, ^poke_index, {:ok, [:ok]}}, effects} =
        StateMachine.apply(
          %{index: poke_index, term: 1, system_time: System.os_time(:millisecond)},
          {:batch, [{:async, node(), {:release_cursor_poke, marker_index}}]},
          state
        )

      assert Enum.any?(effects, &match?({:release_cursor, ^marker_index}, &1))
      assert new_state.applied_count == 10
      assert :atomics.get(last_applied_index, shard_index + 1) == poke_index
      assert :atomics.get(last_released_cursor_index, shard_index + 1) == marker_index
    end

    test "release cursor metrics resolve instance context by name like production Raft config", %{
      state: state,
      shard_index: shard_index
    } do
      instance_name = :"cursor_metric_instance_#{System.unique_integer([:positive])}"
      root = Path.join(System.tmp_dir!(), Atom.to_string(instance_name))
      File.rm_rf!(root)
      File.mkdir_p!(root)

      instance_ctx =
        FerricStore.Instance.build(instance_name, shard_count: shard_index + 1, data_dir: root)

      :atomics.put(instance_ctx.replay_safe_index, shard_index + 1, 77)
      :atomics.put(instance_ctx.flow_lmdb_replay_safe_index, shard_index + 1, 77)

      on_exit({:cursor_metric_instance, instance_name}, fn ->
        FerricStore.Instance.cleanup(instance_name)
        File.rm_rf!(root)
      end)

      state = %{
        state
        | release_cursor_interval: 1,
          instance_ctx: nil,
          instance_name: instance_name
      }

      meta = %{index: 77, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, 77, nil}, effects} =
        StateMachine.apply(meta, {:getdel, "released_cursor_metric_by_name_missing"}, state)

      assert Enum.any?(effects, &match?({:release_cursor, 77}, &1))
      assert :atomics.get(instance_ctx.last_applied_index, shard_index + 1) == 77
      assert :atomics.get(instance_ctx.last_released_cursor_index, shard_index + 1) == 77
    end

    test "named state machine marks checkpoint dirty and blocks release after nosync write", %{
      ets: ets
    } do
      instance_name = :"cursor_dirty_instance_#{System.unique_integer([:positive])}"
      root = Path.join(System.tmp_dir!(), Atom.to_string(instance_name))
      File.rm_rf!(root)
      File.mkdir_p!(root)

      instance_ctx = FerricStore.Instance.build(instance_name, shard_count: 1, data_dir: root)

      on_exit({:cursor_dirty_instance, instance_name}, fn ->
        FerricStore.Instance.cleanup(instance_name)
        File.rm_rf!(root)
      end)

      state =
        init_state_for_release_cursor(ets,
          shard_index: 0,
          release_cursor_interval: 1,
          instance_ctx: nil,
          instance_name: instance_name
        )

      meta = %{index: 88, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, 88, :ok}, effects} =
        StateMachine.apply(meta, {:put, "dirty_named_rc_key", "value", 0}, state)

      assert new_state.applied_count == 1
      assert :atomics.get(instance_ctx.checkpoint_flags, 1) == 1
      refute Enum.any?(effects, &match?({:release_cursor, 88}, &1))
      assert Ferricstore.Raft.ReplaySafeIndex.read(new_state.shard_data_path) == 0
    end

    test "release_cursor waits while checkpoint fsync is in flight", %{
      state: state,
      shard_index: shard_index
    } do
      checkpoint_flags = :atomics.new(shard_index + 1, signed: false)
      checkpoint_in_flight = :atomics.new(shard_index + 1, signed: false)

      # The checkpointer clears checkpoint_flags before async fsync starts.
      # StateMachine must still see the in-flight marker and keep Ra log
      # entries until the fsync completion arrives.
      :atomics.put(checkpoint_flags, shard_index + 1, 0)
      :atomics.put(checkpoint_in_flight, shard_index + 1, 1)

      state = %{
        state
        | release_cursor_interval: 1,
          instance_ctx: %{
            checkpoint_flags: checkpoint_flags,
            checkpoint_in_flight: checkpoint_in_flight
          }
      }

      meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, 1, nil}, effects} =
        StateMachine.apply(meta, {:getdel, "missing_during_checkpoint"}, state)

      refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
             "Raft cursor must not advance while Bitcask fsync is still in flight"
    end

    test "release_cursor waits when custom instance checkpoint state is unresolved", %{
      state: state
    } do
      name = :"missing_custom_instance_#{System.unique_integer([:positive])}"

      state = %{
        state
        | release_cursor_interval: 1,
          instance_ctx: nil,
          instance_name: name
      }

      meta = %{index: 1, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, 1, nil}, effects} =
        StateMachine.apply(meta, {:getdel, "missing_custom_checkpoint_ctx"}, state)

      refute Enum.any?(effects, &match?({:release_cursor, 1}, &1)),
             "custom instance state must fail closed until checkpoint atomics are resolved"
    end

    test "release_cursor emitted at every interval multiple", %{store: _store, ets: ets} do
      interval = 3

      state =
        init_state_for_release_cursor(ets, release_cursor_interval: interval)

      # Apply 9 commands, expect release_cursor at positions 3, 6, 9
      {_final_state, cursor_indices} =
        Enum.reduce(1..9, {state, []}, fn i, {acc, cursors} ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, nil}, effects} =
            StateMachine.apply(meta, {:getdel, "mc_#{i}"}, acc)

          cursor_idx =
            Enum.find_value(effects, fn
              {:release_cursor, idx} -> idx
              _ -> nil
            end)

          if cursor_idx, do: {new_state, cursors ++ [cursor_idx]}, else: {new_state, cursors}
        end)

      assert cursor_indices == [3, 6, 9]
    end

    test "release_cursor emitted for read-only delete miss at interval boundary", %{
      store: _store,
      ets: ets
    } do
      interval = 3

      state =
        init_state_for_release_cursor(ets, release_cursor_interval: interval)

      # Apply two read-only misses (applied_count = 2), then another at the 3rd apply.
      meta1 = %{index: 10, term: 1, system_time: System.os_time(:millisecond)}

      {s1, {:applied_at, _, nil}, _e1} =
        StateMachine.apply(meta1, {:getdel, "del_rc_a"}, state)

      meta2 = %{index: 11, term: 1, system_time: System.os_time(:millisecond)}

      {s2, {:applied_at, _, nil}, _e2} =
        StateMachine.apply(meta2, {:getdel, "del_rc_b"}, s1)

      # 3rd read-only delete miss should trigger release_cursor.
      meta3 = %{index: 12, term: 1, system_time: System.os_time(:millisecond)}
      {_s3, {:applied_at, _, nil}, effects} = StateMachine.apply(meta3, {:getdel, "del_rc_a"}, s2)

      cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
      assert {:release_cursor, 12} = cursor_effect
    end

    test "release_cursor emitted for batch that crosses interval boundary", %{
      store: _store,
      ets: ets
    } do
      interval = 5

      state =
        init_state_for_release_cursor(ets, release_cursor_interval: interval)

      # Apply 3 single commands (applied_count = 3)
      state_before =
        Enum.reduce(1..3, state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, nil}, _e} =
            StateMachine.apply(meta, {:getdel, "pre_#{i}"}, acc)

          new_state
        end)

      assert state_before.applied_count == 3

      # Batch of 3 commands takes applied_count from 3 to 6 -- crosses interval at 5
      batch = [
        {:getdel, "batch_1"},
        {:getdel, "batch_2"},
        {:getdel, "batch_3"}
      ]

      meta = %{index: 4, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, _, {:ok, results}}, effects} =
        StateMachine.apply(meta, {:batch, batch}, state_before)

      assert results == [nil, nil, nil]
      assert new_state.applied_count == 6
      cursor_effect = Enum.find(effects, &match?({:release_cursor, _}, &1))
      assert {:release_cursor, 4} = cursor_effect
    end

    test "release_cursor not emitted when meta has no index", %{state: state} do
      # Use default interval (1000). Even if we manually set applied_count to 999,
      # without an index in meta, release_cursor should not be emitted.
      state_near = %{state | applied_count: 999, release_cursor_interval: 1000}

      # No :index in meta -- simulates unit test / non-ra context
      result = StateMachine.apply(%{}, {:put, "no_idx", "val", 0}, state_near)

      case result do
        {new_state, :ok} ->
          assert new_state.applied_count == 1000

        {_new_state, :ok, _effects} ->
          flunk("release_cursor should not be emitted when meta has no :index key")
      end
    end

    test "release_cursor state snapshot contains correct machine state", %{
      store: _store,
      ets: ets
    } do
      interval = 3

      state =
        init_state_for_release_cursor(ets, shard_index: 2, release_cursor_interval: interval)

      # Apply 3 commands to trigger release_cursor
      state_after =
        Enum.reduce(1..2, state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, _e} =
            StateMachine.apply(meta, {:put, "snap_#{i}", "v#{i}", 0}, acc)

          new_state
        end)

      meta = %{index: 3, term: 1, system_time: System.os_time(:millisecond)}

      {_new_state, {:applied_at, _, :ok}, effects} =
        StateMachine.apply(meta, {:put, "snap_3", "v3", 0}, state_after)

      checkpoint_effect = Enum.find(effects, &match?({:checkpoint, _, _}, &1))
      assert {:checkpoint, 3, cursor_state} = checkpoint_effect

      # The snapshot state should reflect the current state
      assert cursor_state.shard_index == 2
      assert cursor_state.applied_count == 3
      assert is_binary(cursor_state.shard_data_path)
      assert cursor_state.ets == ets
      assert cursor_state.release_cursor_interval == interval
    end

    test "overview/1 includes release_cursor_interval", %{state: state} do
      overview = StateMachine.overview(state)
      assert Map.has_key?(overview, :release_cursor_interval)
      assert is_integer(overview.release_cursor_interval)
    end
  end

  defp state_with_missing_active_file(state, opts \\ []) do
    file_id = 9_000_000 + :erlang.unique_integer([:positive])
    shard_index = Keyword.get(opts, :shard_index, 100_000 + :erlang.unique_integer([:positive]))

    %{
      state
      | shard_index: shard_index,
        active_file_id: file_id,
        active_file_path: Path.join(state.shard_data_path, "#{file_id}.log")
    }
  end

  defp init_state_for_release_cursor(ets, opts \\ []) do
    shard_index = Keyword.get(opts, :shard_index, 0)
    root = Path.join(System.tmp_dir!(), "sm_rc_#{System.unique_integer([:positive])}")
    shard_path = Ferricstore.DataDir.shard_data_path(root, shard_index)
    active_file_path = Path.join(shard_path, "00000.log")

    File.mkdir_p!(shard_path)
    File.touch!(active_file_path)
    on_exit({:sm_release_cursor_root, root}, fn -> File.rm_rf!(root) end)

    config =
      %{
        shard_index: shard_index,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: ets,
        instance_ctx: release_cursor_instance_ctx(shard_index)
      }
      |> Map.merge(Map.new(opts))

    StateMachine.init(config)
  end

  defp release_cursor_instance_ctx(shard_index, durable_index \\ 1_000_000, overrides \\ []) do
    atomics_size = shard_index + 1
    replay_safe_index = :atomics.new(atomics_size, signed: false)
    flow_lmdb_replay_safe_index = :atomics.new(atomics_size, signed: false)

    :atomics.put(replay_safe_index, shard_index + 1, durable_index)
    :atomics.put(flow_lmdb_replay_safe_index, shard_index + 1, durable_index)

    [
      checkpoint_flags: :atomics.new(atomics_size, signed: false),
      checkpoint_in_flight: :atomics.new(atomics_size, signed: false),
      disk_pressure: :atomics.new(atomics_size, signed: false),
      last_applied_index: :atomics.new(atomics_size, signed: false),
      last_released_cursor_index: :atomics.new(atomics_size, signed: false),
      replay_safe_index: replay_safe_index,
      flow_lmdb_replay_safe_index: flow_lmdb_replay_safe_index,
      hot_cache_max_value_size: 65_536
    ]
    |> Keyword.merge(overrides)
    |> Map.new()
  end

  defp set_opts(overrides) do
    Map.merge(
      %{expire_at_ms: 0, nx: false, xx: false, get: false, keepttl: false, has_expiry: false},
      overrides
    )
  end

  defp key_for_shard(ctx, prefix, shard_index) do
    0..10_000
    |> Enum.find_value(fn i ->
      key = "#{prefix}:#{i}"
      if Ferricstore.Store.Router.shard_for(ctx, key) == shard_index, do: key
    end)
  end

  defp prob_test_path(dir, key, ext) do
    Path.join(dir, "#{Base.url_encode64(key, padding: false)}.#{ext}")
  end

  defp apply_result_value({_state, {:applied_at, _now_ms, result}, _effects}), do: result
  defp apply_result_value({_state, result, _effects}), do: result
  defp apply_result_value({_state, result}), do: result

  defp assert_keydir_unavailable_event(request) do
    assert_receive {:sm_keydir_unavailable, [:ferricstore, :store, :shard_unavailable],
                    %{count: 1},
                    %{request: ^request, reason: :keydir_unavailable, source: :raft_apply}}
  end
end
