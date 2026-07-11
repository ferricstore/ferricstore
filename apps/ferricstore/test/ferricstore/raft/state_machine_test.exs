Code.require_file(
  "state_machine_test/sections/coalesces_consecutive_flow_native_index_ops_crossing_ordering_barriers.exs",
  __DIR__
)

Code.require_file("state_machine_test/sections/flow_blob_side_channel_apply.exs", __DIR__)
Code.require_file("state_machine_test/sections/flow_command_time.exs", __DIR__)

Code.require_file(
  "state_machine_test/sections/flow_governance_release_outbox.exs",
  __DIR__
)

Code.require_file("state_machine_test/sections/flow_governance_limit.exs", __DIR__)

Code.require_file("state_machine_test/sections/flow_index_rollback.exs", __DIR__)
Code.require_file("state_machine_test/sections/state_machine_compound_reads.exs", __DIR__)

Code.require_file(
  "state_machine_test/sections/apply_3_put_key_value_expire_at_ms_part_1.exs",
  __DIR__
)

Code.require_file(
  "state_machine_test/sections/apply_3_put_key_value_expire_at_ms_part_2.exs",
  __DIR__
)

Code.require_file(
  "state_machine_test/sections/apply_3_put_key_value_expire_at_ms_part_3.exs",
  __DIR__
)

Code.require_file(
  "state_machine_test/sections/apply_3_set_key_value_expire_at_ms_opts.exs",
  __DIR__
)

Code.require_file("state_machine_test/sections/apply_3_delete_key.exs", __DIR__)
Code.require_file("state_machine_test/sections/handle_aux_5.exs", __DIR__)
Code.require_file("state_machine_test/sections/release_cursor_log_compaction.exs", __DIR__)

Code.require_file(
  "state_machine_test/sections/spop_removes_type_marker_final_set_member_popped.exs",
  __DIR__
)

defmodule Ferricstore.Raft.StateMachineTest.CurrentStateMachine do
  @moduledoc false

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Flow.PolicyCommand
  alias Ferricstore.Raft.StateMachine

  def apply(meta, command, state), do: StateMachine.apply(meta, canonical(command), state)

  defdelegate init(config), to: StateMachine
  defdelegate init_aux(config), to: StateMachine
  defdelegate handle_aux(meta, command, from, state, aux), to: StateMachine
  defdelegate overview(state), to: StateMachine
  defdelegate state_enter(role, state), to: StateMachine
  defdelegate tick(time, state), to: StateMachine
  defdelegate apply_waraft_segment_command(command, meta, state, writer), to: StateMachine
  defdelegate apply_standalone_command(command, state), to: StateMachine
  defdelegate apply_standalone_command(command, meta, state), to: StateMachine

  defdelegate __apply_pending_locations_for_test__(state, file_id, batch, locations),
    to: StateMachine

  defdelegate __validate_pending_locations__(batch, locations), to: StateMachine
  defdelegate __coalesce_flow_native_ops_for_test__(ops), to: StateMachine

  defdelegate __flow_history_projection_same_shard_for_test__(ctx, state, ops),
    to: StateMachine

  defdelegate __flow_history_projection_shards_for_test__(ctx, state, ops), to: StateMachine
  defdelegate __flow_history_projection_value_refs_for_test__(op), to: StateMachine
  defdelegate __observe_tagged_lmdb_enqueue_failure_for_test__(state, reason), to: StateMachine
  defdelegate __safe_ets_select_page_for_test__(table, spec, limit), to: StateMachine
  defdelegate __append_pending_batch_sync_for_test__(path, batch), to: StateMachine

  defdelegate __flow_read_claim_hot_values_for_test__(state, keys, priority, partition_key),
    to: StateMachine

  defp canonical({:cross_shard_tx, shard_batches}) when is_list(shard_batches),
    do: {:cross_shard_tx, canonical_batches(shard_batches)}

  defp canonical({:cross_shard_tx, shard_batches, watched_keys}) when is_list(shard_batches),
    do: {:cross_shard_tx, canonical_batches(shard_batches), watched_keys}

  defp canonical({:flow_shared_ref_write, shard_index, command}),
    do: {:flow_shared_ref_write, shard_index, canonical(command)}

  defp canonical(command) when is_tuple(command) do
    if PolicyCommand.requires_stamp?(command) do
      attrs = elem(command, tuple_size(command) - 1)

      if is_map(attrs) do
        put_elem(
          command,
          tuple_size(command) - 1,
          Map.put_new(attrs, :policy_snapshot_captured, true)
        )
      else
        command
      end
    else
      command
    end
  end

  defp canonical(command), do: command

  defp canonical_batches(shard_batches) do
    Enum.map(shard_batches, fn
      {shard_index, queue, namespace} when is_list(queue) ->
        {shard_index, Enum.map(queue, &canonical_entry/1), namespace}

      other ->
        other
    end)
  end

  defp canonical_entry({index, command}) when is_integer(index) and is_tuple(command),
    do: {index, canonical(command)}

  defp canonical_entry({command, args}) when is_binary(command) and is_list(args) do
    {:ok, prepared} = PreparedCommand.prepare(command, args)
    {prepared.command, prepared.args, prepared.ast}
  end

  defp canonical_entry(command) when is_tuple(command), do: canonical(command)
  defp canonical_entry(other), do: other
end

defmodule Ferricstore.Raft.StateMachineTest do
  @moduledoc """
  Unit tests for `Ferricstore.Raft.StateMachine`.

  These tests exercise the state machine callbacks directly without running
  a full WARaft partition. The state machine is deterministic and its callbacks can
  be tested in isolation by constructing state manually.
  """

  use ExUnit.Case, async: false
  @moduletag :raft
  @moduletag :global_state

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Raft.StateMachineTest.CurrentStateMachine, as: StateMachine
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.{BlobRef, BlobStore, CompoundKey, LFU, Promotion}

  def handle_flow_append_telemetry(_event, measurements, metadata, {test_pid, shard_index}) do
    if metadata[:shard_index] == shard_index do
      send(test_pid, {:flow_bitcask_append, measurements, metadata})
    end
  end

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

  test "two-field async envelopes are rejected without mutation", %{state: state, ets: ets} do
    command = {:async, {:put, "old-async", "value", 0}}

    assert {^state, {:error, {:unknown_command, ^command}}} =
             StateMachine.apply(%{}, command, state)

    assert [] = :ets.lookup(ets, "old-async")
  end

  use Ferricstore.Raft.StateMachineTest.Sections.CoalescesConsecutiveFlowNativeIndexOpsCrossingOrderingBarriers

  use Ferricstore.Raft.StateMachineTest.Sections.FlowGovernanceReleaseOutbox
  use Ferricstore.Raft.StateMachineTest.Sections.FlowGovernanceLimit

  defp safe_delete_ets(table) do
    :ets.delete(table)
  rescue
    ArgumentError -> :ok
  end

  defp write_expired_lmdb_terminal!(state, opts) do
    now_ms = Keyword.fetch!(opts, :now_ms)
    id = "flow-lmdb-expired-keydir-gone"
    type = "lmdb-expired-keydir-gone"
    terminal_state = "completed"
    partition_key = "tenant-lmdb-expired-keydir-gone"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    expire_at_ms = now_ms

    record = %{
      id: id,
      type: type,
      state: terminal_state,
      partition_key: partition_key,
      version: 1,
      run_at_ms: now_ms,
      due_at_ms: nil,
      updated_at_ms: now_ms,
      terminal_retention_until_ms: expire_at_ms
    }

    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)
    terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, id, now_ms)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)
    expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

    terminal_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        id,
        now_ms,
        expire_at_ms,
        state_key,
        count_key
      )

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(state.flow_lmdb_path, [
               {:put, state_key,
                Ferricstore.Flow.LMDB.encode_value(
                  Ferricstore.Flow.encode_record(record),
                  expire_at_ms
                )},
               {:put, terminal_key, terminal_value},
               {:put, expire_key,
                Ferricstore.Flow.LMDB.encode_terminal_expire_value(
                  terminal_key,
                  state_key,
                  count_key
                )}
             ])

    state_key
  end

  defp setup_flow_indexes(state) do
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
  end

  defp flow_record!(state, state_key) do
    case :ets.lookup(state.ets, state_key) do
      [{^state_key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}]
      when is_binary(value) ->
        Ferricstore.Flow.decode_record(value)

      [{^state_key, nil, _expire_at_ms, _lfu, file_id, offset, _value_size}] ->
        path =
          Path.join(
            state.shard_data_path,
            "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
          )

        case NIF.v2_pread_at(path, offset) do
          {:ok, value} when is_binary(value) -> Ferricstore.Flow.decode_record(value)
          other -> flunk("expected cold Flow record at #{path}:#{offset}, got: #{inspect(other)}")
        end

      other ->
        flunk("expected hot Flow state record for #{inspect(state_key)}, got: #{inspect(other)}")
    end
  end

  defp flow_history_fields!(state, id, partition_key, event_id) do
    history_key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, partition_key)

    case :ets.lookup(state.ets, history_key) do
      [
        {^history_key, nil, _expire_at_ms, _lfu, {:flow_history, file_id}, offset, _value_size}
      ] ->
        assert {:ok, value} =
                 Ferricstore.Flow.HistoryProjector.read_value(
                   state.shard_data_path,
                   {:flow_history, file_id},
                   offset
                 )

        record = flow_record!(state, Ferricstore.Flow.Keys.state_key(id, partition_key))
        Ferricstore.Flow.decode_history_fields(value, record)

      [] ->
        # Default hot-history retention can trim the keydir row immediately;
        # the durable projection still has to be present in the history log.
        record = flow_record!(state, Ferricstore.Flow.Keys.state_key(id, partition_key))

        case Ferricstore.Flow.HistoryProjector.scan_event_value(
               state.shard_data_path,
               history_key
             ) do
          {:ok, value} ->
            Ferricstore.Flow.decode_history_fields(value, record)

          other ->
            flunk(
              "expected projected Flow history for #{inspect(history_key)}, got: #{inspect(other)}"
            )
        end

      other ->
        flunk(
          "expected projected Flow history for #{inspect(history_key)}, got: #{inspect(other)}"
        )
    end
  end

  defp assert_flow_history_event!(state, id, partition_key, event_id, event) do
    Ferricstore.Test.Eventually.assert_eventually(fn ->
      fields = flow_history_fields!(state, id, partition_key, event_id)
      assert flow_history_field(fields, "event") == event
      assert flow_history_field(fields, "id") == id
    end)
  end

  defp start_flow_history_projector!(state) do
    {:ok, pid} =
      Ferricstore.Flow.HistoryProjector.start_link(
        shard_index: state.shard_index,
        shard_data_path: state.shard_data_path,
        instance_ctx: Map.get(state, :instance_ctx)
      )

    on_exit(fn ->
      try do
        if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5_000)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end
    end)

    :ok
  end

  defp flow_history_field([key, value | _rest], key), do: value
  defp flow_history_field([_key, _value | rest], key), do: flow_history_field(rest, key)
  defp flow_history_field([], _key), do: nil

  defp flow_value!(state, value_key) do
    case :ets.lookup(state.ets, value_key) do
      [{^value_key, nil, _expire_at_ms, _lfu, file_id, offset, _value_size}] ->
        flow_value_from_location!(state, value_key, file_id, offset)

      other ->
        flow_value_from_lmdb!(state, value_key, other)
    end
  end

  defp flow_value_from_location!(state, _value_key, {:flow_history, _file_id} = file_id, offset) do
    case Ferricstore.Flow.HistoryProjector.read_value(state.shard_data_path, file_id, offset) do
      {:ok, value} when is_binary(value) ->
        value

      other ->
        flunk(
          "expected projected Flow value at #{inspect(file_id)}:#{offset}, got: #{inspect(other)}"
        )
    end
  end

  defp flow_value_from_location!(state, _value_key, file_id, offset)
       when is_integer(file_id) and file_id >= 0 do
    path =
      Path.join(
        state.shard_data_path,
        "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
      )

    case NIF.v2_pread_at(path, offset) do
      {:ok, value} when is_binary(value) ->
        value

      other ->
        flunk("expected cold Flow value at #{path}:#{offset}, got: #{inspect(other)}")
    end
  end

  defp flow_value_from_lmdb!(state, value_key, original_lookup) do
    path = Ferricstore.Flow.LMDB.path(state.shard_data_path)

    case Ferricstore.Flow.LMDB.get(path, value_key) do
      {:ok, blob} when is_binary(blob) ->
        now_ms = System.system_time(:millisecond)

        case Ferricstore.Flow.LMDB.decode_value_locator(blob, now_ms) do
          {:ok, {file_id, offset, _value_size}} ->
            flow_value_from_location!(state, value_key, file_id, offset)

          :not_locator ->
            case Ferricstore.Flow.LMDB.decode_value(blob, now_ms) do
              {:ok, value} when is_binary(value) ->
                value

              other ->
                flunk(
                  "expected LMDB Flow value for #{inspect(value_key)}, got decoded #{inspect(other)}"
                )
            end

          other ->
            flunk(
              "expected LMDB Flow value locator for #{inspect(value_key)}, got #{inspect(other)}"
            )
        end

      other ->
        flunk(
          "expected cold or projected Flow value for #{inspect(value_key)}, got ETS #{inspect(original_lookup)} and LMDB #{inspect(other)}"
        )
    end
  end

  defp assert_flow_blob_value!(state, value_key, expected_payload) do
    disk_value = flow_value!(state, value_key)
    assert BlobRef.encoded_size?(byte_size(disk_value))

    assert {:ok, materialized} =
             Ferricstore.Store.BlobValue.maybe_materialize(
               state.data_dir,
               state.shard_index,
               128,
               disk_value
             )

    assert Ferricstore.Flow.decode_value(materialized) == expected_payload
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  use Ferricstore.Raft.StateMachineTest.Sections.FlowBlobSideChannelApply

  use Ferricstore.Raft.StateMachineTest.Sections.FlowCommandTime

  use Ferricstore.Raft.StateMachineTest.Sections.FlowIndexRollback

  use Ferricstore.Raft.StateMachineTest.Sections.StateMachineCompoundReads

  use Ferricstore.Raft.StateMachineTest.Sections.Apply3PutKeyValueExpireAtMsPart1
  use Ferricstore.Raft.StateMachineTest.Sections.Apply3PutKeyValueExpireAtMsPart2
  use Ferricstore.Raft.StateMachineTest.Sections.Apply3PutKeyValueExpireAtMsPart3

  use Ferricstore.Raft.StateMachineTest.Sections.Apply3SetKeyValueExpireAtMsOpts

  use Ferricstore.Raft.StateMachineTest.Sections.Apply3DeleteKey

  use Ferricstore.Raft.StateMachineTest.Sections.HandleAux5

  use Ferricstore.Raft.StateMachineTest.Sections.ReleaseCursorLogCompaction

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
    flow_history_projected_index = :atomics.new(atomics_size, signed: false)

    :atomics.put(replay_safe_index, shard_index + 1, durable_index)
    :atomics.put(flow_lmdb_replay_safe_index, shard_index + 1, durable_index)
    :atomics.put(flow_history_projected_index, shard_index + 1, durable_index)

    [
      checkpoint_flags: :atomics.new(atomics_size, signed: false),
      checkpoint_in_flight: :atomics.new(atomics_size, signed: false),
      disk_pressure: :atomics.new(atomics_size, signed: false),
      last_applied_index: :atomics.new(atomics_size, signed: false),
      last_released_cursor_index: :atomics.new(atomics_size, signed: false),
      replay_safe_index: replay_safe_index,
      flow_lmdb_replay_safe_index: flow_lmdb_replay_safe_index,
      flow_history_projected_index: flow_history_projected_index,
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

  use Ferricstore.Raft.StateMachineTest.Sections.SpopRemovesTypeMarkerFinalSetMemberPopped

  def relay_compound_put_append_telemetry(_event, measurements, metadata, test_pid) do
    send(test_pid, {:compound_put_append, measurements, metadata})
  end
end
