defmodule Ferricstore.Raft.StateMachineLatencyTraceTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.StateMachine

  setup do
    shard_index = 19_000 + :rand.uniform(999)
    root = Path.join(System.tmp_dir!(), "sm_trace_test_#{:rand.uniform(9_999_999)}")
    dir = Ferricstore.DataDir.shard_data_path(root, shard_index)
    File.mkdir_p!(dir)

    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    keydir_name = :"sm_trace_test_keydir_#{:rand.uniform(9_999_999)}"
    :ets.new(keydir_name, [:set, :public, :named_table])

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir_name
      })

    on_exit(fn ->
      try do
        :ets.delete(keydir_name)
      rescue
        ArgumentError -> :ok
      end

      File.rm_rf!(root)
    end)

    %{state: state, ets: keydir_name}
  end

  test "traced apply returns internal stage timings without changing command result", %{
    state: state,
    ets: ets
  } do
    result =
      StateMachine.apply(
        %{index: 1, system_time: System.system_time(:millisecond)},
        {:ferricstore_latency_trace, {:put, "trace-key", "trace-value", 0}},
        state
      )

    assert {
             _new_state,
             {:applied_at, 1, {:ferricstore_latency_trace_result, :ok, trace}},
             _effects
           } = result

    for key <- [
          "server_apply_us",
          "server_bitcask_append_us",
          "server_pending_locations_us",
          "server_flow_index_update_us",
          "server_zset_index_update_us"
        ] do
      assert is_integer(trace[key])
      assert trace[key] >= 0
    end

    assert [{_, "trace-value", 0, _lfu, 0, offset, value_size}] = :ets.lookup(ets, "trace-key")
    assert is_integer(offset)
    assert value_size == byte_size("trace-value")
  end
end
