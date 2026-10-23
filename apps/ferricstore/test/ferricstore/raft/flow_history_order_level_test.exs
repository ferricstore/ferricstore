defmodule Ferricstore.Raft.FlowHistoryOrderLevelTest do
  @moduledoc """
  Apply trims a Flow's oldest history events and reads its latest one. A
  millisecond's events take version order there only from feature level 5:
  older releases ordered them by id bytes, and every replica must agree.
  """
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Flow.{LMDB, NativeOrderedIndex}
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Store.LFU

  @history_key "f:{f}:h:level-order"

  setup do
    shard_index = 30_000 + System.unique_integer([:positive])
    root = Path.join(System.tmp_dir!(), "flow-history-order-level-#{shard_index}")
    shard_path = Ferricstore.DataDir.shard_data_path(root, shard_index)
    path = Path.join(shard_path, "00000.log")
    File.mkdir_p!(shard_path)
    File.touch!(path)

    keydir = :ets.new(:flow_history_order_level, [:set, :public])

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: path,
        ets: keydir
      })

    on_exit(fn ->
      _ = LMDB.release(state.flow_lmdb_path)
      File.rm_rf!(root)
    end)

    native = NativeOrderedIndex.get(state.flow_index_name, state.flow_lookup_name)

    members =
      [{"999-1", 999}, {"999-2", 999}] ++ Enum.map(3..12, &{"1000-#{&1}", 1_000})

    :ok = NativeOrderedIndex.put_members(native, @history_key, members)

    %{state: state, keydir: keydir}
  end

  test "below level 5 a millisecond's events keep id byte order", %{state: state} do
    assert ids(StateMachine.__flow_history_end_range_for_test__(state, @history_key, 4, false)) ==
             ["999-1", "999-2", "1000-10", "1000-11"]

    assert ids(StateMachine.__flow_history_end_range_for_test__(state, @history_key, 3, true)) ==
             ["1000-9", "1000-8", "1000-7"]
  end

  test "from level 5 a millisecond's events take version order", %{
    state: state,
    keydir: keydir
  } do
    marker = FeatureLevel.marker()

    true =
      :ets.insert(
        keydir,
        {FeatureLevel.key(5), marker, 0, LFU.initial(), 0, 0, byte_size(marker)}
      )

    assert ids(StateMachine.__flow_history_end_range_for_test__(state, @history_key, 4, false)) ==
             ["999-1", "999-2", "1000-3", "1000-4"]

    assert ids(StateMachine.__flow_history_end_range_for_test__(state, @history_key, 3, true)) ==
             ["1000-12", "1000-11", "1000-10"]
  end

  defp ids(events), do: Enum.map(events, &elem(&1, 0))
end
