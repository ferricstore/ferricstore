defmodule Ferricstore.Flow.LMDB.TerminalCountsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDB.TerminalCounts

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_terminal_counts_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    %{path: path}
  end

  test "count batch decoding requires exact cardinality and valid values" do
    keys = ["count-a", "count-b"]
    valid = [LMDB.encode_count(1), LMDB.encode_count(2)]

    assert {:ok, [{:value, 1}, {:value, 2}]} =
             TerminalCounts.__decode_count_results_for_test__(
               keys,
               Enum.map(valid, &{:ok, &1})
             )

    for results <- [
          [{:ok, hd(valid)}],
          Enum.map(valid, &{:ok, &1}) ++ [:not_found],
          :invalid
        ] do
      assert {:error, {:invalid_terminal_count_batch, _reason}} =
               TerminalCounts.__decode_count_results_for_test__(keys, results)
    end

    assert {:error, {:invalid_terminal_count_value, "count-b"}} =
             TerminalCounts.__decode_count_results_for_test__(
               keys,
               [{:ok, hd(valid)}, {:ok, "corrupt"}]
             )
  end

  test "single corrupt count rows fail closed instead of becoming zero", %{path: path} do
    state_index_key = "state:completed"
    count_key = LMDB.terminal_count_key(state_index_key)

    assert :ok = LMDB.write_batch(path, [{:put, count_key, "corrupt"}])

    assert {:error, :invalid_terminal_count_value} =
             LMDB.terminal_count(path, state_index_key)
  end

  test "count reads observe direct writes after a prior read", %{path: path} do
    state_index_key = "state:completed"
    count_key = LMDB.terminal_count_key(state_index_key)

    assert :ok = LMDB.write_batch(path, [{:put, count_key, LMDB.encode_count(1)}])
    assert {:ok, 1} = LMDB.terminal_count(path, state_index_key)
    assert :ok = LMDB.write_batch(path, [{:put, count_key, LMDB.encode_count(2)}])

    assert {:ok, 2} = LMDB.terminal_count(path, state_index_key)
    assert {:ok, [2]} = LMDB.terminal_counts(path, [state_index_key])
  end

  test "terminal count writes do not retain process-local cache entries", %{path: path} do
    for index <- 1..64 do
      assert :ok = LMDB.put_terminal_count(path, "state:#{index}", index)
    end

    entries =
      case :ets.whereis(:ferricstore_flow_lmdb_terminal_count_cache) do
        :undefined -> []
        table -> :ets.match_object(table, {{path, :_}, :_})
      end

    assert entries == []
  end

  test "terminal counts have no process-local cache lifecycle" do
    owner = Module.concat(Ferricstore.Flow.LMDB, TerminalCountCacheOwner)

    assert Process.whereis(owner) == nil
    assert :ets.whereis(:ferricstore_flow_lmdb_terminal_count_cache) == :undefined
  end
end
