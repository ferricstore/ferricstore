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

    assert {:ok, [{:cache, 1}, {:cache, 2}]} =
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

  test "count reads ignore stale cache entries after a concurrent direct write", %{path: path} do
    state_index_key = "state:completed"
    count_key = LMDB.terminal_count_key(state_index_key)

    assert :ok = LMDB.write_batch(path, [{:put, count_key, LMDB.encode_count(1)}])
    assert :ok = TerminalCounts.put_cached_count_key(path, count_key, 1)
    assert :ok = LMDB.write_batch(path, [{:put, count_key, LMDB.encode_count(2)}])

    assert {:ok, 2} = LMDB.terminal_count(path, state_index_key)
    assert {:ok, [2]} = LMDB.terminal_counts(path, [state_index_key])
  end
end
