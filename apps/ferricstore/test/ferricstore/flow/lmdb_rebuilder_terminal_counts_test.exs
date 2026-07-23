defmodule Ferricstore.Flow.LMDBRebuilder.TerminalCountsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBRebuilder.TerminalCounts

  setup do
    previous =
      Application.fetch_env(
        :ferricstore,
        :flow_lmdb_rebuild_count_key_page_size
      )

    Application.put_env(:ferricstore, :flow_lmdb_rebuild_count_key_page_size, 2)

    on_exit(fn ->
      case previous do
        {:ok, value} ->
          Application.put_env(:ferricstore, :flow_lmdb_rebuild_count_key_page_size, value)

        :error ->
          Application.delete_env(:ferricstore, :flow_lmdb_rebuild_count_key_page_size)
      end
    end)

    :ok
  end

  test "terminal count reconciliation deletes stale count keys and writes exact counts" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-terminal-count-reconcile-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    existing_key = LMDB.terminal_count_key("state:completed")
    new_key = LMDB.terminal_count_key("state:failed")
    stale_state_index_key = "state:cancelled"
    stale_a = LMDB.terminal_count_key(stale_state_index_key)
    stale_b = LMDB.terminal_count_key("state:removed")

    terminal_ops =
      Enum.map(1..3, &terminal_put("state:completed", "completed-#{&1}", &1)) ++
        Enum.map(1..5, &terminal_put("state:failed", "failed-#{&1}", &1))

    assert :ok =
             LMDB.write_batch(
               path,
               terminal_ops ++
                 [
                   {:put, existing_key, LMDB.encode_count(99)},
                   {:put, stale_a, LMDB.encode_count(7)},
                   {:put, stale_b, LMDB.encode_count(11)}
                 ]
             )

    stats = %{lmdb_errors: 0}
    expected_stats = Map.put(stats, :terminal_count_keys, 2)

    assert ^expected_stats = TerminalCounts.persist(stats, path)
    assert {:ok, value} = LMDB.get(path, existing_key)
    assert value == LMDB.encode_count(3)
    assert {:ok, value} = LMDB.get(path, new_key)
    assert value == LMDB.encode_count(5)
    assert :not_found = LMDB.get(path, stale_a)
    assert :not_found = LMDB.get(path, stale_b)
    assert {:ok, [0]} = LMDB.terminal_counts(path, [stale_state_index_key])

    assert ^expected_stats = TerminalCounts.persist(stats, path)
    assert :not_found = LMDB.get(path, stale_a)
    assert :not_found = LMDB.get(path, stale_b)
  end

  test "terminal count reconciliation carries one bounded group across scan pages" do
    entries =
      (Enum.map(1..3, &terminal_entry("state:a", "a-#{&1}", &1)) ++
         Enum.map(1..2, &terminal_entry("state:b", "b-#{&1}", &1)) ++
         [terminal_entry("state:c", "c-1", 1)])
      |> Enum.sort()

    [page_a, page_b, page_c] = Enum.chunk_every(entries, 2)

    assert {:ok, state_a, []} =
             TerminalCounts.__page_count_ops_for_test__(page_a, {nil, 0, 0})

    assert {:ok, state_b, ops_b} =
             TerminalCounts.__page_count_ops_for_test__(page_b, state_a)

    assert ops_b == [{:put, LMDB.terminal_count_key("state:a"), LMDB.encode_count(3)}]

    assert {:ok, {count_c, 1, 2}, ops_c} =
             TerminalCounts.__page_count_ops_for_test__(page_c, state_b)

    assert count_c == LMDB.terminal_count_key("state:c")
    assert ops_c == [{:put, LMDB.terminal_count_key("state:b"), LMDB.encode_count(2)}]
    assert length(ops_b) <= length(page_b)
    assert length(ops_c) <= length(page_c)
  end

  test "terminal count reconciliation rejects a row owned by another count prefix" do
    {terminal_key, value} = terminal_entry("state:a", "flow-a", 1)
    wrong_count_key = LMDB.terminal_count_key("state:b")
    wrong_value = LMDB.encode_terminal_index_value("flow-a", 1, 0, "state-key", wrong_count_key)

    assert {:error, :invalid_terminal_index_count_entry} =
             TerminalCounts.__page_count_ops_for_test__(
               [{terminal_key, wrong_value}, {terminal_key, value}],
               {nil, 0, 0}
             )
  end

  defp terminal_put(state_index_key, id, updated_at_ms) do
    {key, value} = terminal_entry(state_index_key, id, updated_at_ms)
    {:put, key, value}
  end

  defp terminal_entry(state_index_key, id, updated_at_ms) do
    count_key = LMDB.terminal_count_key(state_index_key)
    key = LMDB.terminal_index_key(state_index_key, id, updated_at_ms)
    value = LMDB.encode_terminal_index_value(id, updated_at_ms, 0, "state:#{id}", count_key)
    {key, value}
  end
end
