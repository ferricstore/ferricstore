defmodule Ferricstore.Flow.LMDBRebuilder.TerminalProjectionTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBRebuilder.TerminalProjection

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

    assert :ok =
             LMDB.write_batch(path, [
               {:put, existing_key, LMDB.encode_count(99)},
               {:put, stale_a, LMDB.encode_count(7)},
               {:put, stale_b, LMDB.encode_count(11)}
             ])

    stats = %{
      terminal_counts: %{existing_key => 3, new_key => 5},
      lmdb_errors: 0
    }

    assert ^stats = TerminalProjection.persist_terminal_counts(stats, path)
    assert {:ok, value} = LMDB.get(path, existing_key)
    assert value == LMDB.encode_count(3)
    assert {:ok, value} = LMDB.get(path, new_key)
    assert value == LMDB.encode_count(5)
    assert :not_found = LMDB.get(path, stale_a)
    assert :not_found = LMDB.get(path, stale_b)
    assert {:ok, [0]} = LMDB.terminal_counts(path, [stale_state_index_key])

    assert ^stats = TerminalProjection.persist_terminal_counts(stats, path)
    assert :not_found = LMDB.get(path, stale_a)
    assert :not_found = LMDB.get(path, stale_b)
  end

  test "terminal count reconciliation uses bounded retryable write pages" do
    desired_a = LMDB.terminal_count_key("state:a")
    desired_b = LMDB.terminal_count_key("state:b")
    desired_c = LMDB.terminal_count_key("state:c")
    stale_a = LMDB.terminal_count_key("state:stale-a")
    stale_b = LMDB.terminal_count_key("state:stale-b")
    counts = %{desired_a => 2, desired_b => 3, desired_c => 5}

    pages = [
      [{desired_a, LMDB.encode_count(1)}, {stale_a, LMDB.encode_count(7)}],
      [{desired_b, LMDB.encode_count(1)}, {stale_b, LMDB.encode_count(11)}],
      [{desired_c, LMDB.encode_count(1)}]
    ]

    parent = self()

    scan_fun = fn page_fun ->
      Enum.reduce_while(pages, :ok, fn page, :ok ->
        case page_fun.(page) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end

    write_fun = fn ops ->
      send(parent, {:terminal_count_batch, ops})
      :ok
    end

    assert :ok =
             TerminalProjection.__reconcile_terminal_counts_for_test__(
               counts,
               2,
               scan_fun,
               write_fun
             )

    batches =
      for _index <- 1..4 do
        assert_receive {:terminal_count_batch, ops}
        ops
      end

    assert Enum.all?(batches, &(length(&1) <= 2))

    {put_batches, delete_batches} =
      Enum.split_while(batches, fn ops ->
        Enum.all?(ops, &match?({:put, _key, _value}, &1))
      end)

    assert length(put_batches) == 2
    assert length(delete_batches) == 2

    assert MapSet.new(List.flatten(put_batches)) ==
             MapSet.new([
               {:put, desired_a, LMDB.encode_count(2)},
               {:put, desired_b, LMDB.encode_count(3)},
               {:put, desired_c, LMDB.encode_count(5)}
             ])

    assert MapSet.new(List.flatten(delete_batches)) ==
             MapSet.new([{:delete, stale_a}, {:delete, stale_b}])
  end

  test "terminal count reconciliation does not delete stale keys after a desired write failure" do
    desired_key = LMDB.terminal_count_key("state:desired")
    parent = self()

    scan_fun = fn _page_fun ->
      send(parent, :terminal_count_scan_started)
      :ok
    end

    assert {:error, :busy} =
             TerminalProjection.__reconcile_terminal_counts_for_test__(
               %{desired_key => 1},
               1,
               scan_fun,
               fn _ops -> {:error, :busy} end
             )

    refute_receive :terminal_count_scan_started
  end

  test "terminal reverse scans preserve backend failures" do
    keydir = :ets.new(:terminal_projection_reverse_keydir, [:set])

    assert {:ok, []} =
             TerminalProjection.__cleanup_stale_terminal_reverse_scan_result_for_test__(
               {:ok, []},
               keydir,
               fn _entry -> [] end
             )

    assert {:error, :busy} =
             TerminalProjection.__cleanup_stale_terminal_reverse_scan_result_for_test__(
               {:error, :busy},
               keydir,
               fn _entry -> [] end
             )
  end

  test "terminal reverse scans preserve strict delete-planning failures" do
    keydir = :ets.new(:terminal_projection_delete_keydir, [:set])
    state_key = "flow-state-key"
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    assert {:error, :busy} =
             TerminalProjection.__cleanup_stale_terminal_reverse_scan_with_delete_for_test__(
               {:ok, [{reverse_key, "terminal-key"}]},
               keydir,
               fn _entry -> [] end,
               fn "terminal-key", nil -> {:error, :busy} end
             )
  end

  test "terminal count-key scans preserve backend failures" do
    assert {:ok, [{:delete, "count-b"}]} =
             TerminalProjection.__stale_terminal_count_delete_ops_result_for_test__(
               {:ok, [{"count-a", "1"}, {"count-b", "2"}]},
               %{"count-a" => 1}
             )

    assert {:error, :busy} =
             TerminalProjection.__stale_terminal_count_delete_ops_result_for_test__(
               {:error, :busy},
               %{}
             )
  end

  test "stale terminal cleanup never deletes the current active state row" do
    state_key = "current-active-state"
    terminal_key = "stale-terminal-key"

    terminal_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        "flow-id",
        10,
        0,
        state_key,
        "count-key"
      )

    parent = self()

    delete_fun = fn ^terminal_key, delete_state_key ->
      send(parent, {:delete_state_key, delete_state_key})
      {:ok, [{:delete, terminal_key}]}
    end

    assert {:ok, ops} =
             TerminalProjection.__cleanup_stale_terminal_entries_for_test__(
               {:ok, [{terminal_key, terminal_value}]},
               "flow-id",
               state_key,
               false,
               delete_fun
             )

    assert_receive {:delete_state_key, nil}
    refute {:delete, state_key} in ops
    assert {:delete, Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)} in ops
  end

  test "terminal rebuild scan limits reject non-positive, malformed, and excessive values" do
    assert TerminalProjection.__normalize_scan_limit_for_test__(0) == 4_096
    assert TerminalProjection.__normalize_scan_limit_for_test__("all") == 4_096
    assert TerminalProjection.__normalize_scan_limit_for_test__(2_000_000) == 65_536
    assert TerminalProjection.__normalize_scan_limit_for_test__(5_000) == 5_000
  end

  test "active-record cleanup does not fall back to a full terminal-index scan" do
    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore/flow/lmdb_rebuilder/terminal_projection.ex",
          __DIR__
        )
      )

    refute source =~ "LMDB.terminal_index_global_prefix()"
  end
end
