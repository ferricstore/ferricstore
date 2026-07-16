defmodule Ferricstore.Flow.LMDBIndexReadTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.LMDBIndexRead
  alias Ferricstore.Flow.LMDB

  setup do
    previous = Application.get_env(:ferricstore, :flow_lmdb_terminal_sweep_limit)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_lmdb_terminal_sweep_limit)
        value -> Application.put_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, value)
      end
    end)
  end

  test "query_entries returns no cold entries for non-positive count" do
    assert LMDBIndexRead.query_entries(:ctx, "idx", nil, 0, false, %{}, 1_000) == {:ok, []}
    assert LMDBIndexRead.query_entries_window(:ctx, "idx", nil, 0, false, %{}) == {:ok, []}
  end

  test "terminal_entries skips non-terminal states" do
    assert LMDBIndexRead.terminal_entries(
             :ctx,
             "idx",
             "queued",
             nil,
             10,
             true,
             false,
             nil,
             ["completed"],
             1_000
           ) == {:ok, []}
  end

  test "terminal_entries skips cold storage when disabled" do
    assert LMDBIndexRead.terminal_entries(
             :ctx,
             "idx",
             "completed",
             nil,
             10,
             false,
             false,
             nil,
             ["completed"],
             1_000
           ) == {:ok, []}
  end

  test "query_scan_count delegates to bounded LMDB query window" do
    assert LMDBIndexRead.query_scan_count(10, 1_000) == 74
  end

  test "exact query windows treat expired rows as scanned without claiming exhaustion" do
    {ctx, path} = tmp_lmdb_context()
    index_key = "parent:p1"

    assert :ok =
             LMDB.write_batch(path, [
               query_index_put(index_key, "expired-1", 1, 1),
               query_index_put(index_key, "expired-2", 2, 1),
               query_index_put(index_key, "live", 3, 0)
             ])

    query = %{from_ms: nil, to_ms: nil, rev?: false, before_id: nil}

    assert {:ok, [], false, 2} =
             LMDBIndexRead.query_entries_window_with_count(
               ctx,
               index_key,
               nil,
               2,
               false,
               query
             )
  end

  test "exact query windows report exhaustion within narrow time bounds" do
    {ctx, path} = tmp_lmdb_context()
    index_key = "parent:p1"

    assert :ok =
             LMDB.write_batch(
               path,
               Enum.map(1..5, fn updated_at_ms ->
                 query_index_put(
                   index_key,
                   "flow-#{updated_at_ms}",
                   updated_at_ms,
                   0
                 )
               end)
             )

    query = %{from_ms: 5, to_ms: 5, rev?: false, before_id: nil}

    assert {:ok, [{"flow-5", 5, nil}], true, 1} =
             LMDBIndexRead.query_entries_window_with_count(
               ctx,
               index_key,
               nil,
               2,
               false,
               query
             )
  end

  test "exact terminal windows sweep canonical expired rows before reporting exhaustion" do
    {ctx, path} = tmp_lmdb_context()
    index_key = "state:completed"
    count_key = LMDB.terminal_count_key(index_key)

    terminal_ops =
      Enum.flat_map(
        [
          {"expired-1", 1, 1},
          {"expired-2", 2, 1},
          {"live", 3, 0}
        ],
        fn {id, updated_at_ms, expire_at_ms} ->
          terminal_index_put_ops(index_key, count_key, id, updated_at_ms, expire_at_ms)
        end
      )

    assert :ok =
             LMDB.write_batch(
               path,
               [{:put, count_key, LMDB.encode_count(3)} | terminal_ops]
             )

    query = %{from_ms: nil, to_ms: nil, rev?: false}

    assert {:ok, [{"live", 3}], true, 1} =
             LMDBIndexRead.terminal_entries_window_with_count(
               ctx,
               index_key,
               "completed",
               nil,
               2,
               true,
               false,
               query,
               ["completed"]
             )
  end

  test "raw prefix discovery applies its candidate limit globally across LMDB paths" do
    {ctx, [path_0, path_1]} = tmp_lmdb_paths(2)
    index_key_prefix = "attribute:"

    assert :ok =
             LMDB.write_batch(path_0, [
               query_index_put("attribute:a", "a-1", 1, 0),
               query_index_put("attribute:c", "c-1", 3, 0),
               query_index_put("attribute:e", "e-1", 5, 0)
             ])

    assert :ok =
             LMDB.write_batch(path_1, [
               query_index_put("attribute:b", "b-1", 2, 0),
               query_index_put("attribute:d", "d-1", 4, 0),
               query_index_put("attribute:f", "f-1", 6, 0)
             ])

    assert {:ok, chunks} =
             LMDBIndexRead.query_prefix_raw_entries(
               ctx,
               index_key_prefix,
               nil,
               3,
               false
             )

    assert Enum.sum(Enum.map(chunks, fn {_path, entries} -> length(entries) end)) == 3
  end

  test "terminal expiry sweep limits remain positive when configuration is malformed" do
    Application.put_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, 17)
    assert LMDBIndexRead.__terminal_lmdb_sweep_limit_for_test__() == 17
    assert LMDBIndexRead.__terminal_lmdb_sweep_limit_for_test__(5) == 5

    for invalid <- [0, -1, "all", nil] do
      Application.put_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, invalid)
      assert LMDBIndexRead.__terminal_lmdb_sweep_limit_for_test__() == 10_000
      assert LMDBIndexRead.__terminal_lmdb_sweep_limit_for_test__(5) == 5
    end
  end

  defp query_index_put(index_key, id, updated_at_ms, expire_at_ms) do
    key = LMDB.query_index_key(index_key, id, updated_at_ms)
    value = LMDB.encode_query_index_value(id, updated_at_ms, expire_at_ms)
    {:put, key, value}
  end

  defp terminal_index_put_ops(index_key, count_key, id, updated_at_ms, expire_at_ms) do
    key = LMDB.terminal_index_key(index_key, id, updated_at_ms)
    value = LMDB.encode_terminal_index_value(id, updated_at_ms, expire_at_ms, nil, count_key)
    ops = [{:put, key, value}]

    if expire_at_ms > 0 do
      expire_key = LMDB.terminal_expire_key(expire_at_ms, key)
      expire_value = LMDB.encode_terminal_expire_value(key, nil, count_key)
      [{:put, expire_key, expire_value} | ops]
    else
      ops
    end
  end

  defp tmp_lmdb_context do
    {ctx, [path]} = tmp_lmdb_paths(1)
    {ctx, path}
  end

  defp tmp_lmdb_paths(shard_count) do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-lmdb-index-read-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(data_dir)
    assert :ok = LMDB.ensure_shard_dirs(data_dir, shard_count)
    on_exit(fn -> File.rm_rf!(data_dir) end)

    paths =
      Enum.map(0..(shard_count - 1), fn shard_index ->
        data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> LMDB.path()
      end)

    {%{data_dir: data_dir, shard_count: shard_count}, paths}
  end
end
