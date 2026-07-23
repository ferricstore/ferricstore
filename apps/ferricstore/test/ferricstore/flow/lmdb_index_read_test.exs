defmodule Ferricstore.Flow.LMDBIndexReadTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.LMDBIndexRead
  alias Ferricstore.Flow.LMDB

  @prefix_merge_max_bytes 16 * 1_024 * 1_024

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

  test "single-shard reverse query windows retain semantic order for digest-backed ids" do
    {ctx, path} = tmp_lmdb_context()
    index_key = "parent:p1"

    ids = [String.duplicate("z", 300), String.duplicate("a", 300), String.duplicate("m", 300)]

    assert :ok =
             LMDB.write_batch(
               path,
               Enum.map(ids, &query_index_put(index_key, &1, 100, 0))
             )

    query = %{from_ms: nil, to_ms: nil, rev?: true, before_id: nil}

    assert {:ok, entries, true, 3} =
             LMDBIndexRead.query_entries_window_with_count(
               ctx,
               index_key,
               nil,
               3,
               false,
               query
             )

    assert Enum.map(entries, fn {id, _updated_at_ms, _state_key} -> id end) == Enum.sort(ids)
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

  test "single-shard reverse terminal windows retain semantic order for digest-backed ids" do
    {ctx, path} = tmp_lmdb_context()
    index_key = "state:completed"
    count_key = LMDB.terminal_count_key(index_key)
    ids = [String.duplicate("z", 300), String.duplicate("a", 300), String.duplicate("m", 300)]

    terminal_ops =
      Enum.flat_map(ids, fn id ->
        terminal_index_put_ops(index_key, count_key, id, 100, 0)
      end)

    assert :ok =
             LMDB.write_batch(
               path,
               [{:put, count_key, LMDB.encode_count(length(ids))} | terminal_ops]
             )

    query = %{from_ms: nil, to_ms: nil, rev?: true}

    assert {:ok, entries, true, 3} =
             LMDBIndexRead.terminal_entries_window_with_count(
               ctx,
               index_key,
               "completed",
               nil,
               3,
               true,
               false,
               query,
               ["completed"]
             )

    assert Enum.map(entries, fn {id, _updated_at_ms} -> id end) == Enum.sort(ids, :desc)
  end

  test "raw prefix discovery applies its candidate limit globally across LMDB paths" do
    {ctx, [path_0, path_1]} = tmp_lmdb_paths(2)

    index_key_prefix =
      Ferricstore.Flow.Keys.attribute_index_prefix("job", "queued", "color", "tenant-a")

    index_key = fn value ->
      Ferricstore.Flow.Keys.attribute_index_key(
        "job",
        "queued",
        "color",
        Ferricstore.Flow.Attributes.index_value(value),
        "tenant-a"
      )
    end

    assert :ok =
             LMDB.write_batch(path_0, [
               query_index_put(index_key.("a"), "a-1", 1, 0),
               query_index_put(index_key.("c"), "c-1", 3, 0),
               query_index_put(index_key.("e"), "e-1", 5, 0)
             ])

    assert :ok =
             LMDB.write_batch(path_1, [
               query_index_put(index_key.("b"), "b-1", 2, 0),
               query_index_put(index_key.("d"), "d-1", 4, 0),
               query_index_put(index_key.("f"), "f-1", 6, 0)
             ])

    assert {:ok, chunks} =
             LMDBIndexRead.query_prefix_raw_entries(
               ctx,
               index_key_prefix,
               nil,
               3,
               false
             )

    raw_prefix = LMDB.query_index_raw_prefix(index_key_prefix)
    assert {:ok, entries_0} = LMDB.prefix_entries(path_0, raw_prefix, 3)
    assert {:ok, entries_1} = LMDB.prefix_entries(path_1, raw_prefix, 3)

    assert chunks == reference_raw_path_chunks([{path_0, entries_0}, {path_1, entries_1}], 3)
  end

  test "native prefix merge preserves duplicate rows, source ownership, and scan bounds" do
    {_ctx, [path_0, path_1]} = tmp_lmdb_paths(2)
    prefix = "merge:"

    assert :ok =
             LMDB.write_batch(path_0, [
               {:put, prefix <> "a", "a-0"},
               {:put, prefix <> "c", "c-0"},
               {:put, prefix <> "e", "e-0"}
             ])

    assert :ok =
             LMDB.write_batch(path_1, [
               {:put, prefix <> "a", "a-1"},
               {:put, prefix <> "b", "b-1"},
               {:put, prefix <> "d", "d-1"}
             ])

    assert {:ok,
            [
              {0, "merge:a", "a-0"},
              {1, "merge:a", "a-1"},
              {1, "merge:b", "b-1"},
              {0, "merge:c", "c-0"}
            ], scanned} = LMDB.prefix_merge_entries([path_0, path_1], prefix, 4, 40)

    assert scanned <= 8
    assert {:ok, [], 0} = LMDB.prefix_merge_entries([path_0, path_1], prefix, 0, 1)

    assert {:error, :prefix_merge_byte_budget_exceeded} =
             LMDB.prefix_merge_entries([path_0, path_1], prefix, 4, 39)

    assert {:error, :invalid_lmdb_prefix_merge} =
             LMDB.prefix_merge_entries([], prefix, 4, @prefix_merge_max_bytes)

    assert {:error, :invalid_lmdb_prefix_merge} =
             LMDB.prefix_merge_entries([path_0], "", 4, @prefix_merge_max_bytes)

    assert {:error, :invalid_lmdb_prefix_merge} =
             LMDB.prefix_merge_entries([path_0], prefix, 4, 0)
  end

  test "prefix merge applies the byte cap to the globally selected rows" do
    {_ctx, [path_0, path_1]} = tmp_lmdb_paths(2)
    prefix = "merge-cap:"
    selected_key = prefix <> "a"

    assert :ok = LMDB.write_batch(path_0, [{:put, prefix <> "z", :binary.copy("z", 4_096)}])
    assert :ok = LMDB.write_batch(path_1, [{:put, selected_key, "v"}])
    selected_bytes = byte_size(selected_key) + 1

    assert {:ok, [{1, ^selected_key, "v"}], 2} =
             LMDB.prefix_merge_entries([path_0, path_1], prefix, 1, selected_bytes)

    assert {:error, :prefix_merge_byte_budget_exceeded} =
             LMDB.prefix_merge_entries([path_0, path_1], prefix, 1, selected_bytes - 1)
  end

  test "raw prefix discovery preserves the bounded LMDB order for one shard" do
    {ctx, path} = tmp_lmdb_context()

    index_key_prefix =
      Ferricstore.Flow.Keys.attribute_index_prefix("job", "queued", "color", "tenant-a")

    puts =
      Enum.map(1..5, fn index ->
        index_key =
          Ferricstore.Flow.Keys.attribute_index_key(
            "job",
            "queued",
            "color",
            Ferricstore.Flow.Attributes.index_value("value-#{index}"),
            "tenant-a"
          )

        query_index_put(index_key, "flow-#{index}", index, 0)
      end)

    assert :ok = LMDB.write_batch(path, puts)

    raw_prefix = LMDB.query_index_raw_prefix(index_key_prefix)
    assert {:ok, all_entries} = LMDB.prefix_entries(path, raw_prefix, 10)

    assert {:ok, [{^path, entries}]} =
             LMDBIndexRead.query_prefix_raw_entries(
               ctx,
               index_key_prefix,
               nil,
               3,
               false
             )

    assert entries == Enum.take(all_entries, 3)
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
    {key, value} = LMDB.query_index_entry(index_key, id, updated_at_ms, expire_at_ms)
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

  defp reference_raw_path_chunks(chunks, count) do
    selected_by_path =
      chunks
      |> Enum.flat_map(fn {path, entries} -> Enum.map(entries, &{path, &1}) end)
      |> Enum.sort_by(fn {_path, {key, _value}} -> key end)
      |> Enum.take(count)
      |> Enum.group_by(
        fn {path, _entry} -> path end,
        fn {_path, entry} -> entry end
      )

    Enum.flat_map(chunks, fn {path, _entries} ->
      case Map.fetch(selected_by_path, path) do
        {:ok, entries} -> [{path, entries}]
        :error -> []
      end
    end)
  end
end
