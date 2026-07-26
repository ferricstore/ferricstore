defmodule Ferricstore.Flow.Query.QueryRowStoreTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB, Locator}
  alias Ferricstore.Flow.Query.{QueryRowCodec, QueryRowReference, QueryRowStore}

  test "reads a bounded prefix of query rows and applies expiry" do
    path = tmp_path()
    first_key = Keys.state_key("run-1", "tenant-a")
    second_key = Keys.state_key("run-2", "tenant-a")
    assert {:ok, first} = QueryRowCodec.encode(first_key, record("run-1"), locator("run-1"), 0)

    assert {:ok, second} =
             QueryRowCodec.encode(second_key, record("run-2"), locator("run-2", 500), 500)

    assert :ok = LMDB.write_batch(path, [{:put, first_key, first}, {:put, second_key, second}])

    assert {:ok, [%{record: %{id: "run-1"}}], bytes, false} =
             QueryRowStore.read_many(path, [first_key, second_key], 1_000, byte_size(first))

    assert bytes == byte_size(first)

    assert {:ok, [%{record: %{id: "run-1"}}, nil], _bytes, true} =
             QueryRowStore.read_many(path, [first_key, second_key], 1_000, 1_000_000)
  end

  test "fails closed on a corrupt row" do
    path = tmp_path()
    state_key = Keys.state_key("run-1", "tenant-a")
    assert :ok = LMDB.write_batch(path, [{:put, state_key, "corrupt"}])

    assert {:error, :invalid_query_row} =
             QueryRowStore.read_many(path, [state_key], 0, 1_000)
  end

  test "reads bounded hydration references without materializing metadata" do
    path = tmp_path()
    first_key = Keys.state_key("run-1", "tenant-a")
    second_key = Keys.state_key("run-2", "tenant-a")
    assert {:ok, first} = QueryRowCodec.encode(first_key, record("run-1"), locator("run-1"), 0)

    assert {:ok, second} =
             QueryRowCodec.encode(second_key, record("run-2"), locator("run-2", 500), 500)

    assert :ok = LMDB.write_batch(path, [{:put, first_key, first}, {:put, second_key, second}])

    assert {:ok,
            [
              %QueryRowReference{state_key: ^first_key, flow_id: "run-1", version: 1},
              nil
            ], bytes, true} =
             QueryRowStore.read_references_many(
               path,
               [first_key, second_key],
               1_000,
               1_000_000
             )

    assert bytes == byte_size(first) + byte_size(second)
  end

  defp record(id) do
    %{
      id: id,
      version: 1,
      type: "job",
      state: "waiting",
      partition_key: "tenant-a",
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: 1,
      priority: 0,
      attempts: 0
    }
  end

  defp locator(id, expire_at_ms \\ nil) do
    Locator.new!(
      flow_id: id,
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: 1,
      offset: 0,
      value_size: 100,
      checksum: :binary.copy(<<1>>, 32),
      expire_at_ms: expire_at_ms
    )
  end

  defp tmp_path do
    path =
      Path.join(
        System.tmp_dir!(),
        "query_row_store_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
