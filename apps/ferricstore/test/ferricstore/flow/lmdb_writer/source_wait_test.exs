defmodule Ferricstore.Flow.LMDBWriter.SourceWaitTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, LMDBWriter}
  alias Ferricstore.Test.IsolatedInstance

  setup do
    settings = [
      flow_lmdb_flush_interval_ms: 60_000,
      flow_lmdb_flush_jitter_ms: 0,
      flow_lmdb_source_pending_retries: 40,
      flow_lmdb_source_pending_sleep_ms: 5
    ]

    previous =
      Map.new(settings, fn {key, _} -> {key, Application.fetch_env(:ferricstore, key)} end)

    for {key, value} <- settings, do: Application.put_env(:ferricstore, key, value)

    ctx =
      IsolatedInstance.checkout(
        shard_count: 2,
        query_index_provider: FerricStore.Flow.QueryIndexProvider.Disabled
      )

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)

      for {key, value} <- previous do
        case value do
          {:ok, old} -> Application.put_env(:ferricstore, key, old)
          :error -> Application.delete_env(:ferricstore, key)
        end
      end
    end)

    %{ctx: ctx}
  end

  test "a batch of missing versioned sources shares one wait window", %{ctx: ctx} do
    ops =
      for n <- 1..64,
          do:
            {:project_flow_query_state_from_source, Keys.state_key("missing-#{n}", "wait-window"),
             3}

    assert :ok = LMDBWriter.enqueue(ctx.name, 0, ops)
    started = System.monotonic_time(:millisecond)
    task = Task.async(fn -> LMDBWriter.flush(ctx.name, 0) end)

    try do
      assert {:ok, :ok} = Task.yield(task, 2_500)
      assert System.monotonic_time(:millisecond) - started < 2_500
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "waiting for a source does not monopolize the runtime LMDB permit", %{ctx: ctx} do
    key = Keys.state_key("pending", "permit-window")
    table = elem(ctx.keydir_refs, 0)
    :ets.insert(table, {key, nil, 0, 0, :pending, 0, 0})
    assert :ok = LMDBWriter.enqueue(ctx.name, 1, [{:put, "other-shard", "warm"}])
    assert :ok = LMDBWriter.flush(ctx.name, 1)
    assert :ok = LMDBWriter.enqueue(ctx.name, 0, [{:project_flow_state_from_source, key, 3}])
    blocked = Task.async(fn -> LMDBWriter.flush(ctx.name, 0) end)
    Process.sleep(15)
    assert :ok = LMDBWriter.enqueue(ctx.name, 1, [{:put, "other-shard", "progress"}])
    other = Task.async(fn -> LMDBWriter.flush(ctx.name, 1) end)

    try do
      assert {:ok, :ok} = Task.yield(other, 150)
      assert {:error, {:source_pending, ^key}} = Task.await(blocked, 2_500)
      source_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
      assert Ferricstore.Flow.LMDBReplaySafeIndex.read(source_path) == 0
      path = ctx.data_dir |> Ferricstore.DataDir.shard_data_path(1) |> LMDB.path()
      assert {:ok, "progress"} = LMDB.get(path, "other-shard")
    after
      Task.shutdown(other, :brutal_kill)
      Task.shutdown(blocked, :brutal_kill)
      :ets.delete(table, key)
    end
  end

  test "an unresolved source cannot authorize a requested replay-safe watermark", %{ctx: ctx} do
    key = Keys.state_key("watermark-pending", "watermark-window")
    table = elem(ctx.keydir_refs, 0)
    :ets.insert(table, {key, nil, 0, 0, :pending, 0, 0})
    path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    assert :ok = LMDBWriter.enqueue(ctx.name, 0, [{:project_flow_state_from_source, key, 3}])
    assert :requested = LMDBWriter.request(ctx, 0, path, 77)

    try do
      assert {:error, {:source_pending, ^key}} = LMDBWriter.flush(ctx.name, 0)
      assert Ferricstore.Flow.LMDBReplaySafeIndex.read(path) == 0
      assert LMDBWriter.durable_index(ctx, 0, path) == 0
    after
      :ets.delete(table, key)
    end
  end

  test "a source published during the retry window is validated before projection", %{ctx: ctx} do
    id = "eventual-source"

    partition =
      Stream.iterate(0, &(&1 + 1))
      |> Enum.find_value(fn n ->
        p = "eventual-#{n}"
        if Ferricstore.Store.Router.shard_for(ctx, Keys.state_key(id, p)) == 0, do: p
      end)

    assert :ok =
             FerricStore.Impl.flow_create(ctx, id,
               partition_key: partition,
               type: "eventual",
               state: "queued",
               payload: "checked"
             )

    key = Keys.state_key(id, partition)
    table = elem(ctx.keydir_refs, 0)
    [row] = :ets.lookup(table, key)
    :ets.insert(table, {key, nil, 0, 0, :pending, 0, 0})

    updater =
      Task.async(fn ->
        Process.sleep(40)
        :ets.insert(table, row)
      end)

    assert :ok = LMDBWriter.enqueue(ctx.name, 0, [{:project_flow_state_from_source, key, 1}])

    try do
      assert :ok = LMDBWriter.flush(ctx.name, 0)
      Task.await(updater)
      path = ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()
      assert {:ok, encoded} = LMDB.get(path, key)

      assert {:ok, %{record: %{version: 1, state: "queued"}, locator: locator}} =
               Ferricstore.Flow.Query.QueryRowCodec.decode(encoded, key)

      assert locator.file_id == elem(row, 4)
      assert locator.offset == elem(row, 5)
    after
      Task.shutdown(updater, :brutal_kill)
      :ets.insert(table, row)
    end
  end
end
