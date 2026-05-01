defmodule Ferricstore.BatchWorkerTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.DiskPressure
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @ns "batch_worker"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns, "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "batch_set returns async per-key errors instead of pretending success" do
    ctx = FerricStore.Instance.get(:default)
    key = "#{@ns}:pressure_#{System.unique_integer([:positive])}"
    idx = Router.shard_for(ctx, key)

    DiskPressure.set(ctx, idx)

    {:ok, worker} = FerricStore.BatchWorker.start()

    try do
      assert [{:error, "ERR disk pressure on shard " <> _}] =
               GenServer.call(worker, {:batch_set, [{key, "blocked"}]})

      assert {:ok, nil} == FerricStore.get(key)
    after
      DiskPressure.clear(ctx, idx)
      GenServer.stop(worker)
    end
  end
end
