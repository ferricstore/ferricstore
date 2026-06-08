defmodule Ferricstore.BatchWorkerTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  @ns "batch_worker"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.reset(@ns)

    on_exit(fn ->
      Ferricstore.NamespaceConfig.reset(@ns)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "batch_set submits through quorum and returns per-key results" do
    key = "#{@ns}:quorum_#{System.unique_integer([:positive])}"

    {:ok, worker} = FerricStore.BatchWorker.start()

    try do
      assert [:ok] = GenServer.call(worker, {:batch_set, [{key, "written"}]})

      assert {:ok, "written"} == FerricStore.get(key)
    after
      GenServer.stop(worker)
    end
  end
end
