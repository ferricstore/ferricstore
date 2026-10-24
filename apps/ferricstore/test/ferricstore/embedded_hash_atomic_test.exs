defmodule Ferricstore.EmbeddedHashAtomicTest do
  @moduledoc """
  At feature level 4 the gateways run HDEL and HSETNX as one replicated entry,
  so concurrent clients each see one outcome. FerricStore.hdel/2 and
  FerricStore.hsetnx/3 must do the same.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 4}} = FerricStore.raise_cluster_feature_level(4)
    :ok
  end

  setup do
    %{key: "{embedded-hash:#{System.unique_integer([:positive])}}"}
  end

  test "concurrent hdel of one field removes it for one caller", %{key: key} do
    for _round <- 1..5 do
      assert :ok = FerricStore.hset(key, %{"f" => "v"})
      results = concurrently(16, fn _ -> FerricStore.hdel(key, ["f"]) end)
      assert 1 == Enum.count(results, &(&1 == {:ok, 1})), inspect(results)
    end
  end

  # HSET through the command path, whose reply says whether it created the
  # field.
  test "hsetnx racing HSET: one of them creates the field", %{key: key} do
    ctx = FerricStore.Instance.get(:default)

    for round <- 1..10 do
      field = "f#{round}"

      results =
        concurrently(16, fn
          1 -> {:nx, FerricStore.hsetnx(key, field, "nx")}
          client -> {:set, Dispatcher.dispatch("HSET", [key, field, "s#{client}"], ctx)}
        end)

      created =
        Enum.count(results, fn
          {:nx, {:ok, true}} -> true
          {:set, 1} -> true
          _other -> false
        end)

      assert created == 1, "round #{round}: #{inspect(results)}"
    end
  end

  defp concurrently(n, fun) do
    1..n
    |> Task.async_stream(fun, max_concurrency: n, timeout: 60_000)
    |> Enum.map(fn {:ok, result} -> result end)
  end
end
