defmodule Ferricstore.Commands.TDigestMergeDestinationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.TDigest
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Test.MockStore

  test "merge never overwrites a destination holding another value type" do
    for opts <- [[], [override: true]] do
      store = MockStore.make()
      source = unique_key("source")
      destination = unique_key("string-destination")

      assert :ok == TDigest.handle_ast({:tdigest_create, source, 100}, store)
      assert :ok == TDigest.handle_ast({:tdigest_add, source, [1.0, 2.0]}, store)
      assert :ok == store.put.(destination, "plain string", 0)

      assert {:error, message} =
               TDigest.handle_ast({:tdigest_merge, destination, [source], opts}, store)

      assert message =~ "WRONGTYPE"
      assert "plain string" == store.get.(destination)
    end
  end

  test "destination read failures abort merge before its write" do
    for opts <- [[], [override: true]] do
      base = MockStore.make()
      source = unique_key("source")
      destination = unique_key("failed-destination")

      assert :ok == TDigest.handle_ast({:tdigest_create, source, 100}, base)
      assert :ok == TDigest.handle_ast({:tdigest_add, source, [1.0]}, base)

      store =
        Map.put(base, :get, fn
          ^destination -> ReadResult.failure(:disk_error)
          key -> base.get.(key)
        end)

      assert {:error, "ERR storage read failed"} ==
               TDigest.handle_ast({:tdigest_merge, destination, [source], opts}, store)

      assert nil == base.get.(destination)
    end
  end

  defp unique_key(prefix), do: "#{prefix}:#{System.unique_integer([:positive, :monotonic])}"
end
