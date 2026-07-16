defmodule Ferricstore.Commands.ZsetCleanupFailureTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Test.MockStore

  test "ZREM restores the member when empty-zset cleanup cannot read the count" do
    base = MockStore.make()
    assert 1 == SortedSet.handle("ZADD", ["zset", "1", "member"], base)

    store =
      Map.put(base, :zset_score_count, fn "zset", :neg_inf, :inf ->
        ReadResult.failure(:disk_read_failed)
      end)

    assert {:error, "ERR storage read failed"} =
             SortedSet.handle("ZREM", ["zset", "member"], store)

    assert "1.0" == SortedSet.handle("ZSCORE", ["zset", "member"], base)
  end
end
