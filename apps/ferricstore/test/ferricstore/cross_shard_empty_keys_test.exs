defmodule Ferricstore.CrossShardEmptyKeysTest do
  use ExUnit.Case, async: true

  test "empty operations are rejected before shard coordination" do
    execute = fn _store -> flunk("empty operations must not execute") end

    assert {:error, "ERR cross-shard operation requires at least one key"} =
             Ferricstore.CrossShardOp.execute([], execute,
               instance: FerricStore.Instance.get(:default)
             )
  end
end
