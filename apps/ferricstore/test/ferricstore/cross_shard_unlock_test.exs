defmodule Ferricstore.CrossShardUnlockTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CrossShardOp

  test "unlock collection converts crashed and timed-out shard calls into retry work" do
    shards = [{1, ["one"]}, {2, ["two"]}, {3, ["three"]}]
    started_at = System.monotonic_time(:millisecond)

    unlock_fun = fn
      1, ["one"], _owner -> {:ok, :ok, :leader}
      2, ["two"], _owner -> raise "unlock transport crashed"
      3, ["three"], _owner -> Process.sleep(250)
    end

    assert [{2, ["two"]}, {3, ["three"]}] ==
             CrossShardOp.__attempt_unlock_for_test__(
               shards,
               make_ref(),
               unlock_fun,
               20
             )

    assert System.monotonic_time(:millisecond) - started_at < 200
  end
end
