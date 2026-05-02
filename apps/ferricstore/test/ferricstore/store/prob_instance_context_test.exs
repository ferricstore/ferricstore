defmodule Ferricstore.Store.ProbInstanceContextTest do
  @moduledoc false

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "probabilistic reads use the passed instance context", %{ctx: ctx} do
    key = "custom_prob_#{System.unique_integer([:positive])}"

    assert :ok = Bloom.handle("BF.RESERVE", [key, "0.01", "100"], ctx)
    assert 1 = Bloom.handle("BF.ADD", [key, "hello"], ctx)
    assert 1 = Bloom.handle("BF.EXISTS", [key, "hello"], ctx)
  end
end
