defmodule Ferricstore.Commands.ProbLocalInstanceTest do
  # Custom instances apply commands directly, without a Raft log or index.
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout()
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  defp run(ctx, command, args), do: Dispatcher.dispatch(command, args, ctx)

  test "every probabilistic write on a key is applied, not only the first", %{ctx: ctx} do
    assert 1 == run(ctx, "BF.ADD", ["bf", "alpha"])
    assert 1 == run(ctx, "BF.ADD", ["bf", "beta"])
    assert [1, 1] == run(ctx, "BF.MADD", ["bf", "gamma", "delta"])
    assert 1 == run(ctx, "BF.ADD", ["bf", "epsilon"])

    assert [1, 1, 1, 1, 1, 0] ==
             run(ctx, "BF.MEXISTS", ["bf", "alpha", "beta", "gamma", "delta", "epsilon", "zeta"])

    assert :ok == run(ctx, "CMS.INITBYDIM", ["cms", "50", "4"])
    assert [3] == run(ctx, "CMS.INCRBY", ["cms", "x", "3"])
    assert [5] == run(ctx, "CMS.INCRBY", ["cms", "y", "5"])
    assert [4] == run(ctx, "CMS.INCRBY", ["cms", "x", "1"])
    assert [4, 5] == run(ctx, "CMS.QUERY", ["cms", "x", "y"])

    assert 1 == run(ctx, "CF.ADD", ["cf", "a"])
    assert 1 == run(ctx, "CF.ADD", ["cf", "b"])
    assert [1, 1] == run(ctx, "CF.MEXISTS", ["cf", "a", "b"])

    assert :ok == run(ctx, "TOPK.RESERVE", ["tk", "2"])
    run(ctx, "TOPK.ADD", ["tk", "p"])
    run(ctx, "TOPK.ADD", ["tk", "p"])
    run(ctx, "TOPK.ADD", ["tk", "q"])
    assert ["p", 2, "q", 1] == run(ctx, "TOPK.LIST", ["tk", "WITHCOUNT"])
  end

  test "creating a probabilistic key that exists is refused", %{ctx: ctx} do
    assert :ok == run(ctx, "BF.RESERVE", ["bf", "0.01", "100"])
    assert {:error, "ERR item exists"} = run(ctx, "BF.RESERVE", ["bf", "0.01", "100"])
    assert :ok == run(ctx, "CMS.INITBYDIM", ["cms", "10", "2"])
    assert {:error, _exists} = run(ctx, "CMS.INITBYDIM", ["cms", "10", "2"])
  end
end
