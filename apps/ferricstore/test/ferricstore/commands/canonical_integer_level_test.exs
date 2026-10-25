defmodule Ferricstore.Commands.CanonicalIntegerLevelTest do
  @moduledoc """
  Redis reads an INCR or HINCRBY operand with string2ll, which takes only an
  integer's canonical form: "05", "+5", "-0" and "00" are not integers. From
  cluster feature level 6 FerricStore refuses them too; below it apply keeps
  accepting them, as older releases apply the same entries.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  @non_canonical ["05", "+5", "-0", "00", "-007"]

  defp cmd(ctx, name, args), do: Dispatcher.dispatch(name, args, ctx)
  defp ukey, do: "{canonical-int:#{System.unique_integer([:positive])}}"

  describe "below feature level 6" do
    test "INCR and HINCRBY still accept a non-canonical stored integer" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, %{active: active}} = FerricStore.cluster_feature_level()
      assert active < 6

      key = ukey()
      assert :ok == cmd(ctx, "SET", [key, "05"])
      assert {:ok, 6} == cmd(ctx, "INCR", [key])

      hash = ukey()
      assert 1 == cmd(ctx, "HSET", [hash, "f", "+5"])
      assert 6 == cmd(ctx, "HINCRBY", [hash, "f", "1"])
    end
  end

  describe "at feature level 6" do
    setup do
      isolated = ShardHelpers.setup_isolated_data_dir()
      on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
      assert {:ok, %{active: 6}} = FerricStore.raise_cluster_feature_level(6)
      %{ctx: FerricStore.Instance.get(:default)}
    end

    test "INCR, INCRBY and DECR refuse a non-canonical stored integer", %{ctx: ctx} do
      for stored <- @non_canonical, command <- [["INCR"], ["INCRBY", "2"], ["DECR"]] do
        key = ukey()
        [name | rest] = command
        assert :ok == cmd(ctx, "SET", [key, stored])

        assert {:error, "ERR value is not an integer or out of range"} ==
                 cmd(ctx, name, [key | rest]),
               "#{name} of #{inspect(stored)}"

        assert stored == cmd(ctx, "GET", [key])
      end

      key = ukey()
      assert :ok == cmd(ctx, "SET", [key, "-5"])
      assert {:ok, -4} == cmd(ctx, "INCR", [key])
    end

    test "HINCRBY refuses a non-canonical stored integer", %{ctx: ctx} do
      for stored <- @non_canonical do
        hash = ukey()
        assert 1 == cmd(ctx, "HSET", [hash, "f", stored])

        assert {:error, "ERR hash value is not an integer"} ==
                 cmd(ctx, "HINCRBY", [hash, "f", "1"]),
               "HINCRBY of #{inspect(stored)}"
      end
    end

    test "INCR in a transaction refuses a non-canonical stored integer", %{ctx: ctx} do
      key = ukey()
      assert :ok == cmd(ctx, "SET", [key, "05"])

      assert {:ok, [result]} = FerricStore.multi(fn tx -> FerricStore.Tx.incr(tx, key) end)
      assert {:error, "ERR value is not an integer or out of range"} = result
      assert "05" == cmd(ctx, "GET", [key])
    end
  end
end
