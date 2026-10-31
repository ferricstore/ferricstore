defmodule Ferricstore.Commands.ListLmpopTest do
  @moduledoc "LMPOP, as Redis answers it."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Transaction.ReplicaCompat

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "lmpop:{#{System.unique_integer([:positive])}}"

    %{
      ctx: ctx,
      d: fn command, args -> Dispatcher.dispatch(command, args, ctx) end,
      k: &(tag <> &1)
    }
  end

  test "pops from the first list with elements", %{d: d, k: k} do
    3 = d.("RPUSH", [k.("b"), "1", "2", "3"])
    2 = d.("RPUSH", [k.("c"), "x", "y"])

    assert [k.("b"), ["1"]] == d.("LMPOP", ["3", k.("a"), k.("b"), k.("c"), "LEFT"])
    assert [k.("b"), ["3", "2"]] == d.("LMPOP", ["2", k.("b"), k.("c"), "RIGHT", "COUNT", "5"])
    assert 0 == d.("EXISTS", [k.("b")])
    assert [k.("c"), ["x", "y"]] == d.("LMPOP", ["1", k.("c"), "left", "count", "2"])
    assert nil == d.("LMPOP", ["2", k.("a"), k.("c"), "LEFT"])
  end

  test "errors", %{d: d, k: k} do
    :ok = d.("SET", [k.("s"), "v"])
    assert {:error, "WRONGTYPE" <> _} = d.("LMPOP", ["1", k.("s"), "LEFT"])

    assert {:error, "ERR numkeys should be greater than 0"} ==
             d.("LMPOP", ["0", k.("a"), "LEFT"])

    assert {:error, "ERR syntax error"} == d.("LMPOP", ["2", k.("a"), "LEFT"])
    assert {:error, "ERR syntax error"} == d.("LMPOP", ["1", k.("a"), "UP"])

    assert {:error, "ERR count should be greater than 0"} ==
             d.("LMPOP", ["1", k.("a"), "LEFT", "COUNT", "0"])

    assert {:error, "ERR wrong number of arguments for 'lmpop' command"} ==
             d.("LMPOP", ["1", k.("a")])
  end

  test "LMPOP fires the event of the side it popped", %{d: d, k: k} do
    :ok = Ferricstore.Config.set("notify-keyspace-events", "KEA")

    on_exit(fn ->
      Ferricstore.Config.set("notify-keyspace-events", "")
      Ferricstore.PubSub.cleanup(self())
    end)

    :ok = Ferricstore.PubSub.subscribe("__keyevent@0__:rpop", self())
    key = k.("l")
    2 = d.("RPUSH", [key, "1", "2"])
    [^key, ["2"]] = d.("LMPOP", ["1", key, "RIGHT"])
    assert_receive {:pubsub_message, "__keyevent@0__:rpop", ^key}, 1_000
  end

  test "inside MULTI it needs feature level 12", %{ctx: ctx, k: k} do
    assert {:ok, prepared} = PreparedCommand.prepare("LMPOP", ["1", k.("l"), "LEFT"])
    assert [k.("l")] == prepared.command_keys
    assert 12 == ReplicaCompat.required_level(prepared.ast)
    assert ReplicaCompat.queue_error("LMPOP", prepared.ast, prepared.command_keys, ctx)
  end
end

defmodule Ferricstore.Commands.ListLmpopLevel12Test do
  @moduledoc "From feature level 12, LMPOP of several keys runs where it applies."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 12}} = FerricStore.raise_cluster_feature_level(12)
    :ok
  end

  test "plain and in a transaction" do
    ctx = FerricStore.Instance.get(:default)
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    tag = "lmpop12:{#{System.unique_integer([:positive])}}"
    2 = d.("RPUSH", [tag <> "b", "1", "2"])

    assert [tag <> "b", ["1"]] == d.("LMPOP", ["2", tag <> "a", tag <> "b", "LEFT"])

    {:ok, prepared} = PreparedCommand.prepare("LMPOP", ["2", tag <> "a", tag <> "b", "RIGHT"])

    assert [[tag <> "b", ["2"]]] ==
             Ferricstore.Transaction.Coordinator.execute([prepared], %{}, nil)
  end
end
