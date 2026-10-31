defmodule Ferricstore.Commands.HmsetTouchTimeTest do
  @moduledoc "HMSET, TOUCH and TIME, as Redis answers them."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, PreparedCommand}
  alias Ferricstore.Transaction.ReplicaCompat

  setup do
    ctx = FerricStore.Instance.get(:default)
    tag = "added:{#{System.unique_integer([:positive])}}"

    %{
      ctx: ctx,
      d: fn command, args -> Dispatcher.dispatch(command, args, ctx) end,
      k: &(tag <> &1)
    }
  end

  test "HMSET sets fields and answers OK", %{d: d, k: k} do
    assert :ok == d.("HMSET", [k.("h"), "a", "1", "b", "2"])
    assert :ok == d.("HMSET", [k.("h"), "a", "3"])
    assert ["3", "2"] == d.("HMGET", [k.("h"), "a", "b"])

    assert {:error, "ERR wrong number of arguments for 'hmset' command"} ==
             d.("HMSET", [k.("h"), "a"])

    assert {:error, "ERR wrong number of arguments for 'hmset' command"} ==
             d.("HMSET", [k.("h"), "a", "1", "b"])

    :ok = d.("SET", [k.("s"), "v"])
    assert {:error, "WRONGTYPE" <> _} = d.("HMSET", [k.("s"), "a", "1"])
  end

  test "HMSET fires Redis's hset event", %{d: d, k: k} do
    :ok = Ferricstore.Config.set("notify-keyspace-events", "KEA")

    on_exit(fn ->
      Ferricstore.Config.set("notify-keyspace-events", "")
      Ferricstore.PubSub.cleanup(self())
    end)

    :ok = Ferricstore.PubSub.subscribe("__keyevent@0__:hset", self())
    key = k.("h")
    :ok = d.("HMSET", [key, "a", "1"])
    assert_receive {:pubsub_message, "__keyevent@0__:hset", ^key}, 1_000
  end

  test "TOUCH counts the keys that exist", %{d: d, k: k} do
    :ok = d.("SET", [k.("a"), "v"])
    1 = d.("HSET", [k.("b"), "f", "v"])
    assert 2 == d.("TOUCH", [k.("a"), k.("b"), k.("missing")])
    assert 0 == d.("TOUCH", [k.("missing")])
    assert {:error, "ERR wrong number of arguments for 'touch' command"} == d.("TOUCH", [])
  end

  test "TIME answers the server's seconds and microseconds", %{d: d} do
    assert [seconds, micros] = d.("TIME", [])
    assert abs(String.to_integer(seconds) - System.os_time(:second)) <= 2
    assert String.to_integer(micros) in 0..999_999
    assert {:error, "ERR wrong number of arguments for 'time' command"} == d.("TIME", ["x"])
  end

  test "inside MULTI, HMSET and TOUCH need feature level 12", %{ctx: ctx, k: k} do
    for {command, args} <- [{"HMSET", [k.("h"), "a", "1"]}, {"TOUCH", [k.("h")]}] do
      assert {:ok, prepared} = PreparedCommand.prepare(command, args)
      assert 12 == ReplicaCompat.required_level(prepared.ast), command

      assert ReplicaCompat.queue_error(command, prepared.ast, prepared.command_keys, ctx) =~
               "needs cluster feature level 12"
    end
  end
end

defmodule Ferricstore.Commands.HmsetTouchLevel12Test do
  @moduledoc "From feature level 12, MULTI runs HMSET and TOUCH."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: 12}} = FerricStore.raise_cluster_feature_level(12)
    :ok
  end

  test "a transaction runs HMSET and TOUCH" do
    key = "added12:{#{System.unique_integer([:positive])}}h"

    queue =
      for {command, args} <- [
            {"HMSET", [key, "a", "1", "b", "2"]},
            {"TOUCH", [key]},
            {"HGET", [key, "b"]}
          ] do
        {:ok, prepared} = PreparedCommand.prepare(command, args)
        prepared
      end

    assert [:ok, 1, "2"] == Ferricstore.Transaction.Coordinator.execute(queue, %{}, nil)
  end
end
