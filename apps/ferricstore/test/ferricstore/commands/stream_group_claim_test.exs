defmodule Ferricstore.Commands.StreamGroupClaimTest do
  @moduledoc """
  XCLAIM, XAUTOCLAIM, XGROUP DESTROY/CREATECONSUMER/DELCONSUMER/SETID and
  XSETID, as Redis answers them (entries in FerricStore's flat form, as XRANGE
  answers them). They are replicated stream operations older
  releases do not have, so they run from feature level 11.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Test.ShardHelpers

  @nokey "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE " <>
           "you may want to use the MKSTREAM option to create an empty stream automatically."

  # Feature levels never go down, so each level gets one throwaway data dir.
  def isolate(level) do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    isolated = ShardHelpers.setup_isolated_data_dir()
    ExUnit.Callbacks.on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, %{active: ^level}} = FerricStore.raise_cluster_feature_level(level)
    :ok
  end

  def start do
    ctx = FerricStore.Instance.get(:default)
    tag = "xclaim:{#{System.unique_integer([:positive])}}"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    k = &(tag <> &1)

    :ok = d.("XGROUP", ["CREATE", k.("s"), "g", "0", "MKSTREAM"])
    for id <- ~w(1-0 2-0 3-0 4-0 5-0), do: ^id = d.("XADD", [k.("s"), id, "f", id])
    [[_, [_, _, _]]] = d.("XREADGROUP", ~w(GROUP g alice COUNT 3 STREAMS) ++ [k.("s"), ">"])
    %{d: d, k: k}
  end

  defp pending(d, k), do: d.("XPENDING", [k.("s"), "g", "-", "+", "100"])

  setup_all do: isolate(11)
  setup do: start()

  describe "at feature level 11" do
    test "XCLAIM moves idle entries to a consumer", %{d: d, k: k} do
      assert [["1-0", "f", "1-0"], ["2-0", "f", "2-0"]] ==
               d.("XCLAIM", [k.("s"), "g", "bob", "0", "1-0", "2-0"])

      assert [["1-0", "bob", _, 2], ["2-0", "bob", _, 2], ["3-0", "alice", _, 1]] = pending(d, k)

      # Not idle long enough.
      assert [] == d.("XCLAIM", [k.("s"), "g", "carol", "3600000", "3-0"])

      # JUSTID answers IDs and does not count a delivery.
      assert ["1-0"] == d.("XCLAIM", [k.("s"), "g", "alice", "0", "1-0", "JUSTID"])
      assert [["1-0", "alice", _, 2] | _] = pending(d, k)

      assert [_] = d.("XCLAIM", [k.("s"), "g", "bob", "0", "2-0", "RETRYCOUNT", "7"])
      assert [_, ["2-0", "bob", _, 7], _] = pending(d, k)

      assert [_] = d.("XCLAIM", [k.("s"), "g", "bob", "0", "3-0", "IDLE", "5000"])
      assert [_, _, ["3-0", "bob", idle, _]] = pending(d, k)
      assert idle >= 5000

      assert [%{"name" => "bob"} | _] =
               d.("XINFO", ["CONSUMERS", k.("s"), "g"]) |> Enum.filter(&(&1["name"] == "bob"))
    end

    test "XCLAIM FORCE, deleted entries and LASTID", %{d: d, k: k} do
      # 5-0 was never delivered: only FORCE claims it.
      assert [] == d.("XCLAIM", [k.("s"), "g", "bob", "0", "5-0"])
      assert [["5-0", "f", "5-0"]] == d.("XCLAIM", [k.("s"), "g", "bob", "0", "5-0", "FORCE"])
      # Created with one delivery, then claimed: two, as Redis counts it.
      assert ["5-0", "bob", _, 2] = List.last(pending(d, k))

      # An entry deleted from the stream leaves the pending list unclaimed.
      assert 1 == d.("XDEL", [k.("s"), "2-0"])
      assert [] == d.("XCLAIM", [k.("s"), "g", "carol", "0", "2-0"])
      refute Enum.any?(pending(d, k), &match?(["2-0" | _], &1))

      assert ["1-0"] ==
               d.("XCLAIM", [k.("s"), "g", "bob", "0", "1-0", "JUSTID", "LASTID", "9-0"])

      assert [%{"last-delivered-id" => "9-0"}] = d.("XINFO", ["GROUPS", k.("s")])
    end

    test "XCLAIM errors", %{d: d, k: k} do
      assert {:error, "NOGROUP No such key '#{k.("s")}' or consumer group 'nope'"} ==
               d.("XCLAIM", [k.("s"), "nope", "bob", "0", "1-0"])

      assert {:error, "ERR Invalid min-idle-time argument for XCLAIM"} ==
               d.("XCLAIM", [k.("s"), "g", "bob", "soon", "1-0"])

      # IDs run until an argument is not one; the rest are options.
      assert {:error, "ERR Unrecognized XCLAIM option 'x'"} ==
               d.("XCLAIM", [k.("s"), "g", "bob", "0", "x"])

      assert {:error, "ERR Unrecognized XCLAIM option 'SOON'"} ==
               d.("XCLAIM", [k.("s"), "g", "bob", "0", "1-0", "SOON"])
    end

    test "XAUTOCLAIM pages through idle entries", %{d: d, k: k} do
      assert ["3-0", [["1-0", "f", "1-0"], ["2-0", "f", "2-0"]], []] ==
               d.("XAUTOCLAIM", [k.("s"), "g", "bob", "0", "0-0", "COUNT", "2"])

      assert 1 == d.("XDEL", [k.("s"), "3-0"])

      assert ["0-0", [], ["3-0"]] ==
               d.("XAUTOCLAIM", [k.("s"), "g", "bob", "0", "3-0", "COUNT", "2"])

      assert ["0-0", ["1-0", "2-0"], []] ==
               d.("XAUTOCLAIM", [k.("s"), "g", "carol", "0", "-", "JUSTID"])

      assert [["1-0", "carol", _, 2], ["2-0", "carol", _, 2]] = pending(d, k)

      assert {:error, "ERR COUNT must be > 0"} ==
               d.("XAUTOCLAIM", [k.("s"), "g", "bob", "0", "0-0", "COUNT", "0"])
    end

    test "XGROUP CREATECONSUMER, DELCONSUMER, SETID and DESTROY", %{d: d, k: k} do
      assert 1 == d.("XGROUP", ["CREATECONSUMER", k.("s"), "g", "dave"])
      assert 0 == d.("XGROUP", ["CREATECONSUMER", k.("s"), "g", "dave"])
      assert Enum.any?(d.("XINFO", ["CONSUMERS", k.("s"), "g"]), &(&1["name"] == "dave"))

      assert 3 == d.("XGROUP", ["DELCONSUMER", k.("s"), "g", "alice"])
      assert [0, nil, nil, nil] == d.("XPENDING", [k.("s"), "g"])
      assert 0 == d.("XGROUP", ["DELCONSUMER", k.("s"), "g", "nobody"])

      assert :ok == d.("XGROUP", ["SETID", k.("s"), "g", "0"])
      assert [[_, entries]] = d.("XREADGROUP", ~w(GROUP g bob STREAMS) ++ [k.("s"), ">"])
      assert 5 == length(entries)
      assert :ok == d.("XGROUP", ["SETID", k.("s"), "g", "$"])
      assert [%{"last-delivered-id" => "5-0"}] = d.("XINFO", ["GROUPS", k.("s")])

      assert 1 == d.("XGROUP", ["DESTROY", k.("s"), "g"])
      assert 0 == d.("XGROUP", ["DESTROY", k.("s"), "g"])
      assert {:error, "NOGROUP" <> _} = d.("XPENDING", [k.("s"), "g"])

      for sub <- [["CREATECONSUMER", "c"], ["DELCONSUMER", "c"], ["SETID", "0"]] do
        [name | rest] = sub

        assert {:error, "NOGROUP No such consumer group 'g' for key name '#{k.("s")}'"} ==
                 d.("XGROUP", [name, k.("s"), "g" | rest])
      end

      assert {:error, @nokey} == d.("XGROUP", ["DESTROY", k.("missing"), "g"])

      assert {:error, "ERR wrong number of arguments for 'xgroup|destroy' command"} ==
               d.("XGROUP", ["DESTROY", k.("s")])

      assert {:error, "ERR unknown subcommand 'FOO'. Try XGROUP HELP."} ==
               d.("XGROUP", ["FOO", k.("s"), "g"])
    end

    test "XGROUP and XSETID fire Redis's keyspace events", %{d: d, k: k} do
      :ok = Ferricstore.Config.set("notify-keyspace-events", "KEA")

      on_exit(fn ->
        Ferricstore.Config.set("notify-keyspace-events", "")
        Ferricstore.PubSub.cleanup(self())
      end)

      events = ~w(xgroup-create xgroup-createconsumer xgroup-delconsumer xgroup-setid
                  xgroup-destroy xsetid)

      for event <- events,
          do: :ok = Ferricstore.PubSub.subscribe("__keyevent@0__:" <> event, self())

      key = k.("s")
      :ok = d.("XGROUP", ["CREATE", key, "g2", "0"])
      1 = d.("XGROUP", ["CREATECONSUMER", key, "g2", "c"])
      0 = d.("XGROUP", ["CREATECONSUMER", key, "g2", "c"])
      0 = d.("XGROUP", ["DESTROY", key, "nope"])
      :ok = d.("XGROUP", ["SETID", key, "g2", "$"])
      1 = d.("XGROUP", ["DESTROY", key, "g2"])
      :ok = d.("XSETID", [key, "100-0"])

      for event <- ~w(xgroup-create xgroup-createconsumer xgroup-setid xgroup-destroy xsetid) do
        channel = "__keyevent@0__:" <> event
        assert_receive {:pubsub_message, ^channel, ^key}, 1_000
        refute_receive {:pubsub_message, ^channel, ^key}, 50
      end
    end

    test "XSETID moves the last generated ID", %{d: d, k: k} do
      assert :ok == d.("XSETID", [k.("s"), "100-0"])
      assert %{"last-generated-id" => "100-0"} = d.("XINFO", ["STREAM", k.("s")])
      assert "100-1" == d.("XADD", [k.("s"), "100-*", "f", "v"])

      assert {:error, "ERR The ID specified in XSETID is smaller than the target stream top item"} ==
               d.("XSETID", [k.("s"), "1-0"])

      assert {:error, "ERR no such key"} == d.("XSETID", [k.("missing"), "1-0"])
    end
  end
end

defmodule Ferricstore.Commands.StreamGroupClaimBelowLevel11Test do
  @moduledoc "Below feature level 11 the stream group writes are refused."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.StreamGroupClaimTest

  setup_all do: StreamGroupClaimTest.isolate(10)
  setup do: StreamGroupClaimTest.start()

  describe "below feature level 11" do
    test "the new stream writes are refused", %{d: d, k: k} do
      for args <- [
            ["XCLAIM", k.("s"), "g", "bob", "0", "1-0"],
            ["XAUTOCLAIM", k.("s"), "g", "bob", "0", "0-0"],
            ["XSETID", k.("s"), "100-0"],
            ["XGROUP", "DESTROY", k.("s"), "g"]
          ] do
        [command | rest] = args
        assert {:error, message} = d.(command, rest)
        assert message =~ "needs cluster feature level 11", inspect(args)
      end
    end
  end
end
