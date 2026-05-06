defmodule Ferricstore.Commands.PubSubTest do
  @moduledoc """
  Unit tests for `Ferricstore.Commands.PubSub` — the command handler for
  PUBLISH and PUBSUB subcommands that go through the normal dispatcher.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.PubSub, as: PubSubCmd
  alias Ferricstore.PubSub

  setup do
    if :ets.whereis(:ferricstore_pubsub) == :undefined do
      start_supervised!(PubSub)
    end

    # Clean ETS tables between tests
    :ets.delete_all_objects(:ferricstore_pubsub)
    :ets.delete_all_objects(:ferricstore_pubsub_patterns)
    :ok
  end

  # ---------------------------------------------------------------------------
  # PUBLISH
  # ---------------------------------------------------------------------------

  describe "PUBLISH" do
    test "to channel with no subscribers returns 0" do
      assert PubSubCmd.handle("PUBLISH", ["empty", "hello"]) == 0
    end

    test "to channel with one subscriber returns 1" do
      PubSub.subscribe("ch", self())
      assert PubSubCmd.handle("PUBLISH", ["ch", "data"]) == 1
      assert_receive {:pubsub_message, "ch", "data"}
    end

    test "to channel with multiple subscribers returns count" do
      pid1 = spawn(fn -> Process.sleep(:infinity) end)
      pid2 = spawn(fn -> Process.sleep(:infinity) end)

      PubSub.subscribe("multi", self())
      PubSub.subscribe("multi", pid1)
      PubSub.subscribe("multi", pid2)

      assert PubSubCmd.handle("PUBLISH", ["multi", "msg"]) == 3

      Process.exit(pid1, :kill)
      Process.exit(pid2, :kill)
    end

    test "with wrong number of arguments returns error" do
      assert {:error, "ERR wrong number of arguments for 'publish' command"} =
               PubSubCmd.handle("PUBLISH", [])

      assert {:error, "ERR wrong number of arguments for 'publish' command"} =
               PubSubCmd.handle("PUBLISH", ["only_channel"])

      assert {:error, "ERR wrong number of arguments for 'publish' command"} =
               PubSubCmd.handle("PUBLISH", ["ch", "msg", "extra"])
    end
  end

  # ---------------------------------------------------------------------------
  # PUBSUB CHANNELS
  # ---------------------------------------------------------------------------

  describe "PUBSUB CHANNELS" do
    test "returns empty list when no active channels" do
      assert PubSubCmd.handle("PUBSUB", ["CHANNELS"]) == []
    end

    test "returns active channels" do
      PubSub.subscribe("alpha", self())
      PubSub.subscribe("beta", self())

      result = PubSubCmd.handle("PUBSUB", ["CHANNELS"])
      assert Enum.sort(result) == ["alpha", "beta"]
    end

    test "filters channels with pattern" do
      PubSub.subscribe("news.tech", self())
      PubSub.subscribe("news.sports", self())
      PubSub.subscribe("weather.today", self())

      result = PubSubCmd.handle("PUBSUB", ["CHANNELS", "news.*"])
      assert Enum.sort(result) == ["news.sports", "news.tech"]
    end

    test "filters channels with Redis glob character classes" do
      PubSub.subscribe("news.a", self())
      PubSub.subscribe("news.b", self())
      PubSub.subscribe("news.c", self())

      result = PubSubCmd.handle("PUBSUB", ["CHANNELS", "news.[ab]"])
      assert Enum.sort(result) == ["news.a", "news.b"]
    end

    test "pattern filter with no matches returns empty list" do
      PubSub.subscribe("alpha", self())

      assert PubSubCmd.handle("PUBSUB", ["CHANNELS", "zzz*"]) == []
    end

    test "with too many arguments returns error" do
      assert {:error, _} = PubSubCmd.handle("PUBSUB", ["CHANNELS", "a", "b"])
    end
  end

  # ---------------------------------------------------------------------------
  # PUBSUB NUMSUB
  # ---------------------------------------------------------------------------

  describe "PUBSUB NUMSUB" do
    test "returns empty list for no channels" do
      assert PubSubCmd.handle("PUBSUB", ["NUMSUB"]) == []
    end

    test "returns counts for specified channels" do
      PubSub.subscribe("x", self())

      pid2 = spawn(fn -> Process.sleep(:infinity) end)
      PubSub.subscribe("x", pid2)
      PubSub.subscribe("y", self())

      result = PubSubCmd.handle("PUBSUB", ["NUMSUB", "x", "y", "z"])
      assert result == ["x", 2, "y", 1, "z", 0]

      Process.exit(pid2, :kill)
    end

    test "returns 0 for channels with no subscribers" do
      result = PubSubCmd.handle("PUBSUB", ["NUMSUB", "nonexistent"])
      assert result == ["nonexistent", 0]
    end
  end

  # ---------------------------------------------------------------------------
  # PUBSUB NUMPAT
  # ---------------------------------------------------------------------------

  describe "PUBSUB NUMPAT" do
    test "returns 0 when no pattern subscriptions" do
      assert PubSubCmd.handle("PUBSUB", ["NUMPAT"]) == 0
    end

    test "returns count of pattern subscriptions" do
      PubSub.psubscribe("a.*", self())
      PubSub.psubscribe("b.*", self())

      assert PubSubCmd.handle("PUBSUB", ["NUMPAT"]) == 2
    end

    test "PUBLISH matches pattern subscriptions with Redis glob character classes" do
      PubSub.psubscribe("news.[ab]", self())

      assert PubSubCmd.handle("PUBLISH", ["news.a", "payload"]) == 1
      assert_receive {:pubsub_pmessage, "news.[ab]", "news.a", "payload"}

      assert PubSubCmd.handle("PUBLISH", ["news.c", "payload"]) == 0
      refute_receive {:pubsub_pmessage, "news.[ab]", "news.c", "payload"}
    end

    test "with extra arguments returns error" do
      assert {:error, _} = PubSubCmd.handle("PUBSUB", ["NUMPAT", "extra"])
    end
  end

  # ---------------------------------------------------------------------------
  # PUBSUB — unknown subcommand
  # ---------------------------------------------------------------------------

  describe "PUBSUB unknown subcommand" do
    test "returns error for unknown subcommand" do
      assert {:error, "ERR unknown subcommand 'bogus'. Try PUBSUB HELP."} =
               PubSubCmd.handle("PUBSUB", ["BOGUS"])
    end

    test "returns error for no subcommand" do
      assert {:error, "ERR wrong number of arguments for 'pubsub' command"} =
               PubSubCmd.handle("PUBSUB", [])
    end
  end
end
