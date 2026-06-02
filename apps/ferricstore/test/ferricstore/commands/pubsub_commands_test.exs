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
    clear_table(:ferricstore_pubsub)
    clear_table(:ferricstore_pubsub_patterns)
    clear_table(:ferricstore_pubsub_channel_cache)
    :ok
  end

  defp clear_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      _tid -> :ets.delete_all_objects(table)
    end
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

    test "pattern publish hot path streams ETS instead of copying the pattern table" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [publish_source] = Regex.run(~r/def publish\(channel, message\).*?^  end/ms, source)

      assert publish_source =~ ":ets.foldl"
      refute publish_source =~ ":ets.tab2list"
    end

    test "exact publish skips pattern scan when there are no pattern subscribers" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [publish_source] = Regex.run(~r/def publish\(channel, message\).*?^  end/ms, source)

      assert publish_source =~ ":ets.info(@patterns_table, :size)"

      PubSub.subscribe("exact-only", self())

      assert PubSubCmd.handle("PUBLISH", ["exact-only", "msg"]) == 1
      assert_receive {:pubsub_message, "exact-only", "msg"}
      refute_receive {:pubsub_pmessage, _pattern, "exact-only", "msg"}
    end

    test "pattern subscribe relies on ETS bag idempotence without a pre-insert scan" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [psubscribe_source] = Regex.run(~r/def psubscribe\(pattern, pid\).*?^  end/ms, source)

      refute psubscribe_source =~ ":ets.match"
      assert psubscribe_source =~ ":ets.insert"

      PubSub.psubscribe("dup.*", self())
      PubSub.psubscribe("dup.*", self())

      assert PubSub.numpat() == 1
      assert PubSubCmd.handle("PUBLISH", ["dup.1", "data"]) == 1
      assert_receive {:pubsub_pmessage, "dup.*", "dup.1", "data"}
      refute_receive {:pubsub_pmessage, "dup.*", "dup.1", "data"}
    end

    test "pattern publish uses preclassified simple pattern matchers" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [publish_source] = Regex.run(~r/def publish\(channel, message\).*?^  end/ms, source)

      assert source =~ "pattern_matcher(pattern)"
      assert publish_source =~ "pattern_matches?(channel, matcher)"
      refute publish_source =~ "Ferricstore.GlobMatcher.match?(channel, pattern)"

      PubSub.psubscribe("prefix:*", self())
      PubSub.psubscribe("*:suffix", self())
      PubSub.psubscribe("literal", self())
      PubSub.psubscribe("*", self())

      assert PubSubCmd.handle("PUBLISH", ["prefix:1", "data"]) == 2
      assert_receive {:pubsub_pmessage, "prefix:*", "prefix:1", "data"}
      assert_receive {:pubsub_pmessage, "*", "prefix:1", "data"}

      assert PubSubCmd.handle("PUBLISH", ["other:suffix", "data"]) == 2
      assert_receive {:pubsub_pmessage, "*:suffix", "other:suffix", "data"}
      assert_receive {:pubsub_pmessage, "*", "other:suffix", "data"}

      assert PubSubCmd.handle("PUBLISH", ["literal", "data"]) == 2
      assert_receive {:pubsub_pmessage, "literal", "literal", "data"}
      assert_receive {:pubsub_pmessage, "*", "literal", "data"}
    end

    test "pubsub exposes bulk subscribe APIs so connection setup monitors once per command" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))

      assert source =~ "def subscribe_many(channels, pid)"
      assert source =~ "def psubscribe_many(patterns, pid)"
      assert source =~ "def unsubscribe_many(channels, pid)"
      assert source =~ "def punsubscribe_many(patterns, pid)"
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

    test "channels command walks unique ETS keys without copying subscriber rows" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [channels_source] = Regex.run(~r/def channels\(pattern \\\\ nil\).*?^  end/ms, source)

      assert channels_source =~ ":ets.first"
      assert source =~ ":ets.next(@channels_table, channel)"
      refute channels_source =~ ":ets.tab2list"
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

    test "numsub builds the alternating reply without flat_map allocation" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))

      [numsub_source] =
        Regex.run(~r/def numsub\(channel_list\).*?^  end/ms, source)

      assert numsub_source =~ "numsub_reply(channel_list, [])"

      refute numsub_source =~ "Enum.flat_map",
             "NUMSUB replies are already ordered pairs; use a reverse accumulator instead of flat_map"
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
