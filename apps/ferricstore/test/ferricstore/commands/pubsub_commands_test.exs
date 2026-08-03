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

    reset_registry()
    :ok
  end

  defp reset_registry do
    :sys.replace_state(PubSub, fn state ->
      for {_pid, ref} <- :ets.tab2list(:ferricstore_pubsub_monitors) do
        Process.demonitor(ref, [:flush])
      end

      for table <- [
            :ferricstore_pubsub,
            :ferricstore_pubsub_patterns,
            :ferricstore_pubsub_channel_cache,
            :ferricstore_pubsub_pattern_cache,
            :ferricstore_pubsub_pid_channels,
            :ferricstore_pubsub_pid_patterns,
            :ferricstore_pubsub_channel_memberships,
            :ferricstore_pubsub_pattern_memberships,
            :ferricstore_pubsub_pattern_prefix_index,
            :ferricstore_pubsub_pattern_suffix_index,
            :ferricstore_pubsub_pattern_glob_index,
            :ferricstore_pubsub_monitors,
            :ferricstore_pubsub_delivery_guards
          ] do
        if :ets.whereis(table) != :undefined, do: :ets.delete_all_objects(table)
      end

      state
    end)
  end

  describe "subscription snapshot" do
    test "subscription state can only be mutated by the registry owner" do
      for table <- [
            :ferricstore_pubsub,
            :ferricstore_pubsub_patterns,
            :ferricstore_pubsub_channel_cache,
            :ferricstore_pubsub_pattern_cache,
            :ferricstore_pubsub_pid_channels,
            :ferricstore_pubsub_pid_patterns,
            :ferricstore_pubsub_channel_memberships,
            :ferricstore_pubsub_pattern_memberships,
            :ferricstore_pubsub_pattern_prefix_index,
            :ferricstore_pubsub_pattern_suffix_index,
            :ferricstore_pubsub_pattern_glob_index
          ] do
        assert :ets.info(table, :protection) == :protected
      end
    end

    test "does not materialize unbounded ETS tables" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))

      refute source =~ ":ets.tab2list"
      assert source =~ "@pattern_cache_table"
      assert source =~ "safe_ets_size(@monitors_table)"
    end

    test "keeps exact pattern counts and one active subscriber per pid" do
      PubSub.subscribe("exact", self())
      PubSub.psubscribe("news.*", self())
      PubSub.psubscribe("news.*", self())

      assert %{
               patterns: [%{pattern: "news.*", subscribers: 1}],
               active_subscribers: 1,
               pattern_subscriptions: 1
             } = PubSub.subscription_snapshot(10)
    end

    test "reverse indexes remain idempotent through unsubscribe and cleanup" do
      PubSub.subscribe("reverse-channel", self())
      PubSub.subscribe("reverse-channel", self())
      PubSub.psubscribe("reverse:*", self())
      PubSub.psubscribe("reverse:*", self())

      assert :ets.lookup(:ferricstore_pubsub_pid_channels, self()) == [
               {self(), "reverse-channel"}
             ]

      assert :ets.lookup(:ferricstore_pubsub_pid_patterns, self()) == [
               {self(), "reverse:*"}
             ]

      PubSub.unsubscribe("reverse-channel", self())
      assert :ets.lookup(:ferricstore_pubsub_pid_channels, self()) == []

      PubSub.cleanup(self())
      assert :ets.lookup(:ferricstore_pubsub_pid_patterns, self()) == []
      assert :ets.lookup(:ferricstore_pubsub_patterns, "reverse:*") == []
    end

    test "bounds snapshots and orders by subscriber count then name" do
      PubSub.subscribe_many(["delta", "alpha", "charlie", "bravo"], self())
      PubSub.psubscribe_many(["zeta.*", "alpha.*", "charlie.*", "bravo.*"], self())

      parent = self()

      extra_subscriber =
        spawn(fn ->
          PubSub.subscribe_many(["charlie", "bravo"], self())
          PubSub.psubscribe("charlie.*", self())
          send(parent, {:snapshot_subscriber_ready, self()})
          Process.sleep(:infinity)
        end)

      on_exit(fn ->
        if Process.alive?(extra_subscriber), do: Process.exit(extra_subscriber, :kill)
      end)

      assert_receive {:snapshot_subscriber_ready, ^extra_subscriber}

      snapshot = PubSub.subscription_snapshot(2)

      assert snapshot.channels == [
               %{channel: "bravo", subscribers: 2},
               %{channel: "charlie", subscribers: 2}
             ]

      assert snapshot.patterns == [
               %{pattern: "charlie.*", subscribers: 2},
               %{pattern: "alpha.*", subscribers: 1}
             ]
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

    test "guarded subscribers reserve delivery bytes before mailbox insertion" do
      test_pid = self()

      assert :ok =
               PubSub.set_delivery_guard(self(), fn bytes ->
                 send(test_pid, {:delivery_reserved, bytes})
                 {:ok, :delivery_lease}
               end)

      PubSub.subscribe("guarded", self())

      assert PubSubCmd.handle("PUBLISH", ["guarded", "payload"]) == 1
      assert_receive {:delivery_reserved, bytes}
      assert bytes >= byte_size("guarded") + byte_size("payload")
      assert_receive {:pubsub_message, "guarded", "payload", :delivery_lease}
    end

    test "same-channel publish batches reserve and enqueue once per guarded subscriber" do
      test_pid = self()

      assert :ok =
               PubSub.set_delivery_guard(self(), fn bytes ->
                 send(test_pid, {:batch_delivery_reserved, bytes})
                 {:ok, :batch_delivery_lease}
               end)

      PubSub.subscribe("guarded-batch", self())

      assert PubSub.publish_many([
               {"guarded-batch", "one"},
               {"guarded-batch", "two"}
             ]) == [1, 1]

      assert_receive {:batch_delivery_reserved, bytes}

      assert bytes >=
               byte_size("guarded-batch") * 2 + byte_size("one") + byte_size("two")

      refute_receive {:batch_delivery_reserved, _bytes}

      assert_receive {:pubsub_messages, "guarded-batch", ["one", "two"], :batch_delivery_lease}
    end

    test "unrelated pattern subscriptions do not disable exact batch delivery" do
      test_pid = self()

      assert :ok =
               PubSub.set_delivery_guard(self(), fn bytes ->
                 send(test_pid, {:unrelated_pattern_reserved, bytes})
                 {:ok, :unrelated_pattern_lease}
               end)

      PubSub.subscribe("exact-batch", self())
      PubSub.psubscribe("unrelated:*", self())

      assert PubSub.publish_many([
               {"exact-batch", "one"},
               {"exact-batch", "two"}
             ]) == [1, 1]

      assert_receive {:unrelated_pattern_reserved, _bytes}
      refute_receive {:unrelated_pattern_reserved, _bytes}

      assert_receive {:pubsub_messages, "exact-batch", ["one", "two"], :unrelated_pattern_lease}
    end

    test "same-channel batches prepare one shared native value for guarded subscribers" do
      parent = self()

      subscribers =
        Enum.map(1..2, fn _index ->
          spawn(fn ->
            :ok =
              PubSub.set_delivery_guard(
                self(),
                fn _bytes -> {:ok, :shared_batch_lease} end,
                prepared_batches: true
              )

            :ok = PubSub.subscribe("shared-batch", self())
            send(parent, {:shared_batch_ready, self()})

            receive do
              event -> send(parent, {:shared_batch_event, self(), event})
            end
          end)
        end)

      on_exit(fn ->
        Enum.each(subscribers, fn pid ->
          if Process.alive?(pid), do: Process.exit(pid, :kill)
        end)
      end)

      Enum.each(subscribers, fn pid ->
        assert_receive {:shared_batch_ready, ^pid}
      end)

      prepared = make_ref()

      prepare = fn channel, messages ->
        send(parent, {:shared_batch_prepared, channel, messages})
        prepared
      end

      assert PubSub.publish_many(
               [{"shared-batch", "one"}, {"shared-batch", "two"}],
               prepare
             ) == [2, 2]

      assert_receive {:shared_batch_prepared, "shared-batch", ["one", "two"]}
      refute_receive {:shared_batch_prepared, _, _}

      Enum.each(subscribers, fn pid ->
        assert_receive {:shared_batch_event, ^pid,
                        {:pubsub_messages, "shared-batch", ["one", "two"], ^prepared,
                         :shared_batch_lease}}
      end)
    end

    test "same-channel batches prepare only after two compatible subscribers are admitted" do
      parent = self()

      accepted =
        spawn(fn ->
          :ok =
            PubSub.set_delivery_guard(
              self(),
              fn _bytes -> {:ok, :accepted_batch_lease} end,
              prepared_batches: true
            )

          :ok = PubSub.subscribe("admitted-shared-batch", self())
          send(parent, {:admitted_shared_batch_ready, self()})

          receive do
            event -> send(parent, {:admitted_shared_batch_event, self(), event})
          end
        end)

      rejected =
        spawn(fn ->
          :ok =
            PubSub.set_delivery_guard(
              self(),
              fn _bytes -> {:error, :limit} end,
              prepared_batches: true
            )

          :ok = PubSub.subscribe("admitted-shared-batch", self())
          send(parent, {:admitted_shared_batch_ready, self()})
          Process.sleep(:infinity)
        end)

      subscribers = [accepted, rejected]

      on_exit(fn ->
        Enum.each(subscribers, fn pid ->
          if Process.alive?(pid), do: Process.exit(pid, :kill)
        end)
      end)

      Enum.each(subscribers, fn pid ->
        assert_receive {:admitted_shared_batch_ready, ^pid}
      end)

      prepare = fn channel, messages ->
        send(parent, {:admitted_shared_batch_prepared, channel, messages})
        make_ref()
      end

      assert PubSub.publish_many(
               [
                 {"admitted-shared-batch", "one"},
                 {"admitted-shared-batch", "two"}
               ],
               prepare
             ) == [1, 1]

      refute_receive {:admitted_shared_batch_prepared, _, _}

      assert_receive {:admitted_shared_batch_event, ^accepted,
                      {:pubsub_messages, "admitted-shared-batch", ["one", "two"],
                       :accepted_batch_lease}}
    end

    test "same-channel batches do not prepare values unused by legacy guarded subscribers" do
      parent = self()

      subscribers =
        Enum.map(1..2, fn _index ->
          spawn(fn ->
            :ok = PubSub.set_delivery_guard(self(), fn _bytes -> {:ok, :legacy_batch_lease} end)
            :ok = PubSub.subscribe("legacy-batch", self())
            send(parent, {:legacy_batch_ready, self()})

            receive do
              event -> send(parent, {:legacy_batch_event, self(), event})
            end
          end)
        end)

      on_exit(fn ->
        Enum.each(subscribers, fn pid ->
          if Process.alive?(pid), do: Process.exit(pid, :kill)
        end)
      end)

      Enum.each(subscribers, fn pid ->
        assert_receive {:legacy_batch_ready, ^pid}
      end)

      prepare = fn channel, messages ->
        send(parent, {:legacy_batch_prepared, channel, messages})
        make_ref()
      end

      assert PubSub.publish_many(
               [{"legacy-batch", "one"}, {"legacy-batch", "two"}],
               prepare
             ) == [2, 2]

      refute_receive {:legacy_batch_prepared, _, _}

      Enum.each(subscribers, fn pid ->
        assert_receive {:legacy_batch_event, ^pid,
                        {:pubsub_messages, "legacy-batch", ["one", "two"], :legacy_batch_lease}}
      end)
    end

    test "same-channel batches do not prepare a value for one guarded subscriber" do
      parent = self()

      assert :ok =
               PubSub.set_delivery_guard(self(), fn _bytes -> {:ok, :single_batch_lease} end)

      assert :ok = PubSub.subscribe("single-shared-batch", self())

      prepare = fn channel, messages ->
        send(parent, {:single_batch_prepared, channel, messages})
        make_ref()
      end

      assert PubSub.publish_many(
               [{"single-shared-batch", "one"}, {"single-shared-batch", "two"}],
               prepare
             ) == [1, 1]

      refute_receive {:single_batch_prepared, _, _}

      assert_receive {:pubsub_messages, "single-shared-batch", ["one", "two"],
                      :single_batch_lease}
    end

    test "an aggregate reservation rejection falls back to per-message admission" do
      test_pid = self()
      individual_limit = byte_size("fallback-batch") + byte_size("one") + 128

      assert :ok =
               PubSub.set_delivery_guard(self(), fn bytes ->
                 send(test_pid, {:fallback_delivery_reserved, bytes})

                 if bytes <= individual_limit,
                   do: {:ok, {:individual_delivery_lease, bytes}},
                   else: {:error, :limit}
               end)

      PubSub.subscribe("fallback-batch", self())

      assert PubSub.publish_many([
               {"fallback-batch", "one"},
               {"fallback-batch", "two"}
             ]) == [1, 1]

      assert_receive {:fallback_delivery_reserved, aggregate_bytes}
      assert aggregate_bytes > individual_limit
      assert_receive {:fallback_delivery_reserved, ^individual_limit}
      assert_receive {:fallback_delivery_reserved, ^individual_limit}

      assert_receive {:pubsub_message, "fallback-batch", "one",
                      {:individual_delivery_lease, ^individual_limit}}

      assert_receive {:pubsub_message, "fallback-batch", "two",
                      {:individual_delivery_lease, ^individual_limit}}
    end

    test "batch publish preserves exact and pattern event order" do
      PubSub.subscribe("ordered-batch", self())
      PubSub.psubscribe("ordered-*", self())

      assert PubSub.publish_many([
               {"ordered-batch", "one"},
               {"ordered-batch", "two"}
             ]) == [2, 2]

      events =
        for _index <- 1..4 do
          receive do
            event -> event
          after
            100 -> flunk("expected exact and pattern PubSub events")
          end
        end

      assert events == [
               {:pubsub_message, "ordered-batch", "one"},
               {:pubsub_pmessage, "ordered-*", "ordered-batch", "one"},
               {:pubsub_message, "ordered-batch", "two"},
               {:pubsub_pmessage, "ordered-*", "ordered-batch", "two"}
             ]
    end

    test "homogeneous pattern batches match a complex channel only once" do
      pattern = "trace-?-*"
      channel = "trace-a-orders"
      PubSub.psubscribe(pattern, self())
      matcher = {Ferricstore.PubSub, :pattern_matches?, 2}
      tracer = spawn(fn -> glob_match_trace_loop(0) end)

      :erlang.trace_pattern(matcher, true, [:local])
      :erlang.trace(self(), true, [:call, {:tracer, tracer}])

      try do
        assert PubSub.publish_many([{channel, "one"}, {channel, "two"}]) == [1, 1]

        send(tracer, {:report, self()})
        assert_receive {:glob_match_calls, 1}
      after
        :erlang.trace(self(), false, [:call])
        :erlang.trace_pattern(matcher, false, [:local])
        Process.exit(tracer, :kill)
      end
    end

    test "high-cardinality homogeneous batches evaluate indexed globs once" do
      pattern = "?*indexed-batch-target*?"
      channel = "xindexed-batch-targety"
      decoys = for index <- 1..17, do: "indexed-batch-decoy-#{index}"

      PubSub.subscribe(channel, self())
      PubSub.psubscribe_many(decoys ++ [pattern], self())

      Code.ensure_loaded!(Ferricstore.GlobMatcher)
      matcher = {Ferricstore.GlobMatcher, :do_match, 6}
      tracer = spawn(fn -> indexed_glob_trace_loop(0) end)

      :erlang.trace_pattern(matcher, true, [:local])
      :erlang.trace(self(), true, [:call, {:tracer, tracer}])

      try do
        assert PubSub.publish_many([{channel, "one"}, {channel, "two"}]) == [2, 2]

        events =
          for _index <- 1..4 do
            receive do
              event -> event
            after
              100 -> flunk("expected exact and indexed pattern PubSub events")
            end
          end

        assert events == [
                 {:pubsub_message, channel, "one"},
                 {:pubsub_pmessage, pattern, channel, "one"},
                 {:pubsub_message, channel, "two"},
                 {:pubsub_pmessage, pattern, channel, "two"}
               ]

        send(tracer, {:report, self()})
        assert_receive {:indexed_glob_match_calls, 1}
      after
        :erlang.trace(self(), false, [:call])
        :erlang.trace_pattern(matcher, false, [:local])
        Process.exit(tracer, :kill)
      end
    end

    test "high-cardinality batches preserve indexed pattern category order" do
      channel = "indexed-order:target"
      patterns = [channel, "*", "indexed-order:*", "*:target", "?*ndexed-order*?"]
      decoys = for index <- 1..17, do: "indexed-order-decoy-#{index}"

      PubSub.subscribe(channel, self())
      PubSub.psubscribe_many(decoys ++ patterns, self())

      assert PubSub.publish_many([{channel, "one"}, {channel, "two"}]) == [6, 6]

      events =
        for _index <- 1..12 do
          receive do
            event -> event
          after
            100 -> flunk("expected ordered high-cardinality PubSub events")
          end
        end

      expected_for = fn message ->
        [{:pubsub_message, channel, message}] ++
          Enum.map(patterns, &{:pubsub_pmessage, &1, channel, message})
      end

      assert events == expected_for.("one") ++ expected_for.("two")
    end

    test "mixed-channel batches preserve publish and delivery order" do
      PubSub.subscribe_many(["mixed-a", "mixed-b"], self())

      assert PubSub.publish_many([
               {"mixed-a", "one"},
               {"mixed-b", "two"},
               {"mixed-a", "three"}
             ]) == [1, 1, 1]

      assert_receive {:pubsub_message, "mixed-a", "one"}
      assert_receive {:pubsub_message, "mixed-b", "two"}
      assert_receive {:pubsub_message, "mixed-a", "three"}
    end

    test "exact delivery cache follows guard replacement and clearing" do
      PubSub.subscribe("guard-cache", self())

      assert PubSubCmd.handle("PUBLISH", ["guard-cache", "unguarded"]) == 1
      assert_receive {:pubsub_message, "guard-cache", "unguarded"}

      assert :ok =
               PubSub.set_delivery_guard(self(), fn _bytes ->
                 {:ok, :replacement_lease}
               end)

      assert PubSubCmd.handle("PUBLISH", ["guard-cache", "guarded"]) == 1
      assert_receive {:pubsub_message, "guard-cache", "guarded", :replacement_lease}

      assert :ok = PubSub.clear_delivery_guard(self())
      assert PubSubCmd.handle("PUBLISH", ["guard-cache", "cleared"]) == 1
      assert_receive {:pubsub_message, "guard-cache", "cleared"}
    end

    test "pattern delivery cache follows guard replacement and clearing" do
      PubSub.psubscribe("guard-pattern:*", self())

      assert PubSubCmd.handle("PUBLISH", ["guard-pattern:1", "unguarded"]) == 1
      assert_receive {:pubsub_pmessage, "guard-pattern:*", "guard-pattern:1", "unguarded"}

      assert :ok =
               PubSub.set_delivery_guard(self(), fn _bytes ->
                 {:ok, :replacement_lease}
               end)

      assert PubSubCmd.handle("PUBLISH", ["guard-pattern:2", "guarded"]) == 1

      assert_receive {:pubsub_pmessage, "guard-pattern:*", "guard-pattern:2", "guarded",
                      :replacement_lease}

      assert :ok = PubSub.clear_delivery_guard(self())
      assert PubSubCmd.handle("PUBLISH", ["guard-pattern:3", "cleared"]) == 1
      assert_receive {:pubsub_pmessage, "guard-pattern:*", "guard-pattern:3", "cleared"}
    end

    test "high-fanout cache removal drops prepared-capable guarded deliveries" do
      channel = "prepared-removal:channel"
      pattern = "prepared-removal:*"
      subscribers = Enum.map(1..32, fn _index -> spawn(fn -> Process.sleep(:infinity) end) end)

      on_exit(fn ->
        Enum.each(subscribers, fn pid ->
          if Process.alive?(pid), do: Process.exit(pid, :kill)
        end)
      end)

      Enum.each(subscribers, fn pid ->
        PubSub.subscribe(channel, pid)
        PubSub.psubscribe(pattern, pid)
      end)

      assert :ok =
               PubSub.set_delivery_guard(
                 self(),
                 fn _bytes -> {:ok, :prepared_removal_lease} end,
                 prepared_batches: true
               )

      PubSub.subscribe(channel, self())
      PubSub.psubscribe(pattern, self())
      PubSub.unsubscribe(channel, self())
      PubSub.punsubscribe(pattern, self())

      assert PubSub.publish(channel, "after-removal") == 64
      refute_receive {:pubsub_message, ^channel, "after-removal", _lease}
      refute_receive {:pubsub_pmessage, ^pattern, ^channel, "after-removal", _lease}
    end

    test "a delivery guard failure disconnects the slow subscriber without queueing payload" do
      parent = self()

      subscriber =
        spawn(fn ->
          :ok = PubSub.set_delivery_guard(self(), fn _bytes -> {:error, :limit} end)
          PubSub.subscribe("overloaded", self())
          send(parent, {:subscriber_ready, self()})
          Process.sleep(:infinity)
        end)

      monitor = Process.monitor(subscriber)
      assert_receive {:subscriber_ready, ^subscriber}

      assert PubSubCmd.handle("PUBLISH", ["overloaded", "payload"]) == 0

      assert_receive {:DOWN, ^monitor, :process, ^subscriber,
                      {:shutdown, :pubsub_outbound_overflow}}
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

      assert publish_source =~ "@pattern_cache_table"
      assert source =~ "defp publish_pattern_scan"
      assert source =~ ":ets.foldl"
      refute source =~ ":ets.tab2list(@pattern_cache_table)"
    end

    test "exact publish skips pattern scan when there are no pattern subscribers" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [publish_source] = Regex.run(~r/def publish\(channel, message\).*?^  end/ms, source)

      assert publish_source =~ ":ets.info(@pattern_cache_table, :size)"

      PubSub.subscribe("exact-only", self())

      assert PubSubCmd.handle("PUBLISH", ["exact-only", "msg"]) == 1
      assert_receive {:pubsub_message, "exact-only", "msg"}
      refute_receive {:pubsub_pmessage, _pattern, "exact-only", "msg"}
    end

    test "pattern subscribe relies on ETS bag idempotence without a pre-insert scan" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))

      [psubscribe_source] =
        Regex.run(
          ~r/def handle_call\(\{:pattern_subscription_changed.*?^  end/ms,
          source
        )

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
      assert source =~ "pattern_matches?(channel, matcher)"
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

    test "adaptive matcher indexes preserve high-cardinality pattern delivery" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [publish_source] = Regex.run(~r/def publish\(channel, message\).*?^  end/ms, source)

      assert source =~ "@pattern_linear_scan_limit"
      assert publish_source =~ "publish_pattern_indexes"

      for table <- [
            :ferricstore_pubsub_pattern_prefix_index,
            :ferricstore_pubsub_pattern_suffix_index,
            :ferricstore_pubsub_pattern_glob_index
          ] do
        assert :ets.info(table, :protection) == :protected
      end

      decoys = for index <- 1..17, do: "indexed-decoy-#{index}"

      PubSub.psubscribe_many(
        decoys ++ ["indexed-exact", "indexed-prefix:*", "*:indexed-suffix", "indexed-glob:?"],
        self()
      )

      assert PubSub.publish("indexed-exact", "exact") == 1
      assert_receive {:pubsub_pmessage, "indexed-exact", "indexed-exact", "exact"}

      assert PubSub.publish("indexed-prefix:value", "prefix") == 1
      assert_receive {:pubsub_pmessage, "indexed-prefix:*", "indexed-prefix:value", "prefix"}

      assert PubSub.publish("value:indexed-suffix", "suffix") == 1
      assert_receive {:pubsub_pmessage, "*:indexed-suffix", "value:indexed-suffix", "suffix"}

      assert PubSub.publish("indexed-glob:x", "glob") == 1
      assert_receive {:pubsub_pmessage, "indexed-glob:?", "indexed-glob:x", "glob"}
    end

    test "complex glob indexes use safe literal boundary anchors" do
      decoys = for index <- 1..17, do: "glob-anchor-decoy-#{index}"
      prefix_pattern = "anchored-prefix:?*[ab]"
      suffix_pattern = "[ab]*?:anchored-suffix"
      escaped_prefix_pattern = "escaped:\\?prefix:*[ab]"
      unanchored_pattern = "?*middle*?"

      PubSub.psubscribe_many(
        decoys ++
          [prefix_pattern, suffix_pattern, escaped_prefix_pattern, unanchored_pattern],
        self()
      )

      assert :ets.lookup(:ferricstore_pubsub_pattern_prefix_index, "anchored-prefix:") ==
               [{"anchored-prefix:", prefix_pattern, :glob}]

      assert :ets.lookup(:ferricstore_pubsub_pattern_suffix_index, ":anchored-suffix") ==
               [{":anchored-suffix", suffix_pattern, :glob}]

      assert :ets.lookup(:ferricstore_pubsub_pattern_prefix_index, "escaped:?prefix:") ==
               [{"escaped:?prefix:", escaped_prefix_pattern, :glob}]

      assert :ets.lookup(:ferricstore_pubsub_pattern_glob_index, unanchored_pattern) ==
               [{unanchored_pattern}]

      assert PubSub.publish("anchored-prefix:xxa", "prefix") == 1
      assert_receive {:pubsub_pmessage, ^prefix_pattern, "anchored-prefix:xxa", "prefix"}

      assert PubSub.publish("bxx:anchored-suffix", "suffix") == 1
      assert_receive {:pubsub_pmessage, ^suffix_pattern, "bxx:anchored-suffix", "suffix"}

      assert PubSub.publish("escaped:?prefix:xa", "escaped") == 1

      assert_receive {:pubsub_pmessage, ^escaped_prefix_pattern, "escaped:?prefix:xa", "escaped"}

      assert PubSub.publish("xmiddley", "fallback") == 1
      assert_receive {:pubsub_pmessage, ^unanchored_pattern, "xmiddley", "fallback"}

      PubSub.punsubscribe_many(
        [prefix_pattern, suffix_pattern, escaped_prefix_pattern, unanchored_pattern],
        self()
      )

      assert :ets.lookup(:ferricstore_pubsub_pattern_prefix_index, "anchored-prefix:") == []
      assert :ets.lookup(:ferricstore_pubsub_pattern_suffix_index, ":anchored-suffix") == []
      assert :ets.lookup(:ferricstore_pubsub_pattern_prefix_index, "escaped:?prefix:") == []
      assert :ets.lookup(:ferricstore_pubsub_pattern_glob_index, unanchored_pattern) == []
    end

    test "pubsub exposes bulk subscribe APIs so connection setup monitors once per command" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))

      assert source =~ "def subscribe_many(channels, pid)"
      assert source =~ "def psubscribe_many(patterns, pid)"
      assert source =~ "def unsubscribe_many(channels, pid)"
      assert source =~ "def punsubscribe_many(patterns, pid)"
    end

    test "high-fanout subscription changes use thresholded cache mutation" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))

      assert source =~ "@incremental_cache_fanout_threshold"
      assert source =~ "@channel_memberships_table"
      assert source =~ "refresh_exact_cache_after_add"
      assert source =~ "refresh_pattern_cache_after_remove"

      for table <- [
            :ferricstore_pubsub_channel_memberships,
            :ferricstore_pubsub_pattern_memberships
          ] do
        assert :ets.info(table, :protection) == :protected
      end

      subscribers = for _index <- 1..33, do: spawn(fn -> Process.sleep(:infinity) end)

      on_exit(fn ->
        Enum.each(subscribers, fn pid ->
          if Process.alive?(pid), do: Process.exit(pid, :kill)
        end)
      end)

      Enum.each(subscribers, fn pid ->
        PubSub.subscribe("incremental", pid)
        PubSub.psubscribe("incremental:*", pid)
      end)

      PubSub.subscribe("incremental", self())
      PubSub.psubscribe("incremental:*", self())
      PubSub.subscribe("incremental", self())
      PubSub.psubscribe("incremental:*", self())

      assert PubSub.publish("incremental", "exact") == 34
      assert_receive {:pubsub_message, "incremental", "exact"}

      assert PubSub.publish("incremental:value", "pattern") == 34
      assert_receive {:pubsub_pmessage, "incremental:*", "incremental:value", "pattern"}

      PubSub.unsubscribe("incremental", self())
      PubSub.punsubscribe("incremental:*", self())

      assert PubSub.publish("incremental", "exact") == 33
      refute_receive {:pubsub_message, "incremental", "exact"}

      assert PubSub.publish("incremental:value", "pattern") == 33
      refute_receive {:pubsub_pmessage, "incremental:*", "incremental:value", "pattern"}
    end
  end

  defp glob_match_trace_loop(count) do
    receive do
      {:trace, _pid, :call, {Ferricstore.PubSub, :pattern_matches?, [_channel, _matcher]}} ->
        glob_match_trace_loop(count + 1)

      {:report, caller} ->
        send(caller, {:glob_match_calls, count})
    end
  end

  defp indexed_glob_trace_loop(count) do
    receive do
      {:trace, _pid, :call,
       {Ferricstore.GlobMatcher, :do_match,
        [_subject, _subject_position, _subject_size, _pattern, _pattern_position, _pattern_size]}} ->
        indexed_glob_trace_loop(count + 1)

      {:report, caller} ->
        send(caller, {:indexed_glob_match_calls, count})
    end
  end

  # ---------------------------------------------------------------------------
  # PUBSUB CHANNELS
  # ---------------------------------------------------------------------------

  describe "PUBSUB CHANNELS" do
    test "returns empty list when no active channels" do
      assert PubSubCmd.handle("PUBSUB", ["CHANNELS"]) == []
    end

    test "prepared AST subcommands remain case-insensitive" do
      assert PubSubCmd.handle_ast({:pubsub, ["channels"]}) == []
      assert PubSubCmd.handle_ast({:pubsub, ["numsub", "missing"]}) == ["missing", 0]
      assert PubSubCmd.handle_ast({:pubsub, ["numpat"]}) == 0
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

    test "channels preclassifies simple filters once before walking the registry" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))
      [channels_source] = Regex.run(~r/def channels\(pattern \\\\ nil\).*?^  end/ms, source)

      assert channels_source =~ "pattern_matcher(pattern)"
      assert source =~ "pattern_matches?(channel, matcher)"

      PubSub.subscribe_many(
        ["literal", "prefix:value", "value:suffix", "glob:a", "glob:b"],
        self()
      )

      assert PubSub.channels("literal") == ["literal"]
      assert PubSub.channels("prefix:*") == ["prefix:value"]
      assert PubSub.channels("*:suffix") == ["value:suffix"]
      assert Enum.sort(PubSub.channels("glob:[ab]")) == ["glob:a", "glob:b"]
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

    test "cached counts follow duplicate subscriptions, unsubscription, and cleanup" do
      PubSub.subscribe("cached-count", self())
      PubSub.subscribe("cached-count", self())
      assert PubSub.numsub(["cached-count"]) == ["cached-count", 1]

      PubSub.unsubscribe("cached-count", self())
      assert PubSub.numsub(["cached-count"]) == ["cached-count", 0]

      subscriber = spawn(fn -> Process.sleep(:infinity) end)
      PubSub.subscribe("cached-count", subscriber)
      assert PubSub.numsub(["cached-count"]) == ["cached-count", 1]

      PubSub.cleanup(subscriber)
      assert PubSub.numsub(["cached-count"]) == ["cached-count", 0]
      Process.exit(subscriber, :kill)
    end

    test "numsub builds the alternating reply without flat_map allocation" do
      source = File.read!(Path.expand("../../../lib/ferricstore/pubsub.ex", __DIR__))

      [numsub_source] =
        Regex.run(~r/def numsub\(channel_list\).*?^  end/ms, source)

      assert numsub_source =~ "numsub_reply(channel_list, [])"
      assert source =~ ":ets.lookup_element(@channel_cache_table, channel, 3, 0)"

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
