defmodule Ferricstore.Raft.NamespaceBatcherTest do
  @moduledoc """
  Tests for the namespace-aware group commit batcher (spec section 2F.3).

  Validates that the batcher correctly:
    * Extracts key prefixes from commands
    * Maintains independent per-namespace buffers with separate timers
    * Uses per-namespace `window_ms` from the `:ferricstore_ns_config` ETS table
    * Falls back to defaults (1 ms, :quorum) for unconfigured prefixes
    * Exposes namespace metrics via Prometheus scrape

  These tests reuse the application-supervised ra system and shard servers.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    NamespaceConfig.reset_all()

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
      NamespaceConfig.reset_all()
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper to generate unique keys with a specific prefix
  defp pkey(prefix, base), do: "#{prefix}:nsbatcher_#{base}_#{:rand.uniform(9_999_999)}"

  # Helper to generate a key without a colon (root namespace)
  defp rootkey(base), do: "nsbatcher_root_#{base}_#{:rand.uniform(9_999_999)}"

  def forward_caller_quorum_submit(_event, measurements, metadata, {test_pid, shard_idx}) do
    if metadata.shard_index == shard_idx do
      send(test_pid, {:caller_quorum_submit, measurements, metadata})
    end
  end

  defp drain_caller_quorum_submits(acc \\ []) do
    receive do
      {:caller_quorum_submit, measurements, metadata} ->
        drain_caller_quorum_submits([{measurements, metadata} | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  # ---------------------------------------------------------------------------
  # Prefix extraction
  # ---------------------------------------------------------------------------

  describe "extract_prefix/1" do
    test "extracts prefix before first colon" do
      assert "session" == Batcher.extract_prefix({:put, "session:abc123", "v", 0})
    end

    test "extracts first segment for multi-colon keys" do
      assert "ts" == Batcher.extract_prefix({:put, "ts:sensor:42", "v", 0})
    end

    test "returns _root for keys without colon" do
      assert "_root" == Batcher.extract_prefix({:delete, "nocolon"})
    end

    test "works with all command types" do
      assert "a" == Batcher.extract_prefix({:put, "a:b", "v", 0})
      assert "b" == Batcher.extract_prefix({:delete, "b:c"})
      assert "c" == Batcher.extract_prefix({:incr_float, "c:d", 1.0})
      assert "d" == Batcher.extract_prefix({:append, "d:e", "suf"})
      assert "e" == Batcher.extract_prefix({:getset, "e:f", "v"})
      assert "f" == Batcher.extract_prefix({:getdel, "f:g"})
      assert "g" == Batcher.extract_prefix({:getex, "g:h", 1000})
      assert "h" == Batcher.extract_prefix({:setrange, "h:i", 0, "v"})
    end

    test "empty key returns _root" do
      assert "_root" == Batcher.extract_prefix({:delete, ""})
    end

    test "key starting with colon has empty prefix" do
      assert "" == Batcher.extract_prefix({:delete, ":leadingcolon"})
    end

    test "compound keys use their parent Redis key namespace" do
      key = "cmpd_async:user:1"

      assert "cmpd_async" ==
               Batcher.extract_prefix({:put, CompoundKey.hash_field(key, "name"), "alice", 0})

      assert "cmpd_async" ==
               Batcher.extract_prefix({:delete, CompoundKey.type_key(key)})

      assert "cmpd_async" ==
               Batcher.extract_prefix({:put, CompoundKey.list_meta_key(key), "meta", 0})
    end
  end

  # ---------------------------------------------------------------------------
  # Independent namespace batching
  # ---------------------------------------------------------------------------

  describe "independent namespace batching" do
    test "commands with different prefixes are written successfully" do
      k1 = pkey("alpha", "ind1")
      k2 = pkey("beta", "ind2")
      k3 = rootkey("ind3")

      shard1 = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard2 = Router.shard_for(FerricStore.Instance.get(:default), k2)
      shard3 = Router.shard_for(FerricStore.Instance.get(:default), k3)

      assert :ok == Batcher.write(shard1, {:put, k1, "val_alpha", 0})
      assert :ok == Batcher.write(shard2, {:put, k2, "val_beta", 0})
      assert :ok == Batcher.write(shard3, {:put, k3, "val_root", 0})

      assert "val_alpha" == Router.get(FerricStore.Instance.get(:default), k1)
      assert "val_beta" == Router.get(FerricStore.Instance.get(:default), k2)
      assert "val_root" == Router.get(FerricStore.Instance.get(:default), k3)
    end

    test "commands with same prefix are batched together" do
      # Generate multiple keys with the same prefix, all hashing to the same shard
      prefix = "batchtest"

      keys =
        for i <- 1..20 do
          pkey(prefix, "same_#{i}")
        end

      # Group by shard and pick one shard with multiple keys
      by_shard =
        Enum.group_by(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)

      {shard_idx, shard_keys} = Enum.max_by(by_shard, fn {_, ks} -> length(ks) end)

      # Send all writes concurrently -- they should be batched within the same
      # namespace slot
      tasks =
        Enum.map(shard_keys, fn k ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, k, "batched_ns", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      for k <- shard_keys do
        assert "batched_ns" == Router.get(FerricStore.Instance.get(:default), k)
      end
    end

    test "caller-backed quorum writes share the namespace window before Ra submit" do
      prefix = "callerwin_#{System.unique_integer([:positive])}"
      :ok = NamespaceConfig.set(prefix, "window_ms", "50")

      {shard_idx, [k1, k2]} =
        1..20_000
        |> Enum.reduce_while(%{}, fn i, by_shard ->
          key = "#{prefix}:#{i}"
          shard = Router.shard_for(FerricStore.Instance.get(:default), key)
          keys = [key | Map.get(by_shard, shard, [])]

          if length(keys) == 2 do
            {:halt, {shard, Enum.reverse(keys)}}
          else
            {:cont, Map.put(by_shard, shard, keys)}
          end
        end)

      test_pid = self()
      handler_id = {:caller_backed_window_batching, test_pid, make_ref()}

      :ok =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :batcher, :quorum_submit],
          &__MODULE__.forward_caller_quorum_submit/4,
          {test_pid, shard_idx}
        )

      try do
        ref1 = make_ref()
        ref2 = make_ref()

        assert :ok == Batcher.write_async(shard_idx, {:put, k1, "v1", 0}, {self(), ref1})
        assert :ok == Batcher.write_async(shard_idx, {:put, k2, "v2", 0}, {self(), ref2})

        assert_receive {^ref1, :ok}, 5_000
        assert_receive {^ref2, :ok}, 5_000

        submits = drain_caller_quorum_submits()

        assert Enum.any?(submits, fn {measurements, metadata} ->
                 metadata.kind == :batch and measurements.batch_size == 2 and
                   measurements.caller_count == 2
               end),
               "expected the two caller-backed writes to submit as one batch, got: #{inspect(submits)}"

        assert "v1" == Router.get(FerricStore.Instance.get(:default), k1)
        assert "v2" == Router.get(FerricStore.Instance.get(:default), k2)
      after
        :telemetry.detach(handler_id)
      end
    end

    test "different prefixes on the same shard are batched independently" do
      # Create keys with different prefixes but force them to the same shard
      # by trying multiple random suffixes until we find matches
      shard_idx = 0

      alpha_key =
        Enum.find(1..1000, fn i ->
          k = "alpha:indep_#{i}"
          Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx
        end)
        |> then(fn i -> "alpha:indep_#{i}" end)

      beta_key =
        Enum.find(1..1000, fn i ->
          k = "beta:indep_#{i}"
          Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx
        end)
        |> then(fn i -> "beta:indep_#{i}" end)

      # Configure different windows (both still short enough for test)
      NamespaceConfig.set("alpha", "window_ms", "1")
      NamespaceConfig.set("beta", "window_ms", "1")

      # Write both -- they use separate slots internally
      assert :ok == Batcher.write(shard_idx, {:put, alpha_key, "alpha_val", 0})
      assert :ok == Batcher.write(shard_idx, {:put, beta_key, "beta_val", 0})

      assert "alpha_val" == Router.get(FerricStore.Instance.get(:default), alpha_key)
      assert "beta_val" == Router.get(FerricStore.Instance.get(:default), beta_key)
    end
  end

  # ---------------------------------------------------------------------------
  # Per-namespace window_ms configuration
  # ---------------------------------------------------------------------------

  describe "per-namespace window_ms" do
    test "uses configured window_ms for a namespace" do
      NamespaceConfig.set("fast", "window_ms", "1")
      NamespaceConfig.set("slow", "window_ms", "50")

      # Verify that the config is actually read correctly
      assert 1 == NamespaceConfig.window_for("fast")
      assert 50 == NamespaceConfig.window_for("slow")

      # Both should work for writes, just with different batching windows
      k_fast = pkey("fast", "wtest")
      k_slow = pkey("slow", "wtest")

      shard_fast = Router.shard_for(FerricStore.Instance.get(:default), k_fast)
      shard_slow = Router.shard_for(FerricStore.Instance.get(:default), k_slow)

      assert :ok == Batcher.write(shard_fast, {:put, k_fast, "fast_val", 0})
      assert :ok == Batcher.write(shard_slow, {:put, k_slow, "slow_val", 0})

      assert "fast_val" == Router.get(FerricStore.Instance.get(:default), k_fast)
      assert "slow_val" == Router.get(FerricStore.Instance.get(:default), k_slow)
    end

    test "uses default window for unconfigured prefix" do
      assert 1 == NamespaceConfig.default_window_ms()
      assert 1 == NamespaceConfig.window_for("unconfigured_prefix")

      k = pkey("unconfigured_prefix", "default_test")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "default_win", 0})
      assert "default_win" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "root namespace uses default window" do
      k = rootkey("rootwin")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "root_val", 0})
      assert "root_val" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "origin ordered submit spec documents Ra target-down errors" do
      batcher_path = Path.expand("../../../lib/ferricstore/raft/batcher.ex", __DIR__)
      source = File.read!(batcher_path)
      [_prefix, spec_and_def] = String.split(source, "@spec origin_submit_ordered", parts: 2)
      [spec, _def_and_rest] = String.split(spec_and_def, "def origin_submit_ordered", parts: 2)

      # This call waits for Ra submission, so callers can see a local target-down
      # error instead of a slot-only :ok. Keep the public contract in sync with
      # the runtime behavior tested below.
      assert spec =~ ":ra_target_down"
    end

    test "origin ordered submit returns pipeline errors instead of slot-only success" do
      shard = 90_000 + :rand.uniform(9_999)
      name = Batcher.batcher_name(shard)

      {:ok, pid} =
        Batcher.start_link(shard_id: :missing_origin_ordered_target, shard_index: shard)

      on_exit(fn ->
        try do
          if Process.alive?(pid), do: GenServer.stop(pid)
        catch
          :exit, _ -> :ok
        end
      end)

      assert {:error, {:ra_target_down, :missing_origin_ordered_target}} =
               Batcher.origin_submit_ordered(
                 shard,
                 {:put, pkey("origin_ordered", "down"), "v", 0}
               )

      assert Process.whereis(name) == pid
    end

    test "origin ordered enqueue rejects known-down local Ra targets before reporting success" do
      shard = 100_000 + :rand.uniform(9_999)
      name = Batcher.batcher_name(shard)

      {:ok, pid} =
        Batcher.start_link(shard_id: :missing_origin_enqueue_target, shard_index: shard)

      on_exit(fn ->
        try do
          if Process.alive?(pid), do: GenServer.stop(pid)
        catch
          :exit, _ -> :ok
        end
      end)

      assert {:error, {:ra_target_down, :missing_origin_enqueue_target}} =
               Batcher.origin_enqueue_ordered(
                 shard,
                 {:put, pkey("origin_ordered", "enqueue_down"), "v", 0}
               )

      assert Process.whereis(name) == pid
    end
  end

  # ---------------------------------------------------------------------------
  # Quorum-only write behavior
  # ---------------------------------------------------------------------------

  describe "quorum-only writes" do
    test "removed durability config is rejected" do
      assert {:error, msg} = NamespaceConfig.set("ephemeral", "durability", "async")
      assert msg =~ "unknown namespace config field 'durability'"

      assert {:error, msg} = NamespaceConfig.set("durable", "durability", "quorum")
      assert msg =~ "unknown namespace config field 'durability'"
    end

    test "namespace writes complete through quorum path" do
      k = pkey("durable", "quorum_test")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "quorum_val", 0})
      assert "quorum_val" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Flush
  # ---------------------------------------------------------------------------

  describe "flush with namespaces" do
    test "flush drains all namespace slots" do
      k1 = pkey("ns_flush_a", "f1")
      k2 = pkey("ns_flush_b", "f2")

      shard1 = Router.shard_for(FerricStore.Instance.get(:default), k1)
      shard2 = Router.shard_for(FerricStore.Instance.get(:default), k2)

      :ok = Batcher.write(shard1, {:put, k1, "fv1", 0})
      :ok = Batcher.write(shard2, {:put, k2, "fv2", 0})

      # Flush all shards
      for i <- 0..3 do
        :ok = Batcher.flush(i)
      end

      assert "fv1" == Router.get(FerricStore.Instance.get(:default), k1)
      assert "fv2" == Router.get(FerricStore.Instance.get(:default), k2)
    end

    test "flush returns :ok when no slots have pending writes" do
      assert :ok == Batcher.flush(0)
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace metrics
  # ---------------------------------------------------------------------------

  describe "namespace metrics" do
    test "scrape includes namespace window_ms gauge when config exists" do
      NamespaceConfig.set("metrics_ns", "window_ms", "5")

      text = Ferricstore.Metrics.scrape()

      assert String.contains?(text, "# HELP ferricstore_namespace_window_ms")
      assert String.contains?(text, "# TYPE ferricstore_namespace_window_ms gauge")

      assert String.contains?(
               text,
               "ferricstore_namespace_window_ms{prefix=\"metrics_ns\"} 5"
             )
    end

    test "scrape includes multiple namespace entries" do
      NamespaceConfig.set("ns_a", "window_ms", "2")
      NamespaceConfig.set("ns_b", "window_ms", "10")

      text = Ferricstore.Metrics.scrape()

      assert String.contains?(text, "ferricstore_namespace_window_ms{prefix=\"ns_a\"} 2")
      assert String.contains?(text, "ferricstore_namespace_window_ms{prefix=\"ns_b\"} 10")
      refute String.contains?(text, "ferricstore_namespace_durability")
    end

    test "scrape omits namespace metrics when no namespaces are configured" do
      NamespaceConfig.reset_all()

      text = Ferricstore.Metrics.scrape()

      refute String.contains?(text, "ferricstore_namespace_window_ms")
      refute String.contains?(text, "ferricstore_namespace_durability")
    end

    test "namespace metrics do not break the base metrics" do
      NamespaceConfig.set("check_base", "window_ms", "3")

      text = Ferricstore.Metrics.scrape()

      # All base metrics should still be present
      base_metrics = [
        "ferricstore_connected_clients",
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_used_memory_bytes",
        "ferricstore_uptime_seconds"
      ]

      for metric <- base_metrics do
        assert String.contains?(text, metric),
               "Expected base metric #{metric} to still be present"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "write with delete command routes to correct namespace" do
      k = pkey("delns", "edge_del")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard, {:put, k, "to_delete", 0})
      assert "to_delete" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard, {:delete, k})
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "write with append command routes to correct namespace" do
      k = pkey("appns", "edge_app")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard, {:put, k, "hello", 0})
      {:ok, 10} = Batcher.write(shard, {:append, k, "world"})
      assert "helloworld" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "concurrent writes across multiple namespaces on same shard" do
      shard_idx = 0

      # Find keys for different prefixes that all hash to shard 0
      keys_by_prefix =
        for prefix <- ["conc_a", "conc_b", "conc_c"] do
          key =
            Enum.find(1..5000, fn i ->
              k = "#{prefix}:conc_#{i}"
              Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx
            end)
            |> then(fn i -> "#{prefix}:conc_#{i}" end)

          {prefix, key}
        end

      # Write all concurrently
      tasks =
        Enum.map(keys_by_prefix, fn {_prefix, key} ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, key, "concurrent_val", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      for {_prefix, key} <- keys_by_prefix do
        assert "concurrent_val" == Router.get(FerricStore.Instance.get(:default), key)
      end
    end

    test "namespace config change is picked up on next write" do
      # Start with default config
      k1 = pkey("dynamic", "cfg1")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k1)
      :ok = Batcher.write(shard, {:put, k1, "before", 0})

      # Change the config
      NamespaceConfig.set("dynamic", "window_ms", "2")

      # Next write should use the new window
      k2 = pkey("dynamic", "cfg2")
      shard2 = Router.shard_for(FerricStore.Instance.get(:default), k2)
      :ok = Batcher.write(shard2, {:put, k2, "after", 0})

      assert "before" == Router.get(FerricStore.Instance.get(:default), k1)
      assert "after" == Router.get(FerricStore.Instance.get(:default), k2)
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace config cache (ns_cache)
  # ---------------------------------------------------------------------------

  describe "ns_cache: first write triggers ETS lookup, subsequent writes use cache" do
    test "write with uncached prefix populates cache, second write reuses it" do
      # Configure a custom window so we can verify the cached value
      NamespaceConfig.set("cached_ns", "window_ms", "7")

      # Allow the :ns_config_changed broadcast to be processed by the batcher
      # so the cache is cleared from any prior test state
      ShardHelpers.eventually(
        fn ->
          7 == NamespaceConfig.window_for("cached_ns")
        end,
        "ns config not applied",
        20,
        10
      )

      k1 = pkey("cached_ns", "cache_hit_1")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k1)

      # First write: triggers ETS lookup for "cached_ns", populates ns_cache
      assert :ok == Batcher.write(shard, {:put, k1, "v1", 0})
      assert "v1" == Router.get(FerricStore.Instance.get(:default), k1)

      # Inspect batcher state to verify cache is populated
      batcher_pid = Process.whereis(Batcher.batcher_name(shard))
      state = :sys.get_state(batcher_pid)
      assert Map.has_key?(state.ns_cache, "cached_ns")
      assert 7 == Map.get(state.ns_cache, "cached_ns")

      # Second write with same prefix: should use cached config (no ETS needed)
      k2 = pkey("cached_ns", "cache_hit_2")
      shard2 = Router.shard_for(FerricStore.Instance.get(:default), k2)

      # Only test on the same shard to verify in-process cache
      if shard2 == shard do
        assert :ok == Batcher.write(shard2, {:put, k2, "v2", 0})
        assert "v2" == Router.get(FerricStore.Instance.get(:default), k2)

        # Cache should still have the entry
        state2 = :sys.get_state(batcher_pid)
        assert 7 == Map.get(state2.ns_cache, "cached_ns")
      end
    end
  end

  describe "ns_cache: config change invalidates cache" do
    test "NamespaceConfig.set broadcasts :ns_config_changed and clears batcher cache" do
      # Pre-populate cache by writing with a known prefix
      NamespaceConfig.set("inval_ns", "window_ms", "5")

      ShardHelpers.eventually(
        fn ->
          5 == NamespaceConfig.window_for("inval_ns")
        end,
        "ns config not applied",
        20,
        10
      )

      k1 = pkey("inval_ns", "inval_1")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k1)
      assert :ok == Batcher.write(shard, {:put, k1, "before_inval", 0})

      batcher_pid = Process.whereis(Batcher.batcher_name(shard))
      state_before = :sys.get_state(batcher_pid)
      assert Map.has_key?(state_before.ns_cache, "inval_ns")
      assert 5 == Map.get(state_before.ns_cache, "inval_ns")

      # Change the config -- this should broadcast :ns_config_changed
      NamespaceConfig.set("inval_ns", "window_ms", "20")

      # Wait for the batcher to process the :ns_config_changed message
      ShardHelpers.eventually(
        fn ->
          state = :sys.get_state(batcher_pid)
          state.ns_cache == %{}
        end,
        "batcher cache not invalidated",
        20,
        10
      )

      # Cache should now be empty
      state_after = :sys.get_state(batcher_pid)
      assert state_after.ns_cache == %{}

      # Next write should re-read from ETS and cache the new value
      k2 = pkey("inval_ns", "inval_2")
      shard2 = Router.shard_for(FerricStore.Instance.get(:default), k2)

      if shard2 == shard do
        assert :ok == Batcher.write(shard2, {:put, k2, "after_inval", 0})
        assert "after_inval" == Router.get(FerricStore.Instance.get(:default), k2)

        state_new = :sys.get_state(batcher_pid)
        assert 20 == Map.get(state_new.ns_cache, "inval_ns")
      end
    end

    test "NamespaceConfig.reset clears batcher cache" do
      NamespaceConfig.set("reset_ns", "window_ms", "15")

      ShardHelpers.eventually(
        fn ->
          15 == NamespaceConfig.window_for("reset_ns")
        end,
        "ns config not applied",
        20,
        10
      )

      k = pkey("reset_ns", "reset_1")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)
      assert :ok == Batcher.write(shard, {:put, k, "reset_val", 0})

      batcher_pid = Process.whereis(Batcher.batcher_name(shard))
      state_before = :sys.get_state(batcher_pid)
      assert Map.has_key?(state_before.ns_cache, "reset_ns")

      # Reset the namespace -- should broadcast invalidation
      NamespaceConfig.reset("reset_ns")

      ShardHelpers.eventually(
        fn ->
          state = :sys.get_state(batcher_pid)
          state.ns_cache == %{}
        end,
        "batcher cache not invalidated after reset",
        20,
        10
      )

      state_after = :sys.get_state(batcher_pid)
      assert state_after.ns_cache == %{}
    end

    test "NamespaceConfig.reset_all clears batcher cache" do
      NamespaceConfig.set("resetall_a", "window_ms", "3")
      NamespaceConfig.set("resetall_b", "window_ms", "4")

      ShardHelpers.eventually(
        fn ->
          3 == NamespaceConfig.window_for("resetall_a") and
            4 == NamespaceConfig.window_for("resetall_b")
        end,
        "ns config not applied",
        20,
        10
      )

      # Write with both prefixes to shard 0 to populate its cache
      shard = 0

      k_a =
        Enum.find(1..5000, fn i ->
          Router.shard_for(FerricStore.Instance.get(:default), "resetall_a:ra_#{i}") == shard
        end)
        |> then(fn i -> "resetall_a:ra_#{i}" end)

      k_b =
        Enum.find(1..5000, fn i ->
          Router.shard_for(FerricStore.Instance.get(:default), "resetall_b:ra_#{i}") == shard
        end)
        |> then(fn i -> "resetall_b:ra_#{i}" end)

      assert :ok == Batcher.write(shard, {:put, k_a, "va", 0})
      assert :ok == Batcher.write(shard, {:put, k_b, "vb", 0})

      batcher_pid = Process.whereis(Batcher.batcher_name(shard))
      state_before = :sys.get_state(batcher_pid)
      assert Map.has_key?(state_before.ns_cache, "resetall_a")
      assert Map.has_key?(state_before.ns_cache, "resetall_b")

      # Reset all -- should clear all batcher caches
      NamespaceConfig.reset_all()

      ShardHelpers.eventually(
        fn ->
          state = :sys.get_state(batcher_pid)
          state.ns_cache == %{}
        end,
        "batcher cache not invalidated after reset_all",
        20,
        10
      )

      state_after = :sys.get_state(batcher_pid)
      assert state_after.ns_cache == %{}
    end
  end

  describe "ns_cache: different prefixes cached independently" do
    test "writing with different prefixes creates separate cache entries" do
      NamespaceConfig.set("indep_x", "window_ms", "3")
      NamespaceConfig.set("indep_y", "window_ms", "8")

      ShardHelpers.eventually(
        fn ->
          3 == NamespaceConfig.window_for("indep_x") and
            8 == NamespaceConfig.window_for("indep_y")
        end,
        "ns config not applied",
        20,
        10
      )

      shard = 0

      k_x =
        Enum.find(1..5000, fn i ->
          Router.shard_for(FerricStore.Instance.get(:default), "indep_x:ic_#{i}") == shard
        end)
        |> then(fn i -> "indep_x:ic_#{i}" end)

      k_y =
        Enum.find(1..5000, fn i ->
          Router.shard_for(FerricStore.Instance.get(:default), "indep_y:ic_#{i}") == shard
        end)
        |> then(fn i -> "indep_y:ic_#{i}" end)

      assert :ok == Batcher.write(shard, {:put, k_x, "vx", 0})
      assert :ok == Batcher.write(shard, {:put, k_y, "vy", 0})

      ShardHelpers.eventually(
        fn ->
          Router.get(FerricStore.Instance.get(:default), k_y) == "vy"
        end,
        "quorum write not visible",
        30,
        20
      )

      batcher_pid = Process.whereis(Batcher.batcher_name(shard))
      state = :sys.get_state(batcher_pid)

      assert 3 == Map.get(state.ns_cache, "indep_x")
      assert 8 == Map.get(state.ns_cache, "indep_y")

      # Verify they don't interfere with each other
      assert "vx" == Router.get(FerricStore.Instance.get(:default), k_x)
      assert "vy" == Router.get(FerricStore.Instance.get(:default), k_y)
    end
  end

  describe "ns_cache: default prefix (_root) cached" do
    test "root namespace (no colon in key) is cached as _root" do
      # Ensure no override for _root -- defaults apply (config already reset in setup)

      k = rootkey("rootcache")
      shard = Router.shard_for(FerricStore.Instance.get(:default), k)

      assert :ok == Batcher.write(shard, {:put, k, "rootval", 0})
      assert "rootval" == Router.get(FerricStore.Instance.get(:default), k)

      batcher_pid = Process.whereis(Batcher.batcher_name(shard))
      state = :sys.get_state(batcher_pid)

      assert Map.has_key?(state.ns_cache, "_root")
      assert 1 == Map.get(state.ns_cache, "_root")
    end
  end

  describe "ns_cache: stress test with cached config" do
    @tag timeout: 120_000
    test "1K writes with cached config complete and cache stays populated" do
      # Configure the namespace once
      NamespaceConfig.set("stress", "window_ms", "1")

      ShardHelpers.eventually(
        fn ->
          1 == NamespaceConfig.window_for("stress")
        end,
        "ns config not applied",
        20,
        10
      )

      shard = 0
      count = 1_000

      # Generate keys that all hash to shard 0
      keys =
        Enum.reduce_while(1..50_000, [], fn i, acc ->
          k = "stress:perf_#{i}"

          if Router.shard_for(FerricStore.Instance.get(:default), k) == shard do
            acc = [k | acc]
            if length(acc) >= count, do: {:halt, acc}, else: {:cont, acc}
          else
            {:cont, acc}
          end
        end)

      assert length(keys) == count

      # Time the writes -- with caching, only the first write should
      # hit ETS; the remaining 999 should read from the in-process map.
      {elapsed_us, results} =
        :timer.tc(fn ->
          Enum.map(keys, fn k ->
            Batcher.write(shard, {:put, k, "stress_val", 0})
          end)
        end)

      assert Enum.all?(results, &(&1 == :ok))

      # Verify the batcher cache is populated (just one entry for "stress")
      batcher_pid = Process.whereis(Batcher.batcher_name(shard))
      state = :sys.get_state(batcher_pid)
      assert Map.has_key?(state.ns_cache, "stress")
      assert 1 == Map.get(state.ns_cache, "stress")

      # Log timing for visibility (not a hard assertion since CI is variable)
      elapsed_ms = div(elapsed_us, 1_000)

      IO.puts(
        "\n  [ns_cache stress] #{count} writes in #{elapsed_ms}ms " <>
          "(#{Float.round(elapsed_us / count, 1)} us/write)"
      )

      # Spot-check a few values are readable
      sample = Enum.take_random(keys, 10)

      for k <- sample do
        assert "stress_val" == Router.get(FerricStore.Instance.get(:default), k)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Batcher name
  # ---------------------------------------------------------------------------

  describe "batcher_name/1" do
    test "returns expected atom" do
      assert Batcher.batcher_name(0) == :"Ferricstore.Raft.Batcher.0"
      assert Batcher.batcher_name(3) == :"Ferricstore.Raft.Batcher.3"
    end
  end
end
