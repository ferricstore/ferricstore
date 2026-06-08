defmodule Ferricstore.ReviewR3.ServerIssuesTest do
  @moduledoc """
  Tests proving/disproving Round 3 server, config, and cluster issues.

  R3-H3: DBSIZE counts compound keys (reports 6 instead of 1 for a hash with 5 fields)
  R3-H4: RANDOMKEY returns internal compound keys (H:, T:, S:, Z: prefixed)
  R3-H5: SCAN cursor may skip keys (regression guard)
  R3-H6: FLUSHDB incomplete — Bloom filter data survives flush
  R3-H7: LOCK TTL accuracy — regression guard for the normal path
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # R3-H3: DBSIZE counts compound keys
  # ===========================================================================

  describe "R3-H3: DBSIZE counts compound keys" do
    test "DBSIZE returns 1 for a single hash with 5 fields, not 6+" do
      # Create a hash with 5 fields. Internally this creates:
      #   T:myhash  (type metadata)
      #   H:myhash\0f1, H:myhash\0f2, H:myhash\0f3, H:myhash\0f4, H:myhash\0f5
      # That's 6 ETS entries for 1 logical key.
      :ok = FerricStore.hset("r3h3_hash", %{
        "f1" => "v1",
        "f2" => "v2",
        "f3" => "v3",
        "f4" => "v4",
        "f5" => "v5"
      })

      {:ok, db_size} = FerricStore.dbsize()

      assert db_size == 1,
        "DBSIZE should return 1 for a single hash key, got #{db_size} — " <>
        "compound keys (H:, T:) are being counted"
    end

    test "KEYS * returns 1 for a single hash with 5 fields" do
      :ok = FerricStore.hset("r3h3_keys_hash", %{
        "f1" => "v1",
        "f2" => "v2",
        "f3" => "v3",
        "f4" => "v4",
        "f5" => "v5"
      })

      {:ok, all_keys} = FerricStore.keys()

      assert length(all_keys) == 1,
        "KEYS * should return 1 key, got #{length(all_keys)} — " <>
        "compound keys are leaking through: #{inspect(all_keys)}"

      assert all_keys == ["r3h3_keys_hash"],
        "KEYS * should return the logical key name, got: #{inspect(all_keys)}"
    end

    test "DBSIZE counts mixed types correctly" do
      # 1 string + 1 hash + 1 set = 3 logical keys
      :ok = FerricStore.set("r3h3_str", "value")
      :ok = FerricStore.hset("r3h3_h", %{"f1" => "v1", "f2" => "v2"})
      {:ok, _} = FerricStore.sadd("r3h3_s", ["m1", "m2", "m3"])

      {:ok, db_size} = FerricStore.dbsize()

      assert db_size == 3,
        "DBSIZE should return 3 for 1 string + 1 hash + 1 set, got #{db_size}"
    end
  end

  # ===========================================================================
  # R3-H4: RANDOMKEY returns compound keys
  # ===========================================================================

  describe "R3-H4: RANDOMKEY returns compound keys" do
    test "RANDOMKEY never returns internal compound key prefixes" do
      # Create ONLY hashes — no plain string keys. All ETS entries will be
      # compound keys (H:key\0field, T:key). If RANDOMKEY doesn't filter,
      # it will eventually return one of these.
      for i <- 1..5 do
        :ok = FerricStore.hset("r3h4_hash_#{i}", %{
          "field_a" => "val_a",
          "field_b" => "val_b"
        })
      end

      # Sample RANDOMKEY many times. With 5 hashes × (1 T: + 2 H:) = 15
      # internal entries and 0 plain entries, every call to an unfiltered
      # RANDOMKEY must return an internal key.
      leaked_keys =
        for _ <- 1..100, reduce: [] do
          acc ->
            {:ok, key} = FerricStore.randomkey()

            cond do
              key == nil -> acc
              String.starts_with?(key, "H:") -> [key | acc]
              String.starts_with?(key, "T:") -> [key | acc]
              String.starts_with?(key, "S:") -> [key | acc]
              String.starts_with?(key, "Z:") -> [key | acc]
              String.starts_with?(key, "L:") -> [key | acc]
              String.starts_with?(key, "V:") -> [key | acc]
              String.contains?(key, <<0>>) -> [key | acc]
              true -> acc
            end
        end

      assert leaked_keys == [],
        "RANDOMKEY returned #{length(leaked_keys)} internal compound key(s): " <>
        "#{inspect(Enum.take(leaked_keys, 3))}"
    end

    test "RANDOMKEY returns one of the logical hash key names" do
      keys_created = for i <- 1..5 do
        key = "r3h4_only_#{i}"
        :ok = FerricStore.hset(key, %{"f" => "v"})
        key
      end

      results =
        for _ <- 1..50 do
          {:ok, key} = FerricStore.randomkey()
          key
        end
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()

      # Every returned key must be one of the logical keys we created
      Enum.each(results, fn key ->
        assert key in keys_created,
          "RANDOMKEY returned '#{key}' which is not a user-visible key. " <>
          "Expected one of: #{inspect(keys_created)}"
      end)
    end
  end

  # ===========================================================================
  # R3-H5: SCAN cursor skips keys (regression guard)
  # ===========================================================================

  describe "R3-H5: SCAN returns all keys" do
    test "SCAN with COUNT 1 returns all keys when iterated to completion" do
      # Create a mix of key types
      for i <- 1..10, do: :ok = FerricStore.set("r3h5_str_#{i}", "val")
      :ok = FerricStore.hset("r3h5_hash", %{"f1" => "v1", "f2" => "v2"})
      {:ok, _} = FerricStore.sadd("r3h5_set", ["m1", "m2"])

      # Expected logical keys
      expected =
        (for i <- 1..10, do: "r3h5_str_#{i}") ++ ["r3h5_hash", "r3h5_set"]
        |> Enum.sort()

      # Build a store map that uses Router functions (like the real server does)
      store = build_router_store()

      # Iterate SCAN to completion
      collected = scan_all(store, _count = 2)
      collected_sorted = Enum.sort(collected)

      assert collected_sorted == expected,
        "SCAN missed keys. Expected #{length(expected)}, got #{length(collected)}.\n" <>
        "Missing: #{inspect(expected -- collected_sorted)}\n" <>
        "Extra: #{inspect(collected_sorted -- expected)}"
    end

    test "SCAN with MATCH pattern returns only matching keys" do
      for i <- 1..5, do: :ok = FerricStore.set("r3h5_alpha_#{i}", "val")
      for i <- 1..5, do: :ok = FerricStore.set("r3h5_beta_#{i}", "val")

      store = build_router_store()

      # SCAN with MATCH "r3h5_alpha_*"
      collected = scan_all_with_match(store, "r3h5_alpha_*", 3)

      expected = for i <- 1..5, do: "r3h5_alpha_#{i}"

      assert Enum.sort(collected) == Enum.sort(expected),
        "SCAN MATCH missed keys. Expected #{inspect(expected)}, got #{inspect(collected)}"
    end
  end

  # ===========================================================================
  # R3-H6: FLUSHDB incomplete — Bloom filter data survives flush
  # ===========================================================================

  describe "R3-H6: FLUSHDB clears Bloom filters" do
    test "BF.EXISTS returns 0 after FLUSHDB" do
      # Create and populate a Bloom filter
      :ok = FerricStore.bf_reserve("r3h6_bloom", 0.01, 100)
      {:ok, 1} = FerricStore.bf_add("r3h6_bloom", "element_1")
      {:ok, 1} = FerricStore.bf_add("r3h6_bloom", "element_2")

      # Verify the elements exist before flush
      {:ok, 1} = FerricStore.bf_exists("r3h6_bloom", "element_1")
      {:ok, 1} = FerricStore.bf_exists("r3h6_bloom", "element_2")

      # Flush all keys
      :ok = FerricStore.flushall()

      # After flush, the bloom filter should be gone.
      # BF.EXISTS on a non-existent key returns 0.
      {:ok, result1} = FerricStore.bf_exists("r3h6_bloom", "element_1")
      {:ok, result2} = FerricStore.bf_exists("r3h6_bloom", "element_2")

      assert result1 == 0,
        "BF.EXISTS returned #{result1} after FLUSHDB — Bloom filter was not cleared"

      assert result2 == 0,
        "BF.EXISTS returned #{result2} after FLUSHDB — Bloom filter was not cleared"
    end

    test "BF.ADD after FLUSHDB creates a fresh filter" do
      # Create and populate
      :ok = FerricStore.bf_reserve("r3h6_fresh", 0.01, 100)
      {:ok, 1} = FerricStore.bf_add("r3h6_fresh", "before_flush")

      :ok = FerricStore.flushall()

      # Adding to the same key name should create a new filter
      {:ok, added} = FerricStore.bf_add("r3h6_fresh", "after_flush")

      assert added == 1,
        "BF.ADD after FLUSHDB should return 1 (new element), got #{added}"

      # The old element should not be found in the new filter
      {:ok, old_exists} = FerricStore.bf_exists("r3h6_fresh", "before_flush")

      assert old_exists == 0,
        "BF.EXISTS for pre-flush element returned #{old_exists} — stale Bloom data survived FLUSHDB"
    end
  end

  # ===========================================================================
  # R3-H7: LOCK TTL accuracy (regression guard via normal path)
  # ===========================================================================

  describe "R3-H7: LOCK TTL accuracy" do
    test "LOCK TTL is approximately correct on the normal path" do
      lock_key = "r3h7_lock"
      owner = "test_owner_#{:rand.uniform(100_000)}"
      ttl_ms = 5_000

      # Acquire the lock
      :ok = FerricStore.lock(lock_key, owner, ttl_ms)

      # Check TTL immediately — should be close to 5000ms
      {:ok, remaining_ttl} = FerricStore.ttl(lock_key)

      assert remaining_ttl != nil,
        "Lock key has no TTL — lock was stored without expiry"

      # Allow generous tolerance (within 2 seconds of expected)
      assert remaining_ttl >= ttl_ms - 2_000,
        "Lock TTL too low: #{remaining_ttl}ms, expected ~#{ttl_ms}ms. " <>
        "Possible double-add causing TTL to be halved."

      assert remaining_ttl <= ttl_ms + 1_000,
        "Lock TTL too high: #{remaining_ttl}ms, expected ~#{ttl_ms}ms"

      # Clean up: unlock
      {:ok, 1} = FerricStore.unlock(lock_key, owner)
    end

    test "LOCK expires after TTL and can be re-acquired" do
      lock_key = "r3h7_lock_expire"
      owner1 = "owner1_#{:rand.uniform(100_000)}"
      owner2 = "owner2_#{:rand.uniform(100_000)}"
      ttl_ms = 200

      # Acquire with short TTL
      :ok = FerricStore.lock(lock_key, owner1, ttl_ms)

      # Should not be acquirable by another owner immediately
      result = FerricStore.lock(lock_key, owner2, ttl_ms)
      assert match?({:error, _}, result),
        "Lock should be held, but another owner could acquire it immediately"

      Ferricstore.Test.Utils.eventually(fn ->
        assert :ok = FerricStore.lock(lock_key, owner2, ttl_ms)
      end, 5_000)

      # Clean up
      FerricStore.unlock(lock_key, owner2)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  # Builds a store map using Router functions, matching what the real
  # connection layer passes to Server and Generic command handlers.
  defp build_router_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn ->
        FerricStore.flushall()
      end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end
    }
  end

  # Iterates SCAN to completion, collecting all returned keys.
  defp scan_all(store, count) do
    alias Ferricstore.Commands.Generic

    do_scan("0", store, count, [])
  end

  defp do_scan(cursor, store, count, acc) do
    alias Ferricstore.Commands.Generic

    count_str = Integer.to_string(count)
    [next_cursor, batch] = Generic.handle("SCAN", [cursor, "COUNT", count_str], store)

    new_acc = acc ++ batch

    if next_cursor == "0" do
      Enum.uniq(new_acc)
    else
      do_scan(next_cursor, store, count, new_acc)
    end
  end

  # Iterates SCAN with MATCH pattern to completion.
  defp scan_all_with_match(store, pattern, count) do
    alias Ferricstore.Commands.Generic

    do_scan_match("0", store, pattern, count, [])
  end

  defp do_scan_match(cursor, store, pattern, count, acc) do
    alias Ferricstore.Commands.Generic

    count_str = Integer.to_string(count)

    [next_cursor, batch] =
      Generic.handle("SCAN", [cursor, "MATCH", pattern, "COUNT", count_str], store)

    new_acc = acc ++ batch

    if next_cursor == "0" do
      Enum.uniq(new_acc)
    else
      do_scan_match(next_cursor, store, pattern, count, new_acc)
    end
  end
end
