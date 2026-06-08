defmodule Ferricstore.Store.RouterConcurrencyTest do
  @moduledoc """
  Concurrency and distribution tests for the Router module.

  These tests verify that:
  - `shard_for/2` distributes keys across all available shards
  - The distribution is deterministic and repeatable
  - Keys spread reasonably evenly (not all landing on one shard)

  The pure routing tests (shard_for, shard_name) run without requiring a live
  supervisor because they are stateless hashing functions. Tests that need
  live shards use the application-started supervisor (indices 0..3).
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  setup do
    dir = Path.join(System.tmp_dir!(), "router_conc_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    %{dir: dir}
  end

  # -------------------------------------------------------------------
  # Router key distribution
  # -------------------------------------------------------------------

  describe "router key distribution" do
    test "keys are distributed across multiple shards" do
      shard_assignments =
        Enum.group_by(0..99, fn i -> Router.shard_for(FerricStore.Instance.get(:default), "key_#{i}") end)

      shard_count = map_size(shard_assignments)

      assert shard_count > 1,
             "Expected keys to land on more than 1 shard, but all 100 keys " <>
               "mapped to shard #{inspect(Map.keys(shard_assignments))}"
    end

    test "shard_for is deterministic across calls" do
      results =
        for _ <- 1..1_000 do
          Router.shard_for(FerricStore.Instance.get(:default), "same_key")
        end

      assert Enum.uniq(results) |> length() == 1,
             "shard_for(\"same_key\") returned different values across 1000 calls"
    end

    test "shard_for distributes 1000 keys across all 4 shards" do
      shard_assignments =
        Enum.group_by(0..999, fn i -> Router.shard_for(FerricStore.Instance.get(:default), "key_#{i}") end)

      for shard_idx <- 0..3 do
        assert Map.has_key?(shard_assignments, shard_idx),
               "Shard #{shard_idx} received zero keys out of 1000 -- " <>
                 "distribution is degenerate"
      end

      # Bonus: verify no shard is unreasonably loaded. With 1000 keys and 4
      # shards, perfect distribution is 250 each. We allow a generous margin
      # (50-500 per shard) to avoid flaky tests.
      for {shard_idx, keys} <- shard_assignments do
        count = length(keys)

        assert count >= 50,
               "Shard #{shard_idx} got only #{count} keys (expected >= 50)"

        assert count <= 500,
               "Shard #{shard_idx} got #{count} keys (expected <= 500)"
      end
    end

    test "shard_for handles binary keys with special characters" do
      special_keys = [
        "",
        "key with spaces",
        "key\twith\ttabs",
        "key\nwith\nnewlines",
        String.duplicate("a", 10_000),
        <<0, 1, 2, 3, 255>>,
        "unicode: \u00e9\u00e0\u00fc\u00f1"
      ]

      for key <- special_keys do
        shard = Router.shard_for(FerricStore.Instance.get(:default), key)
        assert shard in 0..3, "shard_for(#{inspect(key)}) returned #{inspect(shard)}"
      end
    end

    test "slot_for distributes keys across many slots" do
      slot_assignments =
        Enum.group_by(0..999, fn i -> Router.slot_for(FerricStore.Instance.get(:default), "key_#{i}") end)

      assert map_size(slot_assignments) > 100,
             "Expected 1000 keys to cover many slots, " <>
               "but only covered #{map_size(slot_assignments)}"
    end
  end

  # -------------------------------------------------------------------
  # Router dispatch through live application shards
  # -------------------------------------------------------------------

  describe "router dispatch through live shards" do
    setup do
      Ferricstore.Test.ShardHelpers.wait_default_quorum_writable(60_000)
      :ok
    end

    test "put and get through router succeeds" do
      key = "router_conc_test_#{:rand.uniform(999_999)}"
      :ok = Router.put(FerricStore.Instance.get(:default), key, "hello")
      assert "hello" == Router.get(FerricStore.Instance.get(:default), key)
      # Clean up
      Router.delete(FerricStore.Instance.get(:default), key)
    end

    test "100 concurrent puts through router all succeed" do
      keys =
        for i <- 0..99 do
          "rconc_#{i}_#{:rand.uniform(999_999)}"
        end

      tasks =
        Enum.map(Enum.with_index(keys), fn {key, i} ->
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), key, "val_#{i}")
          end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 30_000))
      assert Enum.all?(results, &(&1 == :ok))

      # Verify all keys are readable
      for {key, i} <- Enum.with_index(keys) do
        assert "val_#{i}" == Router.get(FerricStore.Instance.get(:default), key),
               "Key #{key} not found after concurrent put"
      end

      # Clean up
      for key <- keys, do: Router.delete(FerricStore.Instance.get(:default), key)
    end

    test "concurrent puts and deletes through router do not crash" do
      base = "router_race_#{:rand.uniform(999_999)}"

      # Seed keys
      for i <- 0..19 do
        Router.put(FerricStore.Instance.get(:default), "#{base}_#{i}", "seed_#{i}")
      end

      writer_tasks =
        Enum.map(0..19, fn i ->
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), "#{base}_#{i}", "updated_#{i}")
          end)
        end)

      deleter_tasks =
        Enum.map(0..9, fn i ->
          Task.async(fn ->
            Router.delete(FerricStore.Instance.get(:default), "#{base}_#{i}")
          end)
        end)

      Enum.map(writer_tasks, &Task.await(&1, 30_000))
      Enum.map(deleter_tasks, &Task.await(&1, 30_000))

      # Shard processes are all still alive
      for i <- 0..3 do
        name = Router.shard_name(FerricStore.Instance.get(:default), i)
        assert Process.alive?(Process.whereis(name)),
               "Shard #{i} (#{name}) died during concurrent operations"
      end

      # Clean up surviving keys
      for i <- 0..19, do: Router.delete(FerricStore.Instance.get(:default), "#{base}_#{i}")
    end

    test "dbsize reflects concurrent operations" do
      base = "dbsize_conc_#{:rand.uniform(999_999)}"

      tasks =
        Enum.map(0..49, fn i ->
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), "#{base}_#{i}", "v_#{i}")
          end)
        end)

      Enum.map(tasks, &Task.await(&1, 30_000))

      # dbsize should include all 50 keys (plus any other keys from other tests)
      size = Router.dbsize(FerricStore.Instance.get(:default))
      assert size >= 50, "Expected dbsize >= 50, got #{size}"

      # Exists? should work correctly
      for i <- 0..49 do
        assert Router.exists?(FerricStore.Instance.get(:default), "#{base}_#{i}"),
               "Key #{base}_#{i} not found via exists? after concurrent put"
      end

      # Clean up
      for i <- 0..49, do: Router.delete(FerricStore.Instance.get(:default), "#{base}_#{i}")
    end

    test "MGET-style concurrent reads through router" do
      base = "mget_conc_#{:rand.uniform(999_999)}"

      # Seed 20 keys
      for i <- 0..19 do
        Router.put(FerricStore.Instance.get(:default), "#{base}_#{i}", "val_#{i}")
      end

      # 20 concurrent readers
      tasks =
        Enum.map(0..19, fn i ->
          Task.async(fn ->
            {i, Router.get(FerricStore.Instance.get(:default), "#{base}_#{i}")}
          end)
        end)

      results = Enum.map(tasks, &Task.await(&1, 30_000))

      for {i, value} <- results do
        assert value == "val_#{i}",
               "Concurrent get for key #{base}_#{i}: expected \"val_#{i}\", got #{inspect(value)}"
      end

      # Clean up
      for i <- 0..19, do: Router.delete(FerricStore.Instance.get(:default), "#{base}_#{i}")
    end
  end
end
