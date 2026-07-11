defmodule Ferricstore.Store.ConcurrencyTest do
  @moduledoc """
  Concurrency and stress tests for the Shard GenServer.

  These tests exercise a single shard under concurrent load: parallel puts,
  gets, deletes, and expiry reads. They verify that the shard process does not
  crash, does not return corrupted data, and keeps the ETS cache consistent
  with the Bitcask NIF store.

  All tests use `async: false` because they start real GenServer processes
  backed by the Bitcask NIF and named ETS tables.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Test.IsolatedInstance

  @task_timeout 30_000

  setup do
    :ok
  end

  # -------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------

  defp start_shard(_context) do
    # Use IsolatedInstance to get a ctx with matching shard count.
    # We only use shard 0 from it.
    ctx = IsolatedInstance.checkout(shard_count: 1)
    pid = Process.whereis(elem(ctx.shard_names, 0))
    index = 0

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{shard: pid, index: index, dir: ctx.data_dir, keydir: elem(ctx.keydir_refs, 0), ctx: ctx}
  end

  defp parallel(count, fun) do
    0..(count - 1)
    |> Enum.map(fn i -> Task.async(fn -> fun.(i) end) end)
    |> Enum.map(&Task.await(&1, @task_timeout))
  end

  # -------------------------------------------------------------------
  # Concurrent puts on a single shard
  # -------------------------------------------------------------------

  describe "concurrent puts on single shard" do
    setup :start_shard

    test "100 concurrent puts all succeed", %{shard: shard} do
      results =
        parallel(100, fn i ->
          GenServer.call(shard, {:put, "key_#{i}", "val_#{i}", 0})
        end)

      assert Enum.all?(results, &(&1 == :ok))

      keys = GenServer.call(shard, :keys)
      assert length(keys) == 100

      for i <- 0..99 do
        assert "key_#{i}" in keys
      end
    end

    test "concurrent puts to same key result in one final value", %{shard: shard} do
      results =
        parallel(50, fn i ->
          GenServer.call(shard, {:put, "shared_key", "val_#{i}", 0})
        end)

      assert Enum.all?(results, &(&1 == :ok))

      value = GenServer.call(shard, {:get, "shared_key"})
      assert is_binary(value)
      # The value must be one of the values that was written
      assert String.starts_with?(value, "val_")

      # Only one key should exist
      keys = GenServer.call(shard, :keys)
      assert keys == ["shared_key"]
    end

    test "concurrent puts and gets do not crash", %{shard: shard} do
      # Seed some keys so readers have something to read
      for i <- 0..19 do
        :ok = GenServer.call(shard, {:put, "key_#{i}", "seed_#{i}", 0})
      end

      # 20 writers and 20 readers in parallel
      writer_tasks =
        Enum.map(0..19, fn i ->
          Task.async(fn ->
            GenServer.call(shard, {:put, "key_#{i}", "updated_#{i}", 0})
          end)
        end)

      reader_tasks =
        Enum.map(0..19, fn i ->
          Task.async(fn ->
            GenServer.call(shard, {:get, "key_#{i}"})
          end)
        end)

      writer_results = Enum.map(writer_tasks, &Task.await(&1, @task_timeout))
      reader_results = Enum.map(reader_tasks, &Task.await(&1, @task_timeout))

      # All writes succeed
      assert Enum.all?(writer_results, &(&1 == :ok))

      # All reads return either the old or new value (no crashes, no BadArg)
      for result <- reader_results do
        assert is_binary(result) or is_nil(result)
      end

      # Shard is still alive and responsive
      assert Process.alive?(shard)
      assert :ok == GenServer.call(shard, {:put, "health_check", "ok", 0})
    end
  end

  # -------------------------------------------------------------------
  # Concurrent deletes
  # -------------------------------------------------------------------

  describe "concurrent deletes" do
    setup :start_shard

    test "concurrent deletes are idempotent", %{shard: shard} do
      # Seed 50 keys
      for i <- 0..49 do
        :ok = GenServer.call(shard, {:put, "key_#{i}", "val_#{i}", 0})
      end

      # 50 tasks each deleting a key; some keys will be targeted by multiple tasks
      results =
        parallel(50, fn i ->
          # Intentionally overlap: task i deletes key_(i rem 30) so some keys
          # get deleted by multiple tasks
          target = rem(i, 30)
          GenServer.call(shard, {:delete, "key_#{target}"})
        end)

      assert Enum.all?(results, &(&1 == :ok))

      # Shard is alive and keys list is consistent
      keys = GenServer.call(shard, :keys)
      assert is_list(keys)

      # Keys 0..29 were all targeted for deletion, keys 30..49 should remain
      for i <- 30..49 do
        assert "key_#{i}" in keys,
               "Expected key_#{i} to survive (was not targeted for deletion)"
      end

      # None of the deleted keys should remain
      for i <- 0..29 do
        refute "key_#{i}" in keys,
               "Expected key_#{i} to be deleted"
      end
    end

    test "put and delete race does not corrupt shard state", %{shard: shard} do
      # 10 tasks alternating put/delete on the same key, 10 iterations each
      results =
        parallel(10, fn _i ->
          for _ <- 1..10 do
            GenServer.call(shard, {:put, "race_key", "val", 0})
            GenServer.call(shard, {:delete, "race_key"})
          end

          :ok
        end)

      assert Enum.all?(results, &(&1 == :ok))

      # Shard is alive and responsive
      assert Process.alive?(shard)
      # The key is either present or absent, but the shard must respond correctly
      value = GenServer.call(shard, {:get, "race_key"})
      assert is_nil(value) or is_binary(value)
    end
  end

  # -------------------------------------------------------------------
  # Concurrent expiry
  # -------------------------------------------------------------------

  describe "concurrent expiry" do
    setup :start_shard

    test "concurrent reads of expired key all return nil", %{shard: shard} do
      past = System.os_time(:millisecond) - 5_000
      :ok = GenServer.call(shard, {:put, "expired_key", "old_value", past})

      results =
        parallel(50, fn _i ->
          GenServer.call(shard, {:get, "expired_key"})
        end)

      assert Enum.all?(results, &is_nil/1)
      assert Process.alive?(shard)
    end

    test "expiry during concurrent reads does not cause errors", %{shard: shard} do
      # Key expires 100ms from now -- some readers will see the value, others nil
      expire_at = System.os_time(:millisecond) + 100
      :ok = GenServer.call(shard, {:put, "expiring_key", "fleeting", expire_at})

      results =
        parallel(50, fn _i ->
          # Random sleep 0-200ms so some readers arrive before and after expiry
          Process.sleep(:rand.uniform(200))
          GenServer.call(shard, {:get, "expiring_key"})
        end)

      for result <- results do
        assert result in [nil, "fleeting"],
               "Expected nil or \"fleeting\", got: #{inspect(result)}"
      end

      assert Process.alive?(shard)
    end
  end

  # -------------------------------------------------------------------
  # Shard GenServer stability / stress
  # -------------------------------------------------------------------

  describe "shard genserver stability" do
    setup :start_shard

    test "shard survives 1000 sequential operations", %{shard: shard} do
      # Put 200 keys
      for i <- 0..199 do
        :ok = GenServer.call(shard, {:put, "seq_#{i}", "val_#{i}", 0})
      end

      # Get all 200
      for i <- 0..199 do
        assert "val_#{i}" == GenServer.call(shard, {:get, "seq_#{i}"})
      end

      # Delete first 100
      for i <- 0..99 do
        :ok = GenServer.call(shard, {:delete, "seq_#{i}"})
      end

      # Remaining 100 are still accessible
      for i <- 100..199 do
        assert "val_#{i}" == GenServer.call(shard, {:get, "seq_#{i}"})
      end

      # Deleted keys return nil
      for i <- 0..99 do
        assert nil == GenServer.call(shard, {:get, "seq_#{i}"})
      end

      keys = GenServer.call(shard, :keys)
      assert length(keys) == 100
    end

    test "shard responds within reasonable time under load", %{shard: shard} do
      {elapsed_us, _} =
        :timer.tc(fn ->
          for i <- 0..499 do
            :ok = GenServer.call(shard, {:put, "load_#{i}", String.duplicate("x", 100), 0})
          end
        end)

      # 30 seconds is extremely generous -- this should complete in under 5s
      # on any reasonable hardware, but CI machines can be slow
      assert elapsed_us < 30_000_000,
             "500 sequential puts took #{div(elapsed_us, 1_000)}ms, expected < 30_000ms"

      # Verify data integrity
      assert "xxxxx" <> _ = GenServer.call(shard, {:get, "load_250"})
      assert length(GenServer.call(shard, :keys)) == 500
    end
  end

  # -------------------------------------------------------------------
  # ETS cache under concurrency
  # -------------------------------------------------------------------

  describe "ETS cache under concurrency" do
    setup :start_shard

    test "concurrent puts populate ETS correctly", %{shard: shard, keydir: keydir} do
      results =
        parallel(30, fn i ->
          GenServer.call(shard, {:put, "ets_key_#{i}", "ets_val_#{i}", 0})
        end)

      assert Enum.all?(results, &(&1 == :ok))

      # Shard uses single-table format: {key, value, expire_at_ms, lfu_counter} in keydir
      entries =
        Enum.filter(:ets.tab2list(keydir), fn
          {<<"ets_key_", _rest::binary>>, _value, _expire_at_ms, _lfu, _fid, _off, _vsize} ->
            true

          _other ->
            false
        end)

      assert length(entries) == 30

      for i <- 0..29 do
        [{k, v, 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, "ets_key_#{i}")
        assert k == "ets_key_#{i}"
        assert v == "ets_val_#{i}"
      end
    end

    test "ETS and NIF stay consistent under concurrent writes", %{shard: shard} do
      # 20 tasks each put then immediately get their own key
      results =
        parallel(20, fn i ->
          key = "own_key_#{i}"
          value = "own_val_#{i}"
          :ok = GenServer.call(shard, {:put, key, value, 0})
          got = GenServer.call(shard, {:get, key})
          {key, value, got}
        end)

      for {key, expected, got} <- results do
        assert got == expected,
               "Key #{key}: expected #{inspect(expected)}, got #{inspect(got)}"
      end
    end

    test "ETS is cleared for deleted keys under concurrency", %{shard: shard, keydir: keydir} do
      # Seed 20 keys
      for i <- 0..19 do
        :ok = GenServer.call(shard, {:put, "del_ets_#{i}", "val_#{i}", 0})
      end

      # Concurrently delete all 20
      results =
        parallel(20, fn i ->
          GenServer.call(shard, {:delete, "del_ets_#{i}"})
        end)

      assert Enum.all?(results, &(&1 == :ok))

      for i <- 0..19 do
        assert [] == :ets.lookup(keydir, "del_ets_#{i}"),
               "Expected ETS entry for del_ets_#{i} to be removed"
      end
    end
  end
end
