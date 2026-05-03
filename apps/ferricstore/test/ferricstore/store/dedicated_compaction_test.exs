defmodule Ferricstore.Store.DedicatedCompactionTest do
  @moduledoc """
  Tests for dedicated promoted Bitcask compaction.

  Covers: write-count trigger, file rotation during compaction, crash
  recovery with multiple files, ETS offset correctness after compaction,
  old file cleanup, and edge cases.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.{Promotion, Router}
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Test.ShardHelpers

  # Low threshold so we can trigger promotion in tests
  @test_threshold 5

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    original_promo = Application.get_env(:ferricstore, :promotion_threshold)

    original_pt =
      try do
        :persistent_term.get(:ferricstore_promotion_threshold)
      rescue
        ArgumentError -> :not_set
      end

    Application.put_env(:ferricstore, :promotion_threshold, @test_threshold)
    :persistent_term.put(:ferricstore_promotion_threshold, @test_threshold)

    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      if original_promo do
        Application.put_env(:ferricstore, :promotion_threshold, original_promo)
      else
        Application.delete_env(:ferricstore, :promotion_threshold)
      end

      case original_pt do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        val -> :persistent_term.put(:ferricstore_promotion_threshold, val)
      end

      ShardHelpers.flush_all_keys()
      ShardHelpers.wait_shards_alive()
    end)
  end

  defp real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn -> :ok end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end,
      compound_get: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get, redis_key, compound_key})
      end,
      compound_get_meta: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
      end,
      compound_delete: fn redis_key, compound_key ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete, redis_key, compound_key})
      end,
      compound_scan: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_scan, redis_key, prefix})
      end,
      compound_count: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_count, redis_key, prefix})
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard =
          Router.shard_name(
            FerricStore.Instance.get(:default),
            Router.shard_for(FerricStore.Instance.get(:default), redis_key)
          )

        GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
      end
    }
  end

  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  defp promote_hash(store, key) do
    pairs =
      Enum.flat_map(1..(@test_threshold + 1), fn i ->
        ["field_#{i}", "value_#{i}"]
      end)

    Hash.handle("HSET", [key | pairs], store)
    key
  end

  defp dedicated_dir(key) do
    shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    Promotion.dedicated_path(data_dir, shard_idx, :hash, key)
  end

  defp log_file_count(dir) do
    case File.ls(dir) do
      {:ok, files} -> Enum.count(files, &String.ends_with?(&1, ".log"))
      _ -> 0
    end
  end

  defp log_files(dir) do
    case File.ls(dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.sort()

      _ ->
        []
    end
  end

  # ---------------------------------------------------------------------------
  # Promoted hash with offset-based reads
  # ---------------------------------------------------------------------------

  describe "promoted reads use offsets" do
    test "HGET on promoted hash returns correct value" do
      store = real_store()
      key = ukey("offset_read")
      promote_hash(store, key)

      assert "value_3" == Hash.handle("HGET", [key, "field_3"], store)
    end

    test "HSET on promoted hash stores offset in ETS" do
      store = real_store()
      key = ukey("offset_ets")
      promote_hash(store, key)

      Hash.handle("HSET", [key, "new_field", "new_value"], store)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{shard_idx}"
      compound_key = "H:#{key}\0new_field"

      case :ets.lookup(keydir, compound_key) do
        [{^compound_key, _val, _exp, _lfu, fid, offset, _vsize}] ->
          assert is_integer(fid)
          assert offset > 0, "Expected non-zero offset, got #{offset}"

        [] ->
          flunk("Compound key not found in ETS")
      end
    end

    test "HGETALL on promoted hash returns all fields" do
      store = real_store()
      key = ukey("offset_getall")
      promote_hash(store, key)

      pairs = Hash.handle("HGETALL", [key], store)
      assert is_list(pairs)
      assert length(pairs) == (@test_threshold + 1) * 2
    end

    test "HGET on promoted hash reads cold offset zero from recorded dedicated fid" do
      store = real_store()
      key = ukey("offset_zero_cold")
      promote_hash(store, key)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{shard_idx}"
      dir = dedicated_dir(key)
      compound_key = "H:#{key}\0cold_zero"
      value = "cold-offset-zero"

      old_file = Path.join(dir, "00099.log")
      active_file = Path.join(dir, "00100.log")

      shared_file =
        Ferricstore.DataDir.shard_data_path(
          FerricStore.Instance.get(:default).data_dir,
          shard_idx
        )
        |> Path.join("00099.log")

      {:ok, {0, record_size}} = NIF.v2_append_record(old_file, compound_key, value, 0)

      {:ok, {0, _shared_record_size}} =
        NIF.v2_append_record(shared_file, compound_key, "wrong", 0)

      File.touch!(active_file)

      :ets.insert(
        keydir,
        {compound_key, nil, 0, Ferricstore.Store.LFU.initial(), 99, 0, record_size}
      )

      assert value == Hash.handle("HGET", [key, "cold_zero"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Compaction triggers on write count
  # ---------------------------------------------------------------------------

  describe "compaction on write count" do
    test "dedicated compaction still runs after restart with cold promotion marker" do
      store = real_store()
      key = ukey("restart_compact")
      promote_hash(store, key)

      dir = dedicated_dir(key)
      assert log_files(dir) == ["00000.log"]

      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_for_key(key)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      shard = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)
      state = :sys.get_state(shard)
      marker_key = Promotion.marker_key(key)

      assert [{^marker_key, "hash", _exp, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(state.keydir, marker_key)

      :sys.replace_state(shard, fn state ->
        dedicated_path = state.promoted_instances[key].path
        ShardCompound.compact_dedicated(state, key, dedicated_path)
      end)

      assert log_files(dir) == ["00001.log"]
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "many writes trigger compaction and data is still readable" do
      store = real_store()
      key = ukey("compact_trigger")
      promote_hash(store, key)

      dir = dedicated_dir(key)
      assert log_file_count(dir) >= 1

      # Write 1001 times to exceed the 1000 threshold
      for i <- 1..1001 do
        Hash.handle("HSET", [key, "churn_field", "value_#{i}"], store)
      end

      # All original fields still readable
      for i <- 1..(@test_threshold + 1) do
        expected = "value_#{i}"
        actual = Hash.handle("HGET", [key, "field_#{i}"], store)
        assert actual == expected, "field_#{i}: expected #{expected}, got #{inspect(actual)}"
      end

      # Churned field has latest value
      assert "value_1001" == Hash.handle("HGET", [key, "churn_field"], store)
    end

    test "writes after compaction still work" do
      store = real_store()
      key = ukey("post_compact")
      promote_hash(store, key)

      for i <- 1..1001 do
        Hash.handle("HSET", [key, "churn", "v#{i}"], store)
      end

      Hash.handle("HSET", [key, "after_compact", "new_value"], store)
      assert "new_value" == Hash.handle("HGET", [key, "after_compact"], store)
    end

    test "deletes count towards compaction threshold" do
      store = real_store()
      key = ukey("del_compact")
      promote_hash(store, key)

      for i <- 1..1001 do
        if rem(i, 2) == 0 do
          Hash.handle("HDEL", [key, "temp_field"], store)
        else
          Hash.handle("HSET", [key, "temp_field", "v#{i}"], store)
        end
      end

      # Original fields survive
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "HLEN is correct after compaction" do
      store = real_store()
      key = ukey("hlen_compact")
      promote_hash(store, key)

      expected_count = @test_threshold + 1

      # Overwrites only — no new fields
      for i <- 1..1001 do
        Hash.handle("HSET", [key, "field_1", "overwrite_#{i}"], store)
      end

      count = Hash.handle("HLEN", [key], store)
      assert count == expected_count
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery with multiple files (crash simulation)
  # ---------------------------------------------------------------------------

  describe "recovery with multiple files" do
    @describetag :shard_kill

    test "data survives shard restart after compaction" do
      store = real_store()
      key = ukey("recover_compact")
      promote_hash(store, key)

      for i <- 1..1001 do
        Hash.handle("HSET", [key, "churn", "v#{i}"], store)
      end

      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      ShardHelpers.eventually(
        fn ->
          "value_1" == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 should survive restart after compaction"
      )
    end

    test "crash leaves two files — recovery reads both" do
      store = real_store()
      key = ukey("crash_two")
      promote_hash(store, key)

      dir = dedicated_dir(key)

      # Simulate crash: manually create second file with extra entry
      new_file = Path.join(dir, "00001.log")
      File.touch!(new_file)
      NIF.v2_append_record(new_file, "H:#{key}\0crash_field", "crash_value", 0)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      # Old file entries survive
      ShardHelpers.eventually(
        fn ->
          "value_1" == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 from old file should survive"
      )

      # New file entries survive
      ShardHelpers.eventually(
        fn ->
          "crash_value" == Hash.handle("HGET", [key, "crash_field"], store)
        end,
        "crash_field from new file should survive"
      )
    end

    test "last-write-wins across files on recovery" do
      store = real_store()
      key = ukey("lww_recov")
      promote_hash(store, key)

      dir = dedicated_dir(key)

      # Write conflicting value in newer file
      new_file = Path.join(dir, "00001.log")
      File.touch!(new_file)
      NIF.v2_append_record(new_file, "H:#{key}\0field_1", "overwritten", 0)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      ShardHelpers.eventually(
        fn ->
          "overwritten" == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 should have value from newer file"
      )
    end

    test "tombstone in newer file deletes entry from older file" do
      store = real_store()
      key = ukey("tomb_recov")
      promote_hash(store, key)

      dir = dedicated_dir(key)

      new_file = Path.join(dir, "00001.log")
      File.touch!(new_file)
      NIF.v2_append_tombstone(new_file, "H:#{key}\0field_1")

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      # field_1 deleted
      ShardHelpers.eventually(
        fn ->
          nil == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 should be deleted by tombstone"
      )

      # Others survive
      ShardHelpers.eventually(
        fn ->
          "value_2" == Hash.handle("HGET", [key, "field_2"], store)
        end,
        "field_2 should survive"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "compaction with all fields deleted" do
      store = real_store()
      key = ukey("all_del")
      promote_hash(store, key)

      for i <- 1..(@test_threshold + 1) do
        Hash.handle("HDEL", [key, "field_#{i}"], store)
      end

      # Trigger compaction via writes
      for i <- 1..1001 do
        Hash.handle("HSET", [key, "temp", "v#{i}"], store)
        Hash.handle("HDEL", [key, "temp"], store)
      end

      # Should not crash
      dir = dedicated_dir(key)
      assert log_file_count(dir) >= 1
    end

    test "concurrent reads and writes on promoted hash" do
      store = real_store()
      key = ukey("concurrent")
      promote_hash(store, key)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            if rem(i, 2) == 0 do
              Hash.handle("HSET", [key, "conc_field", "val_#{i}"], store)
            else
              Hash.handle("HGET", [key, "field_1"], store)
            end
          end)
        end

      results = Task.await_many(tasks, 10_000)
      # No crashes — all tasks completed
      assert length(results) == 50
    end
  end
end
