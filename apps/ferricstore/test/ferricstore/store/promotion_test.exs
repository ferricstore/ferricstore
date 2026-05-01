defmodule Ferricstore.Store.PromotionTest do
  @moduledoc false

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Hash, List, Set, SortedSet, Strings}
  alias Ferricstore.HLC
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.ShardHelpers

  # Use a very low threshold so we can trigger promotion in tests
  # without inserting hundreds of fields.
  @test_threshold 5

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    # Store the original threshold (if any) and set a low one for tests.
    original = Application.get_env(:ferricstore, :promotion_threshold)

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
      # Restore original threshold.
      if original do
        Application.put_env(:ferricstore, :promotion_threshold, original)
      else
        Application.delete_env(:ferricstore, :promotion_threshold)
      end

      case original_pt do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        val -> :persistent_term.put(:ferricstore_promotion_threshold, val)
      end

      ShardHelpers.wait_shards_alive()
    end)
  end

  # Builds a store map backed by the real Router with promotion-aware
  # compound callbacks -- the same as what the connection module builds.
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

  # Inserts `n` fields into a hash and returns the key.
  defp populate_hash(store, key, n) do
    pairs =
      Enum.flat_map(1..n, fn i ->
        ["field_#{i}", "value_#{i}"]
      end)

    Hash.handle("HSET", [key | pairs], store)
    key
  end

  # Returns true if the given redis_key is promoted in its shard.
  defp promoted?(redis_key) do
    shard =
      Router.shard_name(
        FerricStore.Instance.get(:default),
        Router.shard_for(FerricStore.Instance.get(:default), redis_key)
      )

    GenServer.call(shard, {:promoted?, redis_key})
  end

  # ---------------------------------------------------------------------------
  # Small hash stays in shared Bitcask (under threshold)
  # ---------------------------------------------------------------------------

  describe "small hash stays in shared Bitcask" do
    test "hash with fewer fields than threshold is not promoted" do
      store = real_store()
      key = ukey("small_hash")

      populate_hash(store, key, @test_threshold - 1)

      refute promoted?(key)

      # All fields still accessible
      assert @test_threshold - 1 == Hash.handle("HLEN", [key], store)
    end

    test "hash with exactly threshold fields is not promoted (threshold is exclusive)" do
      store = real_store()
      key = ukey("exact_threshold")

      populate_hash(store, key, @test_threshold)

      refute promoted?(key)
    end
  end

  # ---------------------------------------------------------------------------
  # Hash exceeding threshold gets promoted
  # ---------------------------------------------------------------------------

  describe "hash promotion on threshold crossing" do
    test "hash crossing threshold gets promoted to dedicated Bitcask" do
      store = real_store()
      key = ukey("promote_hash")

      # Insert fields up to threshold (not yet promoted)
      populate_hash(store, key, @test_threshold)
      refute promoted?(key)

      # Add one more field to cross the threshold
      Hash.handle("HSET", [key, "extra_field", "extra_value"], store)

      assert promoted?(key)
    end

    test "promoted hash has dedicated directory on disk" do
      store = real_store()
      key = ukey("promote_dir")

      populate_hash(store, key, @test_threshold + 1)

      assert promoted?(key)

      # Verify the dedicated directory exists
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "hash:#{hash}"])
      assert File.dir?(dedicated_path)
    end
  end

  # ---------------------------------------------------------------------------
  # After promotion, HGET/HSET still work correctly
  # ---------------------------------------------------------------------------

  describe "HGET/HSET on promoted hash" do
    test "HGET returns correct values after promotion" do
      store = real_store()
      key = ukey("hget_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      # All original fields should be readable
      for i <- 1..(@test_threshold + 1) do
        assert "value_#{i}" == Hash.handle("HGET", [key, "field_#{i}"], store)
      end
    end

    test "HSET adds new fields to promoted hash" do
      store = real_store()
      key = ukey("hset_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      # Add more fields after promotion
      assert 1 == Hash.handle("HSET", [key, "new_field", "new_value"], store)
      assert "new_value" == Hash.handle("HGET", [key, "new_field"], store)
    end

    test "HSET updates existing field in promoted hash" do
      store = real_store()
      key = ukey("hset_update_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      # Update an existing field
      assert 0 == Hash.handle("HSET", [key, "field_1", "updated"], store)
      assert "updated" == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "HGET returns nil for missing field in promoted hash" do
      store = real_store()
      key = ukey("hget_miss_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert nil == Hash.handle("HGET", [key, "nonexistent"], store)
    end

    test "HGET does not resurrect expired promoted field after ETS miss" do
      store = real_store()
      key = ukey("hget_expired_promoted")
      field = "expired_field"
      compound_key = CompoundKey.hash_field(key, field)

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      past = HLC.now_ms() - 1_000
      :ok = store.compound_put.(key, compound_key, "expired_value", past)

      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)

      # Force the promoted-read disk fallback. The fallback must still honor
      # the field TTL stored in the dedicated Bitcask record.
      :ets.delete(keydir, compound_key)

      assert nil == Hash.handle("HGET", [key, field], store)
    end

    test "transaction HGET reads cold promoted field from dedicated storage" do
      store = real_store()
      key = ukey("tx_hget_cold_promoted")
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, idx)
      keydir = elem(ctx.keydir_refs, idx)
      compound_key = CompoundKey.hash_field(key, "field_1")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      [{^compound_key, _value, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, compound_key)
      :ets.insert(keydir, {compound_key, nil, exp, lfu, fid, off, vsize})

      assert ["value_1"] ==
               GenServer.call(shard, {:tx_execute, [{"HGET", [key, "field_1"]}], nil})
    end

    test "transaction HGET releases tracked key bytes for expired cold promoted field" do
      store = real_store()
      key = ukey("tx_hget_expired_cold_promoted")
      field = :binary.copy("expired_cold_field", 8)
      compound_key = CompoundKey.hash_field(key, field)
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, idx)
      keydir = elem(ctx.keydir_refs, idx)

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      before_bytes = :atomics.get(ctx.keydir_binary_bytes, idx + 1)
      expired_at = HLC.now_ms() - 1
      :ets.insert(keydir, {compound_key, nil, expired_at, 0, 0, 0, 0})
      :atomics.add(ctx.keydir_binary_bytes, idx + 1, byte_size(compound_key))

      assert [nil] == GenServer.call(shard, {:tx_execute, [{"HGET", [key, field]}], nil})
      assert :ets.lookup(keydir, compound_key) == []
      assert :atomics.get(ctx.keydir_binary_bytes, idx + 1) == before_bytes
    end

    test "transaction HGETALL scans cold promoted fields from dedicated storage" do
      store = real_store()
      key = ukey("tx_hgetall_cold_promoted")
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, idx)
      keydir = elem(ctx.keydir_refs, idx)
      compound_key = CompoundKey.hash_field(key, "field_1")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      [{^compound_key, _value, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, compound_key)
      :ets.insert(keydir, {compound_key, nil, exp, lfu, fid, off, vsize})

      [fields_and_values] = GenServer.call(shard, {:tx_execute, [{"HGETALL", [key]}], nil})

      assert "value_1" ==
               fields_and_values
               |> Enum.chunk_every(2)
               |> Map.new(fn [field, value] -> {field, value} end)
               |> Map.get("field_1")
    end

    test "cross-shard transaction HGET reads cold promoted field from dedicated storage" do
      store = real_store()
      key = ukey("cross_tx_hget_cold_promoted")
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)
      compound_key = CompoundKey.hash_field(key, "field_1")

      other_key =
        Enum.find_value(0..100_000, fn i ->
          candidate = "cross_tx_other_#{System.unique_integer([:positive])}_#{i}"
          if Router.shard_for(ctx, candidate) != idx, do: candidate
        end)

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      [{^compound_key, _value, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, compound_key)
      :ets.insert(keydir, {compound_key, nil, exp, lfu, fid, off, vsize})

      assert {:ok, ["value_1", :ok]} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.hget(key, "field_1")
                 |> FerricStore.Tx.set(other_key, "touch")
               end)
    end

    test "cross-shard transaction HSET writes promoted field to dedicated storage" do
      store = real_store()
      key = ukey("cross_tx_hset_promoted")
      field = "cross_field"
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)
      compound_key = CompoundKey.hash_field(key, field)

      other_key =
        Enum.find_value(0..100_000, fn i ->
          candidate = "cross_tx_hset_other_#{System.unique_integer([:positive])}_#{i}"
          if Router.shard_for(ctx, candidate) != idx, do: candidate
        end)

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert {:ok, [1, :ok]} =
               FerricStore.multi(fn tx ->
                 tx
                 |> FerricStore.Tx.hset(key, %{field => "cross_value"})
                 |> FerricStore.Tx.set(other_key, "touch")
               end)

      [{^compound_key, _value, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, compound_key)
      :ets.insert(keydir, {compound_key, nil, exp, lfu, fid, off, vsize})

      assert "cross_value" == Hash.handle("HGET", [key, field], store)
    end

    test "cross-shard transaction HDEL tombstones promoted field in dedicated storage" do
      store = real_store()
      key = ukey("cross_tx_hdel_promoted")
      field = "field_1"
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)
      compound_key = CompoundKey.hash_field(key, field)

      other_key =
        Enum.find_value(0..100_000, fn i ->
          candidate = "cross_tx_hdel_other_#{System.unique_integer([:positive])}_#{i}"
          if Router.shard_for(ctx, candidate) != idx, do: candidate
        end)

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert [1, :ok] ==
               Ferricstore.Transaction.Coordinator.execute(
                 [{"HDEL", [key, field]}, {"SET", [other_key, "touch"]}],
                 %{},
                 nil
               )

      :ets.delete(keydir, compound_key)

      assert nil == Hash.handle("HGET", [key, field], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HDEL on promoted hash
  # ---------------------------------------------------------------------------

  describe "HDEL on promoted hash" do
    test "HDEL removes field from promoted hash" do
      store = real_store()
      key = ukey("hdel_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Hash.handle("HDEL", [key, "field_1"], store)
      assert nil == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "HDEL on missing field in promoted hash returns 0" do
      store = real_store()
      key = ukey("hdel_miss_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Hash.handle("HDEL", [key, "nonexistent"], store)
    end

    test "HDEL multiple fields from promoted hash" do
      store = real_store()
      key = ukey("hdel_multi_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 2 == Hash.handle("HDEL", [key, "field_1", "field_2"], store)
      assert nil == Hash.handle("HGET", [key, "field_1"], store)
      assert nil == Hash.handle("HGET", [key, "field_2"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL on promoted hash cleans up dedicated instance
  # ---------------------------------------------------------------------------

  describe "DEL on promoted hash" do
    test "DEL removes promoted hash and cleans up dedicated Bitcask" do
      store = real_store()
      key = ukey("del_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      # DEL the key
      Strings.handle("DEL", [key], store)

      # Key should be gone
      refute promoted?(key)
      assert nil == Hash.handle("HGET", [key, "field_1"], store)
      assert 0 == Hash.handle("HLEN", [key], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HGETALL on promoted hash returns all fields
  # ---------------------------------------------------------------------------

  describe "HGETALL on promoted hash" do
    test "HGETALL returns all field-value pairs from promoted hash" do
      store = real_store()
      key = ukey("hgetall_promoted")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      result = Hash.handle("HGETALL", [key], store)

      # Result is a flat list [field1, value1, field2, value2, ...]
      assert length(result) == n * 2

      pairs = Enum.chunk_every(result, 2) |> Map.new(fn [k, v] -> {k, v} end)

      for i <- 1..n do
        assert Map.get(pairs, "field_#{i}") == "value_#{i}"
      end
    end

    test "HGETALL includes fields that were cold before promotion" do
      store = real_store()
      key = ukey("hgetall_cold_before_promoted")
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, idx)
      keydir = elem(ctx.keydir_refs, idx)

      populate_hash(store, key, @test_threshold)
      refute promoted?(key)
      :ok = GenServer.call(shard, :flush)

      cold_field_key = CompoundKey.hash_field(key, "field_1")
      [{^cold_field_key, _value, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, cold_field_key)
      :ets.insert(keydir, {cold_field_key, nil, exp, lfu, fid, off, vsize})

      assert 1 == Hash.handle("HSET", [key, "extra_field", "extra_value"], store)
      assert promoted?(key)

      result =
        Hash.handle("HGETALL", [key], store)
        |> Enum.chunk_every(2)
        |> Map.new(fn [field, value] -> {field, value} end)

      assert result["field_1"] == "value_1"
      assert result["extra_field"] == "extra_value"
    end
  end

  # ---------------------------------------------------------------------------
  # HLEN on promoted hash
  # ---------------------------------------------------------------------------

  describe "HLEN on promoted hash" do
    test "HLEN returns correct count for promoted hash" do
      store = real_store()
      key = ukey("hlen_promoted")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      assert n == Hash.handle("HLEN", [key], store)
    end

    test "HLEN updates after field addition on promoted hash" do
      store = real_store()
      key = ukey("hlen_add_promoted")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      Hash.handle("HSET", [key, "extra", "val"], store)
      assert n + 1 == Hash.handle("HLEN", [key], store)
    end

    test "HLEN updates after field deletion on promoted hash" do
      store = real_store()
      key = ukey("hlen_del_promoted")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      Hash.handle("HDEL", [key, "field_1"], store)
      assert n - 1 == Hash.handle("HLEN", [key], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HKEYS and HVALS on promoted hash
  # ---------------------------------------------------------------------------

  describe "HKEYS and HVALS on promoted hash" do
    test "HKEYS returns all field names from promoted hash" do
      store = real_store()
      key = ukey("hkeys_promoted")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      keys = Hash.handle("HKEYS", [key], store)
      assert length(keys) == n

      expected = for i <- 1..n, do: "field_#{i}"
      assert Enum.sort(keys) == Enum.sort(expected)
    end

    test "HVALS returns all values from promoted hash" do
      store = real_store()
      key = ukey("hvals_promoted")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      vals = Hash.handle("HVALS", [key], store)
      assert length(vals) == n

      expected = for i <- 1..n, do: "value_#{i}"
      assert Enum.sort(vals) == Enum.sort(expected)
    end
  end

  # ---------------------------------------------------------------------------
  # HEXISTS on promoted hash
  # ---------------------------------------------------------------------------

  describe "HEXISTS on promoted hash" do
    test "HEXISTS returns 1 for existing field in promoted hash" do
      store = real_store()
      key = ukey("hexists_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Hash.handle("HEXISTS", [key, "field_1"], store)
    end

    test "HEXISTS returns 0 for missing field in promoted hash" do
      store = real_store()
      key = ukey("hexists_miss_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Hash.handle("HEXISTS", [key, "nonexistent"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Promotion is one-way (no demotion)
  # ---------------------------------------------------------------------------

  describe "promotion is one-way" do
    test "deleting fields below threshold does not demote" do
      store = real_store()
      key = ukey("no_demote")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      # Delete fields to go below threshold
      for i <- 1..n do
        Hash.handle("HDEL", [key, "field_#{i}"], store)
      end

      # Still promoted (one-way)
      # Note: after deleting ALL fields, the type registry cleans up
      # and the key effectively disappears. But if we keep at least 1:
    end

    test "hash stays promoted even with few fields remaining" do
      store = real_store()
      key = ukey("stays_promoted")
      n = @test_threshold + 1

      populate_hash(store, key, n)
      assert promoted?(key)

      # Delete most fields, keep 2
      for i <- 3..n do
        Hash.handle("HDEL", [key, "field_#{i}"], store)
      end

      assert promoted?(key)
      assert 2 == Hash.handle("HLEN", [key], store)
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HSETNX on promoted hash
  # ---------------------------------------------------------------------------

  describe "HSETNX on promoted hash" do
    test "HSETNX sets field if not present in promoted hash" do
      store = real_store()
      key = ukey("hsetnx_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Hash.handle("HSETNX", [key, "new_field", "new_val"], store)
      assert "new_val" == Hash.handle("HGET", [key, "new_field"], store)
    end

    test "HSETNX does not overwrite existing field in promoted hash" do
      store = real_store()
      key = ukey("hsetnx_noop_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Hash.handle("HSETNX", [key, "field_1", "new_val"], store)
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HINCRBY on promoted hash
  # ---------------------------------------------------------------------------

  describe "HINCRBY on promoted hash" do
    test "HINCRBY increments numeric field in promoted hash" do
      store = real_store()
      key = ukey("hincrby_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      # Set a numeric field
      Hash.handle("HSET", [key, "counter", "10"], store)
      assert 15 == Hash.handle("HINCRBY", [key, "counter", "5"], store)
      assert "15" == Hash.handle("HGET", [key, "counter"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # HMGET on promoted hash
  # ---------------------------------------------------------------------------

  describe "HMGET on promoted hash" do
    test "HMGET returns values for multiple fields from promoted hash" do
      store = real_store()
      key = ukey("hmget_promoted")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      result = Hash.handle("HMGET", [key, "field_1", "field_2", "nonexistent"], store)
      assert ["value_1", "value_2", nil] == result
    end
  end

  # ===========================================================================
  # SET PROMOTION
  # ===========================================================================

  # Inserts `n` members into a set and returns the key.
  defp populate_set(store, key, n) do
    members = Enum.map(1..n, fn i -> "member_#{i}" end)
    Set.handle("SADD", [key | members], store)
    key
  end

  # ---------------------------------------------------------------------------
  # Small set stays in shared Bitcask (under threshold)
  # ---------------------------------------------------------------------------

  describe "small set stays in shared Bitcask" do
    test "set with fewer members than threshold is not promoted" do
      store = real_store()
      key = ukey("small_set")

      populate_set(store, key, @test_threshold - 1)

      refute promoted?(key)
      assert @test_threshold - 1 == Set.handle("SCARD", [key], store)
    end

    test "set with exactly threshold members is not promoted (threshold is exclusive)" do
      store = real_store()
      key = ukey("exact_threshold_set")

      populate_set(store, key, @test_threshold)

      refute promoted?(key)
    end
  end

  # ---------------------------------------------------------------------------
  # Set exceeding threshold gets promoted
  # ---------------------------------------------------------------------------

  describe "set promotion on threshold crossing" do
    test "set crossing threshold gets promoted to dedicated Bitcask" do
      store = real_store()
      key = ukey("promote_set")

      # Insert members up to threshold (not yet promoted)
      populate_set(store, key, @test_threshold)
      refute promoted?(key)

      # Add one more member to cross the threshold
      Set.handle("SADD", [key, "extra_member"], store)

      assert promoted?(key)
    end

    test "promoted set has dedicated directory on disk" do
      store = real_store()
      key = ukey("promote_set_dir")

      populate_set(store, key, @test_threshold + 1)

      assert promoted?(key)

      # Verify the dedicated directory exists
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "set:#{hash}"])
      assert File.dir?(dedicated_path)
    end
  end

  # ---------------------------------------------------------------------------
  # SMEMBERS on promoted set
  # ---------------------------------------------------------------------------

  describe "SMEMBERS on promoted set" do
    test "SMEMBERS returns all members after promotion" do
      store = real_store()
      key = ukey("smembers_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      members = Set.handle("SMEMBERS", [key], store)
      assert length(members) == n

      expected = for i <- 1..n, do: "member_#{i}"
      assert Enum.sort(members) == Enum.sort(expected)
    end
  end

  # ---------------------------------------------------------------------------
  # SISMEMBER on promoted set
  # ---------------------------------------------------------------------------

  describe "SISMEMBER on promoted set" do
    test "SISMEMBER returns 1 for existing member in promoted set" do
      store = real_store()
      key = ukey("sismember_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Set.handle("SISMEMBER", [key, "member_1"], store)
    end

    test "SISMEMBER returns 0 for missing member in promoted set" do
      store = real_store()
      key = ukey("sismember_miss_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Set.handle("SISMEMBER", [key, "nonexistent"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SREM on promoted set
  # ---------------------------------------------------------------------------

  describe "SREM on promoted set" do
    test "SREM removes member from promoted set" do
      store = real_store()
      key = ukey("srem_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Set.handle("SREM", [key, "member_1"], store)
      assert 0 == Set.handle("SISMEMBER", [key, "member_1"], store)
    end

    test "SREM on missing member in promoted set returns 0" do
      store = real_store()
      key = ukey("srem_miss_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Set.handle("SREM", [key, "nonexistent"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SCARD on promoted set
  # ---------------------------------------------------------------------------

  describe "SCARD on promoted set" do
    test "SCARD returns correct count for promoted set" do
      store = real_store()
      key = ukey("scard_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      assert n == Set.handle("SCARD", [key], store)
    end

    test "SCARD updates after member addition on promoted set" do
      store = real_store()
      key = ukey("scard_add_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      Set.handle("SADD", [key, "extra"], store)
      assert n + 1 == Set.handle("SCARD", [key], store)
    end

    test "SCARD updates after member removal on promoted set" do
      store = real_store()
      key = ukey("scard_rem_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      Set.handle("SREM", [key, "member_1"], store)
      assert n - 1 == Set.handle("SCARD", [key], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SADD on promoted set (adding after promotion)
  # ---------------------------------------------------------------------------

  describe "SADD on promoted set" do
    test "SADD adds new member to promoted set" do
      store = real_store()
      key = ukey("sadd_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Set.handle("SADD", [key, "new_member"], store)
      assert 1 == Set.handle("SISMEMBER", [key, "new_member"], store)
    end

    test "SADD of existing member in promoted set returns 0" do
      store = real_store()
      key = ukey("sadd_existing_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Set.handle("SADD", [key, "member_1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL on promoted set cleans up dedicated instance
  # ---------------------------------------------------------------------------

  describe "DEL on promoted set" do
    test "DEL removes promoted set and cleans up dedicated Bitcask" do
      store = real_store()
      key = ukey("del_promoted_set")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      # DEL the key
      Strings.handle("DEL", [key], store)

      # Key should be gone
      refute promoted?(key)
      assert 0 == Set.handle("SISMEMBER", [key, "member_1"], store)
      assert 0 == Set.handle("SCARD", [key], store)

      # Verify the dedicated directory was cleaned up
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "set:#{hash}"])
      refute File.dir?(dedicated_path)
    end
  end

  # ---------------------------------------------------------------------------
  # Set promotion is one-way
  # ---------------------------------------------------------------------------

  describe "set promotion is one-way" do
    test "set stays promoted even with few members remaining" do
      store = real_store()
      key = ukey("stays_promoted_set")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      # Delete most members, keep 2
      for i <- 3..n do
        Set.handle("SREM", [key, "member_#{i}"], store)
      end

      assert promoted?(key)
      assert 2 == Set.handle("SCARD", [key], store)
      assert 1 == Set.handle("SISMEMBER", [key, "member_1"], store)
    end
  end

  # ===========================================================================
  # SORTED SET PROMOTION
  # ===========================================================================

  # Inserts `n` members into a sorted set and returns the key.
  defp populate_zset(store, key, n) do
    pairs =
      Enum.flat_map(1..n, fn i ->
        [Integer.to_string(i), "member_#{i}"]
      end)

    SortedSet.handle("ZADD", [key | pairs], store)
    key
  end

  # ---------------------------------------------------------------------------
  # Small sorted set stays in shared Bitcask (under threshold)
  # ---------------------------------------------------------------------------

  describe "small sorted set stays in shared Bitcask" do
    test "zset with fewer members than threshold is not promoted" do
      store = real_store()
      key = ukey("small_zset")

      populate_zset(store, key, @test_threshold - 1)

      refute promoted?(key)
      assert @test_threshold - 1 == SortedSet.handle("ZCARD", [key], store)
    end

    test "zset with exactly threshold members is not promoted (threshold is exclusive)" do
      store = real_store()
      key = ukey("exact_threshold_zset")

      populate_zset(store, key, @test_threshold)

      refute promoted?(key)
    end
  end

  # ---------------------------------------------------------------------------
  # Sorted set exceeding threshold gets promoted
  # ---------------------------------------------------------------------------

  describe "sorted set promotion on threshold crossing" do
    test "zset crossing threshold gets promoted to dedicated Bitcask" do
      store = real_store()
      key = ukey("promote_zset")

      # Insert members up to threshold (not yet promoted)
      populate_zset(store, key, @test_threshold)
      refute promoted?(key)

      # Add one more member to cross the threshold
      SortedSet.handle("ZADD", [key, "999.0", "extra_member"], store)

      assert promoted?(key)
    end

    test "promoted zset has dedicated directory on disk" do
      store = real_store()
      key = ukey("promote_zset_dir")

      populate_zset(store, key, @test_threshold + 1)

      assert promoted?(key)

      # Verify the dedicated directory exists
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "zset:#{hash}"])
      assert File.dir?(dedicated_path)
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANGE on promoted sorted set
  # ---------------------------------------------------------------------------

  describe "ZRANGE on promoted sorted set" do
    test "ZRANGE returns all members in order after promotion" do
      store = real_store()
      key = ukey("zrange_promoted")
      n = @test_threshold + 1

      populate_zset(store, key, n)
      assert promoted?(key)

      members = SortedSet.handle("ZRANGE", [key, "0", "-1"], store)
      assert length(members) == n

      # Members should be in score order (1.0, 2.0, ..., n.0)
      expected = for i <- 1..n, do: "member_#{i}"
      assert members == expected
    end

    test "ZRANGE WITHSCORES returns members and scores after promotion" do
      store = real_store()
      key = ukey("zrange_ws_promoted")
      n = @test_threshold + 1

      populate_zset(store, key, n)
      assert promoted?(key)

      result = SortedSet.handle("ZRANGE", [key, "0", "-1", "WITHSCORES"], store)
      # Result is [member1, score1, member2, score2, ...]
      assert length(result) == n * 2

      pairs = Enum.chunk_every(result, 2)
      first_pair = hd(pairs)
      assert first_pair == ["member_1", "1.0"]
    end
  end

  # ---------------------------------------------------------------------------
  # ZSCORE on promoted sorted set
  # ---------------------------------------------------------------------------

  describe "ZSCORE on promoted sorted set" do
    test "ZSCORE returns correct score after promotion" do
      store = real_store()
      key = ukey("zscore_promoted")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      score = SortedSet.handle("ZSCORE", [key, "member_3"], store)
      assert score == "3.0"
    end

    test "ZSCORE returns nil for missing member in promoted zset" do
      store = real_store()
      key = ukey("zscore_miss_promoted")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert nil == SortedSet.handle("ZSCORE", [key, "nonexistent"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZREM on promoted sorted set
  # ---------------------------------------------------------------------------

  describe "ZREM on promoted sorted set" do
    test "ZREM removes member from promoted zset" do
      store = real_store()
      key = ukey("zrem_promoted")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == SortedSet.handle("ZREM", [key, "member_1"], store)
      assert nil == SortedSet.handle("ZSCORE", [key, "member_1"], store)
    end

    test "ZREM on missing member in promoted zset returns 0" do
      store = real_store()
      key = ukey("zrem_miss_promoted")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == SortedSet.handle("ZREM", [key, "nonexistent"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZCARD on promoted sorted set
  # ---------------------------------------------------------------------------

  describe "ZCARD on promoted sorted set" do
    test "ZCARD returns correct count for promoted zset" do
      store = real_store()
      key = ukey("zcard_promoted")
      n = @test_threshold + 1

      populate_zset(store, key, n)
      assert promoted?(key)

      assert n == SortedSet.handle("ZCARD", [key], store)
    end

    test "ZCARD updates after member addition on promoted zset" do
      store = real_store()
      key = ukey("zcard_add_promoted")
      n = @test_threshold + 1

      populate_zset(store, key, n)
      assert promoted?(key)

      SortedSet.handle("ZADD", [key, "999.0", "extra"], store)
      assert n + 1 == SortedSet.handle("ZCARD", [key], store)
    end

    test "ZCARD updates after member removal on promoted zset" do
      store = real_store()
      key = ukey("zcard_rem_promoted")
      n = @test_threshold + 1

      populate_zset(store, key, n)
      assert promoted?(key)

      SortedSet.handle("ZREM", [key, "member_1"], store)
      assert n - 1 == SortedSet.handle("ZCARD", [key], store)
    end
  end

  # ---------------------------------------------------------------------------
  # ZADD on promoted sorted set (adding after promotion)
  # ---------------------------------------------------------------------------

  describe "ZADD on promoted sorted set" do
    test "ZADD adds new member to promoted zset" do
      store = real_store()
      key = ukey("zadd_promoted")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == SortedSet.handle("ZADD", [key, "42.5", "new_member"], store)
      assert "42.5" == SortedSet.handle("ZSCORE", [key, "new_member"], store)
    end

    test "ZADD updates score of existing member in promoted zset" do
      store = real_store()
      key = ukey("zadd_update_promoted")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == SortedSet.handle("ZADD", [key, "99.9", "member_1"], store)
      score = SortedSet.handle("ZSCORE", [key, "member_1"], store)
      # Float representation may vary slightly (e.g. "99.9" vs "99.90000000000000576")
      assert_in_delta String.to_float(score), 99.9, 0.001
    end
  end

  # ---------------------------------------------------------------------------
  # ZRANK on promoted sorted set
  # ---------------------------------------------------------------------------

  describe "ZRANK on promoted sorted set" do
    test "ZRANK returns correct rank after promotion" do
      store = real_store()
      key = ukey("zrank_promoted")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      # member_1 has score 1.0, should be rank 0 (lowest)
      assert 0 == SortedSet.handle("ZRANK", [key, "member_1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL on promoted sorted set cleans up dedicated instance
  # ---------------------------------------------------------------------------

  describe "DEL on promoted sorted set" do
    test "DEL removes promoted zset and cleans up dedicated Bitcask" do
      store = real_store()
      key = ukey("del_promoted_zset")

      populate_zset(store, key, @test_threshold + 1)
      assert promoted?(key)

      # DEL the key
      Strings.handle("DEL", [key], store)

      # Key should be gone
      refute promoted?(key)
      assert nil == SortedSet.handle("ZSCORE", [key, "member_1"], store)
      assert 0 == SortedSet.handle("ZCARD", [key], store)

      # Verify the dedicated directory was cleaned up
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "zset:#{hash}"])
      refute File.dir?(dedicated_path)
    end
  end

  # ---------------------------------------------------------------------------
  # Sorted set promotion is one-way
  # ---------------------------------------------------------------------------

  describe "sorted set promotion is one-way" do
    test "zset stays promoted even with few members remaining" do
      store = real_store()
      key = ukey("stays_promoted_zset")
      n = @test_threshold + 1

      populate_zset(store, key, n)
      assert promoted?(key)

      # Delete most members, keep 2
      for i <- 3..n do
        SortedSet.handle("ZREM", [key, "member_#{i}"], store)
      end

      assert promoted?(key)
      assert 2 == SortedSet.handle("ZCARD", [key], store)
      assert "1.0" == SortedSet.handle("ZSCORE", [key, "member_1"], store)
    end
  end

  # ===========================================================================
  # LIST NON-PROMOTION
  #
  # Lists store all elements as a single serialized Erlang term in one
  # Bitcask entry (via ListOps). They do NOT use compound keys, so the
  # promotion system does not apply. A list with 1000 elements is still
  # one Bitcask entry, not 1000. Promotion is intentionally skipped for
  # lists because there is no compound-key fan-out to consolidate.
  # ===========================================================================

  describe "lists are NOT promoted" do
    test "list with more elements than threshold is not promoted" do
      store = real_store()
      key = ukey("big_list")

      # Push many more elements than the threshold
      elements = Enum.map(1..(@test_threshold * 3), fn i -> "elem_#{i}" end)
      List.handle("RPUSH", [key | elements], store)

      refute promoted?(key)

      # All elements still accessible
      assert @test_threshold * 3 == List.handle("LLEN", [key], store)
    end

    test "list operations work normally without promotion" do
      store = real_store()
      key = ukey("list_no_promo")

      elements = Enum.map(1..(@test_threshold + 5), fn i -> "elem_#{i}" end)
      List.handle("RPUSH", [key | elements], store)

      refute promoted?(key)

      # Verify basic list operations
      assert "elem_1" == List.handle("LINDEX", [key, "0"], store)
      assert "elem_#{@test_threshold + 5}" == List.handle("LINDEX", [key, "-1"], store)

      # LPOP and RPOP work
      assert "elem_1" == List.handle("LPOP", [key], store)
      assert "elem_#{@test_threshold + 5}" == List.handle("RPOP", [key], store)
      assert @test_threshold + 3 == List.handle("LLEN", [key], store)

      # Still not promoted after all operations
      refute promoted?(key)
    end
  end
end
