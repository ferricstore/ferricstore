defmodule Ferricstore.Store.PromotionTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet, Strings}
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.HLC
      alias Ferricstore.Store.{CompoundKey, Promotion, Router}
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Test.ShardHelpers

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

    test "promoted hash type metadata survives shard restart" do
      store = real_store()
      key = ukey("promote_restart_type")

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)
      assert {:simple, "hash"} = Strings.handle("TYPE", [key], store)

      ShardHelpers.flush_all_shards()

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      shard = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)
      old_pid = Process.whereis(shard)
      ref = Process.monitor(old_pid)

      Process.exit(old_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000
      ShardHelpers.wait_shards_alive()

      assert promoted?(key)
      assert {:simple, "hash"} = Strings.handle("TYPE", [key], store)
      assert {:error, "WRONGTYPE" <> _} = Set.handle("SADD", [key, "member"], store)
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
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

    test "dedicated compaction preserves cold large hash fields" do
      store = real_store()
      key = ukey("promoted_large_compact")
      large_value = String.duplicate("x", 70_000)

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      shard = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)
      {state, promoted_instance} = promoted_state(shard, key)
      dedicated_path = promoted_instance.path
      compound_key = CompoundKey.hash_field(key, "large_field")

      assert {:ok, {fid, offset, record_size}} =
               ShardCompound.promoted_write(dedicated_path, compound_key, large_value, 0)

      Ferricstore.Store.Shard.ETS.ets_insert_with_location(
        state,
        compound_key,
        large_value,
        0,
        fid,
        offset,
        record_size
      )

      assert [{^compound_key, nil, 0, _, fid, offset, value_size}] =
               :ets.lookup(state.keydir, compound_key)

      assert is_integer(fid)
      assert is_integer(offset)
      assert value_size > 0

      ShardCompound.compact_dedicated(state, key, dedicated_path)

      assert large_value == Hash.handle("HGET", [key, "large_field"], store)
    end

    test "repeated HGET preserves cold large promoted hash fields after ETS miss" do
      store = real_store()
      key = ukey("promoted_large_cold_repeat")
      field = "large_field"
      compound_key = CompoundKey.hash_field(key, field)
      large_value = String.duplicate("x", 70_000)

      populate_hash(store, key, @test_threshold + 1)
      assert promoted?(key)

      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, shard_idx)
      {state, promoted_instance} = promoted_state(shard, key)
      dedicated_path = promoted_instance.path

      assert {:ok, {fid, offset, record_size}} =
               ShardCompound.promoted_write(dedicated_path, compound_key, large_value, 0)

      Ferricstore.Store.Shard.ETS.ets_insert_with_location(
        state,
        compound_key,
        large_value,
        0,
        fid,
        offset,
        record_size
      )

      assert [{^compound_key, nil, 0, _, ^fid, ^offset, _value_size}] =
               :ets.lookup(state.keydir, compound_key)

      assert large_value == Hash.handle("HGET", [key, field], store)
      assert large_value == Hash.handle("HGET", [key, field], store)
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

      # Force the promoted-read ETS-miss path. Promoted reads should trust ETS
      # and avoid scanning the dedicated file on request path.
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

    test "promotion rejects mismatched cold offsets" do
      store = real_store()
      key = ukey("promote_stale_cold_offset")
      ctx = FerricStore.Instance.get(:default)
      idx = Router.shard_for(ctx, key)
      keydir = elem(ctx.keydir_refs, idx)
      {_file_id, file_path, _shard_path} = Ferricstore.Store.ActiveFile.get(ctx, idx)

      populate_hash(store, key, @test_threshold)
      refute promoted?(key)

      target_field = "stale_target"
      target_key = CompoundKey.hash_field(key, target_field)
      other_key = CompoundKey.hash_field(key, "stale_other")

      {:ok, [{other_offset, _}, {_target_offset, target_size}]} =
        NIF.v2_append_batch(file_path, [
          {other_key, "wrong-value", 0},
          {target_key, "right-value", 0}
        ])

      :ets.insert(
        keydir,
        {target_key, nil, 0, Ferricstore.Store.LFU.initial(), 0, other_offset, target_size}
      )

      assert 1 == Hash.handle("HSET", [key, "extra_field", "extra_value"], store)
      assert promoted?(key)

      result =
        Hash.handle("HGETALL", [key], store)
        |> Enum.chunk_every(2)
        |> Map.new(fn [field, value] -> {field, value} end)

      refute result[target_field] == "wrong-value"
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
    end
  end
end
