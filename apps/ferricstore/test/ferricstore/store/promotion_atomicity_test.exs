defmodule Ferricstore.Store.PromotionAtomicityTest do
  @moduledoc """
  Tests that `Promotion.promote_collection!/6` is crash-safe with respect
  to full process restart + `recover_keydir` + `recover_promoted`.

  Promotion has three logical steps:

    1. Write marker record (shared log + ETS)
    2. Write dedicated-file batch + point keydir at dedicated
    3. Tombstone compound keys in shared log

  A kernel panic between any two steps leaves on-disk state that we
  must recover from. The invariant: after a full restart
  (ETS wiped, recover_keydir + recover_promoted re-run), ALL data is
  reachable via keydir.

  Crash points we test:

    * After step 1 only (marker in shared log, no dedicated data,
      compound keys still in shared log)
    * After steps 1+2 (marker + dedicated data, compound keys still
      in shared log, no tombstones)
    * After steps 1+2+3 (full success)

  The current (pre-fix) code writes in order 2 → 3 → 1, so a crash
  between 3 and 1 leaves tombstones with no marker → `recover_keydir`
  removes the compound keys from keydir, then `recover_promoted`
  doesn't find a marker → data silently vanishes.

  After the fix (order: 1 → 2 → 3), and with `recover_promoted`
  teaching a fallback for "marker present, dedicated empty", all
  crash points recover correctly.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundKey, LFU, LocalTxStore, Ops, Promotion}
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp setup_world do
    shard_index = 0
    tmp = Path.join(System.tmp_dir!(), "prom_atom_#{:erlang.unique_integer([:positive])}")
    data_dir = Path.join(tmp, "data")
    shard_data_path = Path.join([data_dir, "shard_#{shard_index}"])

    File.mkdir_p!(shard_data_path)
    active_path = Path.join(shard_data_path, "00000.log")
    File.touch!(active_path)

    keydir_name = :"keydir_promatom_#{:erlang.unique_integer([:positive])}"
    keydir = :ets.new(keydir_name, [:public, :named_table, :set])

    cleanup = fn ->
      try do
        :ets.delete(keydir_name)
      rescue
        _ -> :ok
      end

      File.rm_rf!(tmp)
    end

    %{
      tmp: tmp,
      data_dir: data_dir,
      shard_data_path: shard_data_path,
      active_path: active_path,
      keydir: keydir,
      shard_index: shard_index,
      cleanup: cleanup
    }
  end

  test "open_dedicated reports directory fsync failures", ctx do
    Process.put(:ferricstore_promotion_fsync_dir_hook, fn path ->
      send(self(), {:promotion_fsync_dir, path})
      {:error, :eio}
    end)

    try do
      redis_key = "hash:fsync-dir-failure"

      assert {:error, {:fsync_dir_failed, :create_dedicated_dir, :eio}} =
               Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      dedicated_path = Promotion.dedicated_path(ctx.data_dir, ctx.shard_index, :hash, redis_key)
      assert_received {:promotion_fsync_dir, parent}
      assert parent == Path.dirname(dedicated_path)
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
    end
  end

  test "cleanup_promoted reports directory fsync failures after removing dedicated dir", ctx do
    redis_key = "hash:cleanup-fsync-dir-failure"
    mk = Promotion.marker_key(redis_key)
    {:ok, {marker_offset, marker_size}} = NIF.v2_append_record(ctx.active_path, mk, "hash", 0)
    :ets.insert(ctx.keydir, {mk, "hash", 0, LFU.initial(), 0, marker_offset, marker_size})

    {:ok, dedicated_path} =
      Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

    Process.put(:ferricstore_promotion_fsync_dir_hook, fn path ->
      send(self(), {:promotion_fsync_dir, path})
      {:error, :eio}
    end)

    try do
      assert_raise RuntimeError, ~r/promotion cleanup directory fsync failed/, fn ->
        Promotion.cleanup_promoted!(
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )
      end

      assert_received {:promotion_fsync_dir, parent}
      assert parent == Path.dirname(dedicated_path)
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
    end
  end

  test "promote_collection reports open_dedicated fsync failures with context", ctx do
    {redis_key, _entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

    Process.put(:ferricstore_promotion_fsync_dir_hook, fn _path -> {:error, :eio} end)

    try do
      assert_raise RuntimeError, ~r/promotion open dedicated failed.*fsync_dir_failed/, fn ->
        Promotion.promote_collection!(
          :hash,
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )
      end
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
    end
  end

  defp seed_hash_entries(active_path, keydir) do
    redis_key = "user:1"
    fields = ~w(name email age city country)

    entries =
      Enum.map(fields, fn field ->
        compound_key = CompoundKey.hash_field(redis_key, field)
        value = "val_#{field}"
        {:ok, {offset, value_size}} = NIF.v2_append_record(active_path, compound_key, value, 0)
        :ets.insert(keydir, {compound_key, value, 0, LFU.initial(), 0, offset, value_size})
        {compound_key, value}
      end)

    {redis_key, entries}
  end

  # Simulates a process restart: wipe the in-memory keydir and re-run
  # recover_keydir + recover_promoted the same way Shard.init does.
  defp simulate_restart(ctx) do
    :ets.delete_all_objects(ctx.keydir)

    ShardLifecycle.recover_keydir(ctx.shard_data_path, ctx.keydir, ctx.shard_index)

    Promotion.recover_promoted(
      ctx.shard_data_path,
      ctx.keydir,
      ctx.data_dir,
      ctx.shard_index
    )
  end

  defp simulate_restart(ctx, instance_ctx) do
    :ets.delete_all_objects(ctx.keydir)

    ShardLifecycle.recover_keydir(ctx.shard_data_path, ctx.keydir, ctx.shard_index)

    Promotion.recover_promoted(
      ctx.shard_data_path,
      ctx.keydir,
      ctx.data_dir,
      ctx.shard_index,
      instance_ctx
    )
  end

  # Reads a compound key through the keydir: returns its on-disk value
  # by pread-ing at the (file_id, offset) the keydir records.
  defp read_ckey(ctx, ckey) do
    case :ets.lookup(ctx.keydir, ckey) do
      [{^ckey, value, _exp, _lfu, _fid, _off, _vs}] when is_binary(value) ->
        {:hot, value}

      [{^ckey, nil, _exp, _lfu, fid, off, _vs}] when fid >= 0 ->
        # Cold entry — read from dedicated dir or shared shard dir.
        for_shared =
          Path.join(
            ctx.shard_data_path,
            "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log"
          )

        case NIF.v2_pread_at(for_shared, off) do
          {:ok, v} when is_binary(v) -> {:cold_shared, v}
          _ -> :missing
        end

      [] ->
        :missing
    end
  end

  setup do
    ctx = setup_world()
    on_exit(ctx.cleanup)
    ctx
  end

  # ---------------------------------------------------------------------------
  # Baseline: full (successful) promotion is fully recoverable
  # ---------------------------------------------------------------------------

  describe "full successful promotion" do
    test "every compound key remains reachable after restart", ctx do
      {redis_key, entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      {:ok, _dedicated_path} =
        Promotion.promote_collection!(
          :hash,
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )

      # Before restart, keydir already sees dedicated.
      for {ckey, _v} <- entries do
        case :ets.lookup(ctx.keydir, ckey) do
          [{^ckey, _v, _e, _l, _fid, _off, _vs}] -> :ok
          _ -> flunk("#{ckey} missing immediately after promotion")
        end
      end

      # After restart, keydir must STILL have all compound keys.
      simulate_restart(ctx)

      for {ckey, _value} <- entries do
        assert read_ckey(ctx, ckey) != :missing,
               "after restart, compound #{ckey} vanished"
      end
    end

    test "recovered promoted entries keep value sizes for compaction accounting", ctx do
      {redis_key, entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      {:ok, _dedicated_path} =
        Promotion.promote_collection!(
          :hash,
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )

      promoted = simulate_restart(ctx)

      for {ckey, value} <- entries do
        assert [{^ckey, ^value, 0, _lfu, _fid, _off, vsize}] = :ets.lookup(ctx.keydir, ckey)
        assert vsize == byte_size(value)
      end

      info = Map.fetch!(promoted, redis_key)
      assert info.total_bytes > 0
      assert info.dead_bytes == 0
    end

    test "recovered promoted entries keep large values cold in ETS", ctx do
      redis_key = "cold:promoted:restart"
      large_value = String.duplicate("x", 128)

      entries =
        Enum.map(~w(field_a field_b), fn field ->
          compound_key = CompoundKey.hash_field(redis_key, field)

          {:ok, {offset, value_size}} =
            NIF.v2_append_record(ctx.active_path, compound_key, large_value, 0)

          :ets.insert(ctx.keydir, {compound_key, nil, 0, LFU.initial(), 0, offset, value_size})
          {compound_key, large_value}
        end)

      {:ok, _dedicated_path} =
        Promotion.promote_collection!(
          :hash,
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )

      instance_ctx = %{hot_cache_max_value_size: 8, keydir_binary_bytes: nil, shard_count: 1}
      promoted = simulate_restart(ctx, instance_ctx)
      info = Map.fetch!(promoted, redis_key)

      for {compound_key, expected_value} <- entries do
        assert [{^compound_key, nil, 0, _lfu, fid, off, value_size}] =
                 :ets.lookup(ctx.keydir, compound_key)

        assert value_size == byte_size(expected_value)
        path = Path.join(info.path, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")
        assert {:ok, ^expected_value} = NIF.v2_pread_at(path, off)
      end
    end

    test "promotion records marker location in the actual active file", ctx do
      {redis_key, _entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      rotated_active = Path.join(ctx.shard_data_path, "00005.log")
      File.touch!(rotated_active)

      {:ok, _dedicated_path} =
        Promotion.promote_collection!(
          :hash,
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )

      mk = Promotion.marker_key(redis_key)
      assert [{^mk, _type, 0, _lfu, 5, _off, _vsize}] = :ets.lookup(ctx.keydir, mk)
    end
  end

  describe "dedicated batch failure" do
    test "promotion aborts without tombstoning shared entries when dedicated batch fails", ctx do
      {redis_key, entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      dedicated_path = Promotion.dedicated_path(ctx.data_dir, ctx.shard_index, :hash, redis_key)
      File.mkdir_p!(dedicated_path)
      File.mkdir!(Path.join(dedicated_path, "00000.log"))

      assert_raise RuntimeError, ~r/promotion dedicated write failed/, fn ->
        Promotion.promote_collection!(
          :hash,
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )
      end

      assert {:ok, records} = NIF.v2_scan_file(ctx.active_path)

      for {ckey, _value} <- entries do
        matching =
          Enum.filter(records, fn {key, _off, _size, _exp, _tombstone?} -> key == ckey end)

        assert matching != []
        refute match?({_key, _off, _size, _exp, true}, List.last(matching))
        assert read_ckey(ctx, ckey) != :missing
      end
    end
  end

  describe "cleanup failure" do
    test "marker tombstone failure preserves marker and dedicated directory", ctx do
      redis_key = "user:cleanup"
      mk = Promotion.marker_key(redis_key)
      type_str = CompoundKey.encode_type(:hash)

      :ets.insert(ctx.keydir, {mk, type_str, 0, LFU.initial(), 0, 0, byte_size(type_str)})

      {:ok, dedicated_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      File.rm!(ctx.active_path)
      File.mkdir!(ctx.active_path)

      assert_raise RuntimeError, ~r/promotion cleanup marker tombstone failed/, fn ->
        Promotion.cleanup_promoted!(
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )
      end

      assert [{^mk, ^type_str, 0, _lfu, 0, 0, _vsize}] = :ets.lookup(ctx.keydir, mk)
      assert File.dir?(dedicated_path)
    end

    test "cold zset marker cleanup removes the zset dedicated directory", ctx do
      redis_key = "zset:cleanup:cold-marker"
      mk = Promotion.marker_key(redis_key)
      type_str = CompoundKey.encode_type(:zset)

      {:ok, {offset, value_size}} = NIF.v2_append_record(ctx.active_path, mk, type_str, 0)
      :ets.insert(ctx.keydir, {mk, nil, 0, LFU.initial(), 0, offset, value_size})

      {:ok, zset_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :zset, redis_key)

      assert File.dir?(zset_path)

      assert :ok =
               Promotion.cleanup_promoted!(
                 redis_key,
                 ctx.shard_data_path,
                 ctx.keydir,
                 ctx.data_dir,
                 ctx.shard_index
               )

      refute File.dir?(zset_path)
      assert [] = :ets.lookup(ctx.keydir, mk)
    end
  end

  # ---------------------------------------------------------------------------
  # Crash during marker-first promotion — every intermediate state recovers
  # ---------------------------------------------------------------------------

  describe "marker-first promotion: every crash point recovers" do
    test "invalid or unsupported promotion markers do not crash recovery", ctx do
      normal_key = "normal_after_bad_marker"
      {:ok, {offset, value_size}} = NIF.v2_append_record(ctx.active_path, normal_key, "live", 0)
      :ets.insert(ctx.keydir, {normal_key, "live", 0, LFU.initial(), 0, offset, value_size})

      invalid_marker = Promotion.marker_key("bad-marker")
      {:ok, {bad_off, bad_size}} = NIF.v2_append_record(ctx.active_path, invalid_marker, "bad", 0)
      :ets.insert(ctx.keydir, {invalid_marker, "bad", 0, LFU.initial(), 0, bad_off, bad_size})

      list_marker = Promotion.marker_key("list-marker")
      {:ok, {list_off, list_size}} = NIF.v2_append_record(ctx.active_path, list_marker, "list", 0)
      :ets.insert(ctx.keydir, {list_marker, "list", 0, LFU.initial(), 0, list_off, list_size})

      promoted = simulate_restart(ctx)

      assert promoted == %{}

      assert [{^normal_key, nil, 0, _lfu, 0, ^offset, _value_size}] =
               :ets.lookup(ctx.keydir, normal_key)

      assert {:cold_shared, "live"} == read_ckey(ctx, normal_key)

      assert [] == :ets.lookup(ctx.keydir, invalid_marker)
      assert [] == :ets.lookup(ctx.keydir, list_marker)
    end

    test "partial dedicated recovery builds the shared compound fallback index once" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/store/promotion.ex", __DIR__))

      [_match, recover_body] =
        Regex.run(
          ~r/(def recover_promoted\(.*?)(?=\n  defp shared_live_compound_keys_by_marker)/s,
          source
        )

      assert recover_body =~ "shared_live_compound_keys_by_marker(keydir, marker_types)"

      refute recover_body =~ "shared_uncovered_live_compound?(keydir",
             "promotion recovery must not scan the full ETS table once per promoted marker"
    end

    test "crash after step 1 (marker only) — fallback to compound keys in shared log", ctx do
      {redis_key, entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      # STEP 1: write marker
      type_str = CompoundKey.encode_type(:hash)
      mk = Promotion.marker_key(redis_key)
      {:ok, {moff, mvs}} = NIF.v2_append_record(ctx.active_path, mk, type_str, 0)
      :ets.insert(ctx.keydir, {mk, type_str, 0, LFU.initial(), 0, moff, mvs})

      # Open dedicated (creates dir + empty 00000.log) — this happens
      # inside promote_collection! before step 2, so simulate it.
      {:ok, _dedicated_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      # CRASH — no step 2, no step 3.

      # Simulate restart.
      simulate_restart(ctx)

      # Data must be reachable. With marker-first + fallback, compound
      # keys in shared log are still authoritative.
      for {ckey, _value} <- entries do
        result = read_ckey(ctx, ckey)
        assert result != :missing, "compound #{ckey} vanished after step-1 crash"
      end
    end

    test "crash after step 1 keeps public compound reads on shared storage", ctx do
      {redis_key, [{field_key, field_value} | _entries]} =
        seed_hash_entries(ctx.active_path, ctx.keydir)

      type_str = CompoundKey.encode_type(:hash)
      mk = Promotion.marker_key(redis_key)
      {:ok, {moff, mvs}} = NIF.v2_append_record(ctx.active_path, mk, type_str, 0)
      :ets.insert(ctx.keydir, {mk, type_str, 0, LFU.initial(), 0, moff, mvs})

      {:ok, _dedicated_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      promoted = simulate_restart(ctx)

      assert field_value == Ops.compound_get(local_tx(ctx, promoted), redis_key, field_key)
    end

    test "crash after step 2 (marker + dedicated, no tombstones)", ctx do
      {redis_key, entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      # Step 1: marker
      type_str = CompoundKey.encode_type(:hash)
      mk = Promotion.marker_key(redis_key)
      {:ok, {moff, mvs}} = NIF.v2_append_record(ctx.active_path, mk, type_str, 0)
      :ets.insert(ctx.keydir, {mk, type_str, 0, LFU.initial(), 0, moff, mvs})

      # Step 2: dedicated batch
      {:ok, dedicated_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      dedicated_active = Promotion.find_active(dedicated_path)
      batch = Enum.map(entries, fn {k, v} -> {k, v, 0} end)
      {:ok, _locs} = NIF.v2_append_batch(dedicated_active, batch)

      # CRASH — no step 3 (no tombstones yet).

      simulate_restart(ctx)

      for {ckey, _value} <- entries do
        assert read_ckey(ctx, ckey) != :missing, "compound #{ckey} vanished after step-2 crash"
      end
    end

    test "crash during partial dedicated batch keeps public reads on shared storage", ctx do
      redis_key = "user:partial-large"

      entries =
        Enum.map(~w(name email age city country), fn field ->
          compound_key = CompoundKey.hash_field(redis_key, field)
          value = :binary.copy("v", 70_000)

          {:ok, {offset, value_size}} =
            NIF.v2_append_record(ctx.active_path, compound_key, value, 0)

          :ets.insert(ctx.keydir, {compound_key, nil, 0, LFU.initial(), 0, offset, value_size})
          {compound_key, value}
        end)

      type_str = CompoundKey.encode_type(:hash)
      mk = Promotion.marker_key(redis_key)
      {:ok, {moff, mvs}} = NIF.v2_append_record(ctx.active_path, mk, type_str, 0)
      :ets.insert(ctx.keydir, {mk, type_str, 0, LFU.initial(), 0, moff, mvs})

      {:ok, dedicated_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      dedicated_active = Promotion.find_active(dedicated_path)

      partial_batch =
        entries
        |> Enum.take(2)
        |> Enum.map(fn {k, v} -> {k, v, 0} end)

      {:ok, _locs} = NIF.v2_append_batch(dedicated_active, partial_batch)

      promoted = simulate_restart(ctx)

      for {field_key, field_value} <- entries do
        assert field_value == Ops.compound_get(local_tx(ctx, promoted), redis_key, field_key)
      end
    end

    test "dedicated tombstone-only recovery removes stale shared rows", ctx do
      redis_key = "user:tombstone-only"
      field_key = CompoundKey.hash_field(redis_key, "name")
      field_value = "stale_shared"

      {:ok, {offset, value_size}} =
        NIF.v2_append_record(ctx.active_path, field_key, field_value, 0)

      :ets.insert(ctx.keydir, {field_key, field_value, 0, LFU.initial(), 0, offset, value_size})

      type_str = CompoundKey.encode_type(:hash)
      mk = Promotion.marker_key(redis_key)
      {:ok, {moff, mvs}} = NIF.v2_append_record(ctx.active_path, mk, type_str, 0)
      :ets.insert(ctx.keydir, {mk, type_str, 0, LFU.initial(), 0, moff, mvs})

      {:ok, dedicated_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      dedicated_active = Promotion.find_active(dedicated_path)
      {:ok, _delete_offset} = NIF.v2_append_tombstone(dedicated_active, field_key)

      promoted = simulate_restart(ctx)

      assert nil == Ops.compound_get(local_tx(ctx, promoted), redis_key, field_key)
      assert [] == :ets.lookup(ctx.keydir, field_key)
    end

    test "crash after step 3 (all three done) — same as full success", ctx do
      # This IS the full-success case, duplicated here for coverage
      # symmetry with the other crash points.
      {redis_key, entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      {:ok, _dp} =
        Promotion.promote_collection!(
          :hash,
          redis_key,
          ctx.shard_data_path,
          ctx.keydir,
          ctx.data_dir,
          ctx.shard_index
        )

      simulate_restart(ctx)

      for {ckey, _value} <- entries do
        assert read_ckey(ctx, ckey) != :missing
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Explicit: the BUGGY current ordering (dedicated-first) would lose data
  # ---------------------------------------------------------------------------

  describe "old (buggy) ordering regression guard" do
    # We simulate the OLD order manually and assert that, under a crash
    # between tombstones and marker, data IS lost.  This test is here to
    # document the failure mode that prompted the reorder and to detect
    # regression if someone moves things back.

    test "dedicated-first ordering + crash before marker = data loss detected", ctx do
      {redis_key, entries} = seed_hash_entries(ctx.active_path, ctx.keydir)

      # OLD step 1 — dedicated batch
      {:ok, dedicated_path} =
        Promotion.open_dedicated(ctx.data_dir, ctx.shard_index, :hash, redis_key)

      dedicated_active = Promotion.find_active(dedicated_path)
      batch = Enum.map(entries, fn {k, v} -> {k, v, 0} end)
      {:ok, _locs} = NIF.v2_append_batch(dedicated_active, batch)

      # OLD step 2 — tombstone compound keys in shared log
      Enum.each(entries, fn {ckey, _v} ->
        {:ok, _} = NIF.v2_append_tombstone(ctx.active_path, ckey)
      end)

      # CRASH — OLD step 3 (marker) never written.

      simulate_restart(ctx)

      # Without marker, recover_promoted doesn't open dedicated dir.
      # Compound keys are tombstoned, so recover_keydir removes them.
      # Result: data is gone.
      all_missing =
        Enum.all?(entries, fn {ckey, _value} ->
          read_ckey(ctx, ckey) == :missing
        end)

      assert all_missing,
             "if this assertion starts FAILING, the recovery path has been " <>
               "extended — update or remove this regression guard."
    end
  end

  defp local_tx(ctx, promoted_instances) do
    instance_ctx =
      FerricStore.Instance.build(
        :"prom_atom_instance_#{:erlang.unique_integer([:positive])}",
        data_dir: ctx.data_dir,
        shard_count: 1
      )

    %LocalTxStore{
      instance_ctx: instance_ctx,
      shard_index: ctx.shard_index,
      shard_state: %{
        instance_ctx: instance_ctx,
        keydir: ctx.keydir,
        index: ctx.shard_index,
        shard_data_path: ctx.shard_data_path,
        data_dir: ctx.data_dir,
        promoted_instances: promoted_instances
      }
    }
  end
end
