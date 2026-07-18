defmodule Ferricstore.Store.PromotionRecoveryGuardTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{CompoundKey, LFU, Promotion}
  alias Ferricstore.Store.Shard.Compound.Promoted
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle

  @promotion_path Path.expand("../../../lib/ferricstore/store/promotion.ex", __DIR__)

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "promotion_recovery_guard_#{System.unique_integer([:positive])}"
      )

    data_dir = Path.join(root, "data")
    shard_path = Path.join(data_dir, "shard_0")
    shared_log = Path.join(shard_path, "00000.log")
    File.mkdir_p!(shard_path)
    File.touch!(shared_log)

    keydir = :ets.new(:promotion_recovery_guard, [:public, :set])

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    %{data_dir: data_dir, keydir: keydir, shard_path: shard_path, shared_log: shared_log}
  end

  test "promoted recovery uses bounded scan pages", ctx do
    previous_page_size = Application.fetch_env(:ferricstore, :recovery_scan_page_size)
    Application.put_env(:ferricstore, :recovery_scan_page_size, 2)

    on_exit(fn ->
      restore_env(:recovery_scan_page_size, previous_page_size)
    end)

    redis_key = "paged-recovery"
    put_marker(ctx, redis_key, "hash")

    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)

    entries =
      [{CompoundKey.type_key(redis_key), "hash", 0}] ++
        for index <- 1..5 do
          {CompoundKey.hash_field(redis_key, "field-#{index}"), "value-#{index}", 0}
        end

    assert {:ok, _locations} = NIF.v2_append_batch(dedicated_log, entries)

    tables_before = owned_ets_tables()

    Process.put(:ferricstore_promotion_recovery_read_hook, fn path, offset, key ->
      observe_recovery_plan_table!(tables_before)
      Ferricstore.Store.ColdRead.pread_keyed(path, offset, key, 10_000)
    end)

    try do
      assert %{^redis_key => %{path: ^dedicated_path}} =
               Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    after
      Process.delete(:ferricstore_promotion_recovery_read_hook)
    end

    assert_recovery_plan_table_cleaned!()

    Enum.each(entries, fn {key, value, 0} ->
      assert [{^key, ^value, 0, _lfu, 0, _offset, _value_size}] =
               :ets.lookup(ctx.keydir, key)
    end)

    calls = nif_calls(@promotion_path)
    refute {:NIF, :v2_scan_file, 1} in calls
    assert {:NIF, :v2_scan_file_page, 3} in calls
  end

  test "dedicated active-file discovery ignores numeric symlinks", ctx do
    dedicated_path = Path.join(ctx.data_dir, "dedicated-symlink-guard")
    File.mkdir_p!(dedicated_path)
    active_path = Path.join(dedicated_path, "00000.log")
    File.touch!(active_path)
    external_path = Path.join(ctx.data_dir, "external.log")
    File.write!(external_path, "external")
    File.ln_s!(external_path, Path.join(dedicated_path, "99999.log"))

    assert Promotion.find_active(dedicated_path) == active_path
  end

  test "dedicated active-file discovery rejects noncanonical segment aliases", ctx do
    dedicated_path = Path.join(ctx.data_dir, "dedicated-segment-alias-guard")
    File.mkdir_p!(dedicated_path)
    File.touch!(Path.join(dedicated_path, "00000.log"))
    File.touch!(Path.join(dedicated_path, "0.log"))

    assert_raise RuntimeError, ~r/noncanonical_segment_filename/, fn ->
      Promotion.find_active(dedicated_path)
    end
  end

  test "dedicated active-file discovery fails closed when the directory cannot be listed", ctx do
    invalid_path = Path.join(ctx.data_dir, "dedicated-not-a-directory")
    File.write!(invalid_path, "not a directory")

    assert_raise RuntimeError, ~r/promotion active-file discovery failed.*not_a_directory/, fn ->
      Promotion.find_active(invalid_path)
    end
  end

  test "promoted directory accounting ignores numeric symlinks", ctx do
    dedicated_path = Path.join(ctx.data_dir, "dedicated-size-symlink-guard")
    File.mkdir_p!(dedicated_path)
    File.write!(Path.join(dedicated_path, "00000.log"), "local")
    external_path = Path.join(ctx.data_dir, "external-size.log")
    File.write!(external_path, :binary.copy("x", 1_024))
    File.ln_s!(external_path, Path.join(dedicated_path, "00001.log"))

    assert Promoted.promoted_dir_size(dedicated_path) == byte_size("local")
  end

  test "promoted recovery does not retain obsolete dedicated tombstones", ctx do
    previous_page_size = Application.fetch_env(:ferricstore, :recovery_scan_page_size)
    Application.put_env(:ferricstore, :recovery_scan_page_size, 3)

    on_exit(fn ->
      restore_env(:recovery_scan_page_size, previous_page_size)
    end)

    redis_key = "bounded-recovery-state"
    put_marker(ctx, redis_key, "hash")

    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    live_key = CompoundKey.hash_field(redis_key, "live")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, type_key, "hash", 0)

    Enum.each(1..64, fn index ->
      key = CompoundKey.hash_field(redis_key, "deleted-#{index}")
      assert {:ok, {_offset, _value_size}} = NIF.v2_append_record(dedicated_log, key, "value", 0)
      assert {:ok, _offset} = NIF.v2_append_tombstone(dedicated_log, key)
    end)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, live_key, "live-value", 0)

    Process.put(:promotion_recovery_max_retained_rows, 0)
    Process.put(:promotion_recovery_observed_pages, 0)

    Process.put(:ferricstore_promotion_recovery_state_hook, fn retained_rows ->
      Process.put(
        :promotion_recovery_max_retained_rows,
        max(Process.get(:promotion_recovery_max_retained_rows, 0), retained_rows)
      )

      Process.put(
        :promotion_recovery_observed_pages,
        Process.get(:promotion_recovery_observed_pages, 0) + 1
      )
    end)

    try do
      assert %{^redis_key => %{path: ^dedicated_path}} =
               Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)

      assert Process.get(:promotion_recovery_observed_pages, 0) > 1
      assert Process.get(:promotion_recovery_max_retained_rows, 0) <= 2
    after
      Process.delete(:ferricstore_promotion_recovery_state_hook)
      Process.delete(:promotion_recovery_max_retained_rows)
      Process.delete(:promotion_recovery_observed_pages)
    end

    assert [{^type_key, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, type_key)

    assert [{^live_key, "live-value", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, live_key)
  end

  test "promoted recovery releases each collection plan before reading the next", ctx do
    redis_keys = ["first-bounded-plan", "second-bounded-plan"]

    Enum.each(redis_keys, fn redis_key ->
      put_marker(ctx, redis_key, "hash")
      {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
      dedicated_log = Promotion.find_active(dedicated_path)

      assert {:ok, _locations} =
               NIF.v2_append_batch(dedicated_log, [
                 {CompoundKey.type_key(redis_key), "hash", 0},
                 {CompoundKey.hash_field(redis_key, "field"), "value", 0}
               ])
    end)

    tables_before = owned_ets_tables()
    Process.put(:promotion_recovery_plan_sizes, [])

    Process.put(:ferricstore_promotion_recovery_read_hook, fn path, offset, key ->
      [plan] =
        owned_ets_tables()
        |> MapSet.difference(tables_before)
        |> Enum.filter(fn table -> :ets.info(table, :name) == :promotion_recovery_plan end)

      Process.put(
        :promotion_recovery_plan_sizes,
        [:ets.info(plan, :size) | Process.get(:promotion_recovery_plan_sizes, [])]
      )

      Ferricstore.Store.ColdRead.pread_keyed(path, offset, key, 10_000)
    end)

    try do
      recovered = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
      assert Map.keys(recovered) |> Enum.sort() == Enum.sort(redis_keys)

      sizes = Process.get(:promotion_recovery_plan_sizes, [])
      assert length(sizes) == 4
      assert Enum.max(sizes) == 0
    after
      Process.delete(:ferricstore_promotion_recovery_read_hook)
      Process.delete(:promotion_recovery_plan_sizes)
    end
  end

  test "promoted recovery fails closed on malformed keydir rows", ctx do
    redis_key = "malformed-keydir"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    member_key = CompoundKey.hash_field(redis_key, "field")

    assert {:ok, _locations} =
             NIF.v2_append_batch(dedicated_log, [
               {CompoundKey.type_key(redis_key), "hash", 0},
               {member_key, "value", 0}
             ])

    :ets.insert(ctx.keydir, {:malformed, :row})

    assert_raise RuntimeError, ~r/index_shared_compound.*invalid_keydir_row/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)

    assert [] = :ets.lookup(ctx.keydir, member_key)
  end

  test "an unreadable cold marker aborts recovery without deleting the marker", ctx do
    redis_key = "unreadable-marker"
    {marker, offset, value_size} = put_marker(ctx, redis_key, "hash", :cold)
    backup_log = ctx.shared_log <> ".unreadable"
    File.rename!(ctx.shared_log, backup_log)

    assert_raise RuntimeError, ~r/promotion recovery read_marker failed/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert [{^marker, nil, 0, _lfu, 0, ^offset, ^value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  test "a readable but invalid cold marker remains intentionally ignorable", ctx do
    redis_key = "invalid-readable-marker"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "unsupported", :cold)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
  end

  test "dedicated open errors abort recovery and preserve the marker", ctx do
    redis_key = "open-error"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    put_shared_hash_field(ctx, redis_key, "field", "shared")
    Process.put(:ferricstore_promotion_fsync_dir_hook, fn _path -> {:error, :eio} end)

    try do
      assert_raise RuntimeError, ~r/promotion recovery open_dedicated failed.*eio/, fn ->
        Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
      end

      assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
               :ets.lookup(ctx.keydir, marker)
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
    end
  end

  test "a missing dedicated directory without a shared source aborts recovery", ctx do
    redis_key = "missing-dedicated"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    dedicated_path = Promotion.dedicated_path(ctx.data_dir, 0, :hash, redis_key)

    assert_raise RuntimeError, ~r/promotion recovery open_dedicated failed.*missing/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    refute File.dir?(dedicated_path)

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  test "an interrupted promotion whose shared source expired is durably cleaned", ctx do
    redis_key = "expired-shared-source"
    marker = Promotion.marker_key(redis_key)
    type_key = CompoundKey.type_key(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")
    expired_at_ms = HLC.now_ms() - 1
    put_shared_record(ctx, marker, "hash", 0)
    put_shared_record(ctx, type_key, "hash", expired_at_ms)
    put_shared_record(ctx, field_key, "expired", expired_at_ms)

    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
    refute File.dir?(Promotion.dedicated_path(ctx.data_dir, 0, :hash, redis_key))

    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
    assert [] = :ets.lookup(ctx.keydir, type_key)
    assert [] = :ets.lookup(ctx.keydir, field_key)
  end

  test "an expired dedicated collection is durably cleaned", ctx do
    redis_key = "expired-dedicated"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    expired_at_ms = HLC.now_ms() - 1

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(
               dedicated_log,
               CompoundKey.type_key(redis_key),
               "hash",
               expired_at_ms
             )

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(
               dedicated_log,
               CompoundKey.hash_field(redis_key, "field"),
               "expired",
               expired_at_ms
             )

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
    refute File.dir?(dedicated_path)

    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
  end

  @tag :hlc_drift_guard
  test "recovery preserves wall-live dedicated collections during unsafe HLC drift", ctx do
    redis_key = "wall-live-dedicated"
    put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")
    wall_ms = System.os_time(:millisecond)
    expire_at_ms = wall_ms + 30_000

    assert {:ok, _locations} =
             NIF.v2_append_batch(dedicated_log, [
               {type_key, "hash", 0},
               {field_key, "wall-live", expire_at_ms}
             ])

    hlc_ref = :persistent_term.get(:ferricstore_hlc_ref)
    previous_hlc = :atomics.get(hlc_ref, 1)

    try do
      :atomics.put(hlc_ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

      assert %{^redis_key => %{path: ^dedicated_path}} =
               Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)

      assert [{^field_key, "wall-live", ^expire_at_ms, _lfu, 0, _offset, _value_size}] =
               :ets.lookup(ctx.keydir, field_key)

      assert File.dir?(dedicated_path)
    after
      :atomics.put(hlc_ref, 1, previous_hlc)
    end
  end

  test "expired cleanup intent survives a final tombstone failure", ctx do
    redis_key = "expired-cleanup-retry"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    expired_at_ms = HLC.now_ms() - 1

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(
               dedicated_log,
               CompoundKey.type_key(redis_key),
               "hash",
               expired_at_ms
             )

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(
               dedicated_log,
               CompoundKey.hash_field(redis_key, "field"),
               "expired",
               expired_at_ms
             )

    Process.put(:ferricstore_promotion_fsync_dir_hook, fn path ->
      File.chmod!(ctx.shared_log, 0o400)
      NIF.v2_fsync_dir(path)
    end)

    try do
      assert_raise RuntimeError, ~r/cleanup_expired_promoted failed.*Permission denied/, fn ->
        Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
      end
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
      File.chmod!(ctx.shared_log, 0o600)
    end

    refute File.dir?(dedicated_path)
    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)

    assert [{^marker, nil, 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
  end

  test "expired cleanup rejects incomplete tombstone batch results", ctx do
    redis_key = "expired-cleanup-short-batch"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    expired_at_ms = HLC.now_ms() - 1

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(
               dedicated_log,
               CompoundKey.type_key(redis_key),
               "hash",
               expired_at_ms
             )

    Process.put(:ferricstore_promotion_recovery_cleanup_append_hook, fn _path, _ops ->
      {:ok, []}
    end)

    try do
      assert_raise RuntimeError,
                   ~r/cleanup_expired_promoted failed.*location_count_mismatch/,
                   fn ->
                     Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
                   end
    after
      Process.delete(:ferricstore_promotion_recovery_cleanup_append_hook)
    end

    refute File.dir?(dedicated_path)
    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)
    assert [{^marker, nil, 0, _lfu, 0, _offset, _value_size}] = :ets.lookup(ctx.keydir, marker)
  end

  test "a dedicated type with no live members is durably cleaned", ctx do
    redis_key = "empty-dedicated"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, type_key, "hash", 0)

    assert {:ok, _offset} = NIF.v2_append_tombstone(dedicated_log, field_key)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
    refute File.dir?(dedicated_path)
  end

  test "shared fallback removes the in-memory marker so routing stays shared", ctx do
    redis_key = "shared-fallback"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    shared_key = put_shared_hash_field(ctx, redis_key, "field", "shared")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)

    assert [{^shared_key, "shared", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, shared_key)

    state = %{
      promoted_instances: %{},
      ets: ctx.keydir,
      data_dir: ctx.data_dir,
      index: 0
    }

    assert nil == Promoted.promoted_store(state, redis_key)
    refute File.dir?(dedicated_path)

    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)

    assert [{^shared_key, nil, 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, shared_key)

    type_key = CompoundKey.type_key(redis_key)

    assert [{^type_key, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, type_key)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
  end

  test "shared fallback rejects type metadata that disagrees with the marker", ctx do
    redis_key = "shared-type-mismatch"
    marker = Promotion.marker_key(redis_key)
    type_key = CompoundKey.type_key(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")
    put_shared_record(ctx, marker, "hash", 0)
    put_shared_record(ctx, type_key, "set", 0)
    put_shared_record(ctx, field_key, "value", 0)
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)

    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)

    assert_raise RuntimeError, ~r/shared_type_mismatch/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert File.dir?(dedicated_path)

    assert [{^marker, nil, 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)

    assert [{^type_key, "set", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, type_key)
  end

  test "fallback directory fsync failure aborts before tombstoning the marker", ctx do
    redis_key = "fallback-fsync-error"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    put_shared_hash_field(ctx, redis_key, "field", "shared")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    Process.put(:ferricstore_promotion_fsync_dir_hook, fn _path -> {:error, :eio} end)

    try do
      assert_raise RuntimeError,
                   ~r/promotion recovery rollback_incomplete_dedicated failed.*eio/,
                   fn ->
                     Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
                   end

      refute File.dir?(dedicated_path)

      assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
               :ets.lookup(ctx.keydir, marker)

      :ets.delete_all_objects(ctx.keydir)
      assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)

      assert [{^marker, _value, 0, _lfu, 0, _offset, _value_size}] =
               :ets.lookup(ctx.keydir, marker)
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
    end
  end

  test "fallback intent retry preserves the shared collection", ctx do
    redis_key = "fallback-intent-retry"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    shared_key = put_shared_hash_field(ctx, redis_key, "field", "shared")
    type_key = CompoundKey.type_key(redis_key)
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)

    Process.put(:ferricstore_promotion_fsync_dir_hook, fn path ->
      File.chmod!(ctx.shared_log, 0o400)
      NIF.v2_fsync_dir(path)
    end)

    try do
      assert_raise RuntimeError,
                   ~r/rollback_incomplete_dedicated failed.*Permission denied/,
                   fn ->
                     Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
                   end
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
      File.chmod!(ctx.shared_log, 0o600)
    end

    refute File.dir?(dedicated_path)
    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)

    assert [{^marker, nil, 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)

    assert [{^type_key, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, type_key)

    assert [{^shared_key, nil, 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, shared_key)
  end

  test "fallback active-log discovery errors preserve marker and dedicated directory", ctx do
    redis_key = "fallback-active-discovery-error"
    marker = Promotion.marker_key(redis_key)
    newer_shared_log = Path.join(ctx.shard_path, "00001.log")
    File.touch!(newer_shared_log)

    assert {:ok, {marker_offset, marker_value_size}} =
             NIF.v2_append_record(newer_shared_log, marker, "hash", 0)

    :ets.insert(
      ctx.keydir,
      {marker, "hash", 0, LFU.initial(), 1, marker_offset, marker_value_size}
    )

    put_shared_hash_field(ctx, redis_key, "field", "shared")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    File.chmod!(ctx.shard_path, 0o300)

    try do
      assert {:error, _reason} = Ferricstore.FS.ls(ctx.shard_path)

      assert_raise RuntimeError, ~r/promotion recovery list_shared_logs failed/, fn ->
        Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
      end

      assert File.dir?(dedicated_path)

      assert [{^marker, "hash", 0, _lfu, 1, ^marker_offset, ^marker_value_size}] =
               :ets.lookup(ctx.keydir, marker)
    after
      File.chmod!(ctx.shard_path, 0o700)
    end

    assert {:ok, older_records} = NIF.v2_scan_file(ctx.shared_log)

    refute Enum.any?(older_records, fn
             {^marker, _offset, _value_size, _expire_at_ms, true} -> true
             _record -> false
           end)
  end

  test "normal cleanup intent lets recovery finish after marker tombstone failure", ctx do
    redis_key = "normal-cleanup-intent"
    marker = Promotion.marker_key(redis_key)
    put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, CompoundKey.type_key(redis_key), "hash", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(
               dedicated_log,
               CompoundKey.hash_field(redis_key, "field"),
               "value",
               0
             )

    Process.put(:ferricstore_promotion_fsync_dir_hook, fn path ->
      File.chmod!(ctx.shared_log, 0o400)
      NIF.v2_fsync_dir(path)
    end)

    try do
      assert_raise RuntimeError, ~r/promotion cleanup marker tombstone failed/, fn ->
        Promotion.cleanup_promoted!(
          redis_key,
          :hash,
          dedicated_path,
          ctx.shard_path,
          ctx.keydir,
          ctx.data_dir,
          0
        )
      end
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
      File.chmod!(ctx.shared_log, 0o600)
    end

    refute File.dir?(dedicated_path)

    :ets.delete_all_objects(ctx.keydir)
    assert :ok = ShardLifecycle.recover_keydir(ctx.shard_path, ctx.keydir, 0)

    assert [{^marker, nil, 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)

    assert %{} = Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    assert [] = :ets.lookup(ctx.keydir, marker)
    refute File.dir?(dedicated_path)
  end

  test "dedicated scan errors abort recovery instead of accepting an older state", ctx do
    redis_key = "scan-error"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    File.chmod!(dedicated_log, 0o000)

    on_exit(fn ->
      File.chmod(dedicated_log, 0o600)
    end)

    assert {:error, _reason} = NIF.v2_scan_file_page(dedicated_log, 0, 2)

    assert_raise RuntimeError, ~r/promotion recovery scan_dedicated_log failed/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  test "a complete CRC-corrupt record aborts recovery instead of accepting its prefix", ctx do
    redis_key = "crc-corrupt"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")

    assert {:ok, {_type_offset, _type_size}} =
             NIF.v2_append_record(dedicated_log, type_key, "hash", 0)

    assert {:ok, {field_offset, _field_size}} =
             NIF.v2_append_record(dedicated_log, field_key, "value", 0)

    flip_file_byte!(dedicated_log, field_offset + 26 + byte_size(field_key))

    assert_raise RuntimeError, ~r/promotion recovery scan_dedicated_log failed/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert [] = :ets.lookup(ctx.keydir, type_key)
    assert [] = :ets.lookup(ctx.keydir, field_key)

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  test "dedicated members without live type metadata abort before publication", ctx do
    redis_key = "missing-dedicated-type"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    field_key = CompoundKey.hash_field(redis_key, "field")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, field_key, "value", 0)

    assert_raise RuntimeError, ~r/dedicated_type_missing/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert [] = :ets.lookup(ctx.keydir, field_key)

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  test "a tombstoned dedicated type aborts before member publication", ctx do
    redis_key = "tombstoned-dedicated-type"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    field_key = CompoundKey.hash_field(redis_key, "field")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, type_key, "hash", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, field_key, "value", 0)

    assert {:ok, _offset} = NIF.v2_append_tombstone(dedicated_log, type_key)

    assert_raise RuntimeError, ~r/dedicated_type_missing/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert [] = :ets.lookup(ctx.keydir, field_key)
    assert [] = :ets.lookup(ctx.keydir, type_key)

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  test "a torn active tail is repaired before later appends", ctx do
    redis_key = "torn-tail"
    put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    stable_key = CompoundKey.hash_field(redis_key, "stable")
    torn_key = CompoundKey.hash_field(redis_key, "torn")
    later_key = CompoundKey.hash_field(redis_key, "later")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, type_key, "hash", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, stable_key, "stable", 0)

    valid_end = File.stat!(dedicated_log).size

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, torn_key, "incomplete", 0)

    bytes = File.read!(dedicated_log)
    File.write!(dedicated_log, binary_part(bytes, 0, byte_size(bytes) - 2))

    assert %{^redis_key => %{path: ^dedicated_path}} =
             Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)

    assert File.stat!(dedicated_log).size == valid_end
    assert [] = :ets.lookup(ctx.keydir, torn_key)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, later_key, "durable", 0)

    assert %{^redis_key => %{path: ^dedicated_path}} =
             Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)

    assert [{^later_key, "durable", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, later_key)
  end

  test "recovery never truncates a log path swapped to a symlink", ctx do
    redis_key = "torn-tail-symlink-swap"
    put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    stable_key = CompoundKey.hash_field(redis_key, "stable")
    torn_key = CompoundKey.hash_field(redis_key, "torn")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, type_key, "hash", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, stable_key, "stable", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, torn_key, "incomplete", 0)

    bytes = File.read!(dedicated_log)
    torn_bytes = binary_part(bytes, 0, byte_size(bytes) - 2)
    File.write!(dedicated_log, torn_bytes)

    outside_log = Path.join(Path.dirname(ctx.data_dir), "outside.log")
    File.write!(outside_log, torn_bytes)

    Process.put(:ferricstore_promotion_recovery_state_hook, fn _retained_rows ->
      unless Process.get(:promotion_recovery_path_swapped, false) do
        Process.put(:promotion_recovery_path_swapped, true)
        File.rm!(dedicated_log)
        File.ln_s!(outside_log, dedicated_log)
      end
    end)

    try do
      assert_raise RuntimeError, fn ->
        Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
      end
    after
      Process.delete(:ferricstore_promotion_recovery_state_hook)
      Process.delete(:promotion_recovery_path_swapped)
    end

    assert File.read!(outside_log) == torn_bytes
  end

  test "a torn tail in a sealed log aborts without truncating it", ctx do
    redis_key = "sealed-torn-tail"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    sealed_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    torn_key = CompoundKey.hash_field(redis_key, "torn")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(sealed_log, type_key, "hash", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(sealed_log, torn_key, "incomplete", 0)

    bytes = File.read!(sealed_log)
    torn_bytes = binary_part(bytes, 0, byte_size(bytes) - 2)
    File.write!(sealed_log, torn_bytes)
    File.touch!(Path.join(dedicated_path, "00001.log"))

    assert_raise RuntimeError, ~r/torn_tail_in_sealed_log/, fn ->
      Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
    end

    assert File.read!(sealed_log) == torn_bytes

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  test "dedicated record read errors abort recovery before publishing partial state", ctx do
    redis_key = "record-read-error"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    type_key = CompoundKey.type_key(redis_key)
    first_key = CompoundKey.hash_field(redis_key, "first")
    second_key = CompoundKey.hash_field(redis_key, "second")

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, type_key, "hash", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, first_key, "first-value", 0)

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, second_key, "second-value", 0)

    Process.put(:promotion_recovery_read_count, 0)
    tables_before = owned_ets_tables()

    Process.put(:ferricstore_promotion_recovery_read_hook, fn path, offset, key ->
      observe_recovery_plan_table!(tables_before)

      case Process.get(:promotion_recovery_read_count, 0) do
        0 ->
          Process.put(:promotion_recovery_read_count, 1)
          Ferricstore.Store.ColdRead.pread_keyed(path, offset, key, 10_000)

        _ ->
          {:error, :eio}
      end
    end)

    try do
      assert_raise RuntimeError, ~r/promotion recovery read_dedicated_record failed.*eio/, fn ->
        Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
      end

      assert [] = :ets.lookup(ctx.keydir, first_key)
      assert [] = :ets.lookup(ctx.keydir, second_key)

      assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
               :ets.lookup(ctx.keydir, marker)
    after
      Process.delete(:ferricstore_promotion_recovery_read_hook)
      Process.delete(:promotion_recovery_read_count)
    end

    assert_recovery_plan_table_cleaned!()
  end

  test "foreign records in a dedicated directory abort without touching shard rows", ctx do
    redis_key = "owner"
    foreign_redis_key = "foreign"
    {marker, _offset, _value_size} = put_marker(ctx, redis_key, "hash")
    {:ok, dedicated_path} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, redis_key)
    dedicated_log = Promotion.find_active(dedicated_path)
    foreign_key = CompoundKey.hash_field(foreign_redis_key, "field")
    foreign_shared_value = "shared-value"

    :ets.insert(
      ctx.keydir,
      {foreign_key, foreign_shared_value, 0, LFU.initial(), 7, 11,
       byte_size(foreign_shared_value)}
    )

    assert {:ok, {_offset, _value_size}} =
             NIF.v2_append_record(dedicated_log, foreign_key, "dedicated-value", 0)

    assert_raise RuntimeError,
                 ~r/promotion recovery scan_dedicated_log failed.*foreign_record/,
                 fn ->
                   Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0)
                 end

    assert [
             {^foreign_key, ^foreign_shared_value, 0, _lfu, 7, 11, _foreign_shared_value_size}
           ] = :ets.lookup(ctx.keydir, foreign_key)

    assert [{^marker, "hash", 0, _lfu, 0, _offset, _value_size}] =
             :ets.lookup(ctx.keydir, marker)
  end

  defp put_marker(ctx, redis_key, type, storage \\ :hot) do
    marker = Promotion.marker_key(redis_key)
    {:ok, {offset, value_size}} = NIF.v2_append_record(ctx.shared_log, marker, type, 0)
    value = if storage == :cold, do: nil, else: type
    :ets.insert(ctx.keydir, {marker, value, 0, LFU.initial(), 0, offset, value_size})
    {marker, offset, value_size}
  end

  defp put_shared_record(ctx, key, value, expire_at_ms) do
    {:ok, {offset, value_size}} =
      NIF.v2_append_record(ctx.shared_log, key, value, expire_at_ms)

    :ets.insert(ctx.keydir, {key, value, expire_at_ms, LFU.initial(), 0, offset, value_size})
    {key, offset, value_size}
  end

  defp put_shared_hash_field(ctx, redis_key, field, value) do
    type_key = CompoundKey.type_key(redis_key)

    if :ets.lookup(ctx.keydir, type_key) == [] do
      {:ok, {type_offset, type_value_size}} =
        NIF.v2_append_record(ctx.shared_log, type_key, "hash", 0)

      :ets.insert(
        ctx.keydir,
        {type_key, "hash", 0, LFU.initial(), 0, type_offset, type_value_size}
      )
    end

    key = CompoundKey.hash_field(redis_key, field)
    {:ok, {offset, value_size}} = NIF.v2_append_record(ctx.shared_log, key, value, 0)
    :ets.insert(ctx.keydir, {key, value, 0, LFU.initial(), 0, offset, value_size})
    key
  end

  defp restore_env(key, {:ok, value}), do: Application.put_env(:ferricstore, key, value)
  defp restore_env(key, :error), do: Application.delete_env(:ferricstore, key)

  defp owned_ets_tables do
    owner = self()

    :ets.all()
    |> Enum.filter(fn table -> :ets.info(table, :owner) == owner end)
    |> MapSet.new()
  end

  defp observe_recovery_plan_table!(tables_before) do
    unless Process.get(:observed_recovery_plan_table) do
      [table] =
        owned_ets_tables()
        |> MapSet.difference(tables_before)
        |> Enum.filter(fn table -> :ets.info(table, :name) == :promotion_recovery_plan end)

      Process.put(
        :observed_recovery_plan_table,
        {table, :ets.info(table, :protection), :ets.info(table, :type)}
      )
    end
  end

  defp assert_recovery_plan_table_cleaned! do
    assert {table, :private, :ordered_set} = Process.delete(:observed_recovery_plan_table)
    assert :undefined == :ets.info(table)
  end

  defp flip_file_byte!(path, offset) do
    {:ok, file} = File.open(path, [:read, :write, :raw, :binary])

    try do
      {:ok, <<byte>>} = :file.pread(file, offset, 1)
      :ok = :file.pwrite(file, offset, <<Bitwise.bxor(byte, 0xFF)>>)
    after
      File.close(file)
    end
  end

  defp nif_calls(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., _, [{:__aliases__, _, [:NIF]}, function]}, _, args} = node, acc
        when is_atom(function) and is_list(args) ->
          {node, [{:NIF, function, length(args)} | acc]}

        node, acc ->
          {node, acc}
      end)

    calls
  end
end
