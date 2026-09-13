defmodule Ferricstore.Store.PromotionRaftRecoveryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundKey, Promotion}

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "promotion-raft-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    shard_path = Path.join(root, "shard_0")
    File.mkdir_p!(shard_path)
    File.touch!(Path.join(shard_path, "00000.log"))
    keydir = :ets.new(:promotion_raft_recovery, [:set, :public])
    on_exit(fn -> File.rm_rf!(root) end)

    %{
      data_dir: root,
      shard_path: shard_path,
      keydir: keydir,
      instance: %{data_dir: root, hot_cache_max_value_size: 0}
    }
  end

  test "cold projection marker recovers a complete dedicated collection twice", ctx do
    key = "cold-marker"
    marker = Promotion.marker_key(key)
    type_key = CompoundKey.type_key(key)
    field = CompoundKey.hash_field(key, "field")
    project(ctx, [{marker, Promotion.encode_marker(:hash, :promoted, 1)}])
    {:ok, dedicated} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, key)

    assert {:ok, _} =
             NIF.v2_append_batch(Promotion.find_active(dedicated), [
               {type_key, "hash", 0},
               {field, "value", 0}
             ])

    for _ <- 1..2 do
      assert %{^key => %{path: ^dedicated}} = recover(ctx)
      assert [{^field, nil, 0, _, _, _, _}] = :ets.lookup(ctx.keydir, field)
    end
  end

  test "cold projection shared type validates an interrupted promotion fallback", ctx do
    key = "cold-shared-type"
    marker = Promotion.marker_key(key)
    type_key = CompoundKey.type_key(key)
    field = CompoundKey.hash_field(key, "field")
    project(ctx, [{type_key, "hash"}, {field, "shared-value"}])
    :ets.insert(ctx.keydir, {marker, Promotion.encode_marker(:hash, :promoted, 1), 0, 0, 0, 0, 0})

    assert recover(ctx) == %{}
    assert :ets.lookup(ctx.keydir, marker) == []
    assert [{^field, nil, 0, _, {:waraft_projection, 2}, _, _}] = :ets.lookup(ctx.keydir, field)

    assert {:ok, "shared-value"} =
             Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
               ctx.instance,
               0,
               {:waraft_projection, 2},
               field
             )

    assert recover(ctx) == %{}
  end

  test "cold WARaft segment locators validate an interrupted promotion fallback", ctx do
    key = "cold-segment-shared-type"
    marker = Promotion.marker_key(key)
    type_key = CompoundKey.type_key(key)
    field = CompoundKey.hash_field(key, "field")

    segment_entries = [
      {marker, Promotion.encode_marker(:hash, :promoted, 1)},
      {type_key, "hash"},
      {field, "segment-value"}
    ]

    marker_value = Promotion.encode_marker(:hash, :promoted, 1)

    {index, offset, size} = persist_segment_batch(ctx, segment_entries)

    Enum.each(segment_entries, fn {entry_key, _value} ->
      :ets.insert(ctx.keydir, {
        entry_key,
        nil,
        0,
        0,
        {:waraft_segment, index},
        offset,
        size
      })
    end)

    assert {:ok, ^marker_value} =
             Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
               ctx.instance,
               0,
               {:waraft_segment, index},
               marker
             )

    assert recover(ctx) == %{}
    assert :ets.lookup(ctx.keydir, marker) == []

    assert [{^field, nil, 0, _, {:waraft_segment, ^index}, ^offset, ^size}] =
             :ets.lookup(ctx.keydir, field)
  end

  test "cold apply-projection locators validate an interrupted promotion fallback", ctx do
    key = "cold-apply-projection-shared-type"
    marker = Promotion.marker_key(key)
    type_key = CompoundKey.type_key(key)
    field = CompoundKey.hash_field(key, "field")

    apply_entries = [
      {marker, Promotion.encode_marker(:hash, :promoted, 1)},
      {type_key, "hash"},
      {field, "apply-value"}
    ]

    marker_value = Promotion.encode_marker(:hash, :promoted, 1)

    {_index, offset, size} = persist_apply_projection_batch(ctx, apply_entries)

    Enum.each(apply_entries, fn {entry_key, _value} ->
      :ets.insert(ctx.keydir, {
        entry_key,
        nil,
        0,
        0,
        {:waraft_apply_projection, 1},
        offset,
        size
      })
    end)

    assert {:ok, ^marker_value} =
             Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
               ctx.instance,
               0,
               {:waraft_apply_projection, 1},
               marker
             )

    assert recover(ctx) == %{}
    assert :ets.lookup(ctx.keydir, marker) == []

    assert [{^field, nil, 0, _, {:waraft_apply_projection, 1}, ^offset, ^size}] =
             :ets.lookup(ctx.keydir, field)
  end

  test "cleanup intent preserves a new cold shared collection type", ctx do
    key = "changed-type"
    marker = Promotion.marker_key(key)
    type_key = CompoundKey.type_key(key)
    project(ctx, [{type_key, "set"}])
    :ets.insert(ctx.keydir, {marker, Promotion.encode_marker(:hash, :cleanup, 1), 0, 0, 0, 0, 0})
    assert {:ok, _} = Promotion.open_dedicated(ctx.data_dir, 0, :hash, key)

    assert recover(ctx) == %{}
    assert :ets.lookup(ctx.keydir, marker) == []
    assert [_] = :ets.lookup(ctx.keydir, type_key)
  end

  test "missing projected marker fails closed without mutating the keydir", ctx do
    marker = Promotion.marker_key("missing")
    row = {marker, nil, 0, 0, {:waraft_projection, 1}, 0, 100}
    :ets.insert(ctx.keydir, row)

    assert_raise RuntimeError, ~r/promotion recovery read_marker failed/, fn -> recover(ctx) end
    assert :ets.lookup(ctx.keydir, marker) == [row]
  end

  test "a projected locator for another key fails closed", ctx do
    project(ctx, [{"unrelated", "hash"}])
    marker = Promotion.marker_key("wrong-key")
    [{_, _, _, _, location, offset, size}] = :ets.lookup(ctx.keydir, "unrelated")
    row = {marker, nil, 0, 0, location, offset, size}
    :ets.insert(ctx.keydir, row)

    assert_raise RuntimeError, ~r/promotion recovery read_marker failed.*record_not_found/, fn ->
      recover(ctx)
    end

    assert :ets.lookup(ctx.keydir, marker) == [row]
  end

  defp recover(ctx),
    do: Promotion.recover_promoted(ctx.shard_path, ctx.keydir, ctx.data_dir, 0, ctx.instance)

  defp project(ctx, entries) do
    root =
      Path.join([
        ctx.data_dir,
        "waraft",
        "ferricstore_waraft_backend.1",
        "segment_projection_log"
      ])

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection(
               to_charlist(root),
               {:raft_log_pos, 42, 7},
               Enum.map(entries, fn {key, value} -> {key, value, 0} end)
             )

    for {{key, value}, index} <- Enum.with_index(entries, 1) do
      assert Ferricstore.Store.Shard.ETS.value_for_ets(value, 0) == nil

      assert {:ok, {_, offset, size}} =
               :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), index)

      :ets.insert(ctx.keydir, {key, nil, 0, 0, {:waraft_projection, index}, offset, size})
    end
  end

  defp persist_segment_batch(ctx, entries) do
    table = :promotion_recovery_segment_test
    partition = System.unique_integer([:positive])
    index = 1_000_000_000 + partition
    log_name = :wa_raft_log.default_name(table, partition)
    previous_database = Application.get_env(:wa_raft, :raft_database)
    previous_options = :wa_raft_part_sup.options(table, partition)
    raft_root = Path.join(ctx.data_dir, "waraft")

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(raft_root))

      :wa_raft_part_sup.prepare_spec(:promotion_recovery_segment_test, %{
        table: table,
        partition: partition,
        log_module: :ferricstore_waraft_spike_segment_log
      })

      log =
        {:raft_log, log_name, :promotion_recovery_segment_test, table, partition,
         :ferricstore_waraft_spike_segment_log}

      assert :ok = :ferricstore_waraft_spike_segment_log.init(log)
      assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.open(log)

      command_entries = Enum.map(entries, fn {key, value} -> {key, value, 0} end)

      assert :ok =
               :ferricstore_waraft_spike_segment_log.append(
                 {:log_view, log, 0, index - 1, :undefined},
                 [{1, {make_ref(), {:put_batch, command_entries}}}],
                 :strict,
                 :low
               )

      source_root = Path.join(raft_root, "#{table}.#{partition}")
      assert :ok = :ferricstore_waraft_spike_segment_log.close(log, %{})
      File.cp_r!(source_root, Path.join(raft_root, "ferricstore_waraft_backend.1"))

      assert {:ok, {_ordinal, offset, size}} =
               :ferricstore_waraft_spike_segment_log.location_for_index(
                 to_charlist(source_root),
                 index
               )

      {index, offset, size}
    after
      :ferricstore_waraft_spike_segment_log.close(
        {:raft_log, log_name, :promotion_recovery_segment_test, table, partition,
         :ferricstore_waraft_spike_segment_log},
        %{}
      )

      if :ets.info(log_name) != :undefined, do: :ets.delete(log_name)

      restore_env(:wa_raft, :raft_database, previous_database)
      restore_partition_options(table, partition, previous_options)
    end
  end

  defp persist_apply_projection_batch(ctx, entries) do
    index = 1

    root =
      Path.join([
        ctx.data_dir,
        "waraft",
        "ferricstore_waraft_backend.1",
        "apply_projection_log"
      ])

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
               to_charlist(root),
               [
                 {{:raft_log_pos, index, 0},
                  Enum.map(entries, fn {key, value} -> {key, value, 0} end)}
               ]
             )

    assert {:ok, {ordinal, offset, size}} =
             :ferricstore_waraft_spike_segment_log.location_for_index(
               to_charlist(root),
               index
             )

    {ordinal, offset, size}
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)

  defp restore_partition_options(table, partition, nil),
    do: :persistent_term.erase({:wa_raft_part_sup, table, partition})

  defp restore_partition_options(table, partition, options),
    do: :persistent_term.put({:wa_raft_part_sup, table, partition}, options)
end
