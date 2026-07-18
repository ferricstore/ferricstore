defmodule Ferricstore.Raft.WARaftStorageExpiryContextTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftStorage
  alias Ferricstore.Raft.ApplyContext
  alias Ferricstore.Store.LFU

  setup do
    keydir = :ets.new(:waraft_storage_expiry_context, [:set, :public])
    hlc_ref = :persistent_term.get(:ferricstore_hlc_ref)
    previous_hlc = :atomics.get(hlc_ref, 1)

    on_exit(fn ->
      :atomics.put(hlc_ref, 1, previous_hlc)
    end)

    %{hlc_ref: hlc_ref, keydir: keydir}
  end

  @tag :hlc_drift_guard
  test "segment projection snapshots retain wall-live rows during unsafe HLC drift", %{
    hlc_ref: hlc_ref,
    keydir: keydir
  } do
    key = "waraft-snapshot-wall-live"
    value = "value"
    wall_ms = System.os_time(:millisecond)
    expire_at_ms = wall_ms + 30_000

    :ets.insert(
      keydir,
      {key, value, expire_at_ms, LFU.initial(), 0, 0, byte_size(value)}
    )

    :atomics.put(hlc_ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

    assert {:ok, [{^key, ^value, ^expire_at_ms}]} =
             WARaftStorage.__collect_segment_projected_entries_for_test__(%{
               ets: keydir,
               instance_ctx: %{},
               shard_index: 0
             })
  end

  @tag :hlc_drift_guard
  test "checkpoint trim relocates wall-live rows during unsafe HLC drift", %{
    hlc_ref: hlc_ref,
    keydir: keydir
  } do
    key = "waraft-checkpoint-wall-live"
    value = "value"
    wall_ms = System.os_time(:millisecond)
    expire_at_ms = wall_ms + 30_000
    row = {key, value, expire_at_ms, LFU.initial(), {:waraft_segment, 1}, 0, byte_size(value)}
    :ets.insert(keydir, row)
    :atomics.put(hlc_ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

    assert {:ok, [{1, {{^key, ^value, ^expire_at_ms}, ^row}}]} =
             WARaftStorage.__segment_projection_checkpoint_relocations_for_test__(
               %{keydir_refs: {keydir}},
               0,
               [{key, value, expire_at_ms}],
               2
             )
  end

  @tag :hlc_drift_guard
  test "snapshot restore applies wall-live rows during unsafe HLC drift", %{
    hlc_ref: hlc_ref,
    keydir: keydir
  } do
    key = "waraft-restore-wall-live"
    value = "value"
    wall_ms = System.os_time(:millisecond)
    expire_at_ms = wall_ms + 30_000
    :atomics.put(hlc_ref, 1, Bitwise.bsl(wall_ms + 60_000, 16))

    state =
      WARaftStorage.__apply_segment_projection_entries_for_test__(
        %{
          ets: keydir,
          shard_index: 0,
          instance_ctx: nil,
          apply_context: ApplyContext.new([])
        },
        :snapshot_restore,
        [{key, value, expire_at_ms}]
      )

    assert state.ets == keydir

    assert [{^key, ^value, ^expire_at_ms, _lfu, {:waraft_segment, 0}, 0, 5}] =
             :ets.lookup(keydir, key)
  end
end
