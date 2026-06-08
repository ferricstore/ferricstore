Code.require_file(
  "router_cold_empty_test/sections/get_file_ref_treats_cold_empty_values_as_valid_file_refs.exs",
  __DIR__
)

Code.require_file(
  "router_cold_empty_test/sections/get_meta_waits_through_delayed_compaction_ets_update.exs",
  __DIR__
)

Code.require_file(
  "router_cold_empty_test/sections/exists_rejects_cold_rows_invalid_offsets.exs",
  __DIR__
)

defmodule Ferricstore.Store.RouterColdEmptyTest do
  @moduledoc false
  use ExUnit.Case, async: false

  @record_header_size 26

  alias Ferricstore.Store.{CompoundKey, LFU}
  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Stats
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)
    shard = Process.whereis(elem(ctx.shard_names, 0))
    keydir = elem(ctx.keydir_refs, 0)

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{ctx: ctx, shard: shard, keydir: keydir}
  end

  use Ferricstore.Store.RouterColdEmptyTest.Sections.GetFileRefTreatsColdEmptyValuesAsValidFileRefs

  use Ferricstore.Store.RouterColdEmptyTest.Sections.GetMetaWaitsThroughDelayedCompactionEtsUpdate

  use Ferricstore.Store.RouterColdEmptyTest.Sections.ExistsRejectsColdRowsInvalidOffsets

  defp attach_pread_corrupt_handler(callback \\ fn -> :ok end) do
    parent = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :bitcask, :pread_corrupt],
        fn event, measurements, metadata, _config ->
          callback.()
          send(parent, {:pread_corrupt, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp attach_cold_retry_exhausted_handler do
    parent = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :store, :cold_read_retry_exhausted],
        fn event, measurements, metadata, _config ->
          send(parent, {:cold_retry_exhausted, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp collect_apply_projection_disk_reads(acc) do
    receive do
      {:apply_projection_disk_read, source} ->
        collect_apply_projection_disk_reads([source | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp insert_replaced_cold_compound_row(ctx, keydir, compound_key, value, expire_at_ms) do
    value_size = byte_size(value)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, {_dead_offset, _dead_record_size}} =
      NIF.v2_append_record(path, compound_key <> ":dead", "old", 0)

    {:ok, {old_offset, _old_record_size}} =
      NIF.v2_append_record(path, compound_key, value, expire_at_ms)

    File.rm!(path)

    {:ok, {new_offset, _new_record_size}} =
      NIF.v2_append_record(path, compound_key, value, expire_at_ms)

    :ets.insert(
      keydir,
      {compound_key, nil, expire_at_ms, LFU.initial(), 0, old_offset, value_size}
    )

    {old_offset, new_offset, value_size}
  end

  defp with_unregistered_shard(ctx, shard, fun) when is_function(fun, 0) do
    shard_name = elem(ctx.shard_names, 0)

    if Process.whereis(shard_name) != nil do
      Process.unregister(shard_name)
    end

    try do
      fun.()
    after
      if Process.alive?(shard) and Process.whereis(shard_name) == nil do
        Process.register(shard, shard_name)
      end
    end
  end
end
