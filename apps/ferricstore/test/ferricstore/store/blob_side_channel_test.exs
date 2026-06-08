Code.require_file(
  "blob_side_channel_test/sections/blob_garbage_sweep_streams_keydir_refs_copying_full_ets_table.exs",
  __DIR__
)

Code.require_file(
  "blob_side_channel_test/sections/ra_generic_batch_accepts_pre_externalized_blob_refs.exs",
  __DIR__
)

Code.require_file(
  "blob_side_channel_test/sections/blob_garbage_sweep_ignores_expired_blob_refs_still_present_in_keydir.exs",
  __DIR__
)

defmodule Ferricstore.Store.BlobSideChannelTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.{
    BlobRef,
    BlobStore,
    ColdRead,
    CompoundKey,
    LFU,
    LocalTxStore,
    Ops,
    Router
  }

  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Test.IsolatedInstance

  @router_source_path Path.expand("../../../lib/ferricstore/store/router/blob_gc.ex", __DIR__)

  setup do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    original_threshold = Application.get_env(:ferricstore, :promotion_threshold)

    original_persistent_threshold =
      try do
        :persistent_term.get(:ferricstore_promotion_threshold)
      rescue
        ArgumentError -> :not_set
      end

    Application.put_env(:ferricstore, :promotion_threshold, 1)
    :persistent_term.put(:ferricstore_promotion_threshold, 1)

    on_exit(fn ->
      case original_threshold do
        nil -> Application.delete_env(:ferricstore, :promotion_threshold)
        value -> Application.put_env(:ferricstore, :promotion_threshold, value)
      end

      case original_persistent_threshold do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        value -> :persistent_term.put(:ferricstore_promotion_threshold, value)
      end

      IsolatedInstance.checkin(ctx)
    end)

    %{ctx: ctx, shard: elem(ctx.shard_names, 0), keydir: elem(ctx.keydir_refs, 0)}
  end

  use Ferricstore.Store.BlobSideChannelTest.Sections.BlobGarbageSweepStreamsKeydirRefsCopyingFullEtsTable
  use Ferricstore.Store.BlobSideChannelTest.Sections.RaGenericBatchAcceptsPreExternalizedBlobRefs

  use Ferricstore.Store.BlobSideChannelTest.Sections.BlobGarbageSweepIgnoresExpiredBlobRefsStillPresentInKeydir

  defp force_rotate_active_file(shard) do
    :sys.replace_state(shard, fn state ->
      new_id = state.active_file_id + 1
      shard_path = state.shard_data_path
      new_path = Ferricstore.Store.Shard.ETS.file_path(shard_path, new_id)

      Ferricstore.FS.touch!(new_path)

      Ferricstore.Store.ActiveFile.publish(
        state.instance_ctx,
        state.index,
        new_id,
        new_path,
        shard_path
      )

      %{
        state
        | active_file_id: new_id,
          active_file_path: new_path,
          active_file_size: 0,
          file_stats: Map.put(state.file_stats, new_id, {0, 0})
      }
    end)
  end

  defp raw_disk_blob_ref(ctx, keydir, key) do
    with {:ok, value} <- raw_disk_value(ctx, keydir, key),
         {:ok, ref} <- BlobRef.decode(value) do
      {:ok, value, ref}
    else
      other -> other
    end
  end

  defp raw_disk_value(ctx, keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, nil, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) and fid >= 0 ->
        path = ShardETS.file_path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), fid)
        ColdRead.pread_at(path, off, key, 5_000)

      [{^key, value, _exp, _lfu, fid, off, _vsize}] when is_integer(fid) and fid >= 0 ->
        path = ShardETS.file_path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), fid)

        case ColdRead.pread_at(path, off, key, 5_000) do
          {:ok, disk_value} -> {:ok, disk_value}
          _ -> {:ok, value}
        end

      other ->
        {:error, {:unexpected_keydir_entry, other}}
    end
  end

  defp promoted_disk_blob_ref(shard, keydir, redis_key, compound_key) do
    state = :sys.get_state(shard)
    dedicated_path = state.promoted_instances[redis_key].path

    with [{^compound_key, _value, _exp, _lfu, fid, off, _vsize}] <-
           :ets.lookup(keydir, compound_key),
         path <- ShardCompound.dedicated_file_path(dedicated_path, fid),
         {:ok, value} <- ColdRead.pread_at(path, off, compound_key, 5_000),
         {:ok, ref} <- BlobRef.decode(value) do
      {:ok, value, ref}
    else
      other -> {:error, {:unexpected_promoted_blob_ref, other}}
    end
  end

  defp assert_state_machine_result(expected, result)
       when is_tuple(result) and tuple_size(result) >= 2 do
    case elem(result, 1) do
      ^expected -> :ok
      {:ok, ^expected} -> :ok
      {:applied_at, _index, ^expected} -> :ok
      {:applied_at, _index, {:ok, ^expected}} -> :ok
      other -> flunk("unexpected state machine result #{inspect(other)}")
    end
  end

  defp attach_blob_gc_handler do
    handler_id = {__MODULE__, self(), make_ref()}
    parent = self()

    :telemetry.attach_many(
      handler_id,
      [[:ferricstore, :blob, :gc], [:ferricstore, :blob, :gc, :failed]],
      fn
        [:ferricstore, :blob, :gc] = event, measurements, metadata, _config ->
          send(parent, {:blob_gc, event, measurements, metadata})

        [:ferricstore, :blob, :gc, :failed] = event, measurements, metadata, _config ->
          send(parent, {:blob_gc_failed, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp write_legacy_blob!(data_dir, shard_index, %BlobRef{} = ref, payload) do
    path = BlobRef.path(data_dir, shard_index, ref)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, payload)
    path
  end

  defp overwrite_segment_payload!(data_dir, shard_index, ref, payload) do
    assert {:ok, {path, offset, size}} = BlobStore.file_ref(data_dir, shard_index, ref)
    assert byte_size(payload) == size

    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.pwrite(io, offset, payload)
    after
      :file.close(io)
    end
  end

  defp corrupt_segment_header!(data_dir, shard_index, ref) do
    assert {:ok, {path, offset, _size}} = BlobStore.file_ref(data_dir, shard_index, ref)
    header_offset = offset - 48
    assert header_offset >= 0

    {:ok, io} = File.open(path, [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.pwrite(io, header_offset, :binary.copy(<<0>>, 48))
    after
      :file.close(io)
    end
  end
end
