defmodule Ferricstore.Commands.ServerBlobGCTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Dispatcher, Server}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx =
      IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 64,
        blob_side_channel_threshold_bytes: 128
      )

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{ctx: ctx, store: %{instance_ctx: ctx}}
  end

  test "FERRICSTORE.BLOBGC preserves append segment records until segment compaction", %{
    ctx: ctx,
    store: store
  } do
    key = "blobgc:dead"
    payload = :binary.copy("G", 1024)

    assert :ok = Router.put(ctx, key, payload, 0)
    assert blob_segment_file_count(ctx) == 1
    assert :ok = Router.delete(ctx, key)

    assert [
             "deleted_files",
             0,
             "deleted_bytes",
             deleted_bytes,
             "kept_files",
             0,
             "deleted_tmp_files",
             0,
             "deleted_tmp_bytes",
             0
           ] = Server.handle("FERRICSTORE.BLOBGC", [], store)

    assert deleted_bytes == 0
    assert blob_segment_file_count(ctx) == 1
  end

  test "FERRICSTORE.BLOBGC rejects arguments", %{store: store} do
    assert {:error, "ERR wrong number of arguments for 'ferricstore.blobgc' command"} =
             Server.handle("FERRICSTORE.BLOBGC", ["NOW"], store)
  end

  test "dispatcher routes RESP parser AST for FERRICSTORE.BLOBGC", %{store: store} do
    assert [
             "deleted_files",
             0,
             "deleted_bytes",
             0,
             "kept_files",
             0,
             "deleted_tmp_files",
             0,
             "deleted_tmp_bytes",
             0
           ] = Dispatcher.dispatch("FERRICSTORE.BLOBGC", [], store)
  end

  defp blob_segment_file_count(ctx) do
    Path.join([ctx.data_dir, "blob", "shard_*", "segments", "*.bloblog"])
    |> Path.wildcard()
    |> length()
  end
end
