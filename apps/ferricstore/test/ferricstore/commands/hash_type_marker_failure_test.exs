defmodule Ferricstore.Commands.HashTypeMarkerFailureTest do
  @moduledoc """
  An HSET the instance cannot run as one replicated entry (a blob-sized value)
  claims a new hash's type marker and then writes its fields, as separate
  replicated writes. When the field write fails, deleting the marker it
  claimed is no undo when the write may have committed (a timeout's outcome is
  unknown) or another client has written fields under the marker since.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  @hook :ferricstore_hash_field_put_hook

  setup do
    on_exit(fn -> Process.delete(@hook) end)
    ctx = FerricStore.Instance.get(:default)

    %{
      ctx: ctx,
      key: "{hash-type-marker:#{System.unique_integer([:positive])}}",
      blob: :binary.copy("b", ctx.blob_side_channel_threshold_bytes + 1)
    }
  end

  test "HSET whose field write commits but times out keeps the hash's marker",
       %{ctx: ctx, key: key, blob: blob} do
    Process.put(@hook, fn put ->
      assert :ok = put.()
      {:error, {:timeout, :unknown_outcome}}
    end)

    assert {:error, {:timeout, :unknown_outcome}} =
             Dispatcher.dispatch("HSET", [key, "f", blob], ctx)

    Process.delete(@hook)

    assert blob == Dispatcher.dispatch("HGET", [key, "f"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
    assert {:simple, "hash"} == Dispatcher.dispatch("TYPE", [key], ctx)
  end

  test "a failed HSET keeps the marker another client's fields were written under",
       %{ctx: ctx, key: key, blob: blob} do
    Process.put(@hook, fn _put ->
      assert 1 =
               Task.async(fn -> Dispatcher.dispatch("HSET", [key, "g", "w"], ctx) end)
               |> Task.await()

      {:error, "ERR storage write failed"}
    end)

    assert {:error, "ERR storage write failed"} =
             Dispatcher.dispatch("HSET", [key, "f", blob], ctx)

    Process.delete(@hook)

    assert "w" == Dispatcher.dispatch("HGET", [key, "g"], ctx)
    assert 1 == Dispatcher.dispatch("EXISTS", [key], ctx)
    assert {:simple, "hash"} == Dispatcher.dispatch("TYPE", [key], ctx)
  end
end
