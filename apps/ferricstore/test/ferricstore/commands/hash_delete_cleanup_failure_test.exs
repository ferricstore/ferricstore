defmodule Ferricstore.Commands.HashDeleteCleanupFailureTest do
  @moduledoc """
  Below cluster feature level 4, HDEL and HGETDEL on the instance run as
  separate replicated writes: the fields' delete, then the hash's type marker
  delete when the hash is left empty. When the second write fails, the first
  has already committed, the failed write may have committed too (a timeout's
  outcome is unknown), and other clients may have written the fields since.
  Writing the deleted values back is no undo: it can leave fields under a
  deleted type marker or overwrite a newer value.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.{CompoundKey, Ops}

  @hook :ferricstore_hash_type_delete_hook

  setup do
    on_exit(fn -> Process.delete(@hook) end)

    %{
      ctx: FerricStore.Instance.get(:default),
      key: "{hash-delete-cleanup:#{System.unique_integer([:positive])}}"
    }
  end

  test "HDEL whose type marker delete commits but times out leaves no field behind",
       %{ctx: ctx, key: key} do
    assert 1 = Dispatcher.dispatch("HSET", [key, "f", "v"], ctx)

    Process.put(@hook, fn delete ->
      assert :ok = delete.()
      {:error, {:timeout, :unknown_outcome}}
    end)

    assert {:error, {:timeout, :unknown_outcome}} = Dispatcher.dispatch("HDEL", [key, "f"], ctx)
    Process.delete(@hook)

    assert nil == Dispatcher.dispatch("HGET", [key, "f"], ctx)
    assert 0 == Dispatcher.dispatch("EXISTS", [key], ctx)
    assert 0 == Ops.compound_count(ctx, key, CompoundKey.hash_prefix(key))
  end

  test "HDEL whose type marker delete fails keeps the field deleted", %{ctx: ctx, key: key} do
    assert 1 = Dispatcher.dispatch("HSET", [key, "f", "v"], ctx)

    Process.put(@hook, fn _delete -> {:error, {:timeout, :unknown_outcome}} end)

    assert {:error, {:timeout, :unknown_outcome}} = Dispatcher.dispatch("HDEL", [key, "f"], ctx)
    Process.delete(@hook)

    assert nil == Dispatcher.dispatch("HGET", [key, "f"], ctx)
    assert 0 == Ops.compound_count(ctx, key, CompoundKey.hash_prefix(key))
  end

  test "a failed HDEL cleanup does not overwrite a value written after the delete",
       %{ctx: ctx, key: key} do
    assert 1 = Dispatcher.dispatch("HSET", [key, "f", "old"], ctx)

    Process.put(@hook, fn _delete ->
      assert 1 =
               Task.async(fn -> Dispatcher.dispatch("HSET", [key, "f", "new"], ctx) end)
               |> Task.await()

      {:error, {:timeout, :unknown_outcome}}
    end)

    assert {:error, {:timeout, :unknown_outcome}} = Dispatcher.dispatch("HDEL", [key, "f"], ctx)
    Process.delete(@hook)

    assert "new" == Dispatcher.dispatch("HGET", [key, "f"], ctx)
  end

  test "HGETDEL whose type marker delete commits but times out leaves no field behind",
       %{ctx: ctx, key: key} do
    assert 1 = Dispatcher.dispatch("HSET", [key, "f", "v"], ctx)

    Process.put(@hook, fn delete ->
      assert :ok = delete.()
      {:error, {:timeout, :unknown_outcome}}
    end)

    assert {:error, {:timeout, :unknown_outcome}} =
             Dispatcher.dispatch("HGETDEL", [key, "FIELDS", "1", "f"], ctx)

    Process.delete(@hook)

    assert nil == Dispatcher.dispatch("HGET", [key, "f"], ctx)
    assert 0 == Ops.compound_count(ctx, key, CompoundKey.hash_prefix(key))
  end
end
