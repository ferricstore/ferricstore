defmodule Ferricstore.Store.PublicReadErrorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Strings
  alias Ferricstore.Store.LFU
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    keydir = elem(ctx.keydir_refs, 0)
    key = "invalid-public-read"
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :invalid_offset, 5})

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    %{ctx: ctx, key: key}
  end

  test "embedded APIs preserve typed read failures", %{ctx: ctx, key: key} do
    assert {:error, {:storage_read_failed, _reason}} = FerricStore.Impl.get(ctx, key)
    assert {:error, {:storage_read_failed, _reason}} = FerricStore.Impl.mget(ctx, [key])
    assert {:error, {:storage_read_failed, _reason}} = FerricStore.Impl.strlen(ctx, key)
  end

  test "command APIs return a stable sanitized error", %{ctx: ctx, key: key} do
    assert {:error, "ERR storage read failed"} = Strings.handle("GET", [key], ctx)
    assert {:error, "ERR storage read failed"} = Strings.handle("MGET", [key], ctx)
    assert {:error, "ERR storage read failed"} = Strings.get_bounded(key, ctx, :unlimited)
  end
end
