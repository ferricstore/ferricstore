defmodule Ferricstore.EmbeddedApiWriteErrorsTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
    end)

    {:ok, ctx: ctx}
  end

  test "set propagates router write errors", %{ctx: ctx} do
    break_active_file(ctx, "set_error")

    assert {:error, _reason} = FerricStore.Impl.set(ctx, "set_error", "value")
    assert {:ok, nil} = FerricStore.Impl.get(ctx, "set_error")
  end

  test "set nx propagates router write errors instead of true", %{ctx: ctx} do
    break_active_file(ctx, "set_nx_error")

    assert {:error, _reason} = FerricStore.Impl.set(ctx, "set_nx_error", "value", nx: true)
    assert {:ok, nil} = FerricStore.Impl.get(ctx, "set_nx_error")
  end

  test "set get propagates router write errors instead of old value", %{ctx: ctx} do
    break_active_file(ctx, "set_get_error")

    assert {:error, _reason} = FerricStore.Impl.set(ctx, "set_get_error", "value", get: true)
    assert {:ok, nil} = FerricStore.Impl.get(ctx, "set_get_error")
  end

  test "set xx propagates router write errors when key exists", %{ctx: ctx} do
    assert :ok = FerricStore.Impl.set(ctx, "set_xx_error", "old")
    break_active_file(ctx, "set_xx_error")

    assert {:error, _reason} = FerricStore.Impl.set(ctx, "set_xx_error", "new", xx: true)
    assert {:ok, "old"} = FerricStore.Impl.get(ctx, "set_xx_error")
  end

  test "setnx propagates router write errors instead of true", %{ctx: ctx} do
    break_active_file(ctx, "setnx_error")

    assert {:error, _reason} = FerricStore.Impl.setnx(ctx, "setnx_error", "value")
    assert {:ok, nil} = FerricStore.Impl.get(ctx, "setnx_error")
  end

  test "setex propagates router write errors", %{ctx: ctx} do
    break_active_file(ctx, "setex_error")

    assert {:error, _reason} = FerricStore.Impl.setex(ctx, "setex_error", 60, "value")
    assert {:ok, nil} = FerricStore.Impl.get(ctx, "setex_error")
  end

  test "psetex propagates router write errors", %{ctx: ctx} do
    break_active_file(ctx, "psetex_error")

    assert {:error, _reason} = FerricStore.Impl.psetex(ctx, "psetex_error", 60_000, "value")
    assert {:ok, nil} = FerricStore.Impl.get(ctx, "psetex_error")
  end

  test "mset propagates the first router write error", %{ctx: ctx} do
    break_active_file(ctx, "mset_error")

    assert {:error, _reason} = FerricStore.Impl.mset(ctx, [{"mset_error", "value"}])
    assert {:ok, nil} = FerricStore.Impl.get(ctx, "mset_error")
  end

  test "del propagates router write errors instead of counting failed deletes", %{ctx: ctx} do
    assert :ok = FerricStore.Impl.set(ctx, "del_error", "value")
    break_active_file(ctx, "del_error")

    assert {:error, _reason} = FerricStore.Impl.del(ctx, ["del_error"])
    assert {:ok, "value"} = FerricStore.Impl.get(ctx, "del_error")
  end

  defp break_active_file(ctx, key) do
    shard = elem(ctx.shard_names, Router.shard_for(ctx, key))
    state = :sys.get_state(shard)

    File.rm_rf!(Path.dirname(state.active_file_path))
  end
end
