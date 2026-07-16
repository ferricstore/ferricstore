defmodule Ferricstore.Store.RouterWriteAdmissionTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Store.DiskPressure
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    ShardHelpers.reset_memory_guard_pressure()
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      :atomics.put(ctx.pressure_flags, 1, 0)
      :atomics.put(ctx.pressure_flags, 2, 0)

      for shard_index <- 0..(ctx.shard_count - 1) do
        DiskPressure.clear(ctx, shard_index)
      end
    end)

    {:ok, ctx: ctx}
  end

  test "batch PUT rejects invalid entries in place without dropping valid work", %{ctx: ctx} do
    limited_ctx = Map.put(ctx, :max_value_size, 8)
    oversized_key = String.duplicate("k", Router.max_key_size() + 1)

    results =
      Router.batch_quorum_put(limited_ctx, [
        {"admission:valid:first", "one"},
        {oversized_key, "two"},
        {"admission:value:oversized", "123456789"},
        {"admission:valid:last", "12345678"}
      ])

    assert [:ok, {:error, key_error}, {:error, value_error}, :ok] = results
    assert key_error =~ "key too large"
    assert value_error =~ "value too large"
    assert "one" == Router.get(ctx, "admission:valid:first")
    assert nil == Router.get(ctx, oversized_key)
    assert nil == Router.get(ctx, "admission:value:oversized")
    assert "12345678" == Router.get(ctx, "admission:valid:last")
  end

  test "batch PUT rejects new keys but permits updates during keydir pressure", %{ctx: ctx} do
    existing = "admission:keydir:existing"
    new_key = "admission:keydir:new"
    :ok = Router.put(ctx, existing, "old", 0)
    :atomics.put(ctx.pressure_flags, 1, 1)
    :atomics.put(ctx.pressure_flags, 2, 1)

    assert [{:error, error}, :ok] =
             Router.batch_quorum_put(ctx, [{new_key, "new"}, {existing, "updated"}])

    assert error =~ "KEYDIR_FULL"
    assert nil == Router.get(ctx, new_key)
    assert "updated" == Router.get(ctx, existing)
  end

  test "batch PUT rejects only entries owned by a pressured shard", %{ctx: ctx} do
    [pressured_key, healthy_key] = ShardHelpers.keys_on_different_shards(2)
    pressured_shard = Router.shard_for(ctx, pressured_key)
    DiskPressure.set(ctx, pressured_shard)

    assert [{:error, error}, :ok] =
             Router.batch_quorum_put(ctx, [{pressured_key, "blocked"}, {healthy_key, "stored"}])

    assert error =~ "disk pressure on shard #{pressured_shard}"
    assert nil == Router.get(ctx, pressured_key)
    assert "stored" == Router.get(ctx, healthy_key)
  end

  test "atomic MSET rejects configured oversized values before every write", %{ctx: ctx} do
    limited_ctx = Map.put(ctx, :max_value_size, 4)
    first = "admission:{mset}:first"
    second = "admission:{mset}:second"

    assert {:error, error} = Router.atomic_mset(limited_ctx, [{first, "okay"}, {second, "large"}])
    assert error =~ "value too large"
    assert nil == Router.get(ctx, first)
    assert nil == Router.get(ctx, second)
  end

  test "string RMW commands reject known oversized outputs before Raft submission", %{ctx: ctx} do
    limited_ctx = Map.put(ctx, :max_value_size, 4)

    assert {:error, append_error} = Router.append(limited_ctx, "admission:append", "12345")
    assert append_error =~ "value too large"

    assert {:error, getset_error} = Router.getset(limited_ctx, "admission:getset", "12345")
    assert getset_error =~ "value too large"

    assert {:error, setrange_error} =
             Router.setrange(limited_ctx, "admission:setrange", 4, "x")

    assert setrange_error =~ "value too large"

    assert {:error, setbit_error} = Router.setbit(limited_ctx, "admission:setbit", 32, 1)
    assert setbit_error =~ "value too large"

    for key <- ["append", "getset", "setrange", "setbit"] do
      assert nil == Router.get(ctx, "admission:#{key}")
    end
  end
end
