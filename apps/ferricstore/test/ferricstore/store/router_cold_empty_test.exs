defmodule Ferricstore.Store.RouterColdEmptyTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)
    shard = Process.whereis(elem(ctx.shard_names, 0))
    keydir = elem(ctx.keydir_refs, 0)

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{ctx: ctx, shard: shard, keydir: keydir}
  end

  test "get_with_file_ref treats cold empty values as valid file refs", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    key = "cold_empty:" <> Integer.to_string(:erlang.unique_integer([:positive]))

    :ok = GenServer.call(shard, {:put, key, "", 0})
    :ok = GenServer.call(shard, :flush)

    assert [{^key, "", exp, lfu, fid, off, 0}] = :ets.lookup(keydir, key)
    :ets.insert(keydir, {key, nil, exp, lfu, fid, off, 0})

    assert {:cold_ref, path, value_offset, 0} = Router.get_with_file_ref(ctx, key)
    assert File.exists?(path)
    assert is_integer(value_offset)
  end
end
