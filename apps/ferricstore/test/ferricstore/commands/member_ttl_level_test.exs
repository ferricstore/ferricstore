defmodule Ferricstore.Commands.MemberTtlLevelTest do
  @moduledoc """
  Below cluster feature level 6 apply writes a member added after EXPIRE
  without a TTL, as releases before 0.12.10 apply the same entry, so a
  mixed-version cluster keeps every replica's rows alike.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.{CompoundKey, Router}

  setup do
    ctx = FerricStore.Instance.get(:default)
    assert {:ok, %{active: active}} = FerricStore.cluster_feature_level()
    assert active < 6

    %{ctx: ctx, key: "{member-ttl-level:#{System.unique_integer([:positive])}}"}
  end

  defp expire_at(ctx, key, compound_key) do
    [{^compound_key, _value, expire_at_ms, _lfu, _fid, _off, _size}] =
      :ets.lookup(elem(ctx.keydir_refs, Router.shard_for(ctx, key)), compound_key)

    expire_at_ms
  end

  test "below level 6 a hash field and a list element added after EXPIRE keep no TTL",
       %{ctx: ctx, key: key} do
    hash = key <> ":h"
    list = key <> ":l"
    assert 1 == Dispatcher.dispatch("HSET", [hash, "a", "1"], ctx)
    assert 1 == Dispatcher.dispatch("RPUSH", [list, "a"], ctx)
    assert 1 == Dispatcher.dispatch("PEXPIRE", [hash, "60000"], ctx)
    assert 1 == Dispatcher.dispatch("PEXPIRE", [list, "60000"], ctx)

    assert 1 == Dispatcher.dispatch("HSET", [hash, "b", "2"], ctx)
    assert 2 == Dispatcher.dispatch("RPUSH", [list, "b"], ctx)

    assert expire_at(ctx, hash, CompoundKey.hash_field(hash, "a")) > 0
    assert 0 == expire_at(ctx, hash, CompoundKey.hash_field(hash, "b"))
    assert 0 == expire_at(ctx, list, CompoundKey.list_meta_key(list))
  end
end
