defmodule Ferricstore.Store.ExpiryRouterTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.FetchOrCompute.Outcome, as: FetchOrComputeOutcome
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  import ExUnit.CaptureLog

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "replicated expiry accepts a mixed plain and compound batch" do
    ctx = FerricStore.Instance.get(:default)
    {redis_key, compound_key, shard_index} = compound_fixture(ctx, "mixed")
    plain_key = same_shard_key(ctx, shard_index, "plain")
    expired_at_ms = System.os_time(:millisecond) - 1_000

    assert :ok = Router.put(ctx, plain_key, "plain", expired_at_ms)
    assert :ok = Router.compound_put(ctx, redis_key, compound_key, "compound", expired_at_ms)

    assert [true, true] =
             Router.expire_if_batch(ctx, shard_index, [
               {compound_key, expired_at_ms},
               {plain_key, expired_at_ms}
             ])

    assert Router.get(ctx, plain_key) == nil
    assert Router.compound_get_meta(ctx, redis_key, compound_key) == nil
  end

  test "replicated expiry rejects entries routed to a different shard" do
    ctx = FerricStore.Instance.get(:default)
    {_redis_key, compound_key, shard_index} = compound_fixture(ctx, "wrong-shard")
    compound_shard = Router.shard_for(ctx, compound_key)

    wrong_shard =
      Enum.find(0..(ctx.shard_count - 1), fn candidate ->
        candidate != shard_index and candidate != compound_shard
      end)

    key = same_shard_key(ctx, shard_index, "wrong-shard")
    expired_at_ms = System.os_time(:millisecond) - 1_000

    assert Router.shard_for(ctx, key) == shard_index

    assert {:error, :invalid_expiry_batch} =
             Router.expire_if_batch(ctx, wrong_shard, [{compound_key, expired_at_ms}])

    assert {:error, :invalid_expiry_batch} =
             Router.expire_if_batch(ctx, wrong_shard, [{key, expired_at_ms}])
  end

  test "replicated expiry ignores a compound key renewed after the sweep scan" do
    ctx = FerricStore.Instance.get(:default)
    {redis_key, compound_key, shard_index} = compound_fixture(ctx, "renewed")
    expired_at_ms = System.os_time(:millisecond) - 1_000
    renewed_at_ms = System.os_time(:millisecond) + 60_000

    assert :ok = Router.compound_put(ctx, redis_key, compound_key, "old", expired_at_ms)
    assert :ok = Router.compound_put(ctx, redis_key, compound_key, "new", renewed_at_ms)

    assert [false] = Router.expire_if_batch(ctx, shard_index, [{compound_key, expired_at_ms}])
    assert {"new", ^renewed_at_ms} = Router.compound_get_meta(ctx, redis_key, compound_key)
  end

  test "the public compound expiry sweep removes keydir rows without warning" do
    ctx = FerricStore.Instance.get(:default)
    {redis_key, compound_key, shard_index} = compound_fixture(ctx, "sweep")
    type_key = CompoundKey.type_key(redis_key)
    sentinel = same_shard_key(ctx, shard_index, "sentinel")

    assert :ok = FerricStore.hset(redis_key, %{"field" => "value"})
    assert {:ok, true} = FerricStore.pexpire(redis_key, 1)
    assert :ok = FerricStore.set(sentinel, "live")

    Process.sleep(10)

    log =
      capture_log(fn ->
        assert :ok = GenServer.call(Router.shard_name(ctx, shard_index), :expiry_sweep)
      end)

    refute log =~ "replicated expiry batch failed"
    assert :ets.lookup(elem(ctx.keydir_refs, shard_index), compound_key) == []
    assert :ets.lookup(elem(ctx.keydir_refs, shard_index), type_key) == []
    assert FerricStore.get(sentinel) == {:ok, "live"}
  end

  test "replicated expiry removes opaque fetch-or-compute outcomes on their owner shard" do
    ctx = FerricStore.Instance.get(:default)
    {key, outcome_key, shard_index, raw_shard} = opaque_outcome_fixture(ctx)
    owner = "expiry-router-owner-#{System.unique_integer([:positive])}"

    assert :ok = Router.fetch_or_compute_lock(ctx, key, owner, 60_000)
    assert :ok = Router.fetch_or_compute_fail(ctx, key, owner, "boom", 1)

    [{^outcome_key, _value, expire_at_ms, _lfu, _fid, _off, _size}] =
      :ets.lookup(elem(ctx.keydir_refs, shard_index), outcome_key)

    wrong_shard = rem(shard_index + 1, ctx.shard_count)
    assert wrong_shard != shard_index
    assert raw_shard != shard_index

    assert [false] =
             Router.expire_if_batch(ctx, wrong_shard, [{outcome_key, expire_at_ms}])

    assert :ets.member(elem(ctx.keydir_refs, shard_index), outcome_key)

    Process.sleep(10)

    log =
      capture_log(fn ->
        assert :ok = GenServer.call(Router.shard_name(ctx, shard_index), :expiry_sweep)
      end)

    refute log =~ "replicated expiry batch failed"
    assert :ets.lookup(elem(ctx.keydir_refs, shard_index), outcome_key) == []
  end

  test "a removed opaque outcome does not reject other expired rows in its snapshot" do
    ctx = FerricStore.Instance.get(:default)
    {key, outcome_key, shard_index, _raw_shard} = opaque_outcome_fixture(ctx)
    owner = "stale-expiry-owner-#{System.unique_integer([:positive])}"
    plain_key = same_shard_key(ctx, shard_index, "stale-outcome")
    expired_at_ms = System.os_time(:millisecond) - 1_000

    assert :ok = Router.fetch_or_compute_lock(ctx, key, owner, 60_000)
    assert :ok = Router.fetch_or_compute_fail(ctx, key, owner, "boom", 1)

    [{^outcome_key, _value, outcome_expiry, _lfu, _fid, _off, _size}] =
      :ets.lookup(elem(ctx.keydir_refs, shard_index), outcome_key)

    Process.sleep(10)
    assert [true] = Router.expire_if_batch(ctx, shard_index, [{outcome_key, outcome_expiry}])
    assert :ok = Router.put(ctx, plain_key, "expired", expired_at_ms)

    assert [false, true, false] =
             Router.expire_if_batch(ctx, shard_index, [
               {outcome_key, outcome_expiry},
               {plain_key, expired_at_ms},
               {outcome_key, outcome_expiry}
             ])

    refute :ets.member(elem(ctx.keydir_refs, shard_index), plain_key)
  end

  test "replicated expiry preserves an opaque outcome renewed after the scan" do
    ctx = FerricStore.Instance.get(:default)
    {key, outcome_key, shard_index, _raw_shard} = opaque_outcome_fixture(ctx)
    owner = "renewed-expiry-owner-#{System.unique_integer([:positive])}"

    assert :ok = Router.fetch_or_compute_lock(ctx, key, owner, 60_000)
    assert :ok = Router.fetch_or_compute_fail(ctx, key, owner, "old", 1)

    [{^outcome_key, _value, outcome_expiry, _lfu, _fid, _off, _size}] =
      :ets.lookup(elem(ctx.keydir_refs, shard_index), outcome_key)

    Process.sleep(10)
    assert :ok = Router.fetch_or_compute_lock(ctx, key, owner, 60_000)
    assert :ok = Router.fetch_or_compute_fail(ctx, key, owner, "new", 60_000)
    [renewed] = :ets.lookup(elem(ctx.keydir_refs, shard_index), outcome_key)

    assert [false] = Router.expire_if_batch(ctx, shard_index, [{outcome_key, outcome_expiry}])
    assert [^renewed] = :ets.lookup(elem(ctx.keydir_refs, shard_index), outcome_key)
  end

  defp compound_fixture(ctx, suffix) do
    nonce = System.unique_integer([:positive])

    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(fn i ->
      redis_key = "expiry-router:#{suffix}:#{nonce}:#{i}"
      compound_key = CompoundKey.hash_field(redis_key, "field")
      {redis_key, compound_key, Router.shard_for(ctx, redis_key)}
    end)
    |> Stream.filter(fn {_, compound_key, shard_index} ->
      Router.shard_for(ctx, compound_key) != shard_index
    end)
    |> Enum.at(0)
  end

  defp same_shard_key(ctx, shard_index, suffix) do
    nonce = System.unique_integer([:positive])

    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(fn i -> "expiry-router:#{suffix}:#{nonce}:#{i}" end)
    |> Stream.filter(&(Router.shard_for(ctx, &1) == shard_index))
    |> Enum.take(1)
    |> hd()
  end

  defp opaque_outcome_fixture(ctx) do
    nonce = System.unique_integer([:positive])

    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(fn i ->
      key = "expiry-router:fetch-or-compute:#{nonce}:#{i}"
      outcome_key = FetchOrComputeOutcome.key(key)
      {key, outcome_key, Router.shard_for(ctx, key), Router.shard_for(ctx, outcome_key)}
    end)
    |> Stream.filter(fn {_key, _outcome_key, shard_index, raw_shard} ->
      shard_index != raw_shard
    end)
    |> Enum.at(0)
  end
end
