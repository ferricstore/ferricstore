defmodule Ferricstore.Commands.HashSingleWARaftTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricStore.Impl
  alias Ferricstore.Store.{CompoundKey, Router}

  setup do
    :ok = Ferricstore.Test.ShardHelpers.wait_default_pipeline_ready()
    ctx = FerricStore.Instance.get(:default)
    key = "single-hset:#{System.unique_integer([:positive])}"
    on_exit(fn -> Impl.del(ctx, [key]) end)
    %{ctx: ctx, key: key}
  end

  test "a public single-field HSET uses one durable command for type and value", %{
    ctx: ctx,
    key: key
  } do
    parent = self()
    handler = {__MODULE__, make_ref()}
    shard_index = Router.shard_for(ctx, key)

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :commit, :stage],
      fn _event, _measurements, metadata, {owner, index} ->
        if metadata.shard_index == index and
             metadata.command_shape in [
               :hset_single,
               :compound_type_claim,
               :compound_batch_put,
               :batch
             ],
           do: send(owner, {:committed, metadata.command_shape})
      end,
      {parent, shard_index}
    )

    try do
      assert {:ok, 1} = Impl.hset(ctx, key, %{"field" => "value"})
      assert_receive {:committed, :hset_single}
      refute_received {:committed, _other}
      assert {:ok, "value"} = Impl.hget(ctx, key, "field")
      assert :ok = FerricStore.hset(key, %{"public-field" => "public-value"})
      assert_receive {:committed, :hset_single}
      refute_received {:committed, _other}
      assert {:ok, "public-value"} = FerricStore.hget(key, "public-field")
    after
      :telemetry.detach(handler)
    end
  end

  test "concurrent single-field insertion reports exactly one new field", %{ctx: ctx, key: key} do
    assert {:ok, 2} = Impl.hset(ctx, key, %{"seed-a" => "a", "seed-b" => "b"})
    parent = self()

    tasks =
      for i <- 1..16 do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> Impl.hset(ctx, key, %{"field" => "value-#{i}"})
          end
        end)
      end

    for task <- tasks, do: assert_receive({:ready, pid} when pid == task.pid)
    for task <- tasks, do: send(task.pid, :go)
    replies = Task.await_many(tasks, 10_000)
    assert Enum.count(replies, &(&1 == {:ok, 1})) == 1
    assert Enum.count(replies, &(&1 == {:ok, 0})) == 15
  end

  test "single-field writes reject a string key without replacing it", %{ctx: ctx, key: key} do
    assert :ok = Router.put(ctx, key, "string", 0)
    assert {:error, "WRONGTYPE" <> _} = Impl.hset(ctx, key, %{"field" => "value"})
    assert Router.get(ctx, key) == "string"
  end

  test "opt-in coalescing preserves public scalar counts for a contended promoted hash", %{
    ctx: ctx,
    key: key
  } do
    previous = Application.get_env(:ferricstore, :waraft_single_hset_coalescing)
    Application.put_env(:ferricstore, :waraft_single_hset_coalescing, true)

    on_exit(fn ->
      if previous == nil,
        do: Application.delete_env(:ferricstore, :waraft_single_hset_coalescing),
        else: Application.put_env(:ferricstore, :waraft_single_hset_coalescing, previous)
    end)

    assert {:ok, 128} = Impl.hset(ctx, key, Map.new(1..128, &{"seed-#{&1}", "seed"}))
    index = Router.shard_for(ctx, key)
    shard = Router.shard_name(ctx, index)

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> GenServer.call(shard, {:promoted?, key}) end,
      "test hash was not promoted"
    )

    parent = self()
    handler = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler,
      [:ferricstore, :waraft, :commit, :stage],
      fn _, _, metadata, _ ->
        if metadata.shard_index == index and metadata.command_shape == :batch,
          do: send(parent, :coalesced_commit)
      end,
      nil
    )

    try do
      for round <- 1..8 do
        tasks =
          for client <- 1..16 do
            Task.async(fn ->
              Impl.hset(ctx, key, %{"shared" => :binary.copy("#{client}", 512)})
            end)
          end

        replies = Task.await_many(tasks, 15_000)
        assert Enum.count(replies, &(&1 == {:ok, 1})) == if(round == 1, do: 1, else: 0)
        assert Enum.all?(replies, &(&1 in [{:ok, 0}, {:ok, 1}]))
      end

      assert_receive :coalesced_commit, 1_000
      assert {:ok, value} = Impl.hget(ctx, key, "shared")
      assert is_binary(value)
    after
      :telemetry.detach(handler)
    end
  end

  test "single-field replacement clears TTL and expired fields count as new", %{
    ctx: ctx,
    key: key
  } do
    field = CompoundKey.hash_field(key, "field")
    assert {:ok, 1} = Impl.hset(ctx, key, %{"field" => "old"})
    expiry = Ferricstore.HLC.now_ms() + 30
    assert :ok = Router.compound_put(ctx, key, field, "old", expiry)
    Process.sleep(60)
    assert {:ok, 1} = Impl.hset(ctx, key, %{"field" => "new"})
    assert Router.compound_get_meta(ctx, key, field) == {"new", 0}
    assert {:ok, 0} = Impl.hset(ctx, key, %{"field" => "replacement"})
  end

  test "oversized single-field creation leaves no type marker or value", %{ctx: ctx, key: key} do
    oversized = :binary.copy("x", ctx.apply_context.max_value_size + 1)
    assert {:error, _reason} = Impl.hset(ctx, key, %{"field" => oversized})
    assert Router.compound_get(ctx, key, CompoundKey.type_key(key)) == nil
    assert Router.compound_get(ctx, key, CompoundKey.hash_field(key, "field")) == nil
  end

  test "durable promoted HSET does not wait for unrelated shared file-server work", %{
    ctx: ctx,
    key: key
  } do
    assert {:ok, 128} = Impl.hset(ctx, key, Map.new(1..128, &{"seed-#{&1}", "seed"}))
    shard = Router.shard_name(ctx, Router.shard_for(ctx, key))

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> GenServer.call(shard, {:promoted?, key}) end,
      "test hash was not promoted"
    )

    assert {:ok, 1} = Impl.hset(ctx, key, %{"field" => "before"})
    :ok = :sys.suspend(:file_server_2)
    task = Task.async(fn -> Impl.hset(ctx, key, %{"field" => "after"}) end)

    early =
      try do
        Task.yield(task, 500)
      after
        :sys.resume(:file_server_2)
      end

    # Drain even a failing candidate before test teardown or another global-state
    # fixture can replace the owned keydir/storage context.
    result =
      case early do
        nil -> Task.await(task, 10_000)
        {:ok, result} -> result
      end

    assert result == {:ok, 0}
    assert early == {:ok, {:ok, 0}}
    assert Impl.hget(ctx, key, "field") == {:ok, "after"}
  end
end
