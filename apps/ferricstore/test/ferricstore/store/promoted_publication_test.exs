defmodule Ferricstore.Store.PromotedPublicationTest do
  use ExUnit.Case, async: false
  @moduletag :global_state
  alias Ferricstore.Store.{CompoundKey, PromotedPublication, PublicationEpoch, Router}
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, promotion_threshold: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    %{ctx: ctx, owner: %{instance_ctx: ctx, index: 0}}
  end

  test "a logical mutation opens at publication and stays protected across nested phases", %{
    ctx: ctx,
    owner: owner
  } do
    parent = self()

    writer =
      Task.async(fn ->
        PromotedPublication.with_scope(fn ->
          assert rem(:atomics.get(ctx.publication_epoch, 1), 2) == 0
          PromotedPublication.publish(owner, fn -> :ok end)

          PromotedPublication.with_scope(fn ->
            PromotedPublication.publish(owner, fn -> :ok end)
            PromotedPublication.finish()
          end)

          send(parent, :between_phases)

          receive do
            :continue -> :ok
          end

          PromotedPublication.publish(owner, fn -> :ok end)
        end)
      end)

    assert_receive :between_phases
    reader = Task.async(fn -> PublicationEpoch.read(ctx, [0], fn -> :stable end) end)

    try do
      assert Task.yield(reader, 20) == nil
      send(writer.pid, :continue)
      assert Task.await(writer) == :ok
      assert Task.await(reader) == :stable
    after
      send(writer.pid, :continue)
      Task.shutdown(writer, :brutal_kill)
      Task.shutdown(reader, :brutal_kill)
    end
  end

  test "borrowing an existing writer does not close it or hide a later publication", %{
    ctx: ctx,
    owner: owner
  } do
    PromotedPublication.with_scope(fn ->
      token = PublicationEpoch.begin_write(ctx, 0)
      PromotedPublication.publish(owner, fn -> :ok end)
      PromotedPublication.finish()
      assert rem(:atomics.get(ctx.publication_epoch, 1), 2) == 1
      PublicationEpoch.end_write(token)
      PromotedPublication.publish(owner, fn -> :ok end)
      assert rem(:atomics.get(ctx.publication_epoch, 1), 2) == 1
    end)

    assert rem(:atomics.get(ctx.publication_epoch, 1), 2) == 0
  end

  test "an exceptional scope releases its publication protection", %{ctx: ctx, owner: owner} do
    assert_raise RuntimeError, "publication failed", fn ->
      PromotedPublication.with_scope(fn ->
        PromotedPublication.publish(owner, fn -> raise "publication failed" end)
      end)
    end

    assert PublicationEpoch.read(ctx, [0], fn -> :stable end) == :stable
    assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
    PromotedPublication.with_scope(fn -> PromotedPublication.publish(owner, fn -> :ok end) end)
    assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
  end

  test "a killed publisher cannot authorize a partial cached value through epoch repair", %{
    ctx: ctx,
    owner: owner
  } do
    keydir = elem(ctx.keydir_refs, 0)

    {pid, monitor} =
      spawn_monitor(fn ->
        PromotedPublication.with_scope(fn ->
          PromotedPublication.publish(owner, fn ->
            :ets.insert(keydir, {"partial-key", "partial"})
            Process.exit(self(), :kill)
          end)
        end)
      end)

    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}

    assert PromotedPublication.read(ctx, 0, fn ->
             :ets.lookup_element(keydir, "partial-key", 2)
           end) == :fallback

    :ets.insert(keydir, {"partial-key", "recovered"})
    PublicationEpoch.reset(ctx, 0)

    assert PromotedPublication.read(ctx, 0, fn ->
             :ets.lookup_element(keydir, "partial-key", 2)
           end) == "recovered"

    :ets.delete(keydir, "partial-key")
  end

  test "actual direct promoted batch publication protects an epoch reader", %{ctx: ctx} do
    key = "publication-batch:#{System.unique_integer([:positive])}"
    assert {:ok, 2} = FerricStore.Impl.hset(ctx, key, %{"a" => "old", "b" => "old"})
    shard = Router.shard_name(ctx, 0)

    Ferricstore.Test.ShardHelpers.eventually(
      fn -> GenServer.call(shard, {:promoted?, key}) end,
      "hash was not promoted"
    )

    parent = self()

    :sys.replace_state(shard, fn state ->
      Process.put(:ferricstore_promoted_publication_hook, fn ->
        Process.delete(:ferricstore_promoted_publication_hook)
        send(parent, {:publishing, self()})

        receive do
          :continue -> :ok
        after
          5_000 -> raise "publication hook timed out"
        end
      end)

      state
    end)

    fields = Enum.map(["a", "b"], &CompoundKey.hash_field(key, &1))

    writer =
      Task.async(fn -> Router.compound_batch_put(ctx, key, Enum.map(fields, &{&1, "new", 0})) end)

    assert_receive {:publishing, publisher}, 2_000

    reader =
      Task.async(fn ->
        PublicationEpoch.read(ctx, [0], fn ->
          Enum.map(fields, fn field -> :ets.lookup_element(elem(ctx.keydir_refs, 0), field, 2) end)
        end)
      end)

    try do
      assert Task.yield(reader, 20) == nil
      send(publisher, :continue)
      assert Task.await(writer) == :ok
      assert Task.await(reader) == ["new", "new"]
    after
      send(publisher, :continue)
      Task.shutdown(writer, :brutal_kill)
      Task.shutdown(reader, :brutal_kill)
    end
  end

  test "lifecycle replacement keeps cache reads on the serialized fallback until complete", %{
    ctx: ctx,
    owner: owner
  } do
    parent = self()

    worker =
      Task.async(fn ->
        PromotedPublication.with_lifecycle(owner, fn ->
          send(parent, :lifecycle_entered)

          receive do
            :continue -> {:ok, :recovered}
          end
        end)
      end)

    assert_receive :lifecycle_entered

    try do
      assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
      PublicationEpoch.reset(ctx, 0)
      assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
      send(worker.pid, :continue)
      assert Task.await(worker) == {:ok, :recovered}
      assert PromotedPublication.read(ctx, 0, fn -> :complete end) == :complete
    after
      send(worker.pid, :continue)
      Task.shutdown(worker, :brutal_kill)
    end
  end

  test "a failed lifecycle remains fenced until a successful replacement", %{
    ctx: ctx,
    owner: owner
  } do
    assert PromotedPublication.with_lifecycle(owner, fn -> {:error, :snapshot_failed} end) ==
             {:error, :snapshot_failed}

    assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback

    assert PromotedPublication.with_lifecycle(owner, fn -> {:ok, :recovered} end) ==
             {:ok, :recovered}

    assert PromotedPublication.read(ctx, 0, fn -> :complete end) == :complete
  end

  test "nested lifecycle work cannot clear the outer replacement barrier", %{
    ctx: ctx,
    owner: owner
  } do
    assert PromotedPublication.with_lifecycle(owner, fn ->
             assert PromotedPublication.with_lifecycle(owner, fn -> {:ok, :inner} end) ==
                      {:ok, :inner}

             assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
             {:ok, :outer}
           end) == {:ok, :outer}

    assert PromotedPublication.read(ctx, 0, fn -> :complete end) == :complete
  end

  test "a completed lifecycle during a read invalidates the cached result", %{
    ctx: ctx,
    owner: owner
  } do
    parent = self()

    reader =
      Task.async(fn ->
        PromotedPublication.read(ctx, 0, fn ->
          send(parent, :reading)

          receive do
            :continue -> :stale
          end
        end)
      end)

    assert_receive :reading

    try do
      assert PromotedPublication.with_lifecycle(owner, fn -> {:ok, :recovered} end) ==
               {:ok, :recovered}

      send(reader.pid, :continue)
      assert Task.await(reader) == :fallback
    after
      Task.shutdown(reader, :brutal_kill)
    end
  end

  test "blocked lifecycle handles remain fenced in all success-shaped return forms", %{
    ctx: ctx,
    owner: owner
  } do
    for handle <- [%{blocked_error: :corrupt}, %{writes_paused: true}],
        result <- [handle, {:ok, handle}, {:ok, handle, []}] do
      assert PromotedPublication.with_lifecycle(owner, fn -> result end) == result
      assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
    end
  end

  test "returned mutation errors retain the fence after a prior publication", %{
    ctx: ctx,
    owner: owner
  } do
    assert PromotedPublication.with_scope(fn ->
             PromotedPublication.publish(owner, fn -> :ok end)
             {:error, :later_append_failed}
           end) == {:error, :later_append_failed}

    assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
  end

  test "a rescued inner publication failure cannot clear the outer failure fence", %{
    ctx: ctx,
    owner: owner
  } do
    PromotedPublication.with_scope(fn ->
      assert_raise RuntimeError, fn ->
        PromotedPublication.with_scope(fn ->
          PromotedPublication.publish(owner, fn -> raise "failed" end)
        end)
      end

      :ok
    end)

    assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
  end

  test "repeated failed replacement never transiently authorizes a partial cache", %{
    ctx: ctx,
    owner: owner
  } do
    PromotedPublication.with_lifecycle(owner, fn -> {:error, :failed} end)
    parent = self()

    reader =
      Task.async(fn ->
        send(parent, :reader_ready)

        check = fn check, samples ->
          receive do
            :stop -> samples
          after
            0 ->
              assert PromotedPublication.read(ctx, 0, fn -> :partial end) == :fallback
              check.(check, samples + 1)
          end
        end

        check.(check, 0)
      end)

    assert_receive :reader_ready

    try do
      for _ <- 1..5_000 do
        PromotedPublication.with_lifecycle(owner, fn -> {:error, :failed} end)
      end

      send(reader.pid, :stop)
      assert Task.await(reader) > 0
    after
      Task.shutdown(reader, :brutal_kill)
    end
  end
end
