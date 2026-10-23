defmodule Ferricstore.Flow.ProjectedValueApplyTest do
  @moduledoc """
  Once the history projector has projected a Flow value, it removes the value's
  keydir row and leaves its location in the shard's LMDB mirror. How far a
  node's projector has got is local to that node, so apply must find a record's
  values either way. Where the answer decides what else is written, that takes
  feature level 5 (Ferricstore.Flow.ProjectedValueLevelTest).
  """
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Store.Router

  test "an idempotent create retry matches a payload the projector moved out of the keydir" do
    for payload <- ["small payload", :binary.copy("blob-payload", 24_000)] do
      id = "projected-retry-#{System.unique_integer([:positive])}"

      opts = [
        type: "projected-retry",
        state: "queued",
        payload: payload,
        run_at_ms: 1_000,
        now_ms: 10,
        idempotent: true
      ]

      assert :ok = FerricStore.flow_create(id, opts)
      assert {:ok, created} = FerricStore.flow_get(id)
      projected!(created.payload_ref)

      assert :ok = FerricStore.flow_create(id, Keyword.put(opts, :now_ms, 20))
      assert {:ok, %{version: version}} = FerricStore.flow_get(id)
      assert version == created.version

      assert {:error, "ERR flow idempotency conflict"} =
               FerricStore.flow_create(id, Keyword.put(opts, :payload, payload <> "!"))
    end
  end

  # A conflict fails the whole batch, so there the answer decides what the batch
  # writes; below feature level 5 apply answers as older releases do.
  test "below level 5 a batch holding a projected retry keeps the keydir's answer" do
    partition = "projected-batch-#{System.unique_integer([:positive])}"
    existing = "existing-#{System.unique_integer([:positive])}"
    fresh = "fresh-#{System.unique_integer([:positive])}"
    item = fn id, payload -> {id, type: "projected-batch", payload: payload, run_at_ms: 1_000} end

    assert :ok =
             FerricStore.flow_create_many(partition, [item.(existing, "kept")],
               idempotent: true,
               now_ms: 10
             )

    assert {:ok, created} = FerricStore.flow_get(existing, partition_key: partition)
    projected!(created.payload_ref)

    assert {:error, "ERR flow idempotency conflict"} =
             FerricStore.flow_create_many(
               partition,
               [item.(existing, "kept"), item.(fresh, "new")],
               idempotent: true,
               now_ms: 20
             )

    # A batch of retries alone writes nothing either way, so it reads the mirror.
    assert :ok =
             FerricStore.flow_create_many(partition, [item.(existing, "kept")],
               idempotent: true,
               now_ms: 30
             )
  end

  # The mirror is derived and per node. One this node cannot read leaves the
  # decision to the keydir, as before the mirror was read, rather than blocking
  # the shard's apply.
  test "an unreadable mirrored payload does not block the shard" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout()
    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)
    id = "projected-unreadable-#{System.unique_integer([:positive])}"

    opts = [
      type: "projected-unreadable",
      state: "queued",
      payload: "kept payload",
      run_at_ms: 1_000,
      now_ms: 10,
      idempotent: true
    ]

    assert :ok = FerricStore.Impl.flow_create(ctx, id, opts)
    assert {:ok, %{payload_ref: ref}} = FerricStore.Impl.flow_get(ctx, id, [])
    projected!(ctx, ref)

    shard_index = Router.shard_for(ctx, ref)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index)
    assert {:ok, blob} = Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(shard_path), ref)

    assert {:ok, {:flow_value_locator, 1, _expire, {:flow_history, file_id}, offset, _size}} =
             Ferricstore.TermCodec.decode(blob)

    log = Path.join([shard_path, "history", String.pad_leading("#{file_id}", 5, "0") <> ".log"])
    {:ok, file} = :file.open(log, [:read, :write, :binary])
    :ok = :file.pwrite(file, offset, :binary.copy(<<0xFF>>, 8))
    :ok = :file.close(file)

    assert {:error, "ERR flow idempotency conflict"} =
             FerricStore.Impl.flow_create(ctx, id, Keyword.put(opts, :now_ms, 20))

    assert :ok =
             FerricStore.Impl.flow_create(ctx, id <> "-after", Keyword.put(opts, :now_ms, 30))
  end

  defp projected!(ref), do: projected!(FerricStore.Instance.get(:default), ref)

  defp projected!(ctx, ref, attempts \\ 500)

  defp projected!(_ctx, ref, 0), do: flunk("the projector kept #{ref} in the keydir")

  defp projected!(ctx, ref, attempts) do
    shard_index = Router.shard_for(ctx, ref)
    assert :ok = HistoryProjector.flush(ctx, shard_index)

    if :ets.lookup(elem(ctx.keydir_refs, shard_index), ref) != [] do
      Process.sleep(10)
      projected!(ctx, ref, attempts - 1)
    end
  end
end
