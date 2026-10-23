defmodule Ferricstore.Flow.ProjectedValueLevelTest do
  @moduledoc """
  From feature level 5, apply reads a record's values through the LMDB mirror
  even where the answer decides what else is written (see
  Ferricstore.Flow.ProjectedValueApplyTest for the reads that need no level).
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 5}} = FeatureLevel.raise(FerricStore.Instance.get(:default), 5)
    :ok
  end

  # A conflict fails the whole batch, so the answer decides what the batch writes.
  test "a create batch retrying a projected item still creates its new items" do
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

    assert :ok =
             FerricStore.flow_create_many(
               partition,
               [item.(existing, "kept"), item.(fresh, "new")],
               idempotent: true,
               now_ms: 20
             )

    assert {:ok, %{id: ^fresh}} = FerricStore.flow_get(fresh, partition_key: partition)
  end

  # Rewinding to a terminal event restarts the record's retention, and its values
  # must last as long as the record.
  test "a rewind extends a projected value's expiry with its record" do
    ctx = FerricStore.Instance.get(:default)
    id = "projected-rewind-#{System.unique_integer([:positive])}"
    type = "projected-rewind-#{System.unique_integer([:positive])}"
    now = System.os_time(:millisecond)

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               retention_ttl_ms: 60_000,
               run_at_ms: now,
               now_ms: now
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type, worker: "rewind", limit: 1, now_ms: now + 1)

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               result: "done",
               now_ms: now + 2
             )

    assert {:ok, [{completed_event, _fields} | _]} = FerricStore.flow_history(id, count: 1)
    assert {:ok, completed} = FerricStore.flow_get(id)
    projected!(completed.result_ref)

    assert :ok = FerricStore.flow_rewind(id, to_event: completed_event, now_ms: now + 30_000)
    assert {:ok, rewound} = FerricStore.flow_get(id)
    assert rewound.result_ref == completed.result_ref
    assert rewound.terminal_retention_until_ms > completed.terminal_retention_until_ms

    assert value_expire_at(ctx, rewound.result_ref) == rewound.terminal_retention_until_ms
  end

  # Where the value now lives: its keydir row, or else its LMDB locator.
  defp value_expire_at(ctx, ref) do
    shard_index = Router.shard_for(ctx, ref)

    case :ets.lookup(elem(ctx.keydir_refs, shard_index), ref) do
      [{^ref, _value, expire_at_ms, _lfu, _file_id, _offset, _size}] ->
        expire_at_ms

      [] ->
        path =
          ctx.data_dir
          |> Ferricstore.DataDir.shard_data_path(shard_index)
          |> Ferricstore.Flow.LMDB.path()

        assert {:ok, blob} = Ferricstore.Flow.LMDB.get(path, ref)

        assert {:ok, {:flow_value_locator, 1, expire_at_ms, _file_id, _offset, _size}} =
                 Ferricstore.TermCodec.decode(blob)

        expire_at_ms
    end
  end

  defp projected!(ref, attempts \\ 500)

  defp projected!(ref, 0), do: flunk("the projector kept #{ref} in the keydir")

  defp projected!(ref, attempts) do
    ctx = FerricStore.Instance.get(:default)
    shard_index = Router.shard_for(ctx, ref)
    assert :ok = HistoryProjector.flush(ctx, shard_index)

    if :ets.lookup(elem(ctx.keydir_refs, shard_index), ref) != [] do
      Process.sleep(10)
      projected!(ref, attempts - 1)
    end
  end
end
