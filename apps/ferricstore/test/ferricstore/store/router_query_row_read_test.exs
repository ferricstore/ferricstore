defmodule Ferricstore.Store.RouterQueryRowReadTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB}
  alias Ferricstore.Flow.Query.{QueryRecordStore, QueryRowCodec}
  alias Ferricstore.Store.Router

  test "reads a flushed Flow through its query-row locator" do
    ctx =
      Ferricstore.Test.IsolatedInstance.checkout(
        shard_count: 1,
        hot_cache_max_value_size: 1
      )

    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    id = "router-query-row-#{System.unique_integer([:positive])}"
    partition = "tenant-router-query-row"

    assert :ok =
             Ferricstore.Flow.create(ctx, id,
               type: "router-query-row",
               partition_key: partition,
               run_at_ms: 1,
               now_ms: 1
             )

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "router-query-row",
               worker: "router-query-row-worker",
               partition_key: partition,
               now_ms: 2
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: 3
             )

    assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 120_000)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, 1)

    state_key = Keys.state_key(id, partition)
    path = ctx.data_dir |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()

    assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

    assert {:ok, encoded_row} = LMDB.get(path, state_key)
    assert {:ok, row} = QueryRowCodec.decode(encoded_row, state_key)
    assert row.record.id == id

    assert {:ok, [encoded_record], true} =
             QueryRecordStore.read_encoded_many(
               ctx,
               0,
               path,
               [state_key],
               3,
               1_000_000
             )

    assert %{id: ^id} = Ferricstore.Flow.decode_record(encoded_record)
    assert [^encoded_record] = Router.flow_lmdb_batch_get_state_keys(ctx, [state_key])
  end
end
