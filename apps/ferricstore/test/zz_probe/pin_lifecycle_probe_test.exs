defmodule Ferricstore.ZzProbe.PinLifecycleProbeTest do
  use ExUnit.Case, async: false
  @moduletag timeout: :infinity
  @moduletag :global_state
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Store.Router

  defp rows(keydir, id) do
    :ets.tab2list(keydir)
    |> Enum.filter(fn row ->
      Keys.value_key?(elem(row, 0)) and String.contains?(elem(row, 0), id)
    end)
    |> Enum.map(&put_elem(&1, 1, :v))
  end

  defp flush(ctx, shard) do
    :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)
    :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard, 60_000)
    :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 60_000)
  end

  defp lmdb(ctx, shard, id) do
    path =
      ctx.data_dir |> Ferricstore.DataDir.shard_data_path(shard) |> Ferricstore.Flow.LMDB.path()

    {:ok, pins} =
      Ferricstore.Flow.LMDB.segment_value_pin_entries_before(path, 1_000_000_000, 10_000)

    pins = Enum.filter(pins, &String.contains?(&1.key, id))

    locators =
      for pin <- pins do
        {pin.key, pin.file_id,
         case Ferricstore.Flow.LMDB.get(path, pin.key) do
           {:ok, blob} -> Ferricstore.Flow.LMDB.decode_value_locator(blob, 0)
           other -> other
         end}
      end

    {length(pins), locators}
  end

  test "pins of a deleted flow's values" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    ctx = FerricStore.Instance.get(:default)
    id = "pinprobe"
    shard = Router.shard_for(ctx, Keys.state_key(id, nil))
    keydir = elem(ctx.keydir_refs, shard)

    :ok = FerricStore.flow_create(id, type: "pinprobe", payload: :binary.copy("p", 4096))
    IO.inspect(rows(keydir, id), label: "after create")
    {:ok, [c]} = FerricStore.flow_claim_due("pinprobe", worker: "w", limit: 1)

    :ok =
      FerricStore.flow_complete(c.id, c.lease_token,
        fencing_token: c.fencing_token,
        result: :binary.copy("r", 4096)
      )

    {:ok, rec} = FerricStore.flow_get(id)
    flush(ctx, shard)
    IO.inspect(rows(keydir, id), label: "after projection")
    IO.inspect(lmdb(ctx, shard, id), label: "lmdb after projection")

    {:ok, r} =
      FerricStore.flow_retention_cleanup(limit: 10, now_ms: rec.terminal_retention_until_ms + 1)

    IO.inspect(r, label: "cleanup")
    flush(ctx, shard)
    IO.inspect(rows(keydir, id), label: "after cleanup")
    IO.inspect(lmdb(ctx, shard, id), label: "lmdb after cleanup")

    root = Path.join([ctx.data_dir, "waraft", "ferricstore_waraft_backend.#{shard + 1}"])

    log =
      {:raft_log, :"raft_log_ferricstore_waraft_backend_#{shard + 1}",
       :ferricstore_waraft_backend, :ferricstore_waraft_backend, shard + 1,
       :ferricstore_waraft_spike_segment_log}

    last = :ferricstore_waraft_spike_segment_log.last_index(log)

    IO.inspect(
      Ferricstore.Raft.WARaftStorage.__prepare_segment_value_pins_for_trim_for_test__(
        root,
        ctx,
        shard,
        last + 1,
        100
      ),
      label: "prepare pins for trim"
    )
  end
end
