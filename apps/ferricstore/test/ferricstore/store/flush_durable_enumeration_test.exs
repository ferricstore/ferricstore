defmodule Ferricstore.Store.FlushDurableEnumerationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Ops.Flush
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard

  test "FLUSHDB durably tombstones expired rows omitted by live key enumeration" do
    data_dir =
      Path.join(System.tmp_dir!(), "flush_durable_#{System.unique_integer([:positive])}")

    name = :"flush_durable_#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.build(name, data_dir: data_dir, shard_count: 1)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    on_exit(fn -> cleanup(ctx, data_dir) end)

    {:ok, pid} = start_shard(ctx, data_dir)
    key = "flush:expired:#{System.unique_integer([:positive])}"
    expired_at_ms = Ferricstore.HLC.now_ms() - 1
    orphaned_dedicated = Path.join([data_dir, "dedicated", "shard_0", "hash:orphaned"])

    File.mkdir_p!(orphaned_dedicated)
    File.write!(Path.join(orphaned_dedicated, "00000.log"), "stale")

    assert :ok = Router.put(ctx, key, "durable", expired_at_ms)
    assert :ok = Flush.flush(ctx)
    assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), key)
    refute File.exists?(orphaned_dedicated)

    Process.unlink(pid)
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000

    {:ok, _restarted_pid} = start_shard(ctx, data_dir)

    assert [] == :ets.lookup(elem(ctx.keydir_refs, 0), key)
  end

  test "FLUSHDB uses constant-size replicated shard resets behind an all-shard gate" do
    source =
      Path.expand("../../../lib/ferricstore/store/ops/flush.ex", __DIR__)
      |> File.read!()

    assert source =~ "pause_writes_for_sync_all"
    assert source =~ "write_flush_shard_paused"
    assert source =~ "{:flush_shard_paused, flush_epoch}"
    refute source =~ "defp flush_storage_logical_keys"
    refute source =~ "defp flush_internal_keys"
    refute source =~ "defp flush_key"
  end

  defp start_shard(ctx, data_dir) do
    Shard.start_link(
      index: 0,
      data_dir: data_dir,
      instance_ctx: ctx,
      flow_shared_ref_backfill?: false
    )
  end

  defp cleanup(ctx, data_dir) do
    case Process.whereis(elem(ctx.shard_names, 0)) do
      pid when is_pid(pid) -> GenServer.stop(pid, :normal, 5_000)
      nil -> :ok
    end

    FerricStore.Instance.cleanup(ctx.name)
    File.rm_rf!(data_dir)
  catch
    :exit, _reason -> :ok
  end
end
