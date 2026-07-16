defmodule Ferricstore.Store.ShardCompactionWorkerTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.Flush

  test "record copying runs outside the shard mailbox" do
    {pid, ctx, dir} = start_shard()
    parent = self()

    try do
      assert :ok = GenServer.call(pid, {:put, "live", "value", 0})
      assert :ok = GenServer.call(pid, {:put, "dead", "old", 0})
      assert :ok = GenServer.call(pid, :flush)
      assert :ok = GenServer.call(pid, {:delete, "dead"})
      assert :ok = GenServer.call(pid, :flush)
      rotate_active_file(pid)

      :sys.replace_state(pid, fn state ->
        Map.put(state, :compaction_copy_fun, fn source, dest, offsets, tombstone_offsets ->
          send(parent, {:compaction_copy_started, self()})

          receive do
            :continue_compaction ->
              if tombstone_offsets == [] do
                NIF.v2_copy_records(source, dest, offsets)
              else
                NIF.v2_copy_records_preserve_tombstones(
                  source,
                  dest,
                  offsets,
                  tombstone_offsets
                )
              end
          end
        end)
      end)

      compaction = Task.async(fn -> GenServer.call(pid, {:run_compaction, [0]}, :infinity) end)

      worker =
        receive do
          {:compaction_copy_started, worker} -> worker
        after
          1_000 -> flunk("compaction copy did not start")
        end

      assert "value" == GenServer.call(pid, {:get, "live"}, 250)
      send(worker, :continue_compaction)
      assert {:ok, {copied, 0, reclaimed}} = Task.await(compaction, 5_000)
      assert copied >= 1
      assert reclaimed > 0
    after
      cleanup_shard(pid, ctx, dir)
    end
  end

  test "compaction worker terminates when its shard is killed" do
    {pid, ctx, dir} = start_shard()
    parent = self()

    try do
      assert :ok = GenServer.call(pid, {:put, "live", "value", 0})
      assert :ok = GenServer.call(pid, :flush)
      rotate_active_file(pid)

      :sys.replace_state(pid, fn state ->
        Map.put(state, :compaction_copy_fun, fn _source, _dest, _offsets, _tombstones ->
          send(parent, {:owned_compaction_started, self()})

          receive do
            :never -> {:error, :unexpected_resume}
          end
        end)
      end)

      caller =
        spawn(fn ->
          result = catch_exit(GenServer.call(pid, {:run_compaction, [0]}, :infinity))
          send(parent, {:compaction_caller_stopped, result})
        end)

      worker =
        receive do
          {:owned_compaction_started, worker} -> worker
        after
          1_000 -> flunk("compaction copy did not start")
        end

      worker_ref = Process.monitor(worker)
      Process.unlink(pid)
      Process.exit(pid, :kill)

      assert_receive {:DOWN, ^worker_ref, :process, ^worker, _reason}, 1_000
      assert_receive {:compaction_caller_stopped, _reason}, 1_000
      refute Process.alive?(worker)
      refute Process.alive?(caller)
    after
      cleanup_shard(pid, ctx, dir)
    end
  end

  test "compaction plans and copies a large segment in bounded source pages" do
    {pid, ctx, dir} = start_shard()
    parent = self()

    try do
      for index <- 1..7 do
        assert :ok = GenServer.call(pid, {:put, "page-live-#{index}", "value-#{index}", 0})
      end

      assert :ok = GenServer.call(pid, :flush)
      rotate_active_file(pid)

      :sys.replace_state(pid, fn state ->
        Map.put(state, :compaction_scan_page_fun, fn path, offset, _configured_limit ->
          result = NIF.v2_scan_file_page(path, offset, 2)

          case result do
            {:ok, records, _next_offset, _done?} ->
              send(parent, {:compaction_source_page, length(records)})

            _other ->
              :ok
          end

          result
        end)
      end)

      assert {:ok, {7, 0, _reclaimed}} =
               GenServer.call(pid, {:run_compaction, [0]}, 10_000)

      page_sizes = collect_page_sizes([])
      assert length(page_sizes) >= 4
      assert Enum.all?(page_sizes, &(&1 <= 2))

      for index <- 1..7 do
        assert "value-#{index}" == GenServer.call(pid, {:get, "page-live-#{index}"})
      end
    after
      cleanup_shard(pid, ctx, dir)
    end
  end

  defp start_shard do
    dir =
      Path.join(
        System.tmp_dir!(),
        "shard_compaction_worker_#{System.unique_integer([:positive])}"
      )

    name = :"shard_compaction_worker_#{System.unique_integer([:positive])}"
    ctx = FerricStore.Instance.build(name, data_dir: dir, shard_count: 1)
    :ok = Ferricstore.DataDir.ensure_layout!(dir, 1)

    {:ok, pid} =
      Shard.start_link(
        index: 0,
        data_dir: dir,
        flush_interval_ms: 5_000,
        flow_shared_ref_backfill?: false,
        instance_ctx: ctx
      )

    {pid, ctx, dir}
  end

  defp rotate_active_file(pid) do
    :sys.replace_state(pid, fn state ->
      Flush.maybe_rotate_file(%{state | active_file_size: state.max_active_file_size})
    end)

    assert :sys.get_state(pid).active_file_id == 1
  end

  defp cleanup_shard(pid, ctx, dir) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    FerricStore.Instance.cleanup(ctx.name)
    File.rm_rf!(dir)

    case Process.whereis(Router.shard_name(ctx, 0)) do
      nil -> :ok
      registered when is_pid(registered) -> Process.exit(registered, :kill)
    end
  end

  defp collect_page_sizes(acc) do
    receive do
      {:compaction_source_page, size} -> collect_page_sizes([size | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
