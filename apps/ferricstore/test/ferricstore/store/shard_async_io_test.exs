Code.require_file("shard_async_io_test/sections/v2_append_batch_nosync_nif.exs", __DIR__)
Code.require_file("shard_async_io_test/sections/concurrent_writes.exs", __DIR__)
Code.require_file("shard_async_io_test/sections/shared_log_compaction.exs", __DIR__)
Code.require_file("shard_async_io_test/sections/file_size_accounting.exs", __DIR__)

defmodule Ferricstore.Store.ShardAsyncIoTest do
  @moduledoc """
  Tests for the optimized async IO path in `Ferricstore.Store.Shard`.

  Covers:
  - v2_append_batch_nosync (write without fsync)
  - Deferred fsync via v2_fsync_async on flush timer
  - Split write+fsync path
  - Async write completion (v2_append_batch_async NIF)
  - fsync_needed state tracking
  - ETS update_element optimization in update_ets_locations
  - Data correctness after nosync write + deferred fsync
  - Concurrent writes with deferred fsync
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundKey, Promotion}
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.Reads, as: ShardReads

  @header_size 26
  @shard_source_paths [
    "../../../lib/ferricstore/store/shard.ex",
    "../../../lib/ferricstore/store/shard/startup.ex",
    "../../../lib/ferricstore/store/shard/calls.ex",
    "../../../lib/ferricstore/store/shard/routing.ex",
    "../../../lib/ferricstore/store/shard/compaction.ex",
    "../../../lib/ferricstore/store/shard/info.ex"
  ]

  defp shard_source do
    Enum.map_join(@shard_source_paths, "\n", fn path ->
      path
      |> Path.expand(__DIR__)
      |> File.read!()
    end)
  end

  defmodule SlowFlushWriter do
    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, nil, name: name)
    @impl true
    def init(nil), do: {:ok, nil}
    @impl true
    def handle_call(:flush, _from, state), do: {:noreply, state}
  end

  setup do
    :ok
  end

  # Start an isolated shard with its own Instance ctx.
  defp start_shard(opts \\ []) do
    dir = Path.join(System.tmp_dir!(), "shard_async_io_#{:rand.uniform(9_999_999)}")
    File.mkdir_p!(dir)
    flush_ms = Keyword.get(opts, :flush_interval_ms, 1)

    name = :"async_io_test_#{:erlang.unique_integer([:positive])}"

    build_opts =
      [
        data_dir: dir,
        shard_count: 1
      ]
      |> maybe_put_opt(:blob_side_channel_threshold_bytes, opts)
      |> maybe_put_opt(:hot_cache_max_value_size, opts)

    ctx =
      FerricStore.Instance.build(name, build_opts)

    Ferricstore.DataDir.ensure_layout!(dir, 1)

    {:ok, pid} =
      Shard.start_link(
        index: 0,
        data_dir: dir,
        flush_interval_ms: flush_ms,
        instance_ctx: ctx
      )

    {pid, 0, dir, ctx}
  end

  defp maybe_put_opt(build_opts, key, opts) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> Keyword.put(build_opts, key, value)
      :error -> build_opts
    end
  end

  defp restart_shard(dir, ctx, flush_ms) do
    {:ok, pid} =
      Shard.start_link(
        index: 0,
        data_dir: dir,
        flush_interval_ms: flush_ms,
        instance_ctx: ctx
      )

    pid
  end

  defp cleanup_shard(pid, ctx, dir) do
    try do
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 5000)
    catch
      :exit, _ -> :ok
    end

    try do
      FerricStore.Instance.cleanup(ctx.name)
    catch
      :exit, _ -> :ok
    end

    File.rm_rf(dir)
  end

  defp force_rotate_active_file(pid) do
    :sys.replace_state(pid, fn state ->
      new_id = state.active_file_id + 1
      sp = state.shard_data_path
      new_path = Ferricstore.Store.Shard.ETS.file_path(sp, new_id)

      Ferricstore.FS.touch!(new_path)

      if ctx = Map.get(state, :instance_ctx) do
        Ferricstore.Store.ActiveFile.publish(ctx, state.index, new_id, new_path, sp)
      end

      %{
        state
        | active_file_id: new_id,
          active_file_path: new_path,
          active_file_size: 0,
          file_stats: Map.put(state.file_stats, new_id, {0, 0})
      }
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # v2_append_batch_nosync NIF
  # ---------------------------------------------------------------------------

  use Ferricstore.Store.ShardAsyncIoTest.Sections.V2AppendBatchNosyncNif
  use Ferricstore.Store.ShardAsyncIoTest.Sections.ConcurrentWrites
  use Ferricstore.Store.ShardAsyncIoTest.Sections.SharedLogCompaction
  use Ferricstore.Store.ShardAsyncIoTest.Sections.FileSizeAccounting
end
