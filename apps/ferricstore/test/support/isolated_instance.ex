defmodule Ferricstore.Test.IsolatedInstance do
  @moduledoc """
  Creates an isolated FerricStore instance for test isolation.

  Each call to `checkout/0` creates a fresh instance with its own:
  - Temp data directory
  - ETS keydir tables (anonymous)
  - Atomics/counters refs
  - Local direct shard processes

  Usage:

      setup do
        ctx = Ferricstore.Test.IsolatedInstance.checkout()
        on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)
        {:ok, ctx: ctx}
      end

      test "isolated test", %{ctx: ctx} do
        FerricStore.Impl.set(ctx, "key", "value")
        assert {:ok, "value"} = FerricStore.Impl.get(ctx, "key")
      end
  """

  @lmdb_release_timeout_ms 30_000

  @doc """
  Creates a new isolated instance. Returns the ctx struct.
  """
  def checkout(opts \\ []) do
    name = :"test_instance_#{:erlang.unique_integer([:positive])}"
    tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_isolated_#{name}")
    File.mkdir_p!(tmp_dir)

    shard_count = Keyword.get(opts, :shard_count, 2)

    instance_opts =
      [
        data_dir: tmp_dir,
        shard_count: shard_count,
        max_memory_bytes: Keyword.get(opts, :max_memory_bytes, 256 * 1024 * 1024),
        keydir_max_ram: Keyword.get(opts, :keydir_max_ram, 64 * 1024 * 1024),
        eviction_policy: Keyword.get(opts, :eviction_policy, :volatile_lfu),
        hot_cache_max_value_size: Keyword.get(opts, :hot_cache_max_value_size, 65_536),
        blob_side_channel_threshold_bytes:
          Keyword.get(opts, :blob_side_channel_threshold_bytes, 0),
        promotion_threshold:
          Keyword.get(
            opts,
            :promotion_threshold,
            Application.get_env(:ferricstore, :promotion_threshold, 100)
          ),
        max_active_file_size: 64 * 1024 * 1024,
        read_sample_rate: Keyword.get(opts, :read_sample_rate, 1),
        lfu_decay_time: 1,
        lfu_log_factor: 10
      ]
      |> Keyword.merge(
        Keyword.take(opts, [
          :flow_metadata_extension,
          :flow_tenancy_mode,
          :query_engine,
          :query_index_provider
        ])
      )

    ctx = FerricStore.Instance.build(name, instance_opts)

    # Ensure data dir layout (ETS tables created by Shard.init)
    Ferricstore.DataDir.ensure_layout!(tmp_dir, shard_count)
    :ok = Ferricstore.Flow.LMDB.ensure_shard_dirs(tmp_dir, shard_count)

    for i <- 0..(shard_count - 1) do
      {:ok, _pid} =
        Ferricstore.Flow.LMDBWriter.start_link(
          shard_index: i,
          data_dir: tmp_dir,
          instance_ctx: ctx
        )

      {:ok, _pid} =
        Ferricstore.Flow.HistoryProjector.start_link(
          shard_index: i,
          shard_data_path: Ferricstore.DataDir.shard_data_path(tmp_dir, i),
          instance_ctx: ctx
        )
    end

    if Keyword.get(opts, :start_shards, true) do
      # Custom instance shards are local/direct; only the default application
      # instance owns Raft.
      for i <- 0..(shard_count - 1) do
        {:ok, _pid} =
          Ferricstore.Store.Shard.start_link(
            index: i,
            data_dir: tmp_dir,
            instance_ctx: ctx
          )
      end

      # Wait for all shards to be alive AND ready to accept calls.
      # Process.alive? alone isn't enough — the shard's init (keydir recovery,
      # ETS table creation) must complete before GenServer.call works.
      Enum.each(0..(shard_count - 1), fn i ->
        shard = elem(ctx.shard_names, i)

        Ferricstore.Test.ShardHelpers.eventually(
          fn ->
            pid = Process.whereis(shard)

            is_pid(pid) and Process.alive?(pid) and
              match?(
                {:ok, _},
                try do
                  {:ok, GenServer.call(shard, :shard_stats, 500)}
                rescue
                  _ -> :error
                catch
                  :exit, _ -> :error
                end
              )
          end,
          "shard #{i} not ready",
          50,
          20
        )
      end)
    end

    ctx
  end

  @doc """
  Cleans up an isolated instance.
  """
  def checkin(%FerricStore.Instance{} = ctx) do
    # Stop shard processes
    for i <- 0..(ctx.shard_count - 1) do
      name = elem(ctx.shard_names, i)

      case Process.whereis(name) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid, :normal, 5000)
          catch
            :exit, _ -> :ok
          end
      end
    end

    Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    for i <- 0..(ctx.shard_count - 1) do
      _ = Ferricstore.Flow.HistoryProjector.flush(ctx, i, 5_000)

      case Process.whereis(Ferricstore.Flow.HistoryProjector.name(ctx, i)) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid, :normal, 5000)
          catch
            :exit, _ -> :ok
          end
      end
    end

    for i <- 0..(ctx.shard_count - 1) do
      case Process.whereis(Ferricstore.Flow.LMDBWriter.name(ctx.name, i)) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid, :normal, 5000)
          catch
            :exit, _ -> :ok
          end
      end
    end

    for i <- 0..(ctx.shard_count - 1) do
      lmdb_path =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(i)
        |> Ferricstore.Flow.LMDB.path()

      if File.dir?(lmdb_path) do
        :ok = Ferricstore.Flow.LMDB.release(lmdb_path, @lmdb_release_timeout_ms)
      end
    end

    # Delete ETS tables
    for i <- 0..(ctx.shard_count - 1) do
      try do
        :ets.delete(elem(ctx.keydir_refs, i))
      rescue
        _ -> :ok
      end
    end

    try do
      :ets.delete(ctx.hotness_table)
    rescue
      _ -> :ok
    end

    try do
      :ets.delete(ctx.config_table)
    rescue
      _ -> :ok
    end

    # Remove from persistent_term cache
    FerricStore.Instance.cleanup(ctx.name)

    # Clean up temp directory
    File.rm_rf!(ctx.data_dir)

    :ok
  end
end
