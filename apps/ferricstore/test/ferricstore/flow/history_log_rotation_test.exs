defmodule Ferricstore.Flow.HistoryLogRotationTest do
  @moduledoc """
  The Flow history projector rolls over to a new log file once the active one
  reaches its size cap, so no single file grows without bound. Reads and
  recovery follow events into every file.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 180_000

  alias Ferricstore.Flow.{HistoryProjector, HistoryRecoveryCheckpoint, LMDB}
  alias Ferricstore.Test.ShardHelpers

  @max_bytes 4_096
  # One partition, so every Flow's history goes to the same shard.
  @partition "history-rotation"

  setup do
    previous = Application.fetch_env(:ferricstore, :flow_history_log_max_bytes)
    Application.put_env(:ferricstore, :flow_history_log_max_bytes, @max_bytes)
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, :flow_history_log_max_bytes, value)
        :error -> Application.delete_env(:ferricstore, :flow_history_log_max_bytes)
      end

      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    {:ok, isolated: isolated, unique: System.unique_integer([:positive])}
  end

  test "the history log rolls over at its cap and every file's events read back",
       %{unique: unique} do
    ids = create_flows(unique, 60)
    ctx = flush_history!()

    files = history_files(ctx)
    assert files != %{}, "expected several history files, got #{inspect(files)}"

    for {_shard, shard_files} <- files,
        {_id, size} <- Enum.drop(shard_files, -1),
        do: assert(size >= @max_bytes)

    assert_history_readable!(ids)
  end

  test "a restart recovers events from every history file",
       %{isolated: isolated, unique: unique} do
    ids = create_flows(unique, 60)
    ctx = flush_history!()
    assert history_files(ctx) != %{}

    :ok = restart_as_new_process(isolated, ctx.shard_count)
    assert_history_readable!(ids)

    more = create_flows(unique + 1, 10)
    flush_history!()
    assert_history_readable!(ids ++ more)
  end

  test "rotation moves the recovery checkpoint to the new file", %{unique: unique} do
    create_flows(unique, 60)
    ctx = flush_history!()
    files = history_files(ctx)
    assert files != %{}

    for {shard, shard_files} <- files do
      shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard)
      {active_id, _size} = List.last(shard_files)

      assert {:ok, marker} =
               LMDB.get(LMDB.path(shard_data_path), HistoryRecoveryCheckpoint.key())

      assert {:ok, {^active_id, _offset, _projected, _digest}} =
               HistoryRecoveryCheckpoint.decode(marker)
    end
  end

  test "a restart without a checkpoint replays every history file",
       %{isolated: isolated, unique: unique} do
    ids = create_flows(unique, 60)
    ctx = flush_history!()
    files = history_files(ctx)
    assert files != %{}

    for shard <- 0..(ctx.shard_count - 1) do
      path = ctx.data_dir |> Ferricstore.DataDir.shard_data_path(shard) |> LMDB.path()
      assert :ok = LMDB.write_batch(path, [{:delete, HistoryRecoveryCheckpoint.key()}])
    end

    :ok = restart_as_new_process(isolated, ctx.shard_count)
    assert_history_readable!(ids)
  end

  defp create_flows(unique, count) do
    # Each create is projected on its own, so the projector appends one small
    # batch at a time and rolls over between them.
    for i <- 1..count do
      id = "rot-#{unique}-#{i}"
      Ferricstore.Flow.Admission.clear_create_pause()

      assert :ok =
               FerricStore.flow_create(id,
                 type: "rot",
                 partition_key: @partition,
                 payload: String.duplicate("x", 200)
               )

      flush_history!()
      id
    end
  end

  defp flush_history! do
    ctx = FerricStore.Instance.get(:default)

    for shard <- 0..(ctx.shard_count - 1),
        do: assert(:ok = HistoryProjector.flush(ctx, shard, 30_000))

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
    ctx
  end

  # %{shard => [{file_id, size}]} for shards with more than one history file.
  defp history_files(ctx) do
    for shard <- 0..(ctx.shard_count - 1),
        dir =
          HistoryProjector.history_dir(Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard)),
        {:ok, names} <- [File.ls(dir)],
        files =
          names
          |> Enum.flat_map(fn name ->
            with ".log" <- Path.extname(name),
                 {id, ""} <- Integer.parse(Path.rootname(name)) do
              [{id, File.stat!(Path.join(dir, name)).size}]
            else
              _other -> []
            end
          end)
          |> Enum.sort(),
        length(files) > 1,
        into: %{},
        do: {shard, files}
  end

  defp assert_history_readable!(ids) do
    for id <- ids do
      assert {:ok, [{_event_id, %{"event" => "created"}} | _]} =
               FerricStore.flow_history(id,
                 partition_key: @partition,
                 include_cold: true,
                 consistent_projection: false,
                 count: 10
               ),
             "#{id} lost its history"
    end
  end

  # A real process restart does not inherit VM-local LMDB writer generations,
  # so startup must not run the in-VM "writer restarted" dirty reconcile.
  defp restart_as_new_process(%{tmp_dir: data_dir, server_started?: server_started?}, shards) do
    for app <- [:ferricstore_server, :ferricstore],
        Enum.any?(Application.started_applications(), &(elem(&1, 0) == app)) do
      _ = Application.stop(app)
    end

    Ferricstore.Raft.WARaftBackend.stop()

    for shard_index <- 0..(shards - 1) do
      :persistent_term.erase(
        Ferricstore.Flow.LMDBWriter.Registry.enqueue_seq_key(:default, shard_index)
      )
    end

    Application.put_env(:ferricstore, :data_dir, data_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ShardHelpers.wait_shards_alive(30_000)

    if server_started? do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    end

    Ferricstore.Health.set_ready(true)
    :ok
  end
end
