defmodule Ferricstore.Flow.HistoryLogMergeTest do
  @moduledoc """
  A closed history file whose records are mostly dead is merged: its live
  records move into a file that takes its place in replay order, every
  reference follows them, and the file is deleted.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 300_000

  alias Ferricstore.Flow.{HistoryProjector, HistoryRecoveryCheckpoint, LMDB}
  alias Ferricstore.Flow.HistoryProjector.{Files, Merge}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @max_bytes 8_192
  @partition "history-merge"

  setup do
    previous =
      for key <- [:flow_history_log_max_bytes, :flow_history_merge_delete_grace_ms],
          do: {key, Application.fetch_env(:ferricstore, key)}

    Application.put_env(:ferricstore, :flow_history_log_max_bytes, @max_bytes)
    Application.put_env(:ferricstore, :flow_history_merge_delete_grace_ms, 0)
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      for {key, value} <- previous do
        case value do
          {:ok, value} -> Application.put_env(:ferricstore, key, value)
          :error -> Application.delete_env(:ferricstore, key)
        end
      end

      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    {:ok, isolated: isolated, unique: System.unique_integer([:positive])}
  end

  test "a mostly dead closed file is merged and deleted, and live history still reads",
       %{isolated: isolated, unique: unique} do
    %{keepers: keepers, doomed: doomed, shard: shard, ctx: ctx} = mostly_dead_history(unique)
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard)
    {:ok, before_ids} = Files.list(shard_data_path)
    before_bytes = history_bytes(shard_data_path)
    assert length(before_ids) > 2

    assert {:ok, %{merged: [_ | _] = merged}} = Merge.run(ctx, shard)

    {:ok, after_ids} = Files.list(shard_data_path)

    for {input, output} <- merged do
      refute input in after_ids, "file #{input} should be deleted"
      assert output in after_ids
      assert div(output, Files.slot()) == div(input, Files.slot())
    end

    assert history_bytes(shard_data_path) < before_bytes
    assert_history!(keepers, doomed)

    # Recovery from the checkpoint and a full replay of every file agree.
    :ok = restart_as_new_process(isolated, ctx.shard_count)
    assert_history!(keepers, doomed)

    path = LMDB.path(shard_data_path)
    assert :ok = LMDB.write_batch(path, [{:delete, HistoryRecoveryCheckpoint.key()}])
    :ok = restart_as_new_process(isolated, ctx.shard_count)
    assert_history!(keepers, doomed)
  end

  test "a file with mostly live records is left alone", %{unique: unique} do
    ctx = FerricStore.Instance.get(:default)
    ids = for i <- 1..300, do: create!("live-#{unique}-#{i}", "history-keep", 0)
    flush!(ctx)
    shard = shard_of(ctx, hd(ids))
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard)
    {:ok, before_ids} = Files.list(shard_data_path)
    assert length(before_ids) > 1

    assert {:ok, %{merged: []}} = Merge.run(ctx, shard)
    assert {:ok, ^before_ids} = Files.list(shard_data_path)
    assert_history!(ids, [])
  end

  # Keepers stay waiting; doomed Flows complete with a 1 ms retention and are
  # cleaned up, which leaves their history records in the closed files dead.
  defp mostly_dead_history(unique) do
    ctx = FerricStore.Instance.get(:default)

    {keepers, doomed} =
      1..60
      |> Enum.map(fn i ->
        if rem(i, 10) == 0,
          do: {:keeper, create!("keep-#{unique}-#{i}", "history-keep", 0)},
          else: {:doomed, create!("doom-#{unique}-#{i}", "history-doom", 1)}
      end)
      |> Enum.split_with(&match?({:keeper, _id}, &1))
      |> then(fn {keepers, doomed} ->
        {Enum.map(keepers, &elem(&1, 1)), Enum.map(doomed, &elem(&1, 1))}
      end)

    complete_all!(length(doomed))
    flush!(ctx)
    cleanup!(ctx, doomed)

    # Start a fresh active file so the dead records sit in closed files.
    for i <- 1..30, do: create!("tail-#{unique}-#{i}", "history-keep", 0)
    flush!(ctx)

    %{keepers: keepers, doomed: doomed, shard: shard_of(ctx, hd(keepers)), ctx: ctx}
  end

  defp create!(id, type, retention_ttl_ms) do
    Ferricstore.Flow.Admission.clear_create_pause()

    opts =
      [
        type: type,
        partition_key: @partition,
        payload: String.duplicate("p", 300),
        run_at_ms: 1_000,
        now_ms: 1_000
      ] ++ if(retention_ttl_ms > 0, do: [retention_ttl_ms: retention_ttl_ms], else: [])

    assert :ok = FerricStore.flow_create(id, opts)
    # Projected on its own, so the projector rolls over between creates.
    flush!(FerricStore.Instance.get(:default))
    id
  end

  defp complete_all!(count) do
    assert {:ok, claimed} =
             FerricStore.flow_claim_due("history-doom",
               partition_key: @partition,
               worker: "w",
               limit: count,
               now_ms: 1_000
             )

    assert length(claimed) == count

    for flow <- claimed do
      assert :ok =
               FerricStore.flow_complete(flow.id, flow.lease_token,
                 partition_key: @partition,
                 fencing_token: flow.fencing_token,
                 result: "done",
                 now_ms: 1_100
               )
    end
  end

  # Runs retention passes until one finds nothing and has no more to do. A
  # doomed Flow reads as gone once its retention expires, before retention has
  # removed its history and values, so reads cannot tell when cleanup is done.
  defp cleanup!(ctx, doomed) do
    Enum.reduce_while(1..200, {:cont, nil}, fn attempt, {_status, continuation} ->
      opts =
        [limit: 50, now_ms: 10_000_000_000_000] ++
          if(continuation, do: [continuation: continuation], else: [])

      assert {:ok, %{flows: cleaned, more?: more?} = result} =
               FerricStore.flow_retention_cleanup(opts)

      flush!(ctx)

      cond do
        more? -> {:cont, {:cont, result.continuation}}
        cleaned > 0 -> {:cont, {:cont, nil}}
        true -> {:halt, {:done, nil}}
      end
    end)
    |> case do
      {:done, nil} -> :ok
      _unfinished -> flunk("retention cleanup did not finish")
    end

    for id <- doomed, do: assert({:ok, nil} = FerricStore.flow_get(id, partition_key: @partition))
    :ok
  end

  defp flush!(ctx) do
    for shard <- 0..(ctx.shard_count - 1),
        do: assert(:ok = HistoryProjector.flush(ctx, shard, 30_000))

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
  end

  defp shard_of(ctx, id),
    do: Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(id, @partition))

  defp history_bytes(shard_data_path) do
    {:ok, ids} = Files.list(shard_data_path)
    Enum.reduce(ids, 0, &(&2 + File.stat!(Files.path(shard_data_path, &1)).size))
  end

  defp assert_history!(keepers, doomed) do
    for id <- keepers do
      assert {:ok, [{_event_id, %{"event" => "created"}} | _]} = history(id), "#{id} lost history"
    end

    for id <- doomed, do: assert({:ok, []} = history(id))
  end

  defp history(id) do
    FerricStore.flow_history(id,
      partition_key: @partition,
      include_cold: true,
      consistent_projection: false,
      count: 10
    )
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

defmodule Ferricstore.Flow.HistoryLogBackgroundMergeTest do
  @moduledoc """
  The history projector merges closed history files on its own, on
  `:flow_history_merge_interval_ms`.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 300_000

  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.HistoryProjector.Files
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @partition "history-merge-background"

  setup do
    env = [
      flow_history_log_max_bytes: 8_192,
      flow_history_merge_delete_grace_ms: 0,
      flow_history_merge_interval_ms: 200
    ]

    previous = for {key, _value} <- env, do: {key, Application.fetch_env(:ferricstore, key)}
    for {key, value} <- env, do: Application.put_env(:ferricstore, key, value)
    # The projector reads the interval when it starts.
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      for {key, value} <- previous do
        case value do
          {:ok, value} -> Application.put_env(:ferricstore, key, value)
          :error -> Application.delete_env(:ferricstore, key)
        end
      end

      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    :ok
  end

  test "dead history files are merged without being asked" do
    ctx = FerricStore.Instance.get(:default)
    unique = System.unique_integer([:positive])
    me = self()

    :telemetry.attach(
      "history-merge-#{unique}",
      [:ferricstore, :flow, :history_merge],
      fn _event, measurements, _meta, _config -> send(me, {:history_merge, measurements}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("history-merge-#{unique}") end)

    doomed = for i <- 1..60, do: create!("bg-doom-#{unique}-#{i}", "bg-doom", 1)
    keepers = for i <- 1..5, do: create!("bg-keep-#{unique}-#{i}", "bg-keep", 0)

    assert {:ok, claimed} =
             FerricStore.flow_claim_due("bg-doom",
               partition_key: @partition,
               worker: "w",
               limit: length(doomed),
               now_ms: 1_000
             )

    for flow <- claimed do
      assert :ok =
               FerricStore.flow_complete(flow.id, flow.lease_token,
                 partition_key: @partition,
                 fencing_token: flow.fencing_token,
                 result: "done",
                 now_ms: 1_100
               )
    end

    flush!(ctx)

    cleanup!(ctx, doomed)

    for i <- 1..30, do: create!("bg-tail-#{unique}-#{i}", "bg-keep", 0)

    flush!(ctx)

    shard = Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(hd(keepers), @partition))
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 {:ok, file_ids} = Files.list(shard_data_path)
                 0 not in file_ids
               end,
               "the projector should merge away the oldest history file",
               100,
               100
             )

    assert_received {:history_merge, %{merged: merged, deleted: deleted}}
                    when merged + deleted > 0

    for id <- keepers do
      assert {:ok, [{_event_id, %{"event" => "created"}} | _]} =
               FerricStore.flow_history(id,
                 partition_key: @partition,
                 include_cold: true,
                 consistent_projection: false,
                 count: 10
               )
    end
  end

  # Runs retention passes until one finds nothing and has no more to do. A
  # doomed Flow reads as gone once its retention expires, before retention has
  # removed its history and values, so reads cannot tell when cleanup is done.
  defp cleanup!(ctx, doomed) do
    Enum.reduce_while(1..200, {:cont, nil}, fn attempt, {_status, continuation} ->
      opts =
        [limit: 50, now_ms: 10_000_000_000_000] ++
          if(continuation, do: [continuation: continuation], else: [])

      assert {:ok, %{flows: cleaned, more?: more?} = result} =
               FerricStore.flow_retention_cleanup(opts)

      flush!(ctx)

      cond do
        more? -> {:cont, {:cont, result.continuation}}
        cleaned > 0 -> {:cont, {:cont, nil}}
        true -> {:halt, {:done, nil}}
      end
    end)
    |> case do
      {:done, nil} -> :ok
      _unfinished -> flunk("retention cleanup did not finish")
    end

    for id <- doomed, do: assert({:ok, nil} = FerricStore.flow_get(id, partition_key: @partition))
    :ok
  end

  defp create!(id, type, retention_ttl_ms) do
    Ferricstore.Flow.Admission.clear_create_pause()

    opts =
      [
        type: type,
        partition_key: @partition,
        payload: String.duplicate("p", 300),
        run_at_ms: 1_000,
        now_ms: 1_000
      ] ++ if(retention_ttl_ms > 0, do: [retention_ttl_ms: retention_ttl_ms], else: [])

    assert :ok = FerricStore.flow_create(id, opts)
    # Projected on its own, so the projector rolls over between creates.
    flush!(FerricStore.Instance.get(:default))
    id
  end

  defp flush!(ctx) do
    for shard <- 0..(ctx.shard_count - 1),
        do: assert(:ok = HistoryProjector.flush(ctx, shard, 30_000))

    assert :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
  end
end
