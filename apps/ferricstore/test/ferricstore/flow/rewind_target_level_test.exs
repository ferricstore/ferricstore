defmodule Ferricstore.Flow.RewindTargetLevelTest do
  @moduledoc """
  The history projector writes and trims a Flow's history outside Raft, so how
  much of it a node holds depends on how far its projector has got. From
  feature level 5 a rewind reads its target event before it is proposed, and
  apply decides from the replicated record, so every replica decides alike.
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

  test "a rewind applies to an event this node's projector has not reached yet" do
    {id, projector} = waiting_flow(10)
    :sys.suspend(projector)

    try do
      signal(id, 1)
      # The rewind is under way while the projector is still held.
      spawn(fn ->
        Process.sleep(200)
        :sys.resume(projector)
      end)

      assert :ok = FerricStore.flow_rewind(id, to_event: "1001-2", now_ms: 9_000)
    after
      :sys.resume(projector)
    end

    assert {:ok, %{version: 3}} = FerricStore.flow_get(id)
  end

  test "apply refuses an event past the history cap that this node still holds" do
    {id, projector} = waiting_flow(3)
    ctx = FerricStore.Instance.get(:default)
    assert {:ok, entry} = Ferricstore.Flow.RewindTarget.read_entry(ctx, id, nil, "1000-1")

    # Three more events put the created event past the cap of three; with the
    # projector held, this node has not trimmed it yet.
    :sys.suspend(projector)

    try do
      for n <- 1..3, do: signal(id, n)

      attrs = %{
        id: id,
        to_event: "1000-1",
        expected_version: nil,
        expect_state: nil,
        run_at_ms: nil,
        reason_ref: nil,
        partition_key: nil,
        now_ms: 9_000,
        target_entry: entry
      }

      assert {:error, "ERR flow rewind target event not found"} =
               Router.flow_rewind(ctx, attrs)
    after
      :sys.resume(projector)
    end
  end

  test "a rewind restores a cold event of a partitioned Flow" do
    ctx = FerricStore.Instance.get(:default)
    id = "rewind-cold-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "rewind-cold",
               state: "waiting",
               partition_key: "tenant-cold",
               attributes: %{"phase" => "created"},
               history_hot_max_events: 1,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    for n <- 1..3 do
      assert :ok =
               FerricStore.flow_signal(id,
                 partition_key: "tenant-cold",
                 signal: "ping",
                 idempotency_key: "cold-#{n}",
                 values: %{"value-#{n}" => "#{n}"},
                 if_state: if(n == 1, do: "waiting", else: "step-#{n - 1}"),
                 transition_to: "step-#{n}",
                 now_ms: 1_000 + n
               )
    end

    shard_index = Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(id, "tenant-cold"))
    assert :ok = HistoryProjector.flush(ctx, shard_index)

    assert :ok =
             FerricStore.flow_rewind(id,
               partition_key: "tenant-cold",
               to_event: "1000-1",
               now_ms: 9_000
             )

    assert {:ok, %{state: "waiting", version: 5, attributes: %{"phase" => "created"}}} =
             FerricStore.flow_get(id, partition_key: "tenant-cold")
  end

  defp waiting_flow(history_max_events) do
    ctx = FerricStore.Instance.get(:default)
    id = "rewind-target-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "rewind-target",
               state: "waiting",
               history_max_events: history_max_events,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    shard_index = Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(id, nil))
    assert :ok = HistoryProjector.flush(ctx, shard_index)
    {id, Process.whereis(HistoryProjector.name(ctx, shard_index))}
  end

  # Event "<1000 + n>-<n + 1>".
  defp signal(id, n) do
    assert :ok =
             FerricStore.flow_signal(id,
               signal: "ping",
               idempotency_key: "signal-#{n}",
               values: %{"value-#{n}" => "#{n}"},
               now_ms: 1_000 + n
             )
  end
end
