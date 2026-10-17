defmodule Ferricstore.Flow.HistoryValueReleaseTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.HistoryValueRelease
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.ValueStore
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # A Flow that keeps running writes its payload under a new version each time it
  # changes, and a retention cleanup member for each of those values. Trimmed
  # history no longer refers to the old ones; a release deletes them and their
  # members, and keeps every value remaining history or the record refers to.

  @partition "history-value-release"

  setup do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, ctx: FerricStore.Instance.get(:default)}
  end

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"

  # Twenty cycles of claim and transition, each with a new payload: payload values
  # at versions 1, 3, ..., 41. History keeps the last six events (versions 36-41),
  # which refer to the payloads of versions 35, 37, 39 and 41.
  defp run_cycles(id, type, create_opts \\ []) do
    assert :ok =
             FerricStore.flow_create(
               id,
               [
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 history_max_events: 6,
                 payload: "payload-0",
                 run_at_ms: 1_000,
                 now_ms: 1_000
               ] ++ create_opts
             )

    for cycle <- 1..20 do
      now = 1_000 + cycle * 10

      assert {:ok, [claimed]} =
               FerricStore.flow_claim_due(type,
                 partition_key: @partition,
                 worker: "w",
                 limit: 1,
                 now_ms: now
               )

      assert :ok =
               FerricStore.flow_transition(id, "running", "queued",
                 lease_token: claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 partition_key: @partition,
                 payload: "payload-#{cycle}",
                 run_at_ms: now + 5,
                 now_ms: now + 1
               )
    end
  end

  # Reads the way history values are read, so a value the history projector moved
  # out of the keydir still counts.
  defp stored_value(ctx, id, version) do
    [value] =
      ValueStore.raw_mget_with_file_refs(
        ctx,
        [Keys.value_key(id, :payload, version, @partition)],
        1_000_000
      )

    value
  end

  defp history_with_values(id),
    do: FerricStore.flow_history(id, partition_key: @partition, count: 100, values: true)

  test "a release deletes the values only trimmed history used, with their cleanup members",
       %{ctx: ctx} do
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    id = unique("release")
    run_cycles(id, "release")

    assert length(cleanup_members(id)) == 21
    assert {:ok, retained} = history_with_values(id)
    assert length(retained) == 6

    assert {:ok, %{released: 17}} = HistoryValueRelease.release(ctx, id, @partition)

    assert length(cleanup_members(id)) == 4
    assert history_with_values(id) == {:ok, retained}

    for version <- [35, 37, 39, 41],
        do: assert(is_binary(stored_value(ctx, id, version)), "version #{version} was deleted")

    for version <- 1..33//2,
        do: assert(stored_value(ctx, id, version) == nil, "version #{version} was kept")

    assert {:ok, %{released: 0}} = HistoryValueRelease.release(ctx, id, @partition)
  end

  test "a release pages through the cleanup members", %{ctx: ctx} do
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    id = unique("release-pages")
    run_cycles(id, "release-pages")

    assert {:ok, %{released: 17}} = HistoryValueRelease.release(ctx, id, @partition, page: 3)
    assert length(cleanup_members(id)) == 4
  end

  test "trimming alone keeps a running Flow's cleanup members bounded" do
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    id = unique("release-background")
    run_cycles(id, "release-background")

    ShardHelpers.eventually(
      fn -> length(cleanup_members(id)) <= 6 end,
      "#{length(cleanup_members(id))} cleanup members stay after trimming",
      100,
      100
    )

    assert {:ok, retained} = history_with_values(id)
    assert Enum.all?(retained, fn {_event_id, fields} -> is_binary(fields["payload"]) end)
  end

  test "pruning a finished Flow deletes its release floor", %{ctx: ctx} do
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    id = unique("release-prune")
    run_cycles(id, "release-prune", retention_ttl_ms: 60_000)
    assert {:ok, %{released: 17}} = HistoryValueRelease.release(ctx, id, @partition)

    floor_key = Keys.history_release_floor_key(id, @partition)
    assert Router.get(ctx, floor_key) == "36"

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("release-prune",
               partition_key: @partition,
               worker: "w",
               limit: 1,
               now_ms: 2_000
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               now_ms: 2_001
             )

    assert {:ok, %{terminal_retention_until_ms: until_ms}} =
             FerricStore.flow_get(id, partition_key: @partition)

    assert {:ok, %{flows: 1}} =
             FerricStore.flow_retention_cleanup(limit: 10, now_ms: until_ms + 1)

    ShardHelpers.eventually(
      fn -> FerricStore.flow_get(id, partition_key: @partition) == {:ok, nil} end,
      "the finished Flow was not pruned"
    )

    assert Router.get(ctx, floor_key) == nil
  end

  test "nothing is released until feature level 2 is active", %{ctx: ctx} do
    id = unique("release-gated")
    run_cycles(id, "release-gated")

    assert {:error, :feature_level_inactive} = HistoryValueRelease.release(ctx, id, @partition)
    assert length(cleanup_members(id)) == 21
  end

  test "a rewound Flow keeps the old value it points at again", %{ctx: ctx} do
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    id = unique("release-rewind")
    run_cycles(id, "release-rewind")

    # Version 37 is the oldest remaining "transitioned" event (payload-18).
    assert {:ok, [_claimed, {event_id, %{"version" => "37"}} | _]} = history_with_values(id)

    assert :ok =
             FerricStore.flow_rewind(id,
               partition_key: @partition,
               to_event: event_id,
               expect_state: "queued",
               now_ms: 5_000
             )

    assert {:ok, %{released: _released}} = HistoryValueRelease.release(ctx, id, @partition)

    assert {:ok, %{payload: "payload-18"}} =
             FerricStore.flow_get(id, partition_key: @partition, payload: true)
  end

  # A release made from history older than the rewind's (a node that trimmed
  # further) may delete a value an event the rewind still sees refers to. The
  # rewind must not bring back a ref to a deleted value.
  test "a rewind cannot restore a value a release deleted", %{ctx: ctx} do
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    id = unique("release-guard")
    run_cycles(id, "release-guard")

    assert {:ok, [_claimed, {event_id, %{"version" => "37"}} | _]} = history_with_values(id)

    assert {:ok, %{released: 20}} =
             Router.flow_history_release_values(ctx, %{
               id: id,
               partition_key: @partition,
               oldest_version: 41,
               keep_refs: [],
               offset: 0,
               limit: 100
             })

    assert {:error, "ERR flow rewind target event not found"} =
             FerricStore.flow_rewind(id,
               partition_key: @partition,
               to_event: event_id,
               expect_state: "queued",
               now_ms: 5_000
             )

    assert {:ok, %{payload: "payload-20"}} =
             FerricStore.flow_get(id, partition_key: @partition, payload: true)
  end

  # Every keydir row of the Flow's retention cleanup members.
  defp cleanup_members(id) do
    marker = "}:rtm:" <> id <> ":"

    0
    |> Stream.iterate(&(&1 + 1))
    |> Stream.map(&:"keydir_#{&1}")
    |> Stream.take_while(&(:ets.info(&1) != :undefined))
    |> Enum.flat_map(fn table ->
      :ets.foldl(
        fn row, keys ->
          key = elem(row, 0)

          if is_binary(key) and :binary.match(key, marker) != :nomatch,
            do: [key | keys],
            else: keys
        end,
        [],
        table
      )
    end)
  end
end
