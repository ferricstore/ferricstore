defmodule Ferricstore.Flow.ScheduleRetentionMembersTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.Internal
  alias Ferricstore.Flow.Schedule
  alias Ferricstore.Test.ShardHelpers

  # A schedule is a Flow that never finishes. Every fire rewrites its record, and
  # with it the payload under a new version and a retention cleanup member for
  # it. Members used to be read only when a finished Flow was pruned, so each
  # fire grew the keydir for good.

  setup do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    previous = Application.get_env(:ferricstore, :flow_schedule_history_max_events)

    on_exit(fn ->
      restore_env(:flow_schedule_history_max_events, previous)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"

  defp create_schedule(schedule_id, opts \\ []) do
    assert {:ok, _schedule} =
             FerricStore.flow_schedule_create(
               schedule_id,
               [
                 kind: :interval,
                 start_at_ms: 2_000,
                 every_ms: 500,
                 now_ms: 1_900,
                 target: [
                   id_prefix: unique("schedule-target"),
                   type: unique("schedule-type"),
                   state: "queued",
                   partition_key: unique("schedule-partition"),
                   payload: "tick"
                 ]
               ] ++ opts
             )
  end

  defp fire(fires) do
    for fire <- fires do
      assert {:ok, %{fired: 1}} =
               FerricStore.flow_schedule_fire_due(
                 now_ms: 2_000 + fire * 500,
                 worker: "members-test"
               )
    end
  end

  test "a recurring schedule keeps a bounded number of retention cleanup members" do
    Application.put_env(:ferricstore, :flow_schedule_history_max_events, 8)
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    schedule_id = unique("schedule-members")
    create_schedule(schedule_id)
    fire(0..29)

    assert {:ok, %{fire_count: 30}} = FerricStore.flow_schedule_get(schedule_id)

    ShardHelpers.eventually(
      fn -> length(cleanup_members(Schedule.flow_id(schedule_id))) <= 8 end,
      "#{length(cleanup_members(Schedule.flow_id(schedule_id)))} cleanup members after 30 fires",
      100,
      100
    )

    fire(30..31)
    assert {:ok, %{fire_count: 32}} = FerricStore.flow_schedule_get(schedule_id)
  end

  test "a new schedule's Flow keeps a short history" do
    schedule_id = unique("schedule-history")
    create_schedule(schedule_id)

    assert %{history_max_events: 100} = schedule_record(schedule_id)
    assert Schedule.history_max_events() == 100
  end

  test "a schedule created with a long history takes the short one on its next fire" do
    Application.put_env(:ferricstore, :flow_schedule_history_max_events, 100_000)
    schedule_id = unique("schedule-shrink")
    create_schedule(schedule_id)
    assert %{history_max_events: 100_000} = schedule_record(schedule_id)

    Application.put_env(:ferricstore, :flow_schedule_history_max_events, 100)
    fire([0])
    assert %{history_max_events: 100_000} = schedule_record(schedule_id)

    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)
    fire([1])
    assert %{history_max_events: 100} = schedule_record(schedule_id)
  end

  test "a long-history schedule held back by overlap keeps a bounded number of retention cleanup members" do
    Application.put_env(:ferricstore, :flow_schedule_history_max_events, 100_000)
    schedule_id = unique("schedule-overlap-members")
    create_schedule(schedule_id, overlap_policy: :queue_after_previous)
    fire([0])

    Application.put_env(:ferricstore, :flow_schedule_history_max_events, 8)
    assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)

    # The target is never claimed, so every due time only records the overlap.
    for check <- 1..30 do
      assert {:ok, _result} =
               FerricStore.flow_schedule_fire_due(
                 now_ms: 2_000 + check * 60_000,
                 worker: "members-test"
               )
    end

    assert {:ok, %{fire_count: 1, version: version}} = FerricStore.flow_schedule_get(schedule_id)
    assert version > 30

    ShardHelpers.eventually(
      fn -> length(cleanup_members(Schedule.flow_id(schedule_id))) <= 8 end,
      "#{length(cleanup_members(Schedule.flow_id(schedule_id)))} cleanup members after 30 overlap checks",
      100,
      100
    )
  end

  # A schedule whose previous target never finishes does not fire again: each due
  # time it records the overlap and reschedules itself, rewriting its record. It
  # must take the short history on that path too, or it keeps every version.
  for policy <- [:queue_after_previous, :skip] do
    test "a long-history schedule held back by overlap (#{policy}) takes the short history" do
      Application.put_env(:ferricstore, :flow_schedule_history_max_events, 100_000)
      schedule_id = unique("schedule-overlap")
      create_schedule(schedule_id, overlap_policy: unquote(policy))
      fire([0])

      Application.put_env(:ferricstore, :flow_schedule_history_max_events, 100)
      assert {:ok, %{active: 2}} = FerricStore.raise_cluster_feature_level(2)

      # The first target is still queued: nothing claimed it.
      assert {:ok, _result} =
               FerricStore.flow_schedule_fire_due(now_ms: 2_500, worker: "members-test")

      assert {:ok, %{fire_count: 1, last_overlap_reason: "previous target still active: " <> _}} =
               FerricStore.flow_schedule_get(schedule_id)

      assert %{history_max_events: 100} = schedule_record(schedule_id)
    end
  end

  defp schedule_record(schedule_id) do
    {:ok, record} =
      Ferricstore.Flow.get(
        FerricStore.Instance.get(:default),
        Schedule.flow_id(schedule_id),
        Internal.put(partition_key: Schedule.partition_key(schedule_id))
      )

    record
  end

  # Every keydir row of the Flow's retention cleanup members, deleted ones included:
  # a row left behind takes keydir memory whether or not it is live.
  defp cleanup_members(flow_id) do
    marker = "}:rtm:" <> flow_id <> ":"

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
