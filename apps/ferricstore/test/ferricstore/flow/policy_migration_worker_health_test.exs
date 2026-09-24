defmodule Ferricstore.Flow.PolicyMigrationWorkerHealthTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.PolicyMigrationWorker
  import ExUnit.CaptureLog

  test "logs a recurring projection failure once until it recovers" do
    name = :"policy_migration_log_#{System.unique_integer([:positive])}"
    ctx = %{name: name, shard_count: 1}
    {:ok, result} = Agent.start_link(fn -> {:error, :policy_catalog_state_projection_pending} end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: name,
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               projection_retry_min_ms: 1,
               shards_per_run: 1,
               attribute_repair_fun: fn _ctx, _shard -> Agent.get(result, & &1) end
             )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      if Process.alive?(result), do: Agent.stop(result)
    end)

    logs =
      capture_log(fn ->
        send(pid, :run)
        :sys.get_state(pid)
        Process.sleep(3)
        send(pid, :run)
        :sys.get_state(pid)
      end)

    assert length(Regex.scan(~r/policy_catalog_state_projection_pending/, logs)) == 1

    assert %{issues: [%{occurrences: 2}]} = PolicyMigrationWorker.health_snapshot(name)

    Agent.update(result, fn _ -> {:ok, %{processed: 1}} end)
    Process.sleep(3)
    send(pid, :run)
    :sys.get_state(pid)
    assert %{status: :healthy, issues: []} = PolicyMigrationWorker.health_snapshot(name)

    Agent.update(result, fn _ -> {:error, :policy_catalog_state_projection_pending} end)

    assert capture_log(fn ->
             send(pid, :run)
             :sys.get_state(pid)
           end) =~ "policy_catalog_state_projection_pending"
  end

  test "publishes and clears bounded typed shard failures" do
    name = :"policy_migration_health_#{System.unique_integer([:positive])}"
    ctx = %{name: name, shard_count: 1}
    {:ok, result} = Agent.start_link(fn -> {:error, :policy_catalog_state_projection_pending} end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: name,
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               projection_retry_min_ms: 1,
               shards_per_run: 1,
               attribute_repair_fun: fn _ctx, _shard -> Agent.get(result, & &1) end
             )

    send(pid, :run)
    :sys.get_state(pid)

    assert %{
             status: :warning,
             issues: [
               %{
                 shard: 0,
                 reason: :policy_catalog_state_projection_pending,
                 occurrences: 1
               }
             ]
           } = PolicyMigrationWorker.health_snapshot(name)

    Agent.update(result, fn _ -> {:ok, %{processed: 1}} end)
    Process.sleep(3)
    send(pid, :run)
    :sys.get_state(pid)

    assert %{status: :healthy, issues: [], updated_at_ms: published_at_ms} =
             PolicyMigrationWorker.health_snapshot(name)

    Process.sleep(2)
    send(pid, :run)
    :sys.get_state(pid)

    assert %{status: :healthy, issues: [], updated_at_ms: ^published_at_ms} =
             PolicyMigrationWorker.health_snapshot(name)
  end

  test "backs off a persistently pending shard while another shard catches up" do
    name = :"policy_migration_cooldown_#{System.unique_integer([:positive])}"
    ctx = %{name: name, shard_count: 2}
    {:ok, calls} = Agent.start_link(fn -> %{0 => 0, 1 => 0} end)

    assert {:ok, pid} =
             PolicyMigrationWorker.start_link(
               instance_ctx: ctx,
               name: name,
               enabled: true,
               initial_delay_ms: 60_000,
               interval_ms: 60_000,
               catchup_delay_ms: 60_000,
               projection_retry_min_ms: 20,
               shards_per_run: 2,
               attribute_repair_fun: fn _ctx, shard ->
                 Agent.get_and_update(calls, fn counts ->
                   result =
                     if shard == 0,
                       do: {:ok, %{processed: 1}},
                       else: {:error, :policy_catalog_state_projection_pending}

                   {result, Map.update!(counts, shard, &(&1 + 1))}
                 end)
               end
             )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      if Process.alive?(calls), do: Agent.stop(calls)
    end)

    for _ <- 1..3 do
      send(pid, :run)
      :sys.get_state(pid)
    end

    assert %{0 => 3, 1 => 1} = Agent.get(calls, & &1)

    assert %{issues: [%{shard: 1, occurrences: 1}]} =
             PolicyMigrationWorker.health_snapshot(name)

    Process.sleep(25)
    send(pid, :run)
    :sys.get_state(pid)

    assert %{0 => 4, 1 => 2} = Agent.get(calls, & &1)

    assert %{issues: [%{shard: 1, occurrences: 2}]} =
             PolicyMigrationWorker.health_snapshot(name)
  end
end
