defmodule Ferricstore.FlowStatsTest do
  use Ferricstore.Test.FlowCase

  @partition "stats-partition"

  setup do
    with_flow_max_count(2)
  end

  test "stats counts active workflows beyond list count cap" do
    type = unique_flow_id("stats-hot-type")

    create_queued(type, "acme")
    create_queued(type, "acme")
    create_queued(type, "acme")

    assert {:error, "ERR flow count exceeds maximum 2"} =
             FerricStore.flow_list(type,
               state: "queued",
               partition_key: @partition,
               count: 3
             )

    assert {:ok, stats} =
             FerricStore.flow_stats(type,
               state: "queued",
               partition_key: @partition
             )

    assert stats.count == 3
    assert stats.attributes == %{}
  end

  test "stats counts indexed attribute candidates beyond list count cap" do
    type = unique_flow_id("stats-attr-type")

    create_queued(type, "acme")
    create_queued(type, "acme")
    create_queued(type, "acme")
    create_queued(type, "other")

    assert {:ok, stats} =
             FerricStore.flow_stats(type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true
             )

    assert stats.count == 3
    assert stats.attributes == %{"tenant" => "acme"}
  end

  test "attribute stats ignore expired terminal projection rows" do
    type = unique_flow_id("stats-expired-attr-type")
    id = create_queued(type, "acme", retention_ttl_ms: 1)
    claimed = claim_one!(type)

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               fencing_token: claimed.fencing_token,
               partition_key: @partition,
               now_ms: 1_002
             )

    assert {:ok, stats} =
             FerricStore.flow_stats(type,
               state: "completed",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true
             )

    assert stats.count == 0
  end

  test "attribute stats fail closed when exact scan would exceed limit" do
    with_flow_stats_attribute_scan_limit(2)
    type = unique_flow_id("stats-attr-limit-type")

    create_queued(type, "acme")
    create_queued(type, "acme")
    create_queued(type, "acme")

    assert {:error, "ERR flow stats exact attribute count exceeds scan limit 2"} =
             FerricStore.flow_stats(type,
               state: "queued",
               partition_key: @partition,
               attributes: %{"tenant" => "acme"},
               consistent_projection: true
             )
  end

  defp create_queued(type, tenant, opts \\ []) do
    id = unique_flow_id("stats-record")

    flow_opts =
      Keyword.merge(
        [
          type: type,
          state: "queued",
          partition_key: @partition,
          attributes: %{"tenant" => tenant},
          run_at_ms: 1_000,
          now_ms: 1_000
        ],
        opts
      )

    assert :ok = FerricStore.flow_create(id, flow_opts)

    id
  end

  defp claim_one!(type) do
    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: @partition,
               worker: "stats-worker",
               limit: 1,
               now_ms: 1_001
             )

    claimed
  end

  defp with_flow_max_count(value) do
    with_env(:flow_max_count, value)
  end

  defp with_flow_stats_attribute_scan_limit(value) do
    with_env(:flow_stats_attribute_scan_limit, value)
  end

  defp with_env(key, value) do
    previous = Application.get_env(:ferricstore, key, :unset)
    Application.put_env(:ferricstore, key, value)

    on_exit(fn ->
      case previous do
        :unset -> Application.delete_env(:ferricstore, key)
        value -> Application.put_env(:ferricstore, key, value)
      end
    end)

    :ok
  end
end
