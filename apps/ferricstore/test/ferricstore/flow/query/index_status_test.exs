defmodule Ferricstore.Flow.Query.IndexStatusTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.MetadataExtension

  alias Ferricstore.Flow.Query.{
    IndexRegistry,
    IndexStatistics,
    IndexStatus,
    StatisticsStore
  }

  setup do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_query_status_#{suffix}")
    name = :"query_status_instance_#{suffix}"

    {:ok, metadata_snapshot} =
      MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    ctx = %{
      name: name,
      data_dir: data_dir,
      shard_count: 1,
      flow_metadata_snapshot: metadata_snapshot
    }

    start_supervised!({IndexRegistry, instance_ctx: ctx, name: IndexRegistry.server_name(ctx)})

    start_supervised!(
      {StatisticsStore, instance_ctx: ctx, name: StatisticsStore.server_name(ctx), max_entries: 8}
    )

    on_exit(fn -> File.rm_rf!(data_dir) end)
    %{ctx: ctx}
  end

  test "returns one actionable catalog, progress, service, and freshness view", %{ctx: ctx} do
    now_ms = 1_000_000
    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)
    [registered | _rest] = snapshot.indexes

    assert :ok =
             StatisticsStore.put(
               StatisticsStore.server_name(ctx),
               statistic(registered, "tenant-secret", now_ms - 25)
             )

    assert {:ok, status} = IndexStatus.fetch(ctx, nil, now_ms: now_ms)

    assert status["contract_version"] == "ferric.flow.query.indexes/v1"
    assert status["observed_at_ms"] == now_ms
    assert status["registry"] == %{"catalog_version" => 3, "epoch" => 1}
    assert status["services"]["registry"] == "ready"
    assert status["services"]["statistics_store"] == "ready"
    assert status["services"]["lifecycle_worker"] == "unavailable"
    assert status["services"]["statistics_worker"] == "unavailable"
    assert status["statistics_max_age_ms"] == IndexStatistics.max_age_ms()

    index =
      Enum.find(status["indexes"], fn index ->
        index["id"] == registered.definition.id and
          index["version"] == registered.definition.version
      end)

    assert index["build_id"] == registered.build_id
    assert index["state"] == "building"
    refute index["queryable"]
    assert index["fields"] != []
    assert index["build"]["scope"] == "catalog_build"
    assert index["build"]["current_phases"] == ["pending"]
    assert index["validation"]["scope"] == "catalog_build"
    assert index["validation"]["status"] == "pending"
    assert index["retirement"] == %{"status" => "not_applicable"}

    assert index["statistics"] == %{
             "fresh_samples" => 1,
             "future_samples" => 0,
             "newest_age_ms" => 25,
             "newest_collected_at_ms" => now_ms - 25,
             "oldest_age_ms" => 25,
             "oldest_collected_at_ms" => now_ms - 25,
             "samples" => 1,
             "stale_samples" => 0,
             "status" => "fresh"
           }

    encoded = inspect(status)
    refute encoded =~ "tenant-secret"
    refute encoded =~ "cursor"
    refute encoded =~ Base.encode16(IndexStatistics.scope_digest("tenant-secret"))
  end

  test "filters by index id and reports an unknown generation", %{ctx: ctx} do
    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)
    target = hd(snapshot.indexes)

    assert {:ok, %{"indexes" => indexes}} = IndexStatus.fetch(ctx, target.definition.id)
    assert indexes != []
    assert Enum.all?(indexes, &(&1["id"] == target.definition.id))

    assert {:error, :query_index_not_found} = IndexStatus.fetch(ctx, "missing-index")
  end

  test "accepts a registry pid as a server reference", %{ctx: ctx} do
    registry = Process.whereis(IndexRegistry.server_name(ctx))

    assert {:ok, status} = IndexStatus.fetch(ctx, nil, registry: registry)
    assert status["services"]["registry"] == "ready"
  end

  test "classifies exact freshness boundaries, stale samples, and future clocks", %{ctx: ctx} do
    now_ms = 1_000_000
    max_age_ms = IndexStatistics.max_age_ms()
    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)
    registered = hd(snapshot.indexes)
    store = StatisticsStore.server_name(ctx)

    samples = [
      {"tenant-secret-boundary", now_ms - max_age_ms},
      {"tenant-secret-stale", now_ms - max_age_ms - 1},
      {"tenant-secret-future", now_ms + 1}
    ]

    for {scope, collected_at_ms} <- samples do
      assert :ok = StatisticsStore.put(store, statistic(registered, scope, collected_at_ms))
    end

    assert {:ok, %{"indexes" => indexes}} =
             IndexStatus.fetch(ctx, registered.definition.id, now_ms: now_ms)

    index =
      Enum.find(indexes, fn index -> index["version"] == registered.definition.version end)

    assert index["statistics"] == %{
             "fresh_samples" => 1,
             "future_samples" => 1,
             "newest_age_ms" => 0,
             "newest_collected_at_ms" => now_ms + 1,
             "oldest_age_ms" => max_age_ms + 1,
             "oldest_collected_at_ms" => now_ms - max_age_ms - 1,
             "samples" => 3,
             "stale_samples" => 2,
             "status" => "mixed"
           }

    encoded = inspect(index["statistics"])
    refute encoded =~ "tenant-secret"
  end

  test "distinguishes missing samples from an unavailable statistics service", %{ctx: ctx} do
    registry = Process.whereis(IndexRegistry.server_name(ctx))

    assert {:ok, %{"indexes" => missing_indexes, "services" => missing_services}} =
             IndexStatus.fetch(ctx, nil, registry: registry)

    assert missing_services["statistics_store"] == "ready"
    assert Enum.all?(missing_indexes, &(&1["statistics"]["status"] == "missing"))

    unavailable_ctx = %{ctx | name: :missing_statistics_status_instance}

    assert {:ok, %{"indexes" => unavailable_indexes, "services" => unavailable_services}} =
             IndexStatus.fetch(unavailable_ctx, nil, registry: registry)

    assert unavailable_services["registry"] == "ready"
    assert unavailable_services["statistics_store"] == "unavailable"
    assert Enum.all?(unavailable_indexes, &(&1["statistics"]["status"] == "unavailable"))
  end

  test "validates index filters at the wire-size boundary", %{ctx: ctx} do
    assert {:error, :invalid_query_index_filter} = IndexStatus.fetch(ctx, "")

    for invalid <- ["contains space", "contains/slash", <<0>>] do
      assert {:error, :invalid_query_index_filter} = IndexStatus.fetch(ctx, invalid)
    end

    assert {:error, :query_index_not_found} =
             IndexStatus.fetch(ctx, String.duplicate("a", 64))

    assert {:error, :invalid_query_index_filter} =
             IndexStatus.fetch(ctx, String.duplicate("a", 65))
  end

  test "projects build completion and validation failure as one actionable lifecycle", %{ctx: ctx} do
    server = IndexRegistry.server_name(ctx)
    assert {:ok, snapshot} = IndexRegistry.snapshot(ctx, 0)
    registered = hd(snapshot.indexes)

    assert :ok =
             IndexRegistry.checkpoint_build(server, registered.build_id, 0,
               phase: :snapshot,
               cursor: "",
               fenced: true,
               scanned_records: 0,
               written_entries: 0,
               written_bytes: 0
             )

    assert :ok =
             IndexRegistry.checkpoint_build(server, registered.build_id, 0,
               phase: :backfill,
               cursor: "tenant-secret-resume-key",
               fenced: true,
               scanned_records: 12,
               written_entries: 9,
               written_bytes: 768
             )

    assert :ok = IndexRegistry.complete_build_shard(server, registered.build_id, 0)

    assert :ok =
             IndexRegistry.validation_failed(server, registered.build_id,
               checked_records: 12,
               checked_entries: 9,
               mismatches: 3,
               reason: :source_index_mismatch
             )

    assert {:ok, %{"indexes" => indexes}} = IndexStatus.fetch(ctx, registered.definition.id)

    index =
      Enum.find(indexes, fn index -> index["version"] == registered.definition.version end)

    assert index["state"] == "failed"
    refute index["queryable"]

    assert index["build"] == %{
             "completed_shards" => 1,
             "current_phases" => ["done"],
             "phase_counts" => %{"done" => 1},
             "scanned_records" => 12,
             "scope" => "catalog_build",
             "total_shards" => 1,
             "written_bytes" => 768,
             "written_entries" => 9
           }

    assert index["validation"]["status"] == "failed"
    assert index["validation"]["failure_reason"] == "source_index_mismatch"
    assert index["validation"]["checked_records"] == 12
    assert index["validation"]["checked_entries"] == 9
    assert index["validation"]["mismatches"] == 3
    assert index["retirement"]["status"] == "pending"

    encoded = inspect(index)
    refute encoded =~ "tenant-secret-resume-key"
    refute encoded =~ "cursor"
  end

  defp statistic(registered, scope, collected_at_ms) do
    IndexStatistics.new!(%{
      index_id: registered.definition.id,
      index_version: registered.definition.version,
      scope_digest: IndexStatistics.scope_digest(scope),
      collected_at_ms: collected_at_ms,
      source_watermark: 1,
      total_entries: 1,
      distinct_runs: 1,
      prefix_counts: %{},
      prefix_observed_at_ms: %{},
      histograms: %{},
      null_counts: %{},
      missing_counts: %{},
      average_entry_bytes: 96,
      average_row_bytes: 384,
      sample_rate_ppm: 1_000_000,
      confidence: :high
    })
  end
end
