defmodule Ferricstore.Flow.Query.StatisticsStoreTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Limits
  alias Ferricstore.Flow.Query.{IndexStatistics, StatisticsStore}

  test "publishes tenant-scoped statistics and rejects monotonic regressions" do
    {ctx, server} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: server, max_entries: 8})

    first = stat("index-a", 1, "tenant-a", 100, 7, 10)
    assert :ok = StatisticsStore.put(server, first)
    assert {:ok, ^first} = StatisticsStore.lookup(ctx, "index-a", 1, "tenant-a")
    assert :not_found = StatisticsStore.lookup(ctx, "index-a", 1, "tenant-b")

    assert {:error, :non_monotonic_query_statistics} =
             StatisticsStore.put(server, stat("index-a", 1, "tenant-a", 99, 8, 11))

    assert {:error, :non_monotonic_query_statistics} =
             StatisticsStore.put(server, stat("index-a", 1, "tenant-a", 101, 6, 11))

    replacement = stat("index-a", 1, "tenant-a", 101, 8, 11)
    assert :ok = StatisticsStore.put(server, replacement)
    assert {:ok, ^replacement} = StatisticsStore.lookup(ctx, "index-a", 1, "tenant-a")

    digest = IndexStatistics.prefix_digest(["tenant-a"])

    stale_prefix =
      stat("index-a", 1, "tenant-a", 102, 9, 12)
      |> Map.from_struct()
      |> Map.update!(:prefix_observed_at_ms, &Map.put(&1, digest, 100))
      |> IndexStatistics.new!()

    assert {:error, :non_monotonic_query_statistics} =
             StatisticsStore.put(server, stale_prefix)
  end

  test "evicts deterministically and never exceeds its entry bound" do
    {ctx, server} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: server, max_entries: 2})

    assert :ok = StatisticsStore.put(server, stat("a", 1, "tenant", 1, 1, 1))
    assert :ok = StatisticsStore.put(server, stat("b", 1, "tenant", 2, 1, 1))
    assert :ok = StatisticsStore.put(server, stat("c", 1, "tenant", 3, 1, 1))

    assert StatisticsStore.size(ctx) == 2
    assert :not_found = StatisticsStore.lookup(ctx, "a", 1, "tenant")
    assert {:ok, _stat} = StatisticsStore.lookup(ctx, "b", 1, "tenant")
    assert {:ok, _stat} = StatisticsStore.lookup(ctx, "c", 1, "tenant")
  end

  test "replacements keep eviction metadata bounded to live cache entries" do
    {ctx, server} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: server, max_entries: 2})

    for sequence <- 1..100 do
      assert :ok = StatisticsStore.put(server, stat("a", 1, "tenant", sequence, sequence, 1))
    end

    assert :ok = StatisticsStore.put(server, stat("b", 1, "tenant", 101, 101, 1))

    state = :sys.get_state(server)
    assert :gb_trees.size(state.eviction_order) == StatisticsStore.size(ctx)

    assert :ok = StatisticsStore.put(server, stat("c", 1, "tenant", 102, 102, 1))
    assert StatisticsStore.size(ctx) == 2
    assert :not_found = StatisticsStore.lookup(ctx, "a", 1, "tenant")
  end

  test "concurrent readers observe only complete immutable structs" do
    {ctx, server} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: server, max_entries: 64})

    tasks =
      for sequence <- 1..32 do
        Task.async(fn ->
          stat = stat("index", sequence, "tenant-#{sequence}", sequence, sequence, sequence)
          :ok = StatisticsStore.put(server, stat)
          StatisticsStore.lookup(ctx, "index", sequence, "tenant-#{sequence}")
        end)
      end

    assert Enum.all?(Task.await_many(tasks), &match?({:ok, %IndexStatistics{}}, &1))
    assert StatisticsStore.size(ctx) == 32
  end

  test "statistics validation is bounded and inspection does not reveal digests" do
    valid = stat("index", 1, "secret-tenant", 1, 1, 1)
    inspected = inspect(valid)

    refute inspected =~ Base.encode16(valid.scope_digest)
    refute inspected =~ "secret-tenant"

    oversized =
      valid
      |> Map.from_struct()
      |> Map.put(
        :prefix_counts,
        Map.new(1..257, fn value ->
          {IndexStatistics.prefix_digest([value]), value}
        end)
      )

    assert {:error, :invalid_query_index_statistics} = IndexStatistics.new(oversized)
  end

  test "summaries report freshness by index without exposing tenant digests" do
    {ctx, server} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: server, max_entries: 8})
    now_ms = 1_000_000
    max_age_ms = IndexStatistics.max_age_ms()

    assert :ok =
             StatisticsStore.put(
               server,
               stat("index-a", 1, "tenant-secret-fresh", now_ms - 10, 1, 1)
             )

    assert :ok =
             StatisticsStore.put(
               server,
               stat(
                 "index-a",
                 1,
                 "tenant-secret-stale",
                 now_ms - max_age_ms - 1,
                 1,
                 1
               )
             )

    assert :ok =
             StatisticsStore.put(
               server,
               stat("unrequested-index", 1, "other-secret", now_ms, 1, 1)
             )

    assert {:ok,
            %{
              {"index-a", 1} => %{
                fresh_samples: 1,
                future_samples: 0,
                newest_age_ms: 10,
                newest_collected_at_ms: 999_990,
                oldest_age_ms: oldest_age_ms,
                oldest_collected_at_ms: oldest_collected_at_ms,
                samples: 2,
                stale_samples: 1,
                status: :mixed
              }
            } = summary} =
             StatisticsStore.summaries(ctx, MapSet.new([{"index-a", 1}]), now_ms)

    assert oldest_collected_at_ms == now_ms - max_age_ms - 1
    assert oldest_age_ms == max_age_ms + 1

    encoded = inspect(summary)
    refute encoded =~ "tenant-secret"
    refute encoded =~ "other-secret"
    refute encoded =~ Base.encode16(IndexStatistics.scope_digest("tenant-secret-fresh"))
  end

  test "summary projection enforces identity bounds without mutating the cache" do
    {ctx, server} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: server, max_entries: 8})
    assert :ok = StatisticsStore.put(server, stat("index-live", 1, "tenant", 1, 1, 1))

    before = :sys.get_state(server)
    identities = MapSet.new(1..32, &{"index-#{&1}", 1})

    assert {:ok, summaries} = StatisticsStore.summaries(ctx, identities, 1)
    assert map_size(summaries) == 32
    assert Enum.all?(summaries, fn {_identity, summary} -> summary.status == :missing end)

    after_summary = :sys.get_state(server)
    assert after_summary.sequence == before.sequence
    assert after_summary.eviction_order == before.eviction_order
    assert StatisticsStore.size(ctx) == 1

    assert {:error, :invalid_query_statistics_summary} =
             StatisticsStore.summaries(
               ctx,
               MapSet.new(1..33, &{"index-#{&1}", 1}),
               1
             )

    assert {:error, :invalid_query_statistics_summary} =
             StatisticsStore.summaries(
               ctx,
               MapSet.new([{String.duplicate("x", 65), 1}]),
               1
             )
  end

  test "rejects histogram counters outside the unsigned 64-bit domain" do
    oversized =
      stat("index", 1, "tenant", 1, 1, 1)
      |> Map.from_struct()
      |> Map.put(:histograms, %{
        :updated_at_ms => [%{lower: 0, upper: 1, count: 0x1_0000_0000_0000_0000}]
      })

    assert {:error, :invalid_query_index_statistics} = IndexStatistics.new(oversized)
  end

  test "rejects advisory counts that exceed their population" do
    base = stat("index", 1, "tenant", 1, 1, 1) |> Map.from_struct()
    digest = IndexStatistics.prefix_digest(["tenant"])

    invalid = [
      Map.put(base, :prefix_counts, %{digest => 2}),
      Map.put(base, :null_counts, %{updated_at_ms: 2}),
      Map.put(base, :missing_counts, %{updated_at_ms: 2}),
      base
      |> Map.put(:null_counts, %{updated_at_ms: 1})
      |> Map.put(:missing_counts, %{updated_at_ms: 1}),
      Map.put(base, :histograms, %{
        updated_at_ms: [
          %{lower: 0, upper: 1, count: 1},
          %{lower: 2, upper: 3, count: 1}
        ]
      })
    ]

    assert Enum.all?(invalid, fn attrs ->
             IndexStatistics.new(attrs) == {:error, :invalid_query_index_statistics}
           end)
  end

  test "histograms are homogeneous and ignored for differently typed query bounds" do
    mixed =
      stat("index", 1, "tenant", 1, 1, 2)
      |> Map.from_struct()
      |> Map.put(:histograms, %{
        updated_at_ms: [
          %{lower: 0, upper: 1, count: 1},
          %{lower: 2.0, upper: 3.0, count: 1}
        ]
      })

    assert {:error, :invalid_query_index_statistics} = IndexStatistics.new(mixed)

    stat =
      stat("index", 1, "tenant", 1, 1, 2)
      |> Map.from_struct()
      |> Map.put(:histograms, %{
        updated_at_ms: [%{lower: 0, upper: 10, count: 2}]
      })
      |> IndexStatistics.new!()

    assert :unknown =
             IndexStatistics.histogram_fraction_ppm(
               stat,
               :updated_at_ms,
               1.0,
               2.0,
               false
             )
  end

  test "exact prefix counts expire by their own observation time" do
    digest = IndexStatistics.prefix_digest(["tenant"])

    stat =
      stat("index", 1, "tenant", 1_000_000, 1, 7)
      |> Map.from_struct()
      |> Map.put(:prefix_observed_at_ms, %{digest => 100})
      |> IndexStatistics.new!()

    assert {:ok, 7} = IndexStatistics.prefix_count(stat, ["tenant"], 100)
    assert :unknown = IndexStatistics.prefix_count(stat, ["tenant"], 1_000_000)
  end

  test "lookup rejects an oversized scope before using its digest" do
    {ctx, server} = context()
    start_supervised!({StatisticsStore, instance_ctx: ctx, name: server, max_entries: 8})
    oversized_scope = String.duplicate("x", Limits.max_partition_key_bytes() + 1)
    forged = stat("index", 1, oversized_scope, 1, 1, 1)
    assert :ok = StatisticsStore.put(server, forged)

    assert :not_found = StatisticsStore.lookup(ctx, "index", 1, oversized_scope)
  end

  defp context do
    suffix = System.unique_integer([:positive, :monotonic])

    {%{name: :"statistics_instance_#{suffix}"}, :"statistics_server_#{suffix}"}
  end

  defp stat(index_id, index_version, scope, collected_at_ms, watermark, count) do
    IndexStatistics.new!(%{
      index_id: index_id,
      index_version: index_version,
      scope_digest: IndexStatistics.scope_digest(scope),
      collected_at_ms: collected_at_ms,
      source_watermark: watermark,
      total_entries: count,
      distinct_runs: count,
      prefix_counts: %{IndexStatistics.prefix_digest([scope]) => count},
      prefix_observed_at_ms: %{IndexStatistics.prefix_digest([scope]) => collected_at_ms},
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
