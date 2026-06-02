defmodule Ferricstore.OperationalLimitsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.OperationalLimits

  @gib 1024 * 1024 * 1024

  test "derives memory and disk thresholds from supplied node capacity" do
    snapshot =
      OperationalLimits.snapshot(
        cpu_count: 16,
        shard_count: 16,
        memory_bytes: 64 * @gib,
        rss_bytes: 52 * @gib,
        disk: %{
          path: "/data",
          total_bytes: 1024 * @gib,
          available_bytes: 174 * @gib
        }
      )

    assert snapshot.cpu_count == 16
    assert snapshot.shard_count == 16
    assert snapshot.memory.level == :pressure
    assert snapshot.memory.thresholds.pressure == trunc(64 * @gib * 0.80)
    assert snapshot.memory.thresholds.reject == trunc(64 * @gib * 0.88)
    assert snapshot.disk.level == :pressure
    assert snapshot.disk.thresholds.pressure == trunc(1024 * @gib * 0.80)
    assert snapshot.disk.thresholds.reject == trunc(1024 * @gib * 0.90)
    assert snapshot.recommendations.shard_count == 16
    assert snapshot.recommendations.claim_batch_size == 500
    assert snapshot.recommendations.pipeline_depth == 50
  end

  test "classifies reject and panic levels by configurable ratios" do
    snapshot =
      OperationalLimits.snapshot(
        memory_bytes: 100,
        rss_bytes: 95,
        disk: %{total_bytes: 100, available_bytes: 4},
        rss_reject_ratio: 0.90,
        rss_panic_ratio: 0.94,
        disk_reject_ratio: 0.90,
        disk_panic_ratio: 0.95
      )

    assert snapshot.memory.level == :panic
    assert snapshot.disk.level == :panic
  end
end
