defmodule Ferricstore.MetricsTest do
  @moduledoc false
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias Ferricstore.Metrics
  alias Ferricstore.PrefixMetricsCache
  alias Ferricstore.QuorumMetrics

  # ---------------------------------------------------------------------------
  # scrape/0 — Prometheus text format validity
  # ---------------------------------------------------------------------------

  describe "scrape/0" do
    test "returns a non-empty binary string" do
      text = Metrics.scrape()
      assert is_binary(text)
      assert byte_size(text) > 0
    end

    test "ends with a trailing newline" do
      text = Metrics.scrape()
      assert String.ends_with?(text, "\n")
    end

    test "contains all expected metric names" do
      text = Metrics.scrape()

      expected_metrics = [
        "ferricstore_connected_clients",
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_hot_reads_total",
        "ferricstore_cold_reads_total",
        "ferricstore_used_memory_bytes",
        "ferricstore_keydir_used_bytes",
        "ferricstore_uptime_seconds",
        "ferricstore_blocked_clients",
        "ferricstore_tracking_clients",
        "ferricstore_slowlog_entries"
      ]

      for metric <- expected_metrics do
        assert String.contains?(text, metric),
               "Expected metric #{metric} to be present in scrape output"
      end
    end

    test "every metric has a HELP line" do
      text = Metrics.scrape()

      expected_metrics = [
        "ferricstore_connected_clients",
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_hot_reads_total",
        "ferricstore_cold_reads_total",
        "ferricstore_used_memory_bytes",
        "ferricstore_keydir_used_bytes",
        "ferricstore_uptime_seconds",
        "ferricstore_blocked_clients",
        "ferricstore_tracking_clients",
        "ferricstore_slowlog_entries"
      ]

      for metric <- expected_metrics do
        assert String.contains?(text, "# HELP #{metric} "),
               "Expected HELP line for #{metric}"
      end
    end

    test "every metric has a TYPE line with counter or gauge" do
      text = Metrics.scrape()

      counters = [
        "ferricstore_total_connections_received",
        "ferricstore_total_commands_processed",
        "ferricstore_hot_reads_total",
        "ferricstore_cold_reads_total"
      ]

      gauges = [
        "ferricstore_connected_clients",
        "ferricstore_used_memory_bytes",
        "ferricstore_keydir_used_bytes",
        "ferricstore_uptime_seconds",
        "ferricstore_blocked_clients",
        "ferricstore_tracking_clients",
        "ferricstore_slowlog_entries"
      ]

      for metric <- counters do
        assert String.contains?(text, "# TYPE #{metric} counter"),
               "Expected TYPE counter for #{metric}"
      end

      for metric <- gauges do
        assert String.contains?(text, "# TYPE #{metric} gauge"),
               "Expected TYPE gauge for #{metric}"
      end
    end

    test "all metric values are non-negative integers" do
      text = Metrics.scrape()

      # Parse each sample line (lines that don't start with #)
      sample_lines =
        text
        |> String.split("\n", trim: true)
        |> Enum.reject(&String.starts_with?(&1, "#"))

      # At least the 11 base metrics, plus any labeled prefix/namespace metrics
      assert length(sample_lines) >= 11,
             "Expected at least 11 sample lines, got #{length(sample_lines)}"

      for line <- sample_lines do
        # Handle labeled metrics: metric_name{labels} value
        value_str =
          case Regex.run(~r/\}\s+(.+)$/, line) do
            [_, val] ->
              val

            nil ->
              [_name, val] = String.split(line, " ", parts: 2)
              val
          end

        {value, ""} = Integer.parse(value_str)

        assert value >= 0,
               "Expected non-negative value for line: #{line}, got #{value}"
      end
    end

    test "follows correct Prometheus triplet order: HELP, TYPE, sample" do
      text = Metrics.scrape()
      lines = String.split(text, "\n", trim: true)

      # Group lines into metric families: each starts with # HELP, then # TYPE,
      # then one or more sample lines (for labeled metrics there can be many).
      families = parse_metric_families(lines)

      for {help_name, type_name, sample_names} <- families do
        assert help_name == type_name,
               "HELP name (#{help_name}) does not match TYPE name (#{type_name})"

        # Each sample name should match or start with the metric name
        # (labeled metrics have {labels} appended)
        for sample_name <- sample_names do
          base_name = sample_name |> String.split("{") |> hd()

          assert base_name == type_name,
                 "TYPE name (#{type_name}) does not match sample name (#{sample_name})"
        end
      end
    end

    test "used_memory_bytes is positive (BEAM always uses some memory)" do
      text = Metrics.scrape()
      value = extract_metric_value(text, "ferricstore_used_memory_bytes")
      assert value > 0
    end

    test "uptime_seconds is non-negative" do
      text = Metrics.scrape()
      value = extract_metric_value(text, "ferricstore_uptime_seconds")
      assert value >= 0
    end

    test "produces at least 11 base metrics" do
      text = Metrics.scrape()

      help_count =
        text
        |> String.split("\n", trim: true)
        |> Enum.count(&String.starts_with?(&1, "# HELP"))

      # 11 base metrics, plus namespace and prefix metrics when data exists
      assert help_count >= 11
    end

    test "includes quorum write-path telemetry counters" do
      QuorumMetrics.reset()

      on_exit(fn ->
        QuorumMetrics.reset()
      end)

      :telemetry.execute(
        [:ferricstore, :batcher, :slot_flush],
        %{batch_size: 3, caller_count: 2, queue_wait_us: 17},
        %{shard_index: 2, durability: :quorum}
      )

      :telemetry.execute(
        [:ferricstore, :batcher, :quorum_submit],
        %{duration_us: 5, batch_size: 3, caller_count: 2, command_bytes: 80},
        %{shard_index: 2, kind: :batch, status: :ok}
      )

      :telemetry.execute(
        [:ferricstore, :raft, :apply],
        %{duration_us: 11},
        %{shard_index: 2, result: :ok, disk: :ok}
      )

      :telemetry.execute(
        [:ferricstore, :bitcask, :append],
        %{duration_us: 7, batch_size: 3, batch_bytes: 42},
        %{shard_index: 2, status: :ok}
      )

      :telemetry.execute(
        [:ferricstore, :batcher, :local_apply_waiters],
        %{depth: 5, oldest_age_ms: 13},
        %{shard_index: 2}
      )

      :telemetry.execute(
        [:ferricstore, :batcher, :local_apply_timeout],
        %{count: 2},
        %{shard_index: 2}
      )

      text = Metrics.handle("FERRICSTORE.METRICS", [])

      assert String.contains?(text, "# TYPE ferricstore_quorum_submit_total counter")

      assert String.contains?(
               text,
               ~s(ferricstore_quorum_slot_flush_total{shard_index="2",durability="quorum"} 1)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_quorum_slot_flush_queue_wait_us_total{shard_index="2",durability="quorum"} 17)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_quorum_submit_total{shard_index="2",kind="batch",status="ok"} 1)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_quorum_submit_command_bytes_total{shard_index="2",kind="batch",status="ok"} 80)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_quorum_apply_total{shard_index="2",result="ok",disk="ok"} 1)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_quorum_bitcask_append_batch_bytes_total{shard_index="2",status="ok"} 42)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_batcher_local_apply_waiters{shard_index="2"} 5)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_batcher_local_apply_waiter_oldest_age_ms{shard_index="2"} 13)
             )

      assert String.contains?(
               text,
               ~s(ferricstore_batcher_local_apply_timeout_total{shard_index="2"} 2)
             )
    end

    test "includes per-shard checkpoint and release cursor gauges" do
      text = Metrics.handle("FERRICSTORE.METRICS", [])

      expected_metrics = [
        "ferricstore_bitcask_last_applied_index",
        "ferricstore_bitcask_last_released_cursor_index",
        "ferricstore_bitcask_release_cursor_gap",
        "ferricstore_bitcask_pending_release_cursor_checkpoint_count",
        "ferricstore_bitcask_release_cursor_blocked_apply_count",
        "ferricstore_bitcask_checkpoint_dirty",
        "ferricstore_bitcask_checkpoint_in_flight"
      ]

      for metric <- expected_metrics do
        assert String.contains?(text, "# HELP #{metric} ")
        assert String.contains?(text, "# TYPE #{metric} gauge")
        assert String.contains?(text, ~s(#{metric}{shard_index="0"}))
      end
    end

    test "serves prefix metrics from cache instead of rescanning on every scrape" do
      PrefixMetricsCache.reset()

      prefix = "metrics_cache_#{System.unique_integer([:positive])}"
      key = "#{prefix}:key"

      :ets.insert(:keydir_0, {key, "value", 0, 0, 0, 0, byte_size("value")})

      on_exit(fn ->
        :ets.delete(:keydir_0, key)
        PrefixMetricsCache.reset()
      end)

      assert :ok = PrefixMetricsCache.refresh_now()

      text = Metrics.handle("FERRICSTORE.METRICS", [])

      assert String.contains?(
               text,
               ~s(ferricstore_prefix_key_count{prefix="#{prefix}"} 1)
             )

      :ets.delete(:keydir_0, key)

      cached_text = Metrics.handle("FERRICSTORE.METRICS", [])

      assert String.contains?(
               cached_text,
               ~s(ferricstore_prefix_key_count{prefix="#{prefix}"} 1)
             )

      assert :ok = PrefixMetricsCache.refresh_now()
      refreshed_text = Metrics.handle("FERRICSTORE.METRICS", [])

      refute String.contains?(
               refreshed_text,
               ~s(ferricstore_prefix_key_count{prefix="#{prefix}"} 1)
             )
    end

    test "prefix metrics cache ignores stale ra_event messages without error logging" do
      pid = Process.whereis(PrefixMetricsCache)
      assert is_pid(pid)

      log =
        capture_log(fn ->
          send(pid, {:ra_event, {:stale_shard, :nonode@nohost}, {:applied, []}})
          Process.sleep(20)
        end)

      refute log =~ "received unexpected message"
    end
  end

  # ---------------------------------------------------------------------------
  # handle/2 — FERRICSTORE.METRICS command dispatch
  # ---------------------------------------------------------------------------

  describe "FERRICSTORE.METRICS command" do
    test "returns scrape text with no arguments" do
      result = Metrics.handle("FERRICSTORE.METRICS", [])
      assert is_binary(result)
      assert String.contains?(result, "ferricstore_connected_clients")
      assert String.contains?(result, "# HELP")
      assert String.contains?(result, "# TYPE")
    end

    test "returns error with extra arguments" do
      assert {:error, msg} = Metrics.handle("FERRICSTORE.METRICS", ["extra"])
      assert msg =~ "wrong number of arguments"
    end

    test "result matches scrape/0 output" do
      # Both should produce equivalent output (collected at roughly the same time)
      command_result = Metrics.handle("FERRICSTORE.METRICS", [])
      scrape_result = Metrics.scrape()

      # Both should have the same metric names and structure
      command_metrics = extract_metric_names(command_result)
      scrape_metrics = extract_metric_names(scrape_result)

      assert command_metrics == scrape_metrics
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: FERRICSTORE.METRICS via the Dispatcher
  # ---------------------------------------------------------------------------

  describe "FERRICSTORE.METRICS via Dispatcher" do
    test "dispatch routes to metrics handler" do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.MockStore

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.METRICS", [], store)

      assert is_binary(result)
      assert String.contains?(result, "# HELP ferricstore_connected_clients")
      assert String.contains?(result, "# TYPE ferricstore_connected_clients gauge")
    end

    test "dispatch is case-insensitive" do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.MockStore

      store = MockStore.make()
      result = Dispatcher.dispatch("ferricstore.metrics", [], store)

      assert is_binary(result)
      assert String.contains?(result, "ferricstore_connected_clients")
    end

    test "dispatch with extra args returns error" do
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Test.MockStore

      store = MockStore.make()
      result = Dispatcher.dispatch("FERRICSTORE.METRICS", ["unexpected"], store)

      assert {:error, msg} = result
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Parses Prometheus text into metric families.
  # Returns [{help_name, type_name, [sample_names]}]
  defp parse_metric_families(lines) do
    parse_metric_families(lines, [])
  end

  defp parse_metric_families([], acc), do: Enum.reverse(acc)

  defp parse_metric_families(["# HELP " <> rest | lines], acc) do
    help_name = rest |> String.split(" ", parts: 2) |> hd()

    case lines do
      ["# TYPE " <> type_rest | sample_lines] ->
        type_name = type_rest |> String.split(" ", parts: 2) |> hd()

        # Collect all sample lines until the next # HELP or end
        {samples, remaining} =
          Enum.split_while(sample_lines, fn line ->
            not String.starts_with?(line, "# HELP") and not String.starts_with?(line, "# TYPE")
          end)

        sample_names =
          samples
          |> Enum.reject(&String.starts_with?(&1, "#"))
          |> Enum.map(fn line ->
            line |> String.split(" ", parts: 2) |> hd()
          end)

        parse_metric_families(remaining, [{help_name, type_name, sample_names} | acc])

      _ ->
        parse_metric_families(lines, acc)
    end
  end

  defp parse_metric_families([_line | lines], acc) do
    parse_metric_families(lines, acc)
  end

  defp extract_metric_value(text, metric_name) do
    text
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.find_value(fn line ->
      case String.split(line, " ", parts: 2) do
        [^metric_name, value_str] ->
          {value, ""} = Integer.parse(value_str)
          value

        _ ->
          nil
      end
    end)
  end

  defp extract_metric_names(text) do
    text
    |> String.split("\n", trim: true)
    |> Enum.filter(&String.starts_with?(&1, "# HELP"))
    |> Enum.map(fn line ->
      line
      |> String.replace_prefix("# HELP ", "")
      |> String.split(" ", parts: 2)
      |> hd()
    end)
    |> Enum.sort()
  end
end
