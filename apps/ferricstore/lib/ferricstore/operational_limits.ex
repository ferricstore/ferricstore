defmodule Ferricstore.OperationalLimits do
  @moduledoc """
  Capacity-derived operational budgets for a FerricStore node.

  Defaults are ratios of the actual runtime environment: detected cgroup/host
  memory, the filesystem backing `:data_dir`, and scheduler count. Operators can
  override the detected ceilings in application config, but normal deployments
  should not need hand tuning when moving between instance sizes.
  """

  @default_rss_warn_ratio 0.70
  @default_rss_pressure_ratio 0.80
  @default_rss_reject_ratio 0.88
  @default_rss_panic_ratio 0.94

  @default_disk_warn_ratio 0.70
  @default_disk_pressure_ratio 0.80
  @default_disk_reject_ratio 0.90
  @default_disk_panic_ratio 0.95

  @default_inline_value_max_bytes 64 * 1024
  @default_blob_threshold_bytes 256 * 1024

  @type level :: :ok | :warn | :pressure | :reject | :panic | :unknown

  @spec snapshot(keyword()) :: map()
  def snapshot(opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, Application.get_env(:ferricstore, :data_dir, "data"))

    cpu_count =
      positive_int(
        Keyword.get(opts, :cpu_count),
        configured_int(:node_cpu_count),
        System.schedulers_online()
      )

    shard_count =
      positive_int(
        Keyword.get(opts, :shard_count),
        Application.get_env(:ferricstore, :shard_count),
        cpu_count
      )

    memory_limit = memory_limit_bytes(opts)
    rss_bytes = Keyword.get(opts, :rss_bytes) || safe_rss_bytes()
    disk = disk_capacity(opts, data_dir)

    rss_ratios = %{
      warn: ratio(opts, :rss_warn_ratio, :operational_rss_warn_ratio, @default_rss_warn_ratio),
      pressure:
        ratio(
          opts,
          :rss_pressure_ratio,
          :operational_rss_pressure_ratio,
          @default_rss_pressure_ratio
        ),
      reject:
        ratio(opts, :rss_reject_ratio, :operational_rss_reject_ratio, @default_rss_reject_ratio),
      panic: ratio(opts, :rss_panic_ratio, :operational_rss_panic_ratio, @default_rss_panic_ratio)
    }

    disk_ratios = %{
      warn: ratio(opts, :disk_warn_ratio, :operational_disk_warn_ratio, @default_disk_warn_ratio),
      pressure:
        ratio(
          opts,
          :disk_pressure_ratio,
          :operational_disk_pressure_ratio,
          @default_disk_pressure_ratio
        ),
      reject:
        ratio(
          opts,
          :disk_reject_ratio,
          :operational_disk_reject_ratio,
          @default_disk_reject_ratio
        ),
      panic:
        ratio(opts, :disk_panic_ratio, :operational_disk_panic_ratio, @default_disk_panic_ratio)
    }

    rss_ratio = ratio_of(rss_bytes, memory_limit)
    disk_ratio = ratio_of(disk.used_bytes, disk.total_bytes)

    memory = %{
      limit_bytes: memory_limit,
      rss_bytes: rss_bytes,
      rss_ratio: rss_ratio,
      level: classify_ratio(rss_ratio, rss_ratios),
      thresholds: bytes_thresholds(memory_limit, rss_ratios)
    }

    disk =
      Map.merge(disk, %{
        used_ratio: disk_ratio,
        level: classify_ratio(disk_ratio, disk_ratios),
        thresholds: bytes_thresholds(disk.total_bytes, disk_ratios)
      })

    %{
      data_dir: data_dir,
      cpu_count: cpu_count,
      shard_count: shard_count,
      memory: memory,
      disk: disk,
      recommendations: recommendations(cpu_count, shard_count, memory_limit, disk.total_bytes)
    }
  end

  @spec memory_limit_bytes(keyword()) :: non_neg_integer()
  def memory_limit_bytes(opts \\ []) do
    positive_int(
      Keyword.get(opts, :memory_bytes),
      configured_int(:operational_memory_limit_bytes),
      configured_int(:max_memory_bytes),
      safe_memory_limit()
    ) || 0
  end

  @spec classify_ratio(nil | number(), map()) :: level()
  def classify_ratio(nil, _thresholds), do: :unknown
  def classify_ratio(ratio, %{panic: panic}) when ratio >= panic, do: :panic
  def classify_ratio(ratio, %{reject: reject}) when ratio >= reject, do: :reject
  def classify_ratio(ratio, %{pressure: pressure}) when ratio >= pressure, do: :pressure
  def classify_ratio(ratio, %{warn: warn}) when ratio >= warn, do: :warn
  def classify_ratio(_ratio, _thresholds), do: :ok

  defp recommendations(cpu_count, shard_count, memory_limit, disk_total) do
    memory_gib = div(memory_limit, 1024 * 1024 * 1024)
    disk_gib = div(disk_total, 1024 * 1024 * 1024)

    %{
      shard_count: max(shard_count, 1),
      queue_workers_per_node: max(cpu_count * 2, 4),
      queue_worker_concurrency: max(cpu_count * 250, 500),
      create_batch_size: 500,
      claim_batch_size: 500,
      pipeline_depth: 50,
      max_connections_hint: max(cpu_count * 16, 64),
      inline_value_max_bytes: @default_inline_value_max_bytes,
      blob_threshold_bytes:
        configured_int(:blob_side_channel_threshold_bytes) || @default_blob_threshold_bytes,
      memory_headroom_gib: max(memory_gib - div(memory_gib * 88, 100), 0),
      disk_headroom_gib: max(disk_gib - div(disk_gib * 90, 100), 0),
      overload: %{
        disk_pressure: "accelerate retention and compaction",
        disk_reject: "reject new writes with backpressure until free space recovers",
        rss_pressure: "skip cold-read promotion and increase cleanup pressure",
        rss_reject: "reject writes according to eviction policy/backpressure"
      }
    }
  end

  defp disk_capacity(opts, data_dir) do
    case Keyword.get(opts, :disk) do
      %{total_bytes: total, available_bytes: available} = disk ->
        %{
          path: Map.get(disk, :path, data_dir),
          total_bytes: max(total, 0),
          available_bytes: max(available, 0),
          used_bytes: max(total - available, 0)
        }

      _ ->
        configured_disk_capacity(data_dir) || detected_disk_capacity(data_dir)
    end
  end

  defp configured_disk_capacity(data_dir) do
    total = configured_int(:operational_disk_total_bytes)
    available = configured_int(:operational_disk_available_bytes)

    if total && available do
      %{
        path: data_dir,
        total_bytes: total,
        available_bytes: available,
        used_bytes: max(total - available, 0)
      }
    end
  end

  defp detected_disk_capacity(data_dir) do
    path = existing_parent(data_dir)

    case System.cmd("df", ["-Pk", path], stderr_to_stdout: true) do
      {output, 0} -> parse_df(output, path)
      _ -> unknown_disk(path)
    end
  rescue
    _ -> unknown_disk(data_dir)
  end

  defp parse_df(output, path) do
    output
    |> String.split("\n", trim: true)
    |> List.last()
    |> case do
      nil ->
        unknown_disk(path)

      line ->
        parts = String.split(line, ~r/\s+/, trim: true)

        with [_, blocks, used, available | _] <- parts,
             {blocks, ""} <- Integer.parse(blocks),
             {used, ""} <- Integer.parse(used),
             {available, ""} <- Integer.parse(available) do
          %{
            path: path,
            total_bytes: blocks * 1024,
            used_bytes: used * 1024,
            available_bytes: available * 1024
          }
        else
          _ -> unknown_disk(path)
        end
    end
  end

  defp unknown_disk(path) do
    %{path: path, total_bytes: 0, used_bytes: 0, available_bytes: 0}
  end

  defp existing_parent(path) do
    expanded = Path.expand(path || ".")

    cond do
      Ferricstore.FS.exists?(expanded) ->
        expanded

      Path.dirname(expanded) == expanded ->
        expanded

      true ->
        existing_parent(Path.dirname(expanded))
    end
  end

  defp ratio(opts, opt_key, env_key, default) do
    case Keyword.get(opts, opt_key, Application.get_env(:ferricstore, env_key, default)) do
      value when is_number(value) and value > 0 and value < 1 -> value * 1.0
      _ -> default
    end
  end

  defp ratio_of(_value, 0), do: nil
  defp ratio_of(nil, _limit), do: nil
  defp ratio_of(value, limit) when limit > 0, do: value / limit

  defp bytes_thresholds(0, _ratios), do: %{warn: 0, pressure: 0, reject: 0, panic: 0}

  defp bytes_thresholds(limit, ratios) do
    %{
      warn: trunc(limit * ratios.warn),
      pressure: trunc(limit * ratios.pressure),
      reject: trunc(limit * ratios.reject),
      panic: trunc(limit * ratios.panic)
    }
  end

  defp positive_int(values) when is_list(values) do
    Enum.find_value(values, fn
      value when is_integer(value) and value > 0 -> value
      _ -> nil
    end)
  end

  defp positive_int(value1, value2, default) do
    positive_int([value1, value2, default])
  end

  defp positive_int(value1, value2, value3, default) do
    positive_int([value1, value2, value3, default])
  end

  defp configured_int(key) do
    case Application.get_env(:ferricstore, key) do
      value when is_integer(value) and value > 0 -> value
      _ -> nil
    end
  end

  defp safe_memory_limit do
    Ferricstore.MemoryGuard.detect_memory_limit()
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp safe_rss_bytes do
    Ferricstore.MemoryGuard.process_rss_bytes()
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end
end
