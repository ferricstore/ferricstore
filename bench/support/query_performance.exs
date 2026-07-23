defmodule Ferricstore.Bench.QueryPerformance do
  @moduledoc false

  @default_percentiles [50, 95, 99]

  def benchee_options(suite) when is_binary(suite) do
    formatters =
      case result_path(suite) do
        nil ->
          [Benchee.Formatters.Console]

        path ->
          [Benchee.Formatters.Console, {__MODULE__.JSONFormatter, %{path: path, suite: suite}}]
      end

    [
      warmup: float_env("BENCH_WARMUP", 1.0),
      time: float_env("BENCH_TIME", 3.0),
      memory_time: float_env("BENCH_MEMORY_TIME", 1.0),
      reduction_time: float_env("BENCH_REDUCTION_TIME", 0.0),
      parallel: int_env("BENCH_PARALLEL", 1, min: 1),
      percentiles: @default_percentiles,
      formatters: formatters
    ]
  end

  def int_env(name, default, options \\ []) do
    minimum = Keyword.get(options, :min, 0)

    case System.get_env(name) do
      nil ->
        default

      raw ->
        case Integer.parse(raw) do
          {value, ""} when value >= minimum -> value
          _ -> raise ArgumentError, "#{name} must be an integer >= #{minimum}"
        end
    end
  end

  def float_env(name, default) do
    case System.get_env(name) do
      nil ->
        default

      raw ->
        case Float.parse(raw) do
          {value, ""} when value >= 0.0 -> value
          _ -> raise ArgumentError, "#{name} must be a non-negative number"
        end
    end
  end

  def bool_env(name, default \\ false) do
    case System.get_env(name) do
      nil -> default
      value when value in ["1", "true", "TRUE"] -> true
      value when value in ["0", "false", "FALSE"] -> false
      _ -> raise ArgumentError, "#{name} must be true, false, 1, or 0"
    end
  end

  def integer_list_env(name, default, options \\ []) do
    minimum = Keyword.get(options, :min, 1)

    case System.get_env(name) do
      nil ->
        default

      raw ->
        values =
          raw
          |> String.split(",", trim: true)
          |> Enum.map(fn value ->
            case Integer.parse(String.trim(value)) do
              {parsed, ""} when parsed >= minimum -> parsed
              _ -> raise ArgumentError, "#{name} entries must be integers >= #{minimum}"
            end
          end)

        if values == [], do: raise(ArgumentError, "#{name} cannot be empty"), else: values
    end
  end

  def percentile([], _rank), do: 0

  def percentile(values, rank) when is_list(values) and rank >= 0 and rank <= 100 do
    sorted = Enum.sort(values)
    index = max(ceil(length(sorted) * rank / 100) - 1, 0)
    Enum.at(sorted, index)
  end

  def timed_ns(fun) when is_function(fun, 0) do
    started = System.monotonic_time(:nanosecond)
    result = fun.()
    {System.monotonic_time(:nanosecond) - started, result}
  end

  def latency_summary(samples) when is_list(samples) do
    %{
      "samples" => length(samples),
      "median_ns" => percentile(samples, 50),
      "p50_ns" => percentile(samples, 50),
      "p95_ns" => percentile(samples, 95),
      "p99_ns" => percentile(samples, 99),
      "max_ns" => Enum.max(samples, fn -> 0 end)
    }
  end

  def print_summary(name, summary) do
    IO.puts(
      "#{name} samples=#{summary["samples"]} " <>
        "p50_ns=#{summary["p50_ns"]} p95_ns=#{summary["p95_ns"]} " <>
        "p99_ns=#{summary["p99_ns"]} max_ns=#{summary["max_ns"]}"
    )
  end

  def write_manual_metrics(suite, metrics) when is_binary(suite) and is_map(metrics) do
    case result_path("#{suite}-manual") do
      nil ->
        :ok

      path ->
        payload = result_payload(suite, %{"metrics" => metrics})
        File.mkdir_p!(Path.dirname(path))
        File.write!(path, Jason.encode!(payload, pretty: true))
        IO.puts("Saved query performance metrics to #{path}")
    end
  end

  def directory_bytes(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reduce(0, fn entry, total ->
      case File.stat(entry) do
        {:ok, %{type: :regular, size: size}} -> total + size
        _ -> total
      end
    end)
  end

  def command_available?(command) when is_binary(command),
    do: System.find_executable(command) != nil

  def result_payload(suite, data) do
    Map.merge(
      %{
        "version" => 1,
        "suite" => suite,
        "system" => %{
          "os" => :os.type() |> Tuple.to_list() |> Enum.map_join("/", &to_string/1),
          "architecture" => system_architecture(),
          "cpu_model" => cpu_model(),
          "otp" => System.otp_release(),
          "elixir" => System.version(),
          "schedulers_online" => System.schedulers_online()
        }
      },
      data
    )
  end

  defp system_architecture do
    case :erlang.system_info(:system_architecture) do
      architecture when is_list(architecture) -> List.to_string(architecture)
      architecture -> to_string(architecture)
    end
  end

  defp cpu_model do
    case :os.type() do
      {:unix, :linux} -> linux_cpu_model()
      {:unix, :darwin} -> command_output("sysctl", ["-n", "machdep.cpu.brand_string"])
      _ -> System.get_env("PROCESSOR_IDENTIFIER", "unknown")
    end
  end

  defp linux_cpu_model do
    with {:ok, cpuinfo} <- File.read("/proc/cpuinfo"),
         [_, model] <- Regex.run(~r/^(?:model name|Model)\s*:\s*(.+)$/m, cpuinfo) do
      String.trim(model)
    else
      _ -> "unknown"
    end
  end

  defp command_output(command, arguments) do
    case System.cmd(command, arguments, stderr_to_stdout: true) do
      {output, 0} -> String.trim(output)
      _ -> "unknown"
    end
  rescue
    ErlangError -> "unknown"
  end

  defp result_path(suite) do
    case System.get_env("BENCH_SAVE") do
      nil ->
        nil

      "" ->
        nil

      root ->
        if Path.extname(root) == ".json" do
          root
        else
          Path.join(root, "#{suite}.json")
        end
    end
  end

  defmodule JSONFormatter do
    @moduledoc false
    @behaviour Benchee.Formatter

    @impl true
    def format(suite, %{path: path, suite: suite_name}) do
      scenarios =
        Map.new(suite.scenarios, fn scenario ->
          runtime = scenario.run_time_data.statistics
          memory = scenario.memory_usage_data.statistics
          reductions = scenario.reductions_data.statistics

          key =
            case scenario.input_name do
              input when is_binary(input) and input != "" -> "#{scenario.job_name}/#{input}"
              _ -> scenario.job_name
            end

          {key,
           %{
             "median_ns" => runtime.median,
             "p95_ns" => percentile(runtime.percentiles, 95),
             "p99_ns" => percentile(runtime.percentiles, 99),
             "average_ns" => runtime.average,
             "ips" => runtime.ips,
             "memory_median_bytes" => memory.median,
             "reductions_median" => reductions.median,
             "sample_size" => runtime.sample_size
           }}
        end)

      payload =
        Ferricstore.Bench.QueryPerformance.result_payload(suite_name, %{
          "scenarios" => scenarios
        })

      {Jason.encode!(payload, pretty: true), path}
    end

    @impl true
    def write({encoded, path}, _options) do
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, encoded)
      IO.puts("Saved query performance results to #{path}")
    end

    defp percentile(nil, _rank), do: nil

    defp percentile(percentiles, rank),
      do: Map.get(percentiles, rank) || Map.get(percentiles, rank * 1.0)
  end
end
