defmodule Ferricstore.Metrics do
  @moduledoc """
  Prometheus-compatible metrics exposition for FerricStore.

  Collects server statistics from `Ferricstore.Stats`, `Ferricstore.MemoryGuard`,
  `Ferricstore.SlowLog`, and BEAM runtime sources, then formats them in the
  Prometheus text exposition format (version 0.0.4).

  No external dependencies are required -- the module produces a plain UTF-8
  string that any Prometheus-compatible scraper can consume.

  ## Exposed metrics

  | Metric name                              | Type    | Source                        |
  |------------------------------------------|---------|-------------------------------|
  | `ferricstore_connected_clients`          | gauge   | Ranch listener connection count |
  | `ferricstore_total_connections_received`  | counter | `Stats.total_connections/0`   |
  | `ferricstore_total_commands_processed`    | counter | `Stats.total_commands/0`      |
  | `ferricstore_hot_reads_total`            | counter | `Stats.total_hot_reads/0`     |
  | `ferricstore_cold_reads_total`           | counter | `Stats.total_cold_reads/0`    |
  | `ferricstore_used_memory_bytes`          | gauge   | `:erlang.memory(:total)`      |
  | `ferricstore_keydir_used_bytes`          | gauge   | shard ETS table memory        |
  | `ferricstore_uptime_seconds`             | gauge   | `Stats.uptime_seconds/0`      |
  | `ferricstore_blocked_clients`            | gauge   | waiters ETS table size        |
  | `ferricstore_tracking_clients`           | gauge   | tracking connections ETS size |
  | `ferricstore_slowlog_entries`            | gauge   | `SlowLog.len/0`              |
  | `ferricstore_namespace_window_ms`        | gauge   | `NamespaceConfig.get_all/0`   |
  | `ferricstore_namespace_durability`       | gauge   | `NamespaceConfig.get_all/0`   |
  | `ferricstore_prefix_*`                   | mixed   | `PrefixMetricsCache`          |
  | `ferricstore_quorum_*`                   | counter | `QuorumMetrics` telemetry     |

  ## Usage

      iex> text = Ferricstore.Metrics.scrape()
      iex> String.contains?(text, "ferricstore_uptime_seconds")
      true

  The `FERRICSTORE.METRICS` Redis command returns this text as a bulk string.
  """

  alias Ferricstore.Stats

  @type metric_type :: :counter | :gauge

  @doc """
  Handles the `FERRICSTORE.METRICS` Redis command.

  Returns the Prometheus text exposition as a bulk string. Accepts no arguments.

  ## Parameters

    * `cmd` -- the uppercased command name (`"FERRICSTORE.METRICS"`)
    * `args` -- argument list (must be empty)

  ## Returns

  The scrape text on success, or `{:error, message}` for wrong arguments.
  """
  @spec handle(binary(), [binary()]) :: binary() | {:error, binary()}
  def handle("FERRICSTORE.METRICS", []), do: scrape()

  def handle("FERRICSTORE.METRICS", _args) do
    {:error, "ERR wrong number of arguments for 'ferricstore.metrics' command"}
  end

  @doc """
  Produces a Prometheus text exposition format string containing all FerricStore
  metrics.

  Each metric includes a `# HELP` line describing its purpose, a `# TYPE` line
  declaring its Prometheus type, and a sample line with the current value.

  ## Returns

  A UTF-8 binary string in Prometheus text exposition format.

  ## Examples

      iex> text = Ferricstore.Metrics.scrape()
      iex> text |> String.split("\\n") |> Enum.count(&String.starts_with?(&1, "# HELP")) >= 11
      true
  """
  @spec scrape() :: binary()
  def scrape do
    base =
      metrics()
      |> Enum.map_join("\n", &format_metric/1)

    ns = namespace_metrics_text()
    prefix = prefix_metrics_text()
    quorum = Ferricstore.QuorumMetrics.prometheus_text()

    [base, ns, prefix, quorum]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  # ---------------------------------------------------------------------------
  # Private: metric collection
  # ---------------------------------------------------------------------------

  @spec metrics() :: [{binary(), metric_type(), binary(), non_neg_integer()}]
  defp metrics do
    [
      {"ferricstore_connected_clients", :gauge,
       "Number of active client connections", connected_clients()},
      {"ferricstore_total_connections_received", :counter,
       "Total number of TCP connections accepted since startup",
       Stats.total_connections()},
      {"ferricstore_total_commands_processed", :counter,
       "Total number of commands dispatched since startup",
       Stats.total_commands()},
      {"ferricstore_hot_reads_total", :counter,
       "Total number of reads served from the ETS hot cache",
       Stats.total_hot_reads(FerricStore.Instance.get(:default))},
      {"ferricstore_cold_reads_total", :counter,
       "Total number of reads that fell through to Bitcask on disk",
       Stats.total_cold_reads(FerricStore.Instance.get(:default))},
      {"ferricstore_used_memory_bytes", :gauge,
       "Total BEAM VM memory usage in bytes", :erlang.memory(:total)},
      {"ferricstore_keydir_used_bytes", :gauge,
       "Total ETS memory used by shard keydir tables in bytes",
       keydir_used_bytes()},
      {"ferricstore_uptime_seconds", :gauge,
       "Server uptime in seconds", Stats.uptime_seconds()},
      {"ferricstore_blocked_clients", :gauge,
       "Number of clients blocked on BLPOP/BRPOP/BLMOVE/BLMPOP",
       safe_ets_size(:ferricstore_waiters)},
      {"ferricstore_tracking_clients", :gauge,
       "Number of clients with client-side caching tracking enabled",
       safe_ets_size(:ferricstore_tracking_connections)},
      {"ferricstore_slowlog_entries", :gauge,
       "Current number of entries in the slow log",
       slowlog_len()}
    ]
  end

  # ---------------------------------------------------------------------------
  # Private: formatting
  # ---------------------------------------------------------------------------

  @spec format_metric({binary(), metric_type(), binary(), integer()}) :: binary()
  defp format_metric({name, type, help, value}) do
    type_str = Atom.to_string(type)

    "# HELP #{name} #{help}\n# TYPE #{name} #{type_str}\n#{name} #{value}"
  end

  # ---------------------------------------------------------------------------
  # Private: data sources
  # ---------------------------------------------------------------------------

  @spec connected_clients() :: non_neg_integer()
  defp connected_clients do
    ctx = FerricStore.Instance.get(:default)
    case ctx.connected_clients_fn do
      nil -> 0
      fun -> fun.()
    end
  end

  @spec keydir_used_bytes() :: non_neg_integer()
  defp keydir_used_bytes do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    Enum.reduce(0..(shard_count - 1), 0, fn i, acc ->
      keydir = :"keydir_#{i}"

      try do
        case :ets.info(keydir, :memory) do
          words when is_integer(words) ->
            acc + words * :erlang.system_info(:wordsize)
          _ ->
            acc
        end
      rescue
        ArgumentError -> acc
      end
    end)
  end

  @spec safe_ets_size(atom()) :: non_neg_integer()
  defp safe_ets_size(table) do
    try do
      case :ets.info(table, :size) do
        :undefined -> 0
        n when is_integer(n) -> n
        _ -> 0
      end
    rescue
      ArgumentError -> 0
    end
  end

  @spec slowlog_len() :: non_neg_integer()
  defp slowlog_len do
    try do
      Ferricstore.SlowLog.len()
    rescue
      _ -> 0
    catch
      :exit, _ -> 0
    end
  end

  # ---------------------------------------------------------------------------
  # Private: prefix metrics (key_count, keydir_bytes, hot/cold reads)
  # ---------------------------------------------------------------------------

  @spec prefix_metrics_text() :: binary()
  defp prefix_metrics_text do
    Ferricstore.PrefixMetricsCache.text()
  end

  # ---------------------------------------------------------------------------
  # Private: namespace metrics
  # ---------------------------------------------------------------------------

  # Produces the Prometheus text block for the two namespace-aware labeled
  # gauge families: ferricstore_namespace_window_ms and
  # ferricstore_namespace_durability.
  #
  # Each configured namespace prefix emits one sample line per metric family
  # with a `prefix` label. The durability gauge encodes the mode as an integer:
  # 1 for :quorum, 0 for :async.
  @spec namespace_metrics_text() :: binary()
  defp namespace_metrics_text do
    entries = namespace_entries()

    if entries == [] do
      ""
    else
      window_samples =
        Enum.map_join(entries, "\n", fn {prefix, window_ms, _durability, _ca, _cb} ->
          "ferricstore_namespace_window_ms{prefix=\"#{escape_label(prefix)}\"} #{window_ms}"
        end)

      durability_samples =
        Enum.map_join(entries, "\n", fn {prefix, _window_ms, durability, _ca, _cb} ->
          mode_str = Atom.to_string(durability)

          "ferricstore_namespace_durability{prefix=\"#{escape_label(prefix)}\",mode=\"#{mode_str}\"} 1"
        end)

      "# HELP ferricstore_namespace_window_ms Configured commit window in milliseconds per namespace prefix\n" <>
        "# TYPE ferricstore_namespace_window_ms gauge\n" <>
        window_samples <>
        "\n" <>
        "# HELP ferricstore_namespace_durability Configured durability mode per namespace prefix (1 = active)\n" <>
        "# TYPE ferricstore_namespace_durability gauge\n" <>
        durability_samples
    end
  end

  # Reads all namespace config entries from ETS. Returns an empty list when
  # the table does not exist or has no entries.
  @spec namespace_entries() :: [tuple()]
  defp namespace_entries do
    try do
      :ets.tab2list(:ferricstore_ns_config)
    rescue
      ArgumentError -> []
    end
  end

  # Escapes label values for Prometheus text format. Backslash, double quote,
  # and newline must be escaped.
  @spec escape_label(binary()) :: binary()
  defp escape_label(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end
end
