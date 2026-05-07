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
  | `ferricstore_bitcask_*`                  | gauge   | per-shard checkpoint atomics  |
  | `ferricstore_prefix_*`                   | mixed   | `PrefixMetricsCache`          |
  | `ferricstore_quorum_*`                   | counter | `QuorumMetrics` telemetry     |
  | `ferricstore_flow_lmdb_*`                | gauge   | per-shard projection atomics  |

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
    checkpoint = checkpoint_metrics_text()
    prefix = prefix_metrics_text()
    quorum = Ferricstore.QuorumMetrics.prometheus_text()

    [base, ns, checkpoint, prefix, quorum]
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
      {"ferricstore_connected_clients", :gauge, "Number of active client connections",
       connected_clients()},
      {"ferricstore_total_connections_received", :counter,
       "Total number of TCP connections accepted since startup", Stats.total_connections()},
      {"ferricstore_total_commands_processed", :counter,
       "Total number of commands dispatched since startup", Stats.total_commands()},
      {"ferricstore_hot_reads_total", :counter,
       "Total number of reads served from the ETS hot cache", Stats.total_hot_reads()},
      {"ferricstore_cold_reads_total", :counter,
       "Total number of reads that fell through to Bitcask on disk", Stats.total_cold_reads()},
      {"ferricstore_used_memory_bytes", :gauge, "Total BEAM VM memory usage in bytes",
       :erlang.memory(:total)},
      {"ferricstore_keydir_used_bytes", :gauge,
       "Total ETS memory used by shard keydir tables in bytes", keydir_used_bytes()},
      {"ferricstore_uptime_seconds", :gauge, "Server uptime in seconds", Stats.uptime_seconds()},
      {"ferricstore_blocked_clients", :gauge,
       "Number of clients blocked on BLPOP/BRPOP/BLMOVE/BLMPOP",
       safe_ets_size(:ferricstore_waiters)},
      {"ferricstore_tracking_clients", :gauge,
       "Number of clients with client-side caching tracking enabled",
       safe_ets_size(:ferricstore_tracking_connections)},
      {"ferricstore_slowlog_entries", :gauge, "Current number of entries in the slow log",
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
    case default_instance() do
      nil -> 0
      ctx -> connected_clients(ctx)
    end
  end

  defp connected_clients(ctx) do
    case ctx.connected_clients_fn do
      nil -> 0
      fun -> fun.()
    end
  rescue
    _ -> 0
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
  # Private: per-shard checkpoint/release cursor metrics
  # ---------------------------------------------------------------------------

  @spec checkpoint_metrics_text() :: binary()
  defp checkpoint_metrics_text do
    ctx = default_instance()

    [
      checkpoint_metric_family(
        "ferricstore_bitcask_last_applied_index",
        "Last Raft index applied by the Bitcask-backed state machine per shard",
        fn shard -> atomic_metric(ctx, :last_applied_index, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_last_released_cursor_index",
        "Last Raft index released for log compaction per shard",
        fn shard -> atomic_metric(ctx, :last_released_cursor_index, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_replay_safe_index",
        "Last replay-safe Raft index marker durably persisted per shard",
        fn shard -> atomic_metric(ctx, :replay_safe_index, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_replay_safe_requested_index",
        "Highest replay-safe Raft index requested for durable marker persistence per shard",
        fn shard -> atomic_metric(ctx, :replay_safe_requested_index, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_replay_safe_lag",
        "Difference between requested and durable replay-safe marker index per shard",
        fn shard ->
          requested = atomic_metric(ctx, :replay_safe_requested_index, shard)
          durable = atomic_metric(ctx, :replay_safe_index, shard)

          max(requested - durable, 0)
        end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_replay_safe_persist_failures_total",
        "Total replay-safe marker persist failures per shard",
        fn shard -> atomic_metric(ctx, :replay_safe_persist_failures, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_flow_lmdb_replay_safe_index",
        "Last Flow LMDB projection replay-safe Raft index durably persisted per shard",
        fn shard -> atomic_metric(ctx, :flow_lmdb_replay_safe_index, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_flow_lmdb_replay_safe_requested_index",
        "Highest Flow LMDB projection replay-safe Raft index requested per shard",
        fn shard -> atomic_metric(ctx, :flow_lmdb_replay_safe_requested_index, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_flow_lmdb_replay_safe_lag",
        "Difference between requested and durable Flow LMDB projection replay-safe index per shard",
        fn shard ->
          requested = atomic_metric(ctx, :flow_lmdb_replay_safe_requested_index, shard)
          durable = atomic_metric(ctx, :flow_lmdb_replay_safe_index, shard)

          max(requested - durable, 0)
        end
      ),
      checkpoint_metric_family(
        "ferricstore_flow_lmdb_replay_safe_persist_failures_total",
        "Total Flow LMDB replay-safe marker persist failures per shard",
        fn shard -> atomic_metric(ctx, :flow_lmdb_replay_safe_persist_failures, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_flow_lmdb_mirror_enqueue_failures_total",
        "Total Flow LMDB mirror enqueue failures per shard",
        fn shard -> atomic_metric(ctx, :flow_lmdb_mirror_enqueue_failures, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_flow_lmdb_mirror_degraded",
        "Whether Flow LMDB cold projection is degraded for this shard",
        fn shard -> atomic_metric(ctx, :flow_lmdb_mirror_degraded, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_release_cursor_gap",
        "Difference between last applied and last released Raft cursor per shard",
        fn shard ->
          last_applied = atomic_metric(ctx, :last_applied_index, shard)
          last_released = atomic_metric(ctx, :last_released_cursor_index, shard)

          max(last_applied - last_released, 0)
        end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_pending_release_cursor_checkpoint_count",
        "Number of shard checkpoint dependencies blocking Raft cursor release",
        fn shard -> atomic_metric(ctx, :pending_release_cursor_checkpoint_count, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_release_cursor_blocked_apply_count",
        "Consecutive applies whose Raft cursor release was blocked by replay-safety compensation",
        fn shard -> atomic_metric(ctx, :release_cursor_blocked_apply_count, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_checkpoint_dirty",
        "Whether the shard has uncheckpointed Bitcask data",
        fn shard -> atomic_metric(ctx, :checkpoint_flags, shard) end
      ),
      checkpoint_metric_family(
        "ferricstore_bitcask_checkpoint_in_flight",
        "Whether a Bitcask fsync checkpoint is currently in flight",
        fn shard -> atomic_metric(ctx, :checkpoint_in_flight, shard) end
      )
    ]
    |> Enum.join("\n")
  end

  defp checkpoint_metric_family(name, help, value_fun) when is_function(value_fun, 1) do
    samples =
      0..(shard_count() - 1)
      |> Enum.map_join("\n", fn shard ->
        "#{name}{shard_index=\"#{shard}\"} #{value_fun.(shard)}"
      end)

    "# HELP #{name} #{help}\n# TYPE #{name} gauge\n#{samples}"
  end

  defp atomic_metric(nil, _field, _shard), do: 0

  defp atomic_metric(ctx, field, shard)
       when is_atom(field) and is_integer(shard) and shard >= 0 do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        index = shard + 1
        if index <= :atomics.info(ref).size, do: :atomics.get(ref, index), else: 0

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp shard_count do
    Application.get_env(:ferricstore, :shard_count, 4)
  end

  defp default_instance do
    FerricStore.Instance.get(:default)
  rescue
    ArgumentError -> nil
  end

  # ---------------------------------------------------------------------------
  # Private: namespace metrics
  # ---------------------------------------------------------------------------

  # Produces the Prometheus text block for namespace commit windows.
  @spec namespace_metrics_text() :: binary()
  defp namespace_metrics_text do
    entries = namespace_entries()

    if entries == [] do
      ""
    else
      window_samples =
        Enum.map_join(entries, "\n", fn {prefix, window_ms, _ca, _cb} ->
          "ferricstore_namespace_window_ms{prefix=\"#{escape_label(prefix)}\"} #{window_ms}"
        end)

      "# HELP ferricstore_namespace_window_ms Configured commit window in milliseconds per namespace prefix\n" <>
        "# TYPE ferricstore_namespace_window_ms gauge\n" <>
        window_samples
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
