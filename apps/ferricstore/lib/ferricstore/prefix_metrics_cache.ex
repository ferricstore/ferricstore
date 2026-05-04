defmodule Ferricstore.PrefixMetricsCache do
  @moduledoc """
  Cached Prometheus text for expensive per-prefix metrics.

  Per-prefix key counts require folding every shard keydir ETS table. That is
  useful operationally, but too expensive to run directly inside every
  `FERRICSTORE.METRICS` command. This process owns a small ETS cache so scrapes
  read the last completed scan and only schedule refresh work when stale.
  """

  use GenServer

  alias Ferricstore.Stats

  @table :ferricstore_prefix_metrics_cache
  @cache_key :prefix_metrics_text
  @refreshing_key :refreshing
  @default_refresh_interval_ms 5_000

  @doc """
  Starts the prefix metrics cache.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns the last completed prefix metrics text block.

  If the cache is missing or stale, this schedules a background refresh and
  returns the previous value immediately. The first scrape may therefore omit
  prefix metrics until the refresh completes.
  """
  @spec text() :: binary()
  def text do
    now = System.monotonic_time(:millisecond)

    case cache_entry() do
      {:ok, text, refreshed_at} ->
        if stale?(now, refreshed_at), do: request_refresh()
        text

      :missing ->
        request_refresh()
        ""
    end
  end

  @doc """
  Rebuilds the prefix metrics cache synchronously.

  Tests and maintenance tooling can call this when they need a deterministic
  cache update.
  """
  @spec refresh_now(timeout()) :: :ok | {:error, :not_started}
  def refresh_now(timeout \\ 30_000) do
    case Process.whereis(__MODULE__) do
      nil -> {:error, :not_started}
      _pid -> GenServer.call(__MODULE__, :refresh, timeout)
    end
  end

  @doc """
  Clears cached prefix metrics.
  """
  @spec reset() :: :ok
  def reset do
    try do
      :ets.delete_all_objects(@table)
      :ok
    rescue
      ArgumentError -> :ok
    end
  end

  @impl true
  def init(_opts) do
    ensure_table()
    {:ok, %{}}
  end

  @impl true
  def handle_call(:refresh, _from, state) do
    refresh_cache()
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast(:refresh, state) do
    refresh_cache()
    {:noreply, state}
  end

  @impl true
  def handle_info({:ra_event, _from, _event}, state) do
    # Ra pipeline replies can occasionally arrive late to a reused pid after a
    # test or supervisor restart. They are unrelated to metrics cache state.
    {:noreply, state}
  end

  @spec cache_entry() :: {:ok, binary(), integer()} | :missing
  defp cache_entry do
    try do
      case :ets.lookup(@table, @cache_key) do
        [{@cache_key, text, refreshed_at}] when is_binary(text) and is_integer(refreshed_at) ->
          {:ok, text, refreshed_at}

        _ ->
          :missing
      end
    rescue
      ArgumentError -> :missing
    end
  end

  @spec stale?(integer(), integer()) :: boolean()
  defp stale?(now, refreshed_at), do: now - refreshed_at >= refresh_interval_ms()

  @spec request_refresh() :: :ok
  defp request_refresh do
    case Process.whereis(__MODULE__) do
      nil ->
        :ok

      _pid ->
        if mark_refreshing() do
          GenServer.cast(__MODULE__, :refresh)
        end

        :ok
    end
  end

  @spec mark_refreshing() :: boolean()
  defp mark_refreshing do
    try do
      :ets.insert_new(@table, {@refreshing_key, true})
    rescue
      ArgumentError -> false
    end
  end

  @spec refresh_cache() :: :ok
  defp refresh_cache do
    try do
      text = scan_prefix_metrics_text()
      refreshed_at = System.monotonic_time(:millisecond)
      :ets.insert(@table, {@cache_key, text, refreshed_at})
      :ets.delete(@table, @refreshing_key)
      :ok
    rescue
      ArgumentError ->
        :ok
    after
      try do
        :ets.delete(@table, @refreshing_key)
      rescue
        ArgumentError -> :ok
      end
    end
  end

  @spec ensure_table() :: :ok
  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :named_table,
          :public,
          :set,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

        :ok

      _tid ->
        :ok
    end
  end

  @spec refresh_interval_ms() :: non_neg_integer()
  defp refresh_interval_ms do
    case Application.get_env(
           :ferricstore,
           :metrics_prefix_refresh_interval_ms,
           @default_refresh_interval_ms
         ) do
      value when is_integer(value) and value >= 0 -> value
      _ -> @default_refresh_interval_ms
    end
  end

  @spec scan_prefix_metrics_text() :: binary()
  defp scan_prefix_metrics_text do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)

    # Aggregate key counts and keydir bytes per prefix across all shards.
    now = System.os_time(:millisecond)

    prefix_data =
      Enum.reduce(0..(shard_count - 1), %{}, fn i, acc ->
        table = :"keydir_#{i}"

        try do
          :ets.foldl(
            fn {key, _value, exp, _lfu, _fid, _off, _vsize}, inner_acc ->
              # Skip expired keys (exp > 0 means has TTL, skip if past).
              if exp > 0 and exp <= now do
                inner_acc
              else
                prefix = Stats.extract_prefix(key)
                key_bytes = byte_size(key) + 8 + 8 + 64
                {count, bytes} = Map.get(inner_acc, prefix, {0, 0})
                Map.put(inner_acc, prefix, {count + 1, bytes + key_bytes})
              end
            end,
            acc,
            table
          )
        rescue
          _ -> acc
        catch
          _, _ -> acc
        end
      end)

    hotness_data =
      try do
        :ets.tab2list(:ferricstore_hotness)
        |> Map.new(fn {prefix, hot, cold} -> {prefix, {hot, cold}} end)
      rescue
        _ -> %{}
      catch
        _, _ -> %{}
      end

    format_prefix_metrics(prefix_data, hotness_data)
  end

  @spec format_prefix_metrics(map(), map()) :: binary()
  defp format_prefix_metrics(prefix_data, hotness_data) do
    if prefix_data == %{} and hotness_data == %{} do
      ""
    else
      all_prefixes =
        MapSet.union(
          MapSet.new(Map.keys(prefix_data)),
          MapSet.new(Map.keys(hotness_data))
        )
        |> Enum.sort()

      key_count_samples =
        Enum.map_join(all_prefixes, "\n", fn prefix ->
          {count, _bytes} = Map.get(prefix_data, prefix, {0, 0})
          "ferricstore_prefix_key_count{prefix=\"#{escape_label(prefix)}\"} #{count}"
        end)

      keydir_bytes_samples =
        Enum.map_join(all_prefixes, "\n", fn prefix ->
          {_count, bytes} = Map.get(prefix_data, prefix, {0, 0})
          "ferricstore_prefix_keydir_bytes{prefix=\"#{escape_label(prefix)}\"} #{bytes}"
        end)

      hot_reads_samples =
        Enum.map_join(all_prefixes, "\n", fn prefix ->
          {hot, _cold} = Map.get(hotness_data, prefix, {0, 0})
          "ferricstore_prefix_hot_reads{prefix=\"#{escape_label(prefix)}\"} #{hot}"
        end)

      cold_reads_samples =
        Enum.map_join(all_prefixes, "\n", fn prefix ->
          {_hot, cold} = Map.get(hotness_data, prefix, {0, 0})
          "ferricstore_prefix_cold_reads{prefix=\"#{escape_label(prefix)}\"} #{cold}"
        end)

      "# HELP ferricstore_prefix_key_count Number of live keys per prefix\n" <>
        "# TYPE ferricstore_prefix_key_count gauge\n" <>
        key_count_samples <>
        "\n" <>
        "# HELP ferricstore_prefix_keydir_bytes Estimated keydir ETS bytes per prefix\n" <>
        "# TYPE ferricstore_prefix_keydir_bytes gauge\n" <>
        keydir_bytes_samples <>
        "\n" <>
        "# HELP ferricstore_prefix_hot_reads Hot reads (ETS cache hits) per prefix\n" <>
        "# TYPE ferricstore_prefix_hot_reads counter\n" <>
        hot_reads_samples <>
        "\n" <>
        "# HELP ferricstore_prefix_cold_reads Cold reads (Bitcask fallbacks) per prefix\n" <>
        "# TYPE ferricstore_prefix_cold_reads counter\n" <>
        cold_reads_samples
    end
  end

  @spec escape_label(binary()) :: binary()
  defp escape_label(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end
end
