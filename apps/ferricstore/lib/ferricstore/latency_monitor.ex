defmodule Ferricstore.LatencyMonitor do
  @moduledoc """
  The LATENCY command's data, as Redis keeps it.

  * The latency monitor: with `latency-monitor-threshold` (milliseconds,
    0 = off, the default) above 0, a command that took at least that long
    adds a sample to the `command` event: one per second (the second's
    worst), the latest #{160} kept, and the event's all-time worst.
  * Latency tracking: with `latency-tracking` on, every command's run time
    counts into its histogram of power-of-two microsecond buckets
    (LATENCY HISTOGRAM). Redis keeps it on by default; FerricStore keeps it
    off, so commands do not pay for a counter update each.

  Samples are recorded where commands are timed for the slow log
  (`Ferricstore.SlowLog`), on the node that ran them.
  """

  use GenServer

  import Bitwise

  @events :ferricstore_latency_events
  @histograms :ferricstore_latency_histograms
  @samples_kept 160
  @threshold_key :ferricstore_latency_monitor_threshold_ms
  @tracking_key :ferricstore_latency_tracking

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # ---------------------------------------------------------------------------
  # Configuration

  @spec threshold_ms() :: non_neg_integer()
  def threshold_ms, do: :persistent_term.get(@threshold_key, 0)

  @spec set_threshold_ms(non_neg_integer()) :: :ok
  def set_threshold_ms(ms) when is_integer(ms) and ms >= 0,
    do: :persistent_term.put(@threshold_key, ms)

  @spec tracking?() :: boolean()
  def tracking?, do: :persistent_term.get(@tracking_key, false)

  @spec set_tracking(boolean()) :: :ok
  def set_tracking(on?) when is_boolean(on?), do: :persistent_term.put(@tracking_key, on?)

  # ---------------------------------------------------------------------------
  # Recording

  @doc "Records that `command` took `duration_us` microseconds."
  @spec record(binary(), non_neg_integer(), integer()) :: :ok
  def record(command, duration_us, now_s \\ System.os_time(:second)) do
    threshold = threshold_ms()

    if threshold > 0 and duration_us >= threshold * 1_000 do
      GenServer.cast(__MODULE__, {:sample, "command", now_s, div(duration_us, 1_000)})
    end

    if tracking?(), do: count(String.downcase(command), duration_us)
    :ok
  rescue
    _error -> :ok
  end

  defp count(command, duration_us) do
    bucket = bucket(duration_us)
    :ets.update_counter(@histograms, {command, :calls}, 1, {{command, :calls}, 0})
    :ets.update_counter(@histograms, {command, bucket}, 1, {{command, bucket}, 0})
  end

  # The smallest power of two at or above the run time, as Redis's buckets.
  defp bucket(duration_us) when duration_us <= 1, do: 1
  defp bucket(duration_us), do: 1 <<< ceil_log2(duration_us)

  defp ceil_log2(n), do: n |> Kernel.-(1) |> Integer.digits(2) |> length()

  @doc false
  @spec sync() :: :ok
  def sync, do: GenServer.call(__MODULE__, :sync)

  @doc false
  @spec reset_histograms() :: :ok
  def reset_histograms do
    :ets.delete_all_objects(@histograms)
    :ok
  end

  # ---------------------------------------------------------------------------
  # LATENCY

  @spec command([binary()]) :: term()
  def command([subcommand | args]) do
    case {String.upcase(subcommand), args} do
      {"LATEST", []} ->
        latest()

      {"HISTORY", [event]} ->
        history(event)

      {"RESET", events} ->
        reset(events)

      {"GRAPH", [event]} ->
        graph(event)

      {"DOCTOR", []} ->
        doctor()

      {"HISTOGRAM", commands} ->
        histogram(commands)

      {"HELP", []} ->
        help()

      {name, _args} when name in ~w(LATEST HISTORY GRAPH DOCTOR HELP) ->
        arity("latency|" <> String.downcase(name))

      _other ->
        {:error, "ERR unknown subcommand '#{subcommand}'. Try LATENCY HELP."}
    end
  end

  def command([]), do: arity("latency")

  defp arity(name), do: {:error, "ERR wrong number of arguments for '#{name}' command"}

  defp latest do
    for {event, %{samples: [{time, latency} | _], max: max}} <- events() do
      [event, time, latency, max]
    end
  end

  defp history(event) do
    case :ets.lookup(@events, event) do
      [{^event, %{samples: samples}}] -> samples |> Enum.reverse() |> Enum.map(&Tuple.to_list/1)
      [] -> []
    end
  end

  defp reset([]), do: GenServer.call(__MODULE__, :reset_all)
  defp reset(events), do: GenServer.call(__MODULE__, {:reset, events})

  defp graph(event) do
    case :ets.lookup(@events, event) do
      [{^event, %{samples: [_ | _] = samples, max: max}}] -> render_graph(event, samples, max)
      _none -> {:error, "ERR No samples available for event '#{event}'"}
    end
  end

  # Oldest to newest, one column a sample, scaled between the series' low and
  # high over four rows; each column is labelled with how long ago it was.
  defp render_graph(event, samples, max) do
    chronological = Enum.reverse(samples)
    latencies = Enum.map(chronological, &elem(&1, 1))
    {low, high} = Enum.min_max(latencies)
    span = max(high - low, 1)
    now = System.os_time(:second)

    rows =
      for row <- 4..1//-1 do
        Enum.map_join(latencies, fn latency ->
          level = round((latency - low) / span * 3) + 1
          if level >= row, do: if(level == row, do: "_", else: "#"), else: " "
        end)
      end

    ages = Enum.map_join(chronological, fn {time, _latency} -> age_label(now - time) end)

    Enum.join(
      [
        "#{event} - high #{high} ms, low #{low} ms (all time high #{max} ms)",
        String.duplicate("-", 80) | rows
      ] ++ [ages, ""],
      "\n"
    )
  end

  defp age_label(seconds) when seconds < 60, do: "s"
  defp age_label(seconds) when seconds < 3_600, do: "m"
  defp age_label(_seconds), do: "h"

  defp doctor do
    cond do
      threshold_ms() == 0 ->
        "I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this " <>
          "FerricStore node. You may use \"CONFIG SET latency-monitor-threshold " <>
          "<milliseconds>.\" in order to enable it.\n"

      events() == [] ->
        "Dave, no latency spike was observed during the lifetime of this FerricStore " <>
          "node, not in the slightest bit. I honestly think you ought to sleep tonight.\n"

      true ->
        lines =
          events()
          |> Enum.with_index(1)
          |> Enum.map(fn {{event, %{samples: samples, max: max}}, index} ->
            latencies = Enum.map(samples, &elem(&1, 1))
            average = Enum.sum(latencies) / length(latencies)

            "#{index}. #{event}: #{length(latencies)} latency spikes (average " <>
              "#{round(average)}ms, worst #{Enum.max(latencies)}ms). Worst all time event " <>
              "#{max}ms."
          end)

        Enum.join(
          [
            "Dave, I have observed latency spikes in this FerricStore node. You don't " <>
              "mind talking about it, do you Dave?",
            "" | lines
          ] ++
            [
              "",
              "The command spikes are commands that ran at least " <>
                "latency-monitor-threshold milliseconds; SLOWLOG GET shows which ones.",
              ""
            ],
          "\n"
        )
    end
  end

  defp histogram(commands) do
    wanted = commands |> Enum.map(&String.downcase/1) |> MapSet.new()

    @histograms
    |> :ets.tab2list()
    |> Enum.group_by(fn {{command, _bucket}, _count} -> command end)
    |> Enum.filter(fn {command, _rows} -> wanted == MapSet.new() or command in wanted end)
    |> Map.new(fn {command, rows} ->
      calls = Enum.find_value(rows, 0, fn {{_command, key}, n} -> if key == :calls, do: n end)

      buckets =
        rows
        |> Enum.reject(fn {{_command, key}, _n} -> key == :calls end)
        |> Enum.map(fn {{_command, bucket}, n} -> {bucket, n} end)
        |> Enum.sort()
        |> Enum.map_reduce(0, fn {bucket, n}, total -> {{bucket, total + n}, total + n} end)
        |> elem(0)
        |> Map.new()

      {command, %{"calls" => calls, "histogram_usec" => buckets}}
    end)
  end

  defp help do
    [
      "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "DOCTOR",
      "    Return a human readable latency analysis report.",
      "GRAPH <event>",
      "    Return an ASCII latency graph for the <event> class.",
      "HISTORY <event>",
      "    Return time-latency samples for the <event> class.",
      "LATEST",
      "    Return the latest latency samples for all events.",
      "RESET [<event> ...]",
      "    Reset latency data of one or more <event> classes.",
      "    (default: reset all data for all event classes)",
      "HISTOGRAM [COMMAND ...]",
      "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names.",
      "    If no commands are specified then all histograms are replied.",
      "HELP",
      "    Print this help."
    ]
  end

  defp events, do: @events |> :ets.tab2list() |> Enum.sort()

  # ---------------------------------------------------------------------------
  # GenServer

  @impl true
  def init(_opts) do
    :ets.new(@events, [:set, :public, :named_table, {:read_concurrency, true}])

    :ets.new(@histograms, [
      :set,
      :public,
      :named_table,
      {:write_concurrency, true},
      {:read_concurrency, true}
    ])

    set_threshold_ms(Application.get_env(:ferricstore, :latency_monitor_threshold_ms, 0))
    set_tracking(Application.get_env(:ferricstore, :latency_tracking, false) == true)
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:sample, event, time, latency}, state) do
    entry =
      case :ets.lookup(@events, event) do
        [{^event, entry}] -> entry
        [] -> %{samples: [], max: 0}
      end

    samples =
      case entry.samples do
        [{^time, previous} | older] -> [{time, max(previous, latency)} | older]
        samples -> Enum.take([{time, latency} | samples], @samples_kept)
      end

    :ets.insert(@events, {event, %{samples: samples, max: max(entry.max, latency)}})
    {:noreply, state}
  end

  @impl true
  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  def handle_call(:reset_all, _from, state) do
    count = :ets.info(@events, :size)
    :ets.delete_all_objects(@events)
    {:reply, count, state}
  end

  def handle_call({:reset, events}, _from, state) do
    count =
      Enum.count(Enum.uniq(events), fn event ->
        :ets.member(@events, event) and :ets.delete(@events, event)
      end)

    {:reply, count, state}
  end
end
