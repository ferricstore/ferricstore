defmodule Ferricstore.Flow.RetentionSweeper do
  @moduledoc """
  Periodic Flow retention cleanup for terminal records and their history/value refs.

  Cleanup itself is a normal Ra-backed Flow command, so correctness stays in the
  state machine. This process only schedules small background batches.
  """

  use GenServer

  require Logger

  @default_initial_delay_ms 60_000
  @default_interval_ms 60_000
  @default_limit 100

  def start_link(opts \\ []) do
    if enabled?() do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    else
      :ignore
    end
  end

  @impl true
  def init(opts) do
    state = %{
      interval_ms:
        opt_pos_int(opts, :interval_ms, :flow_retention_sweeper_interval_ms, @default_interval_ms),
      limit: opt_pos_int(opts, :limit, :flow_retention_sweeper_limit, @default_limit)
    }

    initial_delay =
      opt_non_neg_int(
        opts,
        :initial_delay_ms,
        :flow_retention_sweeper_initial_delay_ms,
        @default_initial_delay_ms
      )

    schedule(initial_delay)
    {:ok, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    started = System.monotonic_time()

    result =
      try do
        FerricStore.flow_retention_cleanup(limit: state.limit)
      rescue
        error ->
          Logger.warning("Flow retention sweeper failed: #{Exception.message(error)}")
          {:error, error}
      end

    emit_sweep(result, state, started)
    schedule(state.interval_ms)
    {:noreply, state}
  end

  defp schedule(delay_ms) do
    Process.send_after(self(), :sweep, delay_ms)
  end

  defp enabled? do
    Application.get_env(:ferricstore, :flow_retention_sweeper_enabled, true) == true
  end

  defp opt_pos_int(opts, key, env_key, default) do
    case Keyword.get(opts, key, Application.get_env(:ferricstore, env_key, default)) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp opt_non_neg_int(opts, key, env_key, default) do
    case Keyword.get(opts, key, Application.get_env(:ferricstore, env_key, default)) do
      value when is_integer(value) and value >= 0 -> value
      _ -> default
    end
  end

  defp emit_sweep(result, state, started) do
    {status, counts, reason} =
      case result do
        {:ok, counts} when is_map(counts) -> {:ok, counts, :none}
        {:error, reason} -> {:error, %{}, reason}
        other -> {:error, %{}, other}
      end

    :telemetry.execute(
      [:ferricstore, :flow, :retention_sweeper, :sweep],
      %{
        duration_us: duration_us(started),
        flows: Map.get(counts, :flows, 0),
        history: Map.get(counts, :history, 0),
        values: Map.get(counts, :values, 0),
        limit: state.limit
      },
      %{status: status, reason: reason}
    )
  end

  defp duration_us(started) do
    System.monotonic_time()
    |> Kernel.-(started)
    |> System.convert_time_unit(:native, :microsecond)
  end
end
