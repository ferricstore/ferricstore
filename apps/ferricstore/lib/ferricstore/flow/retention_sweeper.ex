defmodule Ferricstore.Flow.RetentionSweeper do
  @moduledoc """
  Periodic Flow retention cleanup for terminal records and their history/value refs.

  Cleanup itself is a normal Ra-backed Flow command, so correctness stays in the
  state machine. This process only schedules small background batches.
  """

  use GenServer

  require Logger

  @default_initial_delay_ms 600_000
  @default_interval_ms 600_000
  @default_catchup_delay_ms 100
  @default_limit 100

  def start_link(opts \\ []) do
    if enabled?() do
      name = Keyword.get(opts, :name, __MODULE__)
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      :ignore
    end
  end

  def info(name \\ __MODULE__) do
    case process_for(name) do
      nil ->
        %{
          enabled: enabled?(),
          running: false,
          interval_ms: config_pos_int(:flow_retention_sweeper_interval_ms, @default_interval_ms),
          catchup_delay_ms:
            config_non_neg_int(
              :flow_retention_sweeper_catchup_delay_ms,
              @default_catchup_delay_ms
            ),
          limit: config_pos_int(:flow_retention_sweeper_limit, @default_limit),
          consecutive_limit_hits: 0,
          last_sweep: nil
        }

      pid ->
        GenServer.call(pid, :info)
    end
  catch
    :exit, _reason ->
      %{
        enabled: enabled?(),
        running: false,
        interval_ms: config_pos_int(:flow_retention_sweeper_interval_ms, @default_interval_ms),
        catchup_delay_ms:
          config_non_neg_int(:flow_retention_sweeper_catchup_delay_ms, @default_catchup_delay_ms),
        limit: config_pos_int(:flow_retention_sweeper_limit, @default_limit),
        consecutive_limit_hits: 0,
        last_sweep: nil
      }
  end

  @impl true
  def init(opts) do
    state = %{
      interval_ms:
        opt_pos_int(opts, :interval_ms, :flow_retention_sweeper_interval_ms, @default_interval_ms),
      catchup_delay_ms:
        opt_non_neg_int(
          opts,
          :catchup_delay_ms,
          :flow_retention_sweeper_catchup_delay_ms,
          @default_catchup_delay_ms
        ),
      limit: opt_pos_int(opts, :limit, :flow_retention_sweeper_limit, @default_limit),
      cleanup_fun: Keyword.get(opts, :cleanup_fun, &FerricStore.flow_retention_cleanup/1),
      consecutive_limit_hits: 0,
      last_sweep: nil
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
  def handle_call(:info, _from, state) do
    {:reply,
     %{
       enabled: true,
       running: true,
       interval_ms: state.interval_ms,
       catchup_delay_ms: state.catchup_delay_ms,
       limit: state.limit,
       consecutive_limit_hits: state.consecutive_limit_hits,
       last_sweep: state.last_sweep
     }, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    started = System.monotonic_time()

    result =
      try do
        state.cleanup_fun.(limit: state.limit)
      rescue
        error ->
          Logger.warning("Flow retention sweeper failed: #{Exception.message(error)}")
          {:error, error}
      end

    duration_us = duration_us(started)
    {status, counts, reason} = normalize_result(result)
    limit_hit? = status == :ok and cleanup_limit_hit?(counts, state.limit)
    emit_sweep(status, counts, reason, limit_hit?, duration_us, state)
    maybe_emit_backlog(limit_hit?, counts, state)
    maybe_emit_error(status, reason, state)

    next_state =
      %{
        state
        | consecutive_limit_hits: if(limit_hit?, do: state.consecutive_limit_hits + 1, else: 0),
          last_sweep: %{
            status: status,
            reason: reason,
            flows: Map.get(counts, :flows, 0),
            history: Map.get(counts, :history, 0),
            values: Map.get(counts, :values, 0),
            duration_us: duration_us,
            limit: state.limit,
            limit_hit?: limit_hit?,
            finished_at_ms: System.system_time(:millisecond)
          }
      }

    schedule(if(limit_hit?, do: state.catchup_delay_ms, else: state.interval_ms))
    {:noreply, next_state}
  end

  defp schedule(delay_ms) do
    Process.send_after(self(), :sweep, delay_ms)
  end

  defp enabled? do
    Application.get_env(:ferricstore, :flow_retention_sweeper_enabled, true) == true
  end

  defp process_for(pid) when is_pid(pid), do: pid
  defp process_for(name) when is_atom(name), do: Process.whereis(name)
  defp process_for(_name), do: nil

  defp config_pos_int(env_key, default) do
    case Application.get_env(:ferricstore, env_key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp config_non_neg_int(env_key, default) do
    case Application.get_env(:ferricstore, env_key, default) do
      value when is_integer(value) and value >= 0 -> value
      _ -> default
    end
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

  defp normalize_result({:ok, counts}) when is_map(counts), do: {:ok, counts, :none}
  defp normalize_result({:error, reason}), do: {:error, %{}, reason}
  defp normalize_result(other), do: {:error, %{}, other}

  defp cleanup_limit_hit?(counts, limit) do
    Enum.any?([:flows, :history, :values], fn key ->
      Map.get(counts, key, 0) >= limit
    end)
  end

  defp emit_sweep(status, counts, reason, limit_hit?, duration_us, state) do
    :telemetry.execute(
      [:ferricstore, :flow, :retention_sweeper, :sweep],
      %{
        duration_us: duration_us,
        flows: Map.get(counts, :flows, 0),
        history: Map.get(counts, :history, 0),
        values: Map.get(counts, :values, 0),
        limit: state.limit
      },
      %{
        status: status,
        reason: reason,
        limit_hit?: limit_hit?,
        consecutive_limit_hits: if(limit_hit?, do: state.consecutive_limit_hits + 1, else: 0)
      }
    )
  end

  defp maybe_emit_error(:ok, _reason, _state), do: :ok

  defp maybe_emit_error(:error, reason, state) do
    :telemetry.execute(
      [:ferricstore, :flow, :retention_sweeper, :error],
      %{count: 1},
      %{
        reason: reason,
        limit: state.limit,
        consecutive_limit_hits: state.consecutive_limit_hits
      }
    )
  end

  defp maybe_emit_backlog(false, _counts, _state), do: :ok

  defp maybe_emit_backlog(true, counts, state) do
    :telemetry.execute(
      [:ferricstore, :flow, :retention_sweeper, :backlog],
      %{
        flows: Map.get(counts, :flows, 0),
        history: Map.get(counts, :history, 0),
        values: Map.get(counts, :values, 0),
        limit: state.limit
      },
      %{consecutive_limit_hits: state.consecutive_limit_hits + 1}
    )
  end

  defp duration_us(started) do
    System.monotonic_time()
    |> Kernel.-(started)
    |> System.convert_time_unit(:native, :microsecond)
  end
end
