defmodule Ferricstore.Flow.RetentionSweeper do
  @moduledoc """
  Periodic Flow active-timeout enforcement and terminal retention cleanup.

  Cleanup itself is a normal Ra-backed Flow command, so correctness stays in the
  state machine. This process only schedules small background batches.
  """

  use GenServer

  require Logger

  alias Ferricstore.Store.DiskPressure

  @default_initial_delay_ms 600_000
  @default_interval_ms 600_000
  @default_catchup_delay_ms 100
  @default_limit 100
  @default_pressure_interval_ms 1_000
  @default_pressure_limit 10_000
  @default_pressure_compaction_interval_ms 60_000

  @spec name(FerricStore.Instance.t() | atom()) :: atom()
  def name(%{name: instance_name}), do: name(instance_name)
  def name(:default), do: __MODULE__

  def name(instance_name) when is_atom(instance_name),
    do: :"#{instance_name}.Flow.RetentionSweeper"

  def start_link(opts \\ []) do
    if enabled?() do
      name = Keyword.get(opts, :name, default_name(opts))
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
          pressure_interval_ms:
            config_pos_int(
              :flow_retention_sweeper_pressure_interval_ms,
              @default_pressure_interval_ms
            ),
          pressure_limit:
            config_pos_int(:flow_retention_sweeper_pressure_limit, @default_pressure_limit),
          pressure_compaction_interval_ms:
            config_non_neg_int(
              :flow_retention_sweeper_pressure_compaction_interval_ms,
              @default_pressure_compaction_interval_ms
            ),
          compaction_running?: false,
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
        pressure_interval_ms:
          config_pos_int(
            :flow_retention_sweeper_pressure_interval_ms,
            @default_pressure_interval_ms
          ),
        pressure_limit:
          config_pos_int(:flow_retention_sweeper_pressure_limit, @default_pressure_limit),
        pressure_compaction_interval_ms:
          config_non_neg_int(
            :flow_retention_sweeper_pressure_compaction_interval_ms,
            @default_pressure_compaction_interval_ms
          ),
        compaction_running?: false,
        consecutive_limit_hits: 0,
        last_sweep: nil
      }
  end

  @impl true
  def init(opts) do
    instance_ctx = Keyword.get(opts, :instance_ctx)

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
      pressure_interval_ms:
        opt_pos_int(
          opts,
          :pressure_interval_ms,
          :flow_retention_sweeper_pressure_interval_ms,
          @default_pressure_interval_ms
        ),
      pressure_limit:
        opt_pos_int(
          opts,
          :pressure_limit,
          :flow_retention_sweeper_pressure_limit,
          @default_pressure_limit
        ),
      pressure_compaction_interval_ms:
        opt_non_neg_int(
          opts,
          :pressure_compaction_interval_ms,
          :flow_retention_sweeper_pressure_compaction_interval_ms,
          @default_pressure_compaction_interval_ms
        ),
      pressure_detector_fun:
        Keyword.get(opts, :pressure_detector_fun, fn -> instance_pressure?(instance_ctx) end),
      cleanup_fun:
        Keyword.get(opts, :cleanup_fun, fn cleanup_opts ->
          retention_cleanup(instance_ctx, cleanup_opts)
        end),
      compaction_fun:
        Keyword.get(opts, :compaction_fun, fn -> trigger_merge_checks(instance_ctx) end),
      compaction_ref: nil,
      last_compaction_mono_ms: nil,
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
       pressure_interval_ms: state.pressure_interval_ms,
       pressure_limit: state.pressure_limit,
       pressure_compaction_interval_ms: state.pressure_compaction_interval_ms,
       compaction_running?: state.compaction_ref != nil,
       consecutive_limit_hits: state.consecutive_limit_hits,
       last_sweep: state.last_sweep
     }, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    started = System.monotonic_time()
    pressure? = pressure?(state)
    limit = effective_limit(state, pressure?)

    result =
      try do
        state.cleanup_fun.(limit: limit)
      rescue
        error ->
          Logger.warning("Flow retention sweeper failed: #{Exception.message(error)}")
          {:error, error}
      end

    duration_us = duration_us(started)
    {status, counts, reason} = normalize_result(result)
    limit_hit? = status == :ok and cleanup_limit_hit?(counts, limit)
    catchup? = status == :ok and cleanup_work_done?(counts)

    {state, compaction_triggered?} =
      maybe_trigger_compaction(status, counts, pressure?, limit_hit?, state)

    emit_sweep(
      status,
      counts,
      reason,
      limit_hit?,
      pressure?,
      compaction_triggered?,
      duration_us,
      limit,
      state
    )

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
            active_timeouts: Map.get(counts, :active_timeouts, 0),
            pressure?: pressure?,
            duration_us: duration_us,
            limit: limit,
            limit_hit?: limit_hit?,
            compaction_triggered?: compaction_triggered?,
            finished_at_ms: System.system_time(:millisecond)
          }
      }

    schedule(next_delay_ms(catchup?, pressure?, state))
    {:noreply, next_state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{compaction_ref: ref} = state) do
    {:noreply, %{state | compaction_ref: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  defp schedule(delay_ms) do
    Process.send_after(self(), :sweep, delay_ms)
  end

  defp enabled? do
    Application.get_env(:ferricstore, :flow_retention_sweeper_enabled, true) == true
  end

  defp default_name(opts) do
    case Keyword.get(opts, :instance_ctx) do
      %{name: instance_name} -> name(instance_name)
      _other -> __MODULE__
    end
  end

  defp retention_cleanup(nil, opts), do: FerricStore.flow_retention_cleanup(opts)

  defp retention_cleanup(instance_ctx, opts),
    do: FerricStore.Impl.flow_retention_cleanup(instance_ctx, opts)

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

  defp effective_limit(state, true), do: max(state.limit, state.pressure_limit)
  defp effective_limit(state, false), do: state.limit

  defp normalize_result({:ok, counts}) when is_map(counts), do: {:ok, counts, :none}
  defp normalize_result({:error, reason}), do: {:error, %{}, reason}
  defp normalize_result(other), do: {:error, %{}, other}

  defp cleanup_limit_hit?(counts, limit) do
    Map.get(counts, :flows, 0) + Map.get(counts, :active_timeouts, 0) >= limit or
      Map.get(counts, :history, 0) >= limit or
      Map.get(counts, :values, 0) >= limit
  end

  defp cleanup_work_done?(counts) do
    Enum.any?([:flows, :history, :values, :active_timeouts], &(Map.get(counts, &1, 0) > 0))
  end

  defp emit_sweep(
         status,
         counts,
         reason,
         limit_hit?,
         pressure?,
         compaction_triggered?,
         duration_us,
         limit,
         state
       ) do
    :telemetry.execute(
      [:ferricstore, :flow, :retention_sweeper, :sweep],
      %{
        duration_us: duration_us,
        flows: Map.get(counts, :flows, 0),
        history: Map.get(counts, :history, 0),
        values: Map.get(counts, :values, 0),
        active_timeouts: Map.get(counts, :active_timeouts, 0),
        limit: limit
      },
      %{
        status: status,
        reason: reason,
        pressure?: pressure?,
        compaction_triggered?: compaction_triggered?,
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
        active_timeouts: Map.get(counts, :active_timeouts, 0),
        limit: state.limit
      },
      %{consecutive_limit_hits: state.consecutive_limit_hits + 1}
    )
  end

  defp next_delay_ms(true, true, state),
    do: min(state.catchup_delay_ms, state.pressure_interval_ms)

  defp next_delay_ms(true, false, state), do: state.catchup_delay_ms
  defp next_delay_ms(false, true, state), do: state.pressure_interval_ms
  defp next_delay_ms(false, false, state), do: state.interval_ms

  defp pressure?(state) do
    state.pressure_detector_fun.()
  rescue
    _ -> false
  catch
    _kind, _reason -> false
  end

  defp instance_pressure?(instance_ctx) do
    operational_pressure?() or memory_pressure?() or disk_pressure?(instance_ctx)
  end

  defp operational_pressure? do
    Ferricstore.OperationalGuard.pressure?()
  rescue
    _ -> false
  catch
    _kind, _reason -> false
  end

  defp memory_pressure? do
    Ferricstore.MemoryGuard.reject_writes?() or Ferricstore.MemoryGuard.skip_promotion?()
  rescue
    _ -> false
  catch
    _kind, _reason -> false
  end

  defp disk_pressure?(instance_ctx) do
    ctx = instance_ctx || FerricStore.Instance.get(:default)

    Enum.any?(0..(ctx.shard_count - 1), fn shard_index ->
      DiskPressure.under_pressure?(ctx, shard_index)
    end)
  rescue
    _ -> false
  catch
    _kind, _reason -> false
  end

  defp maybe_trigger_compaction(:ok, counts, pressure?, limit_hit?, state) do
    if should_trigger_compaction?(counts, pressure?, limit_hit?, state) do
      case Task.start(fn -> state.compaction_fun.() end) do
        {:ok, pid} ->
          ref = Process.monitor(pid)

          {%{
             state
             | compaction_ref: ref,
               last_compaction_mono_ms: System.monotonic_time(:millisecond)
           }, true}

        _other ->
          {state, false}
      end
    else
      {state, false}
    end
  end

  defp maybe_trigger_compaction(_status, _counts, _pressure?, _limit_hit?, state),
    do: {state, false}

  defp should_trigger_compaction?(counts, pressure?, limit_hit?, state) do
    state.pressure_compaction_interval_ms > 0 and
      state.compaction_ref == nil and
      (pressure? or limit_hit?) and
      cleanup_count(counts) > 0 and
      compaction_due?(state)
  end

  defp cleanup_count(counts) do
    Map.get(counts, :flows, 0) + Map.get(counts, :history, 0) + Map.get(counts, :values, 0)
  end

  defp compaction_due?(%{last_compaction_mono_ms: nil}), do: true

  defp compaction_due?(state) do
    System.monotonic_time(:millisecond) - state.last_compaction_mono_ms >=
      state.pressure_compaction_interval_ms
  end

  defp trigger_merge_checks(instance_ctx) do
    ctx = instance_ctx || FerricStore.Instance.get(:default)

    if is_integer(ctx.shard_count) and ctx.shard_count > 0 do
      Enum.each(0..(ctx.shard_count - 1), fn shard_index ->
        scheduler = Ferricstore.Merge.Scheduler.scheduler_name(ctx, shard_index)

        if Process.whereis(scheduler) do
          Ferricstore.Merge.Scheduler.trigger_check(scheduler)
        end
      end)
    end

    :ok
  rescue
    error ->
      Logger.warning("Flow retention pressure compaction failed: #{Exception.message(error)}")
      :ok
  catch
    kind, reason ->
      Logger.warning("Flow retention pressure compaction failed: #{inspect({kind, reason})}")
      :ok
  end

  defp duration_us(started) do
    System.monotonic_time()
    |> Kernel.-(started)
    |> System.convert_time_unit(:native, :microsecond)
  end
end
