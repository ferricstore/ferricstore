defmodule Ferricstore.Flow.Scheduler do
  @moduledoc """
  Background schedule fire loop.

  The loop performs bounded `FLOW.CLAIM_DUE` passes for the internal schedule
  type. A finite blocking window avoids idle polling while guaranteeing a
  rescan if another node applied a schedule without delivering a local wakeup.
  Work remains shard-owned and Raft-guarded; this process only asks for due
  schedule records and executes the target create/reschedule sequence.

  Overdue interval schedules use the schedule's `:fire_once` catch-up policy.
  One target is created on recovery, all additional elapsed periods are
  coalesced in constant time, and the next run is one full interval after the
  recovery fire. The scheduler never loops once per missed interval.
  """

  use GenServer

  alias Ferricstore.Flow.Schedule
  alias Ferricstore.Flow.Schedule.TargetOwnership

  @max_wake_tasks 32

  @default_initial_delay_ms 2_000
  @default_error_sleep_ms 1_000
  @default_limit 100
  @default_max_claim_limit 1_000

  def name(%{name: instance_name}), do: name(instance_name)
  def name(:default), do: __MODULE__
  def name(instance_name) when is_atom(instance_name), do: :"#{instance_name}.Flow.Scheduler"

  @doc false
  def notify_target_terminal(%{name: instance_name}, %{id: target_id, state: state} = record)
      when is_binary(target_id) and is_binary(state) do
    if Ferricstore.Flow.LMDB.terminal_state?(state) do
      case TargetOwnership.schedule_id(record) do
        id when is_binary(id) ->
          if pid = Process.whereis(name(instance_name)) do
            GenServer.cast(pid, {:target_terminal, id, target_id})
          end

        _other ->
          :ok
      end
    end

    :ok
  end

  def notify_target_terminal(_ctx, _record), do: :ok

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    config = configuration(opts)

    if config.enabled? do
      Process.send_after(self(), :fire_due, config.initial_delay_ms)
    end

    {:ok,
     %{
       ctx: Keyword.get(opts, :ctx, FerricStore.Instance.get(:default)),
       task: nil,
       wake_tasks: MapSet.new(),
       config: config
     }}
  end

  @doc false
  def configuration(opts \\ []) do
    max_claim_limit =
      positive_application_integer(:flow_max_claim_limit, @default_max_claim_limit)

    %{
      enabled?: boolean_setting(opts, :enabled, :flow_scheduler_enabled, true),
      limit:
        bounded_positive_integer_setting(
          opts,
          :limit,
          :flow_scheduler_limit,
          @default_limit,
          max_claim_limit
        ),
      initial_delay_ms:
        non_negative_integer_setting(
          opts,
          :initial_delay_ms,
          :flow_scheduler_initial_delay_ms,
          @default_initial_delay_ms
        ),
      error_sleep_ms:
        non_negative_integer_setting(
          opts,
          :error_sleep_ms,
          :flow_scheduler_error_sleep_ms,
          @default_error_sleep_ms
        )
    }
  end

  @impl true
  def handle_cast({:target_terminal, schedule_id, target_id}, state) do
    if state.config.enabled? and MapSet.size(state.wake_tasks) < @max_wake_tasks do
      parent = self()

      {:ok, task} =
        Task.start_link(fn ->
          result =
            Schedule.wake_queued(
              state.ctx,
              schedule_id,
              target_id,
              System.system_time(:millisecond)
            )

          send(parent, {:wake_queued_done, self(), result})
        end)

      {:noreply, %{state | wake_tasks: MapSet.put(state.wake_tasks, task)}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info(:fire_due, %{config: %{enabled?: false}} = state), do: {:noreply, state}

  def handle_info(:fire_due, %{task: task} = state) when is_pid(task), do: {:noreply, state}

  def handle_info(:fire_due, state) do
    parent = self()
    ctx = Map.fetch!(state, :ctx)
    config = Map.fetch!(state, :config)

    {:ok, task} =
      Task.start_link(fn ->
        result =
          Schedule.fire_due(ctx,
            worker: worker(),
            limit: config.limit,
            block_ms: max(config.error_sleep_ms, 1)
          )

        send(parent, {:fire_due_done, self(), result})
      end)

    {:noreply, %{state | task: task}}
  end

  def handle_info(
        {:fire_due_done, task, {:ok, %{fired: fired, skipped: skipped}}},
        %{task: task} = state
      )
      when fired > 0 or skipped > 0 do
    Process.send_after(self(), :fire_due, 0)
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:fire_due_done, task, _result}, %{task: task} = state) do
    Process.send_after(self(), :fire_due, state.config.error_sleep_ms)
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:fire_due_done, _old_task, _result}, state), do: {:noreply, state}

  def handle_info({:wake_queued_done, task, result}, state) do
    state = %{state | wake_tasks: MapSet.delete(state.wake_tasks, task)}

    if result == :woken do
      Process.send_after(self(), :fire_due, 0)
    end

    {:noreply, state}
  end

  def handle_info({:EXIT, task, :normal}, %{task: task} = state), do: {:noreply, state}

  def handle_info({:EXIT, task, _reason}, %{task: task} = state) do
    Process.send_after(self(), :fire_due, state.config.error_sleep_ms)
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:EXIT, pid, _reason}, state) do
    {:noreply, %{state | wake_tasks: MapSet.delete(state.wake_tasks, pid)}}
  end

  defp worker, do: "ferricstore-scheduler:" <> Atom.to_string(node())

  defp boolean_setting(opts, option, env, default) do
    case Keyword.get(opts, option, Application.get_env(:ferricstore, env, default)) do
      value when is_boolean(value) -> value
      _invalid -> false
    end
  end

  defp bounded_positive_integer_setting(opts, option, env, default, maximum) do
    opts
    |> Keyword.get(option, Application.get_env(:ferricstore, env, default))
    |> case do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
    |> min(maximum)
  end

  defp positive_application_integer(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end

  defp non_negative_integer_setting(opts, option, env, default) do
    case Keyword.get(opts, option, Application.get_env(:ferricstore, env, default)) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> default
    end
  end
end
