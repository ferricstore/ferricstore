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

  @default_initial_delay_ms 2_000
  @default_error_sleep_ms 1_000
  @default_limit 100
  @default_max_claim_limit 1_000

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

  def handle_info({:EXIT, task, :normal}, %{task: task} = state), do: {:noreply, state}

  def handle_info({:EXIT, task, _reason}, %{task: task} = state) do
    Process.send_after(self(), :fire_due, state.config.error_sleep_ms)
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

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
