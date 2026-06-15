defmodule Ferricstore.Flow.Scheduler do
  @moduledoc """
  Background schedule fire loop.

  The loop blocks on `FLOW.CLAIM_DUE` for the internal schedule type instead of
  polling. Work remains shard-owned and Raft-guarded; this process only asks for
  due schedule records and executes the target create/reschedule sequence.
  """

  use GenServer

  alias Ferricstore.Flow.Schedule

  @default_initial_delay_ms 2_000
  @default_error_sleep_ms 1_000
  @default_limit 100

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    if enabled?() do
      initial_delay_ms = Keyword.get(opts, :initial_delay_ms, scheduler_initial_delay_ms())
      Process.send_after(self(), :fire_due, initial_delay_ms)
    end

    {:ok, %{ctx: Keyword.get(opts, :ctx, FerricStore.Instance.get(:default)), task: nil}}
  end

  @impl true
  def handle_info(:fire_due, %{task: task} = state) when is_pid(task), do: {:noreply, state}

  def handle_info(:fire_due, state) do
    parent = self()
    ctx = Map.fetch!(state, :ctx)

    {:ok, task} =
      Task.start_link(fn ->
        result =
          Schedule.fire_due(ctx,
            worker: worker(),
            limit: scheduler_limit(),
            block_ms: 0
          )

        send(parent, {:fire_due_done, self(), result})
      end)

    {:noreply, %{state | task: task}}
  end

  def handle_info({:fire_due_done, task, {:ok, %{fired: fired}}}, %{task: task} = state)
      when fired > 0 do
    Process.send_after(self(), :fire_due, 0)
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:fire_due_done, task, _result}, %{task: task} = state) do
    Process.send_after(self(), :fire_due, scheduler_error_sleep_ms())
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:fire_due_done, _old_task, _result}, state), do: {:noreply, state}

  def handle_info({:EXIT, task, :normal}, %{task: task} = state), do: {:noreply, state}

  def handle_info({:EXIT, task, _reason}, %{task: task} = state) do
    Process.send_after(self(), :fire_due, scheduler_error_sleep_ms())
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  defp enabled? do
    Application.get_env(:ferricstore, :flow_scheduler_enabled, true) == true
  end

  defp worker, do: "ferricstore-scheduler:" <> Atom.to_string(node())

  defp scheduler_limit do
    env_integer(:flow_scheduler_limit, @default_limit)
  end

  defp scheduler_initial_delay_ms do
    env_integer(:flow_scheduler_initial_delay_ms, @default_initial_delay_ms)
  end

  defp scheduler_error_sleep_ms do
    case Application.get_env(:ferricstore, :flow_scheduler_error_sleep_ms) do
      value when is_integer(value) and value >= 0 ->
        value

      _ ->
        env_integer(:flow_scheduler_idle_sleep_ms, @default_error_sleep_ms)
    end
  end

  defp env_integer(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value >= 0 -> value
      _ -> default
    end
  end
end
