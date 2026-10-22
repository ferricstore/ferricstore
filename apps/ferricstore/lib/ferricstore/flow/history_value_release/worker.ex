defmodule Ferricstore.Flow.HistoryValueRelease.Worker do
  @moduledoc false

  # Releases the values of Flows whose history apply reported trimmed (see
  # Ferricstore.Flow.HistoryValueRelease.notify/3). Hints are deduplicated and
  # bounded; every tick releases a batch, one history flush per shard.

  use GenServer

  require Logger

  alias Ferricstore.Flow.HistoryValueRelease
  alias Ferricstore.Raft.LocalLeadership

  @tick_ms 1_000
  @batch 64
  @max_pending 10_000

  def name(instance_name) when is_atom(instance_name),
    do: :"#{instance_name}.Flow.HistoryValueRelease"

  def child_spec(opts) do
    %{
      id: name(Keyword.fetch!(opts, :instance_ctx).name),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def start_link(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    GenServer.start_link(__MODULE__, opts, name: name(ctx.name))
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       ctx: Keyword.fetch!(opts, :instance_ctx),
       leads_shard?: Keyword.get(opts, :leads_shard?, &LocalLeadership.leads?/2),
       release: Keyword.get(opts, :release, &HistoryValueRelease.release_shard/3),
       pending: MapSet.new(),
       queue: :queue.new(),
       timer: nil
     }}
  end

  @impl true
  def handle_cast({:trimmed, shard_index, id, partition_key}, state) do
    flow = {shard_index, id, partition_key}

    if MapSet.member?(state.pending, flow) or MapSet.size(state.pending) >= @max_pending do
      {:noreply, state}
    else
      state = %{
        state
        | pending: MapSet.put(state.pending, flow),
          queue: :queue.in(flow, state.queue)
      }

      {:noreply, schedule_tick(state)}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    {batch, queue} = take(state.queue, @batch, [])
    state = %{state | queue: queue, pending: MapSet.difference(state.pending, MapSet.new(batch))}

    batch
    |> Enum.group_by(&elem(&1, 0), fn {_shard_index, id, partition_key} -> {id, partition_key} end)
    |> Enum.each(fn {shard_index, flows} ->
      if state.leads_shard?.(state.ctx, shard_index),
        do: release(state, shard_index, flows)
    end)

    state = %{state | timer: nil}
    {:noreply, if(:queue.is_empty(state.queue), do: state, else: schedule_tick(state))}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp release(state, shard_index, flows) do
    case state.release.(state.ctx, shard_index, flows) do
      :ok ->
        :ok

      {:error, :feature_level_inactive} ->
        :ok

      {:error, reason} ->
        Logger.debug(
          "Flow history value release on shard #{shard_index} skipped: #{inspect(reason)}"
        )
    end
  catch
    kind, reason ->
      Logger.debug(
        "Flow history value release on shard #{shard_index} failed: #{inspect({kind, reason})}"
      )
  end

  defp take(queue, 0, acc), do: {Enum.reverse(acc), queue}

  defp take(queue, count, acc) do
    case :queue.out(queue) do
      {{:value, flow}, rest} -> take(rest, count - 1, [flow | acc])
      {:empty, queue} -> {Enum.reverse(acc), queue}
    end
  end

  defp schedule_tick(%{timer: nil} = state),
    do: %{state | timer: Process.send_after(self(), :tick, @tick_ms)}

  defp schedule_tick(state), do: state
end
