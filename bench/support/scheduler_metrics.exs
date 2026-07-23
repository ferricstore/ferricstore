defmodule Ferricstore.Bench.SchedulerMetrics do
  @moduledoc false

  @type wall_time :: %{required(atom()) => %{required(pos_integer()) => {integer(), integer()}}}

  @spec enable_wall_time() :: boolean()
  def enable_wall_time, do: :erlang.system_flag(:scheduler_wall_time, true)

  @spec restore_wall_time(boolean()) :: boolean()
  def restore_wall_time(previous), do: :erlang.system_flag(:scheduler_wall_time, previous)

  @spec wall_time_snapshot() :: wall_time()
  def wall_time_snapshot do
    normal = :erlang.system_info(:schedulers)
    dirty_cpu = :erlang.system_info(:dirty_cpu_schedulers)
    dirty_io = :erlang.system_info(:dirty_io_schedulers)

    entries =
      :scheduler_wall_time_all
      |> :erlang.statistics()
      |> Enum.sort_by(&elem(&1, 0))

    {normal_entries, entries} = Enum.split(entries, normal)
    {dirty_cpu_entries, dirty_io_entries} = Enum.split(entries, dirty_cpu)

    if length(dirty_io_entries) != dirty_io do
      raise "unexpected scheduler wall-time topology"
    end

    %{
      normal: Map.new(normal_entries, &wall_time_entry/1),
      dirty_cpu: Map.new(dirty_cpu_entries, &wall_time_entry/1),
      dirty_io: Map.new(dirty_io_entries, &wall_time_entry/1)
    }
  end

  @spec utilization(wall_time(), wall_time()) :: map()
  def utilization(before, after_snapshot) when is_map(before) and is_map(after_snapshot) do
    Map.new([:normal, :dirty_cpu, :dirty_io], fn scheduler_type ->
      {scheduler_type,
       group_utilization(
         Map.fetch!(before, scheduler_type),
         Map.fetch!(after_snapshot, scheduler_type)
       )}
    end)
  end

  @spec run_queues() :: map()
  def run_queues do
    normal_schedulers = :erlang.system_info(:schedulers_online)
    lengths = :erlang.statistics(:run_queue_lengths_all)
    {normal, aggregates} = Enum.split(lengths, normal_schedulers)

    case aggregates do
      [dirty_cpu, dirty_io] ->
        %{
          normal_total: Enum.sum(normal),
          normal_max: Enum.max(normal, fn -> 0 end),
          dirty_cpu: dirty_cpu,
          dirty_io: dirty_io
        }

      _unexpected ->
        raise "unexpected scheduler run-queue topology"
    end
  end

  defp group_utilization(before, after_snapshot) do
    deltas =
      Map.new(before, fn {id, {active_before, total_before}} ->
        {active_after, total_after} = Map.fetch!(after_snapshot, id)
        {id, {active_after - active_before, total_after - total_before}}
      end)

    active = Enum.sum_by(deltas, fn {_id, {value, _total}} -> value end)
    total = Enum.sum_by(deltas, fn {_id, {_active, value}} -> value end)

    maximum =
      deltas
      |> Enum.map(fn {_id, {scheduler_active, scheduler_total}} ->
        percentage(scheduler_active, scheduler_total)
      end)
      |> Enum.max(fn -> 0.0 end)

    %{
      schedulers: map_size(deltas),
      utilization_percent: percentage(active, total),
      max_scheduler_utilization_percent: maximum
    }
  end

  defp wall_time_entry({id, active, total}), do: {id, {active, total}}

  defp percentage(_active, total) when total <= 0, do: 0.0
  defp percentage(active, total), do: Float.round(active * 100 / total, 3)
end
