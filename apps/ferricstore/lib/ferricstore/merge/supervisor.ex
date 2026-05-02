defmodule Ferricstore.Merge.Supervisor do
  @moduledoc """
  Supervises the merge subsystem: one `Semaphore` and N `Scheduler` processes.

  The supervision tree is:

  ```
  Ferricstore.Merge.Supervisor (:one_for_one)
  ├── Ferricstore.Merge.Semaphore       (node-level, capacity 1)
  ├── Ferricstore.Merge.Scheduler.0     (per-shard)
  ├── Ferricstore.Merge.Scheduler.1
  ├── Ferricstore.Merge.Scheduler.2
  └── Ferricstore.Merge.Scheduler.3
  ```

  The Semaphore must start before any Scheduler so that schedulers can acquire
  it immediately on their first check.

  ## Options

    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:shard_count` -- number of shards (default: 4)
    * `:merge_config` -- per-scheduler merge configuration override (map)
    * `:name` -- supervisor process name (default: `#{inspect(__MODULE__)}`)
    * `:instance_ctx` -- optional embedded instance context. When set, child
      semaphore and scheduler names are scoped to the instance so embedded
      shards cannot publish merge events to the default instance.
  """

  use Supervisor

  @doc "Starts the merge supervisor and all children."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    data_dir = Keyword.fetch!(opts, :data_dir)
    shard_count = Keyword.get(opts, :shard_count, 4)
    merge_config = Keyword.get(opts, :merge_config, %{})
    instance_ctx = Keyword.get(opts, :instance_ctx)
    semaphore_name = Keyword.get(opts, :semaphore_name, semaphore_name(instance_ctx))

    semaphore_child =
      Supervisor.child_spec(
        {Ferricstore.Merge.Semaphore, name: semaphore_name},
        id: :merge_semaphore
      )

    scheduler_children =
      Enum.map(0..(shard_count - 1), fn i ->
        Supervisor.child_spec(
          {Ferricstore.Merge.Scheduler,
           [
             name: Ferricstore.Merge.Scheduler.scheduler_name(instance_ctx, i),
             shard_index: i,
             data_dir: data_dir,
             merge_config: merge_config,
             semaphore: semaphore_name,
             instance_ctx: instance_ctx
           ]},
          id: :"merge_scheduler_#{i}"
        )
      end)

    children = [semaphore_child | scheduler_children]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp semaphore_name(%{name: :default}), do: Ferricstore.Merge.Semaphore
  defp semaphore_name(%{name: name}), do: :"#{name}.Merge.Semaphore"
  defp semaphore_name(_instance_ctx), do: Ferricstore.Merge.Semaphore
end
