defmodule FerricstoreHttp.Server do
  @moduledoc false

  use Supervisor

  alias FerricstoreHttp.{Admission, Auth, CommandBatcher, Config, Listener, Metrics}
  alias FerricstoreHttp.Invocations.{DefinitionSeeder, Runner, SystemSession}

  @spec start_link(keyword() | Config.t()) :: Supervisor.on_start()
  def start_link(%Config{} = config), do: Supervisor.start_link(__MODULE__, config)

  def start_link(opts) when is_list(opts) do
    case Config.new(opts) do
      {:ok, config} -> Supervisor.start_link(__MODULE__, config)
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Supervisor
  def init(%Config{} = config) do
    children =
      [
        {Admission, config.max_in_flight_requests}
      ] ++
        metrics_children(config) ++
        auth_cache_children(config) ++
        command_batcher_children(config) ++ invocation_children(config) ++ [{Listener, config}]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp metrics_children(%Config{metrics_enabled: false}), do: []
  defp metrics_children(%Config{}), do: [Metrics]

  defp auth_cache_children(%Config{auth_cache_enabled: false}), do: []

  defp auth_cache_children(%Config{} = config) do
    [
      {Task.Supervisor, name: Auth.Cache.TaskSupervisor},
      {Auth.Cache,
       max_entries: config.auth_cache_max_entries,
       ttl_ms: config.auth_cache_ttl_ms,
       sweep_interval_ms: min(config.auth_cache_ttl_ms, 60_000)}
    ]
  end

  defp command_batcher_children(%Config{command_batching_enabled: false}), do: []

  defp command_batcher_children(%Config{} = config) do
    [
      {Task.Supervisor, name: CommandBatcher.task_supervisor()},
      {PartitionSupervisor,
       child_spec:
         {CommandBatcher,
          %{
            max_commands: config.command_batch_max_commands,
            name: nil,
            window_ms: config.command_batch_window_ms
          }},
       name: CommandBatcher.partition_supervisor(),
       partitions: config.command_batch_shards}
    ]
  end

  defp invocation_children(%Config{invocations_enabled: false}), do: []

  defp invocation_children(%Config{} = config) do
    workers =
      []
      |> maybe_add_definition_seeder(config)
      |> maybe_add_runner(config)

    case workers do
      [] -> []
      _workers -> [{SystemSession, config} | workers]
    end
  end

  defp maybe_add_definition_seeder(children, %Config{invocation_definitions_file: nil}),
    do: children

  defp maybe_add_definition_seeder(children, %Config{} = config),
    do: children ++ [{DefinitionSeeder, config}]

  defp maybe_add_runner(children, %Config{runner_enabled: false}), do: children
  defp maybe_add_runner(children, %Config{} = config), do: children ++ [{Runner, config}]
end
