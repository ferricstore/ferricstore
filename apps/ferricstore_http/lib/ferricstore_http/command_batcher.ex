defmodule FerricstoreHttp.CommandBatcher do
  @moduledoc false

  use GenServer

  alias FerricstoreHttp.{Auth, CommandService, Config, Deadline, Metrics}

  @partition_supervisor Module.concat(__MODULE__, Partitions)
  @task_supervisor Module.concat(__MODULE__, TaskSupervisor)

  @type executor :: (term(), [{CommandService.prepared(), Deadline.t()}], Config.t() ->
                       [{:ok, map()} | {:error, term()}])

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts) do
    case Map.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @spec execute(Auth.Context.t(), CommandService.prepared(), Config.t(), Deadline.t()) ::
          {:ok, map()} | {:error, term()}
  def execute(authenticated, prepared, %Config{command_batching_enabled: true} = config, deadline) do
    if Process.whereis(@partition_supervisor) do
      key = group_key(authenticated, config)
      server = {:via, PartitionSupervisor, {@partition_supervisor, key}}
      request(server, authenticated, prepared, config, deadline)
    else
      direct(authenticated, prepared, config, deadline)
    end
  end

  def execute(authenticated, prepared, %Config{} = config, deadline) do
    direct(authenticated, prepared, config, deadline)
  end

  @spec request(
          GenServer.server(),
          Auth.Context.t(),
          CommandService.prepared(),
          Config.t(),
          Deadline.t()
        ) :: {:ok, map()} | {:error, term()}
  def request(server, authenticated, prepared, %Config{} = config, deadline) do
    case Deadline.remaining_ms(deadline) do
      {:ok, timeout_ms} ->
        call(server, authenticated, prepared, config, deadline, timeout_ms)

      {:error, :deadline_exceeded} ->
        {:error, :request_timeout}
    end
  end

  @spec partition_supervisor() :: module()
  def partition_supervisor, do: @partition_supervisor

  @spec task_supervisor() :: module()
  def task_supervisor, do: @task_supervisor

  @impl GenServer
  def init(opts) do
    {:ok,
     %{
       executor: Map.get(opts, :executor, &execute_many/3),
       max_commands: Map.fetch!(opts, :max_commands),
       pending: %{},
       task_supervisor: Map.get(opts, :task_supervisor, @task_supervisor),
       waiter_monitors: %{},
       window_ms: Map.fetch!(opts, :window_ms)
     }}
  end

  @impl GenServer
  def handle_call({:execute, authenticated, prepared, config, deadline}, from, state) do
    request = %{
      authenticated: authenticated,
      config: config,
      count: CommandService.command_count(prepared),
      deadline: deadline,
      from: from,
      prepared: prepared
    }

    key = group_key(authenticated, config)
    state = make_room(state, key, request.count)
    monitor = Process.monitor(elem(from, 0))
    request = Map.put(request, :monitor, monitor)
    state = %{state | waiter_monitors: Map.put(state.waiter_monitors, monitor, key)}
    {:noreply, enqueue(state, key, request)}
  end

  @impl GenServer
  def handle_info({:flush, key, token}, state) do
    case Map.get(state.pending, key) do
      %{token: ^token} = group -> {:noreply, flush(state, key, group)}
      _stale_or_missing -> {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    {:noreply, drop_waiter(state, monitor)}
  end

  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.pending, fn {_key, group} ->
      Enum.each(group.requests, &GenServer.reply(&1.from, {:error, :internal_error}))
    end)

    :ok
  end

  defp call(server, authenticated, prepared, config, deadline, timeout_ms) do
    message = {:execute, authenticated, prepared, config, deadline}
    GenServer.call(server, message, timeout_ms + 100)
  catch
    :exit, {:timeout, _call} -> {:error, :request_timeout}
    :exit, _reason -> {:error, :internal_error}
  end

  defp direct(authenticated, prepared, config, deadline) do
    CommandService.execute_prepared(prepared, authenticated.session, config, deadline)
  end

  defp group_key(%Auth.Context{} = authenticated, config) do
    {config.backend, authenticated.cache_key, authenticated.session}
  end

  defp make_room(state, key, incoming_count) do
    case Map.get(state.pending, key) do
      %{command_count: count} = group when count + incoming_count > state.max_commands ->
        flush(state, key, group)

      _room_available ->
        state
    end
  end

  defp enqueue(state, key, request) do
    case Map.get(state.pending, key) do
      nil ->
        token = make_ref()
        timer = Process.send_after(self(), {:flush, key, token}, state.window_ms)

        group = %{
          command_count: request.count,
          requests: [request],
          timer: timer,
          token: token
        }

        put_in(state.pending[key], group)

      group ->
        updated = %{
          group
          | command_count: group.command_count + request.count,
            requests: [request | group.requests]
        }

        put_in(state.pending[key], updated)
    end
  end

  defp flush(state, key, group) do
    _cancelled = Process.cancel_timer(group.timer)
    requests = Enum.reverse(group.requests)
    state = remove_waiter_monitors(state, requests)
    execute_active(requests, state)
    %{state | pending: Map.delete(state.pending, key)}
  end

  defp execute_active(requests, state) do
    {active, expired} = Enum.split_with(requests, &active?/1)
    Enum.each(expired, &GenServer.reply(&1.from, {:error, :request_timeout}))

    if active != [] do
      task = fn -> safely_execute(active, state.executor) end

      case Task.Supervisor.start_child(state.task_supervisor, task) do
        {:ok, _pid} -> :ok
        {:error, _reason} -> reply_all(active, {:error, :internal_error})
      end
    end
  end

  defp active?(request), do: match?({:ok, _remaining}, Deadline.remaining_ms(request.deadline))

  defp safely_execute(requests, executor) do
    first = hd(requests)
    prepared = Enum.map(requests, &{&1.prepared, &1.deadline})
    command_count = Enum.sum_by(requests, & &1.count)
    :ok = Metrics.observe_command_batch(length(requests), command_count)
    results = executor.(first.authenticated.session, prepared, first.config)
    reply(requests, results)
  rescue
    _error -> reply_all(requests, {:error, :internal_error})
  catch
    _kind, _reason -> reply_all(requests, {:error, :internal_error})
  end

  defp execute_many(session, requests, config),
    do: CommandService.execute_many(requests, session, config)

  defp reply(requests, results) when is_list(results) and length(results) == length(requests) do
    Enum.zip_with(requests, results, fn request, result ->
      GenServer.reply(request.from, result)
    end)

    :ok
  end

  defp reply(requests, _invalid), do: reply_all(requests, {:error, :internal_error})

  defp reply_all(requests, result) do
    Enum.each(requests, &GenServer.reply(&1.from, result))
    :ok
  end

  defp drop_waiter(state, monitor) do
    case Map.pop(state.waiter_monitors, monitor) do
      {nil, _monitors} ->
        state

      {key, monitors} ->
        drop_from_group(state, key, monitor, monitors)
    end
  end

  defp drop_from_group(state, key, monitor, monitors) do
    case Map.get(state.pending, key) do
      nil ->
        %{state | waiter_monitors: monitors}

      group ->
        requests = Enum.reject(group.requests, &(&1.monitor == monitor))
        update_waiter_group(state, key, monitors, group, requests)
    end
  end

  defp update_waiter_group(state, key, monitors, group, []) do
    _cancelled = Process.cancel_timer(group.timer)
    %{state | pending: Map.delete(state.pending, key), waiter_monitors: monitors}
  end

  defp update_waiter_group(state, key, monitors, group, requests) do
    updated = %{group | requests: requests, command_count: Enum.sum_by(requests, & &1.count)}
    %{state | pending: Map.put(state.pending, key, updated), waiter_monitors: monitors}
  end

  defp remove_waiter_monitors(state, requests) do
    monitors =
      Enum.reduce(requests, state.waiter_monitors, fn request, monitors ->
        Process.demonitor(request.monitor, [:flush])
        Map.delete(monitors, request.monitor)
      end)

    %{state | waiter_monitors: monitors}
  end
end
