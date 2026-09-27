defmodule Ferricstore.Raft.WARaftBackend.StartupPreopen do
  @moduledoc false

  alias Ferricstore.Raft.WARaftStorage

  @app :ferricstore_waraft_backend
  @timeout 300_000
  @four_worker_memory_bytes 6 * 1024 * 1024 * 1024

  # The upstream partition supervisor starts children synchronously. Preparing
  # the storage handles first permits independent shard recovery to overlap,
  # without allowing any Raft server to observe a half-recovered storage child.
  def prepare(specs, memory_bytes, node_memory_limit) when is_list(specs) do
    default = default_concurrency(memory_bytes, node_memory_limit)

    concurrency =
      :ferricstore
      |> Application.get_env(:waraft_start_preopen_concurrency)
      |> then(&(&1 || default))
      |> case do
        value when is_integer(value) and value >= 0 -> min(value, 4)
        _invalid -> 0
      end

    if concurrency < 2 or length(specs) < 2 or Enum.any?(specs, &running_storage?/1) do
      :ok
    else
      prepare_next(specs, %{}, concurrency)
    end
  end

  @doc false
  def default_concurrency(memory_bytes, node_memory_limit) do
    base = Ferricstore.OperationalLimits.startup_recovery_concurrency(memory_bytes)

    if base == 3 and is_integer(node_memory_limit) and
         node_memory_limit >= @four_worker_memory_bytes and System.schedulers_online() >= 4,
       do: 4,
       else: base
  end

  defp prepare_next(pending, active, concurrency) do
    {pending, active} = fill_preopen_slots(pending, active, concurrency)

    if map_size(active) == 0 do
      :ok
    else
      monitors = Map.new(active, fn {ref, {pid, monitor}} -> {monitor, {ref, pid}} end)

      receive do
        {:waraft_preopen_ready, ref, pid} when is_map_key(active, ref) ->
          {^pid, monitor} = Map.fetch!(active, ref)
          Process.demonitor(monitor, [:flush])
          prepare_next(pending, Map.delete(active, ref), concurrency)

        {:waraft_preopen_failed, ref, error, stacktrace} when is_map_key(active, ref) ->
          abort_preopen_workers(active)
          {:error, {:preopen_failed, error, stacktrace}}

        {:DOWN, monitor, :process, pid, reason} when is_map_key(monitors, monitor) ->
          {_ref, ^pid} = Map.fetch!(monitors, monitor)
          abort_preopen_workers(active)
          {:error, {:preopen_down, reason}}
      after
        @timeout ->
          abort_preopen_workers(active)
          {:error, :preopen_timeout}
      end
    end
  end

  defp fill_preopen_slots([spec | rest], active, concurrency)
       when map_size(active) < concurrency do
    {pid, monitor, ref} = start_worker(spec)
    fill_preopen_slots(rest, Map.put(active, ref, {pid, monitor}), concurrency)
  end

  defp fill_preopen_slots(pending, active, _concurrency), do: {pending, active}

  defp abort_preopen_workers(active) do
    Enum.each(active, fn {_ref, {pid, monitor}} ->
      Process.demonitor(monitor, [:flush])
      Process.exit(pid, :kill)
    end)
  end

  defp running_storage?(%{table: table, partition: partition}) do
    :wa_raft_storage.default_name(table, partition)
    |> Process.whereis()
    |> is_pid()
  end

  defp start_worker(%{table: table, partition: partition}) do
    root = partition_root(table, partition)
    storage_name = :wa_raft_storage.default_name(table, partition)
    options = %{table: table, partition: partition, storage_name: storage_name}
    parent = self()
    ref = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        try do
          handle = WARaftStorage.open_unprepared(options, root)
          call_preopen_hook(options, root)
          owned = Enum.filter(:ets.all(), &(:ets.info(&1, :owner) == self()))
          :persistent_term.put(registry_key(root), {self(), ref})
          send(parent, {:waraft_preopen_ready, ref, self()})

          receive do
            {:waraft_preopen_adopt, ^ref, storage_pid} when is_pid(storage_pid) ->
              Enum.each(owned, fn table ->
                true = :ets.give_away(table, storage_pid, {__MODULE__, ref})
              end)

              send(storage_pid, {:waraft_preopen_adopted, ref, handle})

            {:waraft_preopen_cancel, ^ref} ->
              :ok
          end
        rescue
          error -> send(parent, {:waraft_preopen_failed, ref, error, __STACKTRACE__})
        after
          case :persistent_term.get(registry_key(root), nil) do
            {pid, ^ref} when pid == self() -> :persistent_term.erase(registry_key(root))
            _other -> :ok
          end
        end
      end)

    {pid, monitor, ref}
  end

  if Mix.env() == :test do
    defp call_preopen_hook(options, root) do
      case Application.get_env(:ferricstore, :waraft_start_preopen_hook) do
        hook when is_function(hook, 2) -> hook.(options, root)
        _missing -> :ok
      end
    end
  else
    defp call_preopen_hook(_options, _root), do: :ok
  end

  def take(root, options) do
    key = registry_key(root)

    case :persistent_term.get(key, nil) do
      {worker, ref} when is_pid(worker) and is_reference(ref) ->
        monitor = Process.monitor(worker)
        send(worker, {:waraft_preopen_adopt, ref, self()})

        receive do
          {:waraft_preopen_adopted, ^ref, handle} ->
            Process.demonitor(monitor, [:flush])
            :persistent_term.erase(key)
            {:ok, %{handle | options: options}}

          {:DOWN, ^monitor, :process, ^worker, reason} ->
            {:error, {:preopen_adoption_failed, reason}}
        after
          30_000 ->
            Process.demonitor(monitor, [:flush])
            {:error, :preopen_adoption_timeout}
        end

      _missing ->
        :not_preopened
    end
  end

  def cancel(specs) when is_list(specs) do
    Enum.each(specs, fn %{table: table, partition: partition} ->
      root = partition_root(table, partition)
      key = registry_key(root)

      case :persistent_term.get(key, nil) do
        {worker, ref} when is_pid(worker) ->
          send(worker, {:waraft_preopen_cancel, ref})
          :persistent_term.erase(key)

        _missing ->
          :ok
      end
    end)

    :ok
  end

  defp partition_root(table, partition) do
    @app
    |> :wa_raft_env.database_path()
    |> :wa_raft_part_sup.default_partition_path(table, partition)
    |> to_string()
  end

  defp registry_key(root), do: {__MODULE__, Path.expand(root)}
end
