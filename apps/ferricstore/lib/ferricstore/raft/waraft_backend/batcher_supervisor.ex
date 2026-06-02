defmodule Ferricstore.Raft.WARaftBackend.BatcherSupervisor do
  @moduledoc false

  use DynamicSupervisor

  alias Ferricstore.Raft.WARaftBackend.Batcher

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 20, max_seconds: 10)
  end

  @spec ensure_started(non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def ensure_started(shard_index, opts)
      when is_integer(shard_index) and shard_index >= 0 and is_list(opts) do
    case Process.whereis(__MODULE__) do
      nil ->
        {:error, :supervisor_not_started}

      _pid ->
        child_spec = %{
          id: {:waraft_namespace_batcher, shard_index},
          start: {Batcher, :start_link, [shard_index, opts]},
          restart: :permanent,
          shutdown: 5_000,
          type: :worker
        }

        case DynamicSupervisor.start_child(__MODULE__, child_spec) do
          {:ok, _pid} -> :ok
          {:ok, _pid, _info} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, :already_present} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec stop_all(non_neg_integer()) :: :ok
  def stop_all(shard_count) when is_integer(shard_count) and shard_count > 0 do
    if Process.whereis(__MODULE__) do
      Enum.each(0..(shard_count - 1), &terminate_batcher/1)
    end

    :ok
  end

  def stop_all(_shard_count), do: :ok

  defp terminate_batcher(shard_index) do
    case Process.whereis(Batcher.name(shard_index)) do
      nil ->
        :ok

      pid ->
        case DynamicSupervisor.terminate_child(__MODULE__, pid) do
          :ok -> :ok
          {:error, :not_found} -> :ok
          _other -> :ok
        end
    end
  end
end
