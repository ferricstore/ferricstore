defmodule Ferricstore.Raft.WARaftSegmentReader.DiskReaderSupervisor do
  @moduledoc false

  use DynamicSupervisor

  alias Ferricstore.Raft.WARaftSegmentReader.DiskReader

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 20, max_seconds: 10)
  end

  @spec ensure_reader(binary(), non_neg_integer(), pos_integer()) ::
          {:ok, pid()} | {:error, term()}
  def ensure_reader(root, lane, max_handles)
      when is_binary(root) and root != "" and is_integer(lane) and lane >= 0 and
             is_integer(max_handles) and max_handles > 0 do
    case DiskReader.whereis(root, lane) do
      pid when is_pid(pid) ->
        {:ok, pid}

      nil ->
        child_spec = %{
          id: {DiskReader, root, lane},
          start: {DiskReader, :start_link, [{root, lane, max_handles}]},
          restart: :transient,
          shutdown: 5_000,
          type: :worker
        }

        case DynamicSupervisor.start_child(__MODULE__, child_spec) do
          {:ok, pid} -> {:ok, pid}
          {:ok, pid, _info} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, :already_present} -> existing_reader(root, lane)
          {:error, reason} -> {:error, reason}
        end
    end
  catch
    :exit, reason -> {:error, reason}
  end

  def ensure_reader(_root, _lane, _max_handles),
    do: {:error, :invalid_segment_reader_root}

  @spec stop_reader(binary()) :: :ok
  def stop_reader(root) when is_binary(root) do
    root
    |> DiskReader.reader_pids()
    |> Enum.each(fn pid ->
      case DynamicSupervisor.terminate_child(__MODULE__, pid) do
        :ok -> :ok
        {:error, :not_found} -> :ok
        {:error, _reason} -> :ok
      end
    end)

    :ok
  catch
    :exit, _reason -> :ok
  end

  defp existing_reader(root, lane) do
    case DiskReader.whereis(root, lane) do
      pid when is_pid(pid) -> {:ok, pid}
      nil -> {:error, :segment_reader_start_race}
    end
  end
end
