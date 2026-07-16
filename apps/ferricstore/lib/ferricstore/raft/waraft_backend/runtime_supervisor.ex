defmodule Ferricstore.Raft.WARaftBackend.RuntimeSupervisor do
  @moduledoc false

  use Supervisor

  alias Ferricstore.Raft.WARaftSegmentReader.TableOwner, as: ApplyProjectionTableOwner
  alias Ferricstore.Store.BlobStore.TableOwner, as: BlobTableOwner
  alias Ferricstore.Store.ETSTableHeir

  @apply_projection_table_heir Ferricstore.Raft.WARaftSegmentReader.TableHeir
  @blob_table_heir Ferricstore.Store.BlobStore.TableHeir
  @kernel_child_id __MODULE__
  @owner_wait_attempts 100
  @owner_wait_ms 10

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @spec ensure_started() :: :ok | {:error, term()}
  def ensure_started do
    case ensure_tables() do
      :ok -> :ok
      {:error, :table_owner_unavailable} -> ensure_runtime_started()
    end
  end

  @spec stop() :: :ok
  def stop do
    case Supervisor.terminate_child(:kernel_sup, @kernel_child_id) do
      :ok ->
        _ = Supervisor.delete_child(:kernel_sup, @kernel_child_id)
        :ok

      {:error, :not_found} ->
        :ok
    end
  end

  @impl true
  def init(:ok) do
    children = [
      Supervisor.child_spec(
        {ETSTableHeir, name: @blob_table_heir},
        id: @blob_table_heir
      ),
      BlobTableOwner,
      Supervisor.child_spec(
        {ETSTableHeir, name: @apply_projection_table_heir},
        id: @apply_projection_table_heir
      ),
      ApplyProjectionTableOwner
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp ensure_runtime_started do
    case Supervisor.start_child(:kernel_sup, child_spec([])) do
      {:ok, _pid} ->
        await_table_owner(@owner_wait_attempts)

      {:ok, _pid, _info} ->
        await_table_owner(@owner_wait_attempts)

      {:error, {:already_started, _pid}} ->
        await_table_owner(@owner_wait_attempts)

      {:error, :already_present} ->
        restart_present_runtime()

      {:error, {:shutdown, _reason}} ->
        await_table_owner(@owner_wait_attempts)

      {:error, reason} ->
        {:error, {:waraft_runtime_start_failed, reason}}
    end
  end

  defp restart_present_runtime do
    case Supervisor.restart_child(:kernel_sup, @kernel_child_id) do
      {:ok, _pid} ->
        await_table_owner(@owner_wait_attempts)

      {:ok, _pid, _info} ->
        await_table_owner(@owner_wait_attempts)

      {:error, :running} ->
        await_table_owner(@owner_wait_attempts)

      {:error, reason} ->
        {:error, {:waraft_runtime_restart_failed, reason}}
    end
  end

  defp await_table_owner(0), do: {:error, :waraft_table_owner_unavailable}

  defp await_table_owner(attempts) do
    case ensure_tables() do
      :ok ->
        :ok

      {:error, :table_owner_unavailable} ->
        Process.sleep(@owner_wait_ms)
        await_table_owner(attempts - 1)
    end
  end

  defp ensure_tables do
    with :ok <- BlobTableOwner.ensure_tables(),
         :ok <- ApplyProjectionTableOwner.ensure_table() do
      :ok
    end
  end
end
