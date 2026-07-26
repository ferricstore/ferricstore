defmodule Ferricstore.Raft.WARaftBackend.RuntimeSupervisor do
  @moduledoc false

  use Supervisor

  alias Ferricstore.Raft.WARaftSegmentReader.TableOwner, as: ApplyProjectionTableOwner
  alias Ferricstore.Raft.WARaftSegmentReader.{DiskReader, DiskReaderSupervisor}
  alias Ferricstore.Raft.WARaftBackend.SyncGate.TableOwner, as: SyncAdmissionTableOwner
  alias Ferricstore.Store.ActiveFile.TableOwner, as: ActiveFileTableOwner
  alias Ferricstore.Store.BlobStore.TableOwner, as: BlobTableOwner
  alias Ferricstore.Store.ETSTableHeir

  @active_file_table_heir Ferricstore.Store.ActiveFile.TableHeir
  @apply_projection_table_heir Ferricstore.Raft.WARaftSegmentReader.TableHeir
  @blob_table_heir Ferricstore.Store.BlobStore.TableHeir
  @sync_admission_table_heir Ferricstore.Raft.WARaftBackend.SyncGate.TableHeir
  @kernel_child_id __MODULE__
  @owner_wait_attempts 100
  @owner_wait_ms 10

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @spec ensure_started() :: :ok | {:error, term()}
  def ensure_started do
    case ensure_runtime_ready() do
      :ok -> :ok
      {:error, :table_owner_unavailable} -> ensure_runtime_started()
      {:error, :runtime_component_unavailable} -> repair_runtime_components()
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
        {ETSTableHeir, name: @active_file_table_heir},
        id: @active_file_table_heir
      ),
      ActiveFileTableOwner,
      Supervisor.child_spec(
        {ETSTableHeir, name: @blob_table_heir},
        id: @blob_table_heir
      ),
      BlobTableOwner,
      Supervisor.child_spec(
        {ETSTableHeir, name: @apply_projection_table_heir},
        id: @apply_projection_table_heir
      ),
      ApplyProjectionTableOwner,
      disk_reader_registry_spec(),
      disk_reader_supervisor_spec(),
      Supervisor.child_spec(
        {ETSTableHeir, name: @sync_admission_table_heir},
        id: @sync_admission_table_heir
      ),
      SyncAdmissionTableOwner
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
    case ensure_runtime_ready() do
      :ok ->
        :ok

      {:error, reason}
      when reason in [:table_owner_unavailable, :runtime_component_unavailable] ->
        Process.sleep(@owner_wait_ms)
        await_table_owner(attempts - 1)
    end
  end

  defp ensure_runtime_ready do
    with :ok <- ensure_tables(),
         true <- is_pid(Process.whereis(DiskReader.registry())),
         true <- is_pid(Process.whereis(DiskReaderSupervisor)) do
      :ok
    else
      false -> {:error, :runtime_component_unavailable}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_tables do
    with :ok <- ActiveFileTableOwner.ensure_table(),
         :ok <- BlobTableOwner.ensure_tables(),
         :ok <- ApplyProjectionTableOwner.ensure_table(),
         :ok <- SyncAdmissionTableOwner.ensure_table() do
      :ok
    end
  end

  defp repair_runtime_components do
    case active_runtime_supervisor() do
      nil ->
        ensure_runtime_started()

      supervisor ->
        with :ok <- ensure_supervisor_child(supervisor, disk_reader_registry_spec()),
             :ok <- ensure_supervisor_child(supervisor, disk_reader_supervisor_spec()) do
          await_table_owner(@owner_wait_attempts)
        end
    end
  end

  defp ensure_supervisor_child(supervisor, child_spec) do
    case Supervisor.start_child(supervisor, child_spec) do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, :already_present} -> restart_supervisor_child(supervisor, child_spec.id)
      {:error, reason} -> {:error, {:runtime_component_start_failed, child_spec.id, reason}}
    end
  end

  defp restart_supervisor_child(supervisor, id) do
    case Supervisor.restart_child(supervisor, id) do
      {:ok, _pid} -> :ok
      {:ok, _pid, _info} -> :ok
      {:error, :running} -> :ok
      {:error, reason} -> {:error, {:runtime_component_restart_failed, id, reason}}
    end
  end

  defp active_runtime_supervisor do
    Process.whereis(__MODULE__) || Process.whereis(Ferricstore.Supervisor)
  end

  defp disk_reader_registry_spec do
    Supervisor.child_spec(
      {Registry, keys: :unique, name: DiskReader.registry()},
      id: DiskReader.registry()
    )
  end

  defp disk_reader_supervisor_spec do
    Supervisor.child_spec(DiskReaderSupervisor, id: DiskReaderSupervisor)
  end
end
