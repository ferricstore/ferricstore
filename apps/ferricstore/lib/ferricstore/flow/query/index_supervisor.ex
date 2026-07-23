defmodule Ferricstore.Flow.Query.IndexSupervisor do
  @moduledoc false

  use Supervisor

  alias Ferricstore.Flow.Query.{
    AdmissionController,
    CursorKeyStore,
    IndexCatalog,
    IndexLifecycleWorker,
    IndexRegistry,
    StatisticsStore,
    StatisticsWorker
  }

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    Supervisor.start_link(__MODULE__, opts, name: supervisor_name(ctx))
  end

  @spec child_id(map()) :: atom()
  def child_id(%{name: :default}), do: __MODULE__
  def child_id(%{name: name}) when is_atom(name), do: :"#{name}.Flow.Query.IndexSupervisor"

  @spec supervisor_name(map()) :: atom()
  def supervisor_name(ctx), do: child_id(ctx)

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)

    children = [
      Supervisor.child_spec(
        {AdmissionController, instance_ctx: ctx, name: AdmissionController.server_name(ctx)},
        id: AdmissionController.server_name(ctx)
      ),
      Supervisor.child_spec(
        {CursorKeyStore,
         instance_ctx: ctx,
         name: CursorKeyStore.server_name(ctx),
         key: Keyword.get(opts, :cursor_key)},
        id: CursorKeyStore.server_name(ctx)
      ),
      Supervisor.child_spec(
        {IndexRegistry,
         instance_ctx: ctx,
         name: IndexRegistry.server_name(ctx),
         catalog_path: Keyword.get(opts, :catalog_path, IndexCatalog.default_path())},
        id: IndexRegistry.server_name(ctx)
      ),
      Supervisor.child_spec(
        {IndexLifecycleWorker,
         instance_ctx: ctx,
         registry: IndexRegistry.server_name(ctx),
         name: IndexLifecycleWorker.name(ctx)},
        id: IndexLifecycleWorker.name(ctx)
      ),
      Supervisor.child_spec(
        {StatisticsStore, instance_ctx: ctx, name: StatisticsStore.server_name(ctx)},
        id: StatisticsStore.server_name(ctx)
      ),
      Supervisor.child_spec(
        {StatisticsWorker,
         instance_ctx: ctx,
         statistics_store: StatisticsStore.server_name(ctx),
         name: StatisticsWorker.server_name(ctx)},
        id: StatisticsWorker.server_name(ctx)
      )
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
