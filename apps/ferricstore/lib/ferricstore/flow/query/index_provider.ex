defmodule Ferricstore.Flow.Query.IndexProvider do
  @moduledoc false

  @behaviour FerricStore.Flow.QueryIndexProvider

  alias Ferricstore.Flow.Query.{IndexRegistry, IndexSupervisor}

  @impl true
  def snapshot(ctx, shard_index), do: IndexRegistry.snapshot(ctx, shard_index)

  @impl true
  def child_specs(ctx) do
    [
      Supervisor.child_spec(
        {IndexSupervisor, instance_ctx: ctx},
        id: IndexSupervisor.child_id(ctx)
      )
    ]
  end
end
