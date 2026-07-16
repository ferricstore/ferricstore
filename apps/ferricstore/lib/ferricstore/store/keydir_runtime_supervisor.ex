defmodule Ferricstore.Store.KeydirRuntimeSupervisor do
  @moduledoc false

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)

    shard_supervisor_opts = [
      name: Keyword.get(opts, :shard_supervisor_name, Ferricstore.Store.ShardSupervisor),
      data_dir: Keyword.get(opts, :data_dir, ctx.data_dir),
      shard_count: Keyword.get(opts, :shard_count, ctx.shard_count),
      instance_ctx: ctx
    ]

    children = [
      Supervisor.child_spec(
        {Ferricstore.Store.KeydirTableOwner, instance_ctx: ctx},
        id: Ferricstore.Store.KeydirTableOwner
      ),
      Supervisor.child_spec(
        {Ferricstore.Store.ShardSupervisor, shard_supervisor_opts},
        id: Ferricstore.Store.ShardSupervisor
      )
    ]

    # The table heir is deliberately supervised outside this subtree. If the
    # owner fails, it receives the ETS tables while all dependent shards stop;
    # the replacement owner reclaims them before shards restart.
    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 20, max_seconds: 10)
  end
end
