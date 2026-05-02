defmodule FerricStore.Instance.Supervisor do
  @moduledoc """
  Per-instance supervision tree for a FerricStore instance.

  Starts all processes needed for the instance: shards, batchers,
  writers, merge schedulers, Raft system, MemoryGuard, Stats, etc.

  Each instance is fully isolated — its own ETS tables, Raft WAL,
  data directory, and process tree.
  """

  use Supervisor

  @doc """
  Starts the instance supervisor and all child processes.

  The instance context (`ctx`) must already be built via
  `FerricStore.Instance.build/2` before calling this.
  """
  @spec start_link(atom(), keyword()) :: Supervisor.on_start()
  def start_link(name, opts) do
    Supervisor.start_link(__MODULE__, {name, opts}, name: :"#{name}.Supervisor")
  end

  @impl true
  def init({name, opts}) do
    ctx = FerricStore.Instance.build(name, opts)

    # Ensure data directory layout exists
    Ferricstore.DataDir.ensure_layout!(ctx.data_dir, ctx.shard_count)

    children = [
      # Stats and MemoryGuard are global application processes today. The
      # instance context owns isolated counters/flags, but these GenServers are
      # not instance-scoped yet, so embedded instances must not start duplicates.
      {Ferricstore.Store.ShardSupervisor,
       [
         name: :"#{name}.ShardSupervisor",
         data_dir: ctx.data_dir,
         shard_count: ctx.shard_count,
         instance_ctx: ctx,
         raft_enabled: ctx.raft_enabled
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 20, max_seconds: 10)
  end
end
