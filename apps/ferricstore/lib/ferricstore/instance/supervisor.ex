defmodule FerricStore.Instance.Supervisor do
  @moduledoc """
  Per-instance supervision tree for a FerricStore instance.

  Starts the custom instance shard tree and merge scheduler.

  Custom instances are local/direct only. The default application instance owns
  the Raft system; allowing embedded instances to opt into Raft would collide
  with default shard/server names until per-instance Raft systems exist.
  """

  use Supervisor

  @doc """
  Starts the instance supervisor and all child processes.

  The instance context (`ctx`) must already be built via
  `FerricStore.Instance.build/2` before calling this.
  """
  @spec start_link(atom(), keyword()) :: Supervisor.on_start()
  def start_link(name, opts) do
    case normalize_raft_opts(name, opts) do
      {:ok, opts} ->
        Supervisor.start_link(__MODULE__, {name, opts}, name: :"#{name}.Supervisor")

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def init({name, opts}) do
    ctx = FerricStore.Instance.build(name, opts)

    # Ensure data directory layout exists
    Ferricstore.DataDir.ensure_layout!(ctx.data_dir, ctx.shard_count)

    cleanup_children =
      if name == :default do
        []
      else
        [
          Supervisor.child_spec({FerricStore.Instance.Cleanup, name},
            id: :"#{name}.InstanceCleanup",
            restart: :temporary
          )
        ]
      end

    merge_children =
      if name == :default do
        []
      else
        [
          Supervisor.child_spec(
            {Ferricstore.Merge.Supervisor,
             [
               name: :"#{name}.Merge.Supervisor",
               data_dir: ctx.data_dir,
               shard_count: ctx.shard_count,
               merge_config: Keyword.get(opts, :merge_config, %{}),
               instance_ctx: ctx
             ]},
            id: :"#{name}.MergeSupervisor"
          )
        ]
      end

    children =
      cleanup_children ++
        merge_children ++
        [
          # Stats and MemoryGuard are global application processes today. The
          # instance context owns isolated counters/flags, but these GenServers are
          # not instance-scoped yet, so embedded instances must not start duplicates.
          {Ferricstore.Store.ShardSupervisor,
           [
             name: :"#{name}.ShardSupervisor",
             data_dir: ctx.data_dir,
             shard_count: ctx.shard_count,
             instance_ctx: ctx
           ]}
        ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 20, max_seconds: 10)
  end

  defp normalize_raft_opts(:default, opts), do: {:ok, opts}

  defp normalize_raft_opts(name, opts) do
    if Keyword.has_key?(opts, :raft_enabled) do
      {:error, {:unsupported_custom_option, name, :raft_enabled}}
    else
      {:ok, opts}
    end
  end
end
