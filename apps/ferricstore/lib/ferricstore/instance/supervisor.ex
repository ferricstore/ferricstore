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
    Supervisor.start_link(__MODULE__, {name, opts}, name: :"#{name}.Supervisor")
  end

  @impl true
  def init({name, opts}) do
    try do
      init_instance(name, opts)
    rescue
      error ->
        FerricStore.Instance.cleanup(name)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        FerricStore.Instance.cleanup(name)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp init_instance(name, opts) do
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

    bitcask_writer_children =
      if name == :default do
        []
      else
        Enum.map(0..(ctx.shard_count - 1), fn i ->
          Supervisor.child_spec(
            {Ferricstore.Store.BitcaskWriter, shard_index: i, instance_ctx: ctx},
            id: :"#{name}.BitcaskWriter.#{i}"
          )
        end)
      end

    flow_lmdb_writer_children =
      if name == :default do
        []
      else
        Enum.map(0..(ctx.shard_count - 1), fn i ->
          Supervisor.child_spec(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: i, data_dir: ctx.data_dir, instance_ctx: ctx},
            id: :"#{name}.FlowLMDBWriter.#{i}"
          )
        end)
      end

    query_index_provider_children =
      case FerricStore.Flow.QueryIndexProvider.child_specs(ctx) do
        {:ok, specs} -> specs
        {:error, reason} -> raise "Flow query index provider startup failed: #{inspect(reason)}"
      end

    retention_sweeper_children =
      if name == :default do
        []
      else
        sweeper_opts =
          opts
          |> Keyword.get(:flow_retention_sweeper, [])
          |> case do
            value when is_list(value) -> value
            _other -> []
          end
          |> Keyword.merge(
            name: Ferricstore.Flow.RetentionSweeper.name(ctx),
            instance_ctx: ctx
          )

        [
          Supervisor.child_spec(
            {Ferricstore.Flow.RetentionSweeper, sweeper_opts},
            id: :"#{name}.FlowRetentionSweeper"
          )
        ]
      end

    policy_migration_worker_children =
      if name == :default do
        []
      else
        worker_opts =
          opts
          |> Keyword.get(:flow_policy_migration_worker, [])
          |> case do
            value when is_list(value) -> value
            _other -> []
          end
          |> Keyword.merge(
            name: Ferricstore.Flow.PolicyMigrationWorker.name(ctx),
            instance_ctx: ctx
          )

        [
          Supervisor.child_spec(
            {Ferricstore.Flow.PolicyMigrationWorker, worker_opts},
            id: :"#{name}.FlowPolicyMigrationWorker"
          )
        ]
      end

    limit_reconciler_children =
      if name == :default do
        []
      else
        [
          Supervisor.child_spec(
            {Ferricstore.Flow.Governance.LimitReconciler, instance_ctx: ctx},
            id: :"#{name}.FlowGovernanceLimitReconciler"
          )
        ]
      end

    limit_storage_cleaner_children =
      if name == :default do
        []
      else
        [
          Supervisor.child_spec(
            {Ferricstore.Flow.Governance.LimitStorageCleaner,
             name: Ferricstore.Flow.Governance.LimitStorageCleaner.process_name(ctx),
             instance_ctx: ctx},
            id: :"#{name}.FlowGovernanceLimitStorageCleaner"
          )
        ]
      end

    children =
      cleanup_children ++
        merge_children ++
        bitcask_writer_children ++
        query_index_provider_children ++
        flow_lmdb_writer_children ++
        [
          Supervisor.child_spec(
            {Ferricstore.Store.ETSTableHeir,
             name: Ferricstore.Store.KeydirTableOwner.table_heir_name(ctx)},
            id: :"#{name}.KeydirTableHeir"
          ),
          Supervisor.child_spec(
            {Ferricstore.Store.KeydirRuntimeSupervisor,
             [
               name: :"#{name}.KeydirRuntimeSupervisor",
               shard_supervisor_name: :"#{name}.ShardSupervisor",
               data_dir: ctx.data_dir,
               shard_count: ctx.shard_count,
               instance_ctx: ctx
             ]},
            id: :"#{name}.KeydirRuntimeSupervisor"
          )
        ] ++
        retention_sweeper_children ++
        limit_reconciler_children ++
        limit_storage_cleaner_children ++
        policy_migration_worker_children ++
        [
          Supervisor.child_spec(
            {Ferricstore.Store.BlobGCSweeper, name: :"#{name}.BlobGCSweeper", instance_ctx: ctx},
            id: :"#{name}.BlobGCSweeper"
          )
        ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 20, max_seconds: 10)
  end
end
