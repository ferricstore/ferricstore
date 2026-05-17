defmodule Ferricstore.Store.ShardSupervisor do
  @moduledoc """
  Supervises all `Ferricstore.Store.Shard` GenServers.

  Each child is a shard GenServer responsible for one Bitcask partition and
  its corresponding ETS hot cache. The supervisor uses a `:one_for_one`
  strategy so that a single shard crash does not take down the others.

  ## Options

    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:shard_count` -- number of shards to start (default: 4)
  """

  use Supervisor

  @doc "Starts the shard supervisor and all child shards."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    instance_ctx = Keyword.get(opts, :instance_ctx) || Keyword.get(opts, :instance)

    data_dir =
      Keyword.get(opts, :data_dir) ||
        (instance_ctx && instance_ctx.data_dir) ||
        Keyword.fetch!(opts, :data_dir)

    shard_count =
      Keyword.get(opts, :shard_count) || (instance_ctx && instance_ctx.shard_count) || 4

    children =
      Enum.flat_map(0..(shard_count - 1), fn i ->
        shard_opts = [index: i, data_dir: data_dir]

        shard_opts =
          if instance_ctx,
            do: Keyword.put(shard_opts, :instance_ctx, instance_ctx),
            else: shard_opts

        [
          Supervisor.child_spec(
            {Ferricstore.Raft.ReplaySafeIndexWriter,
             [
               shard_index: i,
               shard_data_path: Ferricstore.DataDir.shard_data_path(data_dir, i),
               instance_ctx: instance_ctx
             ]},
            id: :"replay_safe_index_writer_#{i}"
          ),
          Supervisor.child_spec(
            {Ferricstore.Store.Shard, shard_opts},
            id: :"shard_#{i}"
          ),
          Supervisor.child_spec(
            {Ferricstore.Flow.HistoryProjector,
             [
               shard_index: i,
               shard_data_path: Ferricstore.DataDir.shard_data_path(data_dir, i),
               instance_ctx: instance_ctx,
               recover_on_init: false
             ]},
            id: :"flow_history_projector_#{i}"
          ),
          Supervisor.child_spec(
            {Ferricstore.Store.BitcaskCheckpointer, [index: i, instance_ctx: instance_ctx]},
            id: :"checkpointer_#{i}"
          )
        ]
      end)

    # Allow up to 100 restarts per 60 seconds to accommodate integration tests
    # that deliberately kill shards. Production workloads will never hit this
    # limit under normal operation.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 100, max_seconds: 60)
  end
end
