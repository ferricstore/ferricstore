defmodule Mix.Tasks.Ferricstore.Info do
  @moduledoc """
  Displays FerricStore node status including uptime, shard count, key count,
  memory usage, and Raft consensus status.

  ## Usage

      mix ferricstore.info

  ## Output

  Prints a human-readable summary of the running node's operational state:

    * Server uptime in seconds
    * Total shard count and per-shard key counts
    * Total key count across all shards
    * BEAM memory usage
    * Raft consensus status (enabled/disabled, per-shard leader info)
    * Connection and command counters

  The task ensures the FerricStore application is started before querying
  status, making it safe to run from a cold `mix` invocation.
  """

  use Mix.Task

  @shortdoc "Show FerricStore node status: uptime, shards, keys, memory, raft"

  @doc """
  Runs the info task, printing node status to stdout.

  ## Parameters

    * `_args` -- command-line arguments (ignored)

  """
  @spec run(list()) :: :ok
  @impl Mix.Task
  def run(_args) do
    ensure_started()

    shard_count = Application.get_env(:ferricstore, :shard_count, 4)
    uptime = Ferricstore.Stats.uptime_seconds()
    ctx = FerricStore.Instance.get(:default)
    keys = Ferricstore.Store.Router.keys(ctx)
    total_keys = length(keys)
    memory = :erlang.memory(:total)

    Mix.shell().info("=== FerricStore Node Status ===")
    Mix.shell().info("uptime_seconds: #{uptime}")
    Mix.shell().info("shard_count: #{shard_count}")
    Mix.shell().info("total_keys: #{total_keys}")
    Mix.shell().info("memory_used_bytes: #{memory}")
    Mix.shell().info("total_connections: #{Ferricstore.Stats.total_connections()}")
    Mix.shell().info("total_commands: #{Ferricstore.Stats.total_commands()}")
    Mix.shell().info("run_id: #{Ferricstore.Stats.run_id()}")
    Mix.shell().info("")
    Mix.shell().info("--- Shard Status ---")

    Enum.each(0..(shard_count - 1), fn i ->
      shard_name = Ferricstore.Store.Router.shard_name(ctx, i)
      pid = Process.whereis(shard_name)
      alive? = is_pid(pid) and Process.alive?(pid)
      status = if alive?, do: "ok", else: "down"

      key_count =
        try do
          :ets.info(:"keydir_#{i}", :size)
        rescue
          ArgumentError -> 0
        end

      raft_info =
        case safe_raft_members(i) do
          {:ok, _members, leader} ->
            "leader=#{inspect(leader)}"

          _ ->
            "no_leader"
        end

      Mix.shell().info("shard_#{i}: status=#{status} keys=#{key_count} raft=#{raft_info}")
    end)

    :ok
  end

  defp ensure_started do
    Mix.Task.run("app.start")
  end

  defp safe_raft_members(shard_index) do
    Ferricstore.Raft.Cluster.members(shard_index, 1_000)
  catch
    :exit, _ -> :error
  end
end
