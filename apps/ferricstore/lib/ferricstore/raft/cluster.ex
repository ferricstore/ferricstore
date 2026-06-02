defmodule Ferricstore.Raft.Cluster do
  @moduledoc """
  WARaft cluster facade for FerricStore shards.

  This module keeps the cluster-management boundary narrow so callers do not
  reach into WARaft internals directly.
  """

  alias Ferricstore.Raft.WARaftBackend

  @system :ferricstore_waraft_backend

  @doc "Returns the WARaft system name used by FerricStore."
  @spec system_name() :: atom()
  def system_name, do: @system

  @spec start_system(binary()) :: :ok
  def start_system(_data_dir), do: :ok

  @doc false
  @spec start_system(binary(), term()) :: :ok
  def start_system(_data_dir, _backend), do: :ok

  @spec stop_system() :: :ok
  def stop_system, do: :ok

  @doc false
  @spec stop_system(term()) :: :ok
  def stop_system(_backend), do: :ok

  @spec join_shard_server(
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          binary(),
          atom(),
          [node()],
          keyword()
        ) :: :ok
  def join_shard_server(
        _shard_index,
        _shard_data_path,
        _active_file_id,
        _active_file_path,
        _ets,
        _cluster_members,
        _opts \\ []
      ),
      do: :ok

  @spec start_shard_server(
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          binary(),
          atom(),
          keyword()
        ) :: :ok
  def start_shard_server(
        _shard_index,
        _shard_data_path,
        _active_file_id,
        _active_file_path,
        _ets,
        _opts \\ []
      ),
      do: :ok

  @spec stop_shard_server(non_neg_integer()) :: :ok
  def stop_shard_server(_shard_index), do: :ok

  @spec add_member(non_neg_integer(), node(), atom()) :: :ok | {:error, term()}
  def add_member(shard_index, node, membership \\ :voter)

  def add_member(shard_index, node, :voter) do
    case WARaftBackend.add_member(shard_index, node) do
      {:ok, _position} -> :ok
      :already_member -> :ok
      {:error, :already_member} -> :ok
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  def add_member(shard_index, node, membership) when membership in [:promotable, :non_voter] do
    case WARaftBackend.adjust_membership(shard_index, :remove_membership, node) do
      {:ok, _position} -> :ok
      {:error, :not_a_member} -> add_participant(shard_index, node)
      {:error, :not_member} -> add_participant(shard_index, node)
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  def add_member(_shard_index, _node, membership),
    do: {:error, {:unsupported_membership, membership}}

  defp add_participant(shard_index, node) do
    case WARaftBackend.add_participant(shard_index, node) do
      {:ok, _position} -> :ok
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  @spec remove_member(non_neg_integer(), node()) :: :ok | {:error, term()}
  def remove_member(shard_index, node) do
    case WARaftBackend.adjust_membership(shard_index, :remove_membership, node) do
      {:ok, _position} -> :ok
      {:error, :not_member} -> :ok
      {:error, :not_a_member} -> :ok
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  @spec members(non_neg_integer()) :: {:ok, list(), term()} | {:error, term()}
  def members(shard_index), do: members(shard_index, :default)

  @spec members(non_neg_integer(), timeout() | :default) ::
          {:ok, list(), term()} | {:error, term()}
  def members(shard_index, :default), do: blocking_members(shard_index)

  def members(shard_index, timeout) when is_integer(timeout) and timeout >= 0 do
    case WARaftBackend.cached_members(shard_index) do
      {:ok, _members, _leader} = result -> result
      _miss -> timed_members(shard_index, timeout)
    end
  end

  def members(shard_index, _timeout), do: blocking_members(shard_index)

  @spec member_overview(non_neg_integer() | tuple()) :: {:error, :unsupported_member_overview}
  def member_overview(_shard), do: {:error, :unsupported_member_overview}

  @spec member_overview_on(node(), tuple()) :: {:error, :unsupported_member_overview}
  def member_overview_on(_target_node, _server_id), do: {:error, :unsupported_member_overview}

  @spec stop_server_on(node(), atom(), tuple()) :: :ok
  def stop_server_on(_target_node, _system, _server_id), do: :ok

  @spec force_delete_server_on(node(), atom(), tuple()) :: :ok
  def force_delete_server_on(_target_node, _system, _server_id), do: :ok

  @spec transfer_leadership(non_neg_integer(), node()) :: :ok | {:error, term()}
  def transfer_leadership(shard_index, target_node),
    do: WARaftBackend.transfer_leadership(shard_index, target_node)

  @spec trigger_shard_elections_parallel(non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def trigger_shard_elections_parallel(shard_count, opts \\ [])

  def trigger_shard_elections_parallel(0, _opts), do: :ok

  def trigger_shard_elections_parallel(shard_count, opts)
      when is_integer(shard_count) and shard_count > 0 do
    timeout = Keyword.get(opts, :timeout, 120_000)
    max_concurrency = Keyword.get(opts, :max_concurrency, shard_count)

    0..(shard_count - 1)
    |> Task.async_stream(
      fn shard_index ->
        with :ok <- normalize_election(WARaftBackend.trigger_election(shard_index)),
             :ok <- wait_for_waraft_leader(shard_index) do
          :ok
        else
          {:error, reason} -> {:error, {shard_index, reason}}
          other -> {:error, {shard_index, other}}
        end
      end,
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, {:error, reason}}, :ok -> {:halt, {:error, reason}}
      {:exit, reason}, :ok -> {:halt, {:error, {:election_task_exit, reason}}}
    end)
  end

  @doc false
  @spec wait_for_leader_on_start?(keyword()) :: boolean()
  def wait_for_leader_on_start?(opts), do: Keyword.get(opts, :wait_for_leader, true)

  @doc false
  @spec replay_skip_below_index(binary(), keyword()) :: non_neg_integer()
  def replay_skip_below_index(shard_data_path, opts \\ []) do
    max(
      Keyword.get(opts, :skip_below_index, 0),
      Ferricstore.Raft.ReplaySafeIndex.read(shard_data_path)
    )
  end

  @doc false
  @spec shard_server_id_on(non_neg_integer(), node()) :: {atom(), node()}
  def shard_server_id_on(shard_index, node),
    do: {:"raft_server_ferricstore_waraft_backend_#{shard_index + 1}", node}

  @doc false
  @spec shard_server_id(non_neg_integer()) :: {atom(), node()}
  def shard_server_id(shard_index), do: shard_server_id_on(shard_index, local_raft_node())

  @doc false
  def local_raft_node, do: node()

  @doc false
  def start_error_recovery_action(_reason), do: :fail_closed

  @doc false
  def log_init_args_for_shard(_shard_index), do: %{}

  @doc false
  def boot_initial_members(_shard_index, server_id, []), do: [server_id]

  def boot_initial_members(shard_index, _server_id, cluster_nodes) when is_list(cluster_nodes),
    do: Enum.map(cluster_nodes, &shard_server_id_on(shard_index, &1))

  defp timed_members(shard_index, timeout) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        send(parent, {ref, blocking_members(shard_index)})
      end)

    receive do
      {^ref, result} -> result
    after
      timeout ->
        Process.exit(pid, :kill)
        {:error, :timeout}
    end
  end

  defp blocking_members(shard_index) do
    case WARaftBackend.membership(shard_index) do
      members when is_list(members) ->
        leader = current_leader(shard_index, members)
        {:ok, members, leader}

      {:error, _reason} = error ->
        error

      other ->
        {:error, other}
    end
  end

  defp current_leader(shard_index, members) do
    case WARaftBackend.status(shard_index) do
      status when is_list(status) ->
        leader_node = Keyword.get(status, :leader_id)
        Enum.find(members, fn {_server, node} -> node == leader_node end)

      _other ->
        nil
    end
  end

  defp normalize_election(:ok), do: :ok
  defp normalize_election({:error, _reason} = error), do: error
  defp normalize_election(other), do: {:error, other}

  defp wait_for_waraft_leader(shard_index, attempts \\ 200)
  defp wait_for_waraft_leader(_shard_index, 0), do: {:error, :leader_election_timeout}

  defp wait_for_waraft_leader(shard_index, attempts) do
    case blocking_members(shard_index) do
      {:ok, _members, {_name, _node}} ->
        :ok

      _other ->
        Process.sleep(50)
        wait_for_waraft_leader(shard_index, attempts - 1)
    end
  end
end
