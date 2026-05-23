defmodule Ferricstore.Raft.WARaftBackend do
  @moduledoc """
  Candidate WARaft backend boundary.

  This module is intentionally separate from the production `:ra` path. It is
  the replacement gate: WARaft must prove it can run the real FerricStore state
  machine, preserve restart semantics, install snapshots, and expose the same
  write shapes before Router can select it in production.
  """

  alias Ferricstore.ErrorReasons
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Raft.CommandStamp
  alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher

  @app :ferricstore_waraft_backend
  @table :ferricstore_waraft_backend
  @sup_id :ferricstore_waraft_backend_sup
  @timeout 10_000
  @context_key {__MODULE__, :context}
  @inflight_bytes_key {__MODULE__, :inflight_commit_bytes}
  @max_inflight_bytes_key {__MODULE__, :max_inflight_commit_bytes}
  @shard_count_key {__MODULE__, :shard_count}
  @voter_nodes_key {__MODULE__, :voter_nodes}
  @default_log_module :ferricstore_waraft_spike_segment_log
  @config_apply_poll_ms 10
  @config_redirects 2
  @log_module_callbacks [
    first_index: 1,
    last_index: 1,
    fold: 6,
    fold_terms: 5,
    get: 2,
    term: 2,
    config: 1,
    append: 4,
    init: 1,
    open: 1,
    close: 2,
    reset: 3,
    truncate: 3,
    trim: 3,
    flush: 1
  ]
  @label_module_callbacks [new_label: 2]
  @membership_actions [
    :add,
    :add_witness,
    :remove,
    :remove_witness,
    :add_participant,
    :promote_participant_if_ready,
    :remove_membership,
    :demote_to_witness
  ]

  defguardp is_storage_unknown_outcome_reason(reason)
            when reason == :active_file_unavailable or
                   reason == :storage_apply_timeout or
                   reason == :commit_unreachable_after_submit or
                   reason == :not_leader_after_submit or
                   (is_tuple(reason) and tuple_size(reason) == 2 and
                      elem(reason, 0) in [
                        :bitcask_append_failed,
                        :bitcask_append_result_mismatch,
                        :bitcask_writer_flush_failed,
                        :blob_externalize_failed,
                        :blob_ref_unavailable,
                        :cross_shard_compensation_failed,
                        :flow_history_projection_failed,
                        :delete_prob_file_failed,
                        :storage_blocked,
                        :commit_call_failed_after_submit
                      ]) or
                   (is_tuple(reason) and tuple_size(reason) == 3 and
                      elem(reason, 0) in [
                        :batch_result_mismatch,
                        :fsync_file,
                        :fsync_dir,
                        :fsync_dir_failed,
                        :unsafe_metadata_path,
                        :tombstone_batch_result_mismatch
                      ])

  defguardp valid_shard_index_shape(shard_index)
            when is_integer(shard_index) and shard_index >= 0

  defguardp invalid_shard_index_shape(shard_index)
            when not is_integer(shard_index) or shard_index < 0

  defguardp invalid_redirects_left_shape(redirects_left)
            when not is_integer(redirects_left) or redirects_left < 0

  @doc false
  @spec default_log_module() :: module()
  def default_log_module, do: @default_log_module

  @doc false
  @spec default_commit_batch_interval_ms() :: non_neg_integer()
  def default_commit_batch_interval_ms do
    case Application.fetch_env(:ferricstore, :waraft_commit_batch_interval_ms) do
      {:ok, value} ->
        non_negative_integer_option!(:waraft_commit_batch_interval_ms, value)

      :error ->
        wal_commit_delay_floor_ms()
    end
  end

  @spec start(FerricStore.Instance.t(), keyword()) :: :ok | {:error, term()}
  def start(%FerricStore.Instance{} = ctx, opts \\ []) do
    Ferricstore.Raft.WARaftStorage.validate_supported_apply_mode!()

    config = backend_config!(opts)

    :ok = ensure_started()
    _ = stop()

    Ferricstore.DataDir.ensure_layout!(ctx.data_dir, ctx.shard_count)
    Ferricstore.Store.ActiveFile.init(ctx.shard_count)

    configure(ctx, config)
    :persistent_term.put({@context_key, @table}, ctx)

    specs =
      for partition <- 1..ctx.shard_count do
        partition_spec(partition, config)
      end

    spec =
      @app
      |> :wa_raft_sup.child_spec(specs, %{config_search_apps: [@app, :ferricstore]})
      |> Map.put(:id, @sup_id)

    case Supervisor.start_child(:kernel_sup, spec) do
      {:ok, _pid} -> finish_started(ctx.shard_count, opts)
      {:ok, _pid, _info} -> finish_started(ctx.shard_count, opts)
      {:error, {:already_started, _pid}} -> finish_started(ctx.shard_count, opts)
      {:error, :already_present} -> restart_present_child(ctx, opts)
      {:error, reason} -> cleanup_failed_start({:error, reason})
    end
  end

  @spec stop() :: :ok
  def stop do
    shard_count = registered_partition_count()
    stop_namespace_batchers(shard_count)
    _ = Supervisor.terminate_child(:kernel_sup, @sup_id)
    _ = Supervisor.delete_child(:kernel_sup, @sup_id)
    _ = stop_orphaned_waraft_sup()
    wait_down(registered_names(shard_count), 100)
    :persistent_term.erase({@context_key, @table})
    :persistent_term.erase(@inflight_bytes_key)
    :persistent_term.erase(@max_inflight_bytes_key)
    erase_cached_voter_nodes(shard_count)
    :persistent_term.erase(@shard_count_key)
    :ok
  end

  @doc false
  @spec __registered_names_for_test__(pos_integer()) :: [atom()]
  def __registered_names_for_test__(shard_count)
      when is_integer(shard_count) and shard_count > 0 do
    registered_names(shard_count)
  end

  @doc false
  def __redirect_write_failure_for_test__(kind, reason) do
    redirect_write_failure(kind, reason)
  end

  @doc false
  def __redirect_membership_failure_for_test__(kind, reason) do
    redirect_membership_failure(kind, reason)
  end

  @doc false
  def __redirect_transfer_failure_for_test__(kind, reason) do
    redirect_transfer_failure(kind, reason)
  end

  @doc false
  def __peer_node_for_test__(peer) do
    peer_node(peer)
  end

  @spec context!(atom()) :: FerricStore.Instance.t()
  def context!(table) do
    :persistent_term.get({@context_key, table})
  end

  defp context(table) do
    {:ok, context!(table)}
  catch
    :error, :badarg -> backend_unavailable_error()
  end

  @spec write(non_neg_integer(), tuple()) :: term()
  def write(shard_index, command) when valid_shard_index_shape(shard_index) do
    case maybe_namespace_window_write(shard_index, command) do
      :direct -> commit_or_redirect(shard_index, command, 2)
      result -> result
    end
  end

  def write(shard_index, _command), do: invalid_shard_index_error(shard_index)

  @spec write_many([{non_neg_integer(), tuple()}]) :: [term()]
  def write_many([]), do: []

  def write_many(shard_commands) when is_list(shard_commands) do
    shard_commands
    |> Enum.map(&submit_write_many_entry/1)
    |> Enum.map(&await_write_many_entry/1)
  end

  def write_many(shard_commands), do: {:error, {:invalid_write_many_entries, shard_commands}}

  @doc false
  @spec write_redirected(non_neg_integer(), tuple(), non_neg_integer()) :: term()
  def write_redirected(shard_index, _command, _redirects_left)
      when invalid_shard_index_shape(shard_index),
      do: invalid_shard_index_error(shard_index)

  def write_redirected(_shard_index, _command, redirects_left)
      when invalid_redirects_left_shape(redirects_left),
      do: invalid_redirects_left_error(redirects_left)

  def write_redirected(shard_index, command, redirects_left)
      when is_integer(redirects_left) and redirects_left >= 0 do
    commit_or_redirect(shard_index, command, redirects_left)
  end

  @spec write_batch(non_neg_integer(), [tuple()]) :: term()
  def write_batch(shard_index, _commands) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def write_batch(_shard_index, []), do: {:ok, []}

  def write_batch(shard_index, commands) when is_list(commands) do
    NamespaceBatcher.write_batch(shard_index, commands)
  end

  def write_batch(_shard_index, commands),
    do: {:error, {:invalid_command_batch, commands}}

  @doc false
  @spec __commit_batch_direct__(non_neg_integer(), [tuple()]) :: term()
  def __commit_batch_direct__(shard_index, commands) when is_list(commands) do
    commit_or_redirect(shard_index, {:batch, commands}, 2)
  end

  @spec write_put_batch(non_neg_integer(), [{binary(), binary(), non_neg_integer()}]) :: term()
  def write_put_batch(shard_index, _entries) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def write_put_batch(_shard_index, []), do: {:ok, []}

  def write_put_batch(shard_index, entries) when is_list(entries) do
    NamespaceBatcher.write_put_batch(shard_index, entries)
  end

  def write_put_batch(_shard_index, entries),
    do: {:error, {:invalid_put_batch, entries}}

  @spec write_put_batch_async(
          non_neg_integer(),
          [{binary(), binary(), non_neg_integer()}],
          GenServer.from()
        ) :: :ok | {:direct, term()}
  def write_put_batch_async(shard_index, _entries, from)
      when invalid_shard_index_shape(shard_index) do
    GenServer.reply(from, invalid_shard_index_error(shard_index))
    :ok
  end

  def write_put_batch_async(_shard_index, [], from) do
    GenServer.reply(from, {:ok, []})
    :ok
  end

  def write_put_batch_async(shard_index, entries, from) when is_list(entries) do
    NamespaceBatcher.write_put_batch_async(shard_index, entries, from)
  end

  def write_put_batch_async(_shard_index, entries, from) do
    GenServer.reply(from, {:error, {:invalid_put_batch, entries}})
    :ok
  end

  @spec write_delete_batch(non_neg_integer(), [binary()]) :: term()
  def write_delete_batch(shard_index, _keys) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def write_delete_batch(_shard_index, []), do: {:ok, []}

  def write_delete_batch(shard_index, keys) when is_list(keys) do
    NamespaceBatcher.write_delete_batch(shard_index, keys)
  end

  def write_delete_batch(_shard_index, keys),
    do: {:error, {:invalid_delete_batch, keys}}

  @spec write_delete_batch_async(non_neg_integer(), [binary()], GenServer.from()) ::
          :ok | {:direct, term()}
  def write_delete_batch_async(shard_index, _keys, from)
      when invalid_shard_index_shape(shard_index) do
    GenServer.reply(from, invalid_shard_index_error(shard_index))
    :ok
  end

  def write_delete_batch_async(_shard_index, [], from) do
    GenServer.reply(from, {:ok, []})
    :ok
  end

  def write_delete_batch_async(shard_index, keys, from) when is_list(keys) do
    NamespaceBatcher.write_delete_batch_async(shard_index, keys, from)
  end

  def write_delete_batch_async(_shard_index, keys, from) do
    GenServer.reply(from, {:error, {:invalid_delete_batch, keys}})
    :ok
  end

  @doc false
  @spec __commit_put_batch_direct__(
          non_neg_integer(),
          [{binary(), binary(), non_neg_integer()}]
        ) :: term()
  def __commit_put_batch_direct__(shard_index, entries) when is_list(entries) do
    commit_or_redirect(shard_index, {:put_batch, entries}, 2)
  end

  @doc false
  @spec __commit_delete_batch_direct__(non_neg_integer(), [binary()]) :: term()
  def __commit_delete_batch_direct__(shard_index, keys) when is_list(keys) do
    commit_or_redirect(shard_index, {:delete_batch, keys}, 2)
  end

  @spec inflight_commit_bytes(non_neg_integer()) :: non_neg_integer()
  def inflight_commit_bytes(shard_index) when invalid_shard_index_shape(shard_index), do: 0

  def inflight_commit_bytes(shard_index) do
    case :persistent_term.get(@inflight_bytes_key, nil) do
      ref when is_reference(ref) ->
        idx = partition(shard_index)

        if idx <= :atomics.info(ref).size do
          :atomics.get(ref, idx)
        else
          0
        end

      _other ->
        0
    end
  end

  @spec segment_log_memory_status(non_neg_integer()) :: map()
  def segment_log_memory_status(shard_index) when invalid_shard_index_shape(shard_index),
    do: empty_segment_log_memory_status()

  def segment_log_memory_status(shard_index) do
    partition = partition(shard_index)
    log_name = :wa_raft_log.registered_name(@table, partition)
    log = {:raft_log, log_name, @app, @table, partition, @default_log_module}

    case @default_log_module.memory_status(log) do
      status when is_map(status) -> status
      _other -> empty_segment_log_memory_status()
    end
  rescue
    _ -> empty_segment_log_memory_status()
  catch
    _, _ -> empty_segment_log_memory_status()
  end

  @spec storage_position(non_neg_integer()) :: {:ok, tuple()} | {:error, term()}
  def storage_position(shard_index) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def storage_position(shard_index) do
    storage = :wa_raft_storage.registered_name(@table, partition(shard_index))

    backend_call(fn ->
      {:ok, :wa_raft_storage.position(storage)}
    end)
  end

  @spec create_snapshot(non_neg_integer()) :: {:ok, tuple()} | {:error, term()}
  def create_snapshot(shard_index) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def create_snapshot(shard_index) do
    storage = :wa_raft_storage.registered_name(@table, partition(shard_index))
    backend_call(fn -> :wa_raft_storage.create_snapshot(storage) end)
  end

  @spec install_snapshot(non_neg_integer(), charlist() | binary(), tuple()) ::
          :ok | {:error, term()}
  def install_snapshot(shard_index, _snapshot_path, _position)
      when invalid_shard_index_shape(shard_index),
      do: invalid_shard_index_error(shard_index)

  def install_snapshot(shard_index, snapshot_path, position) do
    with {:ok, snapshot_path_chars} <- snapshot_path_charlist(snapshot_path),
         :ok <- validate_snapshot_position(position) do
      server = :wa_raft_server.registered_name(@table, partition(shard_index))

      backend_call(fn ->
        :wa_raft_server.snapshot_available(server, snapshot_path_chars, position)
      end)
    end
  end

  @spec bootstrap_cluster([node()]) :: :ok | {:error, term()}
  def bootstrap_cluster(nodes) when is_list(nodes) do
    with :ok <- validate_bootstrap_nodes(nodes),
         {:ok, ctx} <- context(@table) do
      0..(ctx.shard_count - 1)
      |> Enum.reduce_while(:ok, fn shard_index, :ok ->
        case bootstrap_cluster_partition(shard_index, nodes) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  def bootstrap_cluster(_nodes), do: {:error, :invalid_cluster_nodes}

  @spec trigger_election(non_neg_integer()) :: :ok | term()
  def trigger_election(shard_index) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def trigger_election(shard_index) do
    server =
      shard_index
      |> partition()
      |> server_name()

    backend_call(fn -> :wa_raft_server.trigger_election(server) end)
  end

  @spec transfer_leadership(non_neg_integer(), node()) :: :ok | {:error, term()}
  def transfer_leadership(shard_index, _target_node) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def transfer_leadership(shard_index, target_node) do
    with :ok <- validate_node_name(target_node) do
      transfer_leadership_or_redirect(shard_index, target_node, 2)
    end
  end

  @doc false
  @spec transfer_leadership_redirected(non_neg_integer(), node(), non_neg_integer()) ::
          :ok | {:error, term()}
  def transfer_leadership_redirected(shard_index, _target_node, _redirects_left)
      when invalid_shard_index_shape(shard_index),
      do: invalid_shard_index_error(shard_index)

  def transfer_leadership_redirected(_shard_index, _target_node, redirects_left)
      when invalid_redirects_left_shape(redirects_left),
      do: invalid_redirects_left_error(redirects_left)

  def transfer_leadership_redirected(shard_index, target_node, redirects_left)
      when is_integer(redirects_left) and redirects_left >= 0 do
    with :ok <- validate_node_name(target_node) do
      transfer_leadership_or_redirect(shard_index, target_node, redirects_left)
    end
  end

  @spec status(non_neg_integer()) :: keyword() | term()
  def status(shard_index) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def status(shard_index) do
    maybe_status_hook(shard_index)

    server =
      shard_index
      |> partition()
      |> server_name()

    backend_call(fn -> :wa_raft_server.status(server) end)
  end

  defp maybe_status_hook(shard_index) do
    case Process.get(:ferricstore_waraft_backend_status_hook) do
      fun when is_function(fun, 1) -> fun.(shard_index)
      _other -> :ok
    end
  end

  @doc false
  @spec cache_config(non_neg_integer(), term()) :: :ok
  def cache_config(shard_index, {_position, config}) when is_integer(shard_index),
    do: cache_config(shard_index, config)

  def cache_config(shard_index, config) when is_integer(shard_index) and is_map(config) do
    :persistent_term.put({@voter_nodes_key, shard_index}, config_voter_nodes(config))
    :ok
  end

  def cache_config(_shard_index, _config), do: :ok

  @spec membership(non_neg_integer()) :: term()
  def membership(shard_index) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def membership(shard_index) do
    result =
      backend_call(fn ->
        shard_index
        |> partition()
        |> server_name()
        |> :wa_raft_server.membership()
      end)

    case result do
      identities when is_list(identities) -> Enum.map(identities, &normalize_identity/1)
      other -> other
    end
  end

  @doc false
  @spec cached_members(non_neg_integer()) :: {:ok, list(), term()} | {:error, term()}
  def cached_members(shard_index) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def cached_members(shard_index) do
    case cached_voter_nodes(shard_index) do
      {:ok, [_ | _] = voters} ->
        partition = partition(shard_index)
        server = server_name(partition)

        leader_node = :wa_raft_info.get_leader(@table, partition)
        leader = if valid_node_name?(leader_node), do: {server, leader_node}, else: nil

        {:ok, Enum.map(voters, &{server, &1}), leader}

      {:ok, []} ->
        {:error, :unknown_cached_membership}

      :unknown ->
        {:error, :unknown_cached_membership}
    end
  catch
    :exit, reason -> backend_exit_error(reason)
    kind, reason -> {:error, {kind, reason}}
  end

  @spec adjust_membership(non_neg_integer(), atom(), node()) ::
          {:ok, tuple()} | {:error, term()}
  def adjust_membership(shard_index, _action, _node_name)
      when invalid_shard_index_shape(shard_index),
      do: invalid_shard_index_error(shard_index)

  def adjust_membership(_shard_index, action, _node_name)
      when action not in @membership_actions,
      do: invalid_membership_action_error(action)

  def adjust_membership(shard_index, action, node_name) when action in @membership_actions do
    with :ok <- validate_node_name(node_name) do
      commit_config_or_redirect(shard_index, action, node_name, @timeout, @config_redirects)
    end
  end

  @doc false
  @spec adjust_membership_redirected(
          non_neg_integer(),
          atom(),
          node(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, tuple()} | {:error, term()}
  def adjust_membership_redirected(shard_index, _action, _node_name, _timeout_ms, _redirects_left)
      when invalid_shard_index_shape(shard_index),
      do: invalid_shard_index_error(shard_index)

  def adjust_membership_redirected(_shard_index, action, _node_name, _timeout_ms, _redirects_left)
      when action not in @membership_actions,
      do: invalid_membership_action_error(action)

  def adjust_membership_redirected(_shard_index, _action, _node_name, _timeout_ms, redirects_left)
      when invalid_redirects_left_shape(redirects_left),
      do: invalid_redirects_left_error(redirects_left)

  def adjust_membership_redirected(shard_index, action, node_name, timeout_ms, redirects_left)
      when action in @membership_actions and is_integer(redirects_left) and redirects_left >= 0 do
    with :ok <- validate_node_name(node_name),
         {:ok, timeout_ms} <- validate_membership_timeout_ms(timeout_ms) do
      commit_config_or_redirect(shard_index, action, node_name, timeout_ms, redirects_left)
    end
  end

  @spec add_member(non_neg_integer(), node(), keyword()) :: {:ok, tuple()} | {:error, term()}
  def add_member(shard_index, node_name, opts \\ [])

  def add_member(shard_index, _node_name, _opts) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def add_member(shard_index, node_name, opts) do
    with :ok <- validate_node_name(node_name),
         {:ok, timeout_ms} <-
           validate_membership_timeout_ms(Keyword.get(opts, :timeout_ms, 10_000)) do
      add_member_or_redirect(shard_index, node_name, timeout_ms, @config_redirects)
    end
  end

  @doc false
  @spec add_member_redirected(non_neg_integer(), node(), non_neg_integer(), non_neg_integer()) ::
          {:ok, tuple()} | {:error, term()}
  def add_member_redirected(shard_index, _node_name, _timeout_ms, _redirects_left)
      when invalid_shard_index_shape(shard_index),
      do: invalid_shard_index_error(shard_index)

  def add_member_redirected(_shard_index, _node_name, _timeout_ms, redirects_left)
      when invalid_redirects_left_shape(redirects_left),
      do: invalid_redirects_left_error(redirects_left)

  def add_member_redirected(shard_index, node_name, timeout_ms, redirects_left)
      when is_integer(redirects_left) and redirects_left >= 0 do
    with :ok <- validate_node_name(node_name),
         {:ok, timeout_ms} <- validate_membership_timeout_ms(timeout_ms) do
      add_member_or_redirect(shard_index, node_name, timeout_ms, redirects_left)
    end
  end

  @spec add_participant(non_neg_integer(), node(), keyword()) :: {:ok, tuple()} | {:error, term()}
  def add_participant(shard_index, node_name, opts \\ [])

  def add_participant(shard_index, _node_name, _opts) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def add_participant(shard_index, node_name, opts) do
    with :ok <- validate_node_name(node_name),
         {:ok, timeout_ms} <-
           validate_membership_timeout_ms(Keyword.get(opts, :timeout_ms, 10_000)) do
      add_participant_or_redirect(shard_index, node_name, timeout_ms, @config_redirects)
    end
  end

  @doc false
  @spec add_participant_redirected(
          non_neg_integer(),
          node(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, tuple()} | {:error, term()}
  def add_participant_redirected(shard_index, _node_name, _timeout_ms, _redirects_left)
      when invalid_shard_index_shape(shard_index),
      do: invalid_shard_index_error(shard_index)

  def add_participant_redirected(_shard_index, _node_name, _timeout_ms, redirects_left)
      when invalid_redirects_left_shape(redirects_left),
      do: invalid_redirects_left_error(redirects_left)

  def add_participant_redirected(shard_index, node_name, timeout_ms, redirects_left)
      when is_integer(redirects_left) and redirects_left >= 0 do
    with :ok <- validate_node_name(node_name),
         {:ok, timeout_ms} <- validate_membership_timeout_ms(timeout_ms) do
      add_participant_or_redirect(shard_index, node_name, timeout_ms, redirects_left)
    end
  end

  defp add_member_or_redirect(shard_index, node_name, timeout_ms, redirects_left) do
    case workflow_redirect_node(shard_index, redirects_left) do
      leader_node when is_atom(leader_node) and not is_nil(leader_node) ->
        redirect_add_member(leader_node, shard_index, node_name, timeout_ms, redirects_left)

      _other ->
        add_member_local(shard_index, node_name, timeout_ms)
    end
  end

  defp add_member_local(shard_index, node_name, timeout_ms) do
    with :ok <- ensure_participant(shard_index, node_name, timeout_ms),
         {:ok, snapshot_position, snapshot_path} <- create_transfer_snapshot(shard_index),
         {:ok, _transport_id} <-
           transfer_snapshot(shard_index, node_name, snapshot_position, snapshot_path, timeout_ms),
         :ok <- wait_peer_ready(shard_index, node_name, timeout_ms),
         {:ok, position} <- promote_participant(shard_index, node_name, timeout_ms) do
      {:ok, position}
    else
      :already_member ->
        current_member_position(shard_index, node_name, timeout_ms)

      {:error, :already_member} ->
        current_member_position(shard_index, node_name, timeout_ms)

      other ->
        other
    end
  end

  defp add_participant_or_redirect(shard_index, node_name, timeout_ms, redirects_left) do
    case workflow_redirect_node(shard_index, redirects_left) do
      leader_node when is_atom(leader_node) and not is_nil(leader_node) ->
        redirect_add_participant(leader_node, shard_index, node_name, timeout_ms, redirects_left)

      _other ->
        add_participant_local(shard_index, node_name, timeout_ms)
    end
  end

  defp add_participant_local(shard_index, node_name, timeout_ms) do
    with :ok <- ensure_participant(shard_index, node_name, timeout_ms),
         {:ok, snapshot_position, snapshot_path} <- create_transfer_snapshot(shard_index),
         {:ok, _transport_id} <-
           transfer_snapshot(shard_index, node_name, snapshot_position, snapshot_path, timeout_ms),
         :ok <- wait_peer_ready(shard_index, node_name, timeout_ms) do
      {:ok, snapshot_position}
    else
      :already_member ->
        {:error, :already_member}

      other ->
        other
    end
  end

  @spec peer_ready(non_neg_integer(), node()) :: :ok | {:error, term()}
  def peer_ready(shard_index, _node_name) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def peer_ready(shard_index, node_name) do
    with :ok <- validate_node_name(node_name) do
      do_peer_ready(shard_index, node_name)
    end
  end

  defp do_peer_ready(shard_index, node_name) do
    partition = partition(shard_index)
    server = server_name(partition)
    backend_call(fn -> :wa_raft_server.is_peer_ready(server, {server, node_name}) end)
  end

  defp submit_write_many_entry({shard_index, command}) do
    {shard_index, command, submit_commit_async(shard_index, command)}
  end

  defp submit_write_many_entry(entry), do: {:invalid, entry}

  defp await_write_many_entry({:invalid, entry}),
    do: {:error, {:invalid_write_many_entry, entry}}

  defp await_write_many_entry({shard_index, command, submission}) do
    submission
    |> await_commit_async()
    |> maybe_redirect_commit(shard_index, command, 2)
    |> normalize_commit_result()
  end

  @spec local_get(term(), term()) :: binary() | nil | {:error, term()}
  def local_get(shard_index, _key) when invalid_shard_index_shape(shard_index),
    do: invalid_shard_index_error(shard_index)

  def local_get(_shard_index, key) when not is_binary(key), do: invalid_key_error(key)

  def local_get(_shard_index, key) when is_binary(key) do
    with {:ok, ctx} <- context(@table) do
      Ferricstore.Store.Router.get(ctx, key)
    end
  end

  defp commit_or_redirect(shard_index, _command, _redirects_left)
       when invalid_shard_index_shape(shard_index),
       do: invalid_shard_index_error(shard_index)

  defp commit_or_redirect(shard_index, command, redirects_left) do
    case commit(shard_index, command) do
      {:error, _reason} = error ->
        error
        |> maybe_redirect_commit(shard_index, command, redirects_left)
        |> normalize_commit_result()

      result ->
        result
    end
  end

  defp maybe_namespace_window_write(shard_index, command) do
    if NamespaceConfig.has_overrides?() do
      prefix = Ferricstore.Raft.CommandPrefix.extract(command)
      window_ms = NamespaceConfig.window_for(prefix)

      if window_ms > NamespaceConfig.default_window_ms() do
        NamespaceBatcher.write(shard_index, prefix, command, window_ms)
      else
        :direct
      end
    else
      :direct
    end
  end

  defp commit(shard_index, command) do
    with :ok <- ensure_local_leader_connected_quorum(shard_index),
         {:ok, acquired_bytes} <- acquire_commit_bytes(shard_index, command) do
      try do
        with {:ok, prepared_command} <- prepare_commit_command(shard_index, command) do
          acceptor = :wa_raft_acceptor.registered_name(@table, partition(shard_index))
          acceptor_pid = Process.whereis(acceptor)
          stamped = CommandStamp.to_ttb(prepared_command)
          started_mono = System.monotonic_time()

          result =
            acceptor
            |> commit_safely(acceptor_pid, {make_ref(), stamped})
            |> normalize_commit_transport_result(acceptor_pid)

          emit_commit_timeout_if_needed(
            shard_index,
            command,
            result,
            acquired_bytes,
            started_mono,
            :sync
          )

          result
        end
      after
        release_commit_bytes(shard_index, acquired_bytes)
      end
    else
      {:error, _reason} = error -> error
    end
  end

  defp submit_commit_async(shard_index, _command) when invalid_shard_index_shape(shard_index),
    do: {:immediate, invalid_shard_index_error(shard_index)}

  defp submit_commit_async(shard_index, command) do
    with :ok <- ensure_local_leader_connected_quorum(shard_index),
         {:ok, acquired_bytes} <- acquire_commit_bytes(shard_index, command) do
      submit_acquired_commit_async(shard_index, command, acquired_bytes)
    else
      {:error, _reason} = error -> {:immediate, error}
    end
  end

  defp ensure_local_leader_connected_quorum(shard_index) do
    case cached_voter_nodes(shard_index) do
      {:ok, voters} ->
        ensure_connected_voter_quorum(voters)

      :unknown ->
        ensure_local_leader_connected_quorum_from_status(shard_index)
    end
  end

  defp ensure_local_leader_connected_quorum_from_status(shard_index) do
    case status(shard_index) do
      status when is_list(status) ->
        config = Keyword.get(status, :config, %{})
        cache_config(shard_index, config)

        if Keyword.get(status, :state) == :leader do
          config
          |> config_voter_nodes()
          |> ensure_connected_voter_quorum()
        else
          :ok
        end

      _other ->
        :ok
    end
  catch
    _kind, _reason -> :ok
  end

  defp ensure_connected_voter_quorum(voters) do
    if connected_voter_quorum?(voters), do: :ok, else: {:error, :no_quorum}
  end

  defp connected_voter_quorum?(voters) when is_list(voters) do
    case voters do
      [] ->
        true

      [_single] ->
        true

      voters ->
        connected = MapSet.new([node() | Node.list(:connected)])
        reachable = Enum.count(voters, &MapSet.member?(connected, &1))
        reachable >= quorum_size(length(voters))
    end
  end

  defp connected_voter_quorum?(_voters), do: true

  defp quorum_size(voter_count), do: div(voter_count, 2) + 1

  defp cached_voter_nodes(shard_index) do
    case :persistent_term.get({@voter_nodes_key, shard_index}, :unknown) do
      voters when is_list(voters) -> {:ok, voters}
      _other -> :unknown
    end
  end

  defp config_voter_nodes(%{} = config) do
    config
    |> Map.get(:membership, [])
    |> Enum.map(&peer_node/1)
    |> Enum.filter(&valid_node_name?/1)
    |> Enum.uniq()
  end

  defp config_voter_nodes(_config), do: []

  defp submit_acquired_commit_async(shard_index, command, acquired_bytes) do
    case prepare_commit_command(shard_index, command) do
      {:ok, prepared_command} ->
        acceptor = :wa_raft_acceptor.registered_name(@table, partition(shard_index))

        case Process.whereis(acceptor) do
          pid when is_pid(pid) ->
            reply_ref = make_ref()
            reply_alias = :erlang.alias([:reply])
            command_ref = make_ref()
            stamped = CommandStamp.to_ttb(prepared_command)
            command_shape = command_shape(command)
            started_mono = System.monotonic_time()

            case commit_async_safely(
                   acceptor,
                   {reply_alias, reply_ref},
                   {command_ref, stamped}
                 ) do
              :ok ->
                {:submitted, shard_index, reply_alias, reply_ref, pid, acquired_bytes,
                 command_shape, started_mono}

              {:error, _reason} = error ->
                flush_reply_alias(reply_alias, reply_ref)
                release_commit_bytes(shard_index, acquired_bytes)
                {:immediate, normalize_commit_transport_result(error, pid)}
            end

          nil ->
            release_commit_bytes(shard_index, acquired_bytes)
            {:immediate, {:error, :unreachable}}
        end

      {:error, _reason} = error ->
        release_commit_bytes(shard_index, acquired_bytes)
        {:immediate, error}
    end
  end

  defp commit_async_safely(acceptor, reply_to, command) do
    :wa_raft_acceptor.commit_async(acceptor, reply_to, command, :low)
  catch
    kind, reason -> {:error, {:commit_call_failed_after_submit, {kind, reason}}}
  end

  defp commit_safely(_acceptor, nil, _command), do: {:error, :unreachable}

  defp commit_safely(acceptor, _acceptor_pid, command) do
    :wa_raft_acceptor.commit(acceptor, command, @timeout, :low)
  catch
    kind, reason -> {:error, {:commit_call_failed_after_submit, {kind, reason}}}
  end

  defp await_commit_async({:immediate, result}), do: result

  defp await_commit_async(
         {:submitted, shard_index, reply_alias, reply_ref, acceptor_pid, acquired_bytes,
          command_shape, started_mono}
       ) do
    try do
      receive do
        {^reply_ref, result} -> normalize_commit_transport_result(result, acceptor_pid)
      after
        @timeout ->
          emit_commit_timeout(shard_index, command_shape, acquired_bytes, started_mono, :async)
          {:error, :timeout}
      end
    after
      flush_reply_alias(reply_alias, reply_ref)
      release_commit_bytes(shard_index, acquired_bytes)
    end
  end

  defp flush_reply_alias(reply_alias, reply_ref) do
    :erlang.unalias(reply_alias)

    receive do
      {^reply_ref, _late_result} -> :ok
    after
      0 -> :ok
    end
  end

  defp normalize_commit_transport_result({:error, :unreachable}, pid) when is_pid(pid),
    do: {:error, :commit_unreachable_after_submit}

  defp normalize_commit_transport_result({:error, {:call_error, reason}}, pid) when is_pid(pid),
    do: {:error, {:commit_call_failed_after_submit, reason}}

  defp normalize_commit_transport_result({:error, :not_leader}, pid) when is_pid(pid),
    do: {:error, :not_leader_after_submit}

  defp normalize_commit_transport_result(result, _pid), do: result

  defp prepare_commit_command(shard_index, command) do
    case fetch_context() do
      {:ok, ctx} ->
        result =
          try do
            if BlobCommand.side_channel_candidate?(ctx, command) do
              BlobCommand.prepare(ctx, shard_index, command,
                single_member?: single_member_waraft_group?(shard_index)
              )
            else
              {:ok, command}
            end
          rescue
            error ->
              {:error, {:blob_prepare_failed, {error.__struct__, Exception.message(error)}}}
          catch
            kind, reason ->
              {:error, {:blob_prepare_failed, {kind, reason}}}
          end

        case result do
          {:error, reason} = error ->
            emit_blob_prepare_failed(shard_index, command, reason)
            error

          other ->
            other
        end

      :error ->
        {:error, :unreachable}
    end
  end

  defp fetch_context do
    {:ok, context!(@table)}
  rescue
    ArgumentError -> :error
  catch
    _kind, _reason -> :error
  end

  defp single_member_waraft_group?(shard_index) do
    case status(shard_index) do
      status when is_list(status) ->
        single_participant_config?(Keyword.get(status, :config, %{}))

      _other ->
        false
    end
  rescue
    _error -> false
  catch
    :exit, _reason -> false
  end

  # A staged participant receives Raft entries before it becomes a voter. Blob
  # side-channel refs are local files, so they are safe only while no other data
  # replica can apply the command.
  defp single_participant_config?(%{} = config) do
    participants = Map.get(config, :participants, Map.get(config, :membership, []))
    membership = Map.get(config, :membership, [])
    witnesses = Map.get(config, :witness, Map.get(config, :witnesses, []))

    case {participants, membership, witnesses} do
      {[peer], [peer], []} -> true
      _other -> false
    end
  end

  defp single_participant_config?(_config), do: false

  defp maybe_redirect_commit(error, _shard_index, _command, redirects_left)
       when redirects_left <= 0,
       do: error

  defp maybe_redirect_commit({:error, :not_leader} = error, shard_index, command, redirects_left) do
    case local_leader_node(shard_index) do
      node_name when is_atom(node_name) and not is_nil(node_name) and node_name != node() ->
        redirect_commit(node_name, shard_index, command, redirects_left)

      _other ->
        error
    end
  end

  defp maybe_redirect_commit(
         {:error, :not_leader_after_submit} = error,
         shard_index,
         command,
         redirects_left
       ) do
    case local_leader_node(shard_index) do
      node_name when is_atom(node_name) and not is_nil(node_name) and node_name != node() ->
        redirect_commit(node_name, shard_index, command, redirects_left)

      _other ->
        error
    end
  end

  defp maybe_redirect_commit(
         {:error, {:notify_redirect, peer}} = error,
         shard_index,
         command,
         redirects_left
       ) do
    case peer_node(peer) do
      node_name when is_atom(node_name) and not is_nil(node_name) and node_name != node() ->
        redirect_commit(node_name, shard_index, command, redirects_left)

      _other ->
        error
    end
  end

  defp maybe_redirect_commit(error, _shard_index, _command, _redirects_left), do: error

  defp redirect_commit(node_name, shard_index, command, redirects_left) do
    try do
      :erpc.call(
        node_name,
        __MODULE__,
        :write_redirected,
        [shard_index, command, redirects_left - 1],
        @timeout
      )
    catch
      kind, reason -> redirect_write_failure(kind, reason)
    end
  end

  defp redirect_write_failure(_kind, {:erpc, :timeout}), do: {:error, :timeout}
  defp redirect_write_failure(_kind, {:erpc, :noconnection}), do: {:error, :leader_unavailable}
  defp redirect_write_failure(_kind, _reason), do: {:error, :leader_unavailable}

  defp redirect_membership_failure(_kind, {:erpc, :timeout}),
    do: ErrorReasons.write_timeout_unknown()

  defp redirect_membership_failure(_kind, reason), do: redirect_write_failure(:exit, reason)

  defp transfer_leadership_or_redirect(shard_index, target_node, redirects_left) do
    shard_index
    |> local_transfer_leadership(target_node)
    |> maybe_redirect_transfer(shard_index, target_node, redirects_left)
  end

  defp local_transfer_leadership(shard_index, target_node) do
    server =
      shard_index
      |> partition()
      |> server_name()

    case backend_call(fn -> server |> :wa_raft_server.handover(target_node) end) do
      {:ok, _peer} -> :ok
      :ok -> :ok
      {:error, :backend_unavailable} -> {:error, :unreachable}
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp maybe_redirect_transfer(:ok, _shard_index, _target_node, _redirects_left), do: :ok

  defp maybe_redirect_transfer(error, _shard_index, _target_node, redirects_left)
       when redirects_left <= 0,
       do: normalize_transfer_result(error)

  defp maybe_redirect_transfer(
         {:error, :not_leader} = error,
         shard_index,
         target_node,
         redirects_left
       ) do
    case local_leader_node(shard_index) do
      node_name when is_atom(node_name) and not is_nil(node_name) and node_name != node() ->
        redirect_transfer(node_name, shard_index, target_node, redirects_left)

      _other ->
        normalize_transfer_result(error)
    end
  end

  defp maybe_redirect_transfer(
         {:error, {:notify_redirect, peer}} = error,
         shard_index,
         target_node,
         redirects_left
       ) do
    case peer_node(peer) do
      node_name when is_atom(node_name) and not is_nil(node_name) and node_name != node() ->
        redirect_transfer(node_name, shard_index, target_node, redirects_left)

      _other ->
        normalize_transfer_result(error)
    end
  end

  defp maybe_redirect_transfer(error, _shard_index, _target_node, _redirects_left),
    do: normalize_transfer_result(error)

  defp redirect_transfer(node_name, shard_index, target_node, redirects_left) do
    try do
      :erpc.call(
        node_name,
        __MODULE__,
        :transfer_leadership_redirected,
        [shard_index, target_node, redirects_left - 1],
        @timeout
      )
    catch
      kind, reason -> redirect_transfer_failure(kind, reason)
    end
  end

  defp redirect_transfer_failure(_kind, {:erpc, :timeout}),
    do: ErrorReasons.write_timeout_unknown()

  defp redirect_transfer_failure(_kind, {:erpc, :noconnection}), do: {:error, :leader_unavailable}
  defp redirect_transfer_failure(_kind, _reason), do: {:error, :leader_unavailable}

  defp normalize_transfer_result({:error, :unreachable}), do: {:error, "ERR shard not available"}

  defp normalize_transfer_result({:error, :timeout}), do: ErrorReasons.write_timeout_unknown()

  defp normalize_transfer_result({:error, :leader_unavailable}),
    do: {:error, "ERR leader unavailable"}

  defp normalize_transfer_result({:error, :not_leader}), do: {:error, "ERR leader unavailable"}

  defp normalize_transfer_result({:error, {:notify_redirect, _peer}}),
    do: {:error, "ERR leader unavailable"}

  defp normalize_transfer_result(result), do: result

  defp normalize_commit_result({:error, reason})
       when reason in [:commit_queue_full, :apply_queue_full, :commit_bytes_full],
       do: {:error, :overloaded}

  defp normalize_commit_result({:error, reason})
       when is_storage_unknown_outcome_reason(reason),
       do: ErrorReasons.write_timeout_unknown()

  defp normalize_commit_result({:error, :timeout}), do: ErrorReasons.write_timeout_unknown()
  defp normalize_commit_result({:error, :unreachable}), do: {:error, "ERR shard not available"}

  defp normalize_commit_result({:error, :leader_unavailable}),
    do: {:error, "ERR leader unavailable"}

  defp normalize_commit_result({:error, :not_leader}), do: {:error, "ERR leader unavailable"}
  defp normalize_commit_result({:error, :commit_stalled}), do: {:error, "ERR leader unavailable"}

  defp normalize_commit_result({:error, {:notify_redirect, _peer}}),
    do: {:error, "ERR leader unavailable"}

  defp normalize_commit_result(result), do: result

  defp local_leader_node(shard_index) do
    case status(shard_index) do
      status when is_list(status) -> peer_node(Keyword.get(status, :leader_id))
      _other -> nil
    end
  catch
    _, _ -> nil
  end

  defp leader_node_for_redirect(shard_index) do
    case local_leader_node(shard_index) do
      leader_node
      when is_atom(leader_node) and not is_nil(leader_node) and leader_node != node() ->
        leader_node

      _other ->
        remote_leader_node_from_members(shard_index)
    end
  end

  defp remote_leader_node_from_members(shard_index) do
    case membership(shard_index) do
      members when is_list(members) ->
        members
        |> Enum.map(&peer_node/1)
        |> Enum.reject(fn peer -> peer in [nil, node()] end)
        |> Enum.find(fn peer ->
          case remote_status(peer, shard_index) do
            status when is_list(status) -> Keyword.get(status, :state) == :leader
            _other -> false
          end
        end)

      _other ->
        nil
    end
  catch
    _, _ -> nil
  end

  defp remote_status(peer, shard_index) do
    :erpc.call(peer, __MODULE__, :status, [shard_index], 1_000)
  catch
    _, _ -> nil
  end

  defp peer_node({:raft_identity, _name, node_name}), do: validated_peer_node(node_name)
  defp peer_node({_name, node_name}), do: validated_peer_node(node_name)
  defp peer_node(node_name) when is_atom(node_name), do: validated_peer_node(node_name)
  defp peer_node(_peer), do: nil

  defp validated_peer_node(node_name) do
    if valid_node_name?(node_name), do: node_name, else: nil
  end

  defp finish_started(shard_count, opts) do
    case finish_start(shard_count, opts) do
      :ok -> :ok
      {:error, _reason} = error -> cleanup_failed_start(error)
    end
  end

  defp partition_spec(partition, config) do
    %{
      table: @table,
      partition: partition,
      log_module: config.log_module,
      storage_module: :ferricstore_waraft_backend_storage
    }
    |> maybe_put(:label_module, config.label_module)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp cleanup_failed_start(error) do
    _ = stop()
    error
  end

  defp finish_start(shard_count, opts) do
    with :ok <-
           0..(shard_count - 1)
           |> Enum.reduce_while(:ok, fn shard_index, :ok ->
             case finish_start_partition(shard_index, opts) do
               :ok -> {:cont, :ok}
               {:error, _reason} = error -> {:halt, error}
             end
           end) do
      start_namespace_batchers(shard_count, opts)
    end
  end

  defp finish_start_partition(shard_index, opts) do
    server = :wa_raft_server.registered_name(@table, partition(shard_index))
    bootstrap? = Keyword.get(opts, :bootstrap, true)
    replay_target = segment_log_last_index(shard_index)

    with {:ok, status} <- wait_status(server, 100),
         :ok <- finish_start_status(server, status, bootstrap?),
         :ok <- wait_log_replayed(server, replay_target, 100),
         :ok <- wait_storage_replayed(shard_index, replay_target, 100) do
      maybe_cache_current_config(shard_index)
    end
  end

  defp finish_start_status(_server, _status, false), do: :ok

  defp finish_start_status(server, status, true) do
    case Keyword.get(status, :state) do
      :stalled ->
        with :ok <- bootstrap(server) do
          wait_leader(server, 100)
        end

      :leader ->
        :ok

      _other ->
        case backend_call(fn -> :wa_raft_server.promote(server, :next, true) end) do
          :ok -> wait_leader(server, 100)
          {:error, _reason} = error -> error
          other -> {:error, other}
        end
    end
  end

  defp bootstrap(server) do
    config =
      :wa_raft_server.make_config([
        {:raft_identity, server, node()}
      ])

    case backend_call(fn ->
           :wa_raft_server.bootstrap(server, {:raft_log_pos, 1, 1}, config, %{})
         end) do
      :ok -> :ok
      {:error, :already_bootstrapped} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp bootstrap_cluster_partition(shard_index, nodes) do
    server = server_name(partition(shard_index))

    config =
      nodes
      |> Enum.map(fn node -> {:raft_identity, server, node} end)
      |> :wa_raft_server.make_config()

    case backend_call(fn ->
           :wa_raft_server.bootstrap(server, {:raft_log_pos, 1, 1}, config, %{})
         end) do
      :ok ->
        cache_config(shard_index, config)

      {:error, :already_bootstrapped} ->
        handle_already_bootstrapped_cluster_partition(shard_index, nodes)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_bootstrap_nodes([]), do: {:error, :empty_cluster}

  defp validate_bootstrap_nodes(nodes) do
    Enum.reduce_while(nodes, MapSet.new(), fn
      node_name, seen ->
        case validate_node_name(node_name) do
          :ok ->
            if MapSet.member?(seen, node_name) do
              {:halt, {:error, {:duplicate_node, node_name}}}
            else
              {:cont, MapSet.put(seen, node_name)}
            end

          {:error, _reason} = error ->
            {:halt, error}
        end
    end)
    |> case do
      %MapSet{} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp invalid_shard_index_error(shard_index), do: {:error, {:invalid_shard_index, shard_index}}

  defp invalid_membership_action_error(action),
    do: {:error, {:invalid_membership_action, action}}

  defp invalid_redirects_left_error(redirects_left),
    do: {:error, {:invalid_redirects_left, redirects_left}}

  defp invalid_key_error(key), do: {:error, {:invalid_key, key}}

  defp backend_unavailable_error, do: {:error, :backend_unavailable}

  defp backend_call(fun) when is_function(fun, 0) do
    fun.()
  catch
    :exit, reason -> backend_exit_error(reason)
    kind, reason -> {:error, {kind, reason}}
  end

  defp backend_exit_error(:noproc), do: backend_unavailable_error()
  defp backend_exit_error({:noproc, _call}), do: backend_unavailable_error()
  defp backend_exit_error({:normal, _call}), do: backend_unavailable_error()
  defp backend_exit_error({:shutdown, _call}), do: backend_unavailable_error()
  defp backend_exit_error(reason), do: {:error, {:exit, reason}}

  defp invalid_snapshot_path_error(path), do: {:error, {:invalid_snapshot_path, path}}

  defp invalid_snapshot_position_error(position),
    do: {:error, {:invalid_snapshot_position, position}}

  defp snapshot_path_charlist(path) when is_binary(path) do
    path
    |> String.to_charlist()
    |> validate_snapshot_path_chars(path)
  end

  defp snapshot_path_charlist(path) when is_list(path) do
    path
    |> to_charlist()
    |> validate_snapshot_path_chars(path)
  rescue
    _error -> invalid_snapshot_path_error(path)
  catch
    _kind, _reason -> invalid_snapshot_path_error(path)
  end

  defp snapshot_path_charlist(path), do: invalid_snapshot_path_error(path)

  defp validate_snapshot_path_chars([], original), do: invalid_snapshot_path_error(original)
  defp validate_snapshot_path_chars(chars, _original), do: {:ok, chars}

  defp validate_snapshot_position({:raft_log_pos, index, term})
       when is_integer(index) and index >= 0 and is_integer(term) and term >= 0,
       do: :ok

  defp validate_snapshot_position(position), do: invalid_snapshot_position_error(position)

  defp validate_node_name(node_name) do
    if valid_node_name?(node_name) do
      :ok
    else
      {:error, {:invalid_node, node_name}}
    end
  end

  defp valid_node_name?(node_name)
       when is_atom(node_name) and node_name not in [nil, true, false],
       do: true

  defp valid_node_name?(_node_name), do: false

  defp validate_membership_timeout_ms(timeout_ms)
       when is_integer(timeout_ms) and timeout_ms >= 0,
       do: {:ok, timeout_ms}

  defp validate_membership_timeout_ms(timeout_ms),
    do: {:error, {:invalid_timeout_ms, timeout_ms}}

  defp handle_already_bootstrapped_cluster_partition(shard_index, requested_nodes) do
    actual_config = current_partition_config(shard_index)
    actual_nodes = config_voter_nodes(actual_config)

    if MapSet.new(actual_nodes) == MapSet.new(requested_nodes) do
      cache_config(shard_index, actual_config)
    else
      {:error, {:already_bootstrapped, actual_nodes}}
    end
  end

  defp current_partition_config(shard_index) do
    case status(shard_index) do
      status when is_list(status) -> Keyword.get(status, :config, %{})
      _other -> %{}
    end
  catch
    _kind, _reason -> %{}
  end

  defp adjust_config(shard_index, action, node_name) do
    partition = partition(shard_index)
    server = server_name(partition)
    backend_call(fn -> :wa_raft_server.adjust_config(server, {action, {server, node_name}}) end)
  end

  defp commit_config_or_redirect(shard_index, action, node_name, timeout_ms, redirects_left) do
    case commit_config(shard_index, action, node_name, timeout_ms) do
      {:error, _reason} = error ->
        maybe_redirect_config(error, shard_index, action, node_name, timeout_ms, redirects_left)

      result ->
        result
    end
  end

  defp maybe_redirect_config(
         error,
         _shard_index,
         _action,
         _node_name,
         _timeout_ms,
         redirects_left
       )
       when redirects_left <= 0,
       do: error

  defp maybe_redirect_config(
         {:error, :not_leader} = error,
         shard_index,
         action,
         node_name,
         timeout_ms,
         redirects_left
       ) do
    case leader_node_for_redirect(shard_index) do
      leader_node
      when is_atom(leader_node) and not is_nil(leader_node) and leader_node != node() ->
        redirect_config(leader_node, shard_index, action, node_name, timeout_ms, redirects_left)

      _other ->
        error
    end
  end

  defp maybe_redirect_config(
         {:error, {:notify_redirect, peer}} = error,
         shard_index,
         action,
         node_name,
         timeout_ms,
         redirects_left
       ) do
    case peer_node(peer) do
      leader_node
      when is_atom(leader_node) and not is_nil(leader_node) and leader_node != node() ->
        redirect_config(leader_node, shard_index, action, node_name, timeout_ms, redirects_left)

      _other ->
        error
    end
  end

  defp maybe_redirect_config(
         error,
         _shard_index,
         _action,
         _node_name,
         _timeout_ms,
         _redirects_left
       ),
       do: error

  defp redirect_config(leader_node, shard_index, action, node_name, timeout_ms, redirects_left) do
    try do
      :erpc.call(
        leader_node,
        __MODULE__,
        :adjust_membership_redirected,
        [shard_index, action, node_name, timeout_ms, redirects_left - 1],
        timeout_ms
      )
    catch
      kind, reason -> redirect_membership_failure(kind, reason)
    end
  end

  defp workflow_redirect_node(_shard_index, redirects_left) when redirects_left <= 0, do: nil

  defp workflow_redirect_node(shard_index, _redirects_left) do
    case status(shard_index) do
      status when is_list(status) ->
        case Keyword.get(status, :state) do
          :leader -> nil
          _other -> leader_node_for_redirect(shard_index)
        end

      _other ->
        leader_node_for_redirect(shard_index)
    end
  catch
    _, _ -> nil
  end

  defp redirect_add_member(leader_node, shard_index, node_name, timeout_ms, redirects_left) do
    try do
      :erpc.call(
        leader_node,
        __MODULE__,
        :add_member_redirected,
        [shard_index, node_name, timeout_ms, redirects_left - 1],
        timeout_ms
      )
    catch
      kind, reason -> redirect_membership_failure(kind, reason)
    end
  end

  defp redirect_add_participant(leader_node, shard_index, node_name, timeout_ms, redirects_left) do
    try do
      :erpc.call(
        leader_node,
        __MODULE__,
        :add_participant_redirected,
        [shard_index, node_name, timeout_ms, redirects_left - 1],
        timeout_ms
      )
    catch
      kind, reason -> redirect_membership_failure(kind, reason)
    end
  end

  defp commit_config(shard_index, action, node_name, timeout_ms) do
    case adjust_config(shard_index, action, node_name) do
      {:ok, position} ->
        case wait_storage_durable_position(shard_index, position, timeout_ms) do
          :ok ->
            {:ok, position}

          {:error, reason} when is_storage_unknown_outcome_reason(reason) ->
            ErrorReasons.write_timeout_unknown()

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} when is_storage_unknown_outcome_reason(reason) ->
        ErrorReasons.write_timeout_unknown()

      other ->
        other
    end
  end

  defp wait_storage_durable_position(_shard_index, _position, timeout_ms) when timeout_ms < 0,
    do: {:error, :storage_apply_timeout}

  defp wait_storage_durable_position(shard_index, target_position, timeout_ms) do
    case storage_status(shard_index) do
      status when is_list(status) ->
        cond do
          reason = Keyword.get(status, :blocked_error) ->
            {:error, {:storage_blocked, reason}}

          position_reached?(Keyword.get(status, :durable_position), target_position) ->
            :ok

          true ->
            Process.sleep(@config_apply_poll_ms)

            wait_storage_durable_position(
              shard_index,
              target_position,
              timeout_ms - @config_apply_poll_ms
            )
        end

      _other ->
        Process.sleep(@config_apply_poll_ms)

        wait_storage_durable_position(
          shard_index,
          target_position,
          timeout_ms - @config_apply_poll_ms
        )
    end
  catch
    _kind, _reason ->
      Process.sleep(@config_apply_poll_ms)

      wait_storage_durable_position(
        shard_index,
        target_position,
        timeout_ms - @config_apply_poll_ms
      )
  end

  defp storage_status(shard_index) do
    storage =
      shard_index
      |> partition()
      |> then(&:wa_raft_storage.registered_name(@table, &1))

    backend_call(fn -> :wa_raft_storage.status(storage) end)
  end

  defp position_reached?(
         {:raft_log_pos, durable_index, _durable_term},
         {:raft_log_pos, index, _term}
       )
       when is_integer(durable_index) and is_integer(index),
       do: durable_index >= index

  defp position_reached?(_durable_position, _target_position), do: false

  defp normalize_identity({:raft_identity, name, node_name}), do: {name, node_name}
  defp normalize_identity(other), do: other

  defp ensure_participant(shard_index, node_name, timeout_ms) do
    case commit_config_or_redirect(
           shard_index,
           :add_participant,
           node_name,
           timeout_ms,
           @config_redirects
         ) do
      {:ok, _position} ->
        wait_storage_participant(shard_index, node_name, timeout_ms)

      {:error, :already_participating} ->
        wait_storage_participant(shard_index, node_name, timeout_ms)

      {:error, :already_member} ->
        :already_member

      {:error, _reason} = error ->
        error
    end
  end

  defp promote_participant(shard_index, node_name, timeout_ms) do
    case commit_config_or_redirect(
           shard_index,
           :promote_participant_if_ready,
           node_name,
           timeout_ms,
           @config_redirects
         ) do
      {:ok, position} ->
        with :ok <- wait_storage_member(shard_index, node_name, timeout_ms) do
          {:ok, position}
        end

      {:error, :already_member} ->
        current_member_position(shard_index, node_name, timeout_ms)

      {:error, _reason} = error ->
        error
    end
  end

  defp current_member_position(shard_index, node_name, timeout_ms) do
    with :ok <- wait_storage_member(shard_index, node_name, timeout_ms),
         {:ok, position} <- storage_position(shard_index) do
      {:ok, position}
    end
  end

  defp wait_storage_participant(_shard_index, _node_name, timeout_ms) when timeout_ms < 0 do
    {:error, :participant_config_timeout}
  end

  defp wait_storage_participant(shard_index, node_name, timeout_ms) do
    if storage_participant?(shard_index, node_name) do
      :ok
    else
      Process.sleep(50)
      wait_storage_participant(shard_index, node_name, timeout_ms - 50)
    end
  end

  defp storage_participant?(shard_index, node_name) do
    partition = partition(shard_index)
    peer = {server_name(partition), node_name}
    storage = :wa_raft_storage.registered_name(@table, partition)

    case backend_call(fn -> :wa_raft_storage.config(storage) end) do
      {:ok, _position, config} ->
        participants = Map.get(config, :participants, Map.get(config, :membership, []))
        peer in participants

      _other ->
        false
    end
  end

  defp wait_storage_member(_shard_index, _node_name, timeout_ms) when timeout_ms < 0 do
    {:error, :member_config_timeout}
  end

  defp wait_storage_member(shard_index, node_name, timeout_ms) do
    if storage_member?(shard_index, node_name) do
      :ok
    else
      Process.sleep(50)
      wait_storage_member(shard_index, node_name, timeout_ms - 50)
    end
  end

  defp storage_member?(shard_index, node_name) do
    partition = partition(shard_index)
    peer = {server_name(partition), node_name}
    storage = :wa_raft_storage.registered_name(@table, partition)

    case backend_call(fn -> :wa_raft_storage.config(storage) end) do
      {:ok, _position, config} -> peer in Map.get(config, :membership, [])
      _other -> false
    end
  end

  defp create_transfer_snapshot(shard_index) do
    partition = partition(shard_index)

    with {:ok, {:raft_log_pos, index, term} = position} <- create_snapshot(shard_index) do
      snapshot_path =
        @table
        |> :wa_raft_part_sup.registered_partition_path(partition)
        |> Path.join("snapshot.#{index}.#{term}")

      {:ok, position, snapshot_path}
    end
  end

  defp transfer_snapshot(shard_index, node_name, position, snapshot_path, timeout_ms) do
    partition = partition(shard_index)

    backend_call(fn ->
      :wa_raft_transport.transfer_snapshot(
        node_name,
        @table,
        partition,
        position,
        to_charlist(snapshot_path),
        false,
        timeout_ms
      )
    end)
  end

  defp wait_peer_ready(_shard_index, _node_name, timeout_ms) when timeout_ms < 0 do
    {:error, :peer_ready_timeout}
  end

  defp wait_peer_ready(shard_index, node_name, timeout_ms) do
    case peer_ready(shard_index, node_name) do
      :ok ->
        :ok

      {:error, :not_ready} ->
        Process.sleep(50)
        wait_peer_ready(shard_index, node_name, timeout_ms - 50)

      {:error, _reason} = error ->
        error
    end
  end

  defp wait_leader(server, attempts), do: wait_leader(server, attempts, nil)

  defp wait_leader(_server, 0, status), do: {:error, {:leader_timeout, status}}

  defp wait_leader(server, attempts, _status) do
    case wait_status(server, 1) do
      {:ok, status} ->
        case Keyword.get(status, :state) do
          :leader ->
            :ok

          _other ->
            Process.sleep(10)
            wait_leader(server, attempts - 1, status)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp wait_log_replayed(server, target_index, attempts),
    do: wait_log_replayed(server, target_index, attempts, nil)

  defp wait_log_replayed(_server, target_index, 0, status),
    do: {:error, {:replay_timeout, target_index, status}}

  defp wait_log_replayed(server, target_index, attempts, _status) do
    case wait_status(server, 1) do
      {:ok, status} ->
        if log_replayed?(status, target_index) do
          :ok
        else
          Process.sleep(10)
          wait_log_replayed(server, target_index, attempts - 1, status)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp log_replayed?(status, target_index) when is_list(status) do
    last_applied = Keyword.get(status, :last_applied)
    log_last = Keyword.get(status, :log_last)
    target_index = max_integer(log_last, target_index)

    is_integer(last_applied) and last_applied >= target_index
  end

  defp log_replayed?(_status, _target_index), do: false

  defp wait_storage_replayed(_shard_index, target_index, _attempts)
       when not is_integer(target_index) or target_index <= 0,
       do: :ok

  defp wait_storage_replayed(shard_index, target_index, attempts),
    do: wait_storage_replayed(shard_index, target_index, attempts, nil)

  defp wait_storage_replayed(_shard_index, target_index, 0, position),
    do: {:error, {:storage_replay_timeout, target_index, position}}

  defp wait_storage_replayed(shard_index, target_index, attempts, _position) do
    case storage_position(shard_index) do
      {:ok, {:raft_log_pos, index, _term} = position} when is_integer(index) ->
        if index >= target_index do
          :ok
        else
          Process.sleep(10)
          wait_storage_replayed(shard_index, target_index, attempts - 1, position)
        end

      other ->
        Process.sleep(10)
        wait_storage_replayed(shard_index, target_index, attempts - 1, other)
    end
  end

  defp segment_log_last_index(shard_index) do
    root_dir =
      @table
      |> :wa_raft_part_sup.registered_partition_path(partition(shard_index))
      |> to_string()

    case :ferricstore_waraft_spike_segment_log.fold_disk(
           root_dir,
           fn index, _entry, acc -> max(index, acc) end,
           0
         ) do
      {:ok, index} when is_integer(index) and index >= 0 -> index
      _other -> 0
    end
  rescue
    _ -> 0
  catch
    _, _ -> 0
  end

  defp max_integer(left, right) when is_integer(left) and is_integer(right), do: max(left, right)
  defp max_integer(left, _right) when is_integer(left), do: max(left, 0)
  defp max_integer(_left, right) when is_integer(right), do: max(right, 0)
  defp max_integer(_left, _right), do: 0

  defp wait_status(_server, 0), do: {:error, :status_timeout}

  defp wait_status(server, attempts) do
    case backend_call(fn -> :wa_raft_server.status(server) end) do
      {:error, _reason} ->
        Process.sleep(10)
        wait_status(server, attempts - 1)

      status ->
        {:ok, status}
    end
  end

  defp maybe_cache_current_config(shard_index) do
    case status(shard_index) do
      status when is_list(status) ->
        cache_config(shard_index, Keyword.get(status, :config, %{}))

      _other ->
        :ok
    end
  catch
    _kind, _reason -> :ok
  end

  defp restart_present_child(ctx, opts) do
    :ok = Supervisor.delete_child(:kernel_sup, @sup_id)
    start(ctx, opts)
  end

  defp stop_orphaned_waraft_sup do
    name = :wa_raft_sup.default_name(@app)

    case Process.whereis(name) do
      nil ->
        :ok

      _pid ->
        _ =
          try do
            Supervisor.stop(name, :shutdown, :infinity)
          catch
            _kind, _reason -> :ok
          end

        kill_orphaned_waraft_sup(name)
    end
  end

  defp kill_orphaned_waraft_sup(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        # A failed partition start can leave the wa_raft application supervisor
        # outside kernel_sup ownership. Replacement tests must start from a
        # clean supervisor tree, so force-remove that orphan before retrying.
        Process.exit(pid, :kill)
        :ok
    end
  end

  defp erase_cached_voter_nodes(shard_count) when is_integer(shard_count) and shard_count > 0 do
    Enum.each(0..(shard_count - 1), fn shard_index ->
      :persistent_term.erase({@voter_nodes_key, shard_index})
    end)
  end

  defp erase_cached_voter_nodes(_shard_count), do: :ok

  defp start_namespace_batchers(shard_count, opts)
       when is_integer(shard_count) and shard_count > 0 do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn shard_index, :ok ->
      case NamespaceBatcher.start_link(shard_index, opts) do
        {:ok, _pid} -> {:cont, :ok}
        {:error, {:already_started, _pid}} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp start_namespace_batchers(_shard_count, _opts), do: :ok

  defp stop_namespace_batchers(shard_count) when is_integer(shard_count) and shard_count > 0 do
    Enum.each(0..(shard_count - 1), &NamespaceBatcher.stop/1)
  end

  defp stop_namespace_batchers(_shard_count), do: :ok

  defp ensure_started do
    case Application.ensure_all_started(:wa_raft) do
      {:ok, _apps} -> :ok
      {:error, {:already_started, :wa_raft}} -> :ok
      {:error, reason} -> raise "failed to start wa_raft: #{inspect(reason)}"
    end
  end

  defp configure(ctx, config) do
    database = Path.join(ctx.data_dir, "waraft")

    :ok = Application.put_env(@app, :raft_database, to_charlist(database))
    :persistent_term.put(@shard_count_key, ctx.shard_count)
    :persistent_term.put(@inflight_bytes_key, :atomics.new(ctx.shard_count, signed: false))
    :persistent_term.put(@max_inflight_bytes_key, config.max_inflight_commit_bytes)

    :ok =
      Application.put_env(
        @app,
        :raft_max_pending_low_priority_commits,
        config.max_pending_low_priority_commits
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_pending_high_priority_commits,
        config.max_pending_high_priority_commits
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_pending_reads,
        config.max_pending_reads
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_pending_applies,
        config.max_pending_applies
      )

    :ok =
      Application.put_env(
        @app,
        :raft_apply_queue_max_size,
        config.apply_queue_max_size
      )

    :ok =
      Application.put_env(
        @app,
        :raft_commit_batch_interval_ms,
        config.commit_batch_interval_ms
      )

    :ok =
      Application.put_env(
        @app,
        :raft_commit_batch_max,
        config.commit_batch_max
      )

    :ok =
      Application.put_env(
        @app,
        :raft_async_log_append,
        config.async_log_append
      )

    :ok =
      Application.put_env(
        @app,
        :raft_election_timeout_ms,
        config.timeout_ms
      )

    :ok =
      Application.put_env(
        @app,
        :raft_election_timeout_ms_max,
        config.timeout_ms_max
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_log_entries_per_heartbeat,
        config.max_log_entries_per_heartbeat
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_heartbeat_size,
        config.max_heartbeat_size
      )

    :ok =
      Application.put_env(
        @app,
        :raft_apply_log_batch_size,
        config.apply_log_batch_size
      )

    :ok =
      Application.put_env(
        @app,
        :raft_apply_batch_max_bytes,
        config.apply_batch_max_bytes
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_log_records_per_file,
        config.log_rotation_interval
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_log_records,
        config.log_rotation_keep
      )

    :ok =
      Application.put_env(
        @app,
        :raft_max_retained_entries,
        config.max_retained_entries
      )
  end

  defp backend_config!(opts) do
    opts
    |> throughput_options!()
    |> Map.merge(election_options!(opts))
    |> Map.merge(queue_options!(opts))
    |> Map.merge(commit_options!(opts))
    |> Map.merge(module_options!(opts))
    |> Map.put(:max_inflight_commit_bytes, max_inflight_commit_bytes_option!(opts))
  end

  defp module_options!(opts) do
    %{
      log_module: log_module_option!(Keyword.get(opts, :log_module, @default_log_module)),
      label_module:
        optional_module_option!(
          :label_module,
          Keyword.get(opts, :label_module),
          @label_module_callbacks
        )
    }
  end

  defp log_module_option!(:wa_raft_log_ets) do
    raise ArgumentError,
          ":log_module does not support volatile ETS log; WARaft evaluation must use the durable segment/keydir log"
  end

  defp log_module_option!(module) do
    required_module_option!(:log_module, module, @log_module_callbacks)
  end

  defp required_module_option!(source, module, callbacks) do
    module_option!(source, module, callbacks)
  end

  defp optional_module_option!(_source, nil, _callbacks), do: nil

  defp optional_module_option!(source, module, callbacks) do
    module_option!(source, module, callbacks)
  end

  defp module_option!(source, module, callbacks) when is_atom(module) do
    unless Code.ensure_loaded?(module) do
      raise ArgumentError, "#{inspect(source)} module is not loaded: #{inspect(module)}"
    end

    case Enum.find(callbacks, fn {function, arity} ->
           not function_exported?(module, function, arity)
         end) do
      nil ->
        module

      {function, arity} ->
        raise ArgumentError,
              "#{inspect(source)} module #{inspect(module)} is missing #{function}/#{arity}"
    end
  end

  defp module_option!(source, module, _callbacks) do
    raise ArgumentError, "#{inspect(source)} must be a module atom, got: #{inspect(module)}"
  end

  defp election_options!(opts) do
    {timeout_source, timeout_value} =
      config_option(opts, :election_timeout_ms, :waraft_election_timeout_ms, 5_000)

    {timeout_max_source, timeout_max_value} =
      config_option(opts, :election_timeout_ms_max, :waraft_election_timeout_ms_max, 7_500)

    timeout_ms = positive_integer_option!(timeout_source, timeout_value)
    timeout_ms_max = positive_integer_option!(timeout_max_source, timeout_max_value)

    if timeout_ms_max < timeout_ms do
      raise ArgumentError,
            "#{inspect(timeout_max_source)} must be >= #{inspect(timeout_source)}, got: #{inspect(timeout_ms_max)} < #{inspect(timeout_ms)}"
    end

    %{timeout_ms: timeout_ms, timeout_ms_max: timeout_ms_max}
  end

  defp throughput_options!(opts) do
    %{
      max_log_entries_per_heartbeat:
        throughput_option(
          opts,
          :max_log_entries_per_heartbeat,
          :waraft_max_log_entries_per_heartbeat,
          1024
        ),
      max_heartbeat_size:
        throughput_option(
          opts,
          :max_heartbeat_size,
          :waraft_max_heartbeat_size,
          16 * 1024 * 1024
        ),
      apply_log_batch_size:
        throughput_option(opts, :apply_log_batch_size, :waraft_apply_log_batch_size, 1024),
      apply_batch_max_bytes:
        throughput_option(
          opts,
          :apply_batch_max_bytes,
          :waraft_apply_batch_max_bytes,
          16 * 1024 * 1024
        ),
      log_rotation_interval:
        throughput_option(
          opts,
          :log_rotation_interval,
          :waraft_log_rotation_interval,
          50_000
        ),
      log_rotation_keep:
        throughput_option(
          opts,
          :log_rotation_keep,
          :waraft_log_rotation_keep,
          100_000
        ),
      max_retained_entries:
        throughput_option(
          opts,
          :max_retained_entries,
          :waraft_max_retained_entries,
          100_000
        )
    }
  end

  defp throughput_option(opts, opt_key, app_key, default) do
    {source, value} = config_option(opts, opt_key, app_key, default)
    positive_integer_option!(source, value)
  end

  defp queue_options!(opts) do
    %{
      max_pending_low_priority_commits:
        queue_option(
          opts,
          :max_pending_low_priority_commits,
          :waraft_max_pending_low_priority_commits,
          100_000
        ),
      max_pending_high_priority_commits:
        queue_option(
          opts,
          :max_pending_high_priority_commits,
          :waraft_max_pending_high_priority_commits,
          100_000
        ),
      max_pending_reads:
        queue_option(opts, :max_pending_reads, :waraft_max_pending_reads, 100_000),
      max_pending_applies:
        queue_option(opts, :max_pending_applies, :waraft_max_pending_applies, 100_000),
      apply_queue_max_size:
        queue_option(opts, :apply_queue_max_size, :waraft_apply_queue_max_size, 100_000)
    }
  end

  defp queue_option(opts, opt_key, app_key, default) do
    {source, value} = config_option(opts, opt_key, app_key, default)
    non_negative_integer_option!(source, value)
  end

  defp commit_options!(opts) do
    {batch_interval_source, batch_interval_value} =
      config_option(
        opts,
        :commit_batch_interval_ms,
        :waraft_commit_batch_interval_ms,
        default_commit_batch_interval_ms()
      )

    %{
      commit_batch_interval_ms:
        non_negative_integer_option!(batch_interval_source, batch_interval_value),
      commit_batch_max:
        throughput_option(opts, :commit_batch_max, :waraft_commit_batch_max, 1024),
      async_log_append:
        boolean_option(
          opts,
          :async_log_append,
          :waraft_async_log_append,
          true
        )
    }
  end

  defp wal_commit_delay_floor_ms do
    delay_us =
      :ferricstore
      |> Application.get_env(:wal_commit_delay_us, 6_000)
      |> then(&non_negative_integer_option!(:wal_commit_delay_us, &1))

    if delay_us == 0, do: 0, else: 1
  end

  defp max_inflight_commit_bytes_option!(opts) do
    {source, value} =
      config_option(
        opts,
        :max_inflight_commit_bytes,
        :waraft_max_inflight_commit_bytes,
        :infinity
      )

    normalize_max_inflight_bytes(source, value)
  end

  defp config_option(opts, opt_key, app_key, default) do
    if Keyword.has_key?(opts, opt_key) do
      {opt_key, Keyword.fetch!(opts, opt_key)}
    else
      {app_key, Application.get_env(:ferricstore, app_key, default)}
    end
  end

  defp positive_integer_option!(_source, value) when is_integer(value) and value > 0, do: value

  defp positive_integer_option!(source, value) do
    raise ArgumentError,
          "#{inspect(source)} must be a positive integer, got: #{inspect(value)}"
  end

  defp non_negative_integer_option!(_source, value) when is_integer(value) and value >= 0,
    do: value

  defp non_negative_integer_option!(source, value) do
    raise ArgumentError,
          "#{inspect(source)} must be a non-negative integer, got: #{inspect(value)}"
  end

  defp boolean_option(opts, opt_key, app_key, default) do
    {source, value} = config_option(opts, opt_key, app_key, default)
    boolean_option!(source, value)
  end

  defp boolean_option!(_source, value) when is_boolean(value), do: value

  defp boolean_option!(source, value) do
    raise ArgumentError, "#{inspect(source)} must be a boolean, got: #{inspect(value)}"
  end

  defp registered_partition_count do
    case :persistent_term.get(@shard_count_key, nil) do
      shard_count when is_integer(shard_count) and shard_count > 0 ->
        shard_count

      _other ->
        64
    end
  end

  defp registered_names(shard_count) do
    [
      :wa_raft_sup.default_name(@app),
      @sup_id
      | Enum.flat_map(1..shard_count, fn partition ->
          partition_registered_names(partition)
        end)
    ]
  end

  defp partition_registered_names(partition) do
    try do
      [
        :wa_raft_part_sup.registered_name(@table, partition),
        :wa_raft_acceptor.registered_name(@table, partition),
        :wa_raft_queue.registered_name(@table, partition),
        :wa_raft_queue.default_read_queue_name(@table, partition),
        :wa_raft_log.registered_name(@table, partition),
        :wa_raft_server.registered_name(@table, partition),
        :wa_raft_storage.registered_name(@table, partition),
        :wa_raft_transport_cleanup.registered_name(@table, partition)
      ]
    rescue
      _ -> []
    end
  end

  defp wait_down(_names, 0), do: :ok

  defp wait_down(names, attempts) do
    case Enum.any?(names, &(Process.whereis(&1) != nil)) do
      false ->
        :ok

      true ->
        Process.sleep(10)
        wait_down(names, attempts - 1)
    end
  end

  defp partition(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    shard_index + 1
  end

  defp empty_segment_log_memory_status do
    %{
      ets_entries: 0,
      ets_bytes: 0,
      disk_first_index: nil,
      disk_last_index: nil,
      max_ets_bytes: 0,
      max_ets_entries: 0,
      min_ets_entries: 0
    }
  end

  defp server_name(partition) do
    :wa_raft_server.registered_name(@table, partition)
  end

  defp acquire_commit_bytes(shard_index, command) do
    case :persistent_term.get(@max_inflight_bytes_key, :infinity) do
      :infinity ->
        {:ok, 0}

      max_bytes when is_integer(max_bytes) ->
        bytes = estimated_term_bytes(command)
        ref = :persistent_term.get(@inflight_bytes_key)
        idx = partition(shard_index)

        if idx <= :atomics.info(ref).size do
          do_acquire_commit_bytes(ref, idx, bytes, max_bytes)
        else
          invalid_shard_index_error(shard_index)
        end
    end
  end

  defp do_acquire_commit_bytes(ref, idx, bytes, max_bytes) do
    current = :atomics.get(ref, idx)

    if current + bytes > max_bytes do
      emit_commit_bytes_rejected(idx - 1, bytes, current, max_bytes)
      {:error, :commit_bytes_full}
    else
      case :atomics.compare_exchange(ref, idx, current, current + bytes) do
        :ok -> {:ok, bytes}
        _actual -> do_acquire_commit_bytes(ref, idx, bytes, max_bytes)
      end
    end
  end

  defp release_commit_bytes(_shard_index, 0), do: :ok

  defp release_commit_bytes(shard_index, bytes) do
    ref = :persistent_term.get(@inflight_bytes_key, nil)

    if is_reference(ref) do
      :atomics.sub(ref, partition(shard_index), bytes)
    end

    :ok
  end

  defp normalize_max_inflight_bytes(_source, :infinity), do: :infinity
  defp normalize_max_inflight_bytes(_source, nil), do: :infinity
  defp normalize_max_inflight_bytes(_source, false), do: :infinity

  defp normalize_max_inflight_bytes(_source, value) when is_integer(value) and value >= 0,
    do: value

  defp normalize_max_inflight_bytes(source, value) do
    raise ArgumentError,
          "#{inspect(source)} must be a non-negative integer or :infinity, got: #{inspect(value)}"
  end

  defp estimated_term_bytes(term) when is_binary(term), do: byte_size(term)
  defp estimated_term_bytes(term) when is_bitstring(term), do: div(bit_size(term) + 7, 8)
  defp estimated_term_bytes(term) when is_atom(term), do: 8
  defp estimated_term_bytes(term) when is_integer(term), do: 8
  defp estimated_term_bytes(term) when is_float(term), do: 8
  defp estimated_term_bytes(pid) when is_pid(pid), do: 16
  defp estimated_term_bytes(ref) when is_reference(ref), do: 16
  defp estimated_term_bytes(fun) when is_function(fun), do: :erlang.external_size(fun)

  defp estimated_term_bytes(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.reduce(8 + tuple_size(tuple) * 4, fn item, acc -> acc + estimated_term_bytes(item) end)
  end

  defp estimated_term_bytes(map) when is_map(map) do
    Enum.reduce(map, 16 + map_size(map) * 8, fn {key, value}, acc ->
      acc + estimated_term_bytes(key) + estimated_term_bytes(value)
    end)
  end

  defp estimated_term_bytes(list) when is_list(list), do: estimated_list_bytes(list, 8)

  defp estimated_term_bytes(other), do: :erlang.external_size(other)

  defp estimated_list_bytes([], acc), do: acc

  defp estimated_list_bytes([head | tail], acc) do
    estimated_list_bytes(tail, acc + 8 + estimated_term_bytes(head))
  end

  defp estimated_list_bytes(tail, acc), do: acc + estimated_term_bytes(tail)

  defp emit_blob_prepare_failed(shard_index, command, reason) do
    :telemetry.execute(
      [:ferricstore, :waraft, :blob_prepare_failed],
      %{count: 1},
      %{
        shard_index: shard_index,
        command_shape: command_shape(command),
        reason: blob_prepare_failure_cause(reason),
        error: reason
      }
    )
  rescue
    _ -> :ok
  end

  defp blob_prepare_failure_cause({:blob_prepare_failed, cause}), do: cause
  defp blob_prepare_failure_cause(reason), do: reason

  defp emit_commit_timeout_if_needed(
         shard_index,
         command,
         {:error, :timeout},
         acquired_bytes,
         started_mono,
         path
       ) do
    emit_commit_timeout(shard_index, command_shape(command), acquired_bytes, started_mono, path)
  end

  defp emit_commit_timeout_if_needed(
         _shard_index,
         _command,
         _result,
         _acquired_bytes,
         _started_mono,
         _path
       ),
       do: :ok

  defp emit_commit_timeout(shard_index, command_shape, acquired_bytes, started_mono, path) do
    duration_us =
      System.monotonic_time()
      |> Kernel.-(started_mono)
      |> System.convert_time_unit(:native, :microsecond)

    :telemetry.execute(
      [:ferricstore, :waraft, :commit, :timeout],
      %{
        count: 1,
        duration_us: max(duration_us, 0),
        timeout_ms: @timeout,
        acquired_bytes: acquired_bytes,
        inflight_bytes: inflight_commit_bytes(shard_index)
      },
      %{
        shard_index: shard_index,
        command_shape: command_shape,
        path: path,
        reason: :timeout
      }
    )
  rescue
    _ -> :ok
  end

  defp command_shape({:put_batch, _entries}), do: :put_batch
  defp command_shape({:delete_batch, _keys}), do: :delete_batch
  defp command_shape({:batch, _commands}), do: :batch
  defp command_shape(command) when is_tuple(command), do: elem(command, 0)
  defp command_shape(_command), do: :unknown

  defp emit_commit_bytes_rejected(shard_index, bytes, current, max_bytes) do
    :telemetry.execute(
      [:ferricstore, :waraft, :commit_bytes, :rejected],
      %{count: 1, bytes: bytes, current_bytes: current, max_bytes: max_bytes},
      %{shard_index: shard_index}
    )
  rescue
    _ -> :ok
  end
end
