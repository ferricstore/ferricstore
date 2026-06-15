defmodule Ferricstore.Raft.WARaftBackend.Sections.Startup do
  @moduledoc false

  # Extracted from WARaftBackend: start_partitions .. wait_initial_leader
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.ErrorReasons
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.CommandStamp
      alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher
      alias Ferricstore.Raft.WARaftBackend.BatcherSupervisor, as: NamespaceBatcherSupervisor
      alias Ferricstore.Raft.WARaftBackend.SyncGate

      defp start_partitions(specs) when is_list(specs) do
        supervisor = :wa_raft_sup.default_name(@app)
        max_concurrency = startup_partition_concurrency(length(specs))

        specs
        |> Task.async_stream(
          fn spec ->
            shard_index = Map.get(spec, :partition, 1) - 1

            try do
              profile_startup_phase(:start_partition, %{shard_index: shard_index}, fn ->
                case :wa_raft_sup.start_partition(supervisor, spec) do
                  {:error, {:already_started, _pid}} -> :ok
                  {:error, :already_present} -> :ok
                  {:error, reason} -> {:error, reason}
                  _other -> :ok
                end
              end)
            catch
              kind, reason -> {:error, {kind, reason}}
            end
          end,
          max_concurrency: max_concurrency,
          ordered: false,
          timeout: :infinity
        )
        |> Enum.reduce_while(:ok, fn
          {:ok, :ok}, :ok -> {:cont, :ok}
          {:ok, {:error, reason}}, :ok -> {:halt, {:error, reason}}
          {:exit, reason}, :ok -> {:halt, {:error, {:partition_start_exit, reason}}}
        end)
      end

      defp startup_partition_concurrency(count) when is_integer(count) and count > 0 do
        default = min(count, max(4, System.schedulers_online() * 2))

        :ferricstore
        |> Application.get_env(:waraft_start_partition_max_concurrency, default)
        |> then(&positive_integer_option!(:waraft_start_partition_max_concurrency, &1))
        |> min(count)
      end

      defp startup_partition_concurrency(_count), do: 1

      defp startup_wait_attempts do
        :ferricstore
        |> Application.get_env(:waraft_start_wait_timeout_ms, 300_000)
        |> then(&positive_integer_option!(:waraft_start_wait_timeout_ms, &1))
        |> Kernel.+(9)
        |> div(10)
        |> max(1)
      end

      defp finish_start(shard_count, opts) do
        with :ok <-
               finish_start_partitions(shard_count, opts) do
          profile_startup_phase(:start_namespace_batchers, %{shard_count: shard_count}, fn ->
            start_namespace_batchers(shard_count, opts)
          end)
        end
      end

      defp finish_start_partitions(shard_count, opts) when shard_count > 0 do
        max_concurrency = startup_partition_concurrency(shard_count)

        0..(shard_count - 1)
        |> Task.async_stream(
          fn shard_index ->
            profile_startup_phase(:finish_start_partition, %{shard_index: shard_index}, fn ->
              finish_start_partition(shard_index, opts)
            end)
          end,
          max_concurrency: max_concurrency,
          ordered: false,
          timeout: :infinity
        )
        |> Enum.reduce_while(:ok, fn
          {:ok, :ok}, :ok -> {:cont, :ok}
          {:ok, {:error, _reason} = error}, :ok -> {:halt, error}
          {:exit, reason}, :ok -> {:halt, {:error, {:finish_start_exit, reason}}}
        end)
      end

      defp finish_start_partitions(_shard_count, _opts), do: :ok

      defp finish_start_partition(shard_index, opts) do
        server = :wa_raft_server.registered_name(@table, partition(shard_index))
        bootstrap? = Keyword.get(opts, :bootstrap, true)
        replay_target = segment_log_last_index(shard_index)
        wait_attempts = startup_wait_attempts()

        with {:ok, status} <- wait_status(server, wait_attempts),
             :ok <- finish_start_status(shard_index, server, status, bootstrap?, wait_attempts),
             :ok <- wait_log_replayed(server, replay_target, wait_attempts),
             :ok <- wait_storage_replayed(shard_index, replay_target, wait_attempts) do
          maybe_cache_current_config(shard_index)
        end
      end

      defp finish_start_status(_shard_index, _server, _status, false, _wait_attempts), do: :ok

      defp finish_start_status(shard_index, server, status, true, wait_attempts) do
        case Keyword.get(status, :state) do
          :stalled ->
            with {:ok, config} <- bootstrap(shard_index, server) do
              wait_initial_leader(shard_index, server, config, wait_attempts)
            end

          :leader ->
            :ok

          _other ->
            config = Keyword.get(status, :config, %{})

            if cluster_config?(config) do
              wait_known_leader(shard_index, server, wait_attempts)
            else
              case backend_call(fn -> :wa_raft_server.promote(server, :next, true) end) do
                :ok -> wait_leader(server, wait_attempts)
                {:error, _reason} = error -> error
                other -> {:error, other}
              end
            end
        end
      end

      defp bootstrap(shard_index, server) do
        config = initial_bootstrap_config(shard_index, server)

        case backend_call(fn ->
               :wa_raft_server.bootstrap(server, {:raft_log_pos, 1, 1}, config, %{})
             end) do
          :ok ->
            :ok = cache_config(shard_index, config)
            {:ok, config}

          {:error, :already_bootstrapped} ->
            handle_already_bootstrapped_initial_partition(shard_index, config)

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp initial_bootstrap_config(shard_index, server) do
        nodes = initial_bootstrap_nodes()

        nodes
        |> Enum.map(fn node_name -> {:raft_identity, server, node_name} end)
        |> :wa_raft_server.make_config()
        |> tap(fn config ->
          maybe_emit_cluster_bootstrap_config_mismatch(shard_index, config)
        end)
      end

      defp initial_bootstrap_nodes do
        cluster_nodes =
          :ferricstore
          |> Application.get_env(:cluster_nodes, [])
          |> normalize_initial_cluster_nodes()

        if node() in cluster_nodes do
          cluster_nodes
        else
          [node()]
        end
      end

      defp normalize_initial_cluster_nodes(nodes) when is_list(nodes) do
        nodes
        |> Enum.filter(&valid_node_name?/1)
        |> Enum.uniq()
      end

      defp normalize_initial_cluster_nodes(_nodes), do: []

      defp handle_already_bootstrapped_initial_partition(shard_index, requested_config) do
        requested_nodes = config_voter_nodes(requested_config)

        case requested_nodes do
          [local_node] when local_node == node() ->
            config = current_partition_config(shard_index)
            effective_config = if map_size(config) > 0, do: config, else: requested_config
            :ok = cache_config(shard_index, effective_config)
            {:ok, effective_config}

          _cluster_nodes ->
            with :ok <-
                   handle_already_bootstrapped_cluster_partition(shard_index, requested_nodes) do
              {:ok, current_partition_config(shard_index)}
            end
        end
      end

      defp maybe_emit_cluster_bootstrap_config_mismatch(shard_index, config) do
        requested_nodes = config_voter_nodes(config)

        if length(requested_nodes) > 1 and node() not in requested_nodes do
          :telemetry.execute(
            [:ferricstore, :waraft, :bootstrap, :invalid_cluster_nodes],
            %{count: 1},
            %{shard_index: shard_index, node: node(), requested_nodes: requested_nodes}
          )
        end
      rescue
        _ -> :ok
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

      defp invalid_shard_index_error(shard_index),
        do: {:error, {:invalid_shard_index, shard_index}}

      defp invalid_membership_action_error(action),
        do: {:error, {:invalid_membership_action, action}}

      defp invalid_redirects_left_error(redirects_left),
        do: {:error, {:invalid_redirects_left, redirects_left}}

      defp invalid_key_error(key), do: {:error, {:invalid_key, key}}

      defp backend_unavailable_error, do: {:error, :backend_unavailable}

      defp with_sync_write(shard_index, fun) when is_function(fun, 0) do
        enter_result =
          Ferricstore.LatencyTrace.span("server_waraft_sync_gate_us", fn ->
            SyncGate.enter(shard_index)
          end)

        case enter_result do
          {:ok, token} ->
            try do
              fun.()
            after
              SyncGate.leave(token)
            end

          {:error, _reason} = error ->
            error
        end
      end

      @doc false
      @spec blob_protection_barrier(non_neg_integer()) :: :ok | {:error, term()}
      @spec blob_protection_barrier(non_neg_integer(), timeout()) :: :ok | {:error, term()}
      def blob_protection_barrier(shard_index, _timeout \\ 30_000)
          when valid_shard_index_shape(shard_index) do
        case context(@table) do
          {:ok, _ctx} ->
            case commit_unstamped_control(shard_index, :noop) do
              :ok -> :ok
              {:error, :backend_unavailable} -> :ok
              {:error, reason} -> {:error, {:blob_protection_barrier_failed, reason}}
              _other -> :ok
            end

          {:error, :backend_unavailable} ->
            :ok

          {:error, reason} ->
            {:error, {:blob_protection_barrier_failed, reason}}
        end
      end

      defp sync_pause_barrier(shard_index) do
        case context(@table) do
          {:ok, _ctx} ->
            case commit_unstamped_control(shard_index, :noop) do
              :ok -> :ok
              {:error, :backend_unavailable} -> :ok
              {:error, reason} -> {:error, {:sync_pause_barrier_failed, reason}}
              _other -> :ok
            end

          {:error, :backend_unavailable} ->
            :ok

          {:error, reason} ->
            {:error, {:sync_pause_barrier_failed, reason}}
        end
      end

      defp commit_unstamped_control(shard_index, command) do
        with :ok <- ensure_local_leader_connected_quorum(shard_index),
             {:ok, acquired_bytes} <- acquire_commit_bytes(shard_index, command) do
          try do
            acceptor = :wa_raft_acceptor.registered_name(@table, partition(shard_index))
            acceptor_pid = Process.whereis(acceptor)

            acceptor
            |> commit_safely(acceptor_pid, {make_ref(), command})
            |> normalize_commit_transport_result(acceptor_pid)
            |> normalize_commit_result()
          after
            release_commit_bytes(shard_index, acquired_bytes)
          end
        else
          {:error, _reason} = error -> error
        end
      end

      defp emit_sync_pause_failed(shard_index, reason) do
        :telemetry.execute(
          [:ferricstore, :waraft, :sync_pause, :failed],
          %{count: 1},
          %{shard_index: shard_index, reason: reason}
        )
      rescue
        _ -> :ok
      end

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
           when is_atom(node_name) and node_name not in [nil, true, false, :undefined],
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

        backend_call(fn ->
          :wa_raft_server.adjust_config(server, {action, {server, node_name}})
        end)
      end

      defp commit_config_or_redirect(shard_index, action, node_name, timeout_ms, redirects_left) do
        case commit_config(shard_index, action, node_name, timeout_ms) do
          {:error, _reason} = error ->
            maybe_redirect_config(
              error,
              shard_index,
              action,
              node_name,
              timeout_ms,
              redirects_left
            )

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
            redirect_config(
              leader_node,
              shard_index,
              action,
              node_name,
              timeout_ms,
              redirects_left
            )

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
            redirect_config(
              leader_node,
              shard_index,
              action,
              node_name,
              timeout_ms,
              redirects_left
            )

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

      defp redirect_config(
             leader_node,
             shard_index,
             action,
             node_name,
             timeout_ms,
             redirects_left
           ) do
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

      defp redirect_add_participant(
             leader_node,
             shard_index,
             node_name,
             timeout_ms,
             redirects_left
           ) do
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

          {:error, reason} = error when is_write_timeout_unknown_reason(reason) ->
            case wait_storage_participant(shard_index, node_name, timeout_ms) do
              :ok -> :ok
              _other -> error
            end

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

          {:error, reason} = error when is_write_timeout_unknown_reason(reason) ->
            case current_member_position(shard_index, node_name, timeout_ms) do
              {:ok, _position} = ok -> ok
              _other -> error
            end

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

      defp wait_initial_leader(shard_index, server, config, attempts) do
        if cluster_config?(config) do
          wait_known_leader(shard_index, server, attempts)
        else
          wait_leader(server, attempts)
        end
      end
    end
  end
end
