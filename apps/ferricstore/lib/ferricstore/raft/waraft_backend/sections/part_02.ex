defmodule Ferricstore.Raft.WARaftBackend.Sections.Part02 do
  @moduledoc false

  # Extracted from WARaftBackend: submit_write_many_entry_after_pause .. start_partitions_then_finish
  defmacro __using__(_opts) do
    quote do
alias Ferricstore.ErrorReasons
alias Ferricstore.NamespaceConfig
alias Ferricstore.Raft.BlobCommand
alias Ferricstore.Raft.CommandStamp
alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher
alias Ferricstore.Raft.WARaftBackend.BatcherSupervisor, as: NamespaceBatcherSupervisor
alias Ferricstore.Raft.WARaftBackend.SyncGate
        defp submit_write_many_entry_after_pause({shard_index, command} = entry)
             when valid_shard_index_shape(shard_index) do
          case SyncGate.enter(shard_index) do
            {:ok, token} ->
              try do
                case submit_write_many_entry(entry) do
                  {^shard_index, ^command, submission} ->
                    {shard_index, command, submission, token}
      
                  other ->
                    SyncGate.leave(token)
                    other
                end
              catch
                kind, reason ->
                  SyncGate.leave(token)
                  :erlang.raise(kind, reason, __STACKTRACE__)
              end
      
            {:error, _reason} = error ->
              {shard_index, command, {:immediate, error}}
          end
        end
      
        defp submit_write_many_entry_after_pause(entry), do: submit_write_many_entry(entry)
      
        defp submit_write_many_entry({shard_index, command}) do
          {shard_index, command, submit_commit_async(shard_index, command)}
        end
      
        defp submit_write_many_entry(entry), do: {:invalid, entry}
      
        defp await_write_many_entry({:invalid, entry}),
          do: {:error, {:invalid_write_many_entry, entry}}
      
        defp await_write_many_entry({shard_index, command, submission, sync_token}) do
          try do
            await_write_many_entry({shard_index, command, submission})
          after
            SyncGate.leave(sync_token)
          end
        end
      
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
          case cached_pre_submit_redirect_node(shard_index, redirects_left) do
            leader_node when is_atom(leader_node) and leader_node not in [nil, node()] ->
              leader_node
              |> redirect_commit(shard_index, command, redirects_left)
              |> normalize_commit_result()
      
            _other ->
              case commit(shard_index, command) do
                {:error, _reason} = error ->
                  error
                  |> maybe_redirect_commit(shard_index, command, redirects_left)
                  |> normalize_commit_result()
      
                result ->
                  result
              end
          end
        end
      
        defp cached_pre_submit_redirect_node(_shard_index, redirects_left) when redirects_left <= 0,
          do: nil
      
        defp cached_pre_submit_redirect_node(shard_index, _redirects_left) do
          case cached_voter_nodes(shard_index) do
            {:ok, voters} when length(voters) > 1 ->
              @table
              |> :wa_raft_info.get_leader(partition(shard_index))
              |> peer_node()
      
            _other ->
              nil
          end
        catch
          _kind, _reason -> nil
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
              with {:ok, prepared_command, blob_protection} <-
                     prepare_commit_command(shard_index, command) do
                acceptor = :wa_raft_acceptor.registered_name(@table, partition(shard_index))
                acceptor_pid = Process.whereis(acceptor)
                stamped = CommandStamp.to_ttb(prepared_command)
                started_mono = System.monotonic_time()
      
                result =
                  acceptor
                  |> commit_safely(acceptor_pid, {make_ref(), stamped})
                  |> normalize_commit_transport_result(acceptor_pid)
      
                :ok =
                  Ferricstore.FaultInjection.maybe_pause(:after_waraft_commit, %{
                    shard_index: shard_index,
                    result: result
                  })
      
                emit_commit_timeout_if_needed(
                  shard_index,
                  command,
                  result,
                  acquired_bytes,
                  started_mono,
                  :sync
                )
      
                release_blob_protection_after_result(blob_protection, result)
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
          voters =
            case Map.get(config, :membership, []) do
              [_ | _] = membership -> membership
              _empty_or_missing -> Map.get(config, :participants, [])
            end
      
          voters
          |> Enum.map(&peer_node/1)
          |> Enum.filter(&valid_node_name?/1)
          |> Enum.uniq()
        end
      
        defp config_voter_nodes(_config), do: []
      
        defp submit_acquired_commit_async(shard_index, command, acquired_bytes) do
          case prepare_commit_command(shard_index, command) do
            {:ok, prepared_command, blob_protection} ->
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
                       command_shape, started_mono, blob_protection}
      
                    {:error, _reason} = error ->
                      flush_reply_alias(reply_alias, reply_ref)
                      release_commit_bytes(shard_index, acquired_bytes)
                      result = normalize_commit_transport_result(error, pid)
                      release_blob_protection_after_result(blob_protection, result)
                      {:immediate, result}
                  end
      
                nil ->
                  release_commit_bytes(shard_index, acquired_bytes)
                  release_blob_protection_after_result(blob_protection, {:error, :unreachable})
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
                command_shape, started_mono, blob_protection}
             ) do
          result =
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
      
          :ok =
            Ferricstore.FaultInjection.maybe_pause(:after_waraft_commit, %{
              shard_index: shard_index,
              result: result
            })
      
          release_blob_protection_after_result(blob_protection, result)
          result
        end
      
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
                    BlobCommand.prepare_protected(ctx, shard_index, command,
                      single_member?: single_member_waraft_group?(shard_index)
                    )
                  else
                    {:ok, command, nil}
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
      
        defp release_blob_protection_after_result(nil, _result), do: :ok
      
        defp release_blob_protection_after_result(blob_protection, result) do
          if keep_blob_protection_after_result?(result) do
            Ferricstore.Store.BlobStore.harden_protection(blob_protection, result: result)
          else
            Ferricstore.Store.BlobStore.unprotect(blob_protection)
          end
      
          :ok
        end
      
        defp keep_blob_protection_after_result?({:error, :timeout}), do: true
        defp keep_blob_protection_after_result?({:error, :commit_unreachable_after_submit}), do: true
        defp keep_blob_protection_after_result?({:error, :not_leader_after_submit}), do: true
      
        defp keep_blob_protection_after_result?({:error, {:commit_call_failed_after_submit, _reason}}),
          do: true
      
        defp keep_blob_protection_after_result?(_result), do: false
      
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
      
        defp maybe_redirect_commit(error, shard_index, command, redirects_left) do
          if redirectable_write_error?(error) do
            maybe_redirect_write_to_leader(error, shard_index, command, redirects_left)
          else
            error
          end
        end
      
        defp redirectable_write_error?({:error, :not_leader}), do: true
        defp redirectable_write_error?({:error, {:notify_redirect, _peer}}), do: true
        defp redirectable_write_error?(_error), do: false
      
        defp maybe_redirect_write_to_leader(
               {:error, reason} = error,
               shard_index,
               command,
               redirects_left
             )
             when reason == :not_leader do
          case local_leader_node(shard_index) do
            node_name when is_atom(node_name) and not is_nil(node_name) and node_name != node() ->
              redirect_commit(node_name, shard_index, command, redirects_left)
      
            _other ->
              error
          end
        end
      
        defp maybe_redirect_write_to_leader(
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
      
        defp redirect_commit(node_name, shard_index, command, redirects_left) do
          try do
            result =
              :erpc.call(
                node_name,
                __MODULE__,
                :write_redirected,
                [shard_index, command, redirects_left - 1],
                @timeout
              )
      
            barrier_redirected_commit(node_name, shard_index, result)
          catch
            kind, reason -> redirect_write_failure(kind, reason)
          end
        end
      
        defp barrier_redirected_commit(_node_name, _shard_index, {:error, _reason} = error), do: error
      
        defp barrier_redirected_commit(node_name, shard_index, result) do
          case remote_storage_position(node_name, shard_index) do
            {:ok, {:raft_log_pos, applied_index, _term}}
            when is_integer(applied_index) and applied_index > 0 ->
              case await_local_storage_applied(shard_index, applied_index, @timeout) do
                :ok -> result
                {:error, :timeout} -> ErrorReasons.write_timeout_unknown()
              end
      
            _other ->
              result
          end
        end
      
        defp remote_storage_position(node_name, shard_index) do
          :erpc.call(node_name, __MODULE__, :storage_position, [shard_index], 5_000)
        catch
          _kind, _reason -> {:error, :leader_unavailable}
        end
      
        defp await_local_storage_applied(_shard_index, target_index, _timeout_ms)
             when not is_integer(target_index) or target_index <= 0,
             do: :ok
      
        defp await_local_storage_applied(shard_index, target_index, timeout_ms) do
          deadline = System.monotonic_time(:millisecond) + max(timeout_ms, 0)
          do_await_local_storage_applied(shard_index, target_index, deadline)
        end
      
        defp do_await_local_storage_applied(shard_index, target_index, deadline) do
          case storage_position(shard_index) do
            {:ok, {:raft_log_pos, applied_index, _term}}
            when is_integer(applied_index) and applied_index >= target_index ->
              :ok
      
            _other ->
              if System.monotonic_time(:millisecond) >= deadline do
                {:error, :timeout}
              else
                Process.sleep(1)
                do_await_local_storage_applied(shard_index, target_index, deadline)
              end
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
          case profile_startup_phase(:finish_start, %{shard_count: shard_count}, fn ->
                 finish_start(shard_count, opts)
               end) do
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
      
        defp start_partitions_then_finish(specs, shard_count, opts) do
          case profile_startup_phase(:start_partitions, %{shard_count: shard_count}, fn ->
                 start_partitions(specs)
               end) do
            :ok -> finish_started(shard_count, opts)
            {:error, _reason} = error -> cleanup_failed_start(error)
          end
        end
    end
  end
end
