defmodule Ferricstore.Raft.WARaftBackend.Sections.Part04 do
  @moduledoc false

  # Extracted from WARaftBackend: cluster_config .. blob_prepare_failure_cause
  defmacro __using__(_opts) do
    quote do
alias Ferricstore.ErrorReasons
alias Ferricstore.NamespaceConfig
alias Ferricstore.Raft.BlobCommand
alias Ferricstore.Raft.CommandStamp
alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher
alias Ferricstore.Raft.WARaftBackend.BatcherSupervisor, as: NamespaceBatcherSupervisor
alias Ferricstore.Raft.WARaftBackend.SyncGate
        defp cluster_config?(config) do
          config
          |> config_voter_nodes()
          |> length()
          |> Kernel.>(1)
        end
      
        defp wait_known_leader(shard_index, server, attempts),
          do: wait_known_leader(shard_index, server, attempts, nil)
      
        defp wait_known_leader(_shard_index, _server, 0, status),
          do: {:error, {:leader_timeout, status}}
      
        defp wait_known_leader(shard_index, server, attempts, _status) do
          case wait_status(server, 1) do
            {:ok, status} ->
              if known_leader?(shard_index, status) do
                :ok
              else
                Process.sleep(10)
                wait_known_leader(shard_index, server, attempts - 1, status)
              end
      
            {:error, reason} ->
              Process.sleep(10)
              wait_known_leader(shard_index, server, attempts - 1, {:status_error, reason})
          end
        end
      
        defp known_leader?(shard_index, status) when is_list(status) do
          leader_node = peer_node(Keyword.get(status, :leader_id))
      
          if valid_node_name?(leader_node) do
            true
          else
            leader_node = :wa_raft_info.get_leader(@table, partition(shard_index))
            valid_node_name?(leader_node)
          end
        end
      
        defp known_leader?(_shard_index, _status), do: false
      
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
      
            {:error, reason} ->
              Process.sleep(10)
              wait_leader(server, attempts - 1, {:status_error, reason})
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
      
            {:error, reason} ->
              Process.sleep(10)
              wait_log_replayed(server, target_index, attempts - 1, {:status_error, reason})
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
            case NamespaceBatcherSupervisor.ensure_started(shard_index, opts) do
              :ok ->
                {:cont, :ok}
      
              {:error, :supervisor_not_started} ->
                case NamespaceBatcher.start_link(shard_index, opts) do
                  {:ok, _pid} -> {:cont, :ok}
                  {:error, {:already_started, _pid}} -> {:cont, :ok}
                  {:error, _reason} = error -> {:halt, error}
                end
      
              {:error, _reason} = error ->
                {:halt, error}
            end
          end)
        end
      
        defp start_namespace_batchers(_shard_count, _opts), do: :ok
      
        defp stop_namespace_batchers(shard_count) when is_integer(shard_count) and shard_count > 0 do
          NamespaceBatcherSupervisor.stop_all(shard_count)
      
          Enum.each(0..(shard_count - 1), fn shard_index ->
            if Process.whereis(NamespaceBatcher.name(shard_index)) do
              NamespaceBatcher.stop(shard_index)
            end
          end)
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
          SyncGate.init_shards(ctx.shard_count)
      
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
              throughput_option(opts, :apply_log_batch_size, :waraft_apply_log_batch_size, 4096),
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
              throughput_option(
                opts,
                :commit_batch_max,
                :waraft_commit_batch_max,
                default_commit_batch_max()
              ),
            async_log_append: true
          }
        end
      
        defp wal_commit_delay_floor_ms do
          delay_us =
            :ferricstore
            |> Application.get_env(:wal_commit_delay_us, 6_000)
            |> then(&non_negative_integer_option!(:wal_commit_delay_us, &1))
      
          if delay_us == 0, do: 0, else: div(delay_us + 999, 1_000)
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
      
        defp registered_partition_count do
          case :persistent_term.get(@shard_count_key, nil) do
            shard_count when is_integer(shard_count) and shard_count > 0 ->
              shard_count
      
            _other ->
              64
          end
        end
      
        defp erase_waraft_option_cache(shard_count) when is_integer(shard_count) and shard_count > 0 do
          # WAraft stores normalized partition options in persistent_term. They
          # include absolute database paths, so embedded/test restarts with a new
          # data_dir must clear them after the old partition supervisors are down.
          :persistent_term.erase({:wa_raft_sup, @app})
      
          Enum.each(1..shard_count, fn partition ->
            :persistent_term.erase({:wa_raft_part_sup, @table, partition})
          end)
      
          :ok
        end
      
        defp erase_waraft_option_cache(_shard_count), do: :ok
      
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
    end
  end
end
