defmodule FerricstoreServer.Connection.Pipeline do
  @moduledoc "Pipeline dispatcher with batch fast paths for GET, SET, mixed GET+SET, and Flow workloads."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.Flow, as: FlowCommand
  alias Ferricstore.Stats
  alias Ferricstore.Store.PipelinePlanner
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Sendfile, as: ConnSendfile
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.TcpOpts
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  @stateful_cmds MapSet.new(~w(
    HELLO CLIENT QUIT AUTH ACL RESET SANDBOX
    MULTI EXEC DISCARD WATCH UNWATCH
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
    BLPOP BRPOP BLMOVE BLMPOP
  ))

  @prefetch_read_only_keyed_cmds MapSet.new(~w(
    EXISTS STRLEN TTL PTTL TYPE
    HGET HMGET HGETALL HEXISTS HLEN HKEYS HVALS
    LRANGE LINDEX LLEN
    SCARD SISMEMBER SMEMBERS SMISMEMBER
    ZCARD ZSCORE ZMSCORE ZRANGE ZREVRANGE
    JSON.GET JSON.MGET
    BF.EXISTS BF.MEXISTS CF.EXISTS CMS.QUERY TOPK.QUERY TDIGEST.INFO
  ))

  @prefetch_read_only_keyless_cmds MapSet.new(~w(
    PING ECHO DBSIZE INFO COMMAND LOLWUT LASTSAVE
    CLUSTER.HEALTH CLUSTER.STATS CLUSTER.SLOTS CLUSTER.STATUS CLUSTER.ROLE
    FERRICSTORE.METRICS MEMORY
  ))

  # Maximum commands in a single pipeline batch (100K).
  @max_pipeline_size 100_000
  @max_key_size 65_535
  @max_value_size 512 * 1024 * 1024
  @max_setrange_offset 536_870_911
  @max_bit_offset 4_294_967_295

  @doc """
  Returns the maximum pipeline batch size.
  """
  @spec max_pipeline_size() :: pos_integer()
  def max_pipeline_size, do: @max_pipeline_size

  # ---------------------------------------------------------------------------
  # Pipeline dispatch entry point
  # ---------------------------------------------------------------------------

  @doc """
  Dispatches a pipeline of commands with tiered fast paths.

  Fast paths (all skip per-command overhead, batch response into one TCP write):
  1. All GETs → direct ETS batch lookup
  2. All SETs → batch Raft/ETS insert
  3. Mixed GET+SET → split, batch each, reassemble
  4. Other pure commands → batch Dispatcher.dispatch_ast with per-command ACL
  5. Stateful (MULTI/AUTH/etc) → sequential through handle_command_fn
  """
  @spec pipeline_dispatch(
          commands :: [term()],
          state :: struct(),
          handle_command_fn :: (term(), struct() -> {atom(), iodata(), struct()}),
          send_response_fn :: (term(), term(), iodata() -> :ok)
        ) :: {:quit, struct()} | {:continue, struct()}
  def pipeline_dispatch([single_cmd], state, handle_command_fn, send_response_fn) do
    case handle_command_fn.(single_cmd, state) do
      {:quit, response, quit_state} ->
        send_response_fn.(state.socket, state.transport, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_response_fn.(state.socket, state.transport, response)
        {:continue, new_state}
    end
  end

  def pipeline_dispatch(commands, state, handle_command_fn, send_response_fn) do
    case try_batch_get_fast_path(commands, state, send_response_fn) do
      {:ok, result} ->
        result

      :fallback ->
        case try_batch_set_fast_path(commands, state, send_response_fn) do
          {:ok, result} ->
            result

          :fallback ->
            case try_batch_flow_write_fast_path(commands, state, send_response_fn) do
              {:ok, result} ->
                result

              :fallback ->
                try_mixed_fast_path(commands, state, handle_command_fn, send_response_fn)
            end
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Batch GET fast path
  # ---------------------------------------------------------------------------

  defp try_batch_get_fast_path(commands, state, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing do
      :fallback
    else
      case extract_plain_gets(commands) do
        {:ok, keys} ->
          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          if not full_acl_fast_path?(state.acl_cache) do
            :fallback
          else
            Stats.incr_commands_by(state.stats_counter, length(keys))

            result =
              case state.sandbox_namespace do
                nil ->
                  dispatch_batch_get_results(keys, keys, state, send_response_fn)

                namespace ->
                  planned_keys = PipelinePlanner.plan_keys(state.instance_ctx, keys, namespace)
                  dispatch_planned_batch_get_results(planned_keys, keys, state, send_response_fn)
              end

            {:ok, result}
          end

        :fallback ->
          :fallback
      end
    end
  end

  defp extract_plain_gets(commands), do: extract_plain_gets(commands, [])

  defp extract_plain_gets([], acc), do: {:ok, Enum.reverse(acc)}

  defp extract_plain_gets([{:command, "GET", [key], {:get, key}, [key]} | rest], acc)
       when is_binary(key),
       do: extract_plain_gets(rest, [key | acc])

  defp extract_plain_gets(_, _acc), do: :fallback

  defp dispatch_batch_get_results(
         keys,
         lookup_keys,
         %{transport: transport} = state,
         send_response_fn
       )
       when transport in [:ranch_tcp, :ranch_ssl] do
    case safe_dispatch(fn ->
           Router.batch_get_with_deferred_blob_file_refs_and_presence(
             state.instance_ctx,
             lookup_keys,
             ConnSendfile.threshold_bytes()
           )
         end) do
      {:ok, {results, has_file_ref?}} ->
        if has_file_ref? do
          stream_get_results(keys, lookup_keys, results, state, send_response_fn)
        else
          send_response_fn.(
            state.socket,
            state.transport,
            Encoder.encode_bulk_strings_or_nulls(results)
          )

          {:continue, ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)}
        end

      {:error, err} ->
        send_response_fn.(
          state.socket,
          state.transport,
          List.duplicate(Encoder.encode(err), length(keys))
        )

        {:continue, state}
    end
  end

  defp dispatch_batch_get_results(keys, lookup_keys, state, send_response_fn) do
    case safe_dispatch(fn -> Router.batch_get(state.instance_ctx, lookup_keys) end) do
      {:ok, values} ->
        send_response_fn.(
          state.socket,
          state.transport,
          Encoder.encode_bulk_strings_or_nulls(values)
        )

        {:continue, ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)}

      {:error, err} ->
        send_response_fn.(
          state.socket,
          state.transport,
          List.duplicate(Encoder.encode(err), length(lookup_keys))
        )

        {:continue, state}
    end
  end

  defp dispatch_planned_batch_get_results(
         planned_keys,
         keys,
         %{transport: transport} = state,
         send_response_fn
       )
       when transport in [:ranch_tcp, :ranch_ssl] do
    case safe_dispatch(fn ->
           Router.batch_get_with_deferred_blob_file_refs_planned_and_presence(
             state.instance_ctx,
             planned_keys,
             ConnSendfile.threshold_bytes()
           )
         end) do
      {:ok, {results, has_file_ref?}} ->
        if has_file_ref? do
          stream_get_results(planned_keys, results, state, send_response_fn)
        else
          send_response_fn.(
            state.socket,
            state.transport,
            Encoder.encode_bulk_strings_or_nulls(results)
          )

          {:continue, ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)}
        end

      {:error, err} ->
        send_response_fn.(
          state.socket,
          state.transport,
          List.duplicate(Encoder.encode(err), length(planned_keys))
        )

        {:continue, state}
    end
  end

  defp dispatch_planned_batch_get_results(planned_keys, keys, state, send_response_fn) do
    case safe_dispatch(fn -> Router.batch_get_planned(state.instance_ctx, planned_keys) end) do
      {:ok, values} ->
        send_response_fn.(
          state.socket,
          state.transport,
          Encoder.encode_bulk_strings_or_nulls(values)
        )

        {:continue, ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)}

      {:error, err} ->
        send_response_fn.(
          state.socket,
          state.transport,
          List.duplicate(Encoder.encode(err), length(planned_keys))
        )

        {:continue, state}
    end
  end

  defp stream_get_results(keys, lookup_keys, results, state, send_response_fn) do
    TcpOpts.set_cork(state.socket, true)

    {result, file_cache} =
      keys
      |> Enum.zip(lookup_keys)
      |> Enum.zip(results)
      |> Enum.reduce_while({{:continue, state}, ConnSendfile.new_file_cache()}, fn
        {{key, lookup_key}, {:file_ref, path, offset, size}},
        {{:continue, acc_state}, file_cache} ->
          case ConnSendfile.send_file_ref_response_cached(
                 key,
                 lookup_key,
                 path,
                 offset,
                 size,
                 acc_state,
                 file_cache
               ) do
            {:sent, new_state, new_cache} ->
              {:cont, {{:continue, new_state}, new_cache}}

            {:fallback, new_cache} ->
              value =
                Router.get(
                  acc_state.instance_ctx,
                  namespace_key(acc_state.sandbox_namespace, key)
                )

              send_response_fn.(
                acc_state.socket,
                acc_state.transport,
                Encoder.encode(value)
              )

              tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
              {:cont, {{:continue, tracked_state}, new_cache}}

            {:error_after_header, _reason, new_cache} ->
              {:halt, {{:quit, acc_state}, new_cache}}
          end

        {{key, _lookup_key}, value}, {{:continue, acc_state}, file_cache} ->
          send_response_fn.(acc_state.socket, acc_state.transport, Encoder.encode(value))

          tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
          {:cont, {{:continue, tracked_state}, file_cache}}
      end)

    ConnSendfile.close_file_cache(file_cache)

    TcpOpts.set_cork(state.socket, false)
    result
  end

  defp stream_get_results(planned_keys, results, state, send_response_fn) do
    TcpOpts.set_cork(state.socket, true)

    {result, file_cache} =
      planned_keys
      |> Enum.zip(results)
      |> Enum.reduce_while({{:continue, state}, ConnSendfile.new_file_cache()}, fn
        {{key, lookup_key, _shard_index, _keydir}, {:file_ref, path, offset, size}},
        {{:continue, acc_state}, file_cache} ->
          case ConnSendfile.send_file_ref_response_cached(
                 key,
                 lookup_key,
                 path,
                 offset,
                 size,
                 acc_state,
                 file_cache
               ) do
            {:sent, new_state, new_cache} ->
              {:cont, {{:continue, new_state}, new_cache}}

            {:fallback, new_cache} ->
              value = Router.get(acc_state.instance_ctx, lookup_key)

              send_response_fn.(
                acc_state.socket,
                acc_state.transport,
                Encoder.encode(value)
              )

              tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
              {:cont, {{:continue, tracked_state}, new_cache}}

            {:error_after_header, _reason, new_cache} ->
              {:halt, {{:quit, acc_state}, new_cache}}
          end

        {{key, _lookup_key, _shard_index, _keydir}, value},
        {{:continue, acc_state}, file_cache} ->
          send_response_fn.(acc_state.socket, acc_state.transport, Encoder.encode(value))

          tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
          {:cont, {{:continue, tracked_state}, file_cache}}
      end)

    ConnSendfile.close_file_cache(file_cache)

    TcpOpts.set_cork(state.socket, false)
    result
  end

  # ---------------------------------------------------------------------------
  # Batch SET fast path
  # ---------------------------------------------------------------------------
  # When ALL commands in a pipeline batch are plain `SET key value` (no options),
  # bypass the normal sliding_window_dispatch which serializes each SET.
  #
  # Submit all SETs to Batcher(s) concurrently, wait for Ra commits, then send
  # all responses. This lets all SETs share the same Batcher batch + WAL
  # fdatasync instead of serializing through it.

  defp try_batch_set_fast_path(commands, state, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing do
      :fallback
    else
      case extract_plain_sets(commands) do
        {:ok, kv_pairs} ->
          ctx = state.instance_ctx
          write_pairs = namespace_kv_pairs(state.sandbox_namespace, kv_pairs)

          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          if not full_acl_fast_path?(state.acl_cache) do
            :fallback
          else
            pressure_ok =
              :atomics.get(ctx.pressure_flags, 1) == 0 and
                :atomics.get(ctx.pressure_flags, 2) == 0

            # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
            if not pressure_ok do
              :fallback
            else
              {:ok, do_batch_set_quorum(write_pairs, state, send_response_fn)}
            end
          end

        :fallback ->
          :fallback
      end
    end
  end

  defp extract_plain_sets(commands) do
    extract_plain_sets(commands, [])
  end

  defp extract_plain_sets([], acc), do: {:ok, Enum.reverse(acc)}

  defp extract_plain_sets(
         [{:command, "SET", [key, value], {:set, key, value}, [key]} | rest],
         acc
       )
       when is_binary(key) and is_binary(value) and
              byte_size(key) <= @max_key_size and byte_size(value) < @max_value_size,
       do: extract_plain_sets(rest, [{key, value} | acc])

  defp extract_plain_sets(_, _acc), do: :fallback

  defp do_batch_set_quorum(kv_pairs, state, send_response_fn) do
    Stats.incr_commands_by(state.stats_counter, length(kv_pairs))

    response =
      case safe_dispatch(fn -> Router.batch_quorum_put(state.instance_ctx, kv_pairs) end) do
        {:ok, results} ->
          kv_pairs
          |> Enum.zip(results)
          |> Enum.map(fn
            {{key, value}, :ok} ->
              notify_pipeline_set_success(key, value, state)
              Encoder.ok_response()

            {_kv_pair, {:error, _} = err} ->
              Encoder.encode(err)
          end)

        {:error, err} ->
          List.duplicate(Encoder.encode(err), length(kv_pairs))
      end

    send_response_fn.(state.socket, state.transport, response)
    {:continue, state}
  end

  # ---------------------------------------------------------------------------
  # Mixed GET+SET fast path
  # ---------------------------------------------------------------------------
  # When a pipeline contains only plain GETs and plain SETs (the 80/20 case),
  # split into two groups, batch each, then reassemble responses in order.

  defp try_mixed_fast_path(commands, state, handle_command_fn, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing do
      general_batch_dispatch(commands, state, handle_command_fn, send_response_fn)
    else
      # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
      if not full_acl_fast_path?(state.acl_cache) do
        general_batch_dispatch(commands, state, handle_command_fn, send_response_fn)
      else
        case classify_mixed_pipeline(commands) do
          {:ok, ops} ->
            do_mixed_fast_path(ops, state, send_response_fn)

          :fallback ->
            general_batch_dispatch(commands, state, handle_command_fn, send_response_fn)
        end
      end
    end
  end

  defp classify_mixed_pipeline(commands),
    do: classify_mixed_pipeline(commands, [], MapSet.new(), MapSet.new())

  defp classify_mixed_pipeline([], acc, _read_keys, _written_keys),
    do: {:ok, Enum.reverse(acc)}

  defp classify_mixed_pipeline(
         [{:command, "GET", [key], {:get, key}, [key]} | rest],
         acc,
         read_keys,
         written_keys
       )
       when is_binary(key) do
    if MapSet.member?(written_keys, key) do
      :fallback
    else
      classify_mixed_pipeline(
        rest,
        [{:get, key} | acc],
        MapSet.put(read_keys, key),
        written_keys
      )
    end
  end

  defp classify_mixed_pipeline(
         [{:command, "SET", [key, value], {:set, key, value}, [key]} | rest],
         acc,
         read_keys,
         written_keys
       )
       when is_binary(key) and is_binary(value) and byte_size(key) <= @max_key_size and
              byte_size(value) < @max_value_size do
    if MapSet.member?(read_keys, key) do
      :fallback
    else
      classify_mixed_pipeline(
        rest,
        [{:set, key, value} | acc],
        read_keys,
        MapSet.put(written_keys, key)
      )
    end
  end

  defp classify_mixed_pipeline(_, _, _, _), do: :fallback

  defp do_mixed_fast_path(ops, state, send_response_fn) do
    ctx = state.instance_ctx
    count = length(ops)
    Stats.incr_commands_by(state.stats_counter, count)

    get_keys = for {:get, key} <- ops, do: key
    lookup_keys = namespace_keys(state.sandbox_namespace, get_keys)
    set_pairs = for {:set, key, value} <- ops, do: {key, value}
    write_pairs = namespace_kv_pairs(state.sandbox_namespace, set_pairs)

    # Reads and writes are disjoint here, so SETs can be grouped without
    # changing any GET result in the same pipeline.
    set_slots =
      case write_pairs do
        [] ->
          []

        _ ->
          pressure_ok =
            :atomics.get(ctx.pressure_flags, 1) == 0 and
              :atomics.get(ctx.pressure_flags, 2) == 0

          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          results =
            if not pressure_ok do
              List.duplicate({:error, "ERR server under pressure"}, length(write_pairs))
            else
              case safe_dispatch(fn -> mixed_set_results(ctx, write_pairs) end) do
                {:ok, results} -> results
                {:error, err} -> List.duplicate(err, length(write_pairs))
              end
            end

          encode_mixed_set_slots(set_pairs, results, state)
      end

    {get_slots, has_file_ref?} =
      case get_keys do
        [] ->
          {[], false}

        _ ->
          get_dispatch =
            if state.transport in [:ranch_tcp, :ranch_ssl] do
              fn ->
                Router.batch_get_with_deferred_blob_file_refs_and_presence(
                  ctx,
                  lookup_keys,
                  ConnSendfile.threshold_bytes()
                )
              end
            else
              fn -> Router.batch_get(ctx, lookup_keys) end
            end

          case safe_dispatch(get_dispatch) do
            {:ok, {values, has_file_ref?}} ->
              slots = encode_mixed_get_slots(get_keys, lookup_keys, values)
              {slots, has_file_ref?}

            {:ok, values} ->
              slots = encode_mixed_get_slots(get_keys, lookup_keys, values)
              {slots, has_file_ref_slot?(slots)}

            {:error, err} ->
              {List.duplicate(Encoder.encode(err), length(get_keys)), false}
          end
      end

    response_slots = assemble_mixed_slots(ops, get_slots, set_slots)

    if has_file_ref? do
      stream_mixed_results(response_slots, state, send_response_fn)
    else
      response = Enum.map(response_slots, &response_iodata/1)
      send_response_fn.(state.socket, state.transport, response)
      {:continue, track_mixed_get_results(response_slots, state)}
    end
  end

  defp encode_mixed_get_slots(get_keys, lookup_keys, values) do
    get_keys
    |> Enum.zip(lookup_keys)
    |> Enum.zip(values)
    |> Enum.map(fn
      {{key, lookup_key}, {:file_ref, path, offset, size}} ->
        {:file_ref, key, lookup_key, path, offset, size}

      {{key, _lookup_key}, value} ->
        {:get_encoded, key, Encoder.encode(value), value}
    end)
  end

  defp encode_mixed_set_slots(set_pairs, results, state) do
    set_pairs
    |> Enum.zip(results)
    |> Enum.map(fn
      {{key, value}, :ok} ->
        notify_pipeline_set_success(key, value, state)
        Encoder.ok_response()

      {_set_pair, {:error, _} = err} ->
        Encoder.encode(err)
    end)
  end

  defp has_file_ref_slot?(slots), do: Enum.any?(slots, &file_ref_slot?/1)

  defp file_ref_slot?({:file_ref, _key, _lookup_key, _path, _offset, _size}), do: true
  defp file_ref_slot?(_slot), do: false

  defp assemble_mixed_slots(ops, get_slots, set_slots),
    do: assemble_mixed_slots(ops, get_slots, set_slots, [])

  defp assemble_mixed_slots([], [], [], acc), do: Enum.reverse(acc)

  defp assemble_mixed_slots([{:get, _key} | rest], [slot | get_slots], set_slots, acc),
    do: assemble_mixed_slots(rest, get_slots, set_slots, [slot | acc])

  defp assemble_mixed_slots(
         [{:set, _key, _value} | rest],
         get_slots,
         [slot | set_slots],
         acc
       ),
       do: assemble_mixed_slots(rest, get_slots, set_slots, [slot | acc])

  defp response_iodata({:get_encoded, _key, encoded, _value}), do: encoded
  defp response_iodata(encoded), do: encoded

  defp track_mixed_get_results(results, state) do
    Enum.reduce(results, state, fn
      {:get_encoded, key, _encoded, value}, acc_state ->
        ConnTracking.maybe_track_read("GET", [key], value, acc_state)

      _other, acc_state ->
        acc_state
    end)
  end

  defp stream_mixed_results(response_slots, state, send_response_fn) do
    TcpOpts.set_cork(state.socket, true)

    {result, file_cache} =
      Enum.reduce_while(
        response_slots,
        {{:continue, state}, ConnSendfile.new_file_cache()},
        fn
          {:file_ref, key, lookup_key, path, offset, size},
          {{:continue, acc_state}, file_cache} ->
            case ConnSendfile.send_file_ref_response_cached(
                   key,
                   lookup_key,
                   path,
                   offset,
                   size,
                   acc_state,
                   file_cache
                 ) do
              {:sent, new_state, new_cache} ->
                {:cont, {{:continue, new_state}, new_cache}}

              {:fallback, new_cache} ->
                send_response_fn.(
                  acc_state.socket,
                  acc_state.transport,
                  Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
                )

                {:cont,
                 {{:continue,
                   ConnTracking.maybe_track_read("GET", [key], :fallback_ok, acc_state)},
                  new_cache}}

              {:error_after_header, _reason, new_cache} ->
                {:halt, {{:quit, acc_state}, new_cache}}
            end

          {:get_encoded, key, encoded, value}, {{:continue, acc_state}, file_cache} ->
            send_response_fn.(acc_state.socket, acc_state.transport, encoded)

            tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
            {:cont, {{:continue, tracked_state}, file_cache}}

          encoded, {{:continue, acc_state}, file_cache} ->
            send_response_fn.(acc_state.socket, acc_state.transport, encoded)
            {:cont, {{:continue, acc_state}, file_cache}}
        end
      )

    ConnSendfile.close_file_cache(file_cache)

    TcpOpts.set_cork(state.socket, false)
    result
  end

  defp mixed_set_results(ctx, kv_pairs) do
    Router.batch_quorum_put(ctx, kv_pairs)
  end

  defp notify_pipeline_set_success(key, value, state) do
    ConnTracking.maybe_notify_keyspace("SET", [key, value], :ok)
    ConnTracking.maybe_notify_tracking("SET", [key, value], :ok, state)
  end

  # ---------------------------------------------------------------------------
  # Batch FLOW write fast path
  # ---------------------------------------------------------------------------
  # Pipelined one-by-one Flow writes keep per-command semantics, but can still
  # share one Raft batch per shard. This is intentionally not FLOW.*_MANY: one
  # bad command fails only that command, while surrounding writes still commit.
  #
  # Terminal-capable commands stay on the normal router path: they may need
  # cross-shard parent/child coordination that the independent batch path does
  # not own. FLOW.RETRY is included because exhausted retries can enter a
  # terminal state.

  defp try_batch_flow_write_fast_path(commands, state, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing or
         not flow_pipeline_fast_paths_enabled?(state) do
      :fallback
    else
      if full_acl_fast_path?(state.acl_cache) do
        case extract_flow_writes(commands) do
          {:ok, writes} ->
            Stats.incr_commands_by(state.stats_counter, length(writes))

            response =
              case safe_dispatch(fn ->
                     Ferricstore.Flow.pipeline_write_batch_independent(state.instance_ctx, writes)
                   end) do
                {:ok, results} ->
                  Enum.map(results, &encode_flow_result/1)

                {:error, err} ->
                  List.duplicate(Encoder.encode(err), length(writes))
              end

            send_response_fn.(state.socket, state.transport, response)
            {:ok, {:continue, state}}

          :fallback ->
            :fallback
        end
      else
        :fallback
      end
    end
  end

  defp extract_flow_writes(commands), do: extract_flow_writes(commands, [])

  defp extract_flow_writes([], acc), do: {:ok, Enum.reverse(acc)}

  defp extract_flow_writes(
         [{:command, "FLOW.CREATE", _args, {:flow_create, id, opts}, _keys} | rest],
         acc
       )
       when is_binary(id) and is_list(opts),
       do: extract_flow_writes(rest, [{:create, id, opts} | acc])

  defp extract_flow_writes(
         [
           {:command, "FLOW.TRANSITION", _args,
            {:flow_transition, id, from_state, to_state, opts}, _keys}
           | rest
         ],
         acc
       )
       when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts),
       do: extract_flow_writes(rest, [{:transition, id, from_state, to_state, opts} | acc])

  defp extract_flow_writes(
         [{:command, "FLOW.SIGNAL", _args, {:flow_signal, id, opts}, _keys} | rest],
         acc
       )
       when is_binary(id) and is_list(opts),
       do: extract_flow_writes(rest, [{:signal, id, opts} | acc])

  defp extract_flow_writes(
         [
           {:command, "FLOW.EXTEND_LEASE", _args, {:flow_extend_lease, id, lease_token, opts},
            _keys}
           | rest
         ],
         acc
       )
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: extract_flow_writes(rest, [{:extend_lease, id, lease_token, opts} | acc])

  defp extract_flow_writes(
         [{:command, "FLOW.VALUE.PUT", _args, {:flow_value_put, value, opts}, _keys} | rest],
         acc
       )
       when is_list(opts) do
    if owned_flow_value_put_opts?(opts) do
      extract_flow_writes(rest, [{:value_put, value, opts} | acc])
    else
      :fallback
    end
  end

  defp extract_flow_writes(
         [
           {:command, "FLOW.COMPLETE", _args, {:flow_complete, id, lease_token, opts}, _keys}
           | rest
         ],
         acc
       )
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: extract_flow_writes(rest, [{:complete, id, lease_token, opts} | acc])

  defp extract_flow_writes(
         [{:command, "FLOW.RETRY", _args, {:flow_retry, id, lease_token, opts}, _keys} | rest],
         acc
       )
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: extract_flow_writes(rest, [{:retry, id, lease_token, opts} | acc])

  defp extract_flow_writes(
         [{:command, "FLOW.FAIL", _args, {:flow_fail, id, lease_token, opts}, _keys} | rest],
         acc
       )
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: extract_flow_writes(rest, [{:fail, id, lease_token, opts} | acc])

  defp extract_flow_writes(
         [{:command, "FLOW.CANCEL", _args, {:flow_cancel, id, opts}, _keys} | rest],
         acc
       )
       when is_binary(id) and is_list(opts),
       do: extract_flow_writes(rest, [{:cancel, id, opts} | acc])

  defp extract_flow_writes(
         [{:command, "FLOW.REWIND", _args, {:flow_rewind, id, opts}, _keys} | rest],
         acc
       )
       when is_binary(id) and is_list(opts),
       do: extract_flow_writes(rest, [{:rewind, id, opts} | acc])

  defp extract_flow_writes(_commands, _acc), do: :fallback

  defp flow_write_op({:command, "FLOW.CREATE", _args, {:flow_create, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:create, id, opts}}

  defp flow_write_op(
         {:command, "FLOW.TRANSITION", _args, {:flow_transition, id, from_state, to_state, opts},
          _keys}
       )
       when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts),
       do: {:ok, {:transition, id, from_state, to_state, opts}}

  defp flow_write_op({:command, "FLOW.SIGNAL", _args, {:flow_signal, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:signal, id, opts}}

  defp flow_write_op(
         {:command, "FLOW.EXTEND_LEASE", _args, {:flow_extend_lease, id, lease_token, opts},
          _keys}
       )
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: {:ok, {:extend_lease, id, lease_token, opts}}

  defp flow_write_op({:command, "FLOW.VALUE.PUT", _args, {:flow_value_put, value, opts}, _keys})
       when is_list(opts) do
    if owned_flow_value_put_opts?(opts) do
      {:ok, {:value_put, value, opts}}
    else
      :fallback
    end
  end

  defp flow_write_op(
         {:command, "FLOW.COMPLETE", _args, {:flow_complete, id, lease_token, opts}, _keys}
       )
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: {:ok, {:complete, id, lease_token, opts}}

  defp flow_write_op({:command, "FLOW.RETRY", _args, {:flow_retry, id, lease_token, opts}, _keys})
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: {:ok, {:retry, id, lease_token, opts}}

  defp flow_write_op({:command, "FLOW.FAIL", _args, {:flow_fail, id, lease_token, opts}, _keys})
       when is_binary(id) and is_binary(lease_token) and is_list(opts),
       do: {:ok, {:fail, id, lease_token, opts}}

  defp flow_write_op({:command, "FLOW.CANCEL", _args, {:flow_cancel, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:cancel, id, opts}}

  defp flow_write_op({:command, "FLOW.REWIND", _args, {:flow_rewind, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:rewind, id, opts}}

  defp flow_write_op(_command), do: :fallback

  defp owned_flow_value_put_opts?(opts) when is_list(opts) do
    is_binary(Keyword.get(opts, :owner_flow_id)) and is_binary(Keyword.get(opts, :name))
  end

  defp owned_flow_value_put_opts?(_opts), do: false

  defp flow_claim_due_op(
         {:command, "FLOW.CLAIM_DUE", _args, {:flow_claim_due, type, opts}, _keys}
       )
       when is_binary(type) and is_list(opts),
       do: {:ok, {:claim_due, type, opts}}

  defp flow_claim_due_op(_command), do: :fallback

  defp flow_read_op({:command, "FLOW.GET", _args, {:flow_get, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:get, id, opts}}

  defp flow_read_op({:command, "FLOW.HISTORY", _args, {:flow_history, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:history, id, opts}}

  defp flow_read_op({:command, "FLOW.VALUE.MGET", _args, {:flow_value_mget, refs}, _keys})
       when is_list(refs),
       do: {:ok, {:value_mget, refs, []}}

  defp flow_read_op({:command, "FLOW.VALUE.MGET", _args, {:flow_value_mget, refs, opts}, _keys})
       when is_list(refs) and is_list(opts),
       do: {:ok, {:value_mget, refs, opts}}

  defp flow_read_op({:command, "FLOW.LIST", _args, {:flow_list, type, opts}, _keys})
       when is_binary(type) and is_list(opts),
       do: {:ok, {:list, type, opts}}

  defp flow_read_op({:command, "FLOW.TERMINALS", _args, {:flow_terminals, type, opts}, _keys})
       when is_binary(type) and is_list(opts),
       do: {:ok, {:terminals, type, opts}}

  defp flow_read_op({:command, "FLOW.FAILURES", _args, {:flow_failures, type, opts}, _keys})
       when is_binary(type) and is_list(opts),
       do: {:ok, {:failures, type, opts}}

  defp flow_read_op({:command, "FLOW.BY_PARENT", _args, {:flow_by_parent, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:by_parent, id, opts}}

  defp flow_read_op({:command, "FLOW.BY_ROOT", _args, {:flow_by_root, id, opts}, _keys})
       when is_binary(id) and is_list(opts),
       do: {:ok, {:by_root, id, opts}}

  defp flow_read_op(
         {:command, "FLOW.BY_CORRELATION", _args, {:flow_by_correlation, id, opts}, _keys}
       )
       when is_binary(id) and is_list(opts),
       do: {:ok, {:by_correlation, id, opts}}

  defp flow_read_op({:command, "FLOW.INFO", _args, {:flow_info, type, opts}, _keys})
       when is_binary(type) and is_list(opts),
       do: {:ok, {:info, type, opts}}

  defp flow_read_op({:command, "FLOW.STUCK", _args, {:flow_stuck, type, opts}, _keys})
       when is_binary(type) and is_list(opts),
       do: {:ok, {:stuck, type, opts}}

  defp flow_read_op(_command), do: :fallback

  defp encode_flow_result(result) do
    result
    |> FlowCommand.normalize_result()
    |> Encoder.encode()
  end

  # ---------------------------------------------------------------------------
  # General batch dispatch (fallback for non-GET/SET pipelines)
  # ---------------------------------------------------------------------------
  # Dispatches each command through the Dispatcher with per-command ACL checks,
  # batches all responses, and sends them in one write. Stateful commands
  # (MULTI, AUTH, SUBSCRIBE, etc.) force a flush-and-sequential boundary.

  defp general_batch_dispatch(commands, state, handle_command_fn, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing or
         not full_acl_fast_path?(state.acl_cache) do
      sequential_dispatch(commands, state, handle_command_fn, send_response_fn)
    else
      case split_at_stateful(commands, state) do
        {:all_pure, pure_cmds} ->
          do_batch_pure(pure_cmds, state, send_response_fn)

        {:split, pure_prefix, stateful_cmd, rest} ->
          case do_batch_pure(pure_prefix, state, send_response_fn) do
            {:quit, _} = quit ->
              quit

            {:continue, new_state} ->
              case handle_command_fn.(stateful_cmd, new_state) do
                {:quit, response, quit_state} ->
                  send_response_fn.(quit_state.socket, quit_state.transport, response)
                  {:quit, quit_state}

                {:continue, response, new_state2} ->
                  send_response_fn.(new_state2.socket, new_state2.transport, response)

                  if rest == [] do
                    {:continue, new_state2}
                  else
                    general_batch_dispatch(rest, new_state2, handle_command_fn, send_response_fn)
                  end
              end
          end
      end
    end
  end

  defp split_at_stateful(commands, state) do
    split_at_stateful(commands, state, [])
  end

  defp split_at_stateful([], _state, acc), do: {:all_pure, Enum.reverse(acc)}

  defp split_at_stateful([cmd | rest], state, acc) do
    name = extract_command_name(cmd)

    if MapSet.member?(@stateful_cmds, name) or
         (is_binary(name) and String.starts_with?(name, "CLIENT")) do
      {:split, Enum.reverse(acc), cmd, rest}
    else
      split_at_stateful(rest, state, [cmd | acc])
    end
  end

  defp do_batch_pure([], state, _send_response_fn), do: {:continue, state}

  defp do_batch_pure(commands, state, send_response_fn) do
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)

    prefetched_reads =
      if has_flow_write?(commands) do
        %{}
      else
        prefetch_pure_tcp_reads(commands, state)
      end

    {action, responses, final_state} =
      commands
      |> Enum.with_index()
      |> dispatch_pure_segments(state, store, prefetched_reads, [])

    responses = Enum.reverse(responses)

    if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, true)

    result =
      if Enum.any?(responses, &streaming_response?/1) do
        stream_response_entries(responses, final_state, send_response_fn)
      else
        send_response_fn.(
          state.socket,
          state.transport,
          Enum.map(responses, fn {:encoded, encoded} -> encoded end)
        )

        {action, final_state}
      end

    if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, false)
    result
  end

  defp dispatch_pure_segments([], state, _store, _prefetched_reads, acc) do
    {:continue, acc, state}
  end

  defp dispatch_pure_segments([{cmd, idx} | rest], state, store, prefetched_reads, acc) do
    flow_fast_paths? = flow_pipeline_fast_paths_enabled?(state)

    case maybe_flow_write_op(cmd, flow_fast_paths?) do
      {:ok, op} ->
        {ops, rest} = take_flow_write_segment(rest, [op])
        entries = dispatch_flow_write_segment(Enum.reverse(ops), state)
        dispatch_pure_segments(rest, state, store, prefetched_reads, Enum.reverse(entries) ++ acc)

      :fallback ->
        case maybe_flow_claim_due_op(cmd, flow_fast_paths?) do
          {:ok, op} ->
            {ops, rest} = take_flow_claim_due_segment(rest, [op])
            entries = dispatch_flow_claim_due_segment(Enum.reverse(ops), state)

            dispatch_pure_segments(
              rest,
              state,
              store,
              prefetched_reads,
              Enum.reverse(entries) ++ acc
            )

          :fallback ->
            case maybe_flow_read_op(cmd, flow_fast_paths?) do
              {:ok, op} ->
                {ops, rest} = take_flow_read_segment(rest, [op])
                entries = dispatch_flow_read_segment(Enum.reverse(ops), state)

                dispatch_pure_segments(
                  rest,
                  state,
                  store,
                  prefetched_reads,
                  Enum.reverse(entries) ++ acc
                )

              :fallback ->
                case phase1_write_op(cmd, idx, state) do
                  {:ok, op} ->
                    {ops, rest} = take_phase1_write_segment(rest, [op], state)

                    {entries, new_state} =
                      dispatch_phase1_write_segment(
                        Enum.reverse(ops),
                        state,
                        store,
                        prefetched_reads
                      )

                    dispatch_pure_segments(
                      rest,
                      new_state,
                      store,
                      prefetched_reads,
                      Enum.reverse(entries) ++ acc
                    )

                  :fallback ->
                    {name, args, _ast, _keys} = command_parts(cmd)

                    {entry, new_state} =
                      dispatch_pure_single(idx, cmd, name, args, store, state, prefetched_reads)

                    dispatch_pure_segments(rest, new_state, store, prefetched_reads, [
                      entry | acc
                    ])
                end
            end
        end
    end
  end

  defp flow_pipeline_fast_paths_enabled?(%{sandbox_namespace: nil}), do: true
  defp flow_pipeline_fast_paths_enabled?(_state), do: false

  defp maybe_flow_write_op(cmd, true), do: flow_write_op(cmd)
  defp maybe_flow_write_op(_cmd, false), do: :fallback

  defp maybe_flow_claim_due_op(cmd, true), do: flow_claim_due_op(cmd)
  defp maybe_flow_claim_due_op(_cmd, false), do: :fallback

  defp maybe_flow_read_op(cmd, true), do: flow_read_op(cmd)
  defp maybe_flow_read_op(_cmd, false), do: :fallback

  defp take_flow_write_segment([{cmd, idx} | rest], acc) do
    case flow_write_op(cmd) do
      {:ok, op} -> take_flow_write_segment(rest, [op | acc])
      :fallback -> {acc, [{cmd, idx} | rest]}
    end
  end

  defp take_flow_write_segment([], acc), do: {acc, []}

  defp take_flow_claim_due_segment([{cmd, idx} | rest], acc) do
    case flow_claim_due_op(cmd) do
      {:ok, op} -> take_flow_claim_due_segment(rest, [op | acc])
      :fallback -> {acc, [{cmd, idx} | rest]}
    end
  end

  defp take_flow_claim_due_segment([], acc), do: {acc, []}

  defp take_flow_read_segment([{cmd, idx} | rest], acc) do
    case flow_read_op(cmd) do
      {:ok, op} -> take_flow_read_segment(rest, [op | acc])
      :fallback -> {acc, [{cmd, idx} | rest]}
    end
  end

  defp take_flow_read_segment([], acc), do: {acc, []}

  defp take_phase1_write_segment([{cmd, idx} | rest], acc, state) do
    case phase1_write_op(cmd, idx, state) do
      {:ok, op} -> take_phase1_write_segment(rest, [op | acc], state)
      :fallback -> {acc, [{cmd, idx} | rest]}
    end
  end

  defp take_phase1_write_segment([], acc, _state), do: {acc, []}

  defp dispatch_flow_write_segment(ops, state) do
    Stats.incr_commands_by(state.stats_counter, length(ops))

    case safe_dispatch(fn ->
           Ferricstore.Flow.pipeline_write_batch_independent(state.instance_ctx, ops)
         end) do
      {:ok, results} ->
        Enum.map(results, fn result -> {:encoded, encode_flow_result(result)} end)

      {:error, err} ->
        List.duplicate({:encoded, Encoder.encode(err)}, length(ops))
    end
  end

  defp dispatch_flow_claim_due_segment(ops, state) do
    Stats.incr_commands_by(state.stats_counter, length(ops))

    case safe_dispatch(fn ->
           Ferricstore.Flow.pipeline_claim_due_batch(state.instance_ctx, ops)
         end) do
      {:ok, results} ->
        Enum.map(results, fn result -> {:encoded, encode_flow_result(result)} end)

      {:error, err} ->
        List.duplicate({:encoded, Encoder.encode(err)}, length(ops))
    end
  end

  defp dispatch_flow_read_segment(ops, state) do
    Stats.incr_commands_by(state.stats_counter, length(ops))

    case safe_dispatch(fn ->
           Ferricstore.Flow.pipeline_read_batch(state.instance_ctx, ops)
         end) do
      {:ok, results} ->
        Enum.map(results, fn result -> {:encoded, encode_flow_result(result)} end)

      {:error, err} ->
        List.duplicate({:encoded, Encoder.encode(err)}, length(ops))
    end
  end

  defp dispatch_phase1_write_segment(ops, state, store, prefetched_reads) do
    if phase1_write_pressure_ok?(state.instance_ctx) do
      Stats.incr_commands_by(state.stats_counter, length(ops))

      keyed_commands = Enum.map(ops, fn op -> {op.key, op.command} end)

      case safe_dispatch(fn ->
             Router.pipeline_write_batch(state.instance_ctx, keyed_commands)
           end) do
        {:ok, results} ->
          Enum.zip(ops, results)
          |> Enum.reduce({[], state}, fn {op, result}, {entries, acc_state} ->
            {
              [response_entry(result) | entries],
              track_non_streaming_pure_result(op.name, op.args, result, acc_state)
            }
          end)
          |> then(fn {entries, final_state} -> {Enum.reverse(entries), final_state} end)

        {:error, err} ->
          {List.duplicate({:encoded, Encoder.encode(err)}, length(ops)), state}
      end
    else
      dispatch_phase1_write_segment_sequential(ops, state, store, prefetched_reads)
    end
  end

  defp dispatch_phase1_write_segment_sequential(ops, state, store, prefetched_reads) do
    Enum.reduce(ops, {[], state}, fn op, {entries, acc_state} ->
      {entry, new_state} =
        dispatch_pure_single(op.idx, op.cmd, op.name, op.args, store, acc_state, prefetched_reads)

      {[entry | entries], new_state}
    end)
    |> then(fn {entries, final_state} -> {Enum.reverse(entries), final_state} end)
  end

  defp phase1_write_pressure_ok?(ctx),
    do: :atomics.get(ctx.pressure_flags, 1) == 0 and :atomics.get(ctx.pressure_flags, 2) == 0

  defp has_flow_write?(commands) do
    Enum.any?(commands, fn command ->
      match?({:ok, _op}, flow_write_op(command))
    end)
  end

  defp phase1_write_op(
         {:command, name, args, ast, _keys} = cmd,
         idx,
         %{sandbox_namespace: namespace} = state
       )
       when is_binary(name) and is_list(args) do
    if not phase1_write_batch_enabled?(state) do
      :fallback
    else
      with {:ok, key, command} <- phase1_write_command(ast, namespace) do
        {:ok, %{idx: idx, cmd: cmd, name: name, args: args, key: key, command: command}}
      end
    end
  end

  defp phase1_write_op(_command, _idx, _state), do: :fallback

  defp phase1_write_batch_enabled?(%{transport: transport, instance_ctx: %{name: :default}})
       when transport in [:ranch_tcp, :ranch_ssl],
       do: true

  defp phase1_write_batch_enabled?(_state), do: false

  defp phase1_write_command({:set, key, value}, namespace)
       when is_binary(key) and is_binary(value) and
              byte_size(key) <= @max_key_size and byte_size(value) < @max_value_size,
       do: phase1_key_command(key, {:put, namespace_key(namespace, key), value, 0}, namespace)

  defp phase1_write_command({:set, _key, _value}, _namespace), do: :fallback

  defp phase1_write_command({:incr, key}, namespace) when is_binary(key),
    do: phase1_key_command(key, {:incr, namespace_key(namespace, key), 1}, namespace)

  defp phase1_write_command({:decr, key}, namespace) when is_binary(key),
    do: phase1_key_command(key, {:incr, namespace_key(namespace, key), -1}, namespace)

  defp phase1_write_command({:incrby, key, delta}, namespace)
       when is_binary(key) and is_integer(delta),
       do: phase1_key_command(key, {:incr, namespace_key(namespace, key), delta}, namespace)

  defp phase1_write_command({:decrby, key, delta}, namespace)
       when is_binary(key) and is_integer(delta),
       do: phase1_key_command(key, {:incr, namespace_key(namespace, key), -delta}, namespace)

  defp phase1_write_command({:incrbyfloat, key, delta}, namespace)
       when is_binary(key) and is_float(delta),
       do: phase1_key_command(key, {:incr_float, namespace_key(namespace, key), delta}, namespace)

  defp phase1_write_command({:append, key, suffix}, namespace) when is_binary(key),
    do: phase1_key_command(key, {:append, namespace_key(namespace, key), suffix}, namespace)

  defp phase1_write_command({:setrange, key, offset, value}, namespace)
       when is_binary(key) and is_integer(offset) and offset >= 0 and
              offset <= @max_setrange_offset,
       do:
         phase1_key_command(
           key,
           {:setrange, namespace_key(namespace, key), offset, value},
           namespace
         )

  defp phase1_write_command({:setrange, _key, _offset, _value}, _namespace), do: :fallback

  defp phase1_write_command({:setbit, key, offset, bit}, namespace)
       when is_binary(key) and is_integer(offset) and offset >= 0 and offset <= @max_bit_offset and
              bit in [0, 1],
       do:
         phase1_key_command(key, {:setbit, namespace_key(namespace, key), offset, bit}, namespace)

  defp phase1_write_command({:setbit, _key, _offset, _bit}, _namespace), do: :fallback

  defp phase1_write_command({:hincrby, key, field, delta}, namespace)
       when is_binary(key) and is_integer(delta),
       do:
         phase1_key_command(
           key,
           {:hincrby, namespace_key(namespace, key), field, delta},
           namespace
         )

  defp phase1_write_command({:hincrbyfloat, key, field, delta}, namespace)
       when is_binary(key) and is_float(delta),
       do:
         phase1_key_command(
           key,
           {:hincrbyfloat, namespace_key(namespace, key), field, delta},
           namespace
         )

  defp phase1_write_command({:zincrby, key, increment, member}, namespace)
       when is_binary(key) and is_float(increment),
       do:
         phase1_key_command(
           key,
           {:zincrby, namespace_key(namespace, key), increment, member},
           namespace
         )

  defp phase1_write_command({:pfadd, [key | elements]}, namespace) when is_binary(key),
    do: phase1_key_command(key, {:pfadd, namespace_key(namespace, key), elements}, namespace)

  defp phase1_write_command({:json_set, key, path, value, flags}, namespace)
       when is_binary(key) and (is_binary(path) or is_list(path)) and is_binary(value) and
              is_list(flags),
       do:
         phase1_key_command(
           key,
           {:json_set, namespace_key(namespace, key), path, value, flags},
           namespace
         )

  defp phase1_write_command({:json_set, _key, _path, _value, _flags}, _namespace),
    do: :fallback

  defp phase1_write_command({:json_del, key, path}, namespace)
       when is_binary(key) and (is_binary(path) or is_list(path)),
       do: phase1_key_command(key, {:json_del, namespace_key(namespace, key), path}, namespace)

  defp phase1_write_command({:json_del, _key, _path}, _namespace), do: :fallback

  defp phase1_write_command({:json_numincrby, key, path, increment}, namespace)
       when is_binary(key) and (is_binary(path) or is_list(path)) and is_number(increment),
       do:
         phase1_key_command(
           key,
           {:json_numincrby, namespace_key(namespace, key), path, increment},
           namespace
         )

  defp phase1_write_command({:json_numincrby, _key, _path, _increment}, _namespace),
    do: :fallback

  defp phase1_write_command({:json_arrappend, key, path, values}, namespace)
       when is_binary(key) and (is_binary(path) or is_list(path)) and is_list(values),
       do:
         phase1_key_command(
           key,
           {:json_arrappend, namespace_key(namespace, key), path, values},
           namespace
         )

  defp phase1_write_command({:json_arrappend, _key, _path, _values}, _namespace),
    do: :fallback

  defp phase1_write_command({:json_toggle, key, path}, namespace)
       when is_binary(key) and (is_binary(path) or is_list(path)),
       do: phase1_key_command(key, {:json_toggle, namespace_key(namespace, key), path}, namespace)

  defp phase1_write_command({:json_toggle, _key, _path}, _namespace), do: :fallback

  defp phase1_write_command({:json_clear, key, path}, namespace)
       when is_binary(key) and (is_binary(path) or is_list(path)),
       do: phase1_key_command(key, {:json_clear, namespace_key(namespace, key), path}, namespace)

  defp phase1_write_command({:json_clear, _key, _path}, _namespace), do: :fallback

  defp phase1_write_command(_ast, _namespace), do: :fallback

  defp phase1_key_command(key, command, namespace) do
    {:ok, namespace_key(namespace, key), command}
  end

  defp dispatch_pure_single(idx, cmd, name, args, store, state, prefetched_reads) do
    {_name, _args, ast, keys} = command_parts(cmd)
    Stats.incr_commands(state.stats_counter)

    result =
      case ConnAuth.check_command_cached(state.acl_cache, name) do
        :ok ->
          case ConnAuth.check_keys_cached(state.acl_cache, name, keys) do
            :ok ->
              try do
                dispatch_pure_command(idx, name, args, ast, store, state, prefetched_reads)
              catch
                :exit, {:noproc, _} ->
                  {:error, "ERR server not ready, shard process unavailable"}

                :exit, {reason, _} ->
                  {:error, "ERR internal error: #{inspect(reason)}"}

                kind, reason ->
                  internal_error(kind, reason)
              end

            {:error, _} = err ->
              log_acl_denied(state, name)
              err
          end

        {:error, _} = err ->
          log_acl_denied(state, name)
          err
      end

    {response_entry(result), track_pure_result(name, args, result, state)}
  end

  defp prefetch_pure_tcp_reads(commands, %{transport: transport} = state)
       when transport in [:ranch_tcp, :ranch_ssl] do
    {gets, mgets, getranges, _written_keys, _prefetch_blocked?} =
      commands
      |> Enum.with_index()
      |> Enum.reduce({[], [], [], MapSet.new(), false}, fn {cmd, idx},
                                                           {get_acc, mget_acc, getrange_acc,
                                                            written_keys, prefetch_blocked?} ->
        case command_parts(cmd) do
          {"GET", [key], {:get, key}, [key]} when is_binary(key) ->
            if prefetch_blocked? or MapSet.member?(written_keys, key) do
              {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
            else
              {
                [{idx, key, namespace_key(state.sandbox_namespace, key)} | get_acc],
                mget_acc,
                getrange_acc,
                written_keys,
                prefetch_blocked?
              }
            end

          {"MGET", keys, {:mget, keys}, keys} when is_list(keys) and keys != [] ->
            if prefetch_blocked? or any_written_key?(keys, written_keys) do
              {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
            else
              lookup_keys = namespace_keys(state.sandbox_namespace, keys)

              {get_acc, [{idx, keys, lookup_keys} | mget_acc], getrange_acc, written_keys,
               prefetch_blocked?}
            end

          {"GETRANGE", [key, _start_arg, _end_arg] = args, {:getrange, key, start_idx, end_idx},
           [key]}
          when is_binary(key) and is_integer(start_idx) and is_integer(end_idx) ->
            if prefetch_blocked? or MapSet.member?(written_keys, key) do
              {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
            else
              {get_acc, mget_acc, [{idx, args, key, start_idx, end_idx} | getrange_acc],
               written_keys, prefetch_blocked?}
            end

          {name, _args, _ast, keys} when is_list(keys) and keys != [] ->
            if read_only_keyed_command?(name) do
              {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
            else
              {get_acc, mget_acc, getrange_acc, mark_written_keys(written_keys, keys),
               prefetch_blocked?}
            end

          {name, _args, _ast, keys} when is_list(keys) ->
            {get_acc, mget_acc, getrange_acc, written_keys,
             prefetch_blocked? or prefetch_keyless_barrier_command?(name)}
        end
      end)

    gets
    |> Enum.reverse()
    |> prefetch_tcp_get_ops(state)
    |> Map.merge(prefetch_tcp_mget_ops(Enum.reverse(mgets), state))
    |> Map.merge(prefetch_tcp_getrange_ops(Enum.reverse(getranges), state))
  end

  defp prefetch_pure_tcp_reads(_commands, _state), do: %{}

  defp any_written_key?(keys, written_keys) do
    Enum.any?(keys, &MapSet.member?(written_keys, &1))
  end

  defp mark_written_keys(written_keys, keys) do
    Enum.reduce(keys, written_keys, fn key, acc -> MapSet.put(acc, key) end)
  end

  defp read_only_keyed_command?(name), do: MapSet.member?(@prefetch_read_only_keyed_cmds, name)

  defp prefetch_keyless_barrier_command?(name) do
    not MapSet.member?(@prefetch_read_only_keyless_cmds, name)
  end

  defp prefetch_tcp_get_ops(gets, state) do
    case gets do
      [] ->
        %{}

      _ ->
        lookup_keys = Enum.map(gets, fn {_idx, _key, lookup_key} -> lookup_key end)

        case safe_dispatch(fn ->
               Router.batch_get_with_deferred_blob_file_refs(
                 state.instance_ctx,
                 lookup_keys,
                 ConnSendfile.threshold_bytes()
               )
             end) do
          {:ok, results} ->
            gets
            |> Enum.zip(results)
            |> Map.new(fn
              {{idx, key, lookup_key}, {:file_ref, path, offset, size}} ->
                {idx, {:file_ref, key, lookup_key, path, offset, size}}

              {{idx, _key, _lookup_key}, value} ->
                {idx, value}
            end)

          {:error, err} ->
            Map.new(gets, fn {idx, _key, _lookup_key} -> {idx, err} end)
        end
    end
  end

  defp prefetch_tcp_mget_ops(mgets, state) do
    Map.new(mgets, fn {idx, keys, lookup_keys} ->
      {idx, prefetch_tcp_mget_result(keys, lookup_keys, state)}
    end)
  end

  defp prefetch_tcp_getrange_ops(getranges, state) do
    Enum.reduce(getranges, %{}, fn {idx, args, key, start_idx, end_idx}, acc ->
      case ConnSendfile.getrange_cold_response(args, key, start_idx, end_idx, state) do
        :fallback ->
          acc

        result ->
          Map.put(acc, idx, result)
      end
    end)
  end

  defp prefetch_tcp_mget_result(keys, lookup_keys, state) do
    case safe_dispatch(fn ->
           Router.batch_get_with_deferred_blob_file_refs(
             state.instance_ctx,
             lookup_keys,
             ConnSendfile.threshold_bytes()
           )
         end) do
      {:ok, results} ->
        if Enum.any?(results, &match?({:file_ref, _, _, _}, &1)) do
          {:array, keys, mget_response_elements(keys, lookup_keys, results)}
        else
          results
        end

      {:error, err} ->
        err
    end
  end

  defp mget_response_elements(keys, lookup_keys, results) do
    keys
    |> Enum.zip(lookup_keys)
    |> Enum.zip(results)
    |> Enum.map(fn
      {{key, lookup_key}, {:file_ref, path, offset, size}} ->
        {:file_ref, key, lookup_key, path, offset, size}

      {{_key, _lookup_key}, value} ->
        {:encoded, Encoder.encode(value)}
    end)
  end

  defp dispatch_pure_command(idx, name, args, ast, store, state, prefetched_reads) do
    case Map.fetch(prefetched_reads, idx) do
      {:ok, result} ->
        result

      :error ->
        dispatch_store_command(
          name,
          args,
          ast,
          store,
          state.instance_ctx,
          state.sandbox_namespace
        )
    end
  end

  defp response_entry({:file_ref, _key, _lookup_key, _path, _offset, _size} = file_ref),
    do: file_ref

  defp response_entry(
         {:file_range, _args, _key, _start_idx, _end_idx, _path, _offset, _size, _validator} =
           file_range
       ),
       do: file_range

  defp response_entry({:array, _keys, _elements} = array), do: array
  defp response_entry({:value, value}), do: {:encoded, Encoder.encode(value)}

  defp response_entry(result), do: {:encoded, Encoder.encode(result)}

  defp streaming_response?({:file_ref, _key, _lookup_key, _path, _offset, _size}), do: true

  defp streaming_response?(
         {:file_range, _args, _key, _start_idx, _end_idx, _path, _offset, _size, _validator}
       ),
       do: true

  defp streaming_response?({:array, _keys, _elements}), do: true
  defp streaming_response?(_result), do: false

  defp track_pure_result(name, args, result, state) when is_tuple(result) do
    if streaming_response?(result) do
      state
    else
      track_non_streaming_pure_result(name, args, result, state)
    end
  end

  defp track_pure_result(name, args, result, state),
    do: track_non_streaming_pure_result(name, args, result, state)

  defp track_non_streaming_pure_result(name, args, result, state) do
    ConnTracking.maybe_notify_keyspace(name, args, result)
    ConnTracking.maybe_notify_tracking(name, args, result, state)
    ConnTracking.maybe_track_read(name, args, result, state)
  end

  defp stream_response_entries(entries, state, send_response_fn) do
    {result, file_cache} =
      Enum.reduce_while(
        entries,
        {{:continue, state}, ConnSendfile.new_file_cache()},
        fn
          {:array, keys, elements}, {{:continue, acc_state}, file_cache} ->
            case stream_array_response(keys, elements, acc_state, send_response_fn, file_cache) do
              {{:sent, new_state}, new_cache} ->
                {:cont, {{:continue, new_state}, new_cache}}

              {{:error_after_header, _reason}, new_cache} ->
                {:halt, {{:quit, acc_state}, new_cache}}
            end

          {:file_range, args, key, start_idx, end_idx, path, offset, size, validator},
          {{:continue, acc_state}, file_cache} ->
            case ConnSendfile.send_file_range_response_cached(
                   args,
                   path,
                   offset,
                   size,
                   validator,
                   acc_state,
                   file_cache
                 ) do
              {:sent, new_state, new_cache} ->
                {:cont, {{:continue, new_state}, new_cache}}

              {:fallback, new_cache} ->
                send_response_fn.(
                  acc_state.socket,
                  acc_state.transport,
                  Encoder.encode(
                    ConnSendfile.materialize_getrange(key, start_idx, end_idx, acc_state)
                  )
                )

                tracked_state =
                  ConnTracking.maybe_track_read("GETRANGE", args, :fallback_ok, acc_state)

                {:cont, {{:continue, tracked_state}, new_cache}}

              {:error_after_header, _reason, new_cache} ->
                {:halt, {{:quit, acc_state}, new_cache}}
            end

          {:file_ref, key, lookup_key, path, offset, size},
          {{:continue, acc_state}, file_cache} ->
            case ConnSendfile.send_file_ref_response_cached(
                   key,
                   lookup_key,
                   path,
                   offset,
                   size,
                   acc_state,
                   file_cache
                 ) do
              {:sent, new_state, new_cache} ->
                {:cont, {{:continue, new_state}, new_cache}}

              {:fallback, new_cache} ->
                send_response_fn.(
                  acc_state.socket,
                  acc_state.transport,
                  Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
                )

                tracked_state =
                  ConnTracking.maybe_track_read("GET", [key], :fallback_ok, acc_state)

                {:cont, {{:continue, tracked_state}, new_cache}}

              {:error_after_header, _reason, new_cache} ->
                {:halt, {{:quit, acc_state}, new_cache}}
            end

          {:encoded, encoded}, {{:continue, acc_state}, file_cache} ->
            send_response_fn.(acc_state.socket, acc_state.transport, encoded)
            {:cont, {{:continue, acc_state}, file_cache}}
        end
      )

    ConnSendfile.close_file_cache(file_cache)
    result
  end

  defp stream_array_response(keys, elements, state, send_response_fn, file_cache) do
    send_response_fn.(state.socket, state.transport, [
      "*",
      Integer.to_string(length(elements)),
      "\r\n"
    ])

    {result, file_cache} =
      Enum.reduce_while(elements, {{:sent, state}, file_cache}, fn
        {:file_ref, key, lookup_key, path, offset, size}, {{:sent, acc_state}, file_cache} ->
          case ConnSendfile.send_file_ref_element_response_cached(
                 key,
                 lookup_key,
                 path,
                 offset,
                 size,
                 acc_state,
                 file_cache
               ) do
            {:sent, new_state, new_cache} ->
              {:cont, {{:sent, new_state}, new_cache}}

            {:fallback, new_cache} ->
              send_response_fn.(
                acc_state.socket,
                acc_state.transport,
                Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
              )

              {:cont, {{:sent, acc_state}, new_cache}}

            {:error_after_header, reason, new_cache} ->
              {:halt, {{:error_after_header, reason}, new_cache}}
          end

        {:encoded, encoded}, {{:sent, acc_state}, file_cache} ->
          send_response_fn.(acc_state.socket, acc_state.transport, encoded)
          {:cont, {{:sent, acc_state}, file_cache}}
      end)

    result =
      case result do
        {:sent, new_state} ->
          {:sent, ConnTracking.maybe_track_read("MGET", keys, :sendfile_ok, new_state)}

        other ->
          other
      end

    {result, file_cache}
  end

  defp log_acl_denied(state, name) do
    FerricstoreServer.Acl.log_command_denied(
      state.username,
      name,
      format_peer(state.peer),
      state.client_id
    )
  end

  # Pure sequential fallback for auth-required or queuing states
  defp sequential_dispatch(commands, state, handle_command_fn, send_response_fn) do
    if state.transport == :ranch_tcp do
      TcpOpts.set_cork(state.socket, true)
      result = do_sequential(commands, state, handle_command_fn, send_response_fn)
      TcpOpts.set_cork(state.socket, false)
      result
    else
      do_sequential(commands, state, handle_command_fn, send_response_fn)
    end
  end

  defp do_sequential([], state, _handle_command_fn, _send_response_fn), do: {:continue, state}

  defp do_sequential([cmd | rest], state, handle_command_fn, send_response_fn) do
    case handle_command_fn.(cmd, state) do
      {:quit, response, quit_state} ->
        send_response_fn.(quit_state.socket, quit_state.transport, response)
        {:quit, quit_state}

      {:continue, response, new_state} ->
        send_response_fn.(new_state.socket, new_state.transport, response)
        do_sequential(rest, new_state, handle_command_fn, send_response_fn)
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp command_parts({:command, name, args, ast, keys})
       when is_binary(name) and is_list(args) and is_list(keys),
       do: {name, args, ast, keys}

  defp command_parts(_other), do: {"UNKNOWN", [], {:unknown, "UNKNOWN", []}, []}

  defp dispatch_store_command(
         _name,
         _args,
         {:pfadd, [key | elements]},
         _store,
         ctx,
         namespace
       ) do
    Router.pfadd(ctx, namespace_key(namespace, key), elements)
  end

  defp dispatch_store_command(
         _name,
         _args,
         {:pfmerge, [dest_key | source_keys]},
         _store,
         ctx,
         namespace
       ) do
    Router.pfmerge(
      ctx,
      namespace_key(namespace, dest_key),
      namespace_keys(namespace, source_keys)
    )
  end

  defp dispatch_store_command(
         _name,
         _args,
         {:json_set, key, path, value, flags},
         _store,
         ctx,
         namespace
       ) do
    Router.json_set(ctx, namespace_key(namespace, key), path, value, flags)
  end

  defp dispatch_store_command(_name, _args, {:json_del, key, path}, _store, ctx, namespace) do
    Router.json_del(ctx, namespace_key(namespace, key), path)
  end

  defp dispatch_store_command(
         _name,
         _args,
         {:json_numincrby, key, path, increment},
         _store,
         ctx,
         namespace
       ) do
    Router.json_numincrby(ctx, namespace_key(namespace, key), path, increment)
  end

  defp dispatch_store_command(
         _name,
         _args,
         {:json_arrappend, key, path, values},
         _store,
         ctx,
         namespace
       ) do
    Router.json_arrappend(ctx, namespace_key(namespace, key), path, values)
  end

  defp dispatch_store_command(_name, _args, {:json_toggle, key, path}, _store, ctx, namespace) do
    Router.json_toggle(ctx, namespace_key(namespace, key), path)
  end

  defp dispatch_store_command(_name, _args, {:json_clear, key, path}, _store, ctx, namespace) do
    Router.json_clear(ctx, namespace_key(namespace, key), path)
  end

  defp dispatch_store_command(name, args, ast, store, _ctx, _namespace)
       when is_tuple(ast) and tuple_size(ast) in 2..6 do
    case Dispatcher.dispatch_ast(ast, store) do
      {:error, "ERR unsupported command AST"} ->
        {:error,
         "ERR unsupported command AST for '#{String.downcase(name)}' command with #{length(args)} args"}

      result ->
        result
    end
  end

  defp dispatch_store_command(_name, _args, ast, store, _ctx, _namespace) when ast in ~w(ping)a,
    do: Dispatcher.dispatch_ast(ast, store)

  defp dispatch_store_command(name, args, _ast, _store, _ctx, _namespace) do
    {:error,
     "ERR unsupported command AST for '#{String.downcase(name)}' command with #{length(args)} args"}
  end

  defp namespace_key(nil, key), do: key
  defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key

  defp namespace_keys(nil, keys), do: keys

  defp namespace_keys(namespace, keys) when is_binary(namespace),
    do: Enum.map(keys, &(namespace <> &1))

  defp namespace_kv_pairs(nil, kv_pairs), do: kv_pairs

  defp namespace_kv_pairs(namespace, kv_pairs) when is_binary(namespace),
    do: Enum.map(kv_pairs, fn {key, value} -> {namespace <> key, value} end)

  defp safe_dispatch(fun) do
    {:ok, fun.()}
  catch
    :exit, {:noproc, _} ->
      {:error, {:error, "ERR server not ready, shard process unavailable"}}

    :exit, {reason, _} ->
      {:error, {:error, "ERR internal error: #{inspect(reason)}"}}

    kind, reason ->
      {:error, internal_error(kind, reason)}
  end

  defp internal_error(kind, reason),
    do: {:error, "ERR internal error: #{inspect({kind, reason})}"}

  defp extract_command_name({:command, name, _args, _ast, _keys}) when is_binary(name), do: name
  defp extract_command_name(_), do: "UNKNOWN"

  defp full_acl_fast_path?(:full_access), do: true

  defp full_acl_fast_path?(%{
         commands: :all,
         keys: :all,
         enabled: true,
         denied_commands: %MapSet{map: denied}
       })
       when map_size(denied) == 0,
       do: true

  defp full_acl_fast_path?(_cache), do: false

  defp requires_auth?(state) do
    not state.authenticated and state.require_auth
  end

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
