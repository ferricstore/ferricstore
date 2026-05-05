defmodule FerricstoreServer.Connection.Pipeline do
  @moduledoc "Pipeline dispatcher with batch fast paths for GET, SET, and mixed GET+SET workloads."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.Flow, as: FlowCommand
  alias Ferricstore.Stats
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

  # Maximum commands in a single pipeline batch (100K).
  @max_pipeline_size 100_000

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
            case try_batch_flow_create_fast_path(commands, state, send_response_fn) do
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
          acl_ok =
            state.acl_cache == :full_access or
              (is_map(state.acl_cache) and state.acl_cache.commands == :all and
                 state.acl_cache.keys == :all)

          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          if not acl_ok do
            :fallback
          else
            Stats.incr_commands_by(state.stats_counter, length(keys))
            lookup_keys = namespace_keys(state.sandbox_namespace, keys)

            {:ok, dispatch_batch_get_results(keys, lookup_keys, state, send_response_fn)}
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
           Router.batch_get_with_file_refs(
             state.instance_ctx,
             lookup_keys,
             ConnSendfile.threshold_bytes()
           )
         end) do
      {:ok, results} ->
        if Enum.any?(results, &match?({:file_ref, _, _, _}, &1)) do
          stream_get_results(keys, lookup_keys, results, state, send_response_fn)
        else
          send_response_fn.(state.socket, state.transport, Enum.map(results, &Encoder.encode/1))
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
        send_response_fn.(state.socket, state.transport, Enum.map(values, &Encoder.encode/1))
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

  defp stream_get_results(keys, lookup_keys, results, state, send_response_fn) do
    TcpOpts.set_cork(state.socket, true)

    result =
      keys
      |> Enum.zip(lookup_keys)
      |> Enum.zip(results)
      |> Enum.reduce_while({:continue, state}, fn
        {{key, _lookup_key}, {:file_ref, path, offset, size}}, {:continue, acc_state} ->
          case ConnSendfile.send_file_ref_response(key, path, offset, size, acc_state) do
            {:sent, new_state} ->
              {:cont, {:continue, new_state}}

            :fallback ->
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

              {:cont, {:continue, ConnTracking.maybe_track_read("GET", [key], value, acc_state)}}

            {:error_after_header, _reason} ->
              {:halt, {:quit, acc_state}}
          end

        {{key, _lookup_key}, value}, {:continue, acc_state} ->
          send_response_fn.(acc_state.socket, acc_state.transport, Encoder.encode(value))

          {:cont, {:continue, ConnTracking.maybe_track_read("GET", [key], value, acc_state)}}
      end)

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

          acl_ok =
            state.acl_cache == :full_access or
              (is_map(state.acl_cache) and state.acl_cache.commands == :all and
                 state.acl_cache.keys == :all)

          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          if not acl_ok do
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
       when is_binary(key) and is_binary(value),
       do: extract_plain_sets(rest, [{key, value} | acc])

  defp extract_plain_sets(_, _acc), do: :fallback

  defp do_batch_set_quorum(kv_pairs, state, send_response_fn) do
    Stats.incr_commands_by(state.stats_counter, length(kv_pairs))

    response =
      case safe_dispatch(fn -> Router.batch_quorum_put(state.instance_ctx, kv_pairs) end) do
        {:ok, results} ->
          Enum.map(results, fn
            :ok -> Encoder.ok_response()
            {:error, _} = err -> Encoder.encode(err)
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
      acl_ok =
        state.acl_cache == :full_access or
          (is_map(state.acl_cache) and state.acl_cache.commands == :all and
             state.acl_cache.keys == :all)

      # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
      if not acl_ok do
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
    do: classify_mixed_pipeline(commands, [], 0, MapSet.new())

  defp classify_mixed_pipeline([], acc, _idx, _written_keys), do: {:ok, Enum.reverse(acc)}

  defp classify_mixed_pipeline(
         [{:command, "GET", [key], {:get, key}, [key]} | rest],
         acc,
         idx,
         written_keys
       )
       when is_binary(key) do
    if MapSet.member?(written_keys, key) do
      :fallback
    else
      classify_mixed_pipeline(rest, [{:get, idx, key} | acc], idx + 1, written_keys)
    end
  end

  defp classify_mixed_pipeline(
         [{:command, "SET", [key, value], {:set, key, value}, [key]} | rest],
         acc,
         idx,
         written_keys
       )
       when is_binary(key) and is_binary(value),
       do:
         classify_mixed_pipeline(
           rest,
           [{:set, idx, key, value} | acc],
           idx + 1,
           MapSet.put(written_keys, key)
         )

  defp classify_mixed_pipeline(_, _, _, _), do: :fallback

  defp do_mixed_fast_path(ops, state, send_response_fn) do
    ctx = state.instance_ctx
    count = length(ops)
    Stats.incr_commands_by(state.stats_counter, count)

    get_ops = for {:get, idx, key} <- ops, do: {idx, key}

    set_ops =
      state.sandbox_namespace
      |> namespace_set_ops(for {:set, idx, key, value} <- ops, do: {idx, key, value})

    # Execute SETs first so subsequent GETs in the same pipeline see the new values.
    set_results =
      case set_ops do
        [] ->
          %{}

        _ ->
          kv_pairs = Enum.map(set_ops, fn {_idx, key, value} -> {key, value} end)

          pressure_ok =
            :atomics.get(ctx.pressure_flags, 1) == 0 and
              :atomics.get(ctx.pressure_flags, 2) == 0

          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          results =
            if not pressure_ok do
              List.duplicate({:error, "ERR server under pressure"}, length(kv_pairs))
            else
              case safe_dispatch(fn -> mixed_set_results(ctx, kv_pairs) end) do
                {:ok, results} -> results
                {:error, err} -> List.duplicate(err, length(kv_pairs))
              end
            end

          set_ops
          |> Enum.zip(results)
          |> Map.new(fn {{idx, _key, _value}, result} ->
            encoded =
              case result do
                :ok -> Encoder.ok_response()
                {:error, _} = err -> Encoder.encode(err)
              end

            {idx, encoded}
          end)
      end

    get_results =
      case get_ops do
        [] ->
          %{}

        _ ->
          keys = Enum.map(get_ops, &elem(&1, 1))
          lookup_keys = namespace_keys(state.sandbox_namespace, keys)

          get_dispatch =
            if state.transport in [:ranch_tcp, :ranch_ssl] do
              fn ->
                Router.batch_get_with_file_refs(ctx, lookup_keys, ConnSendfile.threshold_bytes())
              end
            else
              fn -> Router.batch_get(ctx, lookup_keys) end
            end

          case safe_dispatch(get_dispatch) do
            {:ok, values} ->
              get_ops
              |> Enum.zip(lookup_keys)
              |> Enum.zip(values)
              |> Map.new(fn
                {{{idx, key}, lookup_key}, {:file_ref, path, offset, size}} ->
                  {idx, {:file_ref, key, lookup_key, path, offset, size}}

                {{{idx, key}, _lookup_key}, value} ->
                  {idx, {:get_encoded, key, Encoder.encode(value), value}}
              end)

            {:error, err} ->
              Map.new(get_ops, fn {idx, _key} -> {idx, Encoder.encode(err)} end)
          end
      end

    if map_has_file_ref?(get_results) do
      stream_indexed_results(count, get_results, set_results, state, send_response_fn)
    else
      response =
        for i <- 0..(count - 1),
            do: response_iodata(Map.get(get_results, i) || Map.get(set_results, i))

      send_response_fn.(state.socket, state.transport, response)
      {:continue, track_mixed_get_results(get_results, state)}
    end
  end

  defp map_has_file_ref?(results) do
    Enum.any?(results, fn {_idx, result} ->
      match?({:file_ref, _key, _lookup_key, _path, _offset, _size}, result)
    end)
  end

  defp response_iodata({:get_encoded, _key, encoded, _value}), do: encoded
  defp response_iodata(encoded), do: encoded

  defp track_mixed_get_results(results, state) do
    Enum.reduce(results, state, fn
      {_idx, {:get_encoded, key, _encoded, value}}, acc_state ->
        ConnTracking.maybe_track_read("GET", [key], value, acc_state)

      _other, acc_state ->
        acc_state
    end)
  end

  defp stream_indexed_results(count, primary_results, fallback_results, state, send_response_fn) do
    TcpOpts.set_cork(state.socket, true)

    result =
      Enum.reduce_while(0..(count - 1), {:continue, state}, fn idx, {:continue, acc_state} ->
        case Map.get(primary_results, idx) || Map.get(fallback_results, idx) do
          {:file_ref, key, lookup_key, path, offset, size} ->
            case ConnSendfile.send_file_ref_response(key, path, offset, size, acc_state) do
              {:sent, new_state} ->
                {:cont, {:continue, new_state}}

              :fallback ->
                send_response_fn.(
                  acc_state.socket,
                  acc_state.transport,
                  Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
                )

                {:cont,
                 {:continue, ConnTracking.maybe_track_read("GET", [key], :fallback_ok, acc_state)}}

              {:error_after_header, _reason} ->
                {:halt, {:quit, acc_state}}
            end

          {:get_encoded, key, encoded, value} ->
            send_response_fn.(acc_state.socket, acc_state.transport, encoded)

            {:cont, {:continue, ConnTracking.maybe_track_read("GET", [key], value, acc_state)}}

          encoded ->
            send_response_fn.(acc_state.socket, acc_state.transport, encoded)
            {:cont, {:continue, acc_state}}
        end
      end)

    TcpOpts.set_cork(state.socket, false)
    result
  end

  defp mixed_set_results(ctx, kv_pairs) do
    Router.batch_quorum_put(ctx, kv_pairs)
  end

  # ---------------------------------------------------------------------------
  # Batch FLOW.CREATE fast path
  # ---------------------------------------------------------------------------
  # Pipelined one-by-one creates keep per-command semantics, but can still share
  # one Raft batch. This is intentionally not FLOW.CREATE_MANY: one duplicate
  # fails only that command, while surrounding creates still commit.

  defp try_batch_flow_create_fast_path(commands, state, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing do
      :fallback
    else
      acl_ok =
        state.acl_cache == :full_access or
          (is_map(state.acl_cache) and state.acl_cache.commands == :all and
             state.acl_cache.keys == :all)

      if acl_ok do
        case extract_flow_creates(commands) do
          {:ok, creates} ->
            Stats.incr_commands_by(state.stats_counter, length(creates))

            response =
              state.instance_ctx
              |> Ferricstore.Flow.create_batch_independent(creates)
              |> Enum.map(&encode_flow_result/1)

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

  defp extract_flow_creates(commands), do: extract_flow_creates(commands, [])

  defp extract_flow_creates([], acc), do: {:ok, Enum.reverse(acc)}

  defp extract_flow_creates(
         [{:command, "FLOW.CREATE", _args, {:flow_create, id, opts}, _keys} | rest],
         acc
       )
       when is_binary(id) and is_list(opts),
       do: extract_flow_creates(rest, [{id, opts} | acc])

  defp extract_flow_creates(_commands, _acc), do: :fallback

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
    acl_ok =
      state.acl_cache == :full_access or
        (is_map(state.acl_cache) and state.acl_cache.commands == :all and
           state.acl_cache.keys == :all)

    if requires_auth?(state) or state.multi_state == :queuing or not acl_ok do
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
    prefetched_reads = prefetch_pure_tcp_reads(commands, state)

    {action, responses} =
      commands
      |> Enum.with_index()
      |> Enum.reduce_while({:continue, []}, fn {cmd, idx}, {:continue, acc} ->
        {name, args, ast, keys} = command_parts(cmd)
        Stats.incr_commands(state.stats_counter)

        result =
          case ConnAuth.check_command_cached(state.acl_cache, name) do
            :ok ->
              case ConnAuth.check_keys_cached(state.acl_cache, name, keys) do
                :ok ->
                  try do
                    result =
                      dispatch_pure_command(idx, name, args, ast, store, state, prefetched_reads)

                    unless streaming_response?(result) do
                      ConnTracking.maybe_notify_keyspace(name, args, result)
                      ConnTracking.maybe_notify_tracking(name, args, result, state)
                    end

                    result
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

        {:cont, {:continue, [response_entry(result) | acc]}}
      end)

    responses = Enum.reverse(responses)

    if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, true)

    result =
      if Enum.any?(responses, &streaming_response?/1) do
        stream_response_entries(responses, state, send_response_fn)
      else
        send_response_fn.(
          state.socket,
          state.transport,
          Enum.map(responses, fn {:encoded, encoded} -> encoded end)
        )

        {action, state}
      end

    if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, false)
    result
  end

  defp prefetch_pure_tcp_reads(commands, %{transport: transport} = state)
       when transport in [:ranch_tcp, :ranch_ssl] do
    {gets, mgets, getranges, _written_keys} =
      commands
      |> Enum.with_index()
      |> Enum.reduce({[], [], [], MapSet.new()}, fn {cmd, idx},
                                                    {get_acc, mget_acc, getrange_acc,
                                                     written_keys} ->
        case command_parts(cmd) do
          {"GET", [key], {:get, key}, [key]} when is_binary(key) ->
            if MapSet.member?(written_keys, key) do
              {get_acc, mget_acc, getrange_acc, written_keys}
            else
              {[{idx, key, namespace_key(state.sandbox_namespace, key)} | get_acc], mget_acc,
               getrange_acc, written_keys}
            end

          {"MGET", keys, {:mget, keys}, keys} when is_list(keys) and keys != [] ->
            if any_written_key?(keys, written_keys) do
              {get_acc, mget_acc, getrange_acc, written_keys}
            else
              lookup_keys = namespace_keys(state.sandbox_namespace, keys)
              {get_acc, [{idx, keys, lookup_keys} | mget_acc], getrange_acc, written_keys}
            end

          {"GETRANGE", [key, _start_arg, _end_arg] = args, {:getrange, key, start_idx, end_idx},
           [key]}
          when is_binary(key) and is_integer(start_idx) and is_integer(end_idx) ->
            if MapSet.member?(written_keys, key) do
              {get_acc, mget_acc, getrange_acc, written_keys}
            else
              {get_acc, mget_acc, [{idx, args, key, start_idx, end_idx} | getrange_acc],
               written_keys}
            end

          {name, _args, _ast, keys} when is_list(keys) and keys != [] ->
            if read_only_keyed_command?(name) do
              {get_acc, mget_acc, getrange_acc, written_keys}
            else
              {get_acc, mget_acc, getrange_acc, mark_written_keys(written_keys, keys)}
            end

          _ ->
            {get_acc, mget_acc, getrange_acc, written_keys}
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

  defp prefetch_tcp_get_ops(gets, state) do
    case gets do
      [] ->
        %{}

      _ ->
        lookup_keys = Enum.map(gets, fn {_idx, _key, lookup_key} -> lookup_key end)

        case safe_dispatch(fn ->
               Router.batch_get_with_file_refs(
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
           Router.batch_get_with_file_refs(
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
         {:file_range, _args, _key, _start_idx, _end_idx, _path, _offset, _size} = file_range
       ),
       do: file_range

  defp response_entry({:array, _keys, _elements} = array), do: array
  defp response_entry({:value, value}), do: {:encoded, Encoder.encode(value)}

  defp response_entry(result), do: {:encoded, Encoder.encode(result)}

  defp streaming_response?({:file_ref, _key, _lookup_key, _path, _offset, _size}), do: true

  defp streaming_response?(
         {:file_range, _args, _key, _start_idx, _end_idx, _path, _offset, _size}
       ),
       do: true

  defp streaming_response?({:array, _keys, _elements}), do: true
  defp streaming_response?(_result), do: false

  defp stream_response_entries(entries, state, send_response_fn) do
    Enum.reduce_while(entries, {:continue, state}, fn
      {:array, keys, elements}, {:continue, acc_state} ->
        case stream_array_response(keys, elements, acc_state, send_response_fn) do
          {:sent, new_state} ->
            {:cont, {:continue, new_state}}

          {:error_after_header, _reason} ->
            {:halt, {:quit, acc_state}}
        end

      {:file_range, args, key, start_idx, end_idx, path, offset, size}, {:continue, acc_state} ->
        case ConnSendfile.send_file_range_response(args, path, offset, size, acc_state) do
          {:sent, new_state} ->
            {:cont, {:continue, new_state}}

          :fallback ->
            send_response_fn.(
              acc_state.socket,
              acc_state.transport,
              Encoder.encode(
                ConnSendfile.materialize_getrange(key, start_idx, end_idx, acc_state)
              )
            )

            {:cont, {:continue, acc_state}}

          {:error_after_header, _reason} ->
            {:halt, {:quit, acc_state}}
        end

      {:file_ref, key, lookup_key, path, offset, size}, {:continue, acc_state} ->
        case ConnSendfile.send_file_ref_response(key, path, offset, size, acc_state) do
          {:sent, new_state} ->
            {:cont, {:continue, new_state}}

          :fallback ->
            send_response_fn.(
              acc_state.socket,
              acc_state.transport,
              Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
            )

            {:cont, {:continue, acc_state}}

          {:error_after_header, _reason} ->
            {:halt, {:quit, acc_state}}
        end

      {:encoded, encoded}, {:continue, acc_state} ->
        send_response_fn.(acc_state.socket, acc_state.transport, encoded)
        {:cont, {:continue, acc_state}}
    end)
  end

  defp stream_array_response(keys, elements, state, send_response_fn) do
    send_response_fn.(state.socket, state.transport, [
      "*",
      Integer.to_string(length(elements)),
      "\r\n"
    ])

    elements
    |> Enum.reduce_while({:sent, state}, fn
      {:file_ref, key, lookup_key, path, offset, size}, {:sent, acc_state} ->
        case ConnSendfile.send_file_ref_element_response(key, path, offset, size, acc_state) do
          {:sent, new_state} ->
            {:cont, {:sent, new_state}}

          :fallback ->
            send_response_fn.(
              acc_state.socket,
              acc_state.transport,
              Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
            )

            {:cont, {:sent, acc_state}}

          {:error_after_header, _reason} = error ->
            {:halt, error}
        end

      {:encoded, encoded}, {:sent, acc_state} ->
        send_response_fn.(acc_state.socket, acc_state.transport, encoded)
        {:cont, {:sent, acc_state}}
    end)
    |> case do
      {:sent, new_state} ->
        {:sent, ConnTracking.maybe_track_read("MGET", keys, :sendfile_ok, new_state)}

      other ->
        other
    end
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
       when is_tuple(ast) and tuple_size(ast) in 2..5 do
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

  defp namespace_set_ops(nil, set_ops), do: set_ops

  defp namespace_set_ops(namespace, set_ops) when is_binary(namespace),
    do: Enum.map(set_ops, fn {idx, key, value} -> {idx, namespace <> key, value} end)

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

  defp requires_auth?(state) do
    not state.authenticated and state.require_auth
  end

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
