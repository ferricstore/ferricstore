defmodule FerricstoreServer.Connection.Pipeline do
  @moduledoc "Pipeline dispatcher with batch fast paths for GET, SET, and mixed GET+SET workloads."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.TcpOpts
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  @stateful_cmds MapSet.new(~w(
    HELLO CLIENT QUIT AUTH ACL RESET SANDBOX
    MULTI EXEC DISCARD WATCH UNWATCH
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
    BLPOP BRPOP BLMOVE BLMPOP
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
          {:ok, result} -> result
          :fallback -> try_mixed_fast_path(commands, state, handle_command_fn, send_response_fn)
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
            ctx = state.instance_ctx
            Stats.incr_commands_by(state.stats_counter, length(keys))

            response =
              case safe_dispatch(fn -> Router.batch_get(ctx, keys) end) do
                {:ok, values} -> Enum.map(values, &Encoder.encode/1)
                {:error, err} -> List.duplicate(Encoder.encode(err), length(keys))
              end

            send_response_fn.(state.socket, state.transport, response)
            {:ok, {:continue, state}}
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

  # ---------------------------------------------------------------------------
  # Batch SET fast path
  # ---------------------------------------------------------------------------
  # When ALL commands in a pipeline batch are plain `SET key value` (no options),
  # bypass the normal sliding_window_dispatch which serializes each SET.
  #
  # Async: batch ETS inserts, reply OK immediately.
  # Quorum: submit all to Batcher(s) concurrently, wait for all Ra commits,
  #         then send all responses. This lets all SETs share the same Batcher
  #         batch + WAL fdatasync instead of serializing through it.

  defp try_batch_set_fast_path(commands, state, send_response_fn) do
    if requires_auth?(state) or state.multi_state == :queuing do
      :fallback
    else
      case extract_plain_sets(commands) do
        {:ok, kv_pairs} ->
          ctx = state.instance_ctx
          live_ctx = FerricStore.Instance.get(:default)

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
              case live_ctx.durability_mode do
                :all_async ->
                  {:ok, do_batch_set_async(kv_pairs, state, send_response_fn)}

                :all_quorum ->
                  {:ok, do_batch_set_quorum(kv_pairs, state, send_response_fn)}

                :mixed ->
                  case classify_batch_durability(ctx, kv_pairs) do
                    :all_async ->
                      {:ok, do_batch_set_async(kv_pairs, state, send_response_fn)}

                    :all_quorum ->
                      {:ok, do_batch_set_quorum(kv_pairs, state, send_response_fn)}

                    :mixed ->
                      :fallback
                  end
              end
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

  defp classify_batch_durability(_ctx, []), do: :all_quorum

  defp classify_batch_durability(ctx, [{first_key, _} | rest]) do
    first = Router.durability_for_key_public(ctx, first_key)
    if all_same_durability?(ctx, rest, first), do: durability_to_class(first), else: :mixed
  end

  defp all_same_durability?(_ctx, [], _expected), do: true

  defp all_same_durability?(ctx, [{key, _} | rest], expected) do
    Router.durability_for_key_public(ctx, key) == expected and
      all_same_durability?(ctx, rest, expected)
  end

  defp durability_to_class(:async), do: :all_async
  defp durability_to_class(:quorum), do: :all_quorum

  defp do_batch_set_async(kv_pairs, state, send_response_fn) do
    Stats.incr_commands_by(state.stats_counter, length(kv_pairs))

    response =
      case safe_dispatch(fn -> Router.batch_async_put(state.instance_ctx, kv_pairs) end) do
        {:ok, result} -> encode_async_batch_result(result, length(kv_pairs))
        {:error, err} -> List.duplicate(Encoder.encode(err), length(kv_pairs))
      end

    send_response_fn.(state.socket, state.transport, response)
    {:continue, state}
  end

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
    set_ops = for {:set, idx, key, value} <- ops, do: {idx, key, value}

    # Execute SETs first so subsequent GETs in the same pipeline see the new values.
    set_results =
      case set_ops do
        [] ->
          %{}

        _ ->
          kv_pairs = Enum.map(set_ops, fn {_idx, key, value} -> {key, value} end)
          live_ctx = FerricStore.Instance.get(:default)

          pressure_ok =
            :atomics.get(ctx.pressure_flags, 1) == 0 and
              :atomics.get(ctx.pressure_flags, 2) == 0

          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          results =
            if not pressure_ok do
              List.duplicate({:error, "ERR server under pressure"}, length(kv_pairs))
            else
              case safe_dispatch(fn -> mixed_set_results(live_ctx, ctx, set_ops, kv_pairs) end) do
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

          case safe_dispatch(fn -> Router.batch_get(ctx, keys) end) do
            {:ok, values} ->
              get_ops
              |> Enum.zip(values)
              |> Map.new(fn {{idx, _key}, value} -> {idx, Encoder.encode(value)} end)

            {:error, err} ->
              Map.new(get_ops, fn {idx, _key} -> {idx, Encoder.encode(err)} end)
          end
      end

    response = for i <- 0..(count - 1), do: Map.get(get_results, i) || Map.get(set_results, i)
    send_response_fn.(state.socket, state.transport, response)
    {:continue, state}
  end

  defp mixed_set_results(%{durability_mode: :all_async}, ctx, _set_ops, kv_pairs) do
    ctx
    |> Router.batch_async_put(kv_pairs)
    |> expand_async_batch_result(length(kv_pairs))
  end

  defp mixed_set_results(%{durability_mode: :all_quorum}, ctx, _set_ops, kv_pairs) do
    Router.batch_quorum_put(ctx, kv_pairs)
  end

  defp mixed_set_results(%{durability_mode: :mixed}, ctx, set_ops, _kv_pairs) do
    {async_pairs, quorum_pairs, _} = split_by_durability(ctx, set_ops)

    async_results =
      if async_pairs != [] do
        kv_pairs = Enum.map(async_pairs, fn {_idx, k, v} -> {k, v} end)

        ctx
        |> Router.batch_async_put(kv_pairs)
        |> expand_async_batch_result(length(async_pairs))
      else
        []
      end

    quorum_results =
      if quorum_pairs != [] do
        Router.batch_quorum_put(
          ctx,
          Enum.map(quorum_pairs, fn {_idx, k, v} -> {k, v} end)
        )
      else
        []
      end

    recombine_set_results(async_pairs, async_results, quorum_pairs, quorum_results)
  end

  defp split_by_durability(ctx, set_ops) do
    {async_ops, quorum_ops} =
      Enum.split_with(set_ops, fn {_idx, key, _value} ->
        Router.durability_for_key_public(ctx, key) == :async
      end)

    {async_ops, quorum_ops, nil}
  end

  defp expand_async_batch_result(:ok, count), do: List.duplicate(:ok, count)
  defp expand_async_batch_result({:error, _} = err, count), do: List.duplicate(err, count)

  defp encode_async_batch_result(:ok, count), do: List.duplicate(Encoder.ok_response(), count)

  defp encode_async_batch_result({:error, _} = err, count),
    do: List.duplicate(Encoder.encode(err), count)

  defp recombine_set_results(async_ops, async_results, quorum_ops, quorum_results) do
    async_map =
      async_ops |> Enum.zip(async_results) |> Map.new(fn {{idx, _, _}, r} -> {idx, r} end)

    quorum_map =
      quorum_ops |> Enum.zip(quorum_results) |> Map.new(fn {{idx, _, _}, r} -> {idx, r} end)

    combined = Map.merge(async_map, quorum_map)

    (async_ops ++ quorum_ops)
    |> Enum.sort_by(fn {idx, _, _} -> idx end)
    |> Enum.map(fn {idx, _, _} -> Map.fetch!(combined, idx) end)
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

    {action, responses} =
      Enum.reduce_while(commands, {:continue, []}, fn cmd, {:continue, acc} ->
        {name, args, ast, keys} = command_parts(cmd)
        Stats.incr_commands(state.stats_counter)

        result =
          case ConnAuth.check_command_cached(state.acl_cache, name) do
            :ok ->
              case ConnAuth.check_keys_cached(state.acl_cache, name, keys) do
                :ok ->
                  try do
                    result = dispatch_store_command(name, args, ast, store)
                    ConnTracking.maybe_notify_keyspace(name, args, result)
                    ConnTracking.maybe_notify_tracking(name, args, result, state)
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

        {:cont, {:continue, [Encoder.encode(result) | acc]}}
      end)

    if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, true)
    send_response_fn.(state.socket, state.transport, Enum.reverse(responses))
    if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, false)

    {action, state}
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

  defp dispatch_store_command(name, args, ast, store)
       when is_tuple(ast) and tuple_size(ast) in 2..5 do
    case Dispatcher.dispatch_ast(ast, store) do
      {:error, "ERR unsupported command AST"} ->
        {:error,
         "ERR unsupported command AST for '#{String.downcase(name)}' command with #{length(args)} args"}

      result ->
        result
    end
  end

  defp dispatch_store_command(_name, _args, ast, store) when ast in ~w(ping)a,
    do: Dispatcher.dispatch_ast(ast, store)

  defp dispatch_store_command(name, args, _ast, _store) do
    {:error,
     "ERR unsupported command AST for '#{String.downcase(name)}' command with #{length(args)} args"}
  end

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
