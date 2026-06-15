defmodule FerricstoreServer.Native.Lane do
  @moduledoc """
  Bounded ordered execution lane for native protocol multiplexing.

  A lane is one lightweight process per active logical stream. It preserves
  command order within the lane while different lanes can run concurrently.
  This gives Cassandra-style multiplexing without spawning an unbounded Task per
  frame.
  """

  alias Ferricstore.{LatencyTrace, Stats}
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Native.{Codec, Commands}

  @flag_trace 0x01
  @flag_custom_payload 0x02
  @flag_no_reply 0x10
  @op_get 0x0101
  @op_set 0x0102
  @op_mget 0x0104

  @spec start_link(pid(), non_neg_integer(), map()) :: {:ok, pid()}
  def start_link(owner, lane_id, command_state)
      when is_pid(owner) and is_integer(lane_id) and is_map(command_state) do
    pid = spawn_link(__MODULE__, :loop, [owner, lane_id, command_state])
    {:ok, pid}
  end

  @spec enqueue(pid(), term()) :: :ok
  def enqueue(pid, frame) do
    send(pid, {:native_lane_frame, frame})
    :ok
  end

  @spec enqueue_many(pid(), [term()]) :: :ok
  def enqueue_many(_pid, []), do: :ok

  def enqueue_many(pid, [frame]) do
    enqueue(pid, frame)
  end

  def enqueue_many(pid, frames) when is_pid(pid) and is_list(frames) do
    send(pid, {:native_lane_frames, frames})
    :ok
  end

  @spec stop(pid()) :: :ok
  def stop(pid) when is_pid(pid) do
    Process.unlink(pid)
    Process.exit(pid, :shutdown)
    :ok
  end

  @spec update_command_state(pid(), map()) :: :ok
  def update_command_state(pid, command_state) when is_pid(pid) and is_map(command_state) do
    send(pid, {:native_lane_command_state, command_state})
    :ok
  end

  @doc false
  def loop(owner, lane_id, command_state) do
    receive do
      {:native_lane_frame, frame} ->
        response = execute_frame(frame, command_state)

        case response do
          :noreply -> send(owner, {:native_lane_done, lane_id})
          iodata -> send(owner, {:native_lane_response, lane_id, iodata})
        end

        loop(owner, lane_id, command_state)

      {:native_lane_frames, frames} ->
        {responses, done_count} = execute_frames(frames, command_state)

        case responses do
          [] -> send(owner, {:native_lane_done_many, lane_id, done_count})
          _ -> send(owner, {:native_lane_responses, lane_id, responses, done_count})
        end

        loop(owner, lane_id, command_state)

      {:native_lane_command_state, command_state} ->
        loop(owner, lane_id, command_state)

      :shutdown ->
        :ok
    end
  end

  defp execute_frames(frames, command_state) do
    case try_compact_mget_batch(frames, command_state) do
      {:ok, responses} ->
        {responses, length(frames)}

      :fallback ->
        case try_plain_get_batch(frames, command_state) do
          {:ok, responses} ->
            {responses, length(frames)}

          :fallback ->
            case try_plain_set_batch(frames, command_state) do
              {:ok, responses} ->
                {responses, length(frames)}

              :fallback ->
                {responses, done_count} =
                  Enum.reduce(frames, {[], 0}, fn frame, {responses, done_count} ->
                    case execute_frame(frame, command_state) do
                      :noreply -> {responses, done_count + 1}
                      iodata -> {[iodata | responses], done_count + 1}
                    end
                  end)

                {Enum.reverse(responses), done_count}
            end
        end
    end
  end

  defp try_compact_mget_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         {:ok, requests, keys} <- extract_compact_mget_frames(frames, [], []) do
      Stats.incr_commands_by(command_state.stats_counter, length(requests))

      responses =
        command_state.instance_ctx
        |> Router.batch_get(keys)
        |> encode_compact_mget_batch_responses(requests, command_state)

      {:ok, responses}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp extract_compact_mget_frames([], requests, key_chunks) do
    {:ok, Enum.reverse(requests), key_chunks |> Enum.reverse() |> List.flatten()}
  end

  defp extract_compact_mget_frames(
         [{lane_id, @op_mget, request_id, @flag_custom_payload, body} | rest],
         requests,
         key_chunks
       ) do
    case Codec.decode_body(@op_mget, @flag_custom_payload, body) do
      {:ok, %{"keys" => keys}} when is_list(keys) ->
        extract_compact_mget_frames(
          rest,
          [{lane_id, request_id, length(keys)} | requests],
          [keys | key_chunks]
        )

      _ ->
        :fallback
    end
  end

  defp extract_compact_mget_frames(_frames, _requests, _key_chunks), do: :fallback

  defp encode_compact_mget_batch_responses(values, requests, command_state) do
    {responses, []} =
      Enum.map_reduce(requests, values, fn {lane_id, request_id, count}, remaining ->
        {frame_values, rest} = Enum.split(remaining, count)

        response =
          Codec.encode_command_response_frames(@op_mget, lane_id, request_id, :ok, frame_values,
            compression: Map.get(command_state, :compression, :none),
            compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
            chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0)
          )

        {response, rest}
      end)

    responses
  end

  defp try_plain_get_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         {:ok, requests, keys} <- extract_plain_get_frames(frames, [], []) do
      Stats.incr_commands_by(command_state.stats_counter, length(keys))

      responses =
        command_state.instance_ctx
        |> Router.batch_get(keys)
        |> encode_plain_get_batch_responses(requests, command_state)

      {:ok, responses}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp extract_plain_get_frames([], requests, keys),
    do: {:ok, Enum.reverse(requests), Enum.reverse(keys)}

  defp extract_plain_get_frames(
         [{lane_id, opcode, request_id, 0, body} | rest],
         requests,
         keys
       )
       when opcode == @op_get do
    case Codec.decode_body(opcode, 0, body) do
      {:ok, %{"key" => key} = payload} when is_binary(key) ->
        if map_size(payload) == 1 do
          extract_plain_get_frames(rest, [{opcode, lane_id, request_id} | requests], [key | keys])
        else
          :fallback
        end

      _ ->
        :fallback
    end
  end

  defp extract_plain_get_frames(_frames, _requests, _keys), do: :fallback

  defp encode_plain_get_batch_responses(values, requests, command_state) do
    requests
    |> Enum.zip(values)
    |> Enum.map(fn {{opcode, lane_id, request_id}, value} ->
      Codec.encode_command_response_frames(opcode, lane_id, request_id, :ok, value,
        compression: Map.get(command_state, :compression, :none),
        compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
        chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0)
      )
    end)
  end

  defp try_plain_set_batch(frames, command_state) do
    with true <- plain_set_batch_allowed?(command_state),
         true <- pressure_ok?(command_state),
         {:ok, requests, kv_pairs} <- extract_plain_set_frames(frames, [], []) do
      Stats.incr_commands_by(command_state.stats_counter, length(kv_pairs))

      responses =
        command_state.instance_ctx
        |> Router.batch_quorum_put(kv_pairs)
        |> encode_plain_set_batch_responses(requests, command_state)

      {:ok, responses}
    else
      _ -> :fallback
    end
  rescue
    _ -> :fallback
  end

  defp plain_set_batch_allowed?(%{
         acl_cache: :full_access,
         require_auth: false,
         instance_ctx: ctx
       })
       when not is_nil(ctx),
       do: true

  defp plain_set_batch_allowed?(_command_state), do: false

  defp pressure_ok?(%{instance_ctx: %{pressure_flags: flags}}) when not is_nil(flags) do
    :atomics.get(flags, 1) == 0 and :atomics.get(flags, 2) == 0
  end

  defp pressure_ok?(_command_state), do: true

  defp extract_plain_set_frames([], requests, kv_pairs),
    do: {:ok, Enum.reverse(requests), Enum.reverse(kv_pairs)}

  defp extract_plain_set_frames(
         [{lane_id, @op_set, request_id, 0, body} | rest],
         requests,
         kv_pairs
       ) do
    case Codec.decode_body(@op_set, 0, body) do
      {:ok, %{"key" => key, "value" => value} = payload}
      when is_binary(key) and is_binary(value) ->
        if plain_set_payload?(payload) do
          extract_plain_set_frames(rest, [{lane_id, request_id} | requests], [
            {key, value} | kv_pairs
          ])
        else
          :fallback
        end

      _ ->
        :fallback
    end
  end

  defp extract_plain_set_frames(_frames, _requests, _kv_pairs), do: :fallback

  defp plain_set_payload?(payload) when is_map(payload) do
    not (Map.has_key?(payload, "ttl") or Map.has_key?(payload, "nx") or
           Map.has_key?(payload, "xx") or Map.has_key?(payload, "get") or
           Map.has_key?(payload, "keepttl") or Map.has_key?(payload, "exat") or
           Map.has_key?(payload, "pxat") or Map.has_key?(payload, "deadline_ms"))
  end

  defp encode_plain_set_batch_responses(results, requests, command_state) do
    requests
    |> Enum.zip(results)
    |> Enum.map(fn {{lane_id, request_id}, result} ->
      {status, value} = plain_set_response(result)

      Codec.encode_command_response_frames(@op_set, lane_id, request_id, status, value,
        compression: Map.get(command_state, :compression, :none),
        compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
        chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0)
      )
    end)
  end

  defp plain_set_response(:ok), do: {:ok, "OK"}
  defp plain_set_response({:ok, :ok}), do: {:ok, "OK"}
  defp plain_set_response({:ok, value}), do: {:ok, value}

  defp plain_set_response({:error, reason}) when is_binary(reason) do
    status =
      cond do
        String.starts_with?(reason, "BUSY") -> :busy
        String.starts_with?(reason, "OOM") -> :busy
        true -> :error
      end

    {status, reason}
  end

  defp plain_set_response({:error, reason}), do: {:error, inspect(reason)}
  defp plain_set_response(value), do: {:ok, value}

  defp execute_frame({:native_trace, frame, trace}, command_state) do
    response = execute_frame_with_response({:native_trace, frame, trace}, command_state)

    if no_reply?(frame) do
      :noreply
    else
      response
    end
  end

  defp execute_frame(frame, command_state) do
    response =
      execute_frame_with_response(frame, command_state)

    if no_reply?(frame) do
      :noreply
    else
      response
    end
  end

  defp execute_frame_with_response(
         {:native_trace, {lane_id, opcode, request_id, flags, body}, trace},
         command_state
       ) do
    queue_done_us = monotonic_us()

    trace =
      put_trace_duration(
        trace,
        "server_lane_queue_wait_us",
        queue_done_us,
        "server_lane_enqueue_us"
      )

    decode_started_us = monotonic_us()

    case Codec.decode_body(opcode, flags, body) do
      {:ok, payload} ->
        decode_done_us = monotonic_us()
        Commands.mark_command_seen(command_state)

        {status, value, execute_started_us, execute_done_us, trace} =
          execute_traced_command(opcode, payload, command_state, trace)

        trace =
          trace
          |> put_duration("server_body_decode_us", decode_started_us, decode_done_us)
          |> put_duration("server_command_execute_us", execute_started_us, execute_done_us)

        encode_traced_response(
          opcode,
          lane_id,
          request_id,
          status,
          value,
          trace,
          command_state
        )

      {:error, reason} ->
        decode_done_us = monotonic_us()

        trace =
          put_duration(trace, "server_body_decode_us", decode_started_us, decode_done_us)

        encode_traced_response(
          opcode,
          lane_id,
          request_id,
          :bad_request,
          reason,
          trace,
          command_state
        )
    end
  end

  defp execute_frame_with_response({lane_id, opcode, request_id, flags, body}, command_state) do
    case Codec.decode_body(opcode, flags, body) do
      {:ok, payload} ->
        Commands.mark_command_seen(command_state)
        {status, value, _state} = Commands.execute(opcode, payload, command_state)

        Codec.encode_command_response_frames(opcode, lane_id, request_id, status, value,
          compression: Map.get(command_state, :compression, :none),
          compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
          chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0)
        )

      {:error, reason} ->
        Codec.encode_command_response_frames(opcode, lane_id, request_id, :bad_request, reason,
          compression: Map.get(command_state, :compression, :none),
          compact_flow_responses: Map.get(command_state, :compact_flow_responses, false),
          chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0)
        )
    end
  end

  defp encode_traced_response(opcode, lane_id, request_id, status, value, trace, command_state) do
    encode_started_us = monotonic_us()

    _measurement_frames =
      encode_trace_frames(opcode, lane_id, request_id, status, value, trace, command_state)

    encode_done_us = monotonic_us()
    trace = put_duration(trace, "server_response_encode_us", encode_started_us, encode_done_us)

    encode_trace_frames(opcode, lane_id, request_id, status, value, trace, command_state)
  end

  defp encode_trace_frames(opcode, lane_id, request_id, status, value, trace, command_state) do
    Codec.encode_command_response_frames(
      opcode,
      lane_id,
      request_id,
      status,
      %{"value" => value, "trace" => public_trace(trace)},
      compression: Map.get(command_state, :compression, :none),
      compact_flow_responses: false,
      chunk_bytes: Map.get(command_state, :response_chunk_bytes, 0),
      flags: @flag_trace
    )
  end

  defp no_reply?({_lane_id, _opcode, _request_id, flags, _body}),
    do: Bitwise.band(flags, @flag_no_reply) != 0

  defp execute_traced_command(opcode, payload, command_state, trace) do
    previous_trace = LatencyTrace.start(trace)

    try do
      execute_started_us = monotonic_us()
      {status, value, _state} = Commands.execute(opcode, payload, command_state)
      execute_done_us = monotonic_us()
      trace = LatencyTrace.finish(previous_trace)
      {status, value, execute_started_us, execute_done_us, trace}
    rescue
      error ->
        _ = LatencyTrace.finish(previous_trace)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        _ = LatencyTrace.finish(previous_trace)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp put_trace_duration(trace, key, now_us, source_key) do
    case Map.get(trace, source_key) do
      started_us when is_integer(started_us) -> Map.put(trace, key, max(now_us - started_us, 0))
      _ -> trace
    end
  end

  defp put_duration(trace, key, started_us, done_us),
    do: Map.put(trace, key, max(done_us - started_us, 0))

  defp public_trace(trace), do: Map.delete(trace, "server_lane_enqueue_us")

  defp monotonic_us, do: System.monotonic_time(:microsecond)
end
