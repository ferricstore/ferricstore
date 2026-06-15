defmodule FerricstoreServer.Native.Connection do
  @moduledoc """
  Ranch protocol handler for FerricStore native TCP clients.

  The native listener is a binary, request-id based data plane for SDKs that
  need routing hints, pipelined command execution, and structured Flow results
  without RESP parsing. It shares the same engine path as RESP; this module only
  handles network framing, ACL/protected-mode, client registry, and response
  coalescing.
  """

  @behaviour :ranch_protocol

  alias Ferricstore.Stats
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Native.{Codec, Commands, Lane}

  @max_buffer_size 134_217_728
  @op_goaway 0x000A
  @control_lane 0
  @flag_trace 0x01
  @flag_custom_payload 0x02
  @flag_compressed 0x08
  @flag_no_reply 0x10
  @flag_more_chunks 0x20
  @supported_request_flags Bitwise.bor(
                             Bitwise.bor(
                               Bitwise.bor(@flag_trace, @flag_custom_payload),
                               Bitwise.bor(@flag_no_reply, @flag_compressed)
                             ),
                             @flag_more_chunks
                           )

  defstruct [
    :socket,
    :transport,
    :client_id,
    :client_name,
    :created_at,
    :peer,
    :instance_ctx,
    :stats_counter,
    :max_frame_bytes,
    :max_lanes,
    :lane_max_queue,
    :max_inflight_per_connection,
    :max_inflight_per_lane,
    :response_chunk_bytes,
    :max_pending_chunks,
    :max_pending_chunk_bytes,
    :response_coalesce_max,
    :response_coalesce_bytes,
    :command_state,
    :idle_timeout_ms,
    compression: :none,
    buffer: "",
    lanes: %{},
    chunk_buffers: %{},
    pending_chunk_bytes: 0,
    inflight_total: 0,
    lane_inflight: %{},
    event_subscriptions: MapSet.new(),
    flow_wake_subscription: nil,
    authenticated: false,
    require_auth: false,
    compact_flow_responses: false,
    username: "default",
    acl_cache: nil,
    active_mode: 100,
    close_after_reply: false
  ]

  @impl true
  def start_link(ref, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  @doc false
  def init(ref, transport, opts) do
    {:ok, socket} = :ranch.handshake(ref)

    if transport == :ranch_tcp and require_tls?() do
      send_native_error(
        socket,
        transport,
        "ERR TLS required: plaintext connections are not permitted"
      )

      transport.close(socket)
    else
      active_mode = Application.get_env(:ferricstore, :socket_active_mode, 100)
      :ok = transport.setopts(socket, active: active_mode)

      Stats.incr_connections()

      peer =
        case transport.peername(socket) do
          {:ok, addr} -> addr
          _ -> nil
        end

      case FerricstoreServer.Acl.check_protected_mode(peer) do
        {:error, reason} ->
          send_native_error(socket, transport, reason)
          Stats.decr_connections()
          transport.close(socket)

        :ok ->
          if maxclients_exceeded?() do
            send_native_error(socket, transport, "ERR max number of clients reached")
            Stats.decr_connections()
            transport.close(socket)
          else
            ctx = FerricStore.Instance.get(:default)

            state =
              %__MODULE__{
                socket: socket,
                transport: transport,
                client_id: generate_client_id(),
                client_name: nil,
                created_at: System.monotonic_time(:millisecond),
                peer: peer,
                instance_ctx: ctx,
                stats_counter: ctx.stats_counter,
                require_auth: Commands.default_requires_auth?(),
                acl_cache: FerricstoreServer.Connection.Auth.build_acl_cache("default"),
                active_mode: active_mode,
                max_frame_bytes:
                  Map.get(opts, :max_frame_bytes) ||
                    Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024),
                max_lanes:
                  Map.get(opts, :max_lanes) ||
                    Application.get_env(:ferricstore, :native_max_lanes_per_connection, 1024),
                lane_max_queue:
                  Map.get(opts, :lane_max_queue) ||
                    Application.get_env(:ferricstore, :native_lane_max_queue, 1024),
                max_inflight_per_connection:
                  Application.get_env(:ferricstore, :native_max_inflight_per_connection, 4096),
                max_inflight_per_lane:
                  Application.get_env(:ferricstore, :native_max_inflight_per_lane, 1024),
                response_chunk_bytes:
                  Application.get_env(:ferricstore, :native_response_chunk_bytes, 0),
                max_pending_chunks:
                  Application.get_env(:ferricstore, :native_max_pending_chunks, 1024),
                max_pending_chunk_bytes:
                  Application.get_env(
                    :ferricstore,
                    :native_max_pending_chunk_bytes,
                    64 * 1024 * 1024
                  ),
                response_coalesce_max:
                  max(1, Application.get_env(:ferricstore, :native_response_coalesce_max, 64)),
                response_coalesce_bytes:
                  Application.get_env(
                    :ferricstore,
                    :native_response_coalesce_bytes,
                    8 * 1024 * 1024
                  ),
                idle_timeout_ms:
                  Application.get_env(:ferricstore, :native_idle_timeout_ms, 90_000)
              }
              |> refresh_command_state()

            join_acl_invalidation_group()
            ConnRegistry.register(state.client_id, self(), Commands.summary(state))
            loop(state)
          end
      end
    end
  end

  def loop(%__MODULE__{socket: socket, transport: transport, active_mode: active_mode} = state) do
    if active_mode == :once do
      transport.setopts(socket, active: :once)
    end

    idle_timeout = idle_receive_timeout(state)

    receive do
      {:tcp, ^socket, data} ->
        handle_data(state, data)

      {:ssl, ^socket, data} ->
        handle_data(state, data)

      {:tcp_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        loop(state)

      {:ssl_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        loop(state)

      {:tcp_closed, ^socket} ->
        cleanup_connection(state)

      {:tcp_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:ssl_closed, ^socket} ->
        cleanup_connection(state)

      {:ssl_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      :client_kill ->
        cleanup_connection(state)
        transport.close(socket)

      {:native_goaway, payload} ->
        transport.send(socket, encode_event_for_state(state, @op_goaway, payload))
        loop(state)

      {:native_topology_changed, payload} ->
        maybe_send_event(state, "TOPOLOGY_CHANGED", Map.merge(topology_payload(), payload))
        loop(state)

      {:flow_claim_due_wake, :ready} ->
        state = Commands.refresh_flow_wake_subscription(state)
        maybe_send_event(state, "FLOW_WAKE", Commands.flow_wake_event_payload(state))
        loop(state)

      {:acl_invalidate, username} ->
        refreshed_state =
          FerricstoreServer.Connection.Auth.maybe_refresh_acl_cache(state, username)

        ConnRegistry.update(refreshed_state.client_id, self(), Commands.summary(refreshed_state))

        if acl_invalidation_affects_session?(state, username) do
          maybe_send_event(refreshed_state, "AUTH_INVALIDATED", %{
            username: invalidated_username(username),
            session_username: refreshed_state.username,
            authenticated: refreshed_state.authenticated,
            reconnect: true
          })

          cleanup_connection(refreshed_state)
          transport.close(socket)
        else
          loop(refreshed_state)
        end

      {:native_lane_response, lane_id, iodata} ->
        loop(send_lane_responses(state, lane_id, iodata))

      {:native_lane_responses, lane_id, iodata_list, done_count} ->
        loop(send_lane_responses(state, lane_id, iodata_list, done_count))

      {:native_lane_done, lane_id} ->
        loop(finish_inflight(state, lane_id))

      {:native_lane_done_many, lane_id, done_count} ->
        loop(finish_inflight(state, lane_id, done_count))

      _other ->
        loop(state)
    after
      idle_timeout ->
        cleanup_connection(state)
        transport.close(socket)
    end
  end

  defp idle_receive_timeout(%__MODULE__{idle_timeout_ms: timeout})
       when is_integer(timeout) and timeout > 0,
       do: timeout

  defp idle_receive_timeout(_state), do: :infinity

  defp handle_data(state, data) do
    buffer =
      case state.buffer do
        "" -> data
        buffered -> buffered <> data
      end

    if byte_size(buffer) > @max_buffer_size do
      send_native_error(state.socket, state.transport, "ERR native client buffer exceeded limit")
      cleanup_connection(state)
      state.transport.close(state.socket)
    else
      decode_started_us = monotonic_us()

      case Codec.decode_frames(buffer, state.max_frame_bytes) do
        {:ok, frames, rest} ->
          decode_us = monotonic_us() - decode_started_us
          {responses, state} = dispatch_frames(frames, %{state | buffer: rest}, [], decode_us)

          case responses do
            [] -> :ok
            _ -> state.transport.send(state.socket, Enum.reverse(responses))
          end

          if state.close_after_reply do
            cleanup_connection(state)
            state.transport.close(state.socket)
          else
            loop(state)
          end

        {:error, reason} ->
          send_native_error(state.socket, state.transport, reason)
          cleanup_connection(state)
          state.transport.close(state.socket)
      end
    end
  end

  defp dispatch_frames(frames, state, responses, decode_us) do
    dispatch_frames(frames, state, responses, decode_us, %{})
  end

  defp dispatch_frames([], state, responses, _decode_us, lane_batches) do
    flush_lane_batches(lane_batches)
    {responses, state}
  end

  defp dispatch_frames([frame | rest], state, responses, decode_us, lane_batches) do
    case prepare_frame(frame, state) do
      {:ok, frame, state} ->
        cond do
          Commands.control_opcode?(opcode(frame)) ->
            flush_lane_batches(lane_batches)
            dispatch_control_frame(frame, rest, state, responses, decode_us)

          lane_id(frame) == @control_lane ->
            response =
              encode_response_for_state(
                state,
                opcode(frame),
                lane_id(frame),
                request_id(frame),
                :bad_request,
                "ERR native data commands cannot use control lane 0"
              )

            dispatch_frames(rest, state, [response | responses], decode_us, lane_batches)

          true ->
            dispatch_data_frame(frame, rest, state, responses, decode_us, lane_batches)
        end

      {:error, reason, state} ->
        response =
          encode_response_for_state(
            state,
            opcode(frame),
            lane_id(frame),
            request_id(frame),
            :bad_request,
            reason
          )

        dispatch_frames(rest, state, [response | responses], decode_us, lane_batches)

      {:pending, state} ->
        dispatch_frames(rest, state, responses, decode_us, lane_batches)
    end
  end

  defp dispatch_control_frame(frame, rest, state, responses, decode_us) do
    case Codec.decode_body(opcode(frame), flags(frame), body(frame)) do
      {:ok, payload} ->
        Commands.mark_command_seen(state)
        {status, value, state} = Commands.execute(opcode(frame), payload, state)
        state = refresh_command_state_and_lanes(state)

        response =
          encode_response_for_state(
            state,
            opcode(frame),
            lane_id(frame),
            request_id(frame),
            status,
            value
          )

        cond do
          state.close_after_reply ->
            {[response | responses], state}

          no_reply?(frame) ->
            dispatch_frames(rest, state, responses, decode_us)

          true ->
            dispatch_frames(rest, state, [response | responses], decode_us)
        end

      {:error, reason} ->
        response =
          encode_response_for_state(
            state,
            opcode(frame),
            lane_id(frame),
            request_id(frame),
            :bad_request,
            reason
          )

        if no_reply?(frame) do
          dispatch_frames(rest, state, responses, decode_us)
        else
          dispatch_frames(rest, state, [response | responses], decode_us)
        end
    end
  end

  defp no_reply?(frame), do: Bitwise.band(flags(frame), @flag_no_reply) != 0

  defp dispatch_data_frame(frame, rest, state, responses, decode_us, lane_batches) do
    route_started_us = trace_started_us(frame)

    case reserve_inflight(state, lane_id(frame)) do
      {:error, reason} ->
        response =
          encode_response_for_state(
            state,
            opcode(frame),
            lane_id(frame),
            request_id(frame),
            :busy,
            reason
          )

        dispatch_frames(rest, state, [response | responses], decode_us, lane_batches)

      {:ok, state} ->
        dispatch_data_frame_reserved(
          frame,
          rest,
          state,
          responses,
          decode_us,
          route_started_us,
          lane_batches
        )
    end
  end

  defp dispatch_data_frame_reserved(
         frame,
         rest,
         state,
         responses,
         decode_us,
         route_started_us,
         lane_batches
       ) do
    case ensure_lane(state, lane_id(frame)) do
      {:ok, lane_pid, state} ->
        if lane_backlog_full?(state, lane_id(frame)) do
          response =
            encode_response_for_state(
              state,
              opcode(frame),
              lane_id(frame),
              request_id(frame),
              :busy,
              %{
                "code" => "lane_queue_full",
                "message" => "ERR native lane queue is full",
                "scope" => "lane",
                "lane_id" => lane_id(frame),
                "retry_after_ms" => 10
              }
            )

          dispatch_frames(
            rest,
            finish_inflight(state, lane_id(frame)),
            [response | responses],
            decode_us,
            lane_batches
          )
        else
          lane_batches =
            add_lane_batch(
              lane_batches,
              lane_id(frame),
              lane_pid,
              maybe_trace_frame(frame, decode_us, route_started_us)
            )

          dispatch_frames(rest, state, responses, decode_us, lane_batches)
        end

      {:error, reason} ->
        response =
          encode_response_for_state(
            state,
            opcode(frame),
            lane_id(frame),
            request_id(frame),
            :busy,
            reason
          )

        dispatch_frames(
          rest,
          finish_inflight(state, lane_id(frame)),
          [response | responses],
          decode_us,
          lane_batches
        )
    end
  end

  defp add_lane_batch(lane_batches, lane_id, lane_pid, frame) do
    Map.update(lane_batches, lane_id, {lane_pid, [frame]}, fn {_existing_pid, frames} ->
      {lane_pid, [frame | frames]}
    end)
  end

  defp flush_lane_batches(lane_batches) do
    Enum.each(lane_batches, fn {_lane_id, {lane_pid, frames}} ->
      Lane.enqueue_many(lane_pid, Enum.reverse(frames))
    end)
  end

  defp prepare_frame(frame, state) do
    with :ok <- validate_request_id(frame),
         :ok <- validate_request_flags(frame, state),
         {:ready, frame, state} <- reassemble_chunks(frame, state),
         {:ok, frame} <- maybe_uncompress_frame(frame, state) do
      {:ok, frame, state}
    else
      {:pending, state} -> {:pending, state}
      {:error, reason, state} -> {:error, reason, state}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp validate_request_id(frame) do
    if request_id(frame) == 0 do
      {:error, "ERR native request_id 0 is reserved for server events"}
    else
      :ok
    end
  end

  defp validate_request_flags(frame, state) do
    cond do
      Bitwise.band(flags(frame), @flag_trace) != 0 and not native_trace_enabled?() ->
        {:error, "ERR native trace flag is disabled"}

      Bitwise.band(flags(frame), @flag_compressed) != 0 and state.compression != :zlib ->
        {:error, "ERR native compressed frames are not supported before compression negotiation"}

      Bitwise.band(flags(frame), Bitwise.bnot(@supported_request_flags)) != 0 ->
        {:error, "ERR native request uses unsupported flags"}

      true ->
        :ok
    end
  end

  defp reassemble_chunks(frame, state) do
    key = chunk_key(frame)
    more? = Bitwise.band(flags(frame), @flag_more_chunks) != 0

    case {Map.fetch(state.chunk_buffers, key), more?} do
      {:error, false} ->
        {:ready, frame, state}

      {:error, true} ->
        if map_size(state.chunk_buffers) >= state.max_pending_chunks do
          {:error, "ERR native pending chunk stream limit exceeded", state}
        else
          put_pending_chunk(state, key, flags(frame), [body(frame)], byte_size(body(frame)))
        end

      {{:ok, {stored_flags, chunks, total_size}}, true} ->
        previous_size = total_size
        total_size = total_size + byte_size(body(frame))

        if total_size > state.max_frame_bytes do
          state = drop_chunk(state, key, previous_size)
          {:error, "ERR native chunked request exceeds max_frame_bytes", state}
        else
          put_pending_chunk(
            state,
            key,
            stored_flags,
            [body(frame) | chunks],
            total_size,
            previous_size
          )
        end

      {{:ok, {stored_flags, chunks, total_size}}, false} ->
        previous_size = total_size
        total_size = total_size + byte_size(body(frame))

        if total_size > state.max_frame_bytes do
          state = drop_chunk(state, key, previous_size)
          {:error, "ERR native chunked request exceeds max_frame_bytes", state}
        else
          body = chunks |> Enum.reverse() |> IO.iodata_to_binary() |> Kernel.<>(body(frame))
          state = drop_chunk(state, key, previous_size)

          flags =
            Bitwise.band(Bitwise.bor(stored_flags, flags(frame)), Bitwise.bnot(@flag_more_chunks))

          {:ready, put_frame(frame, flags, body), state}
        end
    end
  end

  defp put_pending_chunk(state, key, flags, chunks, total_size, previous_size \\ 0) do
    pending_chunk_bytes = state.pending_chunk_bytes - previous_size + total_size

    if pending_chunk_bytes > state.max_pending_chunk_bytes do
      state = drop_chunk(state, key, previous_size)
      {:error, "ERR native pending chunk bytes limit exceeded", state}
    else
      state = %{
        state
        | chunk_buffers: Map.put(state.chunk_buffers, key, {flags, chunks, total_size}),
          pending_chunk_bytes: pending_chunk_bytes
      }

      {:pending, state}
    end
  end

  defp drop_chunk(state, key, size) do
    %{
      state
      | chunk_buffers: Map.delete(state.chunk_buffers, key),
        pending_chunk_bytes: max(state.pending_chunk_bytes - size, 0)
    }
  end

  defp maybe_uncompress_frame(frame, state) do
    if Bitwise.band(flags(frame), @flag_compressed) != 0 do
      try do
        body = :zlib.uncompress(body(frame))

        if byte_size(body) > state.max_frame_bytes do
          {:error, "ERR native decompressed frame exceeds max_frame_bytes"}
        else
          {:ok,
           put_frame(frame, Bitwise.band(flags(frame), Bitwise.bnot(@flag_compressed)), body)}
        end
      rescue
        _ -> {:error, "ERR native compressed frame body is invalid"}
      end
    else
      {:ok, frame}
    end
  end

  defp cleanup_connection(state) do
    Ferricstore.Flow.ClaimWaiters.cleanup(self())
    Enum.each(state.lanes, fn {_lane_id, pid} -> Lane.stop(pid) end)
    ConnRegistry.unregister(state.client_id, self())
    Stats.decr_connections()
    :ok
  end

  defp send_native_error(socket, transport, reason) do
    transport.send(socket, Codec.encode_response(0, 0, 0, :error, reason))
  end

  defp maxclients_exceeded? do
    Stats.active_connections() > Application.get_env(:ferricstore, :maxclients, 10_000)
  end

  defp generate_client_id do
    System.unique_integer([:positive, :monotonic])
  end

  defp require_tls? do
    Application.get_env(:ferricstore, :require_tls, false)
  end

  defp lane_id({lane_id, _opcode, _request_id, _flags, _body}), do: lane_id
  defp opcode({_lane_id, opcode, _request_id, _flags, _body}), do: opcode
  defp request_id({_lane_id, _opcode, request_id, _flags, _body}), do: request_id
  defp flags({_lane_id, _opcode, _request_id, flags, _body}), do: flags
  defp body({_lane_id, _opcode, _request_id, _flags, body}), do: body

  defp put_frame({lane_id, opcode, request_id, _flags, _body}, flags, body),
    do: {lane_id, opcode, request_id, flags, body}

  defp chunk_key(frame), do: {lane_id(frame), opcode(frame), request_id(frame)}

  defp trace?(frame), do: Bitwise.band(flags(frame), @flag_trace) != 0

  defp native_trace_enabled?,
    do: Application.get_env(:ferricstore, :native_trace_enabled, false)

  defp trace_started_us(frame) do
    if trace?(frame), do: monotonic_us()
  end

  defp maybe_trace_frame(frame, _decode_us, nil), do: frame

  defp maybe_trace_frame(frame, decode_us, route_started_us) do
    now_us = monotonic_us()

    {:native_trace, frame,
     %{
       "server_decode_us" => decode_us,
       "server_route_us" => now_us - route_started_us,
       "server_lane_enqueue_us" => now_us
     }}
  end

  defp monotonic_us, do: System.monotonic_time(:microsecond)

  defp ensure_lane(state, lane_id) do
    case Map.fetch(state.lanes, lane_id) do
      {:ok, pid} ->
        {:ok, pid, state}

      :error when map_size(state.lanes) >= state.max_lanes ->
        {:error,
         %{
           "code" => "max_lanes_exceeded",
           "message" => "ERR native max lanes per connection exceeded",
           "scope" => "connection",
           "retry_after_ms" => 10
         }}

      :error ->
        {:ok, pid} = Lane.start_link(self(), lane_id, state.command_state)
        {:ok, pid, %{state | lanes: Map.put(state.lanes, lane_id, pid)}}
    end
  end

  defp refresh_command_state_and_lanes(state) do
    old_command_state = state.command_state
    state = refresh_command_state(state)

    if state.command_state != old_command_state do
      Enum.each(state.lanes, fn {_lane_id, pid} ->
        Lane.update_command_state(pid, state.command_state)
      end)
    end

    state
  end

  defp lane_backlog_full?(state, lane_id) do
    is_integer(state.lane_max_queue) and state.lane_max_queue >= 0 and
      Map.get(state.lane_inflight, lane_id, 0) > state.lane_max_queue
  end

  defp refresh_command_state(state) do
    command_state = %{
      client_id: state.client_id,
      client_name: state.client_name,
      created_at: state.created_at,
      peer: state.peer,
      instance_ctx: state.instance_ctx,
      stats_counter: state.stats_counter,
      authenticated: state.authenticated,
      require_auth: state.require_auth,
      username: state.username,
      acl_cache: state.acl_cache,
      event_subscriptions: state.event_subscriptions,
      flow_wake_subscription: state.flow_wake_subscription,
      compression: state.compression,
      compact_flow_responses: state.compact_flow_responses,
      response_chunk_bytes: state.response_chunk_bytes,
      close_after_reply: false
    }

    %{state | command_state: command_state}
  end

  defp reserve_inflight(state, lane_id) do
    lane_count = Map.get(state.lane_inflight, lane_id, 0)

    cond do
      state.inflight_total >= state.max_inflight_per_connection ->
        {:error,
         %{
           "code" => "flow_control_window_exhausted",
           "message" => "ERR native connection inflight window exhausted",
           "scope" => "connection",
           "retry_after_ms" => 1
         }}

      lane_count >= state.max_inflight_per_lane ->
        {:error,
         %{
           "code" => "flow_control_window_exhausted",
           "message" => "ERR native lane inflight window exhausted",
           "scope" => "lane",
           "lane_id" => lane_id,
           "retry_after_ms" => 1
         }}

      true ->
        {:ok,
         %{
           state
           | inflight_total: state.inflight_total + 1,
             lane_inflight: Map.put(state.lane_inflight, lane_id, lane_count + 1)
         }}
    end
  end

  defp finish_inflight(state, lane_id), do: finish_inflight(state, lane_id, 1)

  defp finish_inflight(state, lane_id, count) do
    lane_count = max(Map.get(state.lane_inflight, lane_id, 0) - count, 0)

    lane_inflight =
      if lane_count == 0,
        do: Map.delete(state.lane_inflight, lane_id),
        else: Map.put(state.lane_inflight, lane_id, lane_count)

    %{state | inflight_total: max(state.inflight_total - count, 0), lane_inflight: lane_inflight}
  end

  defp maybe_send_event(state, event, payload) do
    if MapSet.member?(state.event_subscriptions, event) do
      state.transport.send(
        state.socket,
        Codec.encode_event(Commands.event_opcode(), %{
          event: event,
          payload: payload,
          at_ms: System.system_time(:millisecond)
        })
      )
    end

    :ok
  end

  defp send_lane_responses(state, lane_id, iodata) do
    send_lane_responses(state, lane_id, [iodata], 1)
  end

  defp send_lane_responses(state, lane_id, iodata_list, done_count) do
    state = finish_inflight(state, lane_id, done_count)
    responses = Enum.reverse(iodata_list)

    {state, responses} =
      collect_ready_lane_responses(
        state,
        responses,
        done_count,
        response_coalesce_iodata_size(state, responses)
      )

    state.transport.send(state.socket, Enum.reverse(responses))
    state
  end

  defp collect_ready_lane_responses(state, acc, scanned, bytes) do
    cond do
      scanned >= state.response_coalesce_max ->
        {state, acc}

      response_coalesce_bytes_reached?(state, bytes) ->
        {state, acc}

      true ->
        receive do
          {:native_lane_response, lane_id, iodata} ->
            state = finish_inflight(state, lane_id)

            collect_ready_lane_responses(
              state,
              [iodata | acc],
              scanned + 1,
              response_coalesce_add_iodata_size(state, bytes, iodata)
            )

          {:native_lane_responses, lane_id, iodata_list, done_count} ->
            state = finish_inflight(state, lane_id, done_count)

            collect_ready_lane_responses(
              state,
              Enum.reverse(iodata_list) ++ acc,
              scanned + done_count,
              response_coalesce_add_iodata_size(state, bytes, iodata_list)
            )

          {:native_lane_done, lane_id} ->
            state = finish_inflight(state, lane_id)
            collect_ready_lane_responses(state, acc, scanned + 1, bytes)

          {:native_lane_done_many, lane_id, done_count} ->
            state = finish_inflight(state, lane_id, done_count)
            collect_ready_lane_responses(state, acc, scanned + done_count, bytes)
        after
          0 -> {state, acc}
        end
    end
  end

  defp response_coalesce_iodata_size(%{response_coalesce_bytes: limit}, iodata)
       when is_integer(limit) and limit > 0,
       do: :erlang.iolist_size(iodata)

  defp response_coalesce_iodata_size(_state, _iodata), do: 0

  defp response_coalesce_add_iodata_size(%{response_coalesce_bytes: limit}, bytes, iodata)
       when is_integer(limit) and limit > 0,
       do: bytes + :erlang.iolist_size(iodata)

  defp response_coalesce_add_iodata_size(_state, bytes, _iodata), do: bytes

  defp response_coalesce_bytes_reached?(%{response_coalesce_bytes: limit}, bytes)
       when is_integer(limit) and limit > 0,
       do: bytes >= limit

  defp response_coalesce_bytes_reached?(_state, _bytes), do: false

  defp invalidated_username(:all), do: "all"
  defp invalidated_username(username), do: username

  defp encode_response_for_state(state, opcode, lane_id, request_id, status, value) do
    Codec.encode_command_response_frames(opcode, lane_id, request_id, status, value,
      compression: state.compression,
      compact_flow_responses: state.compact_flow_responses,
      chunk_bytes: state.response_chunk_bytes
    )
  end

  defp encode_event_for_state(state, opcode, value) do
    Codec.encode_response_frames(opcode, 0, 0, :ok, value,
      compression: state.compression,
      chunk_bytes: state.response_chunk_bytes
    )
  end

  defp topology_payload do
    %{
      "route_epoch" => :erlang.phash2(FerricStore.Instance.get(:default).slot_map),
      "node" => Atom.to_string(node())
    }
  rescue
    _ -> %{"route_epoch" => 0, "node" => Atom.to_string(node())}
  end

  defp acl_invalidation_affects_session?(_state, :all), do: true
  defp acl_invalidation_affects_session?(state, username), do: state.username == username

  defp join_acl_invalidation_group do
    group = FerricstoreServer.Connection.acl_pg_group()
    :pg.join(group, group, self())
    :ok
  end
end
