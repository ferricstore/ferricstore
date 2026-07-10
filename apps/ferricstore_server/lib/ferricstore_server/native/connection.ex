defmodule FerricstoreServer.Native.Connection do
  @moduledoc """
  Ranch protocol handler for FerricStore native TCP clients.

  The native listener is a binary, request-id based data plane for SDKs that
  need routing hints, pipelined command execution, and structured Flow results
  without text protocol parsing. It shares the same engine path as embedded commands; this module only
  handles network framing, ACL/protected-mode, client registry, and response
  coalescing.
  """

  @behaviour :ranch_protocol

  alias Ferricstore.Stats
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Connection.Send
  alias FerricstoreServer.Native.{Blocking, Codec, Commands, Lane, Session}
  alias FerricstoreServer.Native.Connection.{Chunks, FrameBuffer, Responses}

  @op_goaway 0x000A
  @op_command_exec 0x0100
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
    buffer: %FrameBuffer{},
    lanes: %{},
    chunk_buffers: %{},
    pending_chunk_bytes: 0,
    inflight_total: 0,
    lane_inflight: %{},
    event_subscriptions: MapSet.new(),
    flow_wake_subscription: nil,
    multi_state: :none,
    multi_queue: [],
    multi_queue_count: 0,
    multi_error: false,
    watched_keys: %{},
    sandbox_namespace: nil,
    pubsub_channels: nil,
    pubsub_patterns: nil,
    blocked_requests: %{},
    authenticated: false,
    require_auth: false,
    compact_flow_responses: false,
    decode_paused: false,
    decode_pending: false,
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

    if transport == :ranch_tcp and Responses.require_tls?() do
      send_native_error(
        socket,
        transport,
        "ERR TLS required: plaintext connections are not permitted"
      )

      transport.close(socket)
    else
      active_mode = Application.get_env(:ferricstore, :socket_active_mode, 100)

      max_frame_bytes =
        Map.get(opts, :max_frame_bytes) ||
          Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024)

      max_frame_bytes = FrameBuffer.validate_max_frame_bytes!(max_frame_bytes)

      response_chunk_bytes =
        Codec.effective_response_chunk_bytes(
          Application.get_env(:ferricstore, :native_response_chunk_bytes, 0),
          max_frame_bytes
        )

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
          if Responses.maxclients_exceeded?() do
            send_native_error(socket, transport, "ERR max number of clients reached")
            Stats.decr_connections()
            transport.close(socket)
          else
            ctx = FerricStore.Instance.get(:default)

            state =
              %__MODULE__{
                socket: socket,
                transport: transport,
                client_id: Responses.generate_client_id(),
                client_name: nil,
                created_at: System.monotonic_time(:millisecond),
                peer: peer,
                instance_ctx: ctx,
                stats_counter: ctx.stats_counter,
                require_auth: Commands.default_requires_auth?(),
                acl_cache: FerricstoreServer.Connection.Auth.build_acl_cache("default"),
                active_mode: active_mode,
                max_frame_bytes: max_frame_bytes,
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
                response_chunk_bytes: response_chunk_bytes,
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

            Responses.join_acl_invalidation_group()
            ConnRegistry.register(state.client_id, self(), Commands.summary(state))
            loop(state)
          end
      end
    end
  end

  def loop(%__MODULE__{socket: socket, transport: transport, active_mode: active_mode} = state) do
    if active_mode == :once and not state.decode_paused do
      transport.setopts(socket, active: :once)
    end

    idle_timeout = idle_receive_timeout(state)

    receive do
      {:tcp, ^socket, data} ->
        handle_data(state, data)

      {:ssl, ^socket, data} ->
        handle_data(state, data)

      :native_decode_continue ->
        decode_buffer(%{state | decode_pending: false})

      {:tcp_passive, ^socket} ->
        loop(maybe_reactivate_input(state))

      {:ssl_passive, ^socket} ->
        loop(maybe_reactivate_input(state))

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
        native_send(state, Responses.encode_event(state, @op_goaway, payload), :event)
        loop(state)

      {:native_topology_changed, payload} ->
        maybe_send_event(
          state,
          "TOPOLOGY_CHANGED",
          Map.merge(Responses.topology_payload(), payload)
        )

        loop(state)

      {:flow_claim_due_wake, :ready} ->
        state = Commands.refresh_flow_wake_subscription(state)
        maybe_send_event(state, "FLOW_WAKE", Commands.flow_wake_event_payload(state))
        loop(state)

      {:pubsub_message, channel, message} ->
        native_send(
          state,
          Codec.encode_event(
            Commands.event_opcode(),
            %{
              "event" => "PUBSUB_MESSAGE",
              "payload" => Session.pubsub_payload(:message, channel, nil, message),
              "at_ms" => System.system_time(:millisecond)
            }
          ),
          :event
        )

        loop(state)

      {:pubsub_pmessage, pattern, channel, message} ->
        native_send(
          state,
          Codec.encode_event(
            Commands.event_opcode(),
            %{
              "event" => "PUBSUB_MESSAGE",
              "payload" => Session.pubsub_payload(:pmessage, channel, pattern, message),
              "at_ms" => System.system_time(:millisecond)
            }
          ),
          :event
        )

        loop(state)

      {:native_blocking_response, meta, pid, status, value} ->
        state = remove_blocked_request(state, pid)

        native_send(
          state,
          Responses.encode_response(
            state,
            meta.opcode,
            meta.lane_id,
            meta.request_id,
            status,
            value
          ),
          :response
        )

        loop(state)

      {:acl_invalidate, username} ->
        refreshed_state =
          FerricstoreServer.Connection.Auth.maybe_refresh_acl_cache(state, username)

        ConnRegistry.update(refreshed_state.client_id, self(), Commands.summary(refreshed_state))

        if Responses.acl_invalidation_affects_session?(state, username) do
          maybe_send_event(refreshed_state, "AUTH_INVALIDATED", %{
            username: Responses.invalidated_username(username),
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
    case FrameBuffer.append(
           state.buffer,
           data,
           state.max_frame_bytes,
           FrameBuffer.max_buffer_bytes()
         ) do
      {:incomplete, buffer} ->
        loop(%{state | buffer: buffer})

      {:ready, buffer} ->
        decode_buffer(%{state | buffer: buffer})

      {:error, :buffer_limit} ->
        send_native_error(
          state.socket,
          state.transport,
          "ERR native client buffer exceeded limit"
        )

        cleanup_connection(state)
        state.transport.close(state.socket)
    end
  end

  defp decode_buffer(state) do
    decode_started_us = monotonic_us()
    buffer = FrameBuffer.materialize(state.buffer)

    case Codec.decode_frames(buffer, state.max_frame_bytes) do
      {:ok, frames, rest, continuation} ->
        decode_us = monotonic_us() - decode_started_us

        state =
          state
          |> Map.put(:buffer, FrameBuffer.from_binary(rest, state.max_frame_bytes))
          |> update_decode_backpressure(continuation)

        {responses, state} = dispatch_frames(frames, state, [], decode_us)

        case responses do
          [] -> :ok
          _ -> native_send(state, Enum.reverse(responses), :response)
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

  defp update_decode_backpressure(state, :more) do
    state = pause_input(state)

    if state.decode_pending do
      state
    else
      send(self(), :native_decode_continue)
      %{state | decode_pending: true}
    end
  end

  defp update_decode_backpressure(state, :done), do: resume_input(state)

  defp pause_input(%{decode_paused: true} = state), do: state

  defp pause_input(state) do
    state.transport.setopts(state.socket, active: false)
    %{state | decode_paused: true}
  end

  defp resume_input(%{decode_paused: false} = state), do: state

  defp resume_input(%{active_mode: :once} = state), do: %{state | decode_paused: false}

  defp resume_input(state) do
    state.transport.setopts(state.socket, active: state.active_mode)
    %{state | decode_paused: false}
  end

  defp maybe_reactivate_input(%{decode_paused: true} = state), do: state

  defp maybe_reactivate_input(state) do
    state.transport.setopts(state.socket, active: state.active_mode)
    state
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
          state.multi_state == :queuing and opcode(frame) != @op_command_exec ->
            response =
              Responses.encode_response(
                state,
                opcode(frame),
                lane_id(frame),
                request_id(frame),
                :bad_request,
                "ERR native MULTI requires COMMAND_EXEC frames until EXEC or DISCARD"
              )

            dispatch_frames(rest, state, [response | responses], decode_us, lane_batches)

          Commands.control_opcode?(opcode(frame)) ->
            flush_lane_batches(lane_batches)
            dispatch_control_frame(frame, rest, state, responses, decode_us)

          native_session_frame?(frame, state) ->
            flush_lane_batches(lane_batches)
            dispatch_native_session_frame(frame, rest, state, responses, decode_us)

          lane_id(frame) == @control_lane ->
            response =
              Responses.encode_response(
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
          Responses.encode_response(
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
          Responses.encode_response(
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
          Responses.encode_response(
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

  defp dispatch_native_session_frame(frame, rest, state, responses, decode_us) do
    case Codec.decode_body(opcode(frame), flags(frame), body(frame)) do
      {:ok, payload} ->
        Commands.mark_command_seen(state)

        case dispatch_native_session_payload(frame, payload, state) do
          {:reply, status, value, state} ->
            state = refresh_command_state_and_lanes(state)

            response =
              Responses.encode_response(
                state,
                opcode(frame),
                lane_id(frame),
                request_id(frame),
                status,
                value
              )

            dispatch_frames(rest, state, [response | responses], decode_us)

          {:blocked, state} ->
            state = refresh_command_state_and_lanes(state)
            dispatch_frames(rest, state, responses, decode_us)
        end

      {:error, reason} ->
        response =
          Responses.encode_response(
            state,
            opcode(frame),
            lane_id(frame),
            request_id(frame),
            :bad_request,
            reason
          )

        dispatch_frames(rest, state, [response | responses], decode_us)
    end
  end

  defp dispatch_native_session_payload(frame, payload, state) do
    case Session.prepare_command(payload) do
      {:ok, prepared} ->
        cond do
          state.multi_state == :queuing ->
            {status, value, state} = Session.execute_prepared(prepared, state)
            {:reply, status, value, state}

          Blocking.blocking_command?(prepared.command) ->
            meta = %{
              opcode: opcode(frame),
              lane_id: lane_id(frame),
              request_id: request_id(frame)
            }

            case Blocking.start_prepared(prepared, state, meta) do
              {:ok, pid} ->
                {:blocked, put_blocked_request(state, pid, meta)}

              {:error, status, reason} ->
                {:reply, status, reason, state}
            end

          Session.session_command?(prepared.command) ->
            {status, value, state} = Session.execute_prepared(prepared, state)
            {:reply, status, value, state}

          true ->
            {:reply, :bad_request, "ERR native command is not a session command", state}
        end

      {:error, reason} ->
        {:reply, :bad_request, reason, state}
    end
  end

  defp no_reply?(frame), do: Bitwise.band(flags(frame), @flag_no_reply) != 0

  defp native_session_frame?(frame, state) do
    opcode(frame) == @op_command_exec and
      (state.multi_state == :queuing or native_session_payload?(frame))
  end

  defp native_session_payload?(frame) do
    case Codec.decode_body(opcode(frame), flags(frame), body(frame)) do
      {:ok, %{"command" => command}} when is_binary(command) ->
        command = String.upcase(command)
        Blocking.blocking_command?(command) or Session.session_command?(command)

      _invalid_or_non_session ->
        false
    end
  end

  defp dispatch_data_frame(frame, rest, state, responses, decode_us, lane_batches) do
    route_started_us = trace_started_us(frame)

    case reserve_inflight(state, lane_id(frame)) do
      {:error, reason} ->
        response =
          Responses.encode_response(
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
            Responses.encode_response(
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
          Responses.encode_response(
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
         {:ready, frame, state} <- Chunks.reassemble(frame, state),
         {:ok, frame} <- Chunks.maybe_uncompress(frame, state) do
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

  defp cleanup_connection(state) do
    Session.cleanup_pubsub(state)
    Ferricstore.Flow.ClaimWaiters.cleanup(self())
    Ferricstore.Commands.Stream.cleanup_stream_waiters(self())
    Enum.each(state.blocked_requests, fn {_pid, %{pid: pid}} -> Process.exit(pid, :shutdown) end)
    Enum.each(state.lanes, fn {_lane_id, pid} -> Lane.stop(pid) end)
    ConnRegistry.unregister(state.client_id, self())
    Stats.decr_connections()
    :ok
  end

  defp put_blocked_request(state, pid, meta) do
    put_in(state.blocked_requests[pid], Map.put(meta, :pid, pid))
  end

  defp remove_blocked_request(state, pid) do
    %{state | blocked_requests: Map.delete(state.blocked_requests, pid)}
  end

  defp send_native_error(socket, transport, reason) do
    native_send(socket, transport, Codec.encode_response(0, 0, 0, :error, reason), :error)
  end

  defp native_send(%__MODULE__{} = state, iodata, phase) do
    native_send(state.socket, state.transport, iodata, phase)
  end

  defp native_send(socket, transport, iodata, phase) do
    Send.send(socket, transport, iodata, phase, %{protocol: :native})
  end

  defp lane_id({lane_id, _opcode, _request_id, _flags, _body}), do: lane_id
  defp opcode({_lane_id, opcode, _request_id, _flags, _body}), do: opcode
  defp request_id({_lane_id, _opcode, request_id, _flags, _body}), do: request_id
  defp flags({_lane_id, _opcode, _request_id, flags, _body}), do: flags
  defp body({_lane_id, _opcode, _request_id, _flags, body}), do: body

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
      multi_state: state.multi_state,
      multi_queue: state.multi_queue,
      multi_queue_count: state.multi_queue_count,
      multi_error: state.multi_error,
      watched_keys: state.watched_keys,
      sandbox_namespace: state.sandbox_namespace,
      pubsub_channels: state.pubsub_channels,
      pubsub_patterns: state.pubsub_patterns,
      compression: state.compression,
      compact_flow_responses: state.compact_flow_responses,
      max_frame_bytes: state.max_frame_bytes,
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
      native_send(
        state,
        Codec.encode_event(Commands.event_opcode(), %{
          event: event,
          payload: payload,
          at_ms: System.system_time(:millisecond)
        }),
        :event
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
        Responses.coalesce_iodata_size(state, responses)
      )

    native_send(state, Enum.reverse(responses), :response)
    state
  end

  defp collect_ready_lane_responses(state, acc, scanned, bytes) do
    cond do
      scanned >= state.response_coalesce_max ->
        {state, acc}

      Responses.coalesce_bytes_reached?(state, bytes) ->
        {state, acc}

      true ->
        receive do
          {:native_lane_response, lane_id, iodata} ->
            state = finish_inflight(state, lane_id)

            collect_ready_lane_responses(
              state,
              [iodata | acc],
              scanned + 1,
              Responses.coalesce_add_iodata_size(state, bytes, iodata)
            )

          {:native_lane_responses, lane_id, iodata_list, done_count} ->
            state = finish_inflight(state, lane_id, done_count)

            collect_ready_lane_responses(
              state,
              Enum.reverse(iodata_list) ++ acc,
              scanned + done_count,
              Responses.coalesce_add_iodata_size(state, bytes, iodata_list)
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
end
