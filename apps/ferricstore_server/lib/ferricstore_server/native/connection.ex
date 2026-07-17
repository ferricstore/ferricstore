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

  alias Ferricstore.Commands.PreparedCommand
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Connection.Send

  alias FerricstoreServer.Native.{
    Admission,
    Blocking,
    Codec,
    Commands,
    Lane,
    OutboundBudget,
    ResourceBudget,
    Session
  }

  alias FerricstoreServer.Native.Connection.{Chunks, FrameBuffer, InboundBudget, Responses}

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
  @cleanup_state_key :native_connection_cleanup_state
  @cleanup_done_key :native_connection_cleanup_done

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
    :max_queued_request_bytes_per_connection,
    :max_queued_request_bytes_per_lane,
    :response_chunk_bytes,
    :max_pending_chunks,
    :max_pending_chunk_bytes,
    :max_response_bytes,
    :max_outbound_bytes,
    :outbound_counter,
    :response_coalesce_max,
    :response_coalesce_bytes,
    :command_state,
    :idle_timeout_ms,
    :resource_budget,
    :preauth_max_frame_bytes,
    :frame_assembly_timeout_ms,
    :frame_assembly_deadline_ms,
    :chunk_assembly_deadline_ms,
    :inbound_buffer_token,
    compression: :none,
    buffer: %FrameBuffer{},
    lanes: %{},
    chunk_buffers: %{},
    pending_chunk_bytes: 0,
    decoded_retained_bytes: 0,
    queued_request_bytes: 0,
    lane_queued_request_bytes: %{},
    inflight_total: 0,
    lane_inflight: %{},
    event_subscriptions: MapSet.new(),
    flow_wake_subscription: nil,
    multi_state: :none,
    multi_queue: [],
    multi_queue_count: 0,
    multi_queue_bytes: 0,
    multi_queue_byte_limit: 32 * 1024 * 1024,
    multi_error: false,
    session_byte_token: nil,
    watched_keys: %{},
    watched_key_bytes: 0,
    watch_key_limit: 10_000,
    watch_key_byte_limit: 16 * 1024 * 1024,
    sandbox_namespace: nil,
    pubsub_channels: nil,
    pubsub_patterns: nil,
    pubsub_subscription_bytes: 0,
    pubsub_subscription_token: nil,
    max_pubsub_subscription_bytes: 16 * 1024 * 1024,
    blocked_requests: %{},
    authenticated: false,
    require_auth: false,
    compact_flow_responses: false,
    decode_paused: false,
    decode_pending: false,
    username: "default",
    acl_cache: nil,
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

      safe_close(transport, socket)
    else
      case Admission.acquire() do
        {:ok, token} ->
          Process.delete(@cleanup_done_key)
          Process.delete(@cleanup_state_key)

          try do
            initialize_connection(socket, transport, opts)
          after
            cleanup_latest_connection()
            Admission.release(token)
            safe_close(transport, socket)
          end

        {:error, _reason} ->
          send_native_error(socket, transport, "ERR max number of clients reached")
          safe_close(transport, socket)
      end
    end
  end

  defp initialize_connection(socket, transport, opts) do
    max_frame_bytes =
      Map.get(opts, :max_frame_bytes) ||
        Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024)

    max_frame_bytes = FrameBuffer.validate_max_frame_bytes!(max_frame_bytes)

    preauth_max_frame_bytes =
      opts
      |> Map.get(
        :preauth_max_frame_bytes,
        Application.get_env(:ferricstore, :native_unauthenticated_max_frame_bytes, 64 * 1024)
      )
      |> FrameBuffer.validate_max_frame_bytes!()
      |> min(max_frame_bytes)

    frame_assembly_timeout_ms =
      positive_timeout!(
        Application.get_env(:ferricstore, :native_frame_assembly_timeout_ms, 15_000),
        :native_frame_assembly_timeout_ms
      )

    response_chunk_bytes =
      Codec.effective_response_chunk_bytes(
        Application.get_env(:ferricstore, :native_response_chunk_bytes, 0),
        max_frame_bytes
      )

    max_response_bytes =
      positive_timeout!(
        Application.get_env(
          :ferricstore,
          :native_max_response_bytes,
          64 * 1024 * 1024
        ),
        :native_max_response_bytes
      )

    max_outbound_bytes =
      positive_timeout!(
        Map.get(opts, :max_outbound_bytes) ||
          Application.get_env(
            :ferricstore,
            :native_max_outbound_bytes_per_connection,
            max(max_response_bytes * 2, 128 * 1024 * 1024)
          ),
        :native_max_outbound_bytes_per_connection
      )

    :ok = transport.setopts(socket, active: :once)

    peer =
      case transport.peername(socket) do
        {:ok, addr} -> addr
        _ -> nil
      end

    case FerricstoreServer.Acl.check_protected_mode(peer) do
      {:error, reason} ->
        send_native_error(socket, transport, reason)

      :ok ->
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
            max_frame_bytes: max_frame_bytes,
            resource_budget: Map.get(opts, :resource_budget, ResourceBudget),
            preauth_max_frame_bytes: preauth_max_frame_bytes,
            frame_assembly_timeout_ms: frame_assembly_timeout_ms,
            multi_queue_byte_limit: min(max_frame_bytes * 2, 32 * 1024 * 1024),
            watch_key_byte_limit: min(max_frame_bytes, 16 * 1024 * 1024),
            max_pubsub_subscription_bytes:
              positive_timeout!(
                Map.get(opts, :max_pubsub_subscription_bytes) ||
                  Application.get_env(
                    :ferricstore,
                    :native_max_pubsub_subscription_bytes_per_connection,
                    16 * 1024 * 1024
                  ),
                :native_max_pubsub_subscription_bytes_per_connection
              ),
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
            max_queued_request_bytes_per_connection:
              positive_timeout!(
                Map.get(opts, :max_queued_request_bytes_per_connection) ||
                  Application.get_env(
                    :ferricstore,
                    :native_max_queued_request_bytes_per_connection,
                    max(FrameBuffer.frame_bytes(max_frame_bytes), 64 * 1024 * 1024)
                  ),
                :native_max_queued_request_bytes_per_connection
              ),
            max_queued_request_bytes_per_lane:
              positive_timeout!(
                Map.get(opts, :max_queued_request_bytes_per_lane) ||
                  Application.get_env(
                    :ferricstore,
                    :native_max_queued_request_bytes_per_lane,
                    max(FrameBuffer.frame_bytes(max_frame_bytes), 32 * 1024 * 1024)
                  ),
                :native_max_queued_request_bytes_per_lane
              ),
            response_chunk_bytes: response_chunk_bytes,
            max_pending_chunks:
              Application.get_env(:ferricstore, :native_max_pending_chunks, 1024),
            max_pending_chunk_bytes:
              Application.get_env(
                :ferricstore,
                :native_max_pending_chunk_bytes,
                64 * 1024 * 1024
              ),
            max_response_bytes: max_response_bytes,
            max_outbound_bytes: max_outbound_bytes,
            outbound_counter: OutboundBudget.new_counter(),
            response_coalesce_max:
              max(1, Application.get_env(:ferricstore, :native_response_coalesce_max, 64)),
            response_coalesce_bytes:
              Application.get_env(
                :ferricstore,
                :native_response_coalesce_bytes,
                8 * 1024 * 1024
              ),
            idle_timeout_ms: Application.get_env(:ferricstore, :native_idle_timeout_ms, 90_000)
          }
          |> refresh_command_state()
          |> remember_connection_state()

        Responses.join_acl_invalidation_group()
        ConnRegistry.register(state.client_id, self(), Commands.summary(state))
        loop(state)
    end
  end

  def loop(%__MODULE__{} = state) do
    state = remember_connection_state(state)

    if connection_deadline_expired?(state) do
      cleanup_connection(state)
      state.transport.close(state.socket)
    else
      receive_connection_messages(state)
    end
  end

  defp receive_connection_messages(%__MODULE__{socket: socket, transport: transport} = state) do
    if not state.decode_paused do
      transport.setopts(socket, active: :once)
    end

    receive_timeout = connection_receive_timeout(state)

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

      {:pubsub_message, channel, message, %OutboundBudget{} = lease} ->
        send_guarded_pubsub_event(
          state,
          lease,
          Session.pubsub_payload(:message, channel, nil, message)
        )

      {:pubsub_message, channel, message} ->
        native_send(
          state,
          Responses.encode_event(
            state,
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

      {:pubsub_pmessage, pattern, channel, message, %OutboundBudget{} = lease} ->
        send_guarded_pubsub_event(
          state,
          lease,
          Session.pubsub_payload(:pmessage, channel, pattern, message)
        )

      {:pubsub_pmessage, pattern, channel, message} ->
        native_send(
          state,
          Responses.encode_event(
            state,
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

      {:native_blocking_response, _meta, pid, status, value} ->
        case take_blocked_request(state, pid) do
          {nil, state} ->
            loop(state)

          {request, state} ->
            Process.demonitor(request.monitor_ref, [:flush])
            state = finish_inflight(state, request.lane_id)
            maybe_send_blocking_response(state, request, status, value)
            loop(state)
        end

      {:DOWN, monitor_ref, :process, pid, reason} ->
        case Map.get(state.blocked_requests, pid) do
          %{monitor_ref: ^monitor_ref} ->
            {request, state} = take_blocked_request(state, pid)
            state = finish_inflight(state, request.lane_id)

            maybe_send_blocking_response(
              state,
              request,
              :error,
              "ERR native blocking command terminated: #{inspect(reason)}"
            )

            loop(state)

          _missing_or_stale ->
            loop(state)
        end

      {:acl_invalidate, username, revision} ->
        _status = FerricstoreServer.Acl.CatalogProjector.require_revision(revision)
        handle_acl_invalidation(state, username)

      {:acl_invalidate, username} ->
        handle_acl_invalidation(state, username)

      {:native_lane_response, lane_id, iodata, request_bytes} ->
        loop(send_lane_responses(state, lane_id, iodata, 1, request_bytes))

      {:native_lane_response, lane_id, iodata} ->
        loop(send_lane_responses(state, lane_id, iodata))

      {:native_lane_response_budgeted, lane_id, iodata, request_bytes, lease} ->
        loop(send_lane_response_budgeted(state, lane_id, iodata, request_bytes, lease))

      {:native_lane_responses, lane_id, iodata_list, done_count, request_bytes} ->
        loop(send_lane_responses(state, lane_id, iodata_list, done_count, request_bytes))

      {:native_lane_responses, lane_id, iodata_list, done_count} ->
        loop(send_lane_responses(state, lane_id, iodata_list, done_count))

      {:native_lane_responses_budgeted, lane_id, iodata_list, done_count, request_bytes, lease} ->
        loop(
          send_lane_responses_budgeted(
            state,
            lane_id,
            iodata_list,
            done_count,
            request_bytes,
            lease
          )
        )

      {:native_lane_outbound_overflow, lane_id, done_count, request_bytes} ->
        state = finish_inflight(state, lane_id, done_count, request_bytes)
        cleanup_connection(state)
        transport.close(socket)

      {:native_lane_done, lane_id, request_bytes} ->
        loop(finish_inflight(state, lane_id, 1, request_bytes))

      {:native_lane_done, lane_id} ->
        loop(finish_inflight(state, lane_id))

      {:native_lane_done_many, lane_id, done_count, request_bytes} ->
        loop(finish_inflight(state, lane_id, done_count, request_bytes))

      {:native_lane_done_many, lane_id, done_count} ->
        loop(finish_inflight(state, lane_id, done_count))

      _other ->
        loop(state)
    after
      receive_timeout ->
        cleanup_connection(state)
        transport.close(socket)
    end
  end

  defp idle_receive_timeout(%__MODULE__{inflight_total: inflight}) when inflight > 0,
    do: :infinity

  defp idle_receive_timeout(%__MODULE__{idle_timeout_ms: timeout})
       when is_integer(timeout) and timeout > 0,
       do: timeout

  defp idle_receive_timeout(_state), do: :infinity

  defp connection_receive_timeout(state) do
    state
    |> frame_assembly_timeout()
    |> min_timeout(chunk_assembly_timeout(state))
    |> min_timeout(idle_receive_timeout(state))
  end

  defp frame_assembly_timeout(%{frame_assembly_deadline_ms: deadline})
       when is_integer(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp frame_assembly_timeout(_state), do: :infinity

  defp chunk_assembly_timeout(%{chunk_assembly_deadline_ms: deadline})
       when is_integer(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end

  defp chunk_assembly_timeout(_state), do: :infinity

  defp connection_deadline_expired?(state) do
    now_ms = System.monotonic_time(:millisecond)

    deadline_expired?(state.frame_assembly_deadline_ms, now_ms) or
      deadline_expired?(state.chunk_assembly_deadline_ms, now_ms)
  end

  defp deadline_expired?(deadline, now_ms) when is_integer(deadline), do: deadline <= now_ms
  defp deadline_expired?(_deadline, _now_ms), do: false

  defp min_timeout(:infinity, timeout), do: timeout
  defp min_timeout(timeout, :infinity), do: timeout
  defp min_timeout(left, right), do: min(left, right)

  defp handle_acl_invalidation(state, username) do
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
      state.transport.close(state.socket)
    else
      loop(refreshed_state)
    end
  end

  defp handle_data(state, data) do
    frame_limit = Chunks.logical_frame_limit(state)

    case FrameBuffer.append(
           state.buffer,
           data,
           frame_limit,
           inbound_buffer_limit(state)
         ) do
      {:incomplete, buffer} ->
        case put_inbound_buffer(state, buffer) do
          {:ok, state} -> loop(state)
          {:error, _reason} -> close_for_inbound_budget(state)
        end

      {:ready, buffer} ->
        case put_inbound_buffer(state, buffer) do
          {:ok, state} -> decode_buffer(state)
          {:error, _reason} -> close_for_inbound_budget(state)
        end

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
    frame_limit = Chunks.logical_frame_limit(state)

    case Codec.decode_frames(buffer, frame_limit) do
      {:ok, frames, rest, continuation} ->
        decode_us = monotonic_us() - decode_started_us
        next_buffer = FrameBuffer.from_binary(rest, frame_limit)
        decoded_bytes = Enum.reduce(frames, 0, &(frame_memory_bytes(&1) + &2))
        state = %{state | decoded_retained_bytes: state.decoded_retained_bytes + decoded_bytes}

        case put_inbound_buffer(state, next_buffer) do
          {:error, _reason} ->
            close_for_inbound_budget(state)

          {:ok, state} ->
            state = update_decode_backpressure(state, continuation)
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
  defp resume_input(state), do: %{state | decode_paused: false}

  defp maybe_reactivate_input(%{decode_paused: true} = state), do: state
  defp maybe_reactivate_input(state), do: state

  defp inbound_buffer_limit(state),
    do:
      min(
        FrameBuffer.max_buffer_bytes(),
        FrameBuffer.frame_bytes(Chunks.logical_frame_limit(state))
      )

  defp put_inbound_buffer(state, buffer) do
    stats = FrameBuffer.stats(buffer)
    deadline = next_frame_assembly_deadline(state, stats)
    state = %{state | buffer: buffer, frame_assembly_deadline_ms: deadline}

    case InboundBudget.resize(
           state.resource_budget,
           state.inbound_buffer_token,
           retained_inbound_bytes(state, stats)
         ) do
      {:ok, token} ->
        {:ok, %{state | inbound_buffer_token: token}}

      {:error, _reason} = error ->
        error
    end
  end

  defp resize_inbound_accounting(state) do
    stats = FrameBuffer.stats(state.buffer)

    case InboundBudget.resize(
           state.resource_budget,
           state.inbound_buffer_token,
           retained_inbound_bytes(state, stats)
         ) do
      {:ok, token} -> {:ok, %{state | inbound_buffer_token: token}}
      {:error, _reason} = error -> error
    end
  end

  defp retained_inbound_bytes(state, buffer_stats) do
    buffer_stats.buffered_bytes + state.decoded_retained_bytes + state.queued_request_bytes
  end

  defp discard_decoded_frame(state, decoded_bytes) do
    state = %{
      state
      | decoded_retained_bytes: max(state.decoded_retained_bytes - decoded_bytes, 0)
    }

    case resize_inbound_accounting(state) do
      {:ok, state} -> state
      {:error, _reason} -> state
    end
  end

  defp next_frame_assembly_deadline(_state, %{buffered_bytes: 0}), do: nil
  defp next_frame_assembly_deadline(_state, %{complete?: true}), do: nil

  defp next_frame_assembly_deadline(%{frame_assembly_deadline_ms: deadline}, _stats)
       when is_integer(deadline),
       do: deadline

  defp next_frame_assembly_deadline(state, _stats) do
    System.monotonic_time(:millisecond) + state.frame_assembly_timeout_ms
  end

  defp close_for_inbound_budget(state) do
    send_native_error(
      state.socket,
      state.transport,
      "ERR native global inbound buffer limit exceeded"
    )

    cleanup_connection(state)
    state.transport.close(state.socket)
  end

  defp dispatch_frames(frames, state, responses, decode_us) do
    dispatch_frames(frames, state, responses, decode_us, %{})
  end

  defp dispatch_frames([], state, responses, _decode_us, lane_batches) do
    flush_lane_batches(lane_batches)
    {responses, state}
  end

  defp dispatch_frames([raw_frame | rest], state, responses, decode_us, lane_batches) do
    decoded_bytes = frame_memory_bytes(raw_frame)

    case prepare_frame(raw_frame, state) do
      {:ok, frame, state} ->
        cond do
          state.multi_state == :queuing and opcode(frame) != @op_command_exec ->
            response =
              maybe_encode_response(
                frame,
                state,
                :bad_request,
                "ERR native MULTI requires COMMAND_EXEC frames until EXEC or DISCARD"
              )

            dispatch_frames(
              rest,
              discard_decoded_frame(state, decoded_bytes),
              maybe_prepend_response(frame, response, responses),
              decode_us,
              lane_batches
            )

          Commands.control_opcode?(opcode(frame)) ->
            flush_lane_batches(lane_batches)

            dispatch_control_frame(
              frame,
              rest,
              discard_decoded_frame(state, decoded_bytes),
              responses,
              decode_us
            )

          native_session_frame?(frame, state) ->
            flush_lane_batches(lane_batches)

            dispatch_native_session_frame(
              frame,
              rest,
              discard_decoded_frame(state, decoded_bytes),
              responses,
              decode_us
            )

          lane_id(frame) == @control_lane ->
            response =
              maybe_encode_response(
                frame,
                state,
                :bad_request,
                "ERR native data commands cannot use control lane 0"
              )

            dispatch_frames(
              rest,
              discard_decoded_frame(state, decoded_bytes),
              maybe_prepend_response(frame, response, responses),
              decode_us,
              lane_batches
            )

          true ->
            dispatch_data_frame(
              frame,
              rest,
              state,
              responses,
              decode_us,
              decoded_bytes,
              lane_batches
            )
        end

      {:error, reason, state} ->
        response =
          maybe_encode_response(
            raw_frame,
            state,
            :bad_request,
            reason
          )

        dispatch_frames(
          rest,
          discard_decoded_frame(state, decoded_bytes),
          maybe_prepend_response(raw_frame, response, responses),
          decode_us,
          lane_batches
        )

      {:pending, state} ->
        dispatch_frames(
          rest,
          discard_decoded_frame(state, decoded_bytes),
          responses,
          decode_us,
          lane_batches
        )
    end
  end

  defp dispatch_control_frame(frame, rest, state, responses, decode_us) do
    case execute_control_frame(frame, state) do
      {:reply, response, state} ->
        if state.close_after_reply do
          {maybe_prepend_response(frame, response, responses), state}
        else
          dispatch_frames(
            rest,
            state,
            maybe_prepend_response(frame, response, responses),
            decode_us
          )
        end
    end
  end

  defp execute_control_frame(frame, state) do
    budget = state.resource_budget

    case ResourceBudget.acquire(budget, :executions, self(), 1) do
      {:ok, token} ->
        try do
          case Codec.decode_body(opcode(frame), flags(frame), body(frame)) do
            {:ok, payload} ->
              Commands.mark_command_seen(state)
              {status, value, state} = Commands.execute(opcode(frame), payload, state)
              state = refresh_command_state_and_lanes(state)
              {:reply, maybe_encode_response(frame, state, status, value), state}

            {:error, reason} ->
              {:reply, maybe_encode_response(frame, state, :bad_request, reason), state}
          end
        after
          ResourceBudget.release_async(budget, token)
        end

      {:error, {:limit, :executions}} ->
        {:reply,
         maybe_encode_response(
           frame,
           state,
           :busy,
           "ERR native global execution limit exceeded"
         ), state}

      {:error, _reason} ->
        {:reply,
         maybe_encode_response(frame, state, :busy, "ERR native resource budget unavailable"),
         state}
    end
  end

  defp dispatch_native_session_frame(frame, rest, state, responses, decode_us) do
    case Codec.decode_body(opcode(frame), flags(frame), body(frame)) do
      {:ok, payload} ->
        Commands.mark_command_seen(state)

        case dispatch_native_session_payload(frame, payload, state) do
          {:reply, status, value, state} ->
            state = refresh_command_state_and_lanes(state)
            response = maybe_encode_response(frame, state, status, value)

            dispatch_frames(
              rest,
              state,
              maybe_prepend_response(frame, response, responses),
              decode_us
            )

          {:blocked, state} ->
            state = refresh_command_state_and_lanes(state)
            dispatch_frames(rest, state, responses, decode_us)
        end

      {:error, reason} ->
        response = maybe_encode_response(frame, state, :bad_request, reason)

        dispatch_frames(
          rest,
          state,
          maybe_prepend_response(frame, response, responses),
          decode_us
        )
    end
  end

  defp dispatch_native_session_payload(frame, payload, state) do
    case Session.prepare_command(payload) do
      {:ok, prepared} ->
        cond do
          state.multi_state == :queuing ->
            {status, value, state} = execute_native_session_prepared(prepared, state)
            {:reply, status, value, state}

          Blocking.blocking_command?(prepared.command) ->
            case reserve_inflight(state, lane_id(frame)) do
              {:ok, state} ->
                meta = %{
                  opcode: opcode(frame),
                  lane_id: lane_id(frame),
                  request_id: request_id(frame),
                  no_reply: no_reply?(frame)
                }

                case Blocking.start_prepared(prepared, state, meta) do
                  {:ok, pid, monitor_ref} ->
                    {:blocked, put_blocked_request(state, pid, monitor_ref, meta)}

                  {:error, status, reason} ->
                    {:reply, status, reason, finish_inflight(state, lane_id(frame))}
                end

              {:error, reason} ->
                {:reply, :busy, reason, state}
            end

          Session.session_command?(prepared.command) ->
            {status, value, state} = execute_native_session_prepared(prepared, state)
            {:reply, status, value, state}

          true ->
            {:reply, :bad_request, "ERR native command is not a session command", state}
        end

      {:error, reason} ->
        {:reply, :bad_request, reason, state}
    end
  end

  defp no_reply?(frame), do: Bitwise.band(flags(frame), @flag_no_reply) != 0

  defp execute_native_session_prepared(
         %PreparedCommand{command: command} = prepared,
         state
       )
       when command in ["EXEC", "WATCH"] do
    budget = state.resource_budget

    case ResourceBudget.acquire(budget, :executions, self(), 1) do
      {:ok, token} ->
        try do
          Session.execute_prepared(prepared, state)
        after
          ResourceBudget.release_async(budget, token)
        end

      {:error, {:limit, :executions}} ->
        {:busy, "ERR native global execution limit exceeded", state}

      {:error, _reason} ->
        {:busy, "ERR native resource budget unavailable", state}
    end
  end

  defp execute_native_session_prepared(%PreparedCommand{} = prepared, state),
    do: Session.execute_prepared(prepared, state)

  defp maybe_encode_response(frame, state, status, value) do
    if no_reply?(frame) do
      nil
    else
      Responses.encode_response(
        state,
        opcode(frame),
        lane_id(frame),
        request_id(frame),
        status,
        value
      )
    end
  end

  defp maybe_prepend_response(_frame, nil, responses), do: responses
  defp maybe_prepend_response(_frame, response, responses), do: [response | responses]

  defp native_session_frame?(frame, state) do
    opcode(frame) == @op_command_exec and
      (state.multi_state == :queuing or native_session_payload?(frame))
  end

  defp native_session_payload?(frame) do
    case Codec.peek_command_name(flags(frame), body(frame)) do
      {:ok, command} ->
        command = String.upcase(command)
        Blocking.blocking_command?(command) or Session.session_command?(command)

      _invalid_or_non_session ->
        false
    end
  end

  defp dispatch_data_frame(
         frame,
         rest,
         state,
         responses,
         decode_us,
         decoded_bytes,
         lane_batches
       ) do
    route_started_us = trace_started_us(frame)

    case reserve_inflight(state, lane_id(frame)) do
      {:error, reason} ->
        response = maybe_encode_response(frame, state, :busy, reason)

        dispatch_frames(
          rest,
          discard_decoded_frame(state, decoded_bytes),
          maybe_prepend_response(frame, response, responses),
          decode_us,
          lane_batches
        )

      {:ok, state} ->
        dispatch_data_frame_reserved(
          frame,
          rest,
          state,
          responses,
          decode_us,
          route_started_us,
          decoded_bytes,
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
         decoded_bytes,
         lane_batches
       ) do
    case ensure_lane(state, lane_id(frame)) do
      {:ok, lane_pid, state} ->
        if lane_backlog_full?(state, lane_id(frame)) do
          response =
            maybe_encode_response(
              frame,
              state,
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
            state
            |> discard_decoded_frame(decoded_bytes)
            |> finish_inflight(lane_id(frame)),
            maybe_prepend_response(frame, response, responses),
            decode_us,
            lane_batches
          )
        else
          case reserve_queued_request(state, frame, decoded_bytes) do
            {:ok, state, request_bytes} ->
              accounted_frame =
                frame
                |> maybe_trace_frame(decode_us, route_started_us)
                |> Lane.account_frame(request_bytes)

              lane_batches =
                add_lane_batch(
                  lane_batches,
                  lane_id(frame),
                  lane_pid,
                  accounted_frame
                )

              dispatch_frames(rest, state, responses, decode_us, lane_batches)

            {:error, reason, state} ->
              response = maybe_encode_response(frame, state, :busy, reason)

              dispatch_frames(
                rest,
                finish_inflight(state, lane_id(frame)),
                maybe_prepend_response(frame, response, responses),
                decode_us,
                lane_batches
              )
          end
        end

      {:error, reason} ->
        response = maybe_encode_response(frame, state, :busy, reason)

        dispatch_frames(
          rest,
          state
          |> discard_decoded_frame(decoded_bytes)
          |> finish_inflight(lane_id(frame)),
          maybe_prepend_response(frame, response, responses),
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

  defp reserve_queued_request(state, frame, decoded_bytes) do
    request_bytes = frame_memory_bytes(frame)
    lane_id = lane_id(frame)
    lane_bytes = Map.get(state.lane_queued_request_bytes, lane_id, 0)

    cond do
      state.queued_request_bytes + request_bytes >
          state.max_queued_request_bytes_per_connection ->
        {:error, queued_request_limit_error(:connection, lane_id),
         discard_decoded_frame(state, decoded_bytes)}

      lane_bytes + request_bytes > state.max_queued_request_bytes_per_lane ->
        {:error, queued_request_limit_error(:lane, lane_id),
         discard_decoded_frame(state, decoded_bytes)}

      true ->
        next_state = %{
          state
          | decoded_retained_bytes: max(state.decoded_retained_bytes - decoded_bytes, 0),
            queued_request_bytes: state.queued_request_bytes + request_bytes,
            lane_queued_request_bytes:
              Map.put(state.lane_queued_request_bytes, lane_id, lane_bytes + request_bytes)
        }

        case resize_inbound_accounting(next_state) do
          {:ok, next_state} ->
            {:ok, next_state, request_bytes}

          {:error, _reason} ->
            {:error, queued_request_limit_error(:server, lane_id),
             discard_decoded_frame(state, decoded_bytes)}
        end
    end
  end

  defp queued_request_limit_error(scope, lane_id) do
    {code, message, retry_after_ms} =
      case scope do
        :connection ->
          {"queued_request_bytes_exceeded",
           "ERR native connection queued request byte limit exceeded", 10}

        :lane ->
          {"queued_request_bytes_exceeded", "ERR native lane queued request byte limit exceeded",
           10}

        :server ->
          {"global_inbound_bytes_exceeded", "ERR native global inbound byte limit exceeded", 10}
      end

    %{
      "code" => code,
      "message" => message,
      "scope" => Atom.to_string(scope),
      "lane_id" => lane_id,
      "retry_after_ms" => retry_after_ms
    }
  end

  defp prepare_frame(frame, state) do
    with :ok <- validate_request_id(frame),
         :ok <- validate_request_flags(frame, state),
         {:ready, frame, state} <- Chunks.reassemble(frame, state),
         {:ok, frame} <- Chunks.maybe_uncompress(frame, state),
         :ok <- Chunks.validate_logical_size(frame, state) do
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
    unless Process.get(@cleanup_done_key, false) do
      Process.put(@cleanup_done_key, true)
      _cleared_state = Session.clear(state)
      Ferricstore.Flow.ClaimWaiters.cleanup(self())
      Ferricstore.Commands.Stream.cleanup_stream_waiters(self())
      InboundBudget.release(state.resource_budget, state.inbound_buffer_token)

      Enum.each(state.blocked_requests, fn {_pid, %{pid: pid}} ->
        Process.exit(pid, :shutdown)
      end)

      Enum.each(state.lanes, fn {_lane_id, pid} -> Lane.stop(pid) end)
      ConnRegistry.unregister(state.client_id, self())
    end

    :ok
  end

  defp cleanup_latest_connection do
    case Process.get(@cleanup_state_key) do
      %__MODULE__{} = state -> cleanup_connection(state)
      _missing -> :ok
    end
  end

  defp remember_connection_state(%__MODULE__{} = state) do
    Process.put(@cleanup_state_key, state)
    state
  end

  defp put_blocked_request(state, pid, monitor_ref, meta) do
    request = meta |> Map.put(:pid, pid) |> Map.put(:monitor_ref, monitor_ref)

    state
    |> put_in([Access.key(:blocked_requests), pid], request)
    |> remember_connection_state()
  end

  defp take_blocked_request(state, pid) do
    {request, blocked_requests} = Map.pop(state.blocked_requests, pid)
    {request, %{state | blocked_requests: blocked_requests}}
  end

  defp maybe_send_blocking_response(_state, %{no_reply: true}, _status, _value), do: :ok

  defp maybe_send_blocking_response(state, request, status, value) do
    native_send(
      state,
      Responses.encode_response(
        state,
        request.opcode,
        request.lane_id,
        request.request_id,
        status,
        value
      ),
      :response
    )
  end

  defp send_native_error(socket, transport, reason) do
    native_send(socket, transport, Codec.encode_response(0, 0, 0, :error, reason), :error)
  end

  defp safe_close(transport, socket) do
    transport.close(socket)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
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
  defp frame_memory_bytes(frame), do: FrameBuffer.frame_bytes(byte_size(body(frame)))

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

  defp positive_timeout!(timeout, _name) when is_integer(timeout) and timeout > 0, do: timeout

  defp positive_timeout!(timeout, name) do
    raise ArgumentError, "#{name} must be a positive integer, got: #{inspect(timeout)}"
  end

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
        case Lane.start_link(self(), lane_id, state.command_state) do
          {:ok, pid} ->
            state = %{state | lanes: Map.put(state.lanes, lane_id, pid)}
            {:ok, pid, remember_connection_state(state)}

          {:error, {:limit, :lanes}} ->
            {:error,
             %{
               "code" => "global_lane_limit_exceeded",
               "message" => "ERR native global lane limit exceeded",
               "scope" => "server",
               "retry_after_ms" => 10
             }}

          {:error, _reason} ->
            {:error,
             %{
               "code" => "resource_budget_unavailable",
               "message" => "ERR native resource budget unavailable",
               "scope" => "server",
               "retry_after_ms" => 10
             }}
        end
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
      multi_queue_bytes: state.multi_queue_bytes,
      multi_queue_byte_limit: state.multi_queue_byte_limit,
      multi_error: state.multi_error,
      session_byte_token: state.session_byte_token,
      watched_keys: state.watched_keys,
      watched_key_bytes: state.watched_key_bytes,
      watch_key_limit: state.watch_key_limit,
      watch_key_byte_limit: state.watch_key_byte_limit,
      sandbox_namespace: state.sandbox_namespace,
      pubsub_channels: state.pubsub_channels,
      pubsub_patterns: state.pubsub_patterns,
      pubsub_subscription_bytes: state.pubsub_subscription_bytes,
      pubsub_subscription_token: state.pubsub_subscription_token,
      max_pubsub_subscription_bytes: state.max_pubsub_subscription_bytes,
      compression: state.compression,
      compact_flow_responses: state.compact_flow_responses,
      max_frame_bytes: state.max_frame_bytes,
      response_chunk_bytes: state.response_chunk_bytes,
      max_response_bytes: state.max_response_bytes,
      max_outbound_bytes: state.max_outbound_bytes,
      outbound_counter: state.outbound_counter,
      resource_budget: state.resource_budget,
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
  defp finish_inflight(state, lane_id, count), do: finish_inflight(state, lane_id, count, 0)

  defp finish_inflight(state, lane_id, count, request_bytes) do
    lane_count = max(Map.get(state.lane_inflight, lane_id, 0) - count, 0)

    lane_inflight =
      if lane_count == 0,
        do: Map.delete(state.lane_inflight, lane_id),
        else: Map.put(state.lane_inflight, lane_id, lane_count)

    state = %{
      state
      | inflight_total: max(state.inflight_total - count, 0),
        lane_inflight: lane_inflight
    }

    release_queued_request_bytes(state, lane_id, request_bytes)
  end

  defp release_queued_request_bytes(state, _lane_id, 0), do: state

  defp release_queued_request_bytes(state, lane_id, request_bytes)
       when is_integer(request_bytes) and request_bytes > 0 do
    lane_bytes = Map.get(state.lane_queued_request_bytes, lane_id, 0)
    released_bytes = min(request_bytes, lane_bytes)
    remaining_lane_bytes = lane_bytes - released_bytes

    lane_queued_request_bytes =
      if remaining_lane_bytes == 0 do
        Map.delete(state.lane_queued_request_bytes, lane_id)
      else
        Map.put(state.lane_queued_request_bytes, lane_id, remaining_lane_bytes)
      end

    state = %{
      state
      | queued_request_bytes: max(state.queued_request_bytes - released_bytes, 0),
        lane_queued_request_bytes: lane_queued_request_bytes
    }

    case resize_inbound_accounting(state) do
      {:ok, state} -> state
      {:error, _reason} -> state
    end
  end

  defp maybe_send_event(state, event, payload) do
    if MapSet.member?(state.event_subscriptions, event) do
      native_send(
        state,
        Responses.encode_event(
          state,
          Commands.event_opcode(),
          %{
            event: event,
            payload: payload,
            at_ms: System.system_time(:millisecond)
          }
        ),
        :event
      )
    end

    :ok
  end

  defp send_guarded_pubsub_event(state, lease, pubsub_payload) do
    event_payload = %{
      "event" => "PUBSUB_MESSAGE",
      "payload" => pubsub_payload,
      "at_ms" => System.system_time(:millisecond)
    }

    case safe_encode_guarded_event(state, event_payload) do
      {:ok, iodata} ->
        case OutboundBudget.ensure_iodata(lease, iodata) do
          {:ok, lease} ->
            result =
              try do
                native_send(state, iodata, :event)
              after
                OutboundBudget.release(lease)
              end

            case result do
              :ok -> loop(state)
              {:error, _reason} -> close_for_outbound_failure(state)
            end

          {:error, _reason} ->
            OutboundBudget.release(lease)
            close_for_outbound_failure(state)
        end

      {:error, _reason} ->
        OutboundBudget.release(lease)
        close_for_outbound_failure(state)
    end
  end

  defp safe_encode_guarded_event(state, payload) do
    {:ok, Responses.encode_event(state, Commands.event_opcode(), payload)}
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp close_for_outbound_failure(state) do
    cleanup_connection(state)
    state.transport.close(state.socket)
  end

  defp send_lane_responses(state, lane_id, iodata) do
    send_lane_responses(state, lane_id, [iodata], 1, 0, [])
  end

  defp send_lane_responses(state, lane_id, iodata_list, done_count) do
    send_lane_responses(state, lane_id, iodata_list, done_count, 0, [])
  end

  defp send_lane_responses(state, lane_id, iodata_list, done_count, request_bytes) do
    send_lane_responses(state, lane_id, iodata_list, done_count, request_bytes, [])
  end

  defp send_lane_response_budgeted(state, lane_id, iodata, request_bytes, lease) do
    send_lane_responses(state, lane_id, [iodata], 1, request_bytes, [lease])
  end

  defp send_lane_responses_budgeted(
         state,
         lane_id,
         iodata,
         done_count,
         request_bytes,
         lease
       ) do
    send_lane_responses(state, lane_id, iodata, done_count, request_bytes, [lease])
  end

  defp send_lane_responses(
         state,
         lane_id,
         iodata_list,
         done_count,
         request_bytes,
         leases
       ) do
    state = finish_inflight(state, lane_id, done_count, request_bytes)
    responses = Enum.reverse(iodata_list)

    {state, responses, leases} =
      collect_ready_lane_responses(
        state,
        responses,
        done_count,
        Responses.coalesce_iodata_size(state, responses),
        leases
      )

    try do
      native_send(state, Enum.reverse(responses), :response)
    after
      Enum.each(leases, &OutboundBudget.release/1)
    end

    state
  end

  defp collect_ready_lane_responses(state, acc, scanned, bytes, leases) do
    cond do
      scanned >= state.response_coalesce_max ->
        {state, acc, leases}

      Responses.coalesce_bytes_reached?(state, bytes) ->
        {state, acc, leases}

      true ->
        receive do
          {:native_lane_response_budgeted, lane_id, iodata, request_bytes, lease} ->
            state = finish_inflight(state, lane_id, 1, request_bytes)

            collect_ready_lane_responses(
              state,
              [iodata | acc],
              scanned + 1,
              Responses.coalesce_add_iodata_size(state, bytes, iodata),
              [lease | leases]
            )

          {:native_lane_response, lane_id, iodata, request_bytes} ->
            state = finish_inflight(state, lane_id, 1, request_bytes)

            collect_ready_lane_responses(
              state,
              [iodata | acc],
              scanned + 1,
              Responses.coalesce_add_iodata_size(state, bytes, iodata),
              leases
            )

          {:native_lane_response, lane_id, iodata} ->
            state = finish_inflight(state, lane_id)

            collect_ready_lane_responses(
              state,
              [iodata | acc],
              scanned + 1,
              Responses.coalesce_add_iodata_size(state, bytes, iodata),
              leases
            )

          {:native_lane_responses_budgeted, lane_id, iodata_list, done_count, request_bytes,
           lease} ->
            state = finish_inflight(state, lane_id, done_count, request_bytes)

            collect_ready_lane_responses(
              state,
              Enum.reverse(iodata_list) ++ acc,
              scanned + done_count,
              Responses.coalesce_add_iodata_size(state, bytes, iodata_list),
              [lease | leases]
            )

          {:native_lane_responses, lane_id, iodata_list, done_count, request_bytes} ->
            state = finish_inflight(state, lane_id, done_count, request_bytes)

            collect_ready_lane_responses(
              state,
              Enum.reverse(iodata_list) ++ acc,
              scanned + done_count,
              Responses.coalesce_add_iodata_size(state, bytes, iodata_list),
              leases
            )

          {:native_lane_responses, lane_id, iodata_list, done_count} ->
            state = finish_inflight(state, lane_id, done_count)

            collect_ready_lane_responses(
              state,
              Enum.reverse(iodata_list) ++ acc,
              scanned + done_count,
              Responses.coalesce_add_iodata_size(state, bytes, iodata_list),
              leases
            )

          {:native_lane_done, lane_id, request_bytes} ->
            state = finish_inflight(state, lane_id, 1, request_bytes)
            collect_ready_lane_responses(state, acc, scanned + 1, bytes, leases)

          {:native_lane_done, lane_id} ->
            state = finish_inflight(state, lane_id)
            collect_ready_lane_responses(state, acc, scanned + 1, bytes, leases)

          {:native_lane_done_many, lane_id, done_count, request_bytes} ->
            state = finish_inflight(state, lane_id, done_count, request_bytes)
            collect_ready_lane_responses(state, acc, scanned + done_count, bytes, leases)

          {:native_lane_done_many, lane_id, done_count} ->
            state = finish_inflight(state, lane_id, done_count)
            collect_ready_lane_responses(state, acc, scanned + done_count, bytes, leases)
        after
          0 -> {state, acc, leases}
        end
    end
  end
end
