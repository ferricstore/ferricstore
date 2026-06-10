defmodule FerricstoreServer.Native.Lane do
  @moduledoc """
  Bounded ordered execution lane for native protocol multiplexing.

  A lane is one lightweight process per active logical stream. It preserves
  command order within the lane while different lanes can run concurrently.
  This gives Cassandra-style multiplexing without spawning an unbounded Task per
  frame.
  """

  alias FerricstoreServer.Native.{Codec, Commands}

  @flag_no_reply 0x10

  @spec start_link(pid(), non_neg_integer(), map()) :: {:ok, pid()}
  def start_link(owner, lane_id, command_state)
      when is_pid(owner) and is_integer(lane_id) and is_map(command_state) do
    pid = spawn_link(__MODULE__, :loop, [owner, lane_id, command_state])
    {:ok, pid}
  end

  @spec enqueue(pid(), map()) :: :ok
  def enqueue(pid, frame) do
    send(pid, {:native_lane_frame, frame})
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

      {:native_lane_command_state, command_state} ->
        loop(owner, lane_id, command_state)

      :shutdown ->
        :ok
    end
  end

  defp execute_frame({lane_id, opcode, request_id, flags, body}, command_state) do
    response =
      execute_frame_with_response({lane_id, opcode, request_id, flags, body}, command_state)

    if Bitwise.band(flags, @flag_no_reply) != 0 do
      :noreply
    else
      response
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
end
