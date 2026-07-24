defmodule FerricstoreServer.Native.Connection.Responses do
  @moduledoc false

  alias Ferricstore.Stats
  alias FerricstoreServer.Native.Codec

  def maxclients_exceeded? do
    Stats.active_connections() > Application.get_env(:ferricstore, :maxclients, 10_000)
  end

  def generate_client_id do
    System.unique_integer([:positive, :monotonic])
  end

  def require_tls? do
    Application.get_env(:ferricstore, :require_tls, false)
  end

  def invalidated_username(:all), do: "all"
  def invalidated_username(username), do: username

  def encode_response(state, opcode, lane_id, request_id, status, value) do
    Codec.encode_command_response_frames(opcode, lane_id, request_id, status, value,
      compression: state.compression,
      compact_flow_responses: state.compact_flow_responses,
      compact_response_codecs: state.compact_response_codecs,
      chunk_bytes: response_chunk_bytes(state),
      max_response_bytes: Map.get(state, :max_response_bytes)
    )
  end

  def encode_event(state, opcode, value) do
    Codec.encode_response_frames(opcode, 0, 0, :ok, value,
      compression: state.compression,
      chunk_bytes: response_chunk_bytes(state),
      max_response_bytes: Map.get(state, :max_response_bytes)
    )
  end

  defp response_chunk_bytes(state) do
    max_frame_bytes =
      Map.get(state, :max_frame_bytes) ||
        Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024)

    Codec.effective_response_chunk_bytes(
      Map.get(state, :response_chunk_bytes, 0),
      max_frame_bytes
    )
  end

  def topology_payload do
    %{
      "route_epoch" => :erlang.phash2(FerricStore.Instance.get(:default).slot_map),
      "node" => Atom.to_string(node())
    }
  rescue
    _ -> %{"route_epoch" => 0, "node" => Atom.to_string(node())}
  end

  def acl_invalidation_affects_session?(_state, :all), do: true
  def acl_invalidation_affects_session?(state, username), do: state.username == username

  def coalesce_iodata_size(%{response_coalesce_bytes: limit}, iodata)
      when is_integer(limit) and limit > 0,
      do: IO.iodata_length(iodata)

  def coalesce_iodata_size(_state, _iodata), do: 0

  def coalesce_add_iodata_size(%{response_coalesce_bytes: limit}, bytes, iodata)
      when is_integer(limit) and limit > 0,
      do: bytes + IO.iodata_length(iodata)

  def coalesce_add_iodata_size(_state, bytes, _iodata), do: bytes

  def coalesce_bytes_reached?(%{response_coalesce_bytes: limit}, bytes)
      when is_integer(limit) and limit > 0,
      do: bytes >= limit

  def coalesce_bytes_reached?(_state, _bytes), do: false
end
