defmodule FerricstoreServer.Connection.PubSubSession do
  @moduledoc false

  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Resp.Encoder

  def pubsub_loop(
         %Connection{socket: socket, transport: transport, active_mode: active_mode} = state
       ) do
    # No setopts needed — active mode (true/N/:once) is maintained from
    # the main loop. TCP data keeps arriving and is handled below.
    if active_mode == :once do
      transport.setopts(socket, active: :once)
    end

    receive do
      {:tcp, ^socket, data} ->
        Connection.handle_data(state, data)

      {:ssl, ^socket, data} ->
        Connection.handle_data(state, data)

      {:tcp_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        pubsub_loop(state)

      {:ssl_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        pubsub_loop(state)

      {:tcp_closed, ^socket} ->
        Connection.cleanup_connection(state)

      {:tcp_error, ^socket, _reason} ->
        Connection.cleanup_connection(state)
        transport.close(socket)

      {:ssl_closed, ^socket} ->
        Connection.cleanup_connection(state)

      {:ssl_error, ^socket, _reason} ->
        Connection.cleanup_connection(state)
        transport.close(socket)

      {:pubsub_message, channel, message} ->
        push = {:push, ["message", channel, message]}

        case Connection.send_tracked(state, Encoder.encode(push), :pubsub_message) do
          :ok -> pubsub_loop(state)
          {:error, _reason} -> :ok
        end

      {:pubsub_pmessage, pattern, channel, message} ->
        push = {:push, ["pmessage", pattern, channel, message]}

        case Connection.send_tracked(state, Encoder.encode(push), :pubsub_pmessage) do
          :ok -> pubsub_loop(state)
          {:error, _reason} -> :ok
        end

      {:tracking_invalidation, iodata, _keys} ->
        case Connection.send_tracked(state, iodata, :tracking_invalidation) do
          :ok -> pubsub_loop(state)
          {:error, _reason} -> :ok
        end

      :client_kill ->
        Connection.cleanup_connection(state)
        transport.close(socket)

      {:acl_invalidate, username} ->
        refreshed_state =
          state
          |> ConnAuth.maybe_refresh_acl_cache(username)
          |> enforce_pubsub_acl_after_refresh()

        refreshed_state = Connection.maybe_sync_connection_registry(state, refreshed_state)

        if in_pubsub_mode?(refreshed_state) do
          pubsub_loop(refreshed_state)
        else
          Connection.loop(refreshed_state)
        end
    end
  end

  def in_pubsub_mode?(%{pubsub_channels: nil}), do: false

  def in_pubsub_mode?(state),
    do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  def enforce_pubsub_acl_after_refresh(%{pubsub_channels: nil} = state), do: state

  def enforce_pubsub_acl_after_refresh(state) do
    if pubsub_acl_still_allowed?(state) do
      state
    else
      # ACL changes are rare; removing the subscription here keeps PUBLISH hot
      # path free of per-message permission checks while still failing closed.
      Connection.cleanup_pubsub(state)
      %{state | pubsub_channels: nil, pubsub_patterns: nil}
    end
  end

  def pubsub_acl_still_allowed?(state) do
    exact_channels = MapSet.to_list(state.pubsub_channels)
    patterns = MapSet.to_list(state.pubsub_patterns)

    pubsub_exact_acl_allowed?(state.acl_cache, exact_channels) and
      pubsub_pattern_acl_allowed?(state.acl_cache, patterns)
  end

  def pubsub_exact_acl_allowed?(_cache, []), do: true

  def pubsub_exact_acl_allowed?(cache, channels) do
    with :ok <- ConnAuth.check_command_cached(cache, "SUBSCRIBE"),
         :ok <- ConnAuth.check_channels_cached(cache, channels) do
      true
    else
      _ -> false
    end
  end

  def pubsub_pattern_acl_allowed?(_cache, []), do: true

  def pubsub_pattern_acl_allowed?(cache, patterns) do
    with :ok <- ConnAuth.check_command_cached(cache, "PSUBSCRIBE"),
         :ok <- ConnAuth.check_channels_cached(cache, patterns) do
      true
    else
      _ -> false
    end
  end
end
