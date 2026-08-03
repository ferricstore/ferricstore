defmodule FerricstoreServer.Native.Connection.PubSubCoalescer do
  @moduledoc false

  @event_overhead_bytes 256

  @type event ::
          {:pubsub_message, binary(), binary()}
          | {:pubsub_message, binary(), binary(), term()}
          | {:pubsub_pmessage, binary(), binary(), binary()}
          | {:pubsub_pmessage, binary(), binary(), binary(), term()}

  @spec collect(event(), term(), term()) :: {[event()], term() | nil}
  def collect(first, max_events, max_bytes)
      when not is_integer(max_events) or max_events <= 0 or not is_integer(max_bytes) or
             max_bytes <= 0,
      do: {[first], nil}

  def collect(first, max_events, max_bytes)
      when is_integer(max_events) and max_events > 0 and is_integer(max_bytes) and max_bytes > 0 do
    collect_ready([first], 1, event_bytes(first), max_events, max_bytes)
  end

  @spec event_bytes(event()) :: pos_integer()
  def event_bytes({:pubsub_message, channel, message}) do
    byte_size(channel) + byte_size(message) + @event_overhead_bytes
  end

  def event_bytes({:pubsub_message, channel, message, _lease}) do
    byte_size(channel) + byte_size(message) + @event_overhead_bytes
  end

  def event_bytes({:pubsub_pmessage, pattern, channel, message}) do
    byte_size(pattern) + byte_size(channel) + byte_size(message) + @event_overhead_bytes
  end

  def event_bytes({:pubsub_pmessage, pattern, channel, message, _lease}) do
    byte_size(pattern) + byte_size(channel) + byte_size(message) + @event_overhead_bytes
  end

  defp collect_ready(acc, count, bytes, max_events, max_bytes)
       when count >= max_events or bytes >= max_bytes,
       do: {Enum.reverse(acc), nil}

  defp collect_ready(acc, count, bytes, max_events, max_bytes) do
    receive do
      message ->
        case message do
          {:pubsub_message, channel, event_message} = event ->
            collect_event(
              event,
              channel,
              event_message,
              acc,
              count,
              bytes,
              max_events,
              max_bytes
            )

          {:pubsub_message, channel, event_message, _lease} = event ->
            collect_event(
              event,
              channel,
              event_message,
              acc,
              count,
              bytes,
              max_events,
              max_bytes
            )

          {:pubsub_pmessage, _pattern, _channel, _message} = event ->
            collect_event(event, acc, count, bytes, max_events, max_bytes)

          {:pubsub_pmessage, _pattern, _channel, _message, _lease} = event ->
            collect_event(event, acc, count, bytes, max_events, max_bytes)

          barrier ->
            {Enum.reverse(acc), barrier}
        end
    after
      0 -> {Enum.reverse(acc), nil}
    end
  end

  defp collect_event(event, _channel, _message, acc, count, bytes, max_events, max_bytes) do
    collect_event(event, acc, count, bytes, max_events, max_bytes)
  end

  defp collect_event(event, acc, count, bytes, max_events, max_bytes) do
    collect_ready(
      [event | acc],
      count + 1,
      bytes + event_bytes(event),
      max_events,
      max_bytes
    )
  end
end
