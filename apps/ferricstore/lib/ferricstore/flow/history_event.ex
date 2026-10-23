defmodule Ferricstore.Flow.HistoryEvent do
  @moduledoc false

  @max_u64 18_446_744_073_709_551_615

  def ms(event_id) when is_binary(event_id) do
    case Integer.parse(event_id) do
      {ms, "-" <> _rest} -> ms
      {ms, ""} -> ms
      _ -> 0
    end
  end

  def ms(_event_id), do: 0

  @doc false
  # The version of a "<ms>-<version>" event id, or 0 for any other id, as the
  # native Flow index ranks history members.
  def version(event_id) when is_binary(event_id) do
    with [ms, version] <- :binary.split(event_id, "-"),
         true <- digits?(ms) and digits?(version),
         number when number <= @max_u64 <- String.to_integer(version) do
      number
    else
      _not_generated -> 0
    end
  end

  def version(_event_id), do: 0

  @doc false
  # A Flow's events in order: by millisecond, then version. The ids of one
  # millisecond share their "<ms>-" prefix, so by bytes alone "1000-10" would
  # precede "1000-9".
  def sort_key(event_id), do: {ms(event_id), version(event_id), event_id}

  @doc false
  # sort_key/1 for an {event_id, score} reference, whose score is the event's
  # millisecond.
  def ref_sort_key({event_id, score}), do: {score, version(event_id), event_id}

  defp digits?(<<digit, rest::binary>>) when digit in ?0..?9, do: rest == "" or digits?(rest)
  defp digits?(_value), do: false
end
