defmodule FerricstoreServer.Connection.PubSub do
  @moduledoc """
  PubSub command handling extracted from Connection.

  All functions accept and return the connection state as a map/struct.
  """

  alias Ferricstore.PubSub, as: PS
  alias FerricstoreServer.Resp.Encoder

  # Max channels + patterns per connection to prevent per-connection heap exhaustion.
  @max_subscriptions 100_000

  # ── SUBSCRIBE ──────────────────────────────────────────────────────────

  @spec dispatch_subscribe([binary()], map()) ::
          {:continue, iodata(), map()} | {:quit, iodata(), map()}
  def dispatch_subscribe([], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'subscribe' command"}),
     state}
  end

  def dispatch_subscribe(channels, state) do
    # Lazily initialize MapSets on first subscribe (memory audit L3).
    state = ensure_pubsub_sets(state)

    current_count = MapSet.size(state.pubsub_channels) + MapSet.size(state.pubsub_patterns)

    new_channel_count =
      channels |> MapSet.new() |> MapSet.difference(state.pubsub_channels) |> MapSet.size()

    if current_count + new_channel_count > @max_subscriptions do
      {:continue,
       Encoder.encode(
         {:error, "ERR max subscriptions per connection (#{@max_subscriptions}) reached"}
       ), state}
    else
      {responses, new_state} =
        Enum.reduce(channels, {[], state}, fn ch, {acc, st} ->
          unless MapSet.member?(st.pubsub_channels, ch), do: PS.subscribe(ch, self())
          new_channels = MapSet.put(st.pubsub_channels, ch)
          new_st = %{st | pubsub_channels: new_channels}
          count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
          push = {:push, ["subscribe", ch, count]}
          {[Encoder.encode(push) | acc], new_st}
        end)

      {:continue, Enum.reverse(responses), new_state}
    end
  end

  # ── UNSUBSCRIBE ────────────────────────────────────────────────────────

  @spec dispatch_unsubscribe([binary()], map()) ::
          {:continue, iodata(), map()} | {:quit, iodata(), map()}
  def dispatch_unsubscribe([], state) do
    if state.pubsub_channels == nil do
      {:continue, [], state}
    else
      dispatch_unsubscribe(MapSet.to_list(state.pubsub_channels), state)
    end
  end

  def dispatch_unsubscribe(channels, state) do
    state = ensure_pubsub_sets(state)

    {responses, new_state} =
      Enum.reduce(channels, {[], state}, fn ch, {acc, st} ->
        PS.unsubscribe(ch, self())
        new_channels = MapSet.delete(st.pubsub_channels, ch)
        new_st = %{st | pubsub_channels: new_channels}
        count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
        push = {:push, ["unsubscribe", ch, count]}
        {[Encoder.encode(push) | acc], new_st}
      end)

    {:continue, Enum.reverse(responses), new_state}
  end

  # ── PSUBSCRIBE ─────────────────────────────────────────────────────────

  @spec dispatch_psubscribe([binary()], map()) ::
          {:continue, iodata(), map()} | {:quit, iodata(), map()}
  def dispatch_psubscribe([], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'psubscribe' command"}), state}
  end

  def dispatch_psubscribe(patterns, state) do
    state = ensure_pubsub_sets(state)

    current_count = MapSet.size(state.pubsub_channels) + MapSet.size(state.pubsub_patterns)

    new_pattern_count =
      patterns |> MapSet.new() |> MapSet.difference(state.pubsub_patterns) |> MapSet.size()

    if current_count + new_pattern_count > @max_subscriptions do
      {:continue,
       Encoder.encode(
         {:error, "ERR max subscriptions per connection (#{@max_subscriptions}) reached"}
       ), state}
    else
      {responses, new_state} =
        Enum.reduce(patterns, {[], state}, fn pat, {acc, st} ->
          unless MapSet.member?(st.pubsub_patterns, pat), do: PS.psubscribe(pat, self())
          new_patterns = MapSet.put(st.pubsub_patterns, pat)
          new_st = %{st | pubsub_patterns: new_patterns}
          count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
          push = {:push, ["psubscribe", pat, count]}
          {[Encoder.encode(push) | acc], new_st}
        end)

      {:continue, Enum.reverse(responses), new_state}
    end
  end

  # ── PUNSUBSCRIBE ───────────────────────────────────────────────────────

  @spec dispatch_punsubscribe([binary()], map()) ::
          {:continue, iodata(), map()} | {:quit, iodata(), map()}
  def dispatch_punsubscribe([], state) do
    if state.pubsub_patterns == nil do
      {:continue, [], state}
    else
      dispatch_punsubscribe(MapSet.to_list(state.pubsub_patterns), state)
    end
  end

  def dispatch_punsubscribe(patterns, state) do
    state = ensure_pubsub_sets(state)

    {responses, new_state} =
      Enum.reduce(patterns, {[], state}, fn pat, {acc, st} ->
        PS.punsubscribe(pat, self())
        new_patterns = MapSet.delete(st.pubsub_patterns, pat)
        new_st = %{st | pubsub_patterns: new_patterns}
        count = MapSet.size(new_st.pubsub_channels) + MapSet.size(new_st.pubsub_patterns)
        push = {:push, ["punsubscribe", pat, count]}
        {[Encoder.encode(push) | acc], new_st}
      end)

    {:continue, Enum.reverse(responses), new_state}
  end

  # ── Private ────────────────────────────────────────────────────────────

  defp ensure_pubsub_sets(%{pubsub_channels: nil} = state) do
    %{state | pubsub_channels: MapSet.new(), pubsub_patterns: MapSet.new()}
  end

  defp ensure_pubsub_sets(state), do: state
end
