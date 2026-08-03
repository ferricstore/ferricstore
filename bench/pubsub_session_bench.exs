# FerricStore native-session subscription acknowledgement benchmark.
#
# Run:
#   MIX_ENV=bench mix bench.pubsub_session

Logger.configure(level: :warning)

env_number = fn name, default ->
  case Float.parse(System.get_env(name, default)) do
    {value, ""} when value >= 0 -> value
    _ -> raise ArgumentError, "#{name} must be a non-negative number"
  end
end

batch_sizes =
  System.get_env("BENCH_PUBSUB_SESSION_BATCHES", "256,1024")
  |> String.split(",", trim: true)
  |> Enum.map(fn raw ->
    case Integer.parse(String.trim(raw)) do
      {value, ""} when value >= 1 -> value
      _ -> raise ArgumentError, "BENCH_PUBSUB_SESSION_BATCHES must contain integers >= 1"
    end
  end)
  |> Enum.uniq()

warmup = env_number.("BENCH_WARMUP", "1")
time = env_number.("BENCH_TIME", "3")

defmodule Ferricstore.Bench.PubSubSession do
  @moduledoc false

  def current_subscribe(values) do
    Enum.map_reduce(values, state(), fn value, acc ->
      retained = Map.fetch!(acc, :channels)
      acc = %{acc | channels: MapSet.put(retained, value), bytes: acc.bytes + byte_size(value)}
      {["subscribe", value, count(acc)], acc}
    end)
  end

  def candidate_subscribe(values) do
    retained = MapSet.new(values)
    state = %{state() | channels: retained, bytes: byte_size_sum(values)}

    {acks, _count} =
      Enum.map_reduce(values, 0, fn value, count ->
        count = count + 1
        {["subscribe", value, count], count}
      end)

    {acks, state}
  end

  def current_unsubscribe(values, state) do
    Enum.map_reduce(values, state, fn value, acc ->
      retained = Map.fetch!(acc, :channels)

      acc =
        %{
          acc
          | channels: MapSet.delete(retained, value),
            bytes: max(acc.bytes - byte_size(value), 0)
        }

      {["unsubscribe", value, count(acc)], acc}
    end)
  end

  def candidate_unsubscribe(values, state) do
    retained = Map.fetch!(state, :channels)

    {acks, final, bytes, _count} =
      Enum.reduce(
        values,
        {[], retained, state.bytes, MapSet.size(retained)},
        fn value, {acks, acc, bytes, count} ->
          if MapSet.member?(acc, value) do
            count = count - 1

            {[["unsubscribe", value, count] | acks], MapSet.delete(acc, value),
             max(bytes - byte_size(value), 0), count}
          else
            {[["unsubscribe", value, count] | acks], acc, bytes, count}
          end
        end
      )

    {Enum.reverse(acks), %{state | channels: final, bytes: bytes}}
  end

  def subscribed_state(values) do
    %{state() | channels: MapSet.new(values), bytes: byte_size_sum(values)}
  end

  defp state, do: %{channels: MapSet.new(), patterns: MapSet.new(), bytes: 0}
  defp count(state), do: MapSet.size(state.channels) + MapSet.size(state.patterns)
  defp byte_size_sum(values), do: Enum.reduce(values, 0, &(byte_size(&1) + &2))
end

options = [
  warmup: warmup,
  time: time,
  memory_time: 0,
  reduction_time: 0,
  parallel: 1,
  percentiles: [50, 95, 99],
  formatters: [Benchee.Formatters.Console]
]

IO.puts("=== FerricStore Native Pub/Sub Session Benchmark ===")

Enum.each(batch_sizes, fn batch_size ->
  values = for index <- 1..batch_size, do: "bench:session:#{index}"
  subscribed = Ferricstore.Bench.PubSubSession.subscribed_state(values)

  Benchee.run(
    %{
      "current subscribe ACK batch=#{batch_size}" => fn ->
        Ferricstore.Bench.PubSubSession.current_subscribe(values)
      end,
      "candidate subscribe ACK batch=#{batch_size}" => fn ->
        Ferricstore.Bench.PubSubSession.candidate_subscribe(values)
      end,
      "current unsubscribe ACK batch=#{batch_size}" => fn ->
        Ferricstore.Bench.PubSubSession.current_unsubscribe(values, subscribed)
      end,
      "candidate unsubscribe ACK batch=#{batch_size}" => fn ->
        Ferricstore.Bench.PubSubSession.candidate_unsubscribe(values, subscribed)
      end
    },
    options
  )
end)
