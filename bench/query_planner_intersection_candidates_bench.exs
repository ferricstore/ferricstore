# Benchmark-only multi-index intersection prototype. Production query paths are unchanged.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.QueryPlannerIntersectionCandidates do
  @moduledoc false

  alias Ferricstore.Bench.QueryPerformance

  def run do
    inputs =
      Map.new(
        [
          {128, 10_000, 64},
          {1_000, 1_000, 100},
          {1_000, 1_000, 1_000},
          {10_000, 10_000, 100},
          {10_000, 10_000, 10_000},
          {25_000, 25_000, 100}
        ],
        fn {driving_count, membership_count, overlap_count} ->
          name =
            "driving-#{driving_count}/membership-#{membership_count}/overlap-#{overlap_count}"

          input = build_input(driving_count, membership_count, overlap_count)
          expected = current(input)
          ^expected = intersection(input)
          {name, input}
        end
      )

    Benchee.run(
      %{
        "current hydrate driving index" => &current/1,
        "candidate intersect then hydrate" => &intersection/1
      },
      [inputs: inputs] ++
        QueryPerformance.benchee_options("query-planner-intersection-candidates")
    )
  end

  defp current(%{driving: driving, records: records}) do
    Enum.reduce(driving, [], fn id, matches ->
      record = records |> Map.fetch!(id) |> :erlang.binary_to_term([:safe])
      if record.match?, do: [record.id | matches], else: matches
    end)
    |> Enum.reverse()
  end

  defp intersection(%{driving: driving, membership: membership, records: records}) do
    membership = MapSet.new(membership)

    Enum.reduce(driving, [], fn id, matches ->
      if MapSet.member?(membership, id) do
        record = records |> Map.fetch!(id) |> :erlang.binary_to_term([:safe])
        [record.id | matches]
      else
        matches
      end
    end)
    |> Enum.reverse()
  end

  defp build_input(driving_count, membership_count, overlap_count) do
    driving = Enum.map(1..driving_count, &id("run", &1))
    overlap = Enum.take(driving, overlap_count)

    extra_count = membership_count - overlap_count
    extras = if extra_count == 0, do: [], else: Enum.map(1..extra_count, &id("other", &1))
    membership = overlap ++ extras

    overlap = MapSet.new(overlap)

    records =
      Map.new(driving, fn id ->
        record = %{
          id: id,
          match?: MapSet.member?(overlap, id),
          payload: :binary.copy(<<31>>, 384)
        }

        {id, :erlang.term_to_binary(record)}
      end)

    %{driving: driving, membership: membership, records: records}
  end

  defp id(prefix, number),
    do: prefix <> "-" <> String.pad_leading(Integer.to_string(number), 8, "0")
end

Ferricstore.Bench.QueryPlannerIntersectionCandidates.run()
