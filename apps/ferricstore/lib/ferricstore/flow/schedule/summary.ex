defmodule Ferricstore.Flow.Schedule.Summary do
  @moduledoc false

  @schedule_id_prefix "__ferricstore_schedule__:"
  @max_exact_integer 9_007_199_254_740_991

  @spec fire_results([{map(), term()}], binary() | nil) :: {:ok, map()}
  def fire_results(results, claim_error)
      when is_list(results) and (is_binary(claim_error) or is_nil(claim_error)) do
    summary =
      Enum.reduce(
        results,
        %{
          claimed: 0,
          fired: 0,
          skipped: 0,
          coalesced: 0,
          errors: []
        },
        fn {record, fire_result}, acc ->
          acc
          |> Map.update!(:claimed, &(&1 + 1))
          |> then(&summarize(record, fire_result, &1))
        end
      )

    summary =
      if is_binary(claim_error), do: Map.put(summary, :claim_error, claim_error), else: summary

    {:ok, %{summary | errors: Enum.reverse(summary.errors)}}
  end

  defp summarize(_record, {:ok, target_id, coalesced_count}, acc)
       when is_binary(target_id) and is_integer(coalesced_count) and coalesced_count >= 0 do
    acc
    |> Map.update!(:fired, &(&1 + 1))
    |> add_coalesced_count(coalesced_count)
    |> Map.put(:last_target_id, target_id)
  end

  defp summarize(_record, {:skipped, reason, coalesced_count}, acc)
       when is_binary(reason) and is_integer(coalesced_count) and coalesced_count >= 0 do
    acc
    |> Map.update!(:skipped, &(&1 + 1))
    |> add_coalesced_count(coalesced_count)
    |> Map.put(:last_skip_reason, reason)
  end

  defp summarize(record, {:error, reason}, acc) when is_binary(reason),
    do: add_error(acc, record, reason)

  defp summarize(record, _invalid_result, acc),
    do: add_error(acc, record, "ERR schedule execution failed")

  defp add_error(acc, record, reason),
    do: %{acc | errors: [{schedule_error_id(record), reason} | acc.errors]}

  defp schedule_error_id(%{payload: %{id: id}}) when is_binary(id) and id != "", do: id
  defp schedule_error_id(%{id: @schedule_id_prefix <> id}) when id != "", do: id
  defp schedule_error_id(%{id: id}) when is_binary(id) and id != "", do: id
  defp schedule_error_id(_record), do: "unknown"

  defp add_coalesced_count(result, count) do
    Map.update!(result, :coalesced, &min(&1 + count, @max_exact_integer))
  end
end
