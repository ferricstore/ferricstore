defmodule Ferricstore.BatchResult do
  @moduledoc false

  @type mismatch_reason ::
          {:batch_result_mismatch, non_neg_integer(), non_neg_integer()}
          | {:invalid_batch_results, term()}

  @spec map_exact(list(), term(), (term(), term() -> term())) ::
          {:ok, list()} | {:error, mismatch_reason()}
  def map_exact(expected, results, mapper)
      when is_list(expected) and is_list(results) and is_function(mapper, 2) do
    case matching_counts(expected, results, 0) do
      :ok -> {:ok, Enum.zip_with(expected, results, mapper)}
      {:error, _reason} = error -> error
    end
  end

  def map_exact(expected, results, mapper)
      when is_list(expected) and is_function(mapper, 2) do
    {:error, {:invalid_batch_results, results}}
  end

  defp matching_counts([], [], _seen), do: :ok

  defp matching_counts([], results, seen),
    do: {:error, {:batch_result_mismatch, seen, seen + length(results)}}

  defp matching_counts(expected, [], seen),
    do: {:error, {:batch_result_mismatch, seen + length(expected), seen}}

  defp matching_counts([_expected | expected_rest], [_result | result_rest], seen),
    do: matching_counts(expected_rest, result_rest, seen + 1)
end
