defmodule Ferricstore.Commands.SortedSet.Helpers do
  @moduledoc false

  alias Ferricstore.Commands.CollectionScan

  @max_random_replacement_count 10_000

  def parse_stored_score(score_str) do
    case Float.parse(score_str) do
      {score, ""} -> score
      _ -> 0.0
    end
  rescue
    ArgumentError -> 0.0
  end

  def raw_score_bound(:neg_infinity, _exclusive), do: :neg_inf
  def raw_score_bound(:infinity, _exclusive), do: :inf
  def raw_score_bound(score, true), do: {:exclusive, score}
  def raw_score_bound(score, false), do: {:inclusive, score}

  def normalize_index(index, len) when index < 0, do: max(0, len + index)
  def normalize_index(index, _len), do: index

  def format_score(score) when is_float(score) do
    :erlang.float_to_binary(score, [:compact, decimals: 17])
  end

  def format_score_str(score_str) do
    case Float.parse(score_str) do
      {score, ""} -> format_score(score)
      _ -> score_str
    end
  rescue
    ArgumentError -> score_str
  end

  def score_pairs_to_flat_list(pairs), do: score_pairs_to_flat_list(pairs, [])
  def score_string_pairs_to_flat_list(pairs), do: score_string_pairs_to_flat_list(pairs, [])

  def parse_zadd_opts(args) do
    parse_zadd_opts(args, %{nx: false, xx: false, gt: false, lt: false, ch: false})
  end

  def parse_score(str) do
    case Float.parse(str) do
      {score, ""} ->
        {:ok, score}

      _ ->
        case Integer.parse(str) do
          {int, ""} -> {:ok, int * 1.0}
          _ -> :error
        end
    end
  rescue
    ArgumentError -> :error
    ArithmeticError -> :error
  end

  def checked_score_add(left, right) when is_float(left) and is_float(right) do
    {:ok, left + right}
  rescue
    ArithmeticError -> :overflow
  end

  def parse_score_bound("-inf"), do: {:ok, :neg_infinity, false}
  def parse_score_bound("+inf"), do: {:ok, :infinity, false}
  def parse_score_bound("inf"), do: {:ok, :infinity, false}

  def parse_score_bound("(" <> rest) do
    case parse_score(rest) do
      {:ok, score} -> {:ok, score, true}
      :error -> :error
    end
  end

  def parse_score_bound(str) do
    case parse_score(str) do
      {:ok, score} -> {:ok, score, false}
      :error -> :error
    end
  end

  def score_gte?(_score, :neg_infinity, _exclusive), do: true
  def score_gte?(_score, :infinity, _exclusive), do: false
  def score_gte?(score, bound, true), do: score > bound
  def score_gte?(score, bound, false), do: score >= bound

  def score_lte?(_score, :infinity, _exclusive), do: true
  def score_lte?(_score, :neg_infinity, _exclusive), do: false
  def score_lte?(score, bound, true), do: score < bound
  def score_lte?(score, bound, false), do: score <= bound

  def score_gte_bound?(_score, :neg_inf), do: true
  def score_gte_bound?(_score, :inf), do: false
  def score_gte_bound?(score, {:exclusive, bound}), do: score > bound
  def score_gte_bound?(score, {:inclusive, bound}), do: score >= bound

  def score_lte_bound?(_score, :inf), do: true
  def score_lte_bound?(_score, :neg_inf), do: false
  def score_lte_bound?(score, {:exclusive, bound}), do: score < bound
  def score_lte_bound?(score, {:inclusive, bound}), do: score <= bound

  def parse_range_by_score_opts(opts), do: do_parse_range_by_score_opts(opts, false, 0, :all)

  def apply_limit(list, 0, :all), do: list
  def apply_limit(list, offset, :all), do: Enum.drop(list, offset)
  def apply_limit(list, offset, count), do: list |> Enum.drop(offset) |> Enum.take(count)

  def typed_range_by_score_opts(opts), do: do_typed_range_by_score_opts(opts, false, 0, :all)
  def typed_scan_opts(opts), do: do_typed_scan_opts(opts, nil, 10)

  def parse_cursor(cursor_str), do: CollectionScan.parse_cursor(cursor_str)

  def parse_zscan_opts(opts), do: do_parse_zscan_opts(opts, nil, 10)

  def select_random_members(_pairs, count, _with_scores)
      when count < -@max_random_replacement_count,
      do: {:error, "ERR count exceeds maximum allowed response size"}

  def select_random_members(pairs, count, with_scores) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        selected = Enum.take_random(pairs, count)

        if with_scores do
          score_string_pairs_to_flat_list(selected)
        else
          Enum.map(selected, fn {member, _score} -> member end)
        end

      count < 0 ->
        abs_count = abs(count)

        if pairs == [] do
          []
        else
          tuple = List.to_tuple(pairs)
          size = tuple_size(tuple)
          selected = for _ <- 1..abs_count, do: elem(tuple, :rand.uniform(size) - 1)

          if with_scores do
            score_string_pairs_to_flat_list(selected)
          else
            Enum.map(selected, fn {member, _score} -> member end)
          end
        end
    end
  end

  defp score_pairs_to_flat_list([{member, score} | pairs], acc) do
    score_pairs_to_flat_list(pairs, [format_score(score), member | acc])
  end

  defp score_pairs_to_flat_list([], acc), do: Enum.reverse(acc)

  defp score_string_pairs_to_flat_list([{member, score} | pairs], acc) do
    score_string_pairs_to_flat_list(pairs, [format_score_str(score), member | acc])
  end

  defp score_string_pairs_to_flat_list([], acc), do: Enum.reverse(acc)

  defp parse_zadd_opts(["NX" | rest], opts), do: parse_zadd_opts(rest, %{opts | nx: true})
  defp parse_zadd_opts(["XX" | rest], opts), do: parse_zadd_opts(rest, %{opts | xx: true})
  defp parse_zadd_opts(["GT" | rest], opts), do: parse_zadd_opts(rest, %{opts | gt: true})
  defp parse_zadd_opts(["LT" | rest], opts), do: parse_zadd_opts(rest, %{opts | lt: true})
  defp parse_zadd_opts(["CH" | rest], opts), do: parse_zadd_opts(rest, %{opts | ch: true})

  defp parse_zadd_opts([opt | rest], opts) when is_binary(opt) do
    case String.upcase(opt) do
      normalized when normalized in ["NX", "XX", "GT", "LT", "CH"] ->
        parse_zadd_opts([normalized | rest], opts)

      _not_option ->
        parse_zadd_score_members([opt | rest], opts)
    end
  end

  defp parse_zadd_opts(score_member_args, opts),
    do: parse_zadd_score_members(score_member_args, opts)

  defp parse_zadd_score_members(score_member_args, opts) do
    cond do
      opts.nx and opts.xx ->
        {:error, "ERR XX and NX options at the same time are not compatible"}

      opts.gt and opts.lt ->
        {:error, "ERR GT, LT, and NX options at the same time are not compatible"}

      opts.gt and opts.nx ->
        {:error, "ERR GT, LT, and NX options at the same time are not compatible"}

      opts.lt and opts.nx ->
        {:error, "ERR GT, LT, and NX options at the same time are not compatible"}

      rem(length(score_member_args), 2) != 0 or score_member_args == [] ->
        {:error, "ERR wrong number of arguments for 'zadd' command"}

      true ->
        pairs =
          score_member_args
          |> Enum.chunk_every(2)
          |> Enum.reduce_while([], fn [score_str, member], acc ->
            case parse_score(score_str) do
              {:ok, score} -> {:cont, [{score, member} | acc]}
              :error -> {:halt, :error}
            end
          end)

        case pairs do
          :error -> {:error, "ERR value is not a valid float"}
          pairs -> {:ok, opts, Enum.reverse(pairs)}
        end
    end
  end

  defp do_parse_range_by_score_opts([], ws, offset, count), do: {ws, offset, count}

  defp do_parse_range_by_score_opts([opt | rest], ws, offset, count) do
    case String.upcase(opt) do
      "WITHSCORES" ->
        do_parse_range_by_score_opts(rest, true, offset, count)

      "LIMIT" ->
        case rest do
          [offset_str, count_str | remaining] ->
            with {off, ""} <- Integer.parse(offset_str),
                 {cnt, ""} <- Integer.parse(count_str) do
              if off < 0 do
                {:error, "ERR syntax error"}
              else
                real_count = if cnt < 0, do: :all, else: cnt
                do_parse_range_by_score_opts(remaining, ws, off, real_count)
              end
            else
              _ -> {:error, "ERR value is not an integer or out of range"}
            end

          _ ->
            {:error, "ERR syntax error"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_typed_range_by_score_opts([], with_scores, offset, count),
    do: {:ok, with_scores, offset, count}

  defp do_typed_range_by_score_opts([{:withscores, true} | rest], _with_scores, offset, count),
    do: do_typed_range_by_score_opts(rest, true, offset, count)

  defp do_typed_range_by_score_opts(
         [{:limit, {offset, count}} | rest],
         with_scores,
         _offset,
         _count
       )
       when is_integer(offset) and offset >= 0 and is_integer(count) do
    do_typed_range_by_score_opts(rest, with_scores, offset, if(count < 0, do: :all, else: count))
  end

  defp do_typed_range_by_score_opts(_opts, _with_scores, _offset, _count),
    do: {:error, "ERR syntax error"}

  defp do_typed_scan_opts([], match, count), do: {:ok, match, count}

  defp do_typed_scan_opts([{:match, pattern} | rest], _match, count) when is_binary(pattern) do
    do_typed_scan_opts(rest, pattern, count)
  end

  defp do_typed_scan_opts([{:count, count} | rest], match, _count)
       when is_integer(count) and count > 0 do
    do_typed_scan_opts(rest, match, count)
  end

  defp do_typed_scan_opts(_opts, _match, _count), do: {:error, "ERR syntax error"}

  defp do_parse_zscan_opts([], match, count), do: {:ok, match, count}

  defp do_parse_zscan_opts([opt, value | rest], match, count) do
    case String.upcase(opt) do
      "MATCH" ->
        do_parse_zscan_opts(rest, value, count)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> do_parse_zscan_opts(rest, match, n)
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_parse_zscan_opts([_ | _], _match, _count), do: {:error, "ERR syntax error"}
end
