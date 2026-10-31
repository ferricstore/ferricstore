defmodule Ferricstore.Commands.Lcs do
  @moduledoc """
  LCS: the longest common subsequence of two strings, as Redis answers it
  (the subsequence, LEN, or IDX with MINMATCHLEN and WITHMATCHLEN).

  The answer comes from the dynamic-programming table of prefix lengths,
  O(len(a) * len(b)) time; LEN keeps two rows, the other forms the whole
  table to walk it back. Strings whose lengths multiply past
  #{16_777_216} cells are refused, a bound on the work and memory one
  command takes.
  """

  alias Ferricstore.Commands.Strings

  @max_cells 16_777_216
  @string_values_error "ERR The specified keys must contain string values"

  @spec handle([binary()], map()) :: term()
  def handle([key_a, key_b | options], store) do
    with {:ok, opts} <- parse_options(options, %{len: false, idx: false, min: 0, withlen: false}),
         {:ok, a} <- string_value(key_a, store),
         {:ok, b} <- string_value(key_b, store),
         :ok <- check_size(a, b) do
      answer(a, b, opts)
    end
  end

  def handle(_args, _store), do: {:error, "ERR wrong number of arguments for 'lcs' command"}

  defp parse_options([], %{len: true, idx: true}),
    do: {:error, "ERR If you want both the length and indexes, please just use IDX."}

  defp parse_options([], opts), do: {:ok, opts}

  defp parse_options([option | rest], opts) do
    case String.upcase(option) do
      "LEN" ->
        parse_options(rest, %{opts | len: true})

      "IDX" ->
        parse_options(rest, %{opts | idx: true})

      "WITHMATCHLEN" ->
        parse_options(rest, %{opts | withlen: true})

      "MINMATCHLEN" ->
        case rest do
          [raw | rest] ->
            case Integer.parse(raw) do
              {min, ""} -> parse_options(rest, %{opts | min: max(min, 0)})
              _invalid -> {:error, "ERR value is not an integer or out of range"}
            end

          [] ->
            {:error, "ERR syntax error"}
        end

      _unknown ->
        {:error, "ERR syntax error"}
    end
  end

  defp string_value(key, store) do
    case Strings.handle_ast({:get, key}, store) do
      nil -> {:ok, ""}
      value when is_binary(value) -> {:ok, value}
      {:error, "WRONGTYPE" <> _rest} -> {:error, @string_values_error}
      {:error, _reason} = error -> error
    end
  end

  defp check_size(a, b) do
    if (byte_size(a) + 1) * (byte_size(b) + 1) > @max_cells,
      do: {:error, "ERR LCS of strings whose lengths multiply past #{@max_cells} is refused"},
      else: :ok
  end

  defp answer(a, b, %{len: true, idx: false}), do: last_row_length(a, b)

  defp answer(a, b, %{idx: idx} = opts) do
    rows = table(a, b)
    {lcs, matches} = walk_back(a, b, rows, opts)

    if idx,
      do: %{"matches" => matches, "len" => byte_size(lcs)},
      else: lcs
  end

  # LCS(alen, blen), keeping two rows.
  defp last_row_length(a, b) do
    first = zero_row(b)

    last =
      for <<char <- a>>, reduce: first do
        previous -> next_row(char, b, previous)
      end

    cell(last, byte_size(b))
  end

  # Row i holds LCS(i, j) for every j, as 32-bit integers: the lengths of
  # the common subsequences of a's first i and b's first j bytes.
  defp table(a, b) do
    {rows, _last} =
      for <<char <- a>>, reduce: {[zero_row(b)], zero_row(b)} do
        {rows, previous} ->
          row = next_row(char, b, previous)
          {[row | rows], row}
      end

    rows |> Enum.reverse() |> List.to_tuple()
  end

  defp zero_row(b), do: :binary.copy(<<0::32>>, byte_size(b) + 1)

  defp next_row(char, b, previous), do: next_row(char, b, previous, 0, [<<0::32>>])

  defp next_row(_char, <<>>, _previous, _left, acc),
    do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  defp next_row(char, <<other, b::binary>>, <<diagonal::32, previous::binary>>, left, acc) do
    <<up::32, _rest::binary>> = previous
    length = if char == other, do: diagonal + 1, else: max(up, left)
    next_row(char, b, previous, length, [<<length::32>> | acc])
  end

  defp cell(row, j), do: :binary.decode_unsigned(binary_part(row, j * 4, 4))
  defp lcs_at(rows, i, j), do: rows |> elem(i) |> cell(j)

  # Redis's walk back from the end of both strings: a common byte joins the
  # subsequence and the current range of matches; otherwise the walk steps
  # toward the longer prefix subsequence and the open range is emitted.
  defp walk_back(a, b, rows, opts) do
    state = %{range: nil, matches: [], lcs: []}
    walk_back(a, b, rows, byte_size(a), byte_size(b), opts, state)
  end

  defp walk_back(_a, _b, _rows, i, j, _opts, state) when i == 0 or j == 0,
    do: {IO.iodata_to_binary(state.lcs), Enum.reverse(state.matches)}

  defp walk_back(a, b, rows, i, j, opts, state) do
    if :binary.at(a, i - 1) == :binary.at(b, j - 1) do
      {range, emit?} =
        case state.range do
          nil ->
            {{i - 1, i - 1, j - 1, j - 1}, false}

          {a_start, a_end, b_start, b_end} when a_start == i and b_start == j ->
            {{a_start - 1, a_end, b_start - 1, b_end}, false}

          range ->
            {range, true}
        end

      {a_start, _a_end, b_start, _b_end} = range
      emit? = emit? or a_start == 0 or b_start == 0
      state = %{state | range: range, lcs: [:binary.at(a, i - 1) | state.lcs]}
      walk_back(a, b, rows, i - 1, j - 1, opts, maybe_emit(state, emit?, opts))
    else
      {i, j} =
        if lcs_at(rows, i - 1, j) > lcs_at(rows, i, j - 1), do: {i - 1, j}, else: {i, j - 1}

      walk_back(a, b, rows, i, j, opts, maybe_emit(state, state.range != nil, opts))
    end
  end

  defp maybe_emit(state, false, _opts), do: state

  defp maybe_emit(%{range: {a_start, a_end, b_start, b_end}} = state, true, opts) do
    match_length = a_end - a_start + 1

    matches =
      if opts.min == 0 or match_length >= opts.min do
        match = [[a_start, a_end], [b_start, b_end]]
        match = if opts.withlen, do: match ++ [match_length], else: match
        [match | state.matches]
      else
        state.matches
      end

    %{state | range: nil, matches: matches}
  end
end
