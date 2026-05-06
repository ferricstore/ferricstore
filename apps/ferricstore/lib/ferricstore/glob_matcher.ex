defmodule Ferricstore.GlobMatcher do
  @moduledoc """
  Linear-time binary glob matcher.

  Uses a two-pointer algorithm that avoids exponential backtracking on
  patterns like `*a*a*a*a*b` (CVE-2022-36021, CVE-2024-31228).

  Supports:
    * `*` -- matches zero or more bytes
    * `?` -- matches exactly one byte
    * `[abc]` -- character class (matches any single listed byte)
    * `[a-z]` -- character range
    * `[^abc]` / `[!abc]` -- negated character class
    * `\\` -- escape next character (treat literally)
    * All other bytes match literally

  ## Examples

      iex> Ferricstore.GlobMatcher.match?("hello", "h*o")
      true

      iex> Ferricstore.GlobMatcher.match?("hello", "h?llo")
      true

      iex> Ferricstore.GlobMatcher.match?("hello", "world")
      false
  """

  # Maximum pattern length to prevent DoS via extremely long patterns.
  @max_pattern_length 1024

  @doc """
  Returns `true` if `subject` matches the glob `pattern`.

  Patterns longer than #{@max_pattern_length} bytes always return `false`.
  """
  @spec match?(binary(), binary()) :: boolean()
  def match?(subject, pattern) when is_binary(subject) and is_binary(pattern) do
    if byte_size(pattern) > @max_pattern_length do
      false
    else
      do_match(subject, 0, byte_size(subject), pattern, 0, byte_size(pattern))
    end
  end

  # Linear-time glob matching using the "star restart" technique.
  # When a `*` is encountered, we record the position. If a later literal
  # fails to match, we backtrack to the star position and try consuming
  # one more byte from the subject. This is O(n*m) worst case but avoids
  # exponential branching because we only ever track ONE star restart point
  # (the most recent `*`).
  defp do_match(subject, si, slen, pattern, pi, plen) do
    do_match_loop(subject, si, slen, pattern, pi, plen, -1, -1)
  end

  defp do_match_loop(subject, si, slen, pattern, pi, plen, star_pi, star_si) do
    cond do
      pi == plen and si == slen ->
        # Both exhausted — match
        true

      pi < plen and :binary.at(pattern, pi) == ?* ->
        # Star: record restart point, advance pattern, try zero-length match
        do_match_loop(subject, si, slen, pattern, pi + 1, plen, pi + 1, si)

      pi < plen and si < slen and match_one?(subject, si, pattern, pi) ->
        # Current bytes match — advance both
        skip = pattern_char_len(pattern, pi)
        do_match_loop(subject, si + 1, slen, pattern, pi + skip, plen, star_pi, star_si)

      star_pi >= 0 and star_si < slen ->
        # Mismatch but we have a star — consume one more subject byte
        new_star_si = star_si + 1
        do_match_loop(subject, new_star_si, slen, pattern, star_pi, plen, star_pi, new_star_si)

      true ->
        # No star to backtrack to, and bytes don't match
        false
    end
  end

  # Check if subject[si] matches pattern[pi] (one character/class)
  defp match_one?(subject, si, pattern, pi) do
    pc = :binary.at(pattern, pi)
    sc = :binary.at(subject, si)
    match_char(pc, sc, pattern, pi)
  end

  defp match_char(??, _sc, _pattern, _pi), do: true

  defp match_char(?\\, sc, pattern, pi) do
    if pi + 1 < byte_size(pattern), do: :binary.at(pattern, pi + 1) == sc, else: ?\\ == sc
  end

  defp match_char(?[, sc, pattern, pi) do
    case parse_char_class_at(pattern, pi + 1) do
      {:ok, chars, negated, _end_pi} ->
        in_class = sc in chars
        (negated and not in_class) or (not negated and in_class)

      :error ->
        ?[ == sc
    end
  end

  defp match_char(pc, sc, _pattern, _pi), do: pc == sc

  # How many pattern bytes does the current character consume?
  defp pattern_char_len(pattern, pi) do
    pc = :binary.at(pattern, pi)

    cond do
      pc == ?\\ and pi + 1 < byte_size(pattern) ->
        2

      pc == ?[ ->
        case parse_char_class_at(pattern, pi + 1) do
          {:ok, _chars, _neg, end_pi} -> end_pi - pi
          :error -> 1
        end

      true ->
        1
    end
  end

  # Parse [chars] or [^chars] starting at position `pos` (after the `[`)
  defp parse_char_class_at(pattern, pos) do
    plen = byte_size(pattern)

    {negated, start} =
      cond do
        pos < plen and :binary.at(pattern, pos) in [?^, ?!] -> {true, pos + 1}
        true -> {false, pos}
      end

    collect_class_chars_at(pattern, start, plen, [], negated)
  end

  defp collect_class_chars_at(_pattern, pos, plen, _acc, _negated) when pos >= plen, do: :error

  defp collect_class_chars_at(pattern, pos, plen, acc, negated) do
    c = :binary.at(pattern, pos)

    cond do
      c == ?] ->
        {:ok, expand_class_tokens(Enum.reverse(acc)), negated, pos + 1}

      c == ?\\ and pos + 1 < plen ->
        collect_class_chars_at(
          pattern,
          pos + 2,
          plen,
          [{:escaped, :binary.at(pattern, pos + 1)} | acc],
          negated
        )

      true ->
        collect_class_chars_at(pattern, pos + 1, plen, [{:lit, c} | acc], negated)
    end
  end

  defp expand_class_tokens(tokens), do: do_expand_class_tokens(tokens, [])

  defp do_expand_class_tokens([{:lit, first}, {:lit, ?-}, {:lit, last} | rest], acc)
       when first <= last do
    do_expand_class_tokens(rest, add_class_range(first, last, acc))
  end

  defp do_expand_class_tokens([{:lit, c} | rest], acc),
    do: do_expand_class_tokens(rest, [c | acc])

  defp do_expand_class_tokens([{:escaped, c} | rest], acc),
    do: do_expand_class_tokens(rest, [c | acc])

  defp do_expand_class_tokens([], acc), do: acc

  defp add_class_range(first, last, acc) when first > last, do: acc

  defp add_class_range(first, last, acc), do: add_class_range(first + 1, last, [first | acc])
end
