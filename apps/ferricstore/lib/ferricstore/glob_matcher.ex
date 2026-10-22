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
    * `[^abc]` -- negated character class (only `^` negates, as in Redis)
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
    {matched, _end_pi} = class_match(pattern, pi, sc)
    matched
  end

  defp match_char(pc, sc, _pattern, _pi), do: pc == sc

  # How many pattern bytes does the current character consume?
  defp pattern_char_len(pattern, pi) do
    pc = :binary.at(pattern, pi)

    cond do
      pc == ?\\ and pi + 1 < byte_size(pattern) ->
        2

      pc == ?[ ->
        class_end(pattern, pi) - pi

      true ->
        1
    end
  end

  # Parse [chars] or [^chars] starting at position `pos` (after the `[`)
  @doc """
  Returns the index just past the character class that starts at `pi` (the
  index of its `[`): past its closing `]`, or the end of the pattern when it
  has none.
  """
  @spec class_end(binary(), non_neg_integer()) :: non_neg_integer()
  def class_end(pattern, pi) when is_binary(pattern) and is_integer(pi) do
    {_matched, end_pi} = class_match(pattern, pi, -1)
    end_pi
  end

  # A class matches as in Redis's stringmatchlen: only `^` negates, `\x` is
  # the byte x, `x-y` matches the bytes between x and y whichever is larger
  # (so `[a-]` ranges from `]` to `a`), and a class with no closing `]` runs
  # to the end of the pattern.
  defp class_match(pattern, pi, sc) do
    plen = byte_size(pattern)
    start = pi + 1

    {negated, start} =
      if start < plen and :binary.at(pattern, start) == ?^,
        do: {true, start + 1},
        else: {false, start}

    {matched, end_pi} = scan_class(pattern, start, plen, sc, false)
    {matched != negated, end_pi}
  end

  defp scan_class(_pattern, pos, plen, _sc, matched) when pos >= plen, do: {matched, plen}

  defp scan_class(pattern, pos, plen, sc, matched) do
    byte = :binary.at(pattern, pos)

    cond do
      byte == ?\\ and plen - pos >= 2 ->
        scan_class(pattern, pos + 2, plen, sc, matched or :binary.at(pattern, pos + 1) == sc)

      byte == ?] ->
        {matched, pos + 1}

      plen - pos >= 3 and :binary.at(pattern, pos + 1) == ?- ->
        {low, high} = Enum.min_max([byte, :binary.at(pattern, pos + 2)])
        scan_class(pattern, pos + 3, plen, sc, matched or (sc >= low and sc <= high))

      true ->
        scan_class(pattern, pos + 1, plen, sc, matched or byte == sc)
    end
  end
end
