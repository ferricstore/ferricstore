defmodule FerricstoreServer.Acl.Patterns do
  @moduledoc false

  @spec key_matches_any?(binary(), :read | :write, list()) :: boolean()
  def key_matches_any?(_key, _access_type, []), do: false

  def key_matches_any?(key, access_type, [{_glob, mode, regex} | rest]) do
    if access_permitted?(mode, access_type) and Regex.match?(regex, key) do
      true
    else
      key_matches_any?(key, access_type, rest)
    end
  end

  @spec channel_matches_any?(binary(), list()) :: boolean()
  def channel_matches_any?(_channel, []), do: false

  def channel_matches_any?(channel, [{_glob, regex} | rest]) do
    if Regex.match?(regex, channel) do
      true
    else
      channel_matches_any?(channel, rest)
    end
  end

  @spec compile_glob(binary()) :: Regex.t()
  def compile_glob(pattern) do
    source = pattern |> glob_source(0, []) |> IO.iodata_to_binary()
    Regex.compile!("\\A" <> source <> "\\z", "s")
  end

  defp access_permitted?(:rw, _access_type), do: true
  defp access_permitted?(:read, :read), do: true
  defp access_permitted?(:write, :write), do: true
  defp access_permitted?(_, _), do: false

  # Patterns match keys and channels byte for byte as Redis's stringmatchlen
  # does: `*` any run of bytes, `?` any one byte, a backslash makes the next
  # byte literal, and a character class is the byte set
  # `Ferricstore.GlobMatcher` gives it.
  defp glob_source(pattern, position, acc) when position >= byte_size(pattern),
    do: Enum.reverse(acc)

  defp glob_source(pattern, position, acc) do
    case :binary.at(pattern, position) do
      ?* ->
        glob_source(pattern, skip_stars(pattern, position), [".*" | acc])

      ?? ->
        glob_source(pattern, position + 1, ["." | acc])

      ?\\ when position + 1 < byte_size(pattern) ->
        glob_source(pattern, position + 2, [byte_source(:binary.at(pattern, position + 1)) | acc])

      ?[ ->
        class_end = Ferricstore.GlobMatcher.class_end(pattern, position)
        class = binary_part(pattern, position, class_end - position)
        glob_source(pattern, class_end, [class_source(class) | acc])

      byte ->
        glob_source(pattern, position + 1, [byte_source(byte) | acc])
    end
  end

  defp skip_stars(pattern, position) do
    if position < byte_size(pattern) and :binary.at(pattern, position) == ?*,
      do: skip_stars(pattern, position + 1),
      else: position
  end

  defp class_source(class) do
    case Enum.filter(0..255, &Ferricstore.GlobMatcher.match?(<<&1>>, class)) do
      [] -> "(?!)"
      bytes -> ["[", bytes |> byte_ranges() |> Enum.map(&range_source/1), "]"]
    end
  end

  defp byte_ranges([first | rest]) do
    {ranges, {low, high}} =
      Enum.reduce(rest, {[], {first, first}}, fn
        byte, {ranges, {low, high}} when byte == high + 1 -> {ranges, {low, byte}}
        byte, {ranges, range} -> {[range | ranges], {byte, byte}}
      end)

    Enum.reverse([{low, high} | ranges])
  end

  defp range_source({byte, byte}), do: hex_byte(byte)
  defp range_source({low, high}), do: [hex_byte(low), "-", hex_byte(high)]

  defp byte_source(byte) when byte in ?a..?z or byte in ?A..?Z or byte in ?0..?9,
    do: <<byte>>

  defp byte_source(byte)
       when byte in [?:, ?_, ?-, ?/, ?@, ?#, ?=, ?,, ?%, ?!, ?~, ?', ?", ?;, ?<, ?>, ?&, ?`],
       do: <<byte>>

  defp byte_source(byte), do: hex_byte(byte)

  defp hex_byte(byte),
    do: "\\x" <> String.pad_leading(Integer.to_string(byte, 16), 2, "0")
end
