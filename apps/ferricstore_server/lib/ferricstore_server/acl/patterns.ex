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
    source =
      pattern
      |> String.graphemes()
      |> compile_glob_chars([])
      |> IO.iodata_to_binary()

    case Regex.compile("^" <> source <> "$") do
      {:ok, regex} -> regex
      {:error, _} -> Regex.compile!("^" <> Regex.escape(pattern) <> "$")
    end
  end

  defp access_permitted?(:rw, _access_type), do: true
  defp access_permitted?(:read, :read), do: true
  defp access_permitted?(:write, :write), do: true
  defp access_permitted?(_, _), do: false

  defp compile_glob_chars([], acc), do: Enum.reverse(acc)

  defp compile_glob_chars(["*", "*" | rest], acc),
    do: compile_glob_chars(["*" | rest], acc)

  defp compile_glob_chars(["*" | rest], acc), do: compile_glob_chars(rest, [".*" | acc])
  defp compile_glob_chars(["?" | rest], acc), do: compile_glob_chars(rest, ["." | acc])

  defp compile_glob_chars(["[" | rest], acc) do
    case collect_char_class(rest, []) do
      {:ok, [], remaining} ->
        compile_glob_chars(remaining, [Regex.escape("[]") | acc])

      {:ok, class_chars, remaining} ->
        compile_glob_chars(remaining, [["[", class_chars, "]"] | acc])

      {:unterminated, class_chars} ->
        literal = Regex.escape("[" <> IO.iodata_to_binary(class_chars))
        compile_glob_chars([], [literal | acc])
    end
  end

  defp compile_glob_chars([ch | rest], acc) do
    compile_glob_chars(rest, [Regex.escape(ch) | acc])
  end

  defp collect_char_class([], acc), do: {:unterminated, Enum.reverse(acc)}
  defp collect_char_class(["]" | rest], acc), do: {:ok, Enum.reverse(acc), rest}
  defp collect_char_class([ch | rest], acc), do: collect_char_class(rest, [ch | acc])
end
