defmodule Ferricstore.Commands.Json.Path do
  @moduledoc false

  @spec parse(binary() | [binary() | non_neg_integer()]) ::
          [binary() | non_neg_integer()] | :error
  def parse(segments) when is_list(segments), do: segments
  def parse("$"), do: []
  def parse(<<"$", rest::binary>>), do: parse_segments(rest, [])
  def parse(_), do: :error

  @spec get(term(), [binary() | non_neg_integer()] | :error) ::
          {:ok, term()} | :not_found | {:error, binary()}
  def get(_value, :error), do: {:error, "ERR invalid JSONPath syntax"}
  def get(value, []), do: {:ok, value}

  def get(map, [key | rest]) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, val} -> get(val, rest)
      :error -> :not_found
    end
  end

  def get(list, [idx | rest]) when is_list(list) and is_integer(idx) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)) do
      get(Enum.at(list, actual_idx), rest)
    else
      :not_found
    end
  end

  def get(_value, [_ | _rest]), do: :not_found

  @spec set(term(), [binary() | non_neg_integer()], term()) ::
          {:ok, term()} | :not_found
  def set(_current, [], new_value), do: {:ok, new_value}

  def set(map, [key | rest], new_value) when is_map(map) and is_binary(key) do
    case set(Map.get(map, key), rest, new_value) do
      {:ok, updated} -> {:ok, Map.put(map, key, updated)}
      :not_found -> :not_found
    end
  end

  def set(list, [idx | rest], new_value) when is_list(list) and is_integer(idx) do
    set_list_index(list, idx, rest, new_value)
  end

  # Setting a field on nil creates a new map (auto-vivification for root-level set)
  def set(nil, [key | rest], new_value) when is_binary(key) do
    set(%{}, [key | rest], new_value)
  end

  def set(_value, [_ | _rest], _new_value), do: :not_found

  @spec delete(term(), [binary() | non_neg_integer()] | :error) ::
          {:ok, term()} | :not_found | {:error, binary()}
  def delete(_value, :error), do: {:error, "ERR invalid JSONPath syntax"}
  def delete(_value, []), do: :not_found

  def delete(map, [key]) when is_map(map) and is_binary(key) do
    if Map.has_key?(map, key), do: {:ok, Map.delete(map, key)}, else: :not_found
  end

  def delete(list, [idx]) when is_list(list) and is_integer(idx) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)),
      do: {:ok, List.delete_at(list, actual_idx)},
      else: :not_found
  end

  def delete(map, [key | rest]) when is_map(map) and is_binary(key) do
    with {:ok, child} <- Map.fetch(map, key),
         {:ok, updated_child} <- delete(child, rest) do
      {:ok, Map.put(map, key, updated_child)}
    else
      _ -> :not_found
    end
  end

  def delete(list, [idx | rest]) when is_list(list) and is_integer(idx) do
    delete_list_index(list, idx, rest)
  end

  def delete(_value, [_ | _rest]), do: :not_found

  @spec build([binary() | non_neg_integer()], term()) :: {:ok, term()} | :error
  def build([], value), do: {:ok, value}

  def build([key | rest], value) when is_binary(key) do
    case build(rest, value) do
      {:ok, inner} -> {:ok, %{key => inner}}
      :error -> :error
    end
  end

  # Cannot auto-create array indices on empty documents
  def build([_ | _], _value), do: :error

  defp parse_segments(<<>>, acc), do: Enum.reverse(acc)

  defp parse_segments(<<".", rest::binary>>, acc) do
    case read_field(rest) do
      {:ok, field, remainder} -> parse_segments(remainder, [field | acc])
      :error -> :error
    end
  end

  defp parse_segments(<<"[", rest::binary>>, acc) do
    case read_bracket(rest) do
      {:ok, segment, remainder} -> parse_segments(remainder, [segment | acc])
      :error -> :error
    end
  end

  defp parse_segments(_, _acc), do: :error

  defp read_field(str) do
    case :binary.match(str, [<<".">>, <<"[">>]) do
      {pos, _len} ->
        field = binary_part(str, 0, pos)

        if field == "" do
          :error
        else
          {:ok, field, binary_part(str, pos, byte_size(str) - pos)}
        end

      :nomatch ->
        if str == "", do: :error, else: {:ok, str, <<>>}
    end
  end

  defp read_bracket(str) do
    case :binary.match(str, <<"]">>) do
      {pos, 1} -> parse_bracket_inner(str, pos)
      _ -> :error
    end
  end

  defp parse_bracket_inner(str, pos) do
    inner = binary_part(str, 0, pos)
    remainder = binary_part(str, pos + 1, byte_size(str) - pos - 1)
    parse_bracket_content(inner, remainder)
  end

  defp parse_bracket_content(<<"\"", _::binary>> = inner, remainder) do
    if byte_size(inner) >= 2 and String.ends_with?(inner, "\"") do
      {:ok, String.slice(inner, 1..-2//1), remainder}
    else
      :error
    end
  end

  defp parse_bracket_content(<<"'", _::binary>> = inner, remainder) do
    if byte_size(inner) >= 2 and String.ends_with?(inner, "'") do
      {:ok, String.slice(inner, 1..-2//1), remainder}
    else
      :error
    end
  end

  defp parse_bracket_content(inner, remainder) do
    case Integer.parse(inner) do
      {idx, ""} -> {:ok, idx, remainder}
      _ -> :error
    end
  end

  defp set_list_index(list, idx, rest, new_value) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)) do
      case set(Enum.at(list, actual_idx), rest, new_value) do
        {:ok, updated} -> {:ok, List.replace_at(list, actual_idx, updated)}
        :not_found -> :not_found
      end
    else
      :not_found
    end
  end

  defp delete_list_index(list, idx, rest) do
    actual_idx = normalize_index(idx, length(list))

    if valid_index?(actual_idx, length(list)) do
      case delete(Enum.at(list, actual_idx), rest) do
        {:ok, updated_child} -> {:ok, List.replace_at(list, actual_idx, updated_child)}
        :not_found -> :not_found
      end
    else
      :not_found
    end
  end

  defp normalize_index(idx, len) when idx < 0, do: len + idx
  defp normalize_index(idx, _len), do: idx

  defp valid_index?(idx, len), do: idx >= 0 and idx < len
end
