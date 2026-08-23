defmodule FerricstoreHttp.CommandError do
  @moduledoc false

  @max_code_bytes 128
  @max_text_bytes 1_024
  @max_context_entries 16
  @max_context_list_items 32
  @max_context_key_bytes 128
  @max_context_depth 6
  @max_context_nodes 512
  @min_context_integer -0x8000_0000_0000_0000
  @max_context_integer 0x7FFF_FFFF_FFFF_FFFF
  @max_retry_after_ms 0xFFFF_FFFF_FFFF_FFFF

  @spec normalize(atom(), term()) :: map()
  def normalize(status, value) do
    case structured_details(value) do
      {:ok, details} -> details
      :error -> %{"code" => to_string(status), "message" => message(value)}
    end
  end

  defp structured_details(value) when is_map(value) do
    with {:ok, code} <- bounded_binary(Map.get(value, "code"), @max_code_bytes),
         {:ok, message} <- bounded_binary(Map.get(value, "message"), @max_text_bytes) do
      details = %{"code" => code, "message" => message}

      {:ok,
       details
       |> put_bounded_text(value, "detail")
       |> put_bounded_text(value, "hint")
       |> put_position(value)
       |> put_context(value)
       |> put_boolean(value, "retryable")
       |> put_boolean(value, "safe_to_retry")
       |> put_retry_after(value)}
    else
      :error -> :error
    end
  end

  defp structured_details(_value), do: :error

  defp bounded_binary(value, max_bytes)
       when is_binary(value) and byte_size(value) <= max_bytes do
    if String.valid?(value), do: {:ok, value}, else: :error
  end

  defp bounded_binary(_value, _max_bytes), do: :error

  defp put_bounded_text(details, source, field) do
    case bounded_binary(Map.get(source, field), @max_text_bytes) do
      {:ok, value} -> Map.put(details, field, value)
      :error -> details
    end
  end

  defp put_boolean(details, source, field) do
    case Map.get(source, field) do
      value when is_boolean(value) -> Map.put(details, field, value)
      _invalid_or_missing -> details
    end
  end

  defp put_position(details, source) do
    case Map.get(source, "position") do
      %{"byte" => byte, "line" => line, "column" => column} = position
      when map_size(position) == 3 and is_integer(byte) and byte > 0 and is_integer(line) and
             line > 0 and is_integer(column) and column > 0 ->
        Map.put(details, "position", position)

      _invalid_or_missing ->
        details
    end
  end

  defp put_context(details, source) do
    case Map.get(source, "context") do
      context when is_map(context) and map_size(context) > 0 ->
        if valid_context?(context), do: Map.put(details, "context", context), else: details

      _empty_invalid_or_missing ->
        details
    end
  end

  defp put_retry_after(details, source) do
    case Map.get(source, "retry_after_ms") do
      value when is_integer(value) and value >= 0 and value <= @max_retry_after_ms ->
        Map.put(details, "retry_after_ms", value)

      _invalid_or_missing ->
        details
    end
  end

  defp valid_context?(context)
       when is_map(context) and map_size(context) <= @max_context_entries do
    match?(
      {:ok, _remaining},
      validate_wire_value(context, @max_context_depth, @max_context_nodes)
    )
  end

  defp valid_context?(_context), do: false

  defp validate_wire_value(_value, _depth, 0), do: :error

  defp validate_wire_value(value, _depth, remaining)
       when is_binary(value) and byte_size(value) <= @max_text_bytes do
    if String.valid?(value), do: {:ok, remaining - 1}, else: :error
  end

  defp validate_wire_value(value, _depth, remaining)
       when is_integer(value) and value >= @min_context_integer and value <= @max_context_integer,
       do: {:ok, remaining - 1}

  defp validate_wire_value(value, _depth, remaining) when is_boolean(value) or is_nil(value),
    do: {:ok, remaining - 1}

  defp validate_wire_value(value, depth, remaining)
       when is_map(value) and depth > 0 and map_size(value) <= @max_context_entries do
    Enum.reduce_while(value, {:ok, remaining - 1}, fn entry, accumulator ->
      validate_wire_entry(entry, accumulator, depth)
    end)
  end

  defp validate_wire_value(value, depth, remaining) when is_list(value) and depth > 0 do
    validate_wire_list(value, depth - 1, remaining - 1, @max_context_list_items)
  end

  defp validate_wire_value(_value, _depth, _remaining), do: :error

  defp validate_wire_list([], _depth, remaining, _items), do: {:ok, remaining}
  defp validate_wire_list(_values, _depth, _remaining, 0), do: :error

  defp validate_wire_list([value | rest], depth, remaining, items) do
    case validate_wire_value(value, depth, remaining) do
      {:ok, remaining} -> validate_wire_list(rest, depth, remaining, items - 1)
      :error -> :error
    end
  end

  defp validate_wire_list(_improper, _depth, _remaining, _items), do: :error

  defp validate_wire_entry({key, item}, {:ok, nodes}, depth) do
    if valid_context_key?(key) do
      item
      |> validate_wire_value(depth - 1, nodes)
      |> reduction_directive()
    else
      {:halt, :error}
    end
  end

  defp reduction_directive({:ok, _remaining} = valid), do: {:cont, valid}
  defp reduction_directive(:error), do: {:halt, :error}

  defp valid_context_key?(key),
    do:
      is_binary(key) and key != "" and byte_size(key) <= @max_context_key_bytes and
        String.valid?(key)

  defp message(value) when is_binary(value) do
    case bounded_binary(value, @max_text_bytes) do
      {:ok, message} -> message
      :error -> "FerricStore command failed"
    end
  end

  defp message(value) when is_atom(value), do: Atom.to_string(value)
  defp message(_value), do: "FerricStore command failed"
end
