defmodule Ferricstore.Flow.Attributes do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  @max_attrs 16
  @max_key_bytes 64
  @max_value_bytes 256
  @max_total_bytes 2_048
  @max_list_values 16
  @max_indexed_names 3

  @type attrs :: %{optional(binary()) => binary() | integer() | float() | boolean() | [binary()]}

  def from_opts(opts) do
    opts
    |> fetch_opt(:attributes, %{})
    |> normalize()
  end

  def update_from_opts(opts) do
    with {:ok, base} <- normalize(fetch_opt(opts, :attributes, %{})),
         {:ok, patch} <- normalize(fetch_opt(opts, :attributes_merge, %{})),
         merge = Map.merge(base, patch),
         {:ok, delete} <- normalize_delete(fetch_opt(opts, :attributes_delete, [])) do
      {:ok, merge, delete}
    end
  end

  def record(record) when is_map(record) do
    record
    |> Map.get(:attributes, %{})
    |> decode_sidecar()
  end

  def record(_record), do: %{}

  def put_record(record, attrs) when is_map(record) and is_map(attrs) do
    if map_size(attrs) == 0 do
      Map.delete(record, :attributes)
    else
      Map.put(record, :attributes, attrs)
    end
  end

  def apply_update(record, merge, delete) when is_map(record) do
    attrs =
      record(record)
      |> Map.merge(merge || %{})
      |> Map.drop(delete || [])

    put_record(record, attrs)
  end

  def encode_sidecar(attrs) when is_map(attrs) do
    attrs
    |> decode_sidecar()
    |> Map.new(fn {name, value} -> {name, encode_value(value)} end)
  end

  def encode_sidecar(_attrs), do: %{}

  def decode_sidecar(attrs) when is_map(attrs) do
    attrs
    |> Enum.reduce(%{}, fn
      {name, value}, acc ->
        with {:ok, key} <- normalize_key(name),
             {:ok, value} <- normalize_value(value) do
          Map.put(acc, key, value)
        else
          _ -> acc
        end
    end)
    |> enforce_count_limit()
    |> enforce_total_limit()
  end

  def decode_sidecar(_attrs), do: %{}

  def normalize(attrs) when attrs in [nil, %{}], do: {:ok, %{}}

  def normalize(attrs) when is_map(attrs) do
    with :ok <- validate_count(map_size(attrs)),
         {:ok, normalized} <- normalize_entries(attrs),
         :ok <- validate_total(normalized) do
      {:ok, normalized}
    end
  end

  def normalize(_attrs), do: {:error, "ERR flow attributes must be a map"}

  def normalize_delete(values) when values in [nil, []], do: {:ok, []}

  def normalize_delete(values) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, acc} ->
      case normalize_key(key) do
        {:ok, key} -> {:cont, {:ok, [key | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, keys} -> {:ok, keys |> Enum.uniq() |> Enum.reverse()}
      {:error, _reason} = error -> error
    end
  end

  def normalize_delete(_values), do: {:error, "ERR flow attributes_delete must be a list"}

  def normalize_name(name), do: normalize_key(name)

  def normalize_indexed_names(values) when values in [nil, []], do: {:ok, []}

  def normalize_indexed_names(values) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn name, {:ok, acc} ->
      case normalize_key(name) do
        {:ok, name} -> {:cont, {:ok, [name | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, names} ->
        names = names |> Enum.reverse() |> Enum.uniq()

        if length(names) <= @max_indexed_names do
          {:ok, names}
        else
          {:error, "ERR flow indexed_attributes supports at most #{@max_indexed_names} keys"}
        end

      {:error, _reason} = error ->
        error
    end
  end

  def normalize_indexed_names(_values),
    do: {:error, "ERR flow indexed_attributes must be a list"}

  def indexed_names(record) when is_map(record) do
    case normalize_indexed_names(Map.get(record, :indexed_attributes, [])) do
      {:ok, names} -> names
      {:error, _reason} -> []
    end
  end

  def indexed_names(_record), do: []

  def put_indexed_names(record, names) when is_map(record) do
    case normalize_indexed_names(names) do
      {:ok, []} -> Map.delete(record, :indexed_attributes)
      {:ok, names} -> Map.put(record, :indexed_attributes, names)
      {:error, _reason} -> Map.delete(record, :indexed_attributes)
    end
  end

  def index_entries(record) when is_map(record) do
    id = Map.get(record, :id)
    type = Map.get(record, :type)
    state = Map.get(record, :state)
    partition_key = Map.get(record, :partition_key)
    score = normalize_score(Map.get(record, :updated_at_ms, 0))

    if is_binary(id) and is_binary(type) and is_binary(state) do
      attrs = record(record)
      indexed = MapSet.new(indexed_names(record))

      attrs
      |> Enum.flat_map(fn {name, value} ->
        value
        |> index_values()
        |> Enum.flat_map(fn indexed_value ->
          value = index_value(indexed_value)
          exact = {Keys.attribute_index_key(type, state, name, value, partition_key), id, score}

          if MapSet.member?(indexed, name) do
            [
              exact,
              {Keys.attribute_type_index_key(type, name, value, partition_key), id, score},
              {Keys.attribute_state_index_key(state, name, value, partition_key), id, score},
              {Keys.attribute_partition_index_key(name, value, partition_key), id, score}
            ]
          else
            [exact]
          end
        end)
      end)
    else
      []
    end
  end

  def index_entries(_record), do: []

  def matches?(record, filters) do
    case normalize(filters) do
      {:ok, filters} -> matches_normalized?(record(record), filters)
      {:error, _reason} -> false
    end
  end

  defp matches_normalized?(_attrs, filters) when filters == %{}, do: true

  defp matches_normalized?(attrs, filters) do
    Enum.all?(filters, fn {name, expected} ->
      attrs
      |> Map.get(name)
      |> value_matches?(expected)
    end)
  end

  defp value_matches?(values, expected) when is_list(values) and is_list(expected),
    do: Enum.all?(expected, &(&1 in values))

  defp value_matches?(values, expected) when is_list(values), do: expected in values
  defp value_matches?(value, expected), do: value == expected

  defp normalize_entries(attrs) do
    Enum.reduce_while(attrs, {:ok, %{}}, fn {name, value}, {:ok, acc} ->
      with {:ok, name} <- normalize_key(name),
           {:ok, value} <- normalize_value(value) do
        {:cont, {:ok, Map.put(acc, name, value)}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp normalize_key(key) when is_atom(key), do: key |> Atom.to_string() |> normalize_key()

  defp normalize_key(key) when is_binary(key) do
    key = String.trim(key)

    cond do
      key == "" -> {:error, "ERR flow attribute key must not be empty"}
      byte_size(key) > @max_key_bytes -> {:error, "ERR flow attribute key too large"}
      String.starts_with?(key, "__") -> {:error, "ERR flow attribute key is reserved"}
      true -> {:ok, key}
    end
  end

  defp normalize_key(_key), do: {:error, "ERR flow attribute key must be a string"}

  defp normalize_value(value) when is_binary(value) do
    if byte_size(value) <= @max_value_bytes do
      {:ok, value}
    else
      {:error, "ERR flow attribute value too large"}
    end
  end

  defp normalize_value(value) when is_integer(value) or is_float(value) or is_boolean(value),
    do: {:ok, value}

  defp normalize_value(value) when is_atom(value) and not is_nil(value),
    do: value |> Atom.to_string() |> normalize_value()

  defp normalize_value(values) when is_list(values) do
    cond do
      values == [] ->
        {:error, "ERR flow attribute list must not be empty"}

      length(values) > @max_list_values ->
        {:error, "ERR flow attribute list too large"}

      true ->
        values
        |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
          case normalize_list_value(value) do
            {:ok, value} -> {:cont, {:ok, [value | acc]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, values} -> {:ok, values |> Enum.uniq() |> Enum.reverse()}
          {:error, _reason} = error -> error
        end
    end
  end

  defp normalize_value(_value),
    do: {:error, "ERR flow attribute value must be scalar or string list"}

  defp normalize_list_value(value) when is_binary(value), do: normalize_value(value)

  defp normalize_list_value(value) when is_atom(value),
    do: value |> Atom.to_string() |> normalize_value()

  defp normalize_list_value(_value),
    do: {:error, "ERR flow attribute list values must be strings"}

  defp validate_count(count) when count <= @max_attrs, do: :ok
  defp validate_count(_count), do: {:error, "ERR too many flow attributes"}

  defp validate_total(attrs) do
    if encoded_size(attrs) <= @max_total_bytes do
      :ok
    else
      {:error, "ERR flow attributes too large"}
    end
  end

  defp enforce_total_limit(attrs) do
    if encoded_size(attrs) <= @max_total_bytes, do: attrs, else: %{}
  end

  defp enforce_count_limit(attrs) do
    if map_size(attrs) <= @max_attrs, do: attrs, else: %{}
  end

  defp encoded_size(attrs) do
    Enum.reduce(attrs, 0, fn {name, value}, acc ->
      acc + byte_size(name) + value_size(value)
    end)
  end

  defp value_size(value) when is_binary(value), do: byte_size(value)
  defp value_size(value) when is_integer(value), do: 8
  defp value_size(value) when is_float(value), do: 8
  defp value_size(value) when is_boolean(value), do: 1
  defp value_size(values) when is_list(values), do: Enum.reduce(values, 0, &(&2 + byte_size(&1)))

  defp encode_value(value) when is_list(value), do: value
  defp encode_value(value), do: value

  defp index_values(values) when is_list(values), do: values
  defp index_values(value), do: [value]

  def index_value(value) when is_binary(value),
    do: "s64:" <> Base.url_encode64(value, padding: false)

  def index_value(value) when is_integer(value), do: "i:" <> Integer.to_string(value)

  def index_value(value) when is_float(value),
    do: "f:" <> :erlang.float_to_binary(value, [:compact])

  def index_value(true), do: "b:1"
  def index_value(false), do: "b:0"

  def decode_index_value("s64:" <> value) do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} -> decoded
      :error -> value
    end
  end

  def decode_index_value("s:" <> value), do: value

  def decode_index_value("i:" <> value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> value
    end
  end

  def decode_index_value("f:" <> value) do
    case Float.parse(value) do
      {float, ""} -> float
      _ -> value
    end
  end

  def decode_index_value("b:1"), do: true
  def decode_index_value("b:0"), do: false
  def decode_index_value(value), do: value

  defp normalize_score(value) when is_integer(value), do: value
  defp normalize_score(_value), do: 0

  defp fetch_opt(opts, key, default) when is_map(opts), do: Map.get(opts, key, default)
  defp fetch_opt(opts, key, default) when is_list(opts), do: Keyword.get(opts, key, default)
  defp fetch_opt(_opts, _key, default), do: default
end
