defmodule Ferricstore.Flow.StateMeta do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  @max_states 64
  @max_entries_per_state 16
  @max_state_bytes 64
  @max_key_bytes 64
  @max_value_bytes 256
  @max_total_bytes 16_384
  @max_indexed_keys 1
  @min_i64 -0x8000_0000_0000_0000
  @max_i64 0x7FFF_FFFF_FFFF_FFFF

  @type value :: binary() | integer() | float() | boolean()
  @type state_meta :: %{optional(binary()) => %{optional(binary()) => value()}}

  def update_from_opts(opts) do
    opts
    |> fetch_opt(:state_meta, %{})
    |> normalize_entry_map()
  end

  def record(record) when is_map(record) do
    record
    |> Map.get(:state_meta, %{})
    |> decode_sidecar()
  end

  def record(_record), do: %{}

  def put_record(record, state_meta) when is_map(record) and is_map(state_meta) do
    if map_size(state_meta) == 0 do
      Map.delete(record, :state_meta)
    else
      Map.put(record, :state_meta, state_meta)
    end
  end

  def apply_update(record, attrs) when is_map(record) and is_map(attrs) do
    update = Map.get(attrs, :state_meta_update, %{})

    if map_size(update) == 0 do
      record
    else
      state = Map.get(attrs, :state_meta_state) || logical_state(record)

      with {:ok, state} <- normalize_state(state),
           {:ok, update} <- normalize_entry_map(update),
           {:ok, current} <- normalize(record(record)),
           merged = Map.update(current, state, update, &Map.merge(&1, update)),
           {:ok, merged} <- normalize(merged) do
        put_record(record, merged)
      else
        _ -> record
      end
    end
  end

  def encode_sidecar(state_meta) when is_map(state_meta) do
    case normalize(state_meta) do
      {:ok, state_meta} -> state_meta
      {:error, _reason} -> %{}
    end
  end

  def encode_sidecar(_state_meta), do: %{}

  def decode_sidecar(state_meta) when is_map(state_meta) do
    case normalize(state_meta) do
      {:ok, state_meta} -> state_meta
      {:error, _reason} -> %{}
    end
  end

  def decode_sidecar(_state_meta), do: %{}

  def normalize(state_meta) when state_meta in [nil, %{}], do: {:ok, %{}}

  def normalize(state_meta) when is_map(state_meta) do
    with :ok <- validate_state_count(map_size(state_meta)),
         {:ok, normalized} <- normalize_states(state_meta),
         :ok <- validate_total(normalized) do
      {:ok, normalized}
    end
  end

  def normalize(_state_meta), do: {:error, "ERR flow state_meta must be a map"}

  def query_from_opts(opts) do
    opts
    |> fetch_opt(:state_meta, %{})
    |> normalize()
  end

  def normalize_indexed_key(value) when value in [nil, "", []], do: {:ok, nil}

  def normalize_indexed_key(value) when is_atom(value),
    do: value |> Atom.to_string() |> normalize_indexed_key()

  def normalize_indexed_key(value) when is_binary(value), do: normalize_key(value)

  def normalize_indexed_key(values) when is_list(values) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case normalize_indexed_key(value) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} ->
        values = values |> Enum.reverse() |> Enum.uniq()

        cond do
          values == [] ->
            {:ok, nil}

          length(values) <= @max_indexed_keys ->
            {:ok, hd(values)}

          true ->
            {:error, "ERR flow indexed_state_meta supports at most #{@max_indexed_keys} key"}
        end

      {:error, _reason} = error ->
        error
    end
  end

  def normalize_indexed_key(_value),
    do: {:error, "ERR flow indexed_state_meta must be a string or one-item list"}

  def indexed_key(%{indexed_state_meta: key}) do
    case normalize_indexed_key(key) do
      {:ok, key} -> key
      {:error, _reason} -> nil
    end
  end

  def indexed_key(_policy), do: nil

  def put_indexed_key(record, nil) when is_map(record),
    do: Map.delete(record, :indexed_state_meta)

  def put_indexed_key(record, key) when is_map(record) do
    case normalize_indexed_key(key) do
      {:ok, nil} -> Map.delete(record, :indexed_state_meta)
      {:ok, key} -> Map.put(record, :indexed_state_meta, key)
      {:error, _reason} -> Map.delete(record, :indexed_state_meta)
    end
  end

  def index_entries(record) when is_map(record) do
    id = Map.get(record, :id)
    type = Map.get(record, :type)
    partition_key = Map.get(record, :partition_key)
    score = normalize_score(Map.get(record, :updated_at_ms, 0))
    indexed_key = indexed_key(record)

    if is_binary(id) and is_binary(type) and is_binary(indexed_key) do
      record(record)
      |> Enum.flat_map(fn {state, meta} ->
        case Map.fetch(meta, indexed_key) do
          {:ok, value} ->
            value = index_value(value)

            [
              {Keys.state_meta_index_key(type, state, indexed_key, value, partition_key), id,
               score}
            ]

          :error ->
            []
        end
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

  def candidate_filters(filters) do
    filters
    |> Enum.flat_map(fn {state, meta} ->
      Enum.map(meta, fn {name, value} -> {state, name, value} end)
    end)
  end

  def index_value(value), do: Ferricstore.Flow.Attributes.index_value(value)

  defp matches_normalized?(_state_meta, filters) when filters == %{}, do: true

  defp matches_normalized?(state_meta, filters) do
    Enum.all?(filters, fn {state, expected_meta} ->
      actual_meta = Map.get(state_meta, state, %{})

      Enum.all?(expected_meta, fn {name, expected} ->
        Map.get(actual_meta, name) == expected
      end)
    end)
  end

  defp normalize_states(state_meta) do
    Enum.reduce_while(state_meta, {:ok, %{}}, fn {state, meta}, {:ok, acc} ->
      with {:ok, state} <- normalize_state(state),
           {:ok, meta} <- normalize_entry_map(meta) do
        {:cont, {:ok, Map.put(acc, state, meta)}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp normalize_entry_map(meta) when meta in [nil, %{}], do: {:ok, %{}}

  defp normalize_entry_map(meta) when is_map(meta) do
    with :ok <- validate_entry_count(map_size(meta)),
         {:ok, normalized} <- normalize_entries(meta) do
      {:ok, normalized}
    end
  end

  defp normalize_entry_map(_meta), do: {:error, "ERR flow state_meta must be a map"}

  defp normalize_entries(meta) do
    Enum.reduce_while(meta, {:ok, %{}}, fn {name, value}, {:ok, acc} ->
      with {:ok, name} <- normalize_key(name),
           {:ok, value} <- normalize_value(value) do
        {:cont, {:ok, Map.put(acc, name, value)}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp normalize_state(state) when is_atom(state),
    do: state |> Atom.to_string() |> normalize_state()

  defp normalize_state(state) when is_binary(state) do
    state = String.trim(state)

    cond do
      state == "" -> {:error, "ERR flow state_meta state must not be empty"}
      byte_size(state) > @max_state_bytes -> {:error, "ERR flow state_meta state too large"}
      true -> {:ok, state}
    end
  end

  defp normalize_state(_state), do: {:error, "ERR flow state_meta state must be a string"}

  defp normalize_key(key) when is_atom(key), do: key |> Atom.to_string() |> normalize_key()

  defp normalize_key(key) when is_binary(key) do
    key = String.trim(key)

    cond do
      key == "" -> {:error, "ERR flow state_meta key must not be empty"}
      byte_size(key) > @max_key_bytes -> {:error, "ERR flow state_meta key too large"}
      String.starts_with?(key, "__") -> {:error, "ERR flow state_meta key is reserved"}
      true -> {:ok, key}
    end
  end

  defp normalize_key(_key), do: {:error, "ERR flow state_meta key must be a string"}

  defp normalize_value(value) when is_binary(value) do
    if byte_size(value) <= @max_value_bytes do
      {:ok, value}
    else
      {:error, "ERR flow state_meta value too large"}
    end
  end

  defp normalize_value(value)
       when is_integer(value) and value >= @min_i64 and value <= @max_i64,
       do: {:ok, value}

  defp normalize_value(value) when is_integer(value),
    do: {:error, "ERR flow state_meta integer must fit in signed 64 bits"}

  defp normalize_value(value) when is_float(value) do
    if finite_float?(value),
      do: {:ok, value},
      else: {:error, "ERR flow state_meta float must be finite"}
  end

  defp normalize_value(value) when is_boolean(value), do: {:ok, value}

  defp normalize_value(value) when is_atom(value) and not is_nil(value),
    do: value |> Atom.to_string() |> normalize_value()

  defp normalize_value(_value), do: {:error, "ERR flow state_meta value must be scalar"}

  defp finite_float?(value) do
    <<_sign::1, exponent::11, _fraction::52>> = <<value::float-big-64>>
    exponent != 0x7FF
  end

  defp validate_state_count(count) when count <= @max_states, do: :ok
  defp validate_state_count(_count), do: {:error, "ERR too many flow state_meta states"}

  defp validate_entry_count(count) when count <= @max_entries_per_state, do: :ok
  defp validate_entry_count(_count), do: {:error, "ERR too many flow state_meta entries"}

  defp validate_total(state_meta) do
    if encoded_size(state_meta) <= @max_total_bytes do
      :ok
    else
      {:error, "ERR flow state_meta too large"}
    end
  end

  defp encoded_size(state_meta) do
    Enum.reduce(state_meta, 0, fn {state, meta}, acc ->
      acc + byte_size(state) + entry_size(meta)
    end)
  end

  defp entry_size(meta) do
    Enum.reduce(meta, 0, fn {name, value}, acc ->
      acc + byte_size(name) + value_size(value)
    end)
  end

  defp value_size(value) when is_binary(value), do: byte_size(value)
  defp value_size(value) when is_integer(value), do: 8
  defp value_size(value) when is_float(value), do: 8
  defp value_size(value) when is_boolean(value), do: 1

  defp logical_state(record), do: Map.get(record, :run_state) || Map.get(record, :state)

  defp normalize_score(value) when is_integer(value), do: value
  defp normalize_score(_value), do: 0

  defp fetch_opt(opts, key, default) when is_map(opts), do: Map.get(opts, key, default)
  defp fetch_opt(opts, key, default) when is_list(opts), do: Keyword.get(opts, key, default)
  defp fetch_opt(_opts, _key, default), do: default
end
