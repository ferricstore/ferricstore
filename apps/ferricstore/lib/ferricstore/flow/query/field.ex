defmodule Ferricstore.Flow.Query.Field do
  @moduledoc false

  @missing {:ferric_query, :missing}
  @max_metadata_name_bytes 64

  @builtins %{
    "partition_key" => :partition_key,
    "run_id" => :run_id,
    "event_id" => :event_id,
    "type" => :type,
    "state" => :state,
    "version" => :version,
    "priority" => :priority,
    "created_at_ms" => :created_at_ms,
    "updated_at_ms" => :updated_at_ms,
    "next_run_at_ms" => :next_run_at_ms,
    "lease_deadline_ms" => :lease_deadline_ms,
    "attempts" => :attempts,
    "run_state" => :run_state,
    "max_active_ms" => :max_active_ms,
    "parent_flow_id" => :parent_flow_id,
    "root_flow_id" => :root_flow_id,
    "correlation_id" => :correlation_id
  }

  @integer_fields MapSet.new([
                    :version,
                    :priority,
                    :created_at_ms,
                    :updated_at_ms,
                    :next_run_at_ms,
                    :lease_deadline_ms,
                    :attempts,
                    :max_active_ms
                  ])

  @keyword_fields MapSet.new([
                    :partition_key,
                    :run_id,
                    :event_id,
                    :type,
                    :state,
                    :run_state,
                    :parent_flow_id,
                    :root_flow_id,
                    :correlation_id
                  ])

  @builtin_fields Map.values(@builtins)

  @type builtin ::
          :partition_key
          | :run_id
          | :event_id
          | :type
          | :state
          | :version
          | :priority
          | :created_at_ms
          | :updated_at_ms
          | :next_run_at_ms
          | :lease_deadline_ms
          | :attempts
          | :run_state
          | :max_active_ms
          | :parent_flow_id
          | :root_flow_id
          | :correlation_id

  @type t :: builtin() | {:attribute, binary()} | {:state_meta, binary(), binary()}

  @spec missing() :: {:ferric_query, :missing}
  def missing, do: @missing

  @spec parse(binary()) :: {:ok, t()} | {:error, :unsupported_field}
  def parse(value) when is_binary(value) do
    normalized = ascii_downcase(value)

    case Map.fetch(@builtins, normalized) do
      {:ok, field} ->
        {:ok, field}

      :error ->
        parse_metadata(value)
    end
  end

  def parse(_value), do: {:error, :unsupported_field}

  @spec valid?(term()) :: boolean()
  def valid?(field) when is_atom(field), do: field in @builtin_fields

  def valid?({:attribute, name}), do: valid_metadata_name?(name)

  def valid?({:state_meta, state, name}),
    do: valid_metadata_name?(state) and valid_metadata_name?(name)

  def valid?(_field), do: false

  @spec value_type(t()) :: :integer | :keyword | :dynamic
  def value_type(field) when is_atom(field) do
    cond do
      MapSet.member?(@integer_fields, field) -> :integer
      MapSet.member?(@keyword_fields, field) -> :keyword
      true -> :dynamic
    end
  end

  def value_type({:attribute, _name}), do: :dynamic
  def value_type({:state_meta, _state, _name}), do: :dynamic

  @spec metadata?(t()) :: boolean()
  def metadata?({:attribute, _name}), do: true
  def metadata?({:state_meta, _state, _name}), do: true
  def metadata?(_field), do: false

  @spec external_name(t()) :: binary()
  def external_name({:attribute, name}), do: "attribute." <> name
  def external_name({:state_meta, state, name}), do: "state_meta." <> state <> "." <> name
  def external_name(field) when is_atom(field), do: Atom.to_string(field)

  @spec fetch(map(), t()) :: {:ok, term()} | :missing
  def fetch(record, :run_id) when is_map(record), do: fetch_map(record, :id, "id")

  def fetch(record, {:attribute, name}) when is_map(record) do
    with {:ok, attributes} when is_map(attributes) <-
           fetch_map(record, :attributes, "attributes") do
      fetch_dynamic(attributes, name)
    else
      _ -> :missing
    end
  end

  def fetch(record, {:state_meta, state, name}) when is_map(record) do
    with {:ok, state_meta} when is_map(state_meta) <-
           fetch_map(record, :state_meta, "state_meta"),
         {:ok, metadata} when is_map(metadata) <- fetch_dynamic(state_meta, state) do
      fetch_dynamic(metadata, name)
    else
      _ -> :missing
    end
  end

  def fetch(record, field) when is_map(record) and is_atom(field) do
    fetch_map(record, field, Atom.to_string(field))
  end

  def fetch(_record, _field), do: :missing

  defp parse_metadata(value) do
    case :binary.split(value, ".", [:global]) do
      [prefix, name] ->
        if ascii_downcase(prefix) == "attribute" and valid_metadata_name?(name),
          do: {:ok, {:attribute, name}},
          else: unsupported_field()

      [prefix, state, name] ->
        if ascii_downcase(prefix) == "state_meta" and valid_metadata_name?(state) and
             valid_metadata_name?(name),
           do: {:ok, {:state_meta, state, name}},
           else: unsupported_field()

      _parts ->
        unsupported_field()
    end
  end

  defp valid_metadata_name?(name)
       when is_binary(name) and name != "" and byte_size(name) <= @max_metadata_name_bytes do
    not String.starts_with?(name, "__") and valid_metadata_bytes?(name)
  end

  defp valid_metadata_name?(_name), do: false

  defp valid_metadata_bytes?(<<>>), do: true

  defp valid_metadata_bytes?(<<byte, rest::binary>>)
       when byte in ?a..?z or byte in ?A..?Z or byte in ?0..?9 or byte in [?_, ?-],
       do: valid_metadata_bytes?(rest)

  defp valid_metadata_bytes?(_name), do: false

  defp fetch_map(map, atom_key, string_key) do
    case Map.fetch(map, atom_key) do
      {:ok, value} -> {:ok, value}
      :error -> fetch_dynamic(map, string_key)
    end
  end

  defp fetch_dynamic(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> :missing
    end
  end

  defp unsupported_field, do: {:error, :unsupported_field}

  defp ascii_downcase(value), do: for(<<byte <- value>>, into: <<>>, do: <<lower(byte)>>)
  defp lower(byte) when byte in ?A..?Z, do: byte + 32
  defp lower(byte), do: byte
end
