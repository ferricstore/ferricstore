defmodule Ferricstore.Flow.Query.IndexDefinition do
  @moduledoc false

  alias Ferricstore.Flow.Query.Field

  @storage_prefix "flow-composite-index:1:"
  @max_id_bytes 64
  @max_fields 8
  @max_workloads 16
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @lmdb_max_key_bytes 511
  @hashed_component_bytes 33
  @ordered_component_bytes 9
  @entry_identity_bytes 33
  @scope_header_bytes 3
  @max_scope_bytes 256

  @enforce_keys [:id, :version, :fields, :fingerprint]
  defstruct [
    :id,
    :version,
    :fields,
    :fingerprint,
    source: :runs,
    workloads: [],
    count_prefixes: [],
    scope_bytes: 0
  ]

  @type encoding :: :hashed | :ordered
  @type field_spec :: {Field.t(), :asc | :desc, encoding()}
  @type t :: %__MODULE__{
          id: binary(),
          version: pos_integer(),
          source: :runs,
          fields: [field_spec()],
          fingerprint: <<_::256>>,
          workloads: [binary()],
          count_prefixes: [pos_integer()],
          scope_bytes: non_neg_integer()
        }

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, atom()}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(%{} = attrs) do
    id = Map.get(attrs, :id) || Map.get(attrs, "id")
    version = Map.get(attrs, :version) || Map.get(attrs, "version")
    source = Map.get(attrs, :source) || Map.get(attrs, "source") || :runs
    fields = Map.get(attrs, :fields) || Map.get(attrs, "fields")
    workloads = Map.get(attrs, :workloads) || Map.get(attrs, "workloads") || []

    count_prefixes =
      Map.get(attrs, :count_prefixes) || Map.get(attrs, "count_prefixes") || []

    scope_bytes = Map.get(attrs, :scope_bytes) || Map.get(attrs, "scope_bytes") || 0

    with :ok <- validate_id(id),
         :ok <- validate_version(version),
         :ok <- validate_source(source),
         :ok <- validate_scope_bytes(scope_bytes),
         {:ok, workloads} <- validate_workloads(workloads),
         {:ok, fields} <- validate_fields(fields),
         {:ok, count_prefixes} <- validate_count_prefixes(count_prefixes, fields),
         :ok <- validate_tenant_lead(fields),
         :ok <- validate_unique_fields(fields),
         :ok <- validate_multivalue_fields(fields),
         :ok <- validate_field_encodings(fields),
         :ok <- validate_key_budget(fields, scope_bytes) do
      definition = %__MODULE__{
        id: id,
        version: version,
        source: source,
        workloads: workloads,
        count_prefixes: count_prefixes,
        fields: fields,
        scope_bytes: scope_bytes,
        fingerprint: fingerprint(id, version, source, fields, count_prefixes, scope_bytes)
      }

      {:ok, definition}
    end
  end

  def new(_attrs), do: {:error, :invalid_index_definition}

  @spec validate(t()) :: :ok | {:error, atom()}
  def validate(%__MODULE__{} = definition) do
    case new(Map.from_struct(definition)) do
      {:ok, ^definition} -> :ok
      {:ok, _different} -> {:error, :invalid_index_definition}
      {:error, _reason} = error -> error
    end
  end

  def validate(_definition), do: {:error, :invalid_index_definition}

  @spec new!(map() | keyword()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, definition} -> definition
      {:error, reason} -> raise ArgumentError, "invalid composite index definition: #{reason}"
    end
  end

  @spec storage_prefix(t()) :: binary()
  def storage_prefix(%__MODULE__{version: version, fingerprint: fingerprint}) do
    @storage_prefix <> <<version::unsigned-big-64, fingerprint::binary-size(32)>>
  end

  @spec global_storage_prefix() :: binary()
  def global_storage_prefix, do: @storage_prefix

  @spec max_entry_key_bytes(t()) :: pos_integer()
  def max_entry_key_bytes(%__MODULE__{fields: fields, scope_bytes: scope_bytes}) do
    storage_prefix_bytes() + scope_storage_bytes(scope_bytes) + fields_max_bytes(fields) +
      @entry_identity_bytes
  end

  defp validate_id(id)
       when is_binary(id) and id != "" and byte_size(id) <= @max_id_bytes do
    if valid_id_bytes?(id), do: :ok, else: {:error, :invalid_index_id}
  end

  defp validate_id(_id), do: {:error, :invalid_index_id}

  defp valid_id_bytes?(<<>>), do: true

  defp valid_id_bytes?(<<byte, rest::binary>>)
       when byte in ?a..?z or byte in ?A..?Z or byte in ?0..?9 or byte in [?_, ?-, ?:, ?.],
       do: valid_id_bytes?(rest)

  defp valid_id_bytes?(_id), do: false

  defp validate_version(version)
       when is_integer(version) and version > 0 and version <= @max_u64,
       do: :ok

  defp validate_version(_version), do: {:error, :invalid_index_version}

  defp validate_source(:runs), do: :ok
  defp validate_source(_source), do: {:error, :unsupported_source}

  defp validate_scope_bytes(scope_bytes)
       when is_integer(scope_bytes) and scope_bytes >= 0 and scope_bytes <= @max_scope_bytes,
       do: :ok

  defp validate_scope_bytes(_scope_bytes), do: {:error, :invalid_index_scope_bytes}

  defp validate_workloads(workloads)
       when is_list(workloads) and length(workloads) <= @max_workloads do
    cond do
      not Enum.all?(workloads, &valid_workload?/1) -> {:error, :invalid_index_workload}
      length(workloads) != length(Enum.uniq(workloads)) -> {:error, :duplicate_index_workload}
      true -> {:ok, workloads}
    end
  end

  defp validate_workloads(_workloads), do: {:error, :invalid_index_workload}

  defp validate_count_prefixes(prefixes, fields) when is_list(prefixes) do
    normalized = Enum.sort(prefixes)

    valid =
      prefixes == Enum.uniq(prefixes) and
        Enum.all?(prefixes, fn prefix_length ->
          is_integer(prefix_length) and prefix_length > 0 and prefix_length <= length(fields) and
            fields
            |> Enum.take(prefix_length)
            |> Enum.all?(fn {_field, _direction, encoding} -> encoding == :hashed end)
        end)

    if valid,
      do: {:ok, normalized},
      else: {:error, :invalid_index_count_prefixes}
  end

  defp validate_count_prefixes(_prefixes, _fields),
    do: {:error, :invalid_index_count_prefixes}

  defp valid_workload?(value)
       when is_binary(value) and value != "" and byte_size(value) <= 64,
       do: valid_workload_bytes?(value)

  defp valid_workload?(_value), do: false

  defp valid_workload_bytes?(<<>>), do: true

  defp valid_workload_bytes?(<<byte, rest::binary>>)
       when byte in ?a..?z or byte in ?A..?Z or byte in ?0..?9 or byte in [?_, ?-, ?:, ?.],
       do: valid_workload_bytes?(rest)

  defp valid_workload_bytes?(_value), do: false

  defp validate_fields(fields)
       when is_list(fields) and length(fields) >= 2 and length(fields) <= @max_fields do
    Enum.reduce_while(fields, {:ok, []}, fn spec, {:ok, acc} ->
      with {:ok, normalized} <- normalize_field_spec(spec),
           true <- Field.valid?(elem(normalized, 0)) do
        {:cont, {:ok, [normalized | acc]}}
      else
        false -> {:halt, {:error, :unsupported_index_field}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_fields(fields) when is_list(fields), do: {:error, :invalid_index_field_count}
  defp validate_fields(_fields), do: {:error, :invalid_index_fields}

  defp normalize_field_spec({field, direction}) when direction in [:asc, :desc] do
    encoding = default_encoding(field)
    {:ok, {field, direction, encoding}}
  end

  defp normalize_field_spec({field, direction, encoding})
       when direction in [:asc, :desc] and encoding in [:hashed, :ordered],
       do: {:ok, {field, direction, encoding}}

  defp normalize_field_spec(_invalid), do: {:error, :invalid_index_field}

  defp default_encoding(field) do
    if Field.valid?(field) and Field.value_type(field) == :integer,
      do: :ordered,
      else: :hashed
  end

  defp validate_field_encoding({field, direction, encoding}) do
    cond do
      not Field.valid?(field) ->
        {:error, :unsupported_index_field}

      encoding == :hashed and direction != :asc ->
        {:error, :invalid_hashed_index_direction}

      encoding == :ordered and Field.value_type(field) != :integer ->
        {:error, :unbounded_ordered_index_field}

      true ->
        :ok
    end
  end

  defp validate_field_encodings(fields) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case validate_field_encoding(field) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_tenant_lead([{:partition_key, :asc, :hashed} | _rest]), do: :ok
  defp validate_tenant_lead(_fields), do: {:error, :tenant_field_must_lead}

  defp validate_unique_fields(fields) do
    field_names = Enum.map(fields, &elem(&1, 0))

    if length(field_names) == length(Enum.uniq(field_names)),
      do: :ok,
      else: {:error, :duplicate_index_field}
  end

  defp validate_multivalue_fields(fields) do
    count =
      Enum.count(fields, fn {field, _direction, _encoding} -> match?({:attribute, _}, field) end)

    if count <= 1, do: :ok, else: {:error, :too_many_multivalue_fields}
  end

  defp validate_key_budget(fields, scope_bytes) do
    if storage_prefix_bytes() + scope_storage_bytes(scope_bytes) + fields_max_bytes(fields) +
         @entry_identity_bytes <=
         @lmdb_max_key_bytes,
       do: :ok,
       else: {:error, :index_key_budget_exceeded}
  end

  defp fields_max_bytes(fields) do
    Enum.reduce(fields, 0, fn
      {_field, _direction, :hashed}, bytes -> bytes + @hashed_component_bytes
      {_field, _direction, :ordered}, bytes -> bytes + @ordered_component_bytes
    end)
  end

  defp storage_prefix_bytes, do: byte_size(@storage_prefix) + 8 + 32

  defp scope_storage_bytes(0), do: 0
  defp scope_storage_bytes(scope_bytes), do: @scope_header_bytes + scope_bytes

  defp fingerprint(id, version, source, fields, count_prefixes, scope_bytes) do
    encoded_fields =
      Enum.map(fields, fn {field, direction, encoding} ->
        name = Field.external_name(field)
        direction_byte = if direction == :asc, do: 0, else: 1
        encoding_byte = if encoding == :hashed, do: 0, else: 1
        <<byte_size(name)::unsigned-big-16, name::binary, direction_byte, encoding_byte>>
      end)

    source_name = Atom.to_string(source)

    :crypto.hash(
      :sha256,
      IO.iodata_to_binary([
        "ferric.flow.composite-index/v1",
        <<byte_size(id)::unsigned-big-16, id::binary, version::unsigned-big-64>>,
        <<byte_size(source_name)::unsigned-big-16, source_name::binary>>,
        <<scope_bytes::unsigned-big-16>>,
        <<length(count_prefixes)::unsigned-big-8>>,
        Enum.map(count_prefixes, &<<&1::unsigned-big-8>>),
        <<length(fields)::unsigned-big-8>>,
        encoded_fields
      ])
    )
  end
end
