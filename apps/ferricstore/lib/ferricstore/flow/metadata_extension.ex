defmodule FerricStore.Flow.MetadataExtension do
  @moduledoc """
  Frozen edition boundary for hidden Flow system metadata.

  Providers resolve trusted ingress authority once. Core validates and seals
  their output before it enters a replicated command or physical query plan.
  """

  alias Ferricstore.Flow.SystemMetadata
  alias Ferricstore.TermCodec

  @max_fields 16
  @max_generation 0xFFFF_FFFF_FFFF_FFFF
  @max_field_id 0xFFFF
  @max_name_bytes 64
  @types [:uint64, :int64, :keyword, :boolean, :datetime]
  @roles [:isolation_scope, :system_metadata]
  @visibilities [:hidden, :operator]
  @mutabilities [:immutable, :server_mutable]
  @indexes [:required_prefix, :exact, :none]
  @requirements [:shared, :always, :optional]

  defmodule Snapshot do
    @moduledoc false
    @enforce_keys [:implementation, :mode, :generation, :fields, :schema_digest]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            implementation: module(),
            mode: :dedicated | :shared,
            generation: non_neg_integer(),
            fields: %{optional(non_neg_integer()) => map()},
            schema_digest: <<_::256>>
          }
  end

  @type query_scope ::
          :unscoped
          | {:required, [{non_neg_integer(), :eq, term()}]}
          | {:required_union, [[{non_neg_integer(), :eq, term()}]]}

  @callback configure(keyword()) ::
              {:ok,
               %{
                 required(:mode) => :dedicated | :shared,
                 required(:generation) => non_neg_integer(),
                 required(:fields) => [map()]
               }}
              | {:error, term()}
  @callback bind_write(atom(), map(), Snapshot.t()) :: {:ok, map()} | {:error, term()}
  @callback bind_query(atom(), map(), Snapshot.t()) :: {:ok, query_scope()} | {:error, term()}

  @spec configured_implementation(keyword()) :: module()
  def configured_implementation(opts) when is_list(opts) do
    opts
    |> Keyword.get(
      :flow_metadata_extension,
      Application.get_env(:ferricstore, __MODULE__, __MODULE__.Disabled)
    )
    |> validate_implementation!()
  end

  @spec configure(module(), keyword()) :: {:ok, Snapshot.t()} | {:error, atom()}
  def configure(implementation, opts) when is_atom(implementation) and is_list(opts) do
    with implementation <- validate_implementation!(implementation),
         {:ok, configuration} <- safe_apply(implementation, :configure, [opts]),
         {:ok, normalized} <- validate_configuration(configuration) do
      {:ok,
       %Snapshot{
         implementation: implementation,
         mode: normalized.mode,
         generation: normalized.generation,
         fields: normalized.fields,
         schema_digest: normalized.schema_digest
       }}
    else
      {:error, :invalid_flow_metadata_schema} = error -> error
      {:error, _reason} -> {:error, :flow_metadata_extension_failure}
    end
  rescue
    ArgumentError -> {:error, :flow_metadata_extension_failure}
  end

  @spec validate_configuration(term()) :: {:ok, map()} | {:error, :invalid_flow_metadata_schema}
  def validate_configuration(%{mode: mode, generation: generation, fields: fields})
      when mode in [:dedicated, :shared] and is_integer(generation) and generation >= 0 and
             generation <= @max_generation and is_list(fields) and length(fields) <= @max_fields do
    with {:ok, normalized_fields} <- normalize_fields(fields),
         :ok <- validate_unique_fields(normalized_fields) do
      fields_by_id = Map.new(normalized_fields, &{&1.id, &1})
      canonical = {mode, generation, Enum.sort_by(normalized_fields, & &1.id)}

      {:ok,
       %{
         mode: mode,
         generation: generation,
         fields: fields_by_id,
         schema_digest: schema_digest(canonical)
       }}
    end
  end

  def validate_configuration(_configuration), do: invalid_schema()

  @spec bind_write(Snapshot.t(), atom(), map()) :: {:ok, SystemMetadata.t()} | {:error, atom()}
  def bind_write(%Snapshot{mode: :dedicated}, _operation, _trusted_context), do: {:ok, %{}}

  def bind_write(%Snapshot{} = snapshot, operation, trusted_context)
      when is_atom(operation) and is_map(trusted_context) do
    with {:ok, values} <-
           safe_apply(snapshot.implementation, :bind_write, [operation, trusted_context, snapshot]),
         {:ok, sealed} <- SystemMetadata.seal(snapshot.fields, values),
         :ok <- require_shared_fields(snapshot, sealed) do
      {:ok, sealed}
    else
      {:error, :invalid_flow_system_metadata} = error -> error
      {:error, :flow_scope_required} = error -> error
      {:error, _reason} -> {:error, :flow_metadata_extension_failure}
    end
  end

  def bind_write(%Snapshot{}, _operation, _trusted_context),
    do: {:error, :invalid_flow_system_metadata}

  @spec bind_write(map(), atom()) :: {:ok, SystemMetadata.t()} | {:error, atom()}
  def bind_write(ctx, operation) when is_map(ctx) and is_atom(operation) do
    with {:ok, snapshot} <- snapshot(ctx) do
      bind_write(snapshot, operation, trusted_context(ctx))
    end
  end

  @spec bind_query(Snapshot.t(), atom(), map()) :: {:ok, query_scope()} | {:error, atom()}
  def bind_query(%Snapshot{mode: :dedicated}, _source, _trusted_context), do: {:ok, :unscoped}

  def bind_query(%Snapshot{} = snapshot, source, trusted_context)
      when is_atom(source) and is_map(trusted_context) do
    with {:ok, scope} <-
           safe_apply(snapshot.implementation, :bind_query, [source, trusted_context, snapshot]),
         {:ok, sealed_scope} <- seal_query_scope(snapshot, scope),
         :ok <- require_query_scope(snapshot, sealed_scope) do
      {:ok, sealed_scope}
    else
      {:error, :invalid_flow_system_metadata} = error -> error
      {:error, :flow_scope_required} = error -> error
      {:error, _reason} -> {:error, :flow_metadata_extension_failure}
    end
  end

  def bind_query(%Snapshot{}, _source, _trusted_context),
    do: {:error, :invalid_flow_system_metadata}

  @spec bind_query(map(), atom()) :: {:ok, query_scope()} | {:error, atom()}
  def bind_query(ctx, source) when is_map(ctx) and is_atom(source) do
    with {:ok, snapshot} <- snapshot(ctx) do
      bind_query(snapshot, source, trusted_context(ctx))
    end
  end

  @doc false
  @spec bind_query_metadata(map(), atom()) ::
          {:ok, [SystemMetadata.t()]} | {:error, atom()}
  def bind_query_metadata(ctx, source) when is_map(ctx) and is_atom(source) do
    with {:ok, snapshot} <- snapshot(ctx),
         {:ok, scope} <- bind_query(snapshot, source, trusted_context(ctx)) do
      materialize_query_scope(snapshot, scope)
    end
  end

  @spec snapshot(term()) :: {:ok, Snapshot.t()} | {:error, :flow_metadata_extension_unavailable}
  def snapshot(%Snapshot{} = snapshot), do: {:ok, snapshot}

  def snapshot(%{flow_metadata_snapshot: %Snapshot{} = snapshot}), do: {:ok, snapshot}

  def snapshot(%{instance_ctx: instance_ctx}), do: snapshot(instance_ctx)

  def snapshot(_ctx), do: {:error, :flow_metadata_extension_unavailable}

  @doc false
  @spec fixed_scope_bytes(Snapshot.t()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def fixed_scope_bytes(%Snapshot{mode: :dedicated, fields: fields}) when map_size(fields) == 0,
    do: {:ok, 0}

  def fixed_scope_bytes(%Snapshot{mode: :shared, fields: fields}) do
    required_scope_fields =
      Enum.filter(fields, fn {_id, field} ->
        field.role == :isolation_scope and field.required_in in [:shared, :always]
      end)

    case required_scope_fields do
      [{_id, %{type: :uint64}}] -> {:ok, 8}
      _variable_or_missing -> {:error, :flow_scope_not_fixed_width}
    end
  end

  def fixed_scope_bytes(%Snapshot{}), do: {:error, :flow_scope_not_fixed_width}

  @spec validate_implementation!(term()) :: module()
  def validate_implementation!(implementation) when is_atom(implementation) do
    required = [configure: 1, bind_write: 3, bind_query: 3]

    if Code.ensure_loaded?(implementation) and
         Enum.all?(required, fn {name, arity} ->
           function_exported?(implementation, name, arity)
         end) do
      implementation
    else
      raise ArgumentError, "invalid Flow metadata extension"
    end
  end

  def validate_implementation!(_implementation),
    do: raise(ArgumentError, "invalid Flow metadata extension")

  defp normalize_fields(fields) do
    Enum.reduce_while(fields, {:ok, []}, fn field, {:ok, acc} ->
      case normalize_field(field) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, :invalid_flow_metadata_schema} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, :invalid_flow_metadata_schema} = error -> error
    end
  end

  defp normalize_field(%{
         id: id,
         version: version,
         logical_name: logical_name,
         type: type,
         role: role,
         visibility: visibility,
         mutability: mutability,
         index: index,
         required_in: required_in
       })
       when is_integer(id) and id > 0 and id <= @max_field_id and is_integer(version) and
              version > 0 and version <= @max_generation and is_binary(logical_name) and
              logical_name != "" and byte_size(logical_name) <= @max_name_bytes and
              type in @types and role in @roles and visibility in @visibilities and
              mutability in @mutabilities and index in @indexes and required_in in @requirements do
    if valid_logical_name?(logical_name) do
      {:ok,
       %{
         id: id,
         version: version,
         logical_name: logical_name,
         type: type,
         role: role,
         visibility: visibility,
         mutability: mutability,
         index: index,
         required_in: required_in
       }}
    else
      invalid_schema()
    end
  end

  defp normalize_field(_field), do: invalid_schema()

  defp validate_unique_fields(fields) do
    ids = Enum.map(fields, & &1.id)
    names = Enum.map(fields, & &1.logical_name)

    if length(ids) == length(Enum.uniq(ids)) and length(names) == length(Enum.uniq(names)),
      do: :ok,
      else: invalid_schema()
  end

  defp require_shared_fields(%Snapshot{fields: fields}, sealed) do
    required =
      fields
      |> Enum.filter(fn {_id, field} -> field.required_in in [:shared, :always] end)
      |> Enum.map(&elem(&1, 0))

    if Enum.all?(required, &Map.has_key?(sealed, &1)), do: :ok, else: scope_required()
  end

  defp seal_query_scope(_snapshot, :unscoped), do: {:ok, :unscoped}

  defp seal_query_scope(snapshot, {:required, clauses}) when is_list(clauses) do
    with {:ok, sealed} <- seal_clauses(snapshot, clauses) do
      {:ok, {:required, sealed}}
    end
  end

  defp seal_query_scope(snapshot, {:required_union, branches})
       when is_list(branches) and branches != [] and length(branches) <= 32 do
    branches
    |> Enum.reduce_while({:ok, []}, fn clauses, {:ok, acc} ->
      case seal_clauses(snapshot, clauses) do
        {:ok, sealed} -> {:cont, {:ok, [sealed | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, {:required_union, Enum.reverse(reversed)}}
      {:error, _reason} = error -> error
    end
  end

  defp seal_query_scope(_snapshot, _scope), do: {:error, :invalid_flow_system_metadata}

  defp seal_clauses(snapshot, clauses) when is_list(clauses) and clauses != [] do
    with true <- length(clauses) <= @max_fields,
         true <- Enum.all?(clauses, &match?({id, :eq, _value} when is_integer(id), &1)),
         values <- Map.new(clauses, fn {id, :eq, value} -> {id, value} end),
         true <- map_size(values) == length(clauses),
         {:ok, sealed} <- SystemMetadata.seal(snapshot.fields, values) do
      {:ok,
       sealed
       |> Enum.sort_by(&elem(&1, 0))
       |> Enum.map(fn {id, value} -> {id, :eq, SystemMetadata.typed_value(value)} end)}
    else
      _invalid -> {:error, :invalid_flow_system_metadata}
    end
  end

  defp seal_clauses(_snapshot, _clauses), do: {:error, :invalid_flow_system_metadata}

  defp require_query_scope(%Snapshot{mode: :shared} = snapshot, {:required, clauses}) do
    sealed = Map.new(clauses, fn {id, :eq, value} -> {id, value} end)
    require_shared_fields(snapshot, sealed)
  end

  defp require_query_scope(%Snapshot{mode: :shared} = snapshot, {:required_union, branches}) do
    if Enum.all?(branches, fn clauses ->
         sealed = Map.new(clauses, fn {id, :eq, value} -> {id, value} end)
         require_shared_fields(snapshot, sealed) == :ok
       end),
       do: :ok,
       else: scope_required()
  end

  defp require_query_scope(%Snapshot{mode: :shared}, :unscoped), do: scope_required()
  defp require_query_scope(_snapshot, _scope), do: :ok

  defp materialize_query_scope(_snapshot, :unscoped), do: {:ok, [%{}]}

  defp materialize_query_scope(snapshot, {:required, clauses}) do
    with {:ok, metadata} <- metadata_from_clauses(snapshot, clauses), do: {:ok, [metadata]}
  end

  defp materialize_query_scope(snapshot, {:required_union, branches}) do
    Enum.reduce_while(branches, {:ok, []}, fn clauses, {:ok, acc} ->
      case metadata_from_clauses(snapshot, clauses) do
        {:ok, metadata} -> {:cont, {:ok, [metadata | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp metadata_from_clauses(snapshot, clauses) do
    clauses
    |> Enum.reduce_while({:ok, %{}}, fn
      {id, :eq, {type, value}}, {:ok, values} ->
        case Map.fetch(snapshot.fields, id) do
          {:ok, %{type: ^type}} -> {:cont, {:ok, Map.put(values, id, value)}}
          _missing_or_mismatch -> {:halt, {:error, :invalid_flow_system_metadata}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_flow_system_metadata}}
    end)
    |> case do
      {:ok, values} -> SystemMetadata.seal(snapshot.fields, values)
      {:error, _reason} = error -> error
    end
  end

  defp safe_apply(module, function, args) do
    case apply(module, function, args) do
      {:ok, _value} = result -> result
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_provider_result}
    end
  rescue
    _error -> {:error, :provider_exception}
  catch
    _kind, _reason -> {:error, :provider_exit}
  end

  defp schema_digest({:dedicated, 0, []}), do: <<0::256>>

  defp schema_digest(canonical) do
    canonical
    |> TermCodec.encode()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp trusted_context(%{request_context: %{} = context}), do: context
  defp trusted_context(%{"request_context" => %{} = context}), do: context
  defp trusted_context(_ctx), do: %{}

  defp valid_logical_name?(<<>>), do: true

  defp valid_logical_name?(<<byte, rest::binary>>)
       when byte in ?a..?z or byte in ?0..?9 or byte == ?_,
       do: valid_logical_name?(rest)

  defp valid_logical_name?(_invalid), do: false

  defp invalid_schema, do: {:error, :invalid_flow_metadata_schema}
  defp scope_required, do: {:error, :flow_scope_required}
end

defmodule FerricStore.Flow.MetadataExtension.Disabled do
  @moduledoc false
  @behaviour FerricStore.Flow.MetadataExtension

  @impl true
  def configure(_opts), do: {:ok, %{mode: :dedicated, generation: 0, fields: []}}

  @impl true
  def bind_write(_operation, _trusted_context, _snapshot), do: {:ok, %{}}

  @impl true
  def bind_query(_source, _trusted_context, _snapshot), do: {:ok, :unscoped}
end
