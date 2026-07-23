defmodule Ferricstore.Flow.Query.IndexCatalog do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Field, IndexDefinition}

  @contract_version "ferric.flow.query.index-catalog/v1"
  @max_catalog_bytes 256 * 1_024
  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  @type t :: %{
          version: pos_integer(),
          contract_version: binary(),
          digest: <<_::256>>,
          definitions: [IndexDefinition.t()]
        }

  @spec default_path() :: binary()
  def default_path do
    priv_dir = :ferricstore |> :code.priv_dir() |> to_string()

    resolved_priv_dir =
      case File.read_link(priv_dir) do
        {:ok, target} -> Path.expand(target, Path.dirname(priv_dir))
        {:error, _reason} -> priv_dir
      end

    Path.join(resolved_priv_dir, "flow_query/index-catalog.json")
  end

  @spec load(binary(), keyword()) :: {:ok, t()} | {:error, atom() | term()}
  def load(path \\ default_path(), opts \\ [])

  def load(path, opts) when is_binary(path) and path != "" and is_list(opts) do
    scope_bytes = Keyword.get(opts, :scope_bytes, 0)

    with {:ok, encoded} <- Ferricstore.FS.read_nofollow(path, @max_catalog_bytes),
         true <- encoded != "",
         {:ok, decoded} <- Jason.decode(encoded),
         {:ok, version, definitions} <- decode_catalog(decoded, scope_bytes) do
      {:ok,
       %{
         version: version,
         contract_version: @contract_version,
         digest: digest(version, definitions),
         definitions: definitions
       }}
    else
      false -> {:error, :invalid_query_index_catalog_file}
      {:error, %Jason.DecodeError{}} -> {:error, :invalid_query_index_catalog_json}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_query_index_catalog}
    end
  end

  def load(_path, _opts), do: {:error, :invalid_query_index_catalog_file}

  defp decode_catalog(
         %{
           "catalog_version" => version,
           "contract_version" => @contract_version,
           "indexes" => indexes
         },
         scope_bytes
       )
       when is_integer(version) and version > 0 and version <= @max_u64 and is_list(indexes) and
              indexes != [] and length(indexes) <= 16 and is_integer(scope_bytes) do
    indexes
    |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, acc} ->
      case decode_definition(attrs, scope_bytes) do
        {:ok, definition} -> {:cont, {:ok, [definition | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} ->
        definitions = Enum.reverse(reversed)

        if unique_definitions?(definitions),
          do: {:ok, version, definitions},
          else: {:error, :duplicate_query_index_catalog_entry}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_catalog(_catalog, _scope_bytes), do: {:error, :invalid_query_index_catalog}

  defp decode_definition(
         %{
           "id" => id,
           "version" => version,
           "source" => "runs",
           "workloads" => workloads,
           "fields" => fields
         } = attrs,
         scope_bytes
       )
       when is_list(fields) and is_list(workloads) do
    count_prefixes = Map.get(attrs, "count_prefixes", [])

    with {:ok, fields} <- decode_fields(fields),
         {:ok, definition} <-
           IndexDefinition.new(%{
             id: id,
             version: version,
             source: :runs,
             workloads: workloads,
             count_prefixes: count_prefixes,
             scope_bytes: scope_bytes,
             fields: fields
           }) do
      {:ok, definition}
    end
  end

  defp decode_definition(_definition, _scope_bytes),
    do: {:error, :invalid_query_index_catalog_entry}

  defp decode_fields(fields) do
    Enum.reduce_while(fields, {:ok, []}, fn field, {:ok, acc} ->
      case decode_field(field) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_field(%{"name" => name, "direction" => direction, "encoding" => encoding}) do
    with {:ok, field} <- Field.parse(name),
         {:ok, direction} <- decode_direction(direction),
         {:ok, encoding} <- decode_encoding(encoding) do
      {:ok, {field, direction, encoding}}
    end
  end

  defp decode_field(_field), do: {:error, :invalid_query_index_catalog_field}

  defp decode_direction("asc"), do: {:ok, :asc}
  defp decode_direction("desc"), do: {:ok, :desc}
  defp decode_direction(_direction), do: {:error, :invalid_query_index_catalog_field}

  defp decode_encoding("hashed"), do: {:ok, :hashed}
  defp decode_encoding("ordered"), do: {:ok, :ordered}
  defp decode_encoding(_encoding), do: {:error, :invalid_query_index_catalog_field}

  defp unique_definitions?(definitions) do
    logical_ids = Enum.map(definitions, & &1.id)
    length(logical_ids) == length(Enum.uniq(logical_ids))
  end

  defp digest(version, definitions) do
    canonical =
      definitions
      |> Enum.map(fn definition ->
        {definition.id, definition.version, definition.source, Enum.sort(definition.workloads),
         definition.fields, definition.count_prefixes, definition.scope_bytes,
         definition.fingerprint}
      end)
      |> Enum.sort_by(fn {id, definition_version, _source, _workloads, _fields, _prefixes,
                          _scope_bytes, _fingerprint} ->
        {id, definition_version}
      end)

    :crypto.hash(
      :sha256,
      :erlang.term_to_binary({@contract_version, version, canonical}, minor_version: 2)
    )
  end
end
