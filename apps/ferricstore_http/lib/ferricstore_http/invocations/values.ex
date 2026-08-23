defmodule FerricstoreHttp.Invocations.Values do
  @moduledoc "Invocation-scoped Flow value operations."

  alias FerricstoreHttp.{Auth, Config}
  alias FerricstoreHttp.Invocations.{Backend, Definition, Service}

  @max_value_name_bytes 4_096

  def get(id, name, context, config, opts \\ []) do
    with :ok <- validate_name(name),
         {:ok, scope} <- load_scope(id, context, config, opts),
         :ok <- authorize_name(scope.definition, name, :read),
         {:ok, bytes} <- read_value(scope, name) do
      {:ok, value_response(name, bytes)}
    end
  end

  def content(id, name, context, config, opts \\ []) do
    with :ok <- validate_name(name),
         {:ok, scope} <- load_scope(id, context, config, opts),
         :ok <- authorize_name(scope.definition, name, :read),
         {:ok, bytes} <- read_value(scope, name) do
      {:ok, to_binary(bytes), "application/octet-stream"}
    end
  end

  def batch(id, names, context, config, opts \\ [])

  def batch(id, names, context, config, opts) when is_list(names) do
    with :ok <- validate_names(names, config.max_batch_commands),
         {:ok, scope} <- load_scope(id, context, config, opts) do
      names = Enum.uniq(names)
      {:ok, %{"values" => batch_values(scope, names)}}
    end
  end

  def batch(_id, _names, _context, _config, _opts), do: {:error, :value_names_required}

  def put(id, attrs, context, config, opts \\ []) when is_map(attrs) do
    with {:ok, name} <- fetch_name(attrs),
         {:ok, scope} <- load_scope(id, context, config, opts),
         :ok <- authorize_name(scope.definition, name, :write),
         {:ok, bytes} <- value_bytes(attrs),
         :ok <- check_value_size(bytes, scope.definition),
         {:ok, ref} <-
           Backend.flow_value_put(context, bytes, config,
             owner_flow_id: id,
             name: name,
             partition_key: scope.record["partition_key"],
             deadline: Keyword.get(opts, :deadline)
           ) do
      {:ok, %{"name" => name, "ref" => extract_ref(ref)}}
    end
  end

  defp load_scope(id, %Auth.Context{} = context, %Config{} = config, opts) do
    with {:ok, %{metadata: metadata, record: record}} <-
           Service.scope(id, context, config, opts),
         {:ok, definition} <-
           Definition.from_value_policy(metadata["name"], metadata["value_policy"]) do
      {:ok,
       %{context: context, config: config, definition: definition, record: record, opts: opts}}
    end
  end

  defp batch_values(scope, names) do
    entries = Enum.map(names, &batch_entry(scope, &1))
    refs = entries |> Enum.flat_map(&entry_refs/1) |> Enum.uniq()
    fetched = fetch_refs(scope, refs)

    Map.new(entries, &batch_response(&1, fetched))
  end

  defp batch_entry(scope, name) do
    with :ok <- authorize_name(scope.definition, name, :read),
         {:ok, ref_or_value} <- find_value(scope.record, name) do
      {name, ref_or_value}
    else
      {:error, reason} -> {name, {:error, reason}}
    end
  end

  defp entry_refs({_name, {:ref, ref}}), do: [ref]
  defp entry_refs(_entry), do: []

  defp fetch_refs(_scope, []), do: %{}

  defp fetch_refs(scope, refs) do
    case Backend.flow_value_mget(scope.context, refs, scope.config,
           max_bytes: Definition.max_result_bytes(scope.definition),
           deadline: Keyword.get(scope.opts, :deadline)
         ) do
      {:ok, values} when is_list(values) and length(values) == length(refs) ->
        refs
        |> Enum.zip(values)
        |> Map.new(fn
          {ref, nil} -> {ref, {:error, :value_not_found}}
          {ref, value} -> {ref, {:ok, value}}
        end)

      {:error, reason} ->
        Map.new(refs, &{&1, {:error, reason}})

      _invalid ->
        Map.new(refs, &{&1, {:error, :backend_error}})
    end
  end

  defp batch_response({name, {:inline, value}}, _fetched),
    do: {name, value_response(name, value)}

  defp batch_response({name, {:ref, ref}}, fetched) do
    case Map.fetch!(fetched, ref) do
      {:ok, value} -> {name, value_response(name, value)}
      {:error, reason} -> error_response(name, reason)
    end
  end

  defp batch_response({name, {:error, reason}}, _fetched), do: error_response(name, reason)

  defp error_response(name, reason),
    do: {name, %{"error" => %{"code" => error_code(reason)}}}

  defp authorize_name(definition, name, action) do
    if Definition.value_name_allowed?(definition, action, name),
      do: :ok,
      else: {:error, :value_name_forbidden}
  end

  defp read_value(scope, name) do
    with {:ok, ref_or_value} <- find_value(scope.record, name) do
      fetch_value(ref_or_value, scope)
    end
  end

  defp fetch_value({:inline, value}, _scope), do: {:ok, value}

  defp fetch_value({:ref, ref}, scope) do
    case Backend.flow_value_mget(scope.context, [ref], scope.config,
           max_bytes: Definition.max_result_bytes(scope.definition),
           deadline: Keyword.get(scope.opts, :deadline)
         ) do
      {:ok, [nil]} -> {:error, :value_not_found}
      {:ok, [value]} -> {:ok, value}
      {:error, _reason} = error -> error
      _invalid -> {:error, :backend_error}
    end
  end

  defp validate_names(names, max_names) do
    if length(names) <= max_names and Enum.all?(names, &valid_name?/1),
      do: :ok,
      else: {:error, :value_names_required}
  end

  defp valid_name?(name),
    do: is_binary(name) and name != "" and byte_size(name) <= @max_value_name_bytes

  defp validate_name(name) do
    if valid_name?(name), do: :ok, else: {:error, :value_name_required}
  end

  defp find_value(record, name) do
    refs = record["value_refs"] || %{}
    values = record["values"] || %{}

    cond do
      is_binary(refs[name]) -> {:ok, {:ref, refs[name]}}
      ref = ref_from_value(refs[name]) -> {:ok, {:ref, ref}}
      ref = ref_from_value(values[name]) -> {:ok, {:ref, ref}}
      Map.has_key?(values, name) -> {:ok, {:inline, values[name]}}
      true -> {:error, :value_not_found}
    end
  end

  defp ref_from_value(%{"ref" => ref}) when is_binary(ref), do: ref
  defp ref_from_value(_value), do: nil

  defp value_response(name, bytes) when is_binary(bytes) do
    case Jason.decode(bytes) do
      {:ok, decoded} ->
        %{"name" => name, "content_type" => "application/json", "json" => decoded}

      {:error, _reason} ->
        %{
          "name" => name,
          "content_type" => "application/octet-stream",
          "bytes_base64" => Base.encode64(bytes)
        }
    end
  end

  defp value_response(name, value),
    do: %{"name" => name, "content_type" => "application/json", "json" => value}

  defp fetch_name(%{"name" => name}) do
    case validate_name(name) do
      :ok -> {:ok, name}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_name(_attrs), do: {:error, :value_name_required}

  defp value_bytes(%{"bytes_base64" => encoded}) when is_binary(encoded) do
    case Base.decode64(encoded) do
      {:ok, bytes} -> {:ok, bytes}
      :error -> {:error, :invalid_value_encoding}
    end
  end

  defp value_bytes(%{"json" => value}), do: {:ok, Jason.encode!(value)}
  defp value_bytes(%{"value" => value}) when is_binary(value), do: {:ok, value}
  defp value_bytes(_attrs), do: {:error, :value_required}

  defp check_value_size(bytes, definition) do
    if byte_size(bytes) <= Definition.max_result_bytes(definition),
      do: :ok,
      else: {:error, :value_too_large}
  end

  defp extract_ref(%{"ref" => ref}), do: ref
  defp extract_ref(ref), do: ref
  defp to_binary(value) when is_binary(value), do: value
  defp to_binary(value), do: Jason.encode!(value)
  defp error_code(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp error_code(_reason), do: "backend_error"
end
