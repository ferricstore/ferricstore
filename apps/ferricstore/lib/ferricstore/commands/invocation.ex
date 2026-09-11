defmodule Ferricstore.Commands.Invocation do
  @moduledoc false

  alias Ferricstore.Commands.{Extension, Hash, Set}
  alias Ferricstore.Flow.Keys

  @definition_catalog_key "f:{f}:invocation:definitions:1"
  @max_name_bytes 128
  @max_payload_bytes 262_144
  @max_partition_bytes 4_096
  @name_pattern ~r/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/

  @spec handle(binary(), [binary()], map()) :: term()
  def handle("INVOCATION.DEFINITION.PUT", [encoded], store) do
    with {:ok, definition} <- decode_map(encoded, "invocation definition"),
         {:ok, name} <- definition_name(definition),
         :ok <- validate_definition(definition),
         encoded <- Jason.encode!(definition),
         result when is_integer(result) <-
           Hash.handle("HSET", [@definition_catalog_key, name, encoded], store) do
      "OK"
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, "ERR invocation definition write failed"}
    end
  end

  def handle("INVOCATION.DEFINITION.PUT", _args, _store),
    do: wrong_arity("invocation.definition.put")

  def handle("INVOCATION.DEFINITION.GET", [name], store) do
    with :ok <- validate_name(name) do
      case Hash.handle("HGET", [@definition_catalog_key, name], store) do
        nil -> nil
        encoded when is_binary(encoded) -> decode_stored_definition(encoded)
        {:error, _reason} = error -> error
        _invalid -> {:error, "ERR invalid stored invocation definition"}
      end
    end
  end

  def handle("INVOCATION.DEFINITION.GET", _args, _store),
    do: wrong_arity("invocation.definition.get")

  def handle("INVOCATION.DEFINITION.LIST", [], store) do
    case Hash.handle("HGETALL", [@definition_catalog_key], store) do
      values when is_list(values) -> decode_definition_list(values)
      {:error, _reason} = error -> error
      _invalid -> {:error, "ERR invalid stored invocation definition list"}
    end
  end

  def handle("INVOCATION.DEFINITION.LIST", _args, _store),
    do: wrong_arity("invocation.definition.list")

  def handle("INVOCATION.CREATE", [name, encoded], store) do
    with :ok <- validate_name(name),
         {:ok, definition} <- fetch_definition(name, store),
         :ok <- enabled(definition),
         {:ok, envelope} <- decode_map(encoded, "invocation create envelope"),
         context <- trusted_context(store),
         :ok <- authorize_create(definition, context),
         {:ok, idempotency_key} <- idempotency_key(envelope, definition),
         {:ok, attrs, payload} <- attrs_and_payload(envelope, definition),
         {:ok, identity} <- invocation_identity(name, definition, context, idempotency_key),
         :ok <- remember_partition(name, identity.partition_key, store),
         {:ok, record} <- create_flow(identity, definition, attrs, payload, store) do
      created_response(identity, definition, record)
    end
  end

  def handle("INVOCATION.CREATE", _args, _store), do: wrong_arity("invocation.create")

  def handle("INVOCATION.GET", [id], store) do
    with {:ok, identity} <- parse_invocation_id(id),
         context <- trusted_context(store),
         :ok <- authorize_access(identity, context),
         {:ok, record} <-
           Ferricstore.Flow.get(flow_ctx(store), id, partition_key: identity.partition_key),
         {:ok, metadata} <- invocation_metadata_with_policy(identity, record, store) do
      metadata
    else
      {:error, _reason} = error -> normalize_flow_error(error)
    end
  end

  def handle("INVOCATION.GET", _args, _store), do: wrong_arity("invocation.get")

  def handle("INVOCATION.PARTITION.LIST", [name], store),
    do: list_partitions(name, store)

  def handle("INVOCATION.PARTITION.LIST", _args, _store),
    do: wrong_arity("invocation.partition.list")

  @doc false
  @spec acl_keys(binary(), [binary()]) :: [binary()]
  def acl_keys("INVOCATION.DEFINITION.PUT", [encoded]) do
    case Jason.decode(encoded) do
      {:ok, %{"name" => name}} when is_binary(name) -> ["invocation:definition:#{name}"]
      _invalid -> ["invocation:definition:*"]
    end
  end

  def acl_keys("INVOCATION.DEFINITION.GET", [name]) when is_binary(name),
    do: ["invocation:definition:#{name}"]

  def acl_keys("INVOCATION.DEFINITION.LIST", []), do: ["invocation:definition:*"]

  def acl_keys("INVOCATION.CREATE", [name, _encoded]) when is_binary(name),
    do: [logical_acl_key(name)]

  def acl_keys("INVOCATION.GET", [id]) do
    case parse_invocation_id(id) do
      {:ok, identity} -> [logical_acl_key(identity.name)]
      {:error, _reason} -> ["invocation:invalid"]
    end
  end

  def acl_keys("INVOCATION.PARTITION.LIST", [name]) when is_binary(name),
    do: [logical_acl_key(name)]

  def acl_keys(_command, _args), do: ["invocation:invalid"]

  defp fetch_definition(name, store) do
    case handle("INVOCATION.DEFINITION.GET", [name], store) do
      nil -> {:error, "ERR definition_not_found"}
      %{} = definition -> {:ok, definition}
      {:error, _reason} = error -> error
    end
  end

  defp decode_stored_definition(encoded) do
    case Jason.decode(encoded) do
      {:ok, %{} = definition} -> definition
      _invalid -> {:error, "ERR invalid stored invocation definition"}
    end
  end

  defp decode_definition_list(flat_values) do
    flat_values
    |> Enum.chunk_every(2)
    |> Enum.reduce_while({:ok, []}, fn
      [_name, encoded], {:ok, definitions} ->
        case decode_stored_definition(encoded) do
          %{} = definition -> {:cont, {:ok, [definition | definitions]}}
          {:error, _reason} = error -> {:halt, error}
        end

      _invalid, _definitions ->
        {:halt, {:error, "ERR invalid stored invocation definition list"}}
    end)
    |> case do
      {:ok, definitions} -> Enum.sort_by(definitions, & &1["name"])
      {:error, _reason} = error -> error
    end
  end

  defp definition_name(%{"name" => name}) do
    with :ok <- validate_name(name), do: {:ok, name}
  end

  defp definition_name(_definition), do: {:error, "ERR invalid_invocation_name"}

  defp validate_definition(definition) do
    map_fields = ~w(target limits runner refs acl partition)

    cond do
      not is_boolean(Map.get(definition, "enabled", true)) ->
        {:error, "ERR invalid invocation definition"}

      not Enum.all?(map_fields, &is_map(Map.get(definition, &1, %{}))) ->
        {:error, "ERR invalid invocation definition"}

      not valid_optional_binary?(definition["flow_type"]) ->
        {:error, "ERR invalid invocation definition"}

      not valid_optional_binary?(definition["initial_state"]) ->
        {:error, "ERR invalid invocation definition"}

      true ->
        :ok
    end
  end

  defp valid_optional_binary?(nil), do: true
  defp valid_optional_binary?(value), do: is_binary(value) and value != ""

  defp enabled(%{"enabled" => false}), do: {:error, "ERR invocation_disabled"}
  defp enabled(_definition), do: :ok

  defp decode_map(encoded, label) when is_binary(encoded) do
    case Jason.decode(encoded) do
      {:ok, %{} = decoded} -> {:ok, decoded}
      _invalid -> {:error, "ERR invalid #{label}"}
    end
  end

  defp idempotency_key(envelope, definition) do
    key = normalize_optional_string(envelope["idempotency_key"])
    required? = get_in(definition, ["limits", "idempotency_required"]) == true

    if required? and is_nil(key),
      do: {:error, "ERR idempotency_key_required"},
      else: {:ok, key}
  end

  defp attrs_and_payload(envelope, definition) do
    case Map.fetch(envelope, "attrs") do
      {:ok, attrs} when is_map(attrs) ->
        value = Map.get(attrs, "payload")
        encoded = if is_nil(value), do: nil, else: Jason.encode!(value)
        max_bytes = positive_limit(get_in(definition, ["limits", "max_payload_bytes"]))

        if is_binary(encoded) and byte_size(encoded) > max_bytes,
          do: {:error, "ERR payload_too_large"},
          else: {:ok, attrs, encoded}

      _missing_or_invalid ->
        {:error, "ERR invalid invocation attributes"}
    end
  end

  defp invocation_identity(name, definition, context, idempotency_key) do
    subject = normalize_optional_string(context["subject"])
    token = invocation_token(name, subject, idempotency_key)
    base_id = "inv1.#{encode_component(name)}.#{token}"

    with {:ok, partition_key} <- partition_key(definition, name, subject, base_id) do
      id = base_id <> "." <> encode_component(partition_key)

      {:ok,
       %{
         id: id,
         name: name,
         subject: subject,
         partition_key: partition_key,
         idempotent?: is_binary(idempotency_key)
       }}
    end
  end

  defp invocation_token(name, subject, idempotency_key)
       when is_binary(idempotency_key) do
    digest({name, subject, idempotency_key})
  end

  defp invocation_token(_name, _subject, nil) do
    :crypto.strong_rand_bytes(18) |> Base.url_encode64(padding: false)
  end

  defp partition_key(definition, name, subject, base_id) do
    template = get_in(definition, ["partition", "key"])

    value =
      case template do
        nil ->
          Keys.auto_partition_key(base_id)

        template when is_binary(template) and template != "" ->
          template
          |> String.replace("{name}", name)
          |> replace_required("{subject}", subject)

        _invalid ->
          :invalid
      end

    cond do
      value == :missing -> {:error, "ERR subject_required"}
      value == :invalid -> {:error, "ERR invalid invocation partition"}
      not is_binary(value) or value == "" -> {:error, "ERR invalid invocation partition"}
      byte_size(value) > @max_partition_bytes -> {:error, "ERR invocation partition too large"}
      true -> {:ok, value}
    end
  end

  defp replace_required(value, token, replacement) when is_binary(replacement),
    do: String.replace(value, token, replacement)

  defp replace_required(value, token, nil) do
    if is_binary(value) and String.contains?(value, token), do: :missing, else: value
  end

  defp create_flow(identity, definition, attrs, payload, store) do
    attributes =
      attrs
      |> Map.get("attributes", %{})
      |> normalize_attributes()
      |> Map.merge(%{
        "invocation_name" => identity.name,
        "invocation_subject" => identity.subject
      })
      |> reject_nil_values()

    opts =
      [
        type: Map.get(definition, "flow_type", "invocation:#{identity.name}"),
        state: Map.get(definition, "initial_state", "queued"),
        partition_key: identity.partition_key,
        idempotent: identity.idempotent?,
        attributes: attributes
      ]
      |> put_if_present(:payload, payload)
      |> put_if_present(:correlation_id, attrs["correlation_id"])
      |> put_if_present(:run_at_ms, attrs["run_at_ms"])

    case Ferricstore.Flow.create(flow_ctx(store), identity.id, opts) do
      :ok ->
        Ferricstore.Flow.get(flow_ctx(store), identity.id, partition_key: identity.partition_key)

      {:ok, %{} = record} ->
        {:ok, record}

      {:error, reason} when is_binary(reason) ->
        normalize_create_error(reason)

      {:error, _reason} ->
        {:error, "ERR invocation create failed"}
    end
  end

  defp normalize_create_error(reason) do
    if String.contains?(String.downcase(reason), "idempotency conflict"),
      do: {:error, "ERR idempotency_conflict"},
      else: {:error, reason}
  end

  defp created_response(identity, definition, record) do
    %{
      "invocation_id" => identity.id,
      "id" => identity.id,
      "name" => identity.name,
      "flow_type" => Map.get(definition, "flow_type", "invocation:#{identity.name}"),
      "state" => field(record, "state") || Map.get(definition, "initial_state", "queued"),
      "partition_key" => identity.partition_key,
      "subject" => identity.subject
    }
    |> reject_nil_values()
  end

  defp invocation_metadata(_identity, nil), do: nil

  defp invocation_metadata(identity, record) when is_map(record) do
    type = field(record, "type")
    attributes = field(record, "attributes")
    invocation_name = if is_map(attributes), do: field(attributes, "invocation_name")
    invocation_subject = if is_map(attributes), do: field(attributes, "invocation_subject")

    if is_binary(type) and type != "" and invocation_name == identity.name do
      %{
        "id" => identity.id,
        "invocation_id" => identity.id,
        "name" => identity.name,
        "flow_type" => type,
        "partition_key" => identity.partition_key,
        "subject" => identity.subject || invocation_subject
      }
      |> reject_nil_values()
    else
      nil
    end
  end

  defp invocation_metadata_with_policy(identity, record, store) do
    case invocation_metadata(identity, record) do
      nil ->
        {:ok, nil}

      metadata ->
        with {:ok, definition} <- fetch_definition(identity.name, store) do
          {:ok, Map.put(metadata, "value_policy", value_policy(definition))}
        end
    end
  end

  defp remember_partition(name, partition_key, store) do
    case Set.handle("SADD", [partition_catalog_key(name), partition_key], store) do
      value when is_integer(value) -> :ok
      {:error, _reason} = error -> error
      _invalid -> {:error, "ERR invocation partition catalog write failed"}
    end
  end

  defp list_partitions(name, store) do
    with :ok <- validate_name(name),
         {:ok, _definition} <- fetch_definition(name, store) do
      case Set.handle("SMEMBERS", [partition_catalog_key(name)], store) do
        partitions when is_list(partitions) ->
          Enum.sort(partitions)

        {:error, _reason} = error ->
          error

        _invalid ->
          {:error, "ERR invalid invocation partition catalog"}
      end
    end
  end

  defp partition_catalog_key(name), do: "f:{f}:invocation:partitions:1:" <> digest(name)

  defp parse_invocation_id(id) when is_binary(id) do
    case String.split(id, ".", parts: 4) do
      ["inv1", encoded_name, token, encoded_partition] when token != "" ->
        with {:ok, name} <- decode_component(encoded_name),
             :ok <- validate_name(name),
             {:ok, partition_key} <- decode_non_empty_component(encoded_partition) do
          {:ok,
           %{
             id: id,
             name: name,
             subject: nil,
             partition_key: partition_key
           }}
        else
          _invalid -> {:error, "ERR invocation_not_found"}
        end

      _invalid ->
        {:error, "ERR invocation_not_found"}
    end
  end

  defp parse_invocation_id(_id), do: {:error, "ERR invocation_not_found"}

  defp authorize_create(definition, context) do
    invoke_key = get_in(definition, ["acl", "invoke_key"])

    allowed =
      [
        "invocation:*",
        "invocation:create:*",
        "invocation:create:#{definition["name"]}",
        render_acl_key(
          invoke_key,
          definition["name"],
          normalize_optional_string(context["subject"])
        )
      ]
      |> Enum.reject(&is_nil/1)

    authorize_scopes(context, allowed)
  end

  defp authorize_access(identity, context) do
    allowed =
      [
        "invocation:*",
        "invocation:#{identity.id}",
        "invocation:name:#{identity.name}"
      ]
      |> Enum.reject(&is_nil/1)

    authorize_scopes(context, allowed)
  end

  defp authorize_scopes(%{"scopes" => scopes}, allowed) when is_list(scopes) and scopes != [] do
    if "*" in scopes or Enum.any?(allowed, &(&1 in scopes)),
      do: :ok,
      else: {:error, "ERR forbidden"}
  end

  defp authorize_scopes(_context, _allowed), do: :ok

  defp render_acl_key(nil, _name, _subject), do: nil

  defp render_acl_key(template, name, subject) when is_binary(template) do
    template
    |> String.replace("{name}", name)
    |> replace_required("{subject}", subject)
    |> case do
      :missing -> nil
      rendered -> rendered
    end
  end

  defp render_acl_key(_invalid, _name, _subject), do: nil

  defp trusted_context(store) do
    store
    |> Extension.request_context()
    |> stringify_map()
  end

  defp logical_acl_key(name), do: "invocation:#{name}"

  defp validate_name(name) when is_binary(name) and byte_size(name) <= @max_name_bytes do
    if Regex.match?(@name_pattern, name),
      do: :ok,
      else: {:error, "ERR invalid_invocation_name"}
  end

  defp validate_name(_name), do: {:error, "ERR invalid_invocation_name"}

  defp positive_limit(value) when is_integer(value) and value > 0, do: value
  defp positive_limit(_value), do: @max_payload_bytes

  defp normalize_optional_string(value) when value in [nil, ""], do: nil
  defp normalize_optional_string(value) when is_binary(value), do: value
  defp normalize_optional_string(_value), do: nil

  defp normalize_attributes(%{} = attributes), do: stringify_map(attributes)
  defp normalize_attributes(_invalid), do: %{}

  defp value_policy(definition) do
    %{
      "limits" => Map.take(Map.get(definition, "limits", %{}), ["max_result_bytes"]),
      "refs" =>
        Map.take(Map.get(definition, "refs", %{}), [
          "allowed_read_names",
          "allowed_write_names"
        ])
    }
  end

  defp stringify_map(%{} = map) do
    Map.new(map, fn {key, value} -> {to_string(key), stringify_value(value)} end)
  end

  defp stringify_value(%{} = map), do: stringify_map(map)
  defp stringify_value(values) when is_list(values), do: Enum.map(values, &stringify_value/1)
  defp stringify_value(value), do: value

  defp field(map, name), do: Map.get(map, name) || Map.get(map, String.to_atom(name))

  defp reject_nil_values(map) do
    map |> Enum.reject(fn {_key, value} -> is_nil(value) end) |> Map.new()
  end

  defp normalize_flow_error({:error, reason}) when is_binary(reason), do: {:error, reason}
  defp normalize_flow_error({:error, _reason}), do: {:error, "ERR invocation read failed"}

  defp flow_ctx(%FerricStore.Instance{} = ctx), do: ctx
  defp flow_ctx(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx
  defp flow_ctx(_store), do: FerricStore.Instance.get(:default)

  defp put_if_present(opts, _key, nil), do: opts
  defp put_if_present(opts, key, value), do: Keyword.put(opts, key, value)

  defp encode_component(value), do: Base.url_encode64(value, padding: false)
  defp decode_component(value), do: Base.url_decode64(value, padding: false)

  defp decode_non_empty_component(value) do
    case decode_component(value) do
      {:ok, ""} -> :error
      {:ok, decoded} -> {:ok, decoded}
      :error -> :error
    end
  end

  defp digest(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp wrong_arity(command),
    do: {:error, "ERR wrong number of arguments for '#{command}' command"}
end
