defmodule FerricstoreHttp.Invocations.Definition do
  @moduledoc "Operational view of one named asynchronous invocation."

  @default_max_payload_bytes 262_144
  @default_max_result_bytes 1_048_576

  defstruct [
    :name,
    :flow_type,
    target: %{},
    enabled?: true,
    initial_state: "queued",
    limits: %{},
    runner: %{},
    refs: %{},
    acl: %{},
    partition: %{}
  ]

  @type t :: %__MODULE__{
          name: binary(),
          flow_type: binary(),
          target: map(),
          enabled?: boolean(),
          initial_state: binary(),
          limits: map(),
          runner: map(),
          refs: map(),
          acl: map(),
          partition: map()
        }

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(%{} = attrs) do
    attrs = stringify_keys(attrs)
    name = Map.get(attrs, "name")

    with :ok <- validate_name(name),
         :ok <- validate_boolean(Map.get(attrs, "enabled", true)),
         :ok <- validate_map_fields(attrs) do
      {:ok,
       %__MODULE__{
         name: name,
         enabled?: Map.get(attrs, "enabled", true),
         flow_type: Map.get(attrs, "flow_type", "invocation:#{name}"),
         initial_state: Map.get(attrs, "initial_state", "queued"),
         target: stringify_keys(Map.get(attrs, "target", %{})),
         limits: limits(Map.get(attrs, "limits", %{})),
         runner: stringify_keys(Map.get(attrs, "runner", %{})),
         refs: stringify_keys(Map.get(attrs, "refs", %{})),
         acl: stringify_keys(Map.get(attrs, "acl", %{})),
         partition: stringify_keys(Map.get(attrs, "partition", %{}))
       }}
    end
  end

  def new(_attrs), do: {:error, :invalid_invocation_definition}

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = definition) do
    %{
      "name" => definition.name,
      "enabled" => definition.enabled?,
      "flow_type" => definition.flow_type,
      "initial_state" => definition.initial_state,
      "target" => definition.target,
      "limits" => definition.limits,
      "runner" => definition.runner,
      "refs" => definition.refs,
      "acl" => definition.acl,
      "partition" => definition.partition
    }
  end

  @spec max_payload_bytes(t()) :: pos_integer()
  def max_payload_bytes(%__MODULE__{limits: limits}),
    do: positive_limit(limits["max_payload_bytes"], @default_max_payload_bytes)

  @spec max_result_bytes(t()) :: pos_integer()
  def max_result_bytes(%__MODULE__{limits: limits}),
    do: positive_limit(limits["max_result_bytes"], @default_max_result_bytes)

  @spec idempotency_required?(t()) :: boolean()
  def idempotency_required?(%__MODULE__{limits: limits}),
    do: Map.get(limits, "idempotency_required", false)

  @spec value_name_allowed?(t(), :read | :write, binary()) :: boolean()
  def value_name_allowed?(%__MODULE__{refs: refs}, action, name) do
    key = if action == :read, do: "allowed_read_names", else: "allowed_write_names"
    allowed_value_name?(Map.get(refs, key, "*"), name)
  end

  @spec from_value_policy(binary(), map()) :: {:ok, t()} | {:error, term()}
  def from_value_policy(name, %{} = policy) do
    policy
    |> Map.take(["limits", "refs"])
    |> Map.put("name", name)
    |> new()
  end

  def from_value_policy(_name, _policy), do: {:error, :invalid_invocation_value_policy}

  defp validate_name(name) when is_binary(name) do
    if Regex.match?(~r/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/, name),
      do: :ok,
      else: {:error, :invalid_invocation_name}
  end

  defp validate_name(_name), do: {:error, :invalid_invocation_name}
  defp validate_boolean(value) when is_boolean(value), do: :ok
  defp validate_boolean(_value), do: {:error, :invalid_invocation_definition}

  defp validate_map_fields(attrs) do
    fields = ~w(target limits runner refs acl partition)

    if Enum.all?(fields, &is_map(Map.get(attrs, &1, %{}))),
      do: :ok,
      else: {:error, :invalid_invocation_definition}
  end

  defp limits(attrs) do
    attrs
    |> stringify_keys()
    |> Map.put_new("max_payload_bytes", @default_max_payload_bytes)
    |> Map.put_new("max_result_bytes", @default_max_result_bytes)
    |> Map.put_new("idempotency_required", false)
  end

  defp positive_limit(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_limit(_value, default), do: default

  defp allowed_value_name?("*", _name), do: true
  defp allowed_value_name?(names, name) when is_list(names), do: name in names
  defp allowed_value_name?(name, name) when is_binary(name), do: true
  defp allowed_value_name?(_allowed, _name), do: false

  defp stringify_keys(%{} = map) do
    Map.new(map, fn {key, value} -> {to_string(key), stringify_value(value)} end)
  end

  defp stringify_keys(_other), do: %{}
  defp stringify_value(%{} = map), do: stringify_keys(map)
  defp stringify_value(values) when is_list(values), do: Enum.map(values, &stringify_value/1)
  defp stringify_value(value), do: value
end
