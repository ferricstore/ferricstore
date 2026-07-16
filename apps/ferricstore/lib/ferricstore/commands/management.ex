defmodule Ferricstore.Commands.Management do
  @moduledoc """
  Stable management command adapter.

  These commands are intentionally narrow public command shapes for SDK
  `COMMAND_EXEC`.
  """

  @telemetry_raw_pair_fields MapSet.new(
                               ~w(before_id partition partition_key namespace prefix scope type state event worker)
                             )

  @spec handle(binary(), [binary()], term()) :: term()
  def handle("FERRICSTORE.CAPABILITIES", [], _store) do
    FerricStore.ManagementCapabilities.capabilities()
    |> normalize_value()
  end

  def handle("FERRICSTORE.CAPABILITIES", _args, _store),
    do: wrong_arity("ferricstore.capabilities")

  def handle("ACL", ["SETUSER", username | rules], store),
    do: result(FerricStore.Management.ACL.set_user(username, rules, store_opts(store)))

  def handle("ACL", ["SETUSER"], _store), do: wrong_arity("acl setuser")

  def handle("ACL", ["DELUSER" | usernames], store) when usernames != [],
    do: result(FerricStore.Management.ACL.del_users(usernames, store_opts(store)))

  def handle("ACL", ["DELUSER"], _store), do: wrong_arity("acl deluser")

  def handle("ACL", ["GETUSER", username], store),
    do: result(FerricStore.Management.ACL.get_user(username, store_opts(store)))

  def handle("ACL", ["GETUSER" | _], _store), do: wrong_arity("acl getuser")

  def handle("ACL", ["LIST"], store),
    do: result(FerricStore.Management.ACL.list_users(store_opts(store)))

  def handle("ACL", ["LIST" | _], _store), do: wrong_arity("acl list")

  def handle("ACL", ["SAVE"], store),
    do: result(FerricStore.Management.ACL.save(store_opts(store)))

  def handle("ACL", ["SAVE" | _], _store), do: wrong_arity("acl save")

  def handle("ACL", ["LOAD"], store),
    do: result(FerricStore.Management.ACL.load(store_opts(store)))

  def handle("ACL", ["LOAD" | _], _store), do: wrong_arity("acl load")

  def handle("ACL", [subcmd | _args], _store),
    do: {:error, "ERR unknown ACL subcommand '#{String.downcase(subcmd)}'"}

  def handle("ACL", [], _store), do: wrong_arity("acl")

  def handle("FERRICSTORE.NAMESPACE", ["ENSURE", prefix | rest], store) do
    with {:ok, attrs} <- pair_map(rest) do
      opts = Keyword.put(store_opts(store), :attrs, attrs)
      result(FerricStore.Management.Namespace.ensure_namespace(prefix, opts))
    end
  end

  def handle("FERRICSTORE.NAMESPACE", ["GET", prefix], _store),
    do: result(FerricStore.Management.Namespace.get_namespace(prefix))

  def handle("FERRICSTORE.NAMESPACE", ["LIST"], _store),
    do: result(FerricStore.Management.Namespace.list_namespaces())

  def handle("FERRICSTORE.NAMESPACE", ["DELETE", prefix], store),
    do: result(FerricStore.Management.Namespace.delete_namespace(prefix, store_opts(store)))

  def handle("FERRICSTORE.NAMESPACE", [subcmd | _args], _store),
    do: {:error, "ERR unknown FERRICSTORE.NAMESPACE subcommand '#{String.downcase(subcmd)}'"}

  def handle("FERRICSTORE.NAMESPACE", [], _store), do: wrong_arity("ferricstore.namespace")

  def handle("FERRICSTORE.QUOTA", ["SET", namespace | rest], store) do
    with {:ok, limit_spec} <- pair_map(rest) do
      result(FerricStore.ResourceLimits.set_limit(namespace, limit_spec, store_opts(store)))
    end
  end

  def handle("FERRICSTORE.QUOTA", ["GET", namespace], store),
    do: result(FerricStore.ResourceLimits.get_limit(namespace, store_opts(store)))

  def handle("FERRICSTORE.QUOTA", ["USAGE", namespace], store),
    do: result(FerricStore.ResourceLimits.usage(namespace, store_opts(store)))

  def handle("FERRICSTORE.QUOTA", [subcmd | _args], _store),
    do: {:error, "ERR unknown FERRICSTORE.QUOTA subcommand '#{String.downcase(subcmd)}'"}

  def handle("FERRICSTORE.QUOTA", [], _store), do: wrong_arity("ferricstore.quota")

  def handle("FERRICSTORE.TELEMETRY", ["CLUSTER_INFO"], store),
    do: result(FerricStore.Management.Telemetry.cluster_info(store_opts(store)))

  def handle("FERRICSTORE.TELEMETRY", ["NAMESPACE_USAGE", prefix], store),
    do: result(FerricStore.Management.Telemetry.namespace_usage(prefix, store_opts(store)))

  def handle("FERRICSTORE.TELEMETRY", ["FLOW_QUERY" | rest], store) do
    with {:ok, attrs} <- pair_map(rest, @telemetry_raw_pair_fields) do
      result(FerricStore.Management.Telemetry.flow_query(attrs, store_opts(store)))
    end
  end

  def handle("FERRICSTORE.TELEMETRY", ["FLOW_HISTORY", id | rest], store) do
    with {:ok, attrs} <- pair_map(rest, @telemetry_raw_pair_fields) do
      opts = Keyword.put(store_opts(store), :attrs, attrs)
      result(FerricStore.Management.Telemetry.flow_history(id, opts))
    end
  end

  def handle("FERRICSTORE.TELEMETRY", [subcmd | _args], _store),
    do: {:error, "ERR unknown FERRICSTORE.TELEMETRY subcommand '#{String.downcase(subcmd)}'"}

  def handle("FERRICSTORE.TELEMETRY", [], _store), do: wrong_arity("ferricstore.telemetry")

  defp result(:ok), do: :ok
  defp result({:ok, :ok}), do: :ok
  defp result({:ok, value}), do: normalize_value(value)
  defp result({:error, :unsupported}), do: {:error, "ERR unsupported management command"}
  defp result({:error, reason}) when is_binary(reason), do: {:error, normalize_error(reason)}
  defp result({:error, reason}) when is_atom(reason), do: {:error, "ERR #{reason}"}
  defp result({:error, reason}), do: {:error, "ERR #{inspect(reason)}"}
  defp result(value), do: normalize_value(value)

  defp pair_map(args), do: pair_map(args, MapSet.new())

  defp pair_map([], _raw_fields), do: {:ok, %{}}

  defp pair_map(args, raw_fields) when is_list(args) and rem(length(args), 2) == 0 do
    args
    |> Enum.chunk_every(2)
    |> Map.new(fn [key, value] ->
      key = String.downcase(key)
      {key, parse_pair_value(key, value, raw_fields)}
    end)
    |> then(&{:ok, &1})
  end

  defp pair_map(_args, _raw_fields), do: {:error, "ERR syntax error"}

  # Cursor identifiers are opaque bytes. Parsing numeric- or boolean-looking
  # IDs would change their ordering and make pagination skip or repeat rows.
  defp parse_pair_value(key, value, raw_fields) do
    if MapSet.member?(raw_fields, key), do: value, else: parse_value(value)
  end

  defp parse_value(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> parse_boolean(value)
    end
  end

  defp parse_value(value), do: value

  defp parse_boolean(value) do
    case String.downcase(value) do
      "true" -> true
      "false" -> false
      _ -> value
    end
  end

  defp normalize_value(%_{} = struct), do: struct |> Map.from_struct() |> normalize_value()

  defp normalize_value(value) when is_boolean(value) or is_nil(value), do: value

  defp normalize_value(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {normalize_key(key), normalize_value(value)} end)
  end

  defp normalize_value(values) when is_list(values), do: Enum.map(values, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp normalize_key(key) when is_binary(key), do: key
  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: to_string(key)

  defp normalize_error("ERR " <> _ = reason), do: reason
  defp normalize_error(reason), do: "ERR #{reason}"

  defp store_opts(store), do: [store: store]

  defp wrong_arity(command),
    do: {:error, "ERR wrong number of arguments for '#{command}' command"}
end
