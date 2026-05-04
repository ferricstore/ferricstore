defmodule Ferricstore.Transaction.Ast do
  @moduledoc false

  @type queue_entry :: {binary(), [term()]} | {binary(), [term()], term()}

  @spec normalize_entry(queue_entry()) :: {binary(), [term()], term()}
  def normalize_entry({cmd, args, ast}) when is_binary(cmd) and is_list(args),
    do: {cmd, args, ast}

  def normalize_entry({cmd, args}) when is_binary(cmd) and is_list(args) do
    normalized = normalize_command_name(cmd)
    {normalized, args, legacy_ast(normalized, args)}
  end

  @spec command_name(queue_entry()) :: binary()
  def command_name(entry) do
    {cmd, _args, _ast} = normalize_entry(entry)
    cmd
  end

  @spec command_args(queue_entry()) :: [term()]
  def command_args(entry) do
    {_cmd, args, _ast} = normalize_entry(entry)
    args
  end

  @spec command_ast(queue_entry()) :: term()
  def command_ast(entry) do
    {_cmd, _args, ast} = normalize_entry(entry)
    ast
  end

  @spec namespace_first_key(term(), binary() | nil) :: term()
  def namespace_first_key(ast, nil), do: ast
  def namespace_first_key(ast, ""), do: ast

  def namespace_first_key({tag, [key | rest]}, ns) when is_atom(tag) and is_binary(key),
    do: {tag, [ns <> key | rest]}

  def namespace_first_key({tag, key}, ns) when is_atom(tag) and is_binary(key),
    do: {tag, ns <> key}

  def namespace_first_key(ast, ns) when is_tuple(ast) do
    case Tuple.to_list(ast) do
      [tag, key | rest] when is_atom(tag) and is_binary(key) ->
        List.to_tuple([tag, ns <> key | rest])

      _ ->
        ast
    end
  end

  def namespace_first_key(ast, _ns), do: ast

  defp normalize_command_name(cmd) do
    if cmd == String.upcase(cmd), do: cmd, else: String.upcase(cmd)
  end

  defp legacy_ast("SET", [key, value]), do: {:set, key, value}
  defp legacy_ast("SET", [key, value | opts]), do: {:set, key, value, parse_set_options(opts)}
  defp legacy_ast("GET", [key]), do: {:get, key}
  defp legacy_ast("DEL", keys), do: {:del, keys}
  defp legacy_ast("INCR", [key]), do: {:incr, key}
  defp legacy_ast("PING", []), do: :ping
  defp legacy_ast("PING", args), do: {:ping, args}
  defp legacy_ast("HSET", args), do: {:hset, args}
  defp legacy_ast("HGET", [key, field]), do: {:hget, key, field}
  defp legacy_ast("HGETALL", [key]), do: {:hgetall, key}
  defp legacy_ast(cmd, args), do: {:raw, cmd, args}

  defp parse_set_options(opts), do: parse_set_options(opts, [], false)

  defp parse_set_options([], acc, _has_expiry) do
    cond do
      :nx in acc and :xx in acc ->
        {:error, "ERR XX and NX options at the same time are not compatible"}

      true ->
        Enum.reverse(acc)
    end
  end

  defp parse_set_options([opt | rest], acc, has_expiry) when is_binary(opt) do
    case String.upcase(opt) do
      "NX" ->
        parse_set_options(rest, [:nx | acc], has_expiry)

      "XX" ->
        parse_set_options(rest, [:xx | acc], has_expiry)

      "GET" ->
        parse_set_options(rest, [:get | acc], has_expiry)

      "KEEPTTL" ->
        if has_expiry do
          {:error, "ERR syntax error"}
        else
          parse_set_options(rest, [:keepttl | acc], true)
        end

      "EX" ->
        parse_set_expiry(rest, acc, has_expiry, :ex, 1_000)

      "PX" ->
        parse_set_expiry(rest, acc, has_expiry, :px, 1)

      "EXAT" ->
        parse_set_expiry(rest, acc, has_expiry, :exat, 1)

      "PXAT" ->
        parse_set_expiry(rest, acc, has_expiry, :pxat, 1)

      _ ->
        {:error, "ERR syntax error, option '#{opt}' not recognized"}
    end
  end

  defp parse_set_options(_opts, _acc, _has_expiry), do: {:error, "ERR syntax error"}

  defp parse_set_expiry([value | rest], acc, false, tag, _unit) when is_binary(value) do
    with {parsed, ""} <- Integer.parse(value),
         true <- parsed > 0 do
      parse_set_options(rest, [{tag, parsed} | acc], true)
    else
      false -> {:error, "ERR invalid expire time in 'set' command"}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_set_expiry([_value | _rest], _acc, true, _tag, _unit),
    do: {:error, "ERR syntax error"}

  defp parse_set_expiry(_rest, _acc, _has_expiry, _tag, _unit),
    do: {:error, "ERR syntax error"}
end
