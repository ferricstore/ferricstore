defmodule Ferricstore.Transaction.Ast do
  @moduledoc false

  @type queue_entry :: {binary(), [term()], term()} | {binary(), [term()]}

  @non_key_list_tags ~w(
    acl auth client cluster_demote cluster_failover cluster_join cluster_leave cluster_promote command
    config debug hello module sandbox select slowlog
  )a

  @spec normalize_entry(queue_entry()) :: {binary(), [term()], term()}
  def normalize_entry({cmd, args, ast}) when is_binary(cmd) and is_list(args),
    do: {cmd, args, ast}

  # Pre-AST Ra logs can contain transaction queue entries as `{cmd, args}`.
  # Keep this replay shim compiled in every environment; otherwise a production
  # node upgraded after the AST migration can fail while replaying old entries.
  def normalize_entry({cmd, args}) when is_binary(cmd) and is_list(args) do
    normalized_args = Enum.map(args, &to_command_binary/1)

    case Ferricstore.Commands.Dispatcher.parse_raw(cmd, normalized_args) do
      {:ok, parsed_cmd, parsed_args, ast, _keys} ->
        {parsed_cmd, parsed_args, ast}

      {:error, reason} ->
        raise ArgumentError, "invalid legacy transaction command #{inspect(cmd)}: #{reason}"
    end
  end

  defp to_command_binary(value) when is_binary(value), do: value
  defp to_command_binary(value), do: to_string(value)

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

  def namespace_first_key({:unknown, _cmd, _args} = ast, _ns), do: ast
  def namespace_first_key({:object, :help} = ast, _ns), do: ast
  def namespace_first_key({:object, {:error, _reason}} = ast, _ns), do: ast

  def namespace_first_key({:object, subcmd, key}, ns),
    do: {:object, subcmd, namespace_key(key, ns)}

  def namespace_first_key({tag, args} = ast, _ns)
      when tag in @non_key_list_tags and is_list(args),
      do: ast

  def namespace_first_key({tag, _subcmd, _args} = ast, _ns)
      when tag in @non_key_list_tags,
      do: ast

  def namespace_first_key({tag, keys}, ns)
      when tag in [:del, :unlink, :exists, :mget, :sinter, :sunion, :sdiff, :pfcount] and
             is_list(keys),
      do: {tag, namespace_keys(keys, ns)}

  def namespace_first_key({tag, args}, ns) when tag in [:mset, :msetnx] and is_list(args),
    do: {tag, namespace_key_value_args(args, ns)}

  def namespace_first_key({tag, keys}, ns)
      when tag in [:sdiffstore, :sinterstore, :sunionstore, :pfmerge] and is_list(keys),
      do: {tag, namespace_keys(keys, ns)}

  def namespace_first_key({:sintercard, keys, limit}, ns) when is_list(keys),
    do: {:sintercard, namespace_keys(keys, ns), limit}

  def namespace_first_key({:bitop, op, dest, source_keys}, ns) when is_list(source_keys),
    do: {:bitop, op, namespace_key(dest, ns), namespace_keys(source_keys, ns)}

  def namespace_first_key({:copy, src, dest, replace}, ns),
    do: {:copy, namespace_key(src, ns), namespace_key(dest, ns), replace}

  def namespace_first_key({tag, src, dest}, ns) when tag in [:rename, :renamenx],
    do: {tag, namespace_key(src, ns), namespace_key(dest, ns)}

  def namespace_first_key({tag, src, dest, from_dir, to_dir}, ns) when tag in [:lmove],
    do: {tag, namespace_key(src, ns), namespace_key(dest, ns), from_dir, to_dir}

  def namespace_first_key({:blmove, src, dest, from_dir, to_dir, timeout_ms}, ns),
    do: {:blmove, namespace_key(src, ns), namespace_key(dest, ns), from_dir, to_dir, timeout_ms}

  def namespace_first_key({tag, keys, timeout_ms}, ns)
      when tag in [:blpop, :brpop] and is_list(keys),
      do: {tag, namespace_keys(keys, ns), timeout_ms}

  def namespace_first_key({:blmpop, keys, direction, count, timeout_ms}, ns) when is_list(keys),
    do: {:blmpop, namespace_keys(keys, ns), direction, count, timeout_ms}

  def namespace_first_key({tag, dest, source_keys, tail}, ns)
      when tag in [:cms_merge, :tdigest_merge] and is_list(source_keys),
      do: {tag, namespace_key(dest, ns), namespace_keys(source_keys, ns), tail}

  def namespace_first_key({:xread, count, block, stream_ids}, ns) when is_list(stream_ids),
    do: {:xread, count, block, namespace_stream_ids(stream_ids, ns)}

  def namespace_first_key({:xreadgroup, group, consumer, {count, block, stream_ids}}, ns)
      when is_list(stream_ids),
      do: {:xreadgroup, group, consumer, {count, block, namespace_stream_ids(stream_ids, ns)}}

  def namespace_first_key({tag, [key | rest]}, ns) when is_atom(tag) and is_binary(key),
    do: {tag, [namespace_key(key, ns) | rest]}

  def namespace_first_key({tag, key}, ns) when is_atom(tag) and is_binary(key),
    do: {tag, namespace_key(key, ns)}

  def namespace_first_key(ast, ns) when is_tuple(ast) do
    case Tuple.to_list(ast) do
      [tag, key | rest] when is_atom(tag) and is_binary(key) ->
        List.to_tuple([tag, namespace_key(key, ns) | rest])

      _ ->
        ast
    end
  end

  def namespace_first_key(ast, _ns), do: ast

  defp namespace_key(key, ns) when is_binary(key), do: ns <> key
  defp namespace_key(key, _ns), do: key

  defp namespace_keys(keys, ns), do: Enum.map(keys, &namespace_key(&1, ns))

  defp namespace_key_value_args(args, ns) do
    args
    |> Enum.chunk_every(2)
    |> Enum.flat_map(fn
      [key, value] -> [namespace_key(key, ns), value]
      [key] -> [namespace_key(key, ns)]
    end)
  end

  defp namespace_stream_ids(stream_ids, ns) do
    Enum.map(stream_ids, fn
      {key, id} -> {namespace_key(key, ns), id}
      other -> other
    end)
  end
end
