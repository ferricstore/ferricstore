defmodule Ferricstore.Transaction.Ast do
  @moduledoc false

  alias Ferricstore.Transaction.ExecutionEntry

  @derivation_namespace <<0, "ferricstore-execution-entry-key", 0>>

  @type queue_entry :: ExecutionEntry.t()

  @non_key_list_tags ~w(
    acl auth client cluster_demote cluster_failover cluster_join cluster_leave cluster_promote command
    config debug hello module sandbox select slowlog
  )a

  @spec normalize_entry(queue_entry()) :: {binary(), [term()], term()}
  def normalize_entry(%ExecutionEntry{command: command, ast: ast}), do: {command, [], ast}

  @spec command_name(queue_entry()) :: binary()
  def command_name(%ExecutionEntry{command: command}), do: command

  @spec command_args(queue_entry()) :: [term()]
  def command_args(%ExecutionEntry{}), do: []

  @spec command_ast(queue_entry()) :: term()
  def command_ast(%ExecutionEntry{ast: ast}), do: ast

  @spec namespace_ast_keys(term(), binary() | nil) :: term()
  def namespace_ast_keys(ast, nil), do: ast
  def namespace_ast_keys(ast, ""), do: ast

  def namespace_ast_keys({:unknown, _cmd, _args} = ast, _ns), do: ast
  def namespace_ast_keys({:object, :help} = ast, _ns), do: ast
  def namespace_ast_keys({:object, {:error, _reason}} = ast, _ns), do: ast

  def namespace_ast_keys({:object, subcmd, key}, ns),
    do: {:object, subcmd, namespace_key(key, ns)}

  def namespace_ast_keys({tag, args} = ast, _ns)
      when tag in @non_key_list_tags and is_list(args),
      do: ast

  def namespace_ast_keys({tag, _subcmd, _args} = ast, _ns)
      when tag in @non_key_list_tags,
      do: ast

  def namespace_ast_keys({tag, keys}, ns)
      when tag in [:del, :unlink, :exists, :mget, :sinter, :sunion, :sdiff, :pfcount] and
             is_list(keys),
      do: {tag, namespace_keys(keys, ns)}

  def namespace_ast_keys({tag, args}, ns) when tag in [:mset, :msetnx] and is_list(args),
    do: {tag, namespace_key_value_args(args, ns)}

  def namespace_ast_keys({tag, keys}, ns)
      when tag in [:sdiffstore, :sinterstore, :sunionstore, :pfmerge] and is_list(keys),
      do: {tag, namespace_keys(keys, ns)}

  def namespace_ast_keys({:sintercard, keys, limit}, ns) when is_list(keys),
    do: {:sintercard, namespace_keys(keys, ns), limit}

  def namespace_ast_keys({:bitop, op, dest, source_keys}, ns) when is_list(source_keys),
    do: {:bitop, op, namespace_key(dest, ns), namespace_keys(source_keys, ns)}

  def namespace_ast_keys({:copy, src, dest, replace}, ns),
    do: {:copy, namespace_key(src, ns), namespace_key(dest, ns), replace}

  def namespace_ast_keys({tag, src, dest}, ns) when tag in [:rename, :renamenx, :rpoplpush],
    do: {tag, namespace_key(src, ns), namespace_key(dest, ns)}

  def namespace_ast_keys({:smove, src, dest, member}, ns),
    do: {:smove, namespace_key(src, ns), namespace_key(dest, ns), member}

  def namespace_ast_keys({tag, src, dest, from_dir, to_dir}, ns) when tag in [:lmove],
    do: {tag, namespace_key(src, ns), namespace_key(dest, ns), from_dir, to_dir}

  def namespace_ast_keys({:blmove, src, dest, from_dir, to_dir, timeout_ms}, ns),
    do: {:blmove, namespace_key(src, ns), namespace_key(dest, ns), from_dir, to_dir, timeout_ms}

  def namespace_ast_keys({tag, keys, timeout_ms}, ns)
      when tag in [:blpop, :brpop] and is_list(keys),
      do: {tag, namespace_keys(keys, ns), timeout_ms}

  def namespace_ast_keys({:blmpop, keys, direction, count, timeout_ms}, ns) when is_list(keys),
    do: {:blmpop, namespace_keys(keys, ns), direction, count, timeout_ms}

  def namespace_ast_keys({tag, dest, source_keys, tail}, ns)
      when tag in [:cms_merge, :tdigest_merge] and is_list(source_keys),
      do: {tag, namespace_key(dest, ns), namespace_keys(source_keys, ns), tail}

  def namespace_ast_keys({:xread, count, block, stream_ids}, ns) when is_list(stream_ids),
    do: {:xread, count, block, namespace_stream_ids(stream_ids, ns)}

  def namespace_ast_keys({:xreadgroup, group, consumer, {count, block, stream_ids}}, ns)
      when is_list(stream_ids),
      do: {:xreadgroup, group, consumer, {count, block, namespace_stream_ids(stream_ids, ns)}}

  def namespace_ast_keys({:geosearchstore, [destination, source | rest]}, ns),
    do: {:geosearchstore, [namespace_key(destination, ns), namespace_key(source, ns) | rest]}

  def namespace_ast_keys({:geosearchstore, destination, source, opts}, ns),
    do: {:geosearchstore, namespace_key(destination, ns), namespace_key(source, ns), opts}

  def namespace_ast_keys({tag, [key | rest]}, ns) when is_atom(tag) and is_binary(key),
    do: {tag, [namespace_key(key, ns) | rest]}

  def namespace_ast_keys({tag, key}, ns) when is_atom(tag) and is_binary(key),
    do: {tag, namespace_key(key, ns)}

  def namespace_ast_keys(ast, ns) when is_tuple(ast) do
    case Tuple.to_list(ast) do
      [tag, key | rest] when is_atom(tag) and is_binary(key) ->
        List.to_tuple([tag, namespace_key(key, ns) | rest])

      _ ->
        ast
    end
  end

  def namespace_ast_keys(ast, _ns), do: ast

  @spec derive_command_keys(term()) :: [binary()]
  def derive_command_keys(ast) do
    ast
    |> namespace_ast_keys(@derivation_namespace)
    |> collect_derived_keys(ast, [])
    |> Enum.reverse()
  end

  defp collect_derived_keys(transformed, original, acc)
       when is_binary(transformed) and is_binary(original) do
    if transformed == @derivation_namespace <> original, do: [original | acc], else: acc
  end

  defp collect_derived_keys(transformed, original, acc)
       when is_tuple(transformed) and is_tuple(original) and
              tuple_size(transformed) == tuple_size(original) do
    collect_derived_tuple_keys(transformed, original, 0, tuple_size(original), acc)
  end

  defp collect_derived_keys([transformed | transformed_rest], [original | original_rest], acc) do
    acc = collect_derived_keys(transformed, original, acc)
    collect_derived_keys(transformed_rest, original_rest, acc)
  end

  defp collect_derived_keys([], [], acc), do: acc
  defp collect_derived_keys(_transformed, _original, acc), do: acc

  defp collect_derived_tuple_keys(_transformed, _original, size, size, acc), do: acc

  defp collect_derived_tuple_keys(transformed, original, index, size, acc) do
    acc = collect_derived_keys(elem(transformed, index), elem(original, index), acc)
    collect_derived_tuple_keys(transformed, original, index + 1, size, acc)
  end

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
