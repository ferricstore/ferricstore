defmodule Ferricstore.Store.Router.ZSetTreeReads do
  @moduledoc false

  # Reads of tree-encoded sorted sets (`Ferricstore.Store.ZSetTree`, feature
  # level 9): the router's compound and sorted-set reads check the key's
  # promotion marker first and read the set's tree file directly. A commit to
  # the file is atomic, so a read sees one whole state of the set.
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.{Promotion, ReadResult, ZSetTree}

      # The tree file of a tree-encoded sorted set, or nil.
      defp zset_tree_read_path(ctx, redis_key) when is_binary(redis_key) do
        idx = shard_for(ctx, redis_key)
        marker_key = Promotion.marker_key(redis_key)

        marker =
          case :ets.lookup(resolve_keydir(ctx, idx), marker_key) do
            [] -> nil
            [{^marker_key, value, _exp, _lfu, _fid, _off, _vsize}] when is_binary(value) -> value
            # An evicted or expiring marker: the keydir read sorts it out.
            [_entry] -> compound_get_keydir(ctx, redis_key, marker_key)
          end

        case ZSetTree.marker_generation(marker) do
          nil -> nil
          generation -> ZSetTree.path(ctx.data_dir, idx, redis_key, generation)
        end
      rescue
        ArgumentError -> nil
      end

      defp zset_tree_read_path(_ctx, _redis_key), do: nil

      defp zset_tree_member_key?(redis_key, compound_key),
        do: match?({:ok, _member}, ZSetTree.member(redis_key, compound_key))

      # The tree file when some of `compound_keys` are members of a
      # tree-encoded set, nil otherwise.
      defp zset_tree_members_path(ctx, redis_key, compound_keys) do
        if Enum.any?(compound_keys, &zset_tree_member_key?(redis_key, &1)),
          do: zset_tree_read_path(ctx, redis_key)
      end

      defp zset_tree_prefix_path(ctx, redis_key, prefix) do
        if ZSetTree.member_prefix?(redis_key, prefix), do: zset_tree_read_path(ctx, redis_key)
      end

      defp zset_tree_failure(reason), do: ReadResult.failure({:zset_tree_read_failed, reason})

      defp zset_tree_value({:ok, value}), do: value
      defp zset_tree_value({:error, reason}), do: zset_tree_failure(reason)

      defp zset_tree_index_reply({:ok, _value} = ok), do: ok
      defp zset_tree_index_reply({:error, reason}), do: zset_tree_failure(reason)

      # Scores for compound keys, members from the tree and any other key
      # (its type key) from the keydir.
      defp zset_tree_batch_scores(ctx, path, redis_key, compound_keys, keydir_read) do
        members =
          for key <- compound_keys, {:ok, member} <- [ZSetTree.member(redis_key, key)], do: member

        case ZSetTree.scores(path, members) do
          {:ok, scores} ->
            by_member = Map.new(Enum.zip(members, scores))

            Enum.map(compound_keys, fn key ->
              case ZSetTree.member(redis_key, key) do
                {:ok, member} -> {:tree, Map.fetch!(by_member, member)}
                :error -> {:keydir, keydir_read.(key)}
              end
            end)

          {:error, reason} ->
            List.duplicate({:tree_failure, zset_tree_failure(reason)}, length(compound_keys))
        end
      end
    end
  end
end
