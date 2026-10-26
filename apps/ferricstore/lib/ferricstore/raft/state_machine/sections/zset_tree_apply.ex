defmodule Ferricstore.Raft.StateMachine.Sections.ZSetTreeApply do
  @moduledoc false

  # Sorted sets in their counted tree file (`Ferricstore.Store.ZSetTree`), from
  # cluster feature level 9. The compound primitives hand a tree-encoded set's
  # member keys here; its type key and promotion marker stay in the keydir.
  #
  # Every write carries the entry's Raft position, so replaying an entry cuts
  # the file back to before it and applies again. A set converts in the entry
  # whose write takes it past the promotion threshold; the marker's generation
  # comes from that entry's position, so every replica and every replay name
  # the same file.

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]

      alias Ferricstore.Raft.PromotedFileSync
      alias Ferricstore.Store.{CompoundKey, Promotion, ZSetTree}

      # Ordinals of one entry fit below this; a generation is index * 2^20 +
      # ordinal, unique for each conversion even within one entry.
      @zset_tree_ordinal_limit 1_048_576

      # The Raft position of the entry being applied, for tree writes: its index
      # and an ordinal counting the tree writes within the entry.
      defp with_zset_tree_position(%{index: index}, fun) when is_integer(index) and index > 0 do
        previous = {Process.get(:sm_zset_tree_index), Process.get(:sm_zset_tree_ordinal)}
        Process.put(:sm_zset_tree_index, index)
        Process.put(:sm_zset_tree_ordinal, 0)

        try do
          fun.()
        after
          case previous do
            {nil, _ordinal} ->
              Process.delete(:sm_zset_tree_index)
              Process.delete(:sm_zset_tree_ordinal)

            {previous_index, previous_ordinal} ->
              Process.put(:sm_zset_tree_index, previous_index)
              Process.put(:sm_zset_tree_ordinal, previous_ordinal)
          end
        end
      end

      defp with_zset_tree_position(_meta, fun), do: fun.()

      # A tree write needs the entry's Raft position, so a replay can rewind to
      # it. Outside a WARaft apply there is none, and the write fails.
      defp next_zset_tree_token do
        case Process.get(:sm_zset_tree_index) do
          index when is_integer(index) and index > 0 ->
            ordinal = Process.get(:sm_zset_tree_ordinal, 0) + 1
            Process.put(:sm_zset_tree_ordinal, ordinal)
            {:ok, index, ordinal}

          _outside_raft_apply ->
            {:error, :zset_tree_write_outside_raft_apply}
        end
      end

      defp feature_level_9?(state),
        do:
          do_get(state, Ferricstore.Cluster.FeatureLevel.key(9)) ==
            Ferricstore.Cluster.FeatureLevel.marker()

      # {:tree, path, member_keys, other_keys} when `redis_key` is tree-encoded
      # and some of `compound_keys` are its members; :keydir otherwise.
      defp zset_tree_route(state, redis_key, compound_keys) when is_binary(redis_key) do
        {members, others} =
          Enum.split_with(compound_keys, &match?({:ok, _member}, ZSetTree.member(redis_key, &1)))

        with [_ | _] <- members,
             path when is_binary(path) <- zset_tree_path(state, redis_key) do
          {:tree, path, members, others}
        else
          _keydir -> :keydir
        end
      end

      defp zset_tree_route(_state, _redis_key, _compound_keys), do: :keydir

      # Member writes of a tree-encoded set, after converting the set when these
      # writes take it past the threshold: {:tree, path, member_keys, other_keys},
      # :keydir, or an error.
      defp zset_tree_write_split(state, redis_key, compound_keys) do
        case zset_tree_route(state, redis_key, compound_keys) do
          {:tree, _path, _members, _others} = route ->
            route

          :keydir ->
            case maybe_convert_zset_to_tree(state, redis_key, compound_keys) do
              {:tree, path} ->
                {members, others} =
                  Enum.split_with(
                    compound_keys,
                    &match?({:ok, _member}, ZSetTree.member(redis_key, &1))
                  )

                {:tree, path, members, others}

              other ->
                other
            end
        end
      end

      defp zset_tree_write(_state, path, ops) do
        with {:ok, index, ordinal} <- next_zset_tree_token(),
             {:ok, outcomes} <-
               PromotedFileSync.track_write(path, fn ->
                 ZSetTree.apply_ops(path, {index, ordinal}, ops)
               end) do
          {:ok, outcomes}
        else
          {:error, reason} ->
            record_state_write_failure({:zset_tree_write_failed, reason})
            {:error, {:zset_tree_write_failed, reason}}
        end
      end

      defp zset_tree_put_entries(state, redis_key, path, entries) do
        with {:ok, ops} <- ZSetTree.put_ops(redis_key, entries),
             {:ok, _outcomes} <- zset_tree_write(state, path, ops) do
          :ok
        end
      end

      defp zset_tree_delete_keys(state, redis_key, path, compound_keys) do
        with {:ok, ops} <- ZSetTree.remove_ops(redis_key, compound_keys),
             {:ok, _outcomes} <- zset_tree_write(state, path, ops) do
          :ok
        end
      end

      # DEL of a tree-encoded set. Its marker goes in this entry; the file stays,
      # with this entry's index beside it, until the durable position passes the
      # entry: a replay of an earlier entry still reads it.
      defp zset_tree_delete_set(state, redis_key, path) do
        with :ok <- do_delete_flushed_promotion_marker(state, Promotion.marker_key(redis_key)) do
          record_zset_tree_removal(path)
          zset_index_clear(state, redis_key)
          :ok
        end
      end

      defp record_zset_tree_removal(path) do
        index = current_ra_index() || 0

        case Ferricstore.FS.atomic_replace_nofollow(
               ZSetTree.removed_path(path),
               Integer.to_string(index),
               64
             ) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.warning(
              "Sorted set tree #{path} lost its removal record; its file waits for the next sweep: " <>
                inspect(reason)
            )
        end
      end

      defp zset_tree_member_count(state, redis_key) do
        case zset_tree_path(state, redis_key) do
          nil -> nil
          path -> ZSetTree.card(path)
        end
      end

      # Before member writes to a set still in the keydir: from level 9, a write
      # that would take the set past the promotion threshold first moves the set
      # into its tree, then goes to the tree. Returns {:tree, path} when the
      # writes now belong in the tree, :keydir otherwise.
      defp maybe_convert_zset_to_tree(state, redis_key, compound_keys) do
        threshold = Map.fetch!(state, :apply_context).promotion_threshold
        prefix = CompoundKey.zset_prefix(redis_key)

        new_members =
          for key <- compound_keys, {:ok, member} <- [ZSetTree.member(redis_key, key)], do: member

        with true <- is_integer(threshold) and threshold > 0,
             [_ | _] <- new_members,
             true <- feature_level_9?(state),
             nil <- zset_tree_path(state, redis_key),
             nil <- promoted_compound_path(state, redis_key, prefix),
             {:ok, members} <- zset_members_with_pending(state, prefix),
             true <- map_size(Map.merge(members, Map.new(new_members, &{&1, nil}))) > threshold do
          convert_zset_to_tree(state, redis_key, prefix, members)
        else
          {:error, _reason} = error -> error
          _not_converting -> :keydir
        end
      end

      # The set's members as this entry sees them: the keydir's, with the writes
      # this entry has staged but not yet published laid over them.
      defp zset_members_with_pending(state, prefix) do
        case Ferricstore.Store.Shard.ETS.prefix_scan_entries(
               shard_ets_state(state),
               prefix,
               state.shard_data_path
             ) do
          entries when is_list(entries) ->
            prefix_size = byte_size(prefix)

            members =
              Process.get(:sm_pending_values, %{})
              |> Enum.reduce(Map.new(entries), fn
                {<<^prefix::binary-size(^prefix_size), member::binary>>, :deleted}, acc ->
                  Map.delete(acc, member)

                {<<^prefix::binary-size(^prefix_size), member::binary>>, {value, _expire_at_ms}},
                acc
                when is_binary(value) ->
                  Map.put(acc, member, value)

                _other, acc ->
                  acc
              end)

            {:ok, members}

          {:error, _reason} = error ->
            error
        end
      end

      defp convert_zset_to_tree(state, redis_key, prefix, members) do
        entries = Enum.sort(members)

        with {:ok, index, ordinal} <- zset_tree_conversion_token(),
             :ok <- zset_tree_generation_available(index, ordinal),
             {:ok, ops} <- zset_tree_conversion_ops(entries) do
          generation = index * @zset_tree_ordinal_limit + ordinal
          path = ZSetTree.path(promoted_data_dir(state), state.shard_index, redis_key, generation)

          with {:ok, _outcomes} <-
                 PromotedFileSync.track_write(path, fn ->
                   ZSetTree.apply_ops(path, {index, ordinal}, ops)
                 end),
               # Readers check the marker first: once it names the tree, the
               # members in the keydir are no longer read.
               :ok <-
                 do_put_value_validated(
                   state,
                   Promotion.marker_key(redis_key),
                   ZSetTree.marker_value(generation),
                   0
                 ),
               :ok <- zset_tree_drop_keydir_members(state, redis_key, prefix, entries) do
            zset_index_clear(state, redis_key)
            {:tree, path}
          else
            {:error, reason} ->
              record_state_write_failure({:zset_tree_conversion_failed, reason})
              {:error, {:zset_tree_conversion_failed, reason}}
          end
        else
          # Without a Raft position (a local instance) the set stays in the keydir.
          :no_generation -> :keydir
          {:error, _reason} = error -> error
        end
      end

      # Outside a WARaft apply a set stays in the keydir.
      defp zset_tree_conversion_token do
        case next_zset_tree_token() do
          {:ok, _index, _ordinal} = token -> token
          {:error, :zset_tree_write_outside_raft_apply} -> :no_generation
        end
      end

      defp zset_tree_drop_keydir_members(_state, _redis_key, _prefix, []), do: :ok

      defp zset_tree_drop_keydir_members(state, redis_key, prefix, entries),
        do:
          do_shared_compound_batch_delete(
            state,
            redis_key,
            Enum.map(entries, fn {member, _score} -> prefix <> member end)
          )

      defp zset_tree_generation_available(index, ordinal)
           when is_integer(index) and index > 0 and is_integer(ordinal) and
                  ordinal < @zset_tree_ordinal_limit,
           do: :ok

      defp zset_tree_generation_available(_index, _ordinal), do: :no_generation

      defp zset_tree_conversion_ops(entries) do
        Enum.reduce_while(entries, {:ok, []}, fn {member, value}, {:ok, ops} ->
          case ZSetTree.parse_score(value) do
            {:ok, score} -> {:cont, {:ok, [{:add, member, score, 0} | ops]}}
            :error -> {:halt, {:error, {:invalid_zset_member_score, member}}}
          end
        end)
        |> case do
          {:ok, ops} -> {:ok, Enum.reverse(ops)}
          error -> error
        end
      end

      # --- Reads, for commands run inside apply ---

      defp zset_tree_scan_page(path, cursor, count, match_pattern, fields_only) do
        with {:ok, page} <- ZSetTree.scan_page(path, cursor, count, match_pattern, fields_only) do
          {:ok, {:ok, page}}
        end
      end

      defp zset_tree_store_fields(state) do
        %{
          zset_score_range: fn redis_key, min_bound, max_bound, reverse? ->
            zset_tree_read(state, redis_key, fn path ->
              ZSetTree.score_range_slice(path, min_bound, max_bound, reverse?, 0, :all)
            end)
          end,
          zset_score_range_slice: fn redis_key, min_bound, max_bound, reverse?, offset, count ->
            zset_tree_read(state, redis_key, fn path ->
              ZSetTree.score_range_slice(path, min_bound, max_bound, reverse?, offset, count)
            end)
          end,
          zset_score_count: fn redis_key, min_bound, max_bound ->
            zset_tree_read(state, redis_key, &ZSetTree.score_count(&1, min_bound, max_bound))
          end,
          zset_rank_range: fn redis_key, start_idx, stop_idx, reverse? ->
            zset_tree_read(
              state,
              redis_key,
              &ZSetTree.rank_range(&1, start_idx, stop_idx, reverse?)
            )
          end,
          zset_member_rank: fn redis_key, member, reverse? ->
            zset_tree_read(state, redis_key, &ZSetTree.member_rank(&1, member, reverse?))
          end
        }
      end

      # A tree-encoded set answers from its tree; any other key answers
      # :unavailable, so the caller scans the keydir as before.
      defp zset_tree_read(state, redis_key, read) do
        case zset_tree_path(state, redis_key) do
          nil ->
            :unavailable

          path ->
            case read.(path) do
              {:ok, _value} = ok ->
                ok

              {:error, reason} ->
                Ferricstore.Store.ReadResult.failure({:zset_tree_read_failed, reason})
            end
        end
      end
    end
  end
end
