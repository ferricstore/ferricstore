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
        keys = [:sm_zset_tree_index, :sm_zset_tree_ordinal, :sm_zset_tree_prepared]
        previous = Enum.map(keys, &{&1, Process.get(&1)})
        Process.put(:sm_zset_tree_index, index)
        Process.put(:sm_zset_tree_ordinal, 0)
        Process.put(:sm_zset_tree_prepared, MapSet.new())

        try do
          fun.()
        after
          Enum.each(previous, fn
            {key, nil} -> Process.delete(key)
            {key, value} -> Process.put(key, value)
          end)
        end
      end

      defp with_zset_tree_position(_meta, fun), do: fun.()

      # A replayed entry must read a set as it was when the entry first applied,
      # but the file still holds the frames the run before a restart wrote for
      # this entry and later ones. So the first time an entry touches a tree,
      # those frames are cut off; in a run without a restart there are none.
      defp prepare_zset_tree_for_entry(path) do
        with index when is_integer(index) and index > 0 <- Process.get(:sm_zset_tree_index),
             prepared = Process.get(:sm_zset_tree_prepared, MapSet.new()),
             false <- MapSet.member?(prepared, path) do
          Process.put(:sm_zset_tree_prepared, MapSet.put(prepared, path))

          case Ferricstore.Bitcask.NIF.zset_tree_rewind(path, index, 1) do
            :ok ->
              :ok

            {:error, reason} ->
              # The entry cannot see the set as it first did: stop rather than
              # apply it to a later state.
              raise "sorted set tree #{path} could not be cut back for entry #{index}: " <>
                      inspect(reason)
          end
        end

        path
      end

      # A file this entry created is already as the entry left it.
      defp mark_zset_tree_prepared(path) do
        Process.put(
          :sm_zset_tree_prepared,
          MapSet.put(Process.get(:sm_zset_tree_prepared, MapSet.new()), path)
        )
      end

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
      defp zset_tree_write_split(state, redis_key, compound_keys, staging \\ nil) do
        case zset_tree_route(state, redis_key, compound_keys) do
          {:tree, _path, _members, _others} = route ->
            route

          :keydir ->
            case maybe_convert_zset_to_tree(state, redis_key, compound_keys, staging) do
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
             :ok <- record_zset_tree_tx_write(path, {index, ordinal}),
             {:ok, outcomes} <-
               PromotedFileSync.track_write(path, fn ->
                 ZSetTree.apply_ops(path, {index, ordinal}, ops)
               end) do
          note_zset_tree_written(path)
          {:ok, outcomes}
        else
          {:error, reason} ->
            record_state_write_failure({:zset_tree_write_failed, reason})
            {:error, {:zset_tree_write_failed, reason}}
        end
      end

      # The storage process applies every entry, and its maintenance pass looks
      # at the trees written since the last pass for ones worth compacting.
      defp note_zset_tree_written(path) do
        Process.put(
          :sm_zset_tree_written,
          MapSet.put(Process.get(:sm_zset_tree_written, MapSet.new()), path)
        )
      end

      # Inside MULTI a tree write is applied at once; a transaction that fails
      # cuts each tree it wrote back to before its first write.
      defp record_zset_tree_tx_write(path, token) do
        case Process.get(:sm_zset_tree_tx_writes) do
          writes when is_map(writes) ->
            Process.put(:sm_zset_tree_tx_writes, Map.put_new(writes, path, token))
            :ok

          _not_in_transaction ->
            :ok
        end
      end

      defp begin_zset_tree_tx_writes, do: Process.put(:sm_zset_tree_tx_writes, %{})
      defp end_zset_tree_tx_writes, do: Process.delete(:sm_zset_tree_tx_writes)

      defp rollback_zset_tree_tx_writes do
        writes = Process.get(:sm_zset_tree_tx_writes, %{})
        Process.put(:sm_zset_tree_tx_writes, %{})

        Enum.each(writes, fn {path, {index, ordinal}} ->
          case Ferricstore.Bitcask.NIF.zset_tree_rewind(path, index, ordinal) do
            :ok ->
              :ok

            {:error, reason} ->
              raise "sorted set tree #{path} could not undo a failed transaction: #{inspect(reason)}"
          end
        end)
      end

      # A member write that leaves its score as it is (EXPIRE rewriting the set,
      # say: a tree member has no TTL of its own) changes nothing, and the tree
      # writes no frame for it.
      defp zset_tree_put_entries(state, redis_key, path, entries) do
        with {:ok, ops} <- ZSetTree.put_ops(redis_key, entries),
             {:ok, _outcomes} <- zset_tree_write(state, path, ops) do
          :ok
        end
      end

      # A tree set lives while its type key does, and its marker takes the type
      # key's TTL: when the set expires, its marker goes with it, and a later
      # write starts a new set.
      defp zset_tree_follow_type_ttl(state, redis_key, entries, staging \\ nil) do
        type_key = CompoundKey.type_key(redis_key)
        marker_key = Promotion.marker_key(redis_key)

        with {^type_key, _type, expire_at_ms} <- List.keyfind(entries, type_key, 0),
             marker when is_binary(marker) <- zset_tree_staged_read(state, marker_key),
             true <- ZSetTree.marker?(marker) do
          staging =
            staging ||
              zset_tree_apply_staging(state, redis_key, CompoundKey.zset_prefix(redis_key))

          staging.put_marker.(marker_key, marker, expire_at_ms)
        else
          _no_tree_type_write -> :ok
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
      defp maybe_convert_zset_to_tree(state, redis_key, compound_keys, staging) do
        threshold = Map.fetch!(state, :apply_context).promotion_threshold
        prefix = CompoundKey.zset_prefix(redis_key)

        new_members =
          for key <- compound_keys, {:ok, member} <- [ZSetTree.member(redis_key, key)], do: member

        # A tree write carries its Raft position; with none, the set stays put.
        # A set promoted to a dedicated Bitcask before level 9 converts on its
        # first write, whatever its size.
        with true <- is_integer(threshold) and threshold > 0,
             [_ | _] <- new_members,
             true <- is_integer(Process.get(:sm_zset_tree_index)),
             true <- feature_level_9?(state),
             nil <- zset_tree_path(state, redis_key),
             legacy = promoted_compound_path(state, redis_key, prefix),
             {:ok, legacy_generation} <- legacy_zset_generation(state, redis_key, legacy),
             {:ok, members} <- zset_members_with_pending(state, prefix, legacy),
             true <-
               legacy != nil or
                 map_size(Map.merge(members, Map.new(new_members, &{&1, nil}))) > threshold,
             {:tree, _path} = converted <-
               convert_zset_to_tree(
                 state,
                 redis_key,
                 prefix,
                 members,
                 staging || zset_tree_apply_staging(state, redis_key, prefix)
               ) do
          retire_legacy_zset(redis_key, legacy, legacy_generation)
          converted
        else
          {:error, _reason} = error -> error
          _not_converting -> :keydir
        end
      end

      defp legacy_zset_generation(_state, _redis_key, nil), do: {:ok, nil}

      defp legacy_zset_generation(state, redis_key, _dedicated_path) do
        case promotion_marker_generation_for_delete(state, Promotion.marker_key(redis_key)) do
          generation when is_integer(generation) -> {:ok, generation}
          other -> {:error, {:legacy_zset_marker_unavailable, other}}
        end
      end

      # The dedicated Bitcask goes once the entry commits: the post-commit
      # cleanup finds the set's marker names a tree and removes the directory.
      defp retire_legacy_zset(_redis_key, nil, _generation), do: :ok

      defp retire_legacy_zset(redis_key, dedicated_path, generation) do
        :ok =
          queue_promoted_storage_cleanup_after_flush(redis_key, :zset, dedicated_path, generation)

        record_promoted_instance_removal(redis_key)
        :ok
      end

      # The set's members as this entry sees them: the keydir's (values read
      # from the dedicated Bitcask for a promoted set), with the writes this
      # entry has staged but not yet published laid over them.
      defp zset_members_with_pending(state, prefix, dedicated_path) do
        case Ferricstore.Store.Shard.ETS.prefix_scan_entries(
               shard_ets_state(state),
               prefix,
               dedicated_path || state.shard_data_path
             ) do
          entries when is_list(entries) ->
            prefix_size = byte_size(prefix)

            members =
              Process.get(:sm_pending_values, %{})
              |> Map.merge(Process.get(:tx_pending_values, %{}))
              |> Map.merge(Map.new(Process.get(:tx_deleted_keys, MapSet.new()), &{&1, :deleted}))
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

      # How a conversion stages its keydir changes: through the apply primitives,
      # or MULTI's own staging, so they publish after its earlier writes.
      defp zset_tree_apply_staging(state, redis_key, prefix) do
        %{
          put_marker: fn key, value, expire_at_ms ->
            do_put_value_validated(state, key, value, expire_at_ms)
          end,
          drop_members: fn entries ->
            zset_tree_drop_keydir_members(state, redis_key, prefix, entries)
          end
        }
      end

      defp convert_zset_to_tree(state, redis_key, prefix, members, staging) do
        entries = Enum.sort(members)

        with {:ok, index, ordinal} <- zset_tree_conversion_token(),
             :ok <- zset_tree_generation_available(index, ordinal),
             {:ok, ops} <- zset_tree_conversion_ops(entries) do
          generation = index * @zset_tree_ordinal_limit + ordinal
          path = ZSetTree.path(promoted_data_dir(state), state.shard_index, redis_key, generation)
          mark_zset_tree_prepared(path)

          with :ok <- ZSetTree.write_key(Path.dirname(path), redis_key),
               {:ok, _outcomes} <-
                 PromotedFileSync.track_write(path, fn ->
                   ZSetTree.apply_ops(path, {index, ordinal}, ops)
                 end),
               # Readers check the marker first: once it names the tree, the
               # members in the keydir are no longer read.
               :ok <-
                 staging.put_marker.(
                   Promotion.marker_key(redis_key),
                   ZSetTree.marker_value(generation),
                   zset_tree_type_expire_at(state, redis_key)
                 ),
               :ok <- staging.drop_members.(entries) do
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

      # --- MULTI's store ---

      # The local transaction store with a tree-encoded set's members routed to
      # its tree. A store routed across shards refuses them: its writes would
      # carry another shard's Raft positions.
      defp zset_tree_wrap_tx_store(
             store,
             anchor_state,
             ctx_for_key,
             put_in_ctx,
             delete_in_ctx,
             :local
           ) do
        staging = fn redis_key, prefix ->
          ctx = ctx_for_key.(redis_key)

          %{
            put_marker: fn key, value, expire_at_ms ->
              put_in_ctx.(ctx, key, value, expire_at_ms)
            end,
            delete_marker: fn key -> delete_in_ctx.(ctx, key) end,
            drop_members: fn entries ->
              Enum.each(entries, fn {member, _score} ->
                :ok = delete_in_ctx.(ctx, prefix <> member)
              end)
            end
          }
        end

        tree_store = zset_tree_store_fields(anchor_state)

        store
        |> Map.merge(tree_store, fn _key, keydir_fun, tree_fun ->
          zset_tree_or(anchor_state, keydir_fun, tree_fun)
        end)
        |> zset_tree_wrap_store_reads(anchor_state)
        |> zset_tree_wrap_member_reads(anchor_state)
        |> zset_tree_wrap_member_writes(anchor_state, staging)
      end

      defp zset_tree_wrap_tx_store(store, anchor_state, _ctx_for_key, _put, _delete, :routed) do
        refuse = fn redis_key ->
          if is_binary(redis_key) and zset_tree_path(anchor_state, redis_key) do
            throw({:transaction_store_failure, :zset_tree_cross_shard_unsupported})
          end
        end

        Map.new(store, fn
          {name, fun} when is_function(fun) ->
            if String.starts_with?(Atom.to_string(name), ["compound_", "zset_"]),
              do: {name, zset_tree_refusing(fun, refuse)},
              else: {name, fun}

          other ->
            other
        end)
      end

      defp zset_tree_refusing(fun, refuse) do
        case :erlang.fun_info(fun, :arity) do
          {:arity, 1} ->
            fn key ->
              refuse.(key)
              fun.(key)
            end

          {:arity, 2} ->
            fn key, a ->
              refuse.(key)
              fun.(key, a)
            end

          {:arity, 3} ->
            fn key, a, b ->
              refuse.(key)
              fun.(key, a, b)
            end

          {:arity, 4} ->
            fn key, a, b, c ->
              refuse.(key)
              fun.(key, a, b, c)
            end

          {:arity, 5} ->
            fn key, a, b, c, d ->
              refuse.(key)
              fun.(key, a, b, c, d)
            end

          {:arity, 6} ->
            fn key, a, b, c, d, e ->
              refuse.(key)
              fun.(key, a, b, c, d, e)
            end

          _other ->
            fun
        end
      end

      # Keeps the store's own function for a key that is not tree-encoded.
      defp zset_tree_or(_state, keydir_fun, tree_fun) do
        case :erlang.fun_info(tree_fun, :arity) do
          {:arity, 3} ->
            fn redis_key, a, b ->
              with :unavailable <- tree_fun.(redis_key, a, b), do: keydir_fun.(redis_key, a, b)
            end

          {:arity, 4} ->
            fn redis_key, a, b, c ->
              with :unavailable <- tree_fun.(redis_key, a, b, c),
                   do: keydir_fun.(redis_key, a, b, c)
            end

          {:arity, 6} ->
            fn redis_key, a, b, c, d, e ->
              with :unavailable <- tree_fun.(redis_key, a, b, c, d, e),
                   do: keydir_fun.(redis_key, a, b, c, d, e)
            end
        end
      end

      defp zset_tree_wrap_member_reads(store, state) do
        member_score = fn path, redis_key, compound_key ->
          {:ok, member} = ZSetTree.member(redis_key, compound_key)

          case ZSetTree.score(path, member) do
            {:ok, score} ->
              score

            {:error, reason} ->
              throw({:transaction_store_failure, {:zset_tree_read_failed, reason}})
          end
        end

        meta = fn
          nil -> nil
          score -> {score, 0}
        end

        %{
          store
          | compound_get: fn redis_key, compound_key ->
              case zset_tree_route(state, redis_key, [compound_key]) do
                {:tree, path, [_], []} -> member_score.(path, redis_key, compound_key)
                :keydir -> store.compound_get.(redis_key, compound_key)
              end
            end,
            compound_get_meta: fn redis_key, compound_key ->
              case zset_tree_route(state, redis_key, [compound_key]) do
                {:tree, path, [_], []} -> meta.(member_score.(path, redis_key, compound_key))
                :keydir -> store.compound_get_meta.(redis_key, compound_key)
              end
            end,
            compound_batch_get: fn redis_key, compound_keys ->
              case zset_tree_route(state, redis_key, compound_keys) do
                {:tree, path, _members, _others} ->
                  Enum.map(compound_keys, fn key ->
                    if match?({:ok, _}, ZSetTree.member(redis_key, key)),
                      do: member_score.(path, redis_key, key),
                      else: store.compound_get.(redis_key, key)
                  end)

                :keydir ->
                  store.compound_batch_get.(redis_key, compound_keys)
              end
            end,
            compound_batch_get_meta: fn redis_key, compound_keys ->
              case zset_tree_route(state, redis_key, compound_keys) do
                {:tree, path, _members, _others} ->
                  Enum.map(compound_keys, fn key ->
                    if match?({:ok, _}, ZSetTree.member(redis_key, key)),
                      do: meta.(member_score.(path, redis_key, key)),
                      else: store.compound_get_meta.(redis_key, key)
                  end)

                :keydir ->
                  store.compound_batch_get_meta.(redis_key, compound_keys)
              end
            end
        }
      end

      defp zset_tree_wrap_member_writes(store, state, staging) do
        prefix_staging = fn redis_key ->
          staging.(redis_key, CompoundKey.zset_prefix(redis_key))
        end

        unwrap = fn
          :ok -> :ok
          {:error, reason} -> throw({:transaction_store_failure, reason})
        end

        put_split = fn redis_key, entries, keydir_put ->
          keys = Enum.map(entries, &elem(&1, 0))

          case zset_tree_write_split(state, redis_key, keys, prefix_staging.(redis_key)) do
            {:tree, path, members, _others} ->
              member_keys = MapSet.new(members)

              {tree_entries, rest} =
                Enum.split_with(entries, &MapSet.member?(member_keys, elem(&1, 0)))

              unwrap.(zset_tree_put_entries(state, redis_key, path, tree_entries))

              with :ok <- if(rest == [], do: :ok, else: keydir_put.(rest)) do
                zset_tree_follow_type_ttl(state, redis_key, rest, prefix_staging.(redis_key))
              end

            {:error, reason} ->
              throw({:transaction_store_failure, reason})

            :keydir ->
              with :ok <- keydir_put.(entries) do
                zset_tree_follow_type_ttl(state, redis_key, entries, prefix_staging.(redis_key))
              end
          end
        end

        delete_split = fn redis_key, compound_keys, keydir_delete ->
          case zset_tree_route(state, redis_key, compound_keys) do
            {:tree, path, members, others} ->
              unwrap.(zset_tree_delete_keys(state, redis_key, path, members))
              if others == [], do: :ok, else: keydir_delete.(others)

            :keydir ->
              keydir_delete.(compound_keys)
          end
        end

        %{
          store
          | compound_put: fn redis_key, compound_key, value, expire_at_ms ->
              put_split.(redis_key, [{compound_key, value, expire_at_ms}], fn [{key, v, exp}] ->
                store.compound_put.(redis_key, key, v, exp)
              end)
            end,
            compound_batch_put: fn redis_key, entries ->
              put_split.(redis_key, entries, &store.compound_batch_put.(redis_key, &1))
            end,
            compound_delete: fn redis_key, compound_key ->
              delete_split.(redis_key, [compound_key], fn [key] ->
                store.compound_delete.(redis_key, key)
              end)
            end,
            compound_batch_delete: fn redis_key, compound_keys ->
              delete_split.(
                redis_key,
                compound_keys,
                &store.compound_batch_delete.(redis_key, &1)
              )
            end,
            compound_delete_prefix: fn redis_key, prefix ->
              case ZSetTree.member_prefix?(redis_key, prefix) and zset_tree_path(state, redis_key) do
                path when is_binary(path) ->
                  ctx_staging = prefix_staging.(redis_key)
                  :ok = ctx_staging.delete_marker.(Promotion.marker_key(redis_key))
                  record_zset_tree_removal(path)
                  :ok

                _keydir ->
                  store.compound_delete_prefix.(redis_key, prefix)
              end
            end
        }
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
