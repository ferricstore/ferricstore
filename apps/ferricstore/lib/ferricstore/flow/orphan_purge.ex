defmodule Ferricstore.Flow.OrphanPurge do
  @moduledoc false

  # A registered Flow keeps its state in the keydir or, once pruned or
  # hibernated, in LMDB, which has it before the keydir lets it go. A Flow whose
  # state record is gone from both keeps its registry marker, retention guard,
  # catalog entry and values for good, because retention starts from the state
  # record. find/2 lists such Flows on a shard with what each left; purge/2
  # removes it through the replicated `:flow_purge_orphans` command, which needs
  # cluster feature level 3 and checks every entry again on apply.

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Flow.{Digest, KeydirSpill, Keys, LMDB, LMDBMirror, LMDBWriter}
  alias Ferricstore.Flow.LMDB.IndexCodec
  alias Ferricstore.Flow.Query.{QueryRecordStore, QueryRowCodec}
  alias Ferricstore.Store.Router

  @flush_timeout_ms 30_000
  @command_entries 32
  @page_items 2_000
  @page_bytes 64 * 1_024 * 1_024
  @value_kinds [:payload, :result, :error]

  @type orphan :: %{
          state_key: binary(),
          guard: binary() | nil,
          catalog_key: binary() | nil,
          member_keys: [binary()],
          value_keys: [binary()],
          query_index_keys: [binary()]
        }

  @doc false
  @spec purge(FerricStore.Instance.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def purge(ctx, opts) when is_list(opts) do
    dry_run? = Keyword.get(opts, :dry_run, true) != false
    shards = Keyword.get(opts, :shards, Enum.to_list(0..(ctx.shard_count - 1)))
    report = %{dry_run?: dry_run?, orphans: [], purged: 0, skipped: 0}

    with :ok <- if(dry_run?, do: :ok, else: require_feature_level(ctx)) do
      Enum.reduce_while(shards, {:ok, report}, fn shard_index, {:ok, acc} ->
        with {:ok, orphans} <- find(ctx, shard_index),
             {:ok, purged, skipped} <- maybe_submit(ctx, shard_index, orphans, dry_run?) do
          found = Enum.map(orphans, &Map.put(&1, :shard_index, shard_index))

          {:cont,
           {:ok,
            %{
              acc
              | orphans: acc.orphans ++ found,
                purged: acc.purged + purged,
                skipped: acc.skipped + skipped
            }}}
        else
          {:error, reason} -> {:halt, {:error, {:shard, shard_index, reason}}}
        end
      end)
    end
  end

  @doc false
  @spec find(FerricStore.Instance.t(), non_neg_integer()) :: {:ok, [orphan()]} | {:error, term()}
  def find(ctx, shard_index) do
    cond do
      LMDBMirror.degraded_shard?(ctx, shard_index) ->
        {:error, :lmdb_mirror_degraded}

      (flush = LMDBWriter.flush(ctx.name, shard_index, @flush_timeout_ms)) != :ok ->
        {:error, {:lmdb_flush_failed, flush}}

      true ->
        path = lmdb_path(ctx, shard_index)
        keydir = elem(ctx.keydir_refs, shard_index)
        {state_keys, catalog_keys, member_keys} = scan_keydir(keydir)
        orphaned = Enum.filter(state_keys, &orphaned?(ctx, shard_index, keydir, path, &1))

        with {:ok, index_keys} <- query_index_keys(path, orphaned) do
          keys = %{catalog: catalog_keys, members: member_keys, index: index_keys}
          {:ok, Enum.map(orphaned, &describe(ctx, shard_index, path, &1, keys))}
        end
    end
  end

  defp require_feature_level(ctx) do
    case FeatureLevel.status(ctx) do
      {:ok, %{active: active}} when active >= 3 -> :ok
      {:ok, _status} -> {:error, {:feature_level_inactive, 3}}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_submit(_ctx, _shard_index, _orphans, true), do: {:ok, 0, 0}

  # A Flow without its guard cannot be checked on apply, so it stays.
  defp maybe_submit(ctx, shard_index, orphans, false) do
    {guarded, unguarded} = Enum.split_with(orphans, &is_binary(&1.guard))

    guarded
    |> Enum.chunk_every(@command_entries)
    |> Enum.reduce_while({:ok, 0, length(unguarded)}, fn entries, {:ok, purged, skipped} ->
      case Router.flow_purge_orphans(ctx, shard_index, %{orphans: entries}) do
        {:ok, %{purged: done, skipped: left}} ->
          {:cont, {:ok, purged + done, skipped + left}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  # Registry markers; type catalog entries by the digest of the state key they
  # own; retention cleanup members by their Flow's member prefix, registered or
  # not (startup leaves those of an unreadable owner unregistered).
  defp scan_keydir(keydir) do
    :ets.foldl(
      fn row, {state_keys, catalog_keys, member_keys} = acc ->
        case elem(row, 0) do
          key when is_binary(key) ->
            cond do
              Keys.registry_key?(key) ->
                case Keys.state_key_from_registry_key(key) do
                  {:ok, state_key} -> {[state_key | state_keys], catalog_keys, member_keys}
                  :error -> acc
                end

              Keys.type_catalog_member_key?(key) ->
                state_digest = key |> String.split(":") |> List.last()

                {state_keys, Map.update(catalog_keys, state_digest, [key], &[key | &1]),
                 member_keys}

              Keys.retention_cleanup_member?(key) ->
                {prefix, _digest} = split_last(key, ":")
                {state_keys, catalog_keys, Map.update(member_keys, prefix, [key], &[key | &1])}

              true ->
                acc
            end

          _other ->
            acc
        end
      end,
      {[], %{}, %{}},
      keydir
    )
  end

  defp split_last(key, separator) do
    [last | rest] = key |> String.split(separator) |> Enum.reverse()
    {(rest |> Enum.reverse() |> Enum.join(separator)) <> separator, last}
  end

  # Read in the order a live Flow could move: keydir first, then LMDB, which has
  # the state before the keydir lets it go.
  defp orphaned?(ctx, shard_index, keydir, path, state_key) do
    with [] <- :ets.lookup(keydir, state_key),
         true <- state_gone_from_lmdb?(ctx, shard_index, path, state_key),
         :not_found <- LMDB.get(path, LMDB.cold_park_key_for_state_key(state_key)),
         {:ok, registry_key} <- Keys.registry_key_from_state_key(state_key),
         {:ok, registry} when is_binary(registry) <-
           Router.read_shard_value(ctx, shard_index, registry_key) do
      true
    else
      _present_or_unknown -> false
    end
  end

  # No row, or a row left on a segment-projection frame that no longer holds
  # the record (QueryRowCodec.segment_projection_row?/2).
  defp state_gone_from_lmdb?(ctx, shard_index, path, state_key) do
    case LMDB.get(path, state_key) do
      :not_found ->
        true

      {:ok, encoded} ->
        QueryRowCodec.segment_projection_row?(encoded, state_key) and
          record_lost?(
            QueryRecordStore.read_encoded_many(
              ctx,
              shard_index,
              path,
              [state_key],
              System.system_time(:millisecond),
              QueryRecordStore.max_input_bytes(ctx),
              include_expired: true
            )
          )

      {:error, _reason} ->
        false
    end
  end

  @doc false
  def __record_lost_for_test__(result), do: record_lost?(result)

  # A read that timed out or ran over its byte budget says nothing about the
  # frame, and a purge on it would delete a Flow that still reads back.
  defp record_lost?({:error, reason})
       when reason in [:query_deadline_exceeded, :query_hydration_batch_too_large],
       do: false

  defp record_lost?({:error, _reason}), do: true
  defp record_lost?(_result), do: false

  defp describe(ctx, shard_index, path, state_key, keys) do
    {:ok, partition_key, id} = Keys.flow_tag_from_state_key(state_key)

    %{
      state_key: state_key,
      guard: read_guard(ctx, shard_index, Keys.retention_guard_key(id, partition_key)),
      catalog_key:
        keys.catalog
        |> Map.get(Digest.encode(state_key), [])
        |> Enum.find(&Keys.type_catalog_member_owns_state_key?(&1, state_key)),
      member_keys:
        Map.get(keys.members, Keys.retention_cleanup_member_prefix(id, partition_key), []),
      value_keys: Enum.flat_map(@value_kinds, &value_keys(path, id, partition_key, &1)),
      query_index_keys: Map.get(keys.index, state_key, [])
    }
  end

  defp read_guard(ctx, shard_index, guard_key) do
    result =
      case KeydirSpill.read_local(ctx, shard_index, guard_key) do
        :keydir -> Router.read_shard_value(ctx, shard_index, guard_key)
        other -> other
      end

    case result do
      {:ok, guard} when is_binary(guard) -> guard
      _missing -> nil
    end
  end

  # Values kept in LMDB alone by Flows from before retention cleanup members;
  # newer values are reached through those members on apply.
  defp value_keys(path, id, partition_key, kind) do
    prefix = id |> Keys.value_key(kind, 0, partition_key) |> String.replace_suffix("0", "")

    case LMDB.prefix_entries(path, prefix, 256) do
      {:ok, entries} ->
        for {key, _value} <- entries,
            version = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix)),
            version != "" and version =~ ~r/\A\d+\z/,
            do: key

      {:error, _reason} ->
        []
    end
  end

  defp query_index_keys(_path, []), do: {:ok, %{}}

  defp query_index_keys(path, state_keys) do
    by_id =
      Enum.group_by(state_keys, fn state_key ->
        {:ok, _partition_key, id} = Keys.flow_tag_from_state_key(state_key)
        id
      end)

    scan_query_index(path, by_id, MapSet.new(state_keys), "", %{})
  end

  defp scan_query_index(path, by_id, wanted, after_key, acc) do
    prefix = IndexCodec.query_index_global_prefix()

    case LMDB.prefix_entries_after_bounded(path, prefix, after_key, @page_items, @page_bytes) do
      {:ok, []} ->
        {:ok, acc}

      {:ok, entries} ->
        {last_key, _value} = List.last(entries)

        acc =
          Enum.reduce(entries, acc, fn {key, value}, acc ->
            with {:ok, id} <- IndexCodec.query_index_key_id(key),
                 true <- Map.has_key?(by_id, id),
                 {:ok, row} <- LMDB.decode_query_index_row(key, value),
                 state_key when is_binary(state_key) <- elem(row, tuple_size(row) - 1),
                 true <- MapSet.member?(wanted, state_key) do
              Map.update(acc, state_key, [key], &[key | &1])
            else
              _other -> acc
            end
          end)

        scan_query_index(path, by_id, wanted, last_key, acc)

      {:error, _reason} = error ->
        error
    end
  end

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
