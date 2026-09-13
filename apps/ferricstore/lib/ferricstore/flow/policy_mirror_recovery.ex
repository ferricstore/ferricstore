defmodule Ferricstore.Flow.PolicyMirrorRecovery do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB, PolicyMigration, RetryPolicy}
  alias Ferricstore.Flow.PolicyAttributeCatalog
  alias Ferricstore.Flow.Query.SourceCatalog
  alias Ferricstore.Flow.LMDBWriter.ProjectionOps
  alias Ferricstore.Store.BlobValue

  @batch_size 512
  @max_u64 18_446_744_073_709_551_615
  @max_lmdb_key_bytes 511
  @flow_prefix "f:"
  @global_prefix "f:{f}:"
  @policy_prefix @global_prefix <> "policy:"
  @job_prefix @global_prefix <> "pm:1:"
  @marker_prefix @global_prefix <> "pmg:1:"
  @descriptor_prefix @global_prefix <> "td:1:"
  @attribute_prefix @global_prefix <> "policy-attribute:1:"
  @attribute_member_prefix @global_prefix <> "policy-attribute-member:1:"
  @attribute_revision_prefix @global_prefix <> "policy-attribute-revision:1:"
  @attribute_repair_prefix @global_prefix <> "policy-attribute-repair:1:"
  # Type-catalog primaries use arbitrary partition tags, so there is no safe
  # primary prefix to scan without walking unrelated Flow state. Their source
  # and derived rows are cleaned from visible catalog rows/tombstones by the
  # source-row path below;
  # the shard-specific backfill key is control state, not an absent source.
  @cleanup_prefixes [
    @policy_prefix,
    @job_prefix,
    @marker_prefix,
    @descriptor_prefix,
    @attribute_prefix,
    @attribute_member_prefix,
    @attribute_revision_prefix,
    @attribute_repair_prefix
  ]

  @type source_row ::
          {atom(), binary(),
           :deleted
           | {:hot, binary(), non_neg_integer()}
           | {:read, term(), non_neg_integer(), term(), term()}}

  @spec reconcile_shard(binary(), :ets.tid(), binary(), non_neg_integer(), map()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def reconcile_shard(lmdb_path, keydir, shard_path, shard_index, instance_ctx)
      when is_binary(lmdb_path) and is_binary(shard_path) and is_integer(shard_index) and
             shard_index >= 0 and is_map(instance_ctx) do
    with :ok <- safe_fix_keydir(keydir) do
      try do
        case safe_select_initial_page(keydir) do
          {:ok, page} ->
            reconcile_pages(
              lmdb_path,
              keydir,
              shard_path,
              shard_index,
              instance_ctx,
              page,
              0
            )

          {:error, _reason} = error ->
            error
        end
      after
        safe_unfix_keydir(keydir)
      end
    end
  rescue
    error -> {:error, {:policy_mirror_recovery_failed, error}}
  catch
    kind, reason -> {:error, {:policy_mirror_recovery_failed, {kind, reason}}}
  end

  def reconcile_shard(_lmdb_path, _keydir, _shard_path, _shard_index, _instance_ctx),
    do: {:error, :invalid_policy_mirror_recovery_request}

  defp reconcile_pages(
         lmdb_path,
         keydir,
         _shard_path,
         shard_index,
         _instance_ctx,
         :end_of_table,
         count
       ),
       do: cleanup_absent_mirrors(lmdb_path, keydir, shard_index, count)

  defp reconcile_pages(
         lmdb_path,
         keydir,
         shard_path,
         shard_index,
         instance_ctx,
         {keys, continuation},
         count
       )
       when is_list(keys) do
    with {:ok, source_rows} <- classify_page(keys, keydir, shard_index),
         {:ok, ops, repaired} <-
           build_page_ops(source_rows, lmdb_path, shard_path, shard_index, instance_ctx),
         :ok <- write_ops(lmdb_path, ops),
         {:ok, next_page} <- safe_select_page(continuation) do
      reconcile_pages(
        lmdb_path,
        keydir,
        shard_path,
        shard_index,
        instance_ctx,
        next_page,
        count + repaired
      )
    end
  end

  defp reconcile_pages(
         _lmdb_path,
         _keydir,
         _shard_path,
         _shard_index,
         _instance_ctx,
         _invalid,
         _count
       ),
       do: {:error, :invalid_policy_mirror_keydir_page}

  # A source delete removes the keydir row, so a source-keydir-only scan cannot
  # observe a mirror entry left behind by a failed writer.  The mirror scan is
  # intentionally limited to namespaces owned by this recovery pass.  Each
  # page is guarded by a fresh keydir lookup immediately before its delete
  # batch. Recovery runs in the LMDB writer, so a source write that races after
  # that lookup is queued behind this batch and its later mirror put wins.
  defp cleanup_absent_mirrors(lmdb_path, keydir, shard_index, count) do
    Enum.reduce_while(@cleanup_prefixes, {:ok, count}, fn prefix, {:ok, acc} ->
      case cleanup_mirror_prefix(lmdb_path, keydir, shard_index, prefix) do
        {:ok, _deleted} -> {:cont, {:ok, acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp cleanup_mirror_prefix(lmdb_path, keydir, shard_index, prefix) do
    LMDB.reduce_prefix_entries(lmdb_path, prefix, @batch_size, 0, fn entries, deleted ->
      with {:ok, ops, page_deleted} <- absent_mirror_ops(entries, keydir, shard_index),
           :ok <- write_ops(lmdb_path, ops) do
        {:ok, deleted + page_deleted}
      end
    end)
  end

  defp absent_mirror_ops(entries, keydir, shard_index) when is_list(entries) do
    with {:ok, candidates} <- stale_mirror_candidates(entries, keydir, shard_index),
         {:ok, ops} <- revalidate_mirror_candidates(candidates, keydir, shard_index) do
      {:ok, ops, length(ops)}
    end
  end

  defp absent_mirror_ops(_entries, _keydir, _shard_index),
    do: {:error, :invalid_policy_mirror_page}

  defp stale_mirror_candidates(entries, keydir, shard_index) do
    Enum.reduce_while(entries, {:ok, []}, fn {key, _encoded}, {:ok, reversed_keys} ->
      case safe_lookup_keydir(keydir, key) do
        :not_found ->
          if valid_cleanup_key?(key, shard_index) do
            {:cont, {:ok, [key | reversed_keys]}}
          else
            {:halt, {:error, {:invalid_policy_mirror_key, key}}}
          end

        {:ok, entry} ->
          case classify_entry(entry, shard_index) do
            {:ok, {_kind, ^key, :deleted}} ->
              {:cont, {:ok, [key | reversed_keys]}}

            {:ok, {_kind, ^key, _live}} ->
              {:cont, {:ok, reversed_keys}}

            :ignore ->
              {:cont, {:ok, reversed_keys}}

            {:error, _reason} = error ->
              {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed_keys} -> {:ok, Enum.reverse(reversed_keys)}
      {:error, _reason} = error -> error
    end
  end

  defp revalidate_mirror_candidates(candidates, keydir, shard_index) do
    Enum.reduce_while(candidates, {:ok, []}, fn key, {:ok, reversed_ops} ->
      case absent_mirror_key_ops(key, keydir, shard_index) do
        {:ok, []} ->
          {:cont, {:ok, reversed_ops}}

        {:ok, ops} when is_list(ops) ->
          {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed_ops} -> {:ok, Enum.reverse(reversed_ops)}
      {:error, _reason} = error -> error
    end
  end

  defp absent_mirror_key_ops(key, keydir, shard_index) when is_binary(key) do
    case safe_lookup_keydir(keydir, key) do
      :not_found ->
        if valid_cleanup_key?(key, shard_index) do
          {:ok, [{:delete, key}]}
        else
          {:error, {:invalid_policy_mirror_key, key}}
        end

      {:ok, entry} ->
        case classify_entry(entry, shard_index) do
          {:ok, {_kind, ^key, :deleted}} ->
            {:ok, [{:delete, key}]}

          {:ok, {_kind, ^key, _live}} ->
            {:ok, []}

          :ignore ->
            {:ok, []}

          {:error, _reason} = error ->
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp absent_mirror_key_ops(_key, _keydir, _shard_index),
    do: {:error, :invalid_policy_mirror_key}

  defp valid_cleanup_key?(key, shard_index) do
    case key_kind(key, shard_index) do
      :policy -> match?({:ok, _type}, Keys.policy_type(key))
      :job -> Keys.policy_migration_job_key?(key)
      :marker -> valid_digest_suffix?(key, @marker_prefix)
      :descriptor -> valid_digest_suffix?(key, @descriptor_prefix)
      :attribute -> valid_attribute_key?(key)
      _other -> false
    end
  end

  defp valid_attribute_key?(key) do
    cond do
      String.starts_with?(key, @attribute_member_prefix) ->
        valid_attribute_member_key?(key)

      String.starts_with?(key, @attribute_prefix) ->
        valid_digest_suffix?(key, @attribute_prefix)

      String.starts_with?(key, @attribute_revision_prefix) ->
        valid_digest_suffix?(key, @attribute_revision_prefix)

      String.starts_with?(key, @attribute_repair_prefix) ->
        valid_digest_suffix?(key, @attribute_repair_prefix)

      true ->
        false
    end
  end

  defp classify_page(keys, keydir, shard_index) do
    if is_list(keys) do
      Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, rows} ->
        case classify_keydir_entry(key, keydir, shard_index) do
          :ignore ->
            {:cont, {:ok, rows}}

          {:ok, row} ->
            {:cont, {:ok, [row | rows]}}

          {:error, _reason} = error ->
            {:halt, error}
        end
      end)
      |> case do
        {:ok, rows} -> {:ok, Enum.reverse(rows)}
        {:error, _reason} = error -> error
      end
    else
      {:error, :invalid_policy_mirror_keydir_page}
    end
  end

  defp classify_keydir_entry(key, keydir, shard_index) when is_binary(key) do
    # Avoid touching the ETS row for the ordinary state/value keys that share
    # the keydir with the policy mirror sources.
    if key_kind(key, shard_index) == :ignore do
      :ignore
    else
      case safe_lookup_keydir(keydir, key) do
        {:ok, entry} -> classify_entry(entry, shard_index)
        :not_found -> :ignore
        {:error, _reason} = error -> error
      end
    end
  end

  defp classify_keydir_entry(_key, _keydir, _shard_index), do: :ignore

  defp classify_entry(
         {key, value, expire_at_ms, _lfu, file_id, offset, _value_size},
         shard_index
       )
       when is_binary(key) do
    case key_kind(key, shard_index) do
      :ignore ->
        :ignore

      kind when is_atom(kind) ->
        cond do
          not valid_expiry?(expire_at_ms) ->
            {:error, {:invalid_policy_mirror_expiry, key}}

          file_id == :deleted ->
            {:ok, {kind, key, :deleted}}

          file_id == :pending ->
            {:error, {:policy_mirror_source_pending, key}}

          expired?(expire_at_ms) ->
            {:ok, {kind, key, :deleted}}

          is_binary(value) ->
            {:ok, {kind, key, {:hot, value, expire_at_ms}}}

          is_nil(value) ->
            {:ok, {kind, key, {:read, value, expire_at_ms, file_id, offset}}}

          true ->
            {:error, {:invalid_policy_mirror_source_value, key}}
        end
    end
  end

  defp classify_entry(_entry, _shard_index), do: :ignore

  defp key_kind(key, shard_index) do
    if String.starts_with?(key, @flow_prefix) do
      cond do
        Keys.policy_key?(key) -> :policy
        Keys.policy_migration_job_key?(key) -> :job
        String.starts_with?(key, @job_prefix) -> :job
        String.starts_with?(key, @marker_prefix) -> :marker
        String.starts_with?(key, @descriptor_prefix) -> :descriptor
        Keys.type_catalog_member_key?(key) -> :catalog
        Keys.policy_indexed_attribute_catalog_key?(key) -> :attribute
        String.starts_with?(key, @attribute_prefix) -> :attribute
        String.starts_with?(key, @attribute_member_prefix) -> :attribute
        String.starts_with?(key, @attribute_revision_prefix) -> :attribute
        String.starts_with?(key, @attribute_repair_prefix) -> :attribute
        key == Keys.policy_catalog_backfill_key(shard_index) -> :backfill
        true -> :ignore
      end
    else
      :ignore
    end
  end

  defp build_page_ops([], _lmdb_path, _shard_path, _shard_index, _instance_ctx), do: {:ok, [], 0}

  defp build_page_ops(source_rows, lmdb_path, shard_path, shard_index, instance_ctx) do
    {deleted_rows, remaining_rows} =
      Enum.split_with(source_rows, &match?({_kind, _key, :deleted}, &1))

    {hot_rows, read_rows} =
      Enum.split_with(remaining_rows, &match?({_kind, _key, {:hot, _value, _expiry}}, &1))

    with {:ok, cleanup_ops} <- catalog_cleanup_ops(source_rows, lmdb_path),
         {:ok, hot_ops} <- hot_rows_to_ops(hot_rows, shard_index, instance_ctx),
         {:ok, hydrated} <- hydrate_rows(read_rows, shard_path, shard_index, instance_ctx),
         {:ok, deleted_ops} <- deleted_rows_to_ops(deleted_rows),
         {:ok, read_ops} <- rows_to_ops(Enum.zip(read_rows, hydrated), :hydrated) do
      {:ok, cleanup_ops ++ deleted_ops ++ hot_ops ++ read_ops, length(source_rows)}
    end
  end

  defp hot_rows_to_ops([], _shard_index, _instance_ctx), do: {:ok, []}

  defp hot_rows_to_ops(rows, shard_index, instance_ctx) when is_list(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn {kind, key, {:hot, value, expire_at_ms}},
                                          {:ok, reversed_ops} ->
      with {:ok, value} <- materialize_hot_value(instance_ctx, shard_index, value),
           {:ok, ops} <- source_ops(kind, key, {value, expire_at_ms}) do
        {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> reverse_ops_result()
  end

  defp materialize_hot_value(instance_ctx, shard_index, value)
       when is_map(instance_ctx) and is_integer(shard_index) and is_binary(value) do
    BlobValue.maybe_materialize(
      Map.get(instance_ctx, :data_dir),
      shard_index,
      BlobValue.threshold(instance_ctx),
      value
    )
  end

  defp materialize_hot_value(_instance_ctx, _shard_index, value), do: {:ok, value}

  defp hydrate_rows([], _shard_path, _shard_index, _instance_ctx), do: {:ok, []}

  defp hydrate_rows(rows, shard_path, shard_index, instance_ctx) when is_list(rows) do
    source_state = %{
      shard_data_path: shard_path,
      instance_ctx: instance_ctx,
      shard_index: shard_index
    }

    requests =
      Enum.map(rows, fn
        {_kind, key, {:read, cached_value, expire_at_ms, file_id, offset}} ->
          {key, cached_value, expire_at_ms, file_id, offset}
      end)

    case ProjectionOps.read_source_locations(source_state, requests) do
      {:ok, results} when is_list(results) and length(results) == length(rows) ->
        hydrate_source_results(rows, results)

      {:ok, results} when is_list(results) ->
        {:error,
         {:policy_mirror_source_read_failed,
          {:source_batch_result_count_mismatch, length(rows), length(results)}}}

      {:error, reason} ->
        {:error, {:policy_mirror_source_read_failed, reason}}
    end
  end

  defp hydrate_source_results(rows, results) do
    Enum.zip(rows, results)
    |> Enum.reduce_while({:ok, []}, fn
      {{_kind, _key, {:read, _cached_value, expire_at_ms, _file_id, _offset}},
       {:ok, value, result_expire_at_ms}},
      {:ok, reversed_values}
      when is_binary(value) and result_expire_at_ms == expire_at_ms ->
        {:cont, {:ok, [{value, expire_at_ms} | reversed_values]}}

      {{_kind, key, {:read, _cached_value, expire_at_ms, _file_id, _offset}}, :not_found},
      {:ok, reversed_values} ->
        if expired?(expire_at_ms) do
          {:cont, {:ok, [nil | reversed_values]}}
        else
          {:halt, {:error, {:policy_mirror_source_missing, key}}}
        end

      {{_kind, _key, {:read, _cached_value, _expire_at_ms, _file_id, _offset}}, {:error, reason}},
      {:ok, _reversed_values} ->
        {:halt, {:error, {:policy_mirror_source_read_failed, reason}}}

      {{_kind, _key, _source}, invalid}, {:ok, _reversed_values} ->
        {:halt, {:error, {:invalid_policy_mirror_source_result, invalid}}}
    end)
    |> case do
      {:ok, reversed_values} -> {:ok, Enum.reverse(reversed_values)}
      {:error, _reason} = error -> error
    end
  end

  defp deleted_rows_to_ops([]), do: {:ok, []}

  defp deleted_rows_to_ops(rows) when is_list(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn {kind, key, :deleted}, {:ok, reversed_ops} ->
      case source_ops(kind, key, nil) do
        {:ok, ops} -> {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> reverse_ops_result()
  end

  defp rows_to_ops([], :hydrated), do: {:ok, []}

  defp rows_to_ops(rows, :hydrated) do
    Enum.reduce_while(
      rows,
      {:ok, []},
      fn {{kind, key, {:read, _cached_value, _expire_at_ms, _file_id, _offset}}, source},
         {:ok, reversed_ops} ->
        case source_ops(kind, key, source) do
          {:ok, ops} -> {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}
          {:error, _reason} = error -> {:halt, error}
        end
      end
    )
    |> reverse_ops_result()
  end

  defp catalog_cleanup_ops(rows, lmdb_path) do
    Enum.reduce_while(rows, {:ok, []}, fn
      {:catalog, key, _source}, {:ok, reversed_ops} ->
        case catalog_projection_delete_ops(lmdb_path, key) do
          {:ok, ops} -> {:cont, {:ok, :lists.reverse(ops, reversed_ops)}}
          {:error, _reason} = error -> {:halt, error}
        end

      _row, acc ->
        {:cont, acc}
    end)
    |> reverse_ops_result()
  end

  defp catalog_projection_delete_ops(lmdb_path, catalog_key) do
    case LMDB.get(lmdb_path, catalog_key) do
      :not_found ->
        {:ok, []}

      {:ok, encoded_value} when is_binary(encoded_value) ->
        with {:ok, catalog_value} <- decode_catalog_mirror_for_delete(encoded_value),
             {:ok, catalog} <- PolicyMigration.decode_catalog(catalog_value),
             true <- Keys.state_key?(catalog.state_key),
             true <- Keys.type_catalog_member_owns_state_key?(catalog_key, catalog.state_key),
             {:ok, projection_key} <-
               policy_projection_key(catalog_key, catalog.migration_generation) do
          {:ok, [{:delete, projection_key}]}
        else
          _invalid -> {:error, :corrupt_policy_catalog_primary}
        end

      {:error, _reason} = error ->
        {:error, {:policy_catalog_mirror_read_failed, error}}

      invalid ->
        {:error, {:invalid_policy_catalog_mirror_read, invalid}}
    end
  end

  defp decode_catalog_mirror_for_delete(encoded_value) do
    case LMDB.decode_value(encoded_value, 0) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      _invalid -> {:error, :corrupt_policy_catalog_primary}
    end
  end

  defp reverse_ops_result({:ok, reversed_ops}), do: {:ok, Enum.reverse(reversed_ops)}
  defp reverse_ops_result({:error, _reason} = error), do: error

  defp source_ops(:catalog, key, nil) do
    with {:ok, source_catalog_op} <- SourceCatalog.delete_op(key) do
      {:ok, [{:delete, key}, source_catalog_op]}
    end
  end

  defp source_ops(_kind, key, nil), do: {:ok, [{:delete, key}]}

  defp source_ops(kind, key, {value, expire_at_ms})
       when is_binary(value) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
              expire_at_ms <= @max_u64 do
    with {:ok, primary_ops} <- primary_ops(kind, key, value, expire_at_ms) do
      {:ok, primary_ops}
    end
  end

  defp source_ops(_kind, key, invalid),
    do: {:error, {:invalid_policy_mirror_source_value, {key, invalid}}}

  defp primary_ops(:policy, key, value, expire_at_ms) do
    with {:ok, type} <- Keys.policy_type(key),
         {:ok, {_generation, %{type: ^type}}} <- RetryPolicy.decode_flow_policy_entry(value) do
      {:ok, [{:put, key, LMDB.encode_value(value, expire_at_ms)}]}
    else
      _invalid -> {:error, :corrupt_flow_policy_mirror}
    end
  end

  defp primary_ops(:job, key, value, expire_at_ms) do
    with {:ok, job} <- PolicyMigration.decode_job(value),
         true <- Keys.policy_migration_job_key(job.type) == key do
      {:ok, [{:put, key, LMDB.encode_value(value, expire_at_ms)}]}
    else
      _invalid -> {:error, :corrupt_policy_migration_job}
    end
  end

  defp primary_ops(:marker, key, value, expire_at_ms) do
    with {:ok, job} <- PolicyMigration.decode_job(value),
         true <- Keys.policy_migration_marker_key(job.type) == key do
      {:ok, [{:put, key, LMDB.encode_value(value, expire_at_ms)}]}
    else
      _invalid -> {:error, :corrupt_policy_migration_marker}
    end
  end

  defp primary_ops(:descriptor, key, value, expire_at_ms) do
    with {:ok, descriptor} <- PolicyMigration.decode_type_descriptor(value),
         true <- Keys.type_catalog_descriptor_key(descriptor.type) == key do
      {:ok, [{:put, key, LMDB.encode_value(value, expire_at_ms)}]}
    else
      _invalid -> {:error, :corrupt_policy_type_descriptor}
    end
  end

  defp primary_ops(:catalog, key, value, expire_at_ms) do
    with {:ok, catalog} <- PolicyMigration.decode_catalog(value),
         true <- Keys.state_key?(catalog.state_key),
         true <- Keys.type_catalog_member_owns_state_key?(key, catalog.state_key),
         {:ok, source_catalog_op} <- SourceCatalog.put_op(key, catalog.state_key),
         {:ok, projection_key} <- policy_projection_key(key, catalog.migration_generation) do
      {:ok,
       [
         {:put, key, LMDB.encode_value(value, expire_at_ms)},
         {:put, projection_key, <<1>>},
         source_catalog_op
       ]}
    else
      _invalid -> {:error, :corrupt_policy_catalog_primary}
    end
  end

  defp primary_ops(:attribute, key, value, expire_at_ms) do
    with :ok <- validate_attribute_entry(key, value) do
      {:ok, [{:put, key, LMDB.encode_value(value, expire_at_ms)}]}
    else
      {:error, _reason} = error -> error
    end
  end

  defp primary_ops(:backfill, key, value, expire_at_ms) do
    with {:ok, _progress} <- PolicyMigration.decode_backfill_progress(value) do
      {:ok, [{:put, key, LMDB.encode_value(value, expire_at_ms)}]}
    else
      _invalid -> {:error, :corrupt_policy_catalog_backfill_progress}
    end
  end

  defp primary_ops(_kind, key, _value, _expire_at_ms),
    do: {:error, {:unsupported_policy_mirror_key, key}}

  defp policy_projection_key(catalog_key, generation)
       when is_binary(catalog_key) and is_integer(generation) and generation >= 0 do
    with {:ok, descriptor_key} <- Keys.type_catalog_descriptor_key_from_member(catalog_key),
         <<@descriptor_prefix::binary, type_digest::binary-size(43)>> <- descriptor_key,
         projection_key <-
           Keys.policy_catalog_projection_global_prefix() <>
             type_digest <> ":" <> <<generation::unsigned-big-64, catalog_key::binary>>,
         true <- byte_size(projection_key) <= @max_lmdb_key_bytes do
      {:ok, projection_key}
    else
      _invalid -> {:error, :invalid_policy_catalog_projection_key}
    end
  end

  defp policy_projection_key(_catalog_key, _generation),
    do: {:error, :invalid_policy_catalog_projection_key}

  defp validate_attribute_entry(key, value) do
    cond do
      String.starts_with?(key, @attribute_member_prefix) ->
        if valid_attribute_member_key?(key) and value == <<1>>,
          do: :ok,
          else: {:error, :corrupt_policy_attribute_catalog}

      String.starts_with?(key, @attribute_prefix) ->
        if valid_digest_suffix?(key, @attribute_prefix) and byte_size(value) == 8,
          do: :ok,
          else: {:error, :corrupt_policy_attribute_catalog}

      String.starts_with?(key, @attribute_revision_prefix) ->
        if valid_digest_suffix?(key, @attribute_revision_prefix) and byte_size(value) == 8,
          do: :ok,
          else: {:error, :corrupt_policy_attribute_catalog}

      String.starts_with?(key, @attribute_repair_prefix) ->
        with {:ok, name} <- PolicyAttributeCatalog.decode_repair_request(value),
             true <- Keys.policy_indexed_attribute_repair_key(name) == key do
          :ok
        else
          _invalid -> {:error, :corrupt_policy_attribute_catalog}
        end

      true ->
        {:error, :corrupt_policy_attribute_catalog}
    end
  end

  defp valid_attribute_member_key?(key) do
    case binary_part(
           key,
           byte_size(@attribute_member_prefix),
           byte_size(key) - byte_size(@attribute_member_prefix)
         ) do
      <<name_digest::binary-size(43), ?:, type_digest::binary-size(43)>> ->
        valid_digest?(name_digest) and valid_digest?(type_digest)

      _invalid ->
        false
    end
  rescue
    ArgumentError -> false
  end

  defp valid_digest_suffix?(key, prefix) do
    suffix = binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix))
    valid_digest?(suffix)
  rescue
    ArgumentError -> false
  end

  defp valid_digest?(digest) when is_binary(digest) and byte_size(digest) == 43 do
    case Base.url_decode64(digest, padding: false) do
      {:ok, decoded} when byte_size(decoded) == 32 ->
        Base.url_encode64(decoded, padding: false) == digest

      _invalid ->
        false
    end
  end

  defp valid_digest?(_digest), do: false

  defp valid_expiry?(expire_at_ms) do
    is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64
  end

  defp expired?(expire_at_ms) when is_integer(expire_at_ms) and expire_at_ms > 0 do
    expire_at_ms <= System.system_time(:millisecond)
  end

  defp expired?(_expire_at_ms), do: false

  defp write_ops(_lmdb_path, []), do: :ok

  defp write_ops(lmdb_path, ops) when is_binary(lmdb_path) and is_list(ops) do
    case LMDB.write_batch(lmdb_path, ops) do
      :ok -> :ok
      {:error, _reason} = error -> error
      invalid -> {:error, {:invalid_policy_mirror_write_result, invalid}}
    end
  rescue
    error -> {:error, {:policy_mirror_write_failed, error}}
  catch
    kind, reason -> {:error, {:policy_mirror_write_failed, {kind, reason}}}
  end

  defp safe_fix_keydir(keydir) do
    :ets.safe_fixtable(keydir, true)
    :ok
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp safe_unfix_keydir(keydir) do
    :ets.safe_fixtable(keydir, false)
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp safe_select_initial_page(keydir) do
    case :ets.select(keydir, keydir_match_spec(), @batch_size) do
      :end_of_table -> {:ok, :end_of_table}
      :"$end_of_table" -> {:ok, :end_of_table}
      {entries, continuation} when is_list(entries) -> {:ok, {entries, continuation}}
      invalid -> {:error, {:invalid_policy_mirror_keydir_page, invalid}}
    end
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp safe_select_page(continuation) do
    case :ets.select(continuation) do
      :"$end_of_table" ->
        {:ok, :end_of_table}

      {entries, next_continuation} when is_list(entries) ->
        {:ok, {entries, next_continuation}}

      invalid ->
        {:error, {:invalid_policy_mirror_keydir_page, invalid}}
    end
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp safe_lookup_keydir(keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, _value, _expire_at_ms, _lfu, _file_id, _offset, _value_size} = entry] ->
        {:ok, entry}

      [] ->
        :not_found

      _invalid ->
        {:error, :invalid_policy_mirror_keydir_entry}
    end
  rescue
    ArgumentError -> {:error, :source_keydir_unavailable}
  end

  defp keydir_match_spec do
    [
      {{:"$1", :_, :_, :_, :_, :_, :_}, [], [:"$1"]}
    ]
  end
end
