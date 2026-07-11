defmodule Ferricstore.Flow.PolicyMigration do
  @moduledoc false

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBMirror
  alias Ferricstore.Store.Router

  @catalog_magic <<"FCT", 1>>
  @type_descriptor_magic <<"FTD", 1>>
  @job_magic <<"FPM", 1>>
  @backfill_magic <<"FCB", 1>>
  @max_exact_score 9_007_199_254_740_991
  @source_token_key "flow-policy-catalog-source-token-v1"
  @snapshot_cursor <<0>>
  @work_cursor <<1>>
  @done_cursor <<2>>
  @staging_root <<0, "fpcw:1:">>
  @staging_manifest_root <<0, "fpcm:1:">>

  @type catalog_entry :: %{
          state_key: binary(),
          migration_generation: non_neg_integer()
        }

  @type type_descriptor :: %{
          type: binary(),
          membership_revision: non_neg_integer()
        }

  @type job :: %{
          type: binary(),
          migration_generation: non_neg_integer(),
          membership_revision: non_neg_integer(),
          indexed_state_meta: binary() | nil,
          status: :active | :done
        }

  @type backfill_progress :: %{
          run_token: binary(),
          source_token: binary(),
          cursor: binary(),
          status: :active | :done
        }

  @spec max_exact_score() :: pos_integer()
  def max_exact_score, do: @max_exact_score

  @spec next_generation(non_neg_integer()) :: {:ok, pos_integer()} | {:error, binary()}
  def next_generation(generation)
      when is_integer(generation) and generation >= 0 and generation < @max_exact_score,
      do: {:ok, generation + 1}

  def next_generation(@max_exact_score),
    do: {:error, "ERR flow policy generation exhausted"}

  def next_generation(_generation),
    do: {:error, "ERR invalid flow policy generation"}

  @spec encode_catalog(binary(), binary(), non_neg_integer()) :: binary()
  def encode_catalog(type, state_key, generation)
      when is_binary(type) and type != "" and is_binary(state_key) and state_key != "" and
             is_integer(generation) and generation >= 0 and generation <= @max_exact_score do
    <<@catalog_magic::binary, generation::unsigned-big-64, byte_size(state_key)::unsigned-big-32,
      state_key::binary>>
  end

  @spec decode_catalog(term()) :: {:ok, catalog_entry()} | :error
  def decode_catalog(
        <<@catalog_magic::binary, generation::unsigned-big-64, state_key_size::unsigned-big-32,
          state_key::binary>>
      )
      when generation <= @max_exact_score and state_key_size > 0 and
             byte_size(state_key) == state_key_size do
    {:ok, %{state_key: state_key, migration_generation: generation}}
  end

  def decode_catalog(_value), do: :error

  @spec encode_type_descriptor(binary(), non_neg_integer()) :: binary()
  def encode_type_descriptor(type, membership_revision)
      when is_binary(type) and type != "" and is_integer(membership_revision) and
             membership_revision >= 0 and membership_revision <= 0xFFFFFFFFFFFFFFFF do
    <<@type_descriptor_magic::binary, membership_revision::unsigned-big-64,
      byte_size(type)::unsigned-big-32, type::binary>>
  end

  @spec decode_type_descriptor(term()) :: {:ok, type_descriptor()} | :error
  def decode_type_descriptor(
        <<@type_descriptor_magic::binary, membership_revision::unsigned-big-64,
          type_size::unsigned-big-32, type::binary>>
      )
      when type_size > 0 and byte_size(type) == type_size do
    {:ok, %{type: type, membership_revision: membership_revision}}
  end

  def decode_type_descriptor(_value), do: :error

  @spec encode_job(binary(), non_neg_integer(), binary() | nil, :active | :done) :: binary()
  def encode_job(type, generation, indexed_state_meta, status),
    do: encode_job(type, generation, 0, indexed_state_meta, status)

  @spec encode_job(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          binary() | nil,
          :active | :done
        ) :: binary()
  def encode_job(type, generation, membership_revision, indexed_state_meta, status)
      when is_binary(type) and type != "" and is_integer(generation) and generation >= 0 and
             generation <= @max_exact_score and is_integer(membership_revision) and
             membership_revision >= 0 and membership_revision <= 0xFFFFFFFFFFFFFFFF and
             status in [:active, :done] and
             (is_nil(indexed_state_meta) or is_binary(indexed_state_meta)) do
    indexed_state_meta = indexed_state_meta || ""
    indexed_tag = if indexed_state_meta == "", do: 0, else: 1
    status_tag = if status == :active, do: 0, else: 1

    <<@job_magic::binary, status_tag, indexed_tag, generation::unsigned-big-64,
      membership_revision::unsigned-big-64, byte_size(type)::unsigned-big-32,
      byte_size(indexed_state_meta)::unsigned-big-32, type::binary, indexed_state_meta::binary>>
  end

  @spec decode_job(term()) :: {:ok, job()} | :error
  def decode_job(
        <<@job_magic::binary, status_tag, indexed_tag, generation::unsigned-big-64,
          membership_revision::unsigned-big-64, type_size::unsigned-big-32,
          indexed_size::unsigned-big-32, payload::binary>>
      )
      when status_tag in [0, 1] and indexed_tag in [0, 1] and
             generation <= @max_exact_score and type_size > 0 and
             byte_size(payload) == type_size + indexed_size do
    <<type::binary-size(type_size), indexed::binary-size(indexed_size)>> = payload

    with {:ok, indexed_state_meta} <- decode_indexed_state_meta(indexed_tag, indexed) do
      {:ok,
       %{
         type: type,
         migration_generation: generation,
         membership_revision: membership_revision,
         indexed_state_meta: indexed_state_meta,
         status: if(status_tag == 0, do: :active, else: :done)
       }}
    end
  end

  def decode_job(_value), do: :error

  @spec encode_backfill_progress(binary(), binary(), binary(), :active | :done) :: binary()
  def encode_backfill_progress(run_token, source_token, cursor, status)
      when is_binary(run_token) and run_token != "" and byte_size(run_token) <= 64 and
             is_binary(source_token) and source_token != "" and byte_size(source_token) <= 64 and
             is_binary(cursor) and status in [:active, :done] do
    status_tag = if status == :active, do: 0, else: 1

    <<@backfill_magic::binary, status_tag, byte_size(run_token)::unsigned-big-16,
      byte_size(source_token)::unsigned-big-16, byte_size(cursor)::unsigned-big-32,
      run_token::binary, source_token::binary, cursor::binary>>
  end

  @spec decode_backfill_progress(term()) :: {:ok, backfill_progress()} | :error
  def decode_backfill_progress(
        <<@backfill_magic::binary, status_tag, token_size::unsigned-big-16,
          source_token_size::unsigned-big-16, cursor_size::unsigned-big-32, payload::binary>>
      )
      when status_tag in [0, 1] and token_size > 0 and token_size <= 64 and
             source_token_size > 0 and source_token_size <= 64 and
             byte_size(payload) == token_size + source_token_size + cursor_size do
    <<run_token::binary-size(token_size), source_token::binary-size(source_token_size),
      cursor::binary-size(cursor_size)>> = payload

    {:ok,
     %{
       run_token: run_token,
       source_token: source_token,
       cursor: cursor,
       status: if(status_tag == 0, do: :active, else: :done)
     }}
  end

  def decode_backfill_progress(_value), do: :error

  @spec source_token(FerricStore.Instance.t(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def source_token(ctx, shard_index) do
    path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> LMDB.path()

    case LMDB.get(path, @source_token_key) do
      {:ok, token} when is_binary(token) and token != "" and byte_size(token) <= 64 ->
        {:ok, token}

      :not_found ->
        {:ok, "uninitialized"}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_policy_catalog_source_token}
    end
  end

  @spec rotate_source_token(binary()) :: :ok | {:error, term()}
  def rotate_source_token(lmdb_path) when is_binary(lmdb_path) do
    token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    LMDB.write_batch(lmdb_path, [{:put, @source_token_key, token}])
  end

  @spec backfill_page(
          FerricStore.Instance.t(),
          non_neg_integer(),
          binary(),
          pos_integer(),
          pos_integer()
        ) :: {:ok, map()} | {:error, term()}
  def backfill_page(ctx, shard_index, cursor, max_items, max_bytes)
      when is_binary(cursor) and is_integer(max_items) and max_items > 0 and
             max_items <= 256 and is_integer(max_bytes) and max_bytes > 0 do
    with {:ok, run_token, after_key} <- decode_work_cursor(cursor) do
      path = lmdb_path(ctx, shard_index)
      prefix = staging_prefix(run_token)

      case LMDB.prefix_entries_after_bounded(path, prefix, after_key, max_items, max_bytes) do
        {:ok, []} ->
          {:ok, %{cursor: @done_cursor, candidates: [], done?: true}}

        {:ok, entries} ->
          with {:ok, scanned_to, candidates} <-
                 hydrate_backfill_entries(ctx, shard_index, entries, prefix, max_bytes) do
            {:ok,
             %{
               cursor: encode_work_cursor(run_token, scanned_to),
               candidates: candidates,
               done?: false
             }}
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  def snapshot_cursor, do: @snapshot_cursor
  def done_cursor, do: @done_cursor
  def work_cursor(run_token), do: encode_work_cursor(run_token, "")

  def work_cursor?(cursor, run_token) when is_binary(cursor) and is_binary(run_token) do
    case decode_work_cursor(cursor) do
      {:ok, ^run_token, _after_key} -> true
      _invalid -> false
    end
  end

  def work_cursor?(_cursor, _run_token), do: false

  @spec snapshot_complete?(FerricStore.Instance.t(), non_neg_integer(), binary()) :: boolean()
  def snapshot_complete?(ctx, shard_index, run_token) do
    LMDB.get(lmdb_path(ctx, shard_index), staging_manifest_key(run_token)) == {:ok, run_token}
  end

  @spec snapshot_primary_keydir(
          FerricStore.Instance.t(),
          non_neg_integer(),
          binary(),
          pos_integer(),
          pos_integer()
        ) :: :ok | {:error, term()}
  def snapshot_primary_keydir(ctx, shard_index, run_token, max_items, max_bytes)
      when is_binary(run_token) and run_token != "" and byte_size(run_token) <= 64 and
             is_integer(max_items) and max_items > 0 and max_items <= 256 and
             is_integer(max_bytes) and max_bytes > 0 do
    keydir = elem(ctx.keydir_refs, shard_index)
    path = lmdb_path(ctx, shard_index)
    prefix = staging_prefix(run_token)
    :ets.safe_fixtable(keydir, true)

    try do
      match_spec = [{{:"$1", :_, :_, :_, :_, :_, :_}, [{:is_binary, :"$1"}], [:"$1"]}]

      with :ok <-
             snapshot_primary_pages(
               keydir,
               path,
               prefix,
               max_bytes,
               :ets.select(keydir, match_spec, max_items)
             ) do
        LMDB.write_batch(path, [{:put, staging_manifest_key(run_token), run_token}])
      end
    after
      :ets.safe_fixtable(keydir, false)
    end
  rescue
    error in ArgumentError -> {:error, {:policy_catalog_keydir_unavailable, error}}
  end

  @spec cleanup_snapshot(FerricStore.Instance.t(), non_neg_integer(), binary()) ::
          :ok | {:error, term()}
  def cleanup_snapshot(ctx, shard_index, run_token) do
    path = lmdb_path(ctx, shard_index)
    prefix = staging_prefix(run_token)

    with :ok <- cleanup_snapshot_pages(path, prefix) do
      LMDB.write_batch(path, [{:delete, staging_manifest_key(run_token)}])
    end
  end

  @spec next_job(FerricStore.Instance.t(), non_neg_integer()) ::
          {:ok, nil | %{key: binary(), value: binary(), job: job()}} | {:error, term()}
  def next_job(ctx, shard_index) do
    if LMDBMirror.degraded_shard?(ctx, shard_index) do
      {:error, :flow_policy_migration_projection_degraded}
    else
      next_projected_job(ctx, shard_index)
    end
  end

  defp next_projected_job(ctx, shard_index) do
    case LMDB.prefix_entries(lmdb_path(ctx, shard_index), Keys.policy_migration_job_prefix(), 1) do
      {:ok, []} ->
        {:ok, nil}

      {:ok, [{key, encoded_value}]} ->
        with {:ok, value} <- decode_mirror_value(encoded_value),
             {:ok, %{status: :active} = job} <- decode_job(value),
             true <- Keys.policy_migration_job_key(job.type) == key do
          {:ok, %{key: key, value: value, job: job}}
        else
          _invalid -> {:error, :corrupt_policy_migration_job_projection}
        end

      {:error, _reason} = error ->
        error
    end
  end

  @spec mirror_matches?(FerricStore.Instance.t(), non_neg_integer(), binary(), binary()) ::
          boolean()
  def mirror_matches?(ctx, shard_index, key, expected_value)
      when is_binary(key) and is_binary(expected_value) do
    case LMDB.get(lmdb_path(ctx, shard_index), key) do
      {:ok, encoded_value} -> decode_mirror_value(encoded_value) == {:ok, expected_value}
      _missing_or_error -> false
    end
  end

  @spec catalog_page(
          FerricStore.Instance.t(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          pos_integer(),
          pos_integer()
        ) :: {:ok, %{entries: [map()], done?: boolean()}} | {:error, term()}
  def catalog_page(ctx, shard_index, type, target_generation, limit, max_bytes)
      when is_binary(type) and type != "" and is_integer(target_generation) and
             target_generation >= 0 and target_generation <= @max_exact_score and
             is_integer(limit) and limit > 0 and limit <= 256 and is_integer(max_bytes) and
             max_bytes > 0 do
    path = lmdb_path(ctx, shard_index)
    prefix = Keys.policy_catalog_projection_prefix(type)

    with {:ok, entries} <-
           LMDB.prefix_entries_after_bounded(path, prefix, "", limit + 1, max_bytes),
         {:ok, decoded} <- decode_catalog_projection_entries(type, entries) do
      stale = Enum.take_while(decoded, &(&1.migration_generation < target_generation))

      done? =
        stale == [] or
          Enum.any?(decoded, &(&1.migration_generation >= target_generation))

      with {:ok, hydrated, truncated?} <-
             hydrate_catalog_entries(
               ctx,
               shard_index,
               type,
               Enum.take(stale, limit),
               max_bytes
             ) do
        {:ok, %{entries: hydrated, done?: done? and not truncated?}}
      end
    end
  end

  def catalog_page(_ctx, _shard_index, _type, _target_generation, _limit, _max_bytes),
    do: {:error, :invalid_policy_migration_catalog_page}

  @spec score(non_neg_integer()) :: non_neg_integer()
  def score(generation)
      when is_integer(generation) and generation >= 0 and generation <= @max_exact_score,
      do: generation

  defp backfill_candidate(ctx, shard_index, key) do
    cond do
      Keys.policy_migration_job_key?(key) ->
        {:ok, [%{kind: :job, job_key: key}]}

      Keys.type_catalog_member_key?(key) ->
        case Keys.type_catalog_descriptor_key_from_member(key) do
          {:ok, descriptor_key} ->
            {:ok, [%{kind: :catalog, catalog_key: key, descriptor_key: descriptor_key}]}

          :error ->
            {:ok, []}
        end

      Keys.state_key?(key) ->
        with {:ok, record_value} <- state_record_value(ctx, shard_index, key) do
          {:ok, [%{kind: :state, state_key: key, record_value: record_value}]}
        end

      true ->
        {:ok, []}
    end
  end

  defp staged_backfill_primary_key(stage_key, prefix) do
    case stage_key do
      <<^prefix::binary-size(byte_size(prefix)), primary_key::binary>> when primary_key != "" ->
        {:ok, primary_key}

      _invalid ->
        {:error, :corrupt_policy_catalog_staging_key}
    end
  end

  defp hydrate_backfill_entries(ctx, shard_index, entries, prefix, max_bytes) do
    Enum.reduce_while(entries, {:ok, [], nil, 0}, fn {stage_key, _value},
                                                     {:ok, acc, scanned_to, bytes} ->
      with {:ok, primary_key} <- staged_backfill_primary_key(stage_key, prefix),
           {:ok, candidates} <- backfill_candidate(ctx, shard_index, primary_key) do
        candidate_bytes = :erlang.external_size(candidates)

        if scanned_to != nil and bytes + candidate_bytes > max_bytes do
          {:halt, {:ok, acc, scanned_to, bytes}}
        else
          {:cont, {:ok, Enum.reverse(candidates, acc), stage_key, bytes + candidate_bytes}}
        end
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, candidates, scanned_to, _bytes} when is_binary(scanned_to) ->
        {:ok, scanned_to, Enum.reverse(candidates)}

      {:error, _reason} = error ->
        error
    end
  end

  defp hydrate_catalog_entries(ctx, shard_index, type, entries, max_bytes) do
    Enum.reduce_while(entries, {:ok, [], 0, false}, fn entry, {:ok, acc, bytes, false} ->
      case hydrate_catalog_entry(ctx, shard_index, type, entry) do
        {:ok, hydrated} ->
          entry_bytes = :erlang.external_size(hydrated)

          if acc != [] and bytes + entry_bytes > max_bytes do
            {:halt, {:ok, acc, bytes, true}}
          else
            {:cont, {:ok, [hydrated | acc], bytes + entry_bytes, false}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed, _bytes, truncated?} -> {:ok, Enum.reverse(reversed), truncated?}
      {:error, _reason} = error -> error
    end
  end

  defp hydrate_catalog_entry(ctx, shard_index, type, %{catalog_key: catalog_key} = entry) do
    case Router.read_shard_value(ctx, shard_index, catalog_key) do
      {:ok, nil} ->
        {:ok, Map.put(entry, :record_value, nil)}

      {:ok, catalog_value} when is_binary(catalog_value) ->
        with {:ok, catalog} <- decode_catalog(catalog_value),
             true <- catalog_owned?(catalog, type, catalog_key) do
          if catalog.migration_generation > entry.migration_generation do
            {:ok, Map.put(entry, :record_value, nil)}
          else
            with {:ok, record_value} <- state_record_value(ctx, shard_index, catalog.state_key) do
              {:ok, Map.put(entry, :record_value, record_value)}
            end
          end
        else
          false -> {:error, :corrupt_policy_catalog_primary}
          :error -> {:error, :corrupt_policy_catalog_primary}
          {:error, _reason} = error -> error
        end

      :unavailable ->
        {:error, :policy_catalog_primary_unavailable}

      _invalid ->
        {:error, :corrupt_policy_catalog_primary}
    end
  end

  defp catalog_owned?(catalog, type, catalog_key) do
    Keys.type_catalog_member_owns_state_key?(catalog_key, catalog.state_key) and
      Keys.type_catalog_member_key(type, catalog.state_key) == catalog_key
  end

  defp state_record_value(ctx, shard_index, state_key) do
    case Router.read_shard_value(ctx, shard_index, state_key) do
      {:ok, value} when is_binary(value) ->
        validate_state_record_value(value, state_key)

      {:ok, nil} ->
        state_record_value_from_lmdb(ctx, shard_index, state_key)

      :unavailable ->
        {:error, :policy_catalog_primary_unavailable}

      _invalid ->
        {:error, :corrupt_policy_catalog_state_record}
    end
  end

  defp state_record_value_from_lmdb(ctx, shard_index, state_key) do
    path = lmdb_path(ctx, shard_index)

    case LMDB.get(path, state_key) do
      {:ok, blob} ->
        case LMDB.decode_value(blob, System.system_time(:millisecond)) do
          {:ok, value} when is_binary(value) -> validate_state_record_value(value, state_key)
          :expired -> missing_state_record_value(ctx, shard_index, state_key)
          _invalid -> {:error, :corrupt_policy_catalog_state_projection}
        end

      :not_found ->
        state_record_value_from_cold_park(ctx, shard_index, state_key)

      {:error, _reason} = error ->
        error
    end
  end

  defp state_record_value_from_cold_park(ctx, shard_index, state_key) do
    path = lmdb_path(ctx, shard_index)
    park_key = LMDB.cold_park_key_for_state_key(state_key)

    case LMDB.get(path, park_key) do
      {:ok, park_blob} ->
        case LMDB.decode_cold_park(park_blob) do
          {:ok, %{state_value: value}} when is_binary(value) ->
            validate_state_record_value(value, state_key)

          {:ok, _park_without_value} ->
            {:error, :policy_catalog_cold_state_projection_pending}

          _invalid ->
            {:error, :corrupt_policy_catalog_cold_state_projection}
        end

      :not_found ->
        missing_state_record_value(ctx, shard_index, state_key)

      {:error, _reason} = error ->
        error
    end
  end

  defp missing_state_record_value(ctx, shard_index, state_key) do
    with {:ok, registry_key} <- Keys.registry_key_from_state_key(state_key) do
      case Router.read_shard_value(ctx, shard_index, registry_key) do
        {:ok, nil} -> {:ok, nil}
        {:ok, _registry} -> {:error, :policy_catalog_state_projection_pending}
        :unavailable -> {:error, :policy_catalog_primary_unavailable}
      end
    else
      :error -> {:error, :invalid_policy_catalog_state_key}
    end
  end

  defp validate_state_record_value(value, state_key) do
    record = Flow.decode_record(value)

    if is_map(record) and is_binary(Map.get(record, :id)) and
         Keys.state_key(record.id, Map.get(record, :partition_key)) == state_key do
      {:ok, value}
    else
      {:error, :corrupt_policy_catalog_state_record}
    end
  rescue
    _error -> {:error, :corrupt_policy_catalog_state_record}
  end

  defp snapshot_primary_pages(_keydir, _path, _prefix, _max_bytes, :"$end_of_table"),
    do: :ok

  defp snapshot_primary_pages(keydir, path, prefix, max_bytes, {keys, continuation}) do
    ops =
      keys
      |> Enum.flat_map(&backfill_staging_keys/1)
      |> Enum.uniq()
      |> Enum.map(&{:put, prefix <> &1, <<>>})

    with :ok <- write_snapshot_ops(path, ops, max_bytes) do
      snapshot_primary_pages(keydir, path, prefix, max_bytes, :ets.select(continuation))
    end
  end

  defp write_snapshot_ops(_path, [], _max_bytes), do: :ok

  defp write_snapshot_ops(path, ops, max_bytes) do
    ops
    |> chunk_snapshot_ops(max_bytes)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      case LMDB.write_batch(path, chunk) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp chunk_snapshot_ops(ops, max_bytes) do
    {chunks, current, _bytes} =
      Enum.reduce(ops, {[], [], 0}, fn {:put, key, value} = op, {chunks, current, bytes} ->
        op_bytes = byte_size(key) + byte_size(value)

        if current != [] and bytes + op_bytes > max_bytes do
          {[Enum.reverse(current) | chunks], [op], op_bytes}
        else
          {chunks, [op | current], bytes + op_bytes}
        end
      end)

    [Enum.reverse(current) | chunks]
    |> Enum.reject(&(&1 == []))
    |> Enum.reverse()
  end

  defp cleanup_snapshot_pages(path, prefix) do
    case LMDB.prefix_entries(path, prefix, 256) do
      {:ok, []} ->
        :ok

      {:ok, entries} ->
        case LMDB.write_batch(
               path,
               Enum.map(entries, fn {key, _value} -> {:delete, key} end)
             ) do
          :ok -> cleanup_snapshot_pages(path, prefix)
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp backfill_staging_keys(key) do
    cond do
      Keys.state_key?(key) or Keys.type_catalog_member_key?(key) or
          Keys.policy_migration_job_key?(key) ->
        [key]

      Keys.registry_key?(key) ->
        case Keys.state_key_from_registry_key(key) do
          {:ok, state_key} -> [state_key]
          :error -> []
        end

      true ->
        []
    end
  end

  defp staging_prefix(run_token), do: @staging_root <> run_token <> <<0>>
  defp staging_manifest_key(run_token), do: @staging_manifest_root <> run_token

  defp encode_work_cursor(run_token, after_key) do
    <<@work_cursor::binary, byte_size(run_token)::unsigned-big-8, run_token::binary,
      after_key::binary>>
  end

  defp decode_work_cursor(<<@work_cursor::binary, token_size::unsigned-big-8, payload::binary>>)
       when token_size > 0 and token_size <= 64 and byte_size(payload) >= token_size do
    <<run_token::binary-size(token_size), after_key::binary>> = payload
    prefix = staging_prefix(run_token)

    case after_key do
      "" -> {:ok, run_token, after_key}
      <<^prefix::binary, _primary_key::binary>> -> {:ok, run_token, after_key}
      _invalid -> {:error, :invalid_policy_catalog_work_cursor}
    end
  end

  defp decode_work_cursor(_cursor), do: {:error, :invalid_policy_catalog_work_cursor}

  defp decode_catalog_projection_entries(type, entries) do
    Enum.reduce_while(entries, {:ok, []}, fn {key, _value}, {:ok, acc} ->
      case Keys.decode_policy_catalog_projection_key(type, key) do
        {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
        :error -> {:halt, {:error, :corrupt_policy_catalog_projection}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_mirror_value(encoded_value) when is_binary(encoded_value) do
    case LMDB.decode_value(encoded_value, 0) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      _invalid -> :error
    end
  end

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end

  defp decode_indexed_state_meta(0, ""), do: {:ok, nil}
  defp decode_indexed_state_meta(1, indexed) when indexed != "", do: {:ok, indexed}
  defp decode_indexed_state_meta(_tag, _indexed), do: :error
end
