defmodule Ferricstore.Store.CompactionTombstoneCatalog do
  @moduledoc false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.TermCodec

  @release_timeout_ms 1_000

  @type t :: %{path: binary()}
  @type record ::
          {binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()}

  @spec open(binary(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  def open(shard_path, fid)
      when is_binary(shard_path) and is_integer(fid) and fid >= 0 do
    path = Path.join(shard_path, "compaction_tombstones_#{fid}")

    with :ok <- remove_path(path),
         :ok <- Ferricstore.FS.mkdir_p(path) do
      {:ok, %{path: path}}
    else
      {:error, {:catalog_release_failed, _reason}} = error -> error
      {:error, {:catalog_remove_failed, _path, _reason}} = error -> error
      {:error, reason} -> {:error, {:catalog_create_failed, reason}}
    end
  end

  @doc false
  @spec remove_path(binary()) :: :ok | {:error, term()}
  def remove_path(path) when is_binary(path) do
    with :ok <- release_catalog(path),
         :ok <- Ferricstore.FS.rm_rf(path) do
      :ok
    else
      {:error, {:catalog_release_failed, _reason}} = error -> error
      {:error, reason} -> {:error, {:catalog_remove_failed, path, reason}}
    end
  end

  defp release_catalog(path) do
    if Ferricstore.FS.dir?(path) do
      case LMDB.release(path, @release_timeout_ms) do
        :ok -> :ok
        {:error, reason} -> {:error, {:catalog_release_failed, reason}}
      end
    else
      :ok
    end
  end

  @spec record_source_page(t(), [record()]) :: :ok | {:error, term()}
  def record_source_page(%{path: path}, records) when is_list(records) do
    case record_source_page_count(%{path: path}, records) do
      {:ok, _new_candidates} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @spec record_source_page_count(t(), [record()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def record_source_page_count(%{path: path}, records) when is_list(records) do
    with {:ok, candidates} <- source_candidates(records),
         keys = Map.keys(candidates),
         {:ok, existing} <- LMDB.get_many(path, keys),
         {:ok, new_candidates} <- validate_existing_keys(keys, existing, candidates),
         ops =
           Enum.map(candidates, fn {catalog_key, {key, offset}} ->
             {:put, catalog_key, encode_candidate(key, offset, :missing, nil)}
           end),
         :ok <- LMDB.write_batch(path, ops) do
      {:ok, new_candidates}
    end
  end

  @spec observe_lower_page(t(), [record()]) :: :ok | {:error, term()}
  def observe_lower_page(%{path: path}, records) when is_list(records) do
    case observe_lower_page_count(%{path: path}, records, 0) do
      {:ok, _newly_resolved} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @spec observe_lower_page_count(t(), [record()], non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def observe_lower_page_count(%{path: path}, records, file_id)
      when is_list(records) and is_integer(file_id) and file_id >= 0 do
    keys = unique_catalog_keys(records)

    with {:ok, values} <- LMDB.get_many(path, keys),
         {:ok, candidates} <- decode_candidates(keys, values),
         {:ok, updates, newly_resolved} <- lower_page_updates(records, candidates, file_id),
         ops =
           Enum.map(updates, fn {catalog_key, {key, offset, state, resolved_fid}} ->
             {:put, catalog_key, encode_candidate(key, offset, state, resolved_fid)}
           end),
         :ok <- LMDB.write_batch(path, ops) do
      {:ok, newly_resolved}
    end
  end

  @spec needed_offsets(t(), [record()]) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def needed_offsets(%{path: path}, records) when is_list(records) do
    tombstones = Enum.filter(records, &tombstone_record?/1)
    keys = unique_catalog_keys(tombstones)

    with {:ok, values} <- LMDB.get_many(path, keys),
         {:ok, candidates} <- decode_candidates(keys, values) do
      tombstones
      |> Enum.reduce_while({:ok, []}, fn {key, offset, _size, _expire_at_ms, true}, {:ok, acc} ->
        case Map.get(candidates, catalog_key(key)) do
          {^key, ^offset, :live, _resolved_fid} ->
            {:cont, {:ok, [offset | acc]}}

          {^key, _other_offset, _state, _resolved_fid} ->
            {:cont, {:ok, acc}}

          nil ->
            {:cont, {:ok, acc}}

          {_colliding_key, _offset, _state, _resolved_fid} ->
            {:halt, {:error, :catalog_key_collision}}
        end
      end)
      |> case do
        {:ok, offsets} -> {:ok, Enum.reverse(offsets)}
        {:error, _reason} = error -> error
      end
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(%{path: path}), do: remove_path(path)

  defp source_candidates(records) do
    Enum.reduce_while(records, {:ok, %{}}, fn
      {key, offset, _size, _expire_at_ms, true}, {:ok, acc}
      when is_binary(key) and is_integer(offset) and offset >= 0 ->
        catalog_key = catalog_key(key)

        case Map.get(acc, catalog_key) do
          nil -> {:cont, {:ok, Map.put(acc, catalog_key, {key, offset})}}
          {^key, _old_offset} -> {:cont, {:ok, Map.put(acc, catalog_key, {key, offset})}}
          {_other_key, _old_offset} -> {:halt, {:error, :catalog_key_collision}}
        end

      {_key, _offset, _size, _expire_at_ms, false}, acc ->
        {:cont, acc}

      invalid, _acc ->
        {:halt, {:error, {:invalid_catalog_record, invalid}}}
    end)
  end

  defp validate_existing_keys(keys, values, candidates) when length(keys) == length(values) do
    keys
    |> Enum.zip(values)
    |> Enum.reduce_while(:ok, fn
      {_catalog_key, :not_found}, :ok ->
        {:cont, :ok}

      {catalog_key, {:ok, blob}}, :ok ->
        with {:ok, {existing_key, _offset, _state, _resolved_fid}} <- decode_candidate(blob),
             {^existing_key, _new_offset} <- Map.fetch!(candidates, catalog_key) do
          {:cont, :ok}
        else
          _ -> {:halt, {:error, :catalog_key_collision}}
        end

      {_catalog_key, {:error, reason}}, :ok ->
        {:halt, {:error, reason}}

      {_catalog_key, invalid}, :ok ->
        {:halt, {:error, {:invalid_catalog_read, invalid}}}
    end)
    |> case do
      :ok -> {:ok, Enum.count(values, &(&1 == :not_found))}
      {:error, _reason} = error -> error
    end
  end

  defp validate_existing_keys(_keys, _values, _candidates),
    do: {:error, :catalog_result_count_mismatch}

  defp unique_catalog_keys(records) do
    records
    |> Enum.map(fn {key, _offset, _size, _expire_at_ms, _tombstone?} -> catalog_key(key) end)
    |> Enum.uniq()
  end

  defp decode_candidates(keys, values) when length(keys) == length(values) do
    keys
    |> Enum.zip(values)
    |> Enum.reduce_while({:ok, %{}}, fn
      {_catalog_key, :not_found}, {:ok, acc} ->
        {:cont, {:ok, acc}}

      {catalog_key, {:ok, blob}}, {:ok, acc} ->
        case decode_candidate(blob) do
          {:ok, candidate} -> {:cont, {:ok, Map.put(acc, catalog_key, candidate)}}
          {:error, _reason} = error -> {:halt, error}
        end

      {_catalog_key, {:error, reason}}, _acc ->
        {:halt, {:error, reason}}

      {_catalog_key, invalid}, _acc ->
        {:halt, {:error, {:invalid_catalog_read, invalid}}}
    end)
  end

  defp decode_candidates(_keys, _values), do: {:error, :catalog_result_count_mismatch}

  defp lower_page_updates(records, candidates, file_id) do
    now_ms = Ferricstore.HLC.now_ms()

    records
    |> Enum.reduce_while({:ok, %{}, MapSet.new()}, fn
      {key, _offset, _size, expire_at_ms, tombstone?} = record,
      {:ok, acc, newly_resolved} = unchanged
      when is_binary(key) and is_integer(expire_at_ms) and is_boolean(tombstone?) ->
        catalog_key = catalog_key(key)

        candidate = Map.get(acc, catalog_key, Map.get(candidates, catalog_key))

        case candidate do
          nil ->
            {:cont, unchanged}

          {^key, source_offset, _old_state, resolved_fid}
          when resolved_fid == nil or resolved_fid == file_id ->
            state = dependency_state(record, now_ms)

            next_newly_resolved =
              if resolved_fid == nil,
                do: MapSet.put(newly_resolved, catalog_key),
                else: newly_resolved

            {:cont,
             {:ok, Map.put(acc, catalog_key, {key, source_offset, state, file_id}),
              next_newly_resolved}}

          {^key, _source_offset, _old_state, _newer_fid} ->
            {:cont, unchanged}

          {_colliding_key, _source_offset, _old_state, _resolved_fid} ->
            {:halt, {:error, :catalog_key_collision}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_catalog_record, invalid}}}
    end)
    |> case do
      {:ok, updates, newly_resolved} -> {:ok, updates, MapSet.size(newly_resolved)}
      {:error, _reason} = error -> error
    end
  end

  defp dependency_state({_key, _offset, _size, _expire_at_ms, true}, _now_ms),
    do: :tombstone

  defp dependency_state({_key, _offset, _size, 0, false}, _now_ms), do: :live

  defp dependency_state({_key, _offset, _size, expire_at_ms, false}, now_ms)
       when expire_at_ms > now_ms,
       do: :live

  defp dependency_state(_record, _now_ms), do: :expired

  defp tombstone_record?({_key, _offset, _size, _expire_at_ms, true}), do: true
  defp tombstone_record?(_record), do: false

  defp catalog_key(key), do: <<1, :crypto.hash(:sha256, key)::binary>>

  defp encode_candidate(key, offset, state, resolved_fid),
    do: TermCodec.encode({:compaction_tombstone, 1, key, offset, state, resolved_fid})

  defp decode_candidate(blob) when is_binary(blob) do
    with {:ok, term} <- TermCodec.decode(blob) do
      case term do
        {:compaction_tombstone, 1, key, offset, state, resolved_fid}
        when is_binary(key) and is_integer(offset) and offset >= 0 and
               state in [:missing, :live, :tombstone, :expired] and
               (resolved_fid == nil or (is_integer(resolved_fid) and resolved_fid >= 0)) ->
          {:ok, {key, offset, state, resolved_fid}}

        _invalid ->
          {:error, :invalid_catalog_value}
      end
    else
      {:error, :invalid_external_term} -> {:error, :invalid_catalog_value}
    end
  end
end
