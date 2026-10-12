defmodule Ferricstore.Flow.KeydirSpill do
  @moduledoc false

  # Bookkeeping rows of pruned terminal Flows can leave the keydir: the type
  # catalog member (`tc`) and the retention cleanup members (`rtm`). Their copy
  # lives in LMDB under prefix/1 and is written only by the LMDB writer, from a
  # keydir row whose apply-projection frame is durable, so the copy is never
  # newer than what crash recovery restores. Apply reads fall back to it on a
  # keydir miss; apply deletes leave a keydir tombstone until the copy is gone.
  # Spilled cleanup members stay in the in-RAM cleanup index, and startup
  # registers them again from LMDB (Ferricstore.Flow.SharedRefBackfill).
  #
  # Spilling is off by default. Enable it only when every node runs a version
  # that reads spilled rows: an older node would read a spilled row as absent.

  alias Ferricstore.Flow.{Keys, LMDB, LMDBWriter, NativeOrderedIndex}
  alias Ferricstore.Raft.WARaftSegmentReader

  @prefix "flow-keydir-spill:"
  # Bounds the rows taken from one pruned Flow; the rest stay in the keydir.
  @max_cleanup_members_per_flow 64

  @spec enabled?() :: boolean()
  def enabled?, do: Application.get_env(:ferricstore, :flow_keydir_spill, false) == true

  @doc false
  # Spill keys are grouped by row family so startup can scan one family.
  @spec prefix() :: binary()
  def prefix, do: @prefix

  @spec prefix(:type_catalog_member | :retention_cleanup_member) :: binary()
  def prefix(:type_catalog_member), do: @prefix <> "tc:"
  def prefix(:retention_cleanup_member), do: @prefix <> "rtm:"

  @spec key(binary()) :: binary()
  def key(source_key) when is_binary(source_key), do: prefix(family!(source_key)) <> source_key

  @spec spillable?(binary()) :: boolean()
  def spillable?(key) when is_binary(key), do: family(key) != nil
  def spillable?(_key), do: false

  defp family(key) do
    cond do
      Keys.type_catalog_member_key?(key) -> :type_catalog_member
      Keys.retention_cleanup_member?(key) -> :retention_cleanup_member
      true -> nil
    end
  end

  defp family!(key) do
    case family(key) do
      nil -> raise ArgumentError, "not a spillable keydir key: #{inspect(key)}"
      family -> family
    end
  end

  @doc false
  # Called after a terminal Flow's state row left the keydir. Sends the Flow's
  # hot, durable spillable rows to the shard's LMDB writer. At flush time the
  # writer copies each row that is still unchanged into LMDB, and after the flush
  # drops it from the keydir if it is still unchanged.
  @spec request(map(), non_neg_integer(), :ets.tid() | atom(), map(), binary()) :: :ok
  def request(
        %{name: instance_name, data_dir: data_dir},
        shard_index,
        ets,
        %{type: type, id: id} = record,
        state_key
      )
      when is_atom(instance_name) and is_binary(data_dir) and is_integer(shard_index) and
             shard_index >= 0 and is_binary(type) and type != "" and is_binary(id) and
             is_binary(state_key) do
    if enabled?() do
      # Set before choosing rows: from here on apply deletes of these keys leave a
      # tombstone and remove the spilled copy, whichever order the writer sees.
      :ok = mark_present(instance_name, shard_index)

      cleanup_index_key = Keys.retention_cleanup_index_key(id, Map.get(record, :partition_key))

      rows =
        [Keys.type_catalog_member_key(type, state_key)]
        |> Enum.concat(cleanup_member_keys(instance_name, shard_index, cleanup_index_key))
        |> Enum.flat_map(&spillable_row(data_dir, shard_index, ets, &1))

      with [_ | _] <- rows,
           pid when is_pid(pid) <- Process.whereis(LMDBWriter.name(instance_name, shard_index)) do
        send(pid, {:keydir_spill, shard_index, ets, rows})
      end
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  def request(_instance_ctx, _shard_index, _ets, _record, _state_key), do: :ok

  defp cleanup_member_keys(instance_name, shard_index, cleanup_index_key) do
    {index_table, lookup_table} = NativeOrderedIndex.table_names(instance_name, shard_index)

    case NativeOrderedIndex.get(index_table, lookup_table) do
      nil ->
        []

      native ->
        native
        |> NativeOrderedIndex.range_slice(
          cleanup_index_key,
          :neg_inf,
          :inf,
          false,
          0,
          @max_cleanup_members_per_flow
        )
        |> Enum.map(fn {member_key, _score} -> member_key end)
        |> Enum.filter(&Keys.retention_cleanup_member?/1)
    end
  end

  defp spillable_row(data_dir, shard_index, ets, key) do
    case :ets.lookup(ets, key) do
      [
        {^key, value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset, _size} =
            row
      ]
      when is_binary(value) and is_integer(index) and index > 0 ->
        case WARaftSegmentReader.ensure_apply_projection_entries_durable(
               data_dir,
               shard_index,
               [{index, key}]
             ) do
          {:ok, _removed} -> [row]
          {:error, _reason} -> []
        end

      _missing_cold_or_pending ->
        []
    end
  end

  @doc false
  # Reads outside Raft apply. `:keydir` means the keydir holds the row (or the
  # key is not spillable) and the caller reads it as usual.
  @spec read_local(map(), non_neg_integer(), binary()) ::
          :keydir | {:ok, binary() | nil} | {:error, term()}
  def read_local(
        %{name: instance_name, data_dir: data_dir, keydir_refs: keydir_refs},
        shard_index,
        key
      )
      when is_integer(shard_index) and shard_index >= 0 and is_binary(key) do
    if spillable?(key) and present?(instance_name, shard_index) do
      case :ets.lookup(elem(keydir_refs, shard_index), key) do
        [] -> read_spilled(data_dir, shard_index, key)
        [{^key, nil, 0, :flow_state_deleted, :deleted, _offset, _size}] -> {:ok, nil}
        _row -> :keydir
      end
    else
      :keydir
    end
  rescue
    ArgumentError -> :keydir
  end

  def read_local(_instance_ctx, _shard_index, _key), do: :keydir

  @doc false
  # Batch form of read_local/3: keydir-held keys are read with `keydir_read`,
  # which receives those keys in order and returns `{:ok, values}`.
  @spec read_values(map(), non_neg_integer(), [binary()], ([binary()] -> term())) ::
          {:ok, [binary() | nil]} | term()
  def read_values(instance_ctx, shard_index, keys, keydir_read)
      when is_list(keys) and is_function(keydir_read, 1) do
    resolved = Enum.map(keys, &read_local(instance_ctx, shard_index, &1))

    case Enum.find(resolved, &match?({:error, _reason}, &1)) do
      nil ->
        keydir_keys = for {key, :keydir} <- Enum.zip(keys, resolved), do: key

        with {:ok, values} when length(values) == length(keydir_keys) <-
               read_keydir_values(keydir_keys, keydir_read) do
          {:ok, merge_values(resolved, values, [])}
        end

      error ->
        error
    end
  end

  defp read_keydir_values([], _keydir_read), do: {:ok, []}
  defp read_keydir_values(keys, keydir_read), do: keydir_read.(keys)

  defp merge_values([], _keydir_values, acc), do: Enum.reverse(acc)

  defp merge_values([:keydir | rest], [value | keydir_values], acc),
    do: merge_values(rest, keydir_values, [value | acc])

  defp merge_values([{:ok, value} | rest], keydir_values, acc),
    do: merge_values(rest, keydir_values, [value | acc])

  defp read_spilled(data_dir, shard_index, key) do
    path = data_dir |> Ferricstore.DataDir.shard_data_path(shard_index) |> LMDB.path()

    case LMDB.get(path, key(key)) do
      {:ok, encoded} ->
        case LMDB.decode_value(encoded, System.system_time(:millisecond)) do
          {:ok, value} -> {:ok, value}
          :expired -> {:ok, nil}
          :error -> {:error, {:corrupt_keydir_spill, key}}
        end

      :not_found ->
        {:ok, nil}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec present?(atom(), non_neg_integer()) :: boolean()
  def present?(instance_name, shard_index),
    do: :persistent_term.get(present_key(instance_name, shard_index), false)

  @doc false
  @spec mark_present(atom(), non_neg_integer()) :: :ok
  def mark_present(instance_name, shard_index) do
    unless present?(instance_name, shard_index) do
      :persistent_term.put(present_key(instance_name, shard_index), true)
    end

    :ok
  end

  @doc false
  @spec refresh_present(atom(), non_neg_integer(), binary()) :: :ok
  def refresh_present(instance_name, shard_index, lmdb_path) do
    case LMDB.prefix_entries(lmdb_path, @prefix, 1) do
      {:ok, []} ->
        _ = :persistent_term.erase(present_key(instance_name, shard_index))
        :ok

      {:ok, [_row]} ->
        mark_present(instance_name, shard_index)

      # Unknown means the fallback must stay on: a false negative would read a
      # spilled row as absent.
      {:error, _reason} ->
        mark_present(instance_name, shard_index)
    end
  end

  defp present_key(instance_name, shard_index),
    do: {__MODULE__, :present, instance_name, shard_index}
end
