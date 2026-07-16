defmodule Ferricstore.Store.StandaloneTxLog do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.AppendResult
  alias Ferricstore.TermCodec

  @file_name "standalone_cross_shard_tx.log"
  @magic :ferricstore_standalone_cross_shard_tx_v1
  @compact_threshold_bytes 4 * 1_024 * 1_024
  @max_journal_bytes 64 * 1_024 * 1_024
  @terminal_reserve_bytes 1_024
  @max_txid_bytes 128

  @type group :: {binary(), list()}

  @spec prepare(binary(), [group()]) :: {:ok, binary()} | {:error, term()}
  def prepare(data_dir, groups) when is_binary(data_dir) and is_list(groups) do
    if valid_groups?(data_dir, groups) do
      txid = new_txid()
      stats = group_stats(groups)

      case with_journal_lock(data_dir, fn ->
             append_entry_locked(
               data_dir,
               {@magic, :prepare, txid, groups},
               @terminal_reserve_bytes
             )
           end) do
        :ok ->
          observe(:prepare, stats, %{status: :ok})
          {:ok, txid}

        {:error, _reason} = error ->
          observe(:prepare, stats, %{status: :error})
          error
      end
    else
      {:error, :invalid_groups}
    end
  end

  @spec commit(binary(), binary()) :: :ok | {:error, term()}
  def commit(data_dir, txid) when is_binary(data_dir) and is_binary(txid) do
    case append_terminal(data_dir, :commit, txid) do
      :ok ->
        observe(:commit, %{count: 1}, %{status: :ok})
        :ok

      {:error, _reason} = error ->
        observe(:commit, %{count: 1}, %{status: :error})
        error
    end
  end

  @spec abort(binary(), binary()) :: :ok | {:error, term()}
  def abort(data_dir, txid) when is_binary(data_dir) and is_binary(txid) do
    case append_terminal(data_dir, :abort, txid) do
      :ok ->
        observe(:abort, %{count: 1}, %{status: :ok})
        :ok

      {:error, _reason} = error ->
        observe(:abort, %{count: 1}, %{status: :error})
        error
    end
  end

  @spec recover_once(binary()) :: :ok | {:error, term()}
  def recover_once(data_dir) when is_binary(data_dir), do: recover(data_dir)

  @spec recover(binary()) :: :ok | {:error, term()}
  def recover(data_dir) when is_binary(data_dir) do
    result = with_journal_lock(data_dir, fn -> recover_locked(data_dir) end)

    case result do
      {:ok, stats} ->
        observe(:recover, stats, %{status: :ok})
        :ok

      {:error, reason} = error ->
        observe(:recover, %{pending: 0, replayed: 0, groups: 0, ops: 0}, %{
          status: :error,
          reason: inspect(reason)
        })

        error
    end
  end

  defp recover_locked(data_dir) do
    with {:ok, pending} <- read_pending_transactions(data_dir),
         {:ok, stats} <- recover_pending(data_dir, pending),
         :ok <- compact_committed_locked(data_dir) do
      {:ok, stats}
    end
  end

  defp append_terminal(data_dir, terminal, txid) when terminal in [:commit, :abort] do
    if valid_txid?(txid) do
      with_journal_lock(data_dir, fn ->
        persist_terminal_locked(data_dir, terminal, txid, true)
      end)
    else
      {:error, :invalid_txid}
    end
  end

  defp maybe_compact_committed_locked(data_dir) do
    case File.lstat(path(data_dir)) do
      {:ok, %File.Stat{type: :regular, size: size}} when size >= @compact_threshold_bytes ->
        compact_committed_locked(data_dir)

      {:ok, %File.Stat{type: :regular}} ->
        :ok

      {:ok, %File.Stat{type: type}} ->
        {:error, {:unsafe_journal_type, type}}

      {:error, :enoent} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp compact_committed_locked(data_dir) do
    with {:ok, pending} <- read_pending_transactions(data_dir),
         entries <- pending_entries(pending) do
      rewrite_entries(data_dir, entries)
    end
  end

  defp new_txid do
    unique = System.unique_integer([:positive, :monotonic])
    "#{System.system_time(:nanosecond)}-#{unique}"
  end

  defp recover_pending(data_dir, pending) do
    initial_stats = %{pending: length(pending), replayed: 0, groups: 0, ops: 0}

    pending
    |> Enum.reduce_while({:ok, initial_stats}, fn {txid, groups}, {:ok, stats} ->
      group_stats = group_stats(groups)

      case apply_groups(groups) do
        :ok ->
          case persist_terminal_locked(data_dir, :commit, txid, false) do
            :ok ->
              next_stats = %{
                stats
                | replayed: stats.replayed + 1,
                  groups: stats.groups + group_stats.groups,
                  ops: stats.ops + group_stats.ops
              }

              {:cont, {:ok, next_stats}}

            {:error, reason} ->
              {:halt, {:error, {:commit_after_recover_failed, txid, reason}}}
          end

        {:error, reason} ->
          {:halt, {:error, {:recover_tx_failed, txid, reason}}}
      end
    end)
  end

  defp pending_entries(pending),
    do: Enum.map(pending, fn {txid, groups} -> {@magic, :prepare, txid, groups} end)

  defp pending_transactions({order, prepares, terminals}) do
    order
    |> Enum.reverse()
    |> Enum.reject(&MapSet.member?(terminals, &1))
    |> Enum.map(&{&1, Map.fetch!(prepares, &1)})
  end

  defp apply_groups(groups) do
    Enum.reduce_while(groups, :ok, fn {file_path, batch}, :ok ->
      case append_batch_sync(file_path, batch) do
        {:ok, _locations} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {file_path, reason}}}
      end
    end)
  end

  defp append_batch_sync(file_path, batch) do
    with :ok <- Ferricstore.FS.mkdir_p(Path.dirname(file_path)) do
      do_append_batch_sync(file_path, batch)
    end
  end

  defp do_append_batch_sync(file_path, batch) do
    if Enum.any?(batch, &match?({:delete, _, _}, &1)) do
      ops =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {:put, key, value, expire_at_ms}
          {:delete, key, _prob_path} -> {:delete, key}
        end)

      case NIF.v2_append_ops_batch(file_path, ops) do
        {:ok, locations} ->
          with :ok <- AppendResult.validate_operation_locations(locations, ops) do
            {:ok, locations}
          end

        {:error, _reason} = error ->
          error
      end
    else
      puts =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {key, value, expire_at_ms}
        end)

      case NIF.v2_append_batch(file_path, puts) do
        {:ok, locations} ->
          with :ok <- AppendResult.validate_locations(locations, length(puts)) do
            {:ok, Enum.map(locations, fn {offset, value_size} -> {:put, offset, value_size} end)}
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp persist_terminal_locked(data_dir, terminal, txid, compact?) do
    case append_entry_locked(data_dir, {@magic, terminal, txid}) do
      :ok when compact? -> maybe_compact_committed_locked(data_dir)
      :ok -> :ok
      {:error, {:journal_limit_exceeded, _reason}} -> rewrite_terminal_locked(data_dir, txid)
      {:error, _reason} = error -> error
    end
  end

  defp rewrite_terminal_locked(data_dir, txid) do
    with {:ok, pending} <- read_pending_transactions(data_dir) do
      pending
      |> Enum.reject(fn {pending_txid, _groups} -> pending_txid == txid end)
      |> pending_entries()
      |> then(&rewrite_entries(data_dir, &1))
    end
  end

  defp append_entry_locked(data_dir, entry, reserve_bytes \\ 0) do
    path = path(data_dir)
    dir = Path.dirname(path)
    line = encode_entry(entry) <> "\n"
    append_limit = @max_journal_bytes - reserve_bytes

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, line, append_limit),
         :ok <- fsync_dir(dir) do
      :ok
    else
      {:error, {:too_large, reason}} -> {:error, {:journal_limit_exceeded, reason}}
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp rewrite_entries(data_dir, []) do
    path = path(data_dir)
    dir = Path.dirname(path)

    case Ferricstore.FS.rm(path) do
      :ok -> fsync_dir(dir)
      {:error, {:not_found, _}} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp rewrite_entries(data_dir, entries) do
    path = path(data_dir)
    dir = Path.dirname(path)

    data =
      entries |> Enum.map(fn entry -> [encode_entry(entry), "\n"] end) |> IO.iodata_to_binary()

    with :ok <- Ferricstore.FS.mkdir_p(dir),
         :ok <- Ferricstore.FS.atomic_replace_nofollow(path, data, @max_journal_bytes),
         :ok <- fsync_dir(dir) do
      :ok
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp read_pending_transactions(data_dir) do
    path = path(data_dir)

    case Ferricstore.FS.read_nofollow(path, @max_journal_bytes) do
      {:ok, contents} ->
        {pending_state, skipped} = reduce_journal(contents, data_dir, {[], %{}, MapSet.new()}, 0)

        if skipped > 0 do
          observe(:corrupt_entry, %{count: skipped}, %{data_dir_hash: :erlang.phash2(data_dir)})
          {:error, {:corrupt_entries, skipped}}
        else
          {:ok, pending_transactions(pending_state)}
        end

      {:error, {:not_found, _reason}} ->
        {:ok, []}

      {:error, _reason} = error ->
        error
    end
  end

  defp reduce_journal(<<>>, _data_dir, pending_state, skipped),
    do: {pending_state, skipped}

  defp reduce_journal(contents, data_dir, pending_state, skipped) do
    {line, rest} = take_journal_line(contents)

    case trim_line_ending(line) do
      "" ->
        reduce_journal(rest, data_dir, pending_state, skipped)

      encoded ->
        case decode_line(encoded, data_dir) do
          {:ok, entry} ->
            reduce_journal(rest, data_dir, accumulate_pending(entry, pending_state), skipped)

          :error ->
            reduce_journal(rest, data_dir, pending_state, skipped + 1)
        end
    end
  end

  defp take_journal_line(contents) do
    case :binary.match(contents, "\n") do
      {newline_offset, 1} ->
        line = binary_part(contents, 0, newline_offset + 1)
        rest_offset = newline_offset + 1
        rest = binary_part(contents, rest_offset, byte_size(contents) - rest_offset)
        {line, rest}

      :nomatch ->
        {contents, <<>>}
    end
  end

  defp accumulate_pending(
         {@magic, :prepare, txid, groups},
         {order, prepares, terminals}
       ) do
    if Map.has_key?(prepares, txid) do
      {order, Map.put(prepares, txid, groups), terminals}
    else
      {[txid | order], Map.put(prepares, txid, groups), terminals}
    end
  end

  defp accumulate_pending(
         {@magic, terminal, txid},
         {order, prepares, terminals}
       )
       when terminal in [:commit, :abort],
       do: {order, Map.delete(prepares, txid), MapSet.put(terminals, txid)}

  defp trim_line_ending(line) when is_binary(line) do
    line
    |> trim_last_byte(?\n)
    |> trim_last_byte(?\r)
  end

  defp trim_last_byte(<<>>, _byte), do: <<>>

  defp trim_last_byte(binary, byte) do
    size = byte_size(binary)

    if :binary.at(binary, size - 1) == byte do
      binary_part(binary, 0, size - 1)
    else
      binary
    end
  end

  defp encode_entry(entry), do: Base.encode64(TermCodec.encode(entry))

  defp decode_line(line, data_dir) do
    with {:ok, binary} <- Base.decode64(line),
         {:ok, term} <- TermCodec.decode(binary),
         true <- valid_entry?(term, data_dir) do
      {:ok, term}
    else
      _ -> :error
    end
  end

  defp valid_entry?({@magic, :prepare, txid, groups}, data_dir)
       when is_binary(txid) and is_list(groups),
       do: valid_txid?(txid) and valid_groups?(data_dir, groups)

  defp valid_entry?({@magic, terminal, txid}, _data_dir)
       when terminal in [:commit, :abort] and is_binary(txid),
       do: valid_txid?(txid)

  defp valid_entry?(_other, _data_dir), do: false

  defp valid_txid?(txid),
    do: is_binary(txid) and byte_size(txid) > 0 and byte_size(txid) <= @max_txid_bytes

  defp valid_groups?(data_dir, groups) do
    groups != [] and Enum.all?(groups, &valid_group?(&1, data_dir))
  end

  defp valid_group?({file_path, batch}, data_dir)
       when is_binary(file_path) and is_list(batch) and batch != [],
       do: path_within_data_dir?(file_path, data_dir) and Enum.all?(batch, &valid_batch_op?/1)

  defp valid_group?(_other, _data_dir), do: false

  defp path_within_data_dir?(file_path, data_dir) do
    expanded_root = Path.expand(data_dir)
    expanded_path = Path.expand(file_path)
    expanded_path != expanded_root and String.starts_with?(expanded_path, expanded_root <> "/")
  end

  defp valid_batch_op?({:put, key, value, expire_at_ms})
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
              expire_at_ms >= 0,
       do: true

  defp valid_batch_op?({:put_cold, key, value, expire_at_ms, _lfu})
       when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
              expire_at_ms >= 0,
       do: true

  defp valid_batch_op?({:delete, key, _prob_path}) when is_binary(key), do: true
  defp valid_batch_op?(_other), do: false

  defp path(data_dir), do: Path.join(data_dir, @file_name)

  defp group_stats(groups) do
    %{
      groups: length(groups),
      ops:
        Enum.reduce(groups, 0, fn
          {_file_path, batch}, acc when is_list(batch) -> acc + length(batch)
          _other, acc -> acc
        end)
    }
  end

  defp observe(event, measurements, metadata) do
    :telemetry.execute([:ferricstore, :standalone_tx_log, event], measurements, metadata)
  end

  defp fsync_dir(path) do
    case Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook) do
      hook when is_function(hook, 1) -> hook.(path)
      _ -> NIF.v2_fsync_dir(path)
    end
  end

  defp with_journal_lock(data_dir, fun) when is_function(fun, 0) do
    lock = {{__MODULE__, Path.expand(data_dir)}, self()}

    case :global.trans(lock, fun, [node()]) do
      {:aborted, reason} -> {:error, {:journal_lock_failed, reason}}
      result -> result
    end
  end
end
