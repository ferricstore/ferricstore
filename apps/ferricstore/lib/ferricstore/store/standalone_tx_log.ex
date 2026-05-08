defmodule Ferricstore.Store.StandaloneTxLog do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @file_name "standalone_cross_shard_tx.log"
  @magic :ferricstore_standalone_cross_shard_tx_v1

  @type group :: {binary(), list()}

  @spec prepare(binary(), [group()]) :: {:ok, binary()} | {:error, term()}
  def prepare(data_dir, groups) when is_binary(data_dir) and is_list(groups) do
    txid = new_txid()
    stats = group_stats(groups)

    case append_entry(data_dir, {@magic, :prepare, txid, groups}) do
      :ok ->
        observe(:prepare, stats, %{status: :ok})
        forget_recover_once(data_dir)
        {:ok, txid}

      {:error, _reason} = error ->
        observe(:prepare, stats, %{status: :error})
        error
    end
  end

  @spec commit(binary(), binary()) :: :ok | {:error, term()}
  def commit(data_dir, txid) when is_binary(data_dir) and is_binary(txid) do
    case append_entry(data_dir, {@magic, :commit, txid}) do
      :ok ->
        observe(:commit, %{count: 1}, %{status: :ok})
        _ = compact_committed(data_dir)
        :ok

      {:error, _reason} = error ->
        observe(:commit, %{count: 1}, %{status: :error})
        error
    end
  end

  @spec recover_once(binary()) :: :ok | {:error, term()}
  def recover_once(data_dir) when is_binary(data_dir) do
    key = recover_once_key(data_dir)

    case :persistent_term.get(key, false) do
      true ->
        :ok

      false ->
        case recover(data_dir) do
          :ok ->
            :persistent_term.put(key, true)
            :ok

          {:error, _reason} = error ->
            error
        end
    end
  end

  @spec recover(binary()) :: :ok | {:error, term()}
  def recover(data_dir) when is_binary(data_dir) do
    result =
      with {:ok, entries} <- read_entries(data_dir),
           {:ok, stats} <- recover_entries(data_dir, entries) do
        _ = compact_committed(data_dir)
        {:ok, stats}
      end

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

  defp compact_committed(data_dir) do
    with {:ok, entries} <- read_entries(data_dir),
         entries <- pending_entries(entries) do
      rewrite_entries(data_dir, entries)
    end
  end

  defp new_txid do
    unique = System.unique_integer([:positive, :monotonic])
    "#{System.system_time(:nanosecond)}-#{unique}"
  end

  defp recover_entries(data_dir, entries) do
    {prepares, commits} =
      Enum.reduce(entries, {%{}, MapSet.new()}, fn
        {@magic, :prepare, txid, groups}, {prepares, commits} ->
          {Map.put(prepares, txid, groups), commits}

        {@magic, :commit, txid}, {prepares, commits} ->
          {prepares, MapSet.put(commits, txid)}

        _other, acc ->
          acc
      end)

    pending =
      prepares
      |> Enum.reject(fn {txid, _groups} -> MapSet.member?(commits, txid) end)

    initial_stats = %{pending: length(pending), replayed: 0, groups: 0, ops: 0}

    pending
    |> Enum.reduce_while({:ok, initial_stats}, fn {txid, groups}, {:ok, stats} ->
      group_stats = group_stats(groups)

      case apply_groups(groups) do
        :ok ->
          case commit(data_dir, txid) do
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

  defp pending_entries(entries) do
    {prepares, commits} =
      Enum.reduce(entries, {%{}, MapSet.new()}, fn
        {@magic, :prepare, txid, groups}, {prepares, commits} ->
          {Map.put(prepares, txid, groups), commits}

        {@magic, :commit, txid}, {prepares, commits} ->
          {prepares, MapSet.put(commits, txid)}

        _other, acc ->
          acc
      end)

    prepares
    |> Enum.reject(fn {txid, _groups} -> MapSet.member?(commits, txid) end)
    |> Enum.map(fn {txid, groups} -> {@magic, :prepare, txid, groups} end)
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
    if Enum.any?(batch, &match?({:delete, _, _}, &1)) do
      ops =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {:put, key, value, expire_at_ms}
          {:delete, key, _prob_path} -> {:delete, key}
        end)

      case NIF.v2_append_ops_batch_nosync(file_path, ops) do
        {:ok, locations} ->
          case NIF.v2_fsync(file_path) do
            :ok -> {:ok, locations}
            {:error, reason} -> {:error, reason}
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
          {:ok, Enum.map(locations, fn {offset, value_size} -> {:put, offset, value_size} end)}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp append_entry(data_dir, entry) do
    path = path(data_dir)
    dir = Path.dirname(path)
    line = encode_entry(entry) <> "\n"

    with :ok <- File.mkdir_p(dir),
         {:ok, io} <- File.open(path, [:append, :binary]),
         :ok <- IO.binwrite(io, line),
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- fsync_dir(dir) do
      :ok
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp rewrite_entries(data_dir, []) do
    path = path(data_dir)
    dir = Path.dirname(path)

    case File.rm(path) do
      :ok -> fsync_dir(dir)
      {:error, :enoent} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp rewrite_entries(data_dir, entries) do
    path = path(data_dir)
    dir = Path.dirname(path)
    tmp_path = path <> ".compact"
    data = Enum.map_join(entries, "", fn entry -> encode_entry(entry) <> "\n" end)

    with :ok <- File.mkdir_p(dir),
         {:ok, io} <- File.open(tmp_path, [:write, :binary]),
         :ok <- IO.binwrite(io, data),
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- fsync_dir(dir),
         :ok <- File.rename(tmp_path, path),
         :ok <- fsync_dir(dir) do
      :ok
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp read_entries(data_dir) do
    path = path(data_dir)

    case File.read(path) do
      {:ok, data} ->
        {entries, skipped} =
          data
          |> String.split("\n", trim: true)
          |> Enum.reduce({[], 0}, fn line, {entries, skipped} ->
            case decode_line(line) do
              {:ok, entry} -> {[entry | entries], skipped}
              :error -> {entries, skipped + 1}
            end
          end)

        if skipped > 0 do
          observe(:corrupt_entry, %{count: skipped}, %{data_dir_hash: :erlang.phash2(data_dir)})
        end

        {:ok, Enum.reverse(entries)}

      {:error, :enoent} ->
        {:ok, []}

      {:error, _reason} = error ->
        error
    end
  end

  defp encode_entry(entry), do: Base.encode64(:erlang.term_to_binary(entry, [:compressed]))

  defp decode_line(line) do
    with {:ok, binary} <- Base.decode64(line),
         term <- :erlang.binary_to_term(binary, [:safe]),
         true <- valid_entry?(term) do
      {:ok, term}
    else
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp valid_entry?({@magic, :prepare, txid, groups}) when is_binary(txid) and is_list(groups),
    do: true

  defp valid_entry?({@magic, :commit, txid}) when is_binary(txid), do: true
  defp valid_entry?(_other), do: false

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

  defp recover_once_key(data_dir), do: {__MODULE__, Path.expand(data_dir)}

  defp forget_recover_once(data_dir) do
    :persistent_term.erase(recover_once_key(data_dir))
  rescue
    ArgumentError -> :ok
  end
end
