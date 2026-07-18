defmodule Ferricstore.Store.CompactionJournal do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Store.CompactionPlan
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.TermCodec

  @journal_version 1
  @journal_prefix "compaction_swap_"
  @journal_suffix ".txn"
  @max_journal_bytes 64 * 1024

  @type transaction :: %{
          fid: non_neg_integer(),
          tx_id: binary(),
          shard_path: binary(),
          source: binary(),
          backup: binary(),
          journal: binary(),
          plan: binary(),
          marker_key: binary()
        }

  @spec begin(binary(), non_neg_integer(), binary()) ::
          {:ok, transaction()} | {:error, term()}
  def begin(shard_path, fid, plan_path)
      when is_binary(shard_path) and is_integer(fid) and fid >= 0 and is_binary(plan_path) do
    tx_id = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)
    transaction = transaction(shard_path, fid, tx_id)

    cond do
      plan_path != transaction.plan ->
        {:error, {:invalid_plan_path, plan_path}}

      not regular_file?(transaction.plan) ->
        {:error, {:plan_missing, transaction.plan}}

      Ferricstore.FS.exists?(transaction.journal) ->
        {:error, {:journal_exists, transaction.journal}}

      Ferricstore.FS.exists?(transaction.backup) ->
        {:error, {:backup_exists, transaction.backup}}

      true ->
        payload =
          TermCodec.encode({:ferricstore_compaction_swap, @journal_version, fid, tx_id})

        with :ok <- write_synced_file(transaction.journal <> ".tmp", payload),
             :ok <- rename(transaction.journal <> ".tmp", transaction.journal),
             :ok <- fsync_dir(shard_path) do
          {:ok, transaction}
        else
          {:error, reason} = error ->
            _ = remove(transaction.journal <> ".tmp")
            _ = remove(transaction.journal)
            {:error, {:journal_create_failed, reason, error}}
        end
    end
  end

  @spec marker_op(transaction()) :: {:put, binary(), binary()}
  def marker_op(%{marker_key: marker_key}), do: {:put, marker_key, <<1>>}

  @spec sync_swap(transaction()) :: :ok | {:error, term()}
  def sync_swap(%{shard_path: shard_path}), do: fsync_dir(shard_path)

  @spec complete(transaction()) :: :ok | {:error, term()}
  def complete(%{} = transaction) do
    with :ok <- relocate_cold(transaction, :forward),
         :ok <- remove(transaction.backup),
         :ok <- fsync_dir(transaction.shard_path),
         :ok <- delete_marker(transaction),
         :ok <- CompactionPlan.remove(transaction.plan),
         :ok <- remove(transaction.journal),
         :ok <- fsync_dir(transaction.shard_path) do
      :ok
    end
  end

  @spec rollback(transaction()) :: :ok | {:error, term()}
  def rollback(%{} = transaction) do
    with :ok <- relocate_cold(transaction, :reverse),
         :ok <- remove(transaction.source),
         :ok <- rename(transaction.backup, transaction.source),
         :ok <- fsync_dir(transaction.shard_path),
         :ok <- delete_marker(transaction),
         :ok <- CompactionPlan.remove(transaction.plan),
         :ok <- remove(transaction.journal),
         :ok <- fsync_dir(transaction.shard_path) do
      :ok
    end
  end

  @spec abort_before_swap(transaction()) :: :ok | {:error, term()}
  def abort_before_swap(%{} = transaction) do
    with :ok <- remove(transaction.journal),
         :ok <- CompactionPlan.remove(transaction.plan),
         :ok <- fsync_dir(transaction.shard_path) do
      :ok
    end
  end

  @spec recover_all(binary()) :: :ok | {:error, term()}
  def recover_all(shard_path) when is_binary(shard_path) do
    with {:ok, files} <- Ferricstore.FS.ls(shard_path) do
      files
      |> Enum.filter(&journal_name?/1)
      |> Enum.sort()
      |> Enum.reduce_while(:ok, fn name, :ok ->
        case recover_one(shard_path, Path.join(shard_path, name)) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {name, reason}}}
        end
      end)
    end
  end

  defp recover_one(shard_path, journal_path) do
    with {:ok, payload} <- Ferricstore.FS.read_nofollow(journal_path, @max_journal_bytes),
         {:ok, fid, tx_id} <- decode_journal(payload),
         transaction = transaction(shard_path, fid, tx_id),
         :ok <- validate_journal_identity(journal_path, transaction) do
      recover_transaction(transaction)
    end
  end

  defp validate_journal_identity(journal_path, %{journal: journal_path}), do: :ok

  defp validate_journal_identity(journal_path, %{journal: expected}) do
    {:error, {:journal_identity_mismatch, journal_path, expected}}
  end

  defp recover_transaction(transaction) do
    with {:ok, backup?} <- regular_file_presence(transaction.backup),
         {:ok, source?} <- regular_file_presence(transaction.source) do
      case {backup?, source?, marker_status(transaction)} do
        {false, true, :missing} ->
          clear_recovered_transaction(transaction)

        {false, true, :committed} ->
          complete_without_backup(transaction)

        {true, false, :missing} ->
          restore_backup_without_source(transaction)

        {true, true, :missing} ->
          rollback(transaction)

        {true, true, :committed} ->
          complete(transaction)

        {true, false, :committed} ->
          {:error, :committed_source_missing}

        {_backup?, _source?, {:error, reason}} ->
          {:error, {:marker_read_failed, reason}}

        {false, false, _status} ->
          {:error, :source_and_backup_missing}
      end
    end
  end

  defp regular_file?(path) do
    match?({:ok, %File.Stat{type: :regular}}, File.lstat(path))
  end

  defp regular_file_presence(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> {:ok, true}
      {:error, :enoent} -> {:ok, false}
      {:ok, %File.Stat{type: type}} -> {:error, {:unsafe_compaction_file_type, path, type}}
      {:error, reason} -> {:error, {:compaction_file_stat_failed, path, reason}}
    end
  end

  defp restore_backup_without_source(transaction) do
    with :ok <- relocate_cold(transaction, :reverse),
         :ok <- rename(transaction.backup, transaction.source),
         :ok <- fsync_dir(transaction.shard_path),
         :ok <- delete_marker(transaction),
         :ok <- CompactionPlan.remove(transaction.plan),
         :ok <- remove(transaction.journal),
         :ok <- fsync_dir(transaction.shard_path) do
      :ok
    end
  end

  defp clear_recovered_transaction(transaction) do
    with :ok <- delete_marker(transaction),
         :ok <- CompactionPlan.remove(transaction.plan),
         :ok <- remove(transaction.journal),
         :ok <- fsync_dir(transaction.shard_path) do
      :ok
    end
  end

  defp complete_without_backup(transaction) do
    with :ok <- relocate_cold(transaction, :forward),
         :ok <- delete_marker(transaction),
         :ok <- CompactionPlan.remove(transaction.plan),
         :ok <- remove(transaction.journal),
         :ok <- fsync_dir(transaction.shard_path) do
      :ok
    end
  end

  defp marker_status(transaction) do
    transaction.shard_path
    |> LMDB.path()
    |> LMDB.get(transaction.marker_key)
    |> case do
      {:ok, <<1>>} -> :committed
      :not_found -> :missing
      {:ok, _invalid} -> {:error, :invalid_marker}
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_marker(transaction) do
    LMDB.write_batch(LMDB.path(transaction.shard_path), [{:delete, transaction.marker_key}])
  end

  defp transaction(shard_path, fid, tx_id) do
    %{
      fid: fid,
      tx_id: tx_id,
      shard_path: shard_path,
      source: ShardETS.file_path(shard_path, fid),
      backup: Path.join(shard_path, "compaction_backup_#{fid}.log"),
      journal: Path.join(shard_path, "#{@journal_prefix}#{fid}#{@journal_suffix}"),
      plan: CompactionPlan.path(shard_path, fid),
      marker_key: "ferricstore:compaction:commit:v1:" <> tx_id
    }
  end

  defp relocate_cold(transaction, direction) do
    CompactionPlan.relocate_cold(
      transaction.plan,
      LMDB.path(transaction.shard_path),
      direction
    )
  end

  defp decode_journal(payload) when is_binary(payload) do
    with {:ok, term} <- TermCodec.decode(payload) do
      case term do
        {:ferricstore_compaction_swap, @journal_version, fid, tx_id}
        when is_integer(fid) and fid >= 0 and is_binary(tx_id) ->
          if valid_tx_id?(tx_id) do
            {:ok, fid, tx_id}
          else
            {:error, :invalid_journal}
          end

        _invalid ->
          {:error, :invalid_journal}
      end
    else
      {:error, :invalid_external_term} -> {:error, :invalid_journal}
    end
  end

  defp valid_tx_id?(tx_id) when byte_size(tx_id) == 22 do
    case Base.url_decode64(tx_id, padding: false) do
      {:ok, decoded} when byte_size(decoded) == 16 ->
        Base.url_encode64(decoded, padding: false) == tx_id

      _invalid ->
        false
    end
  end

  defp valid_tx_id?(_tx_id), do: false

  defp journal_name?(name) do
    String.starts_with?(name, @journal_prefix) and String.ends_with?(name, @journal_suffix)
  end

  defp write_synced_file(path, payload) do
    _ = remove(path)

    case :file.open(path, [:write, :binary, :raw, :exclusive]) do
      {:ok, file} ->
        try do
          with :ok <- :file.write(file, payload),
               :ok <- :file.sync(file) do
            :ok
          end
        after
          :file.close(file)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp rename(from, to) do
    case Ferricstore.FS.rename(from, to) do
      :ok -> :ok
      {:error, reason} -> {:error, {:rename_failed, from, to, reason}}
    end
  end

  defp remove(path) do
    case Ferricstore.FS.rm(path) do
      :ok -> :ok
      {:error, {:not_found, _message}} -> :ok
      {:error, reason} -> {:error, {:remove_failed, path, reason}}
    end
  end

  defp fsync_dir(path) do
    case NIF.v2_fsync_dir(path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_dir_failed, path, reason}}
    end
  end
end
