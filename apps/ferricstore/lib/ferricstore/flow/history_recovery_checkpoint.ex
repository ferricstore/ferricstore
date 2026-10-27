defmodule Ferricstore.Flow.HistoryRecoveryCheckpoint do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.{HistoryProjectedIndex, LMDB}
  alias Ferricstore.Flow.{HistoryProjector, HistoryProjector.Files, HistoryProjector.Recovery}

  # The marker names a history file and an offset in it: every record before
  # them is reflected in LMDB, so recovery replays from there on. Version 1
  # markers predate rotation and name file 0.
  @key <<0, "ferricstore:history-recovery-checkpoint:1">>
  @magic 0xF17EC051
  @version 2
  @max_index 0xFFFFFFFFFFFFFFFF

  @doc false
  def key, do: @key

  @spec recovery_offset(binary(), :ets.tid() | atom(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | :full_scan | {:error, term()}
  def recovery_offset(shard_data_path, keydir, projected) do
    if is_integer(projected) and projected >= 0 and checkpoint_mode?(keydir) and
         LMDB.env_present?(LMDB.path(shard_data_path)) do
      case LMDB.get(LMDB.path(shard_data_path), @key) do
        :not_found -> :full_scan
        {:ok, marker} -> verify_marker(shard_data_path, projected, marker)
        {:error, reason} -> {:error, {:history_recovery_checkpoint_read_failed, reason}}
      end
    else
      :full_scan
    end
  end

  defp verify_marker(shard_data_path, projected, marker) do
    with {:ok, {file_id, offset, marker_index, expected_digest}} <- decode(marker),
         path = HistoryProjector.history_file_path(shard_data_path, file_id),
         true <- marker_index <= projected,
         {:ok, %{type: :regular, size: file_size}} <- File.lstat(path),
         true <- offset <= file_size,
         {:ok, ^expected_digest} <- NIF.v2_validated_log_prefix_digest(path, offset) do
      {:ok, {file_id, offset}}
    else
      mismatch -> {:error, {:history_recovery_checkpoint_source_changed, mismatch}}
    end
  end

  @doc """
  Marks the active history file's current end as recovered. Every record
  before it must already be published to LMDB.
  """
  @spec publish(binary(), :ets.tid() | atom()) :: :ok | {:error, term()}
  def publish(shard_data_path, keydir) do
    with true <- checkpoint_mode?(keydir),
         {:ok, projected} <- HistoryProjectedIndex.read_result(shard_data_path),
         {:ok, file_id} <- Files.active(shard_data_path),
         path = HistoryProjector.history_file_path(shard_data_path, file_id),
         {:ok, %{type: :regular, size: file_size}} <- File.lstat(path),
         true <- file_size <= @max_index,
         {:ok, digest} <- NIF.v2_validated_log_prefix_digest(path, file_size) do
      case LMDB.write_batch(LMDB.path(shard_data_path), [
             {:put, @key, encode(file_id, file_size, projected, digest)}
           ]) do
        :ok -> :ok
        {:error, reason} -> {:error, {:checkpoint_write_failed, reason}}
      end
    else
      false -> :ok
      {:error, reason} -> {:error, {:checkpoint_validation_failed, reason}}
      other -> {:error, {:history_recovery_checkpoint_unavailable, other}}
    end
  end

  defp checkpoint_mode?(keydir) do
    Application.get_env(:ferricstore, :flow_async_history, true) == true and
      Recovery.default_history_hot_max_events() == 0 and
      no_hot_history_rows?(keydir)
  end

  defp no_hot_history_rows?(keydir) do
    :ets.foldl(
      fn
        {_key, _value, _expire, _lfu, {:flow_history, _file}, _offset, _size}, _none -> false
        _row, empty? -> empty?
      end,
      true,
      keydir
    )
  rescue
    ArgumentError -> false
  end

  defp encode(file_id, offset, projected, digest)
       when file_id >= 0 and file_id <= @max_index and offset >= 0 and offset <= @max_index and
              projected >= 0 and projected <= @max_index and byte_size(digest) == 32 do
    body =
      <<@magic::32, @version::8, file_id::unsigned-big-64, offset::unsigned-big-64,
        projected::unsigned-big-64, digest::binary-size(32)>>

    <<body::binary, :erlang.crc32(body)::unsigned-big-32>>
  end

  @doc false
  def decode(
        <<@magic::32, @version::8, file_id::unsigned-big-64, offset::unsigned-big-64,
          projected::unsigned-big-64, digest::binary-size(32), crc::unsigned-big-32>>
      ) do
    body =
      <<@magic::32, @version::8, file_id::unsigned-big-64, offset::unsigned-big-64,
        projected::unsigned-big-64, digest::binary-size(32)>>

    if :erlang.crc32(body) == crc,
      do: {:ok, {file_id, offset, projected, digest}},
      else: {:error, :bad_history_recovery_checkpoint_checksum}
  end

  def decode(
        <<@magic::32, 1::8, offset::unsigned-big-64, projected::unsigned-big-64,
          digest::binary-size(32), crc::unsigned-big-32>>
      ) do
    body =
      <<@magic::32, 1::8, offset::unsigned-big-64, projected::unsigned-big-64,
        digest::binary-size(32)>>

    if :erlang.crc32(body) == crc,
      do: {:ok, {0, offset, projected, digest}},
      else: {:error, :bad_history_recovery_checkpoint_checksum}
  end

  def decode(_invalid), do: {:error, :bad_history_recovery_checkpoint}
end
