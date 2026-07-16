defmodule Ferricstore.Store.HintMetadata do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.TermCodec

  @magic "FSHM"
  @version 1
  @max_metadata_bytes 4_096

  @type source_snapshot :: %{
          generation: {non_neg_integer(), non_neg_integer(), pos_integer()},
          size: non_neg_integer()
        }

  @spec source_snapshot(binary()) :: {:ok, source_snapshot()} | {:error, term()}
  def source_snapshot(path) do
    with {:ok, stat} <- File.lstat(path, time: :posix),
         true <- stat.type == :regular,
         true <- is_integer(stat.inode) and stat.inode > 0 do
      {:ok,
       %{
         generation: {stat.major_device, stat.minor_device, stat.inode},
         size: stat.size
       }}
    else
      {:error, reason} -> {:error, {:source_stat_failed, reason}}
      false -> {:error, :source_generation_unavailable}
    end
  end

  @spec prepare_publish(binary(), binary()) :: :ok | {:error, term()}
  def prepare_publish(hint_path, shard_path) do
    with :ok <- remove_metadata_files(hint_path),
         :ok <- NIF.v2_fsync_dir(shard_path) do
      :ok
    end
  end

  @spec publish(binary(), binary(), non_neg_integer(), source_snapshot(), binary()) ::
          :ok | {:error, term()}
  def publish(log_path, hint_path, file_id, source, shard_path) do
    with {:ok, current_source} <- source_snapshot(log_path),
         :ok <- ensure_source_compatible(source, current_source),
         {:ok, hint} <- hint_snapshot(hint_path),
         payload <- encode(file_id, source, hint),
         :ok <- remove_metadata_tmp(hint_path),
         :ok <-
           Ferricstore.FS.atomic_replace_nofollow(
             metadata_path(hint_path),
             payload,
             @max_metadata_bytes
           ),
         :ok <- NIF.v2_fsync_dir(shard_path) do
      :ok
    else
      {:error, _reason} = error ->
        _ = Ferricstore.FS.rm(metadata_path(hint_path) <> ".tmp")
        error
    end
  end

  @spec valid_for_log?(binary(), binary(), non_neg_integer()) :: boolean()
  def valid_for_log?(log_path, hint_path, file_id) do
    match?({:ok, _covered_size}, covered_source_size(log_path, hint_path, file_id))
  end

  @spec covered_source_size(binary(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def covered_source_size(log_path, hint_path, file_id) do
    with {:ok,
          %{
            file_id: ^file_id,
            source: %{size: covered_size} = source,
            hint: hint
          }}
         when is_integer(covered_size) and covered_size >= 0 <- read(hint_path),
         {:ok, current_source} <- source_snapshot(log_path),
         :ok <- ensure_source_compatible(source, current_source),
         {:ok, current_hint} <- hint_snapshot(hint_path),
         true <- current_hint == hint do
      {:ok, covered_size}
    else
      false -> {:error, :hint_changed}
      {:ok, _invalid_metadata} -> {:error, :invalid_hint_metadata}
      {:error, _reason} = error -> error
    end
  end

  @spec remove(binary()) :: :ok | {:error, term()}
  def remove(hint_path), do: remove_metadata_files(hint_path)

  @spec metadata_path(binary()) :: binary()
  def metadata_path(hint_path), do: hint_path <> ".meta"

  defp ensure_source_compatible(
         %{generation: generation, size: covered_size},
         %{generation: generation, size: current_size}
       )
       when current_size >= covered_size,
       do: :ok

  defp ensure_source_compatible(_expected, _actual), do: {:error, :source_changed}

  defp hint_snapshot(path) do
    case File.lstat(path, time: :posix) do
      {:ok, stat} when stat.type == :regular and stat.inode > 0 ->
        {:ok,
         %{
           generation: {stat.major_device, stat.minor_device, stat.inode},
           size: stat.size,
           mtime: stat.mtime,
           ctime: stat.ctime
         }}

      {:ok, _stat} ->
        {:error, :hint_generation_unavailable}

      {:error, reason} ->
        {:error, {:hint_stat_failed, reason}}
    end
  end

  defp encode(file_id, source, hint) do
    term = TermCodec.encode({:ferricstore_hint_metadata, @version, file_id, source, hint})

    <<@magic, byte_size(term)::unsigned-big-32, term::binary,
      :erlang.crc32(term)::unsigned-big-32>>
  end

  defp read(hint_path) do
    with {:ok, bytes} <-
           Ferricstore.FS.read_nofollow(metadata_path(hint_path), @max_metadata_bytes),
         {:ok, term} <- decode(bytes) do
      case term do
        {:ferricstore_hint_metadata, @version, file_id, source, hint}
        when is_integer(file_id) and file_id >= 0 and is_map(source) and is_map(hint) ->
          {:ok, %{file_id: file_id, source: source, hint: hint}}

        _ ->
          {:error, :invalid_hint_metadata}
      end
    end
  end

  defp decode(
         <<@magic, payload_size::unsigned-big-32, payload::binary-size(payload_size),
           stored_crc::unsigned-big-32>>
       ) do
    if :erlang.crc32(payload) == stored_crc do
      case TermCodec.decode(payload) do
        {:ok, term} -> {:ok, term}
        {:error, :invalid_external_term} -> {:error, :invalid_hint_metadata}
      end
    else
      {:error, :invalid_hint_metadata_crc}
    end
  end

  defp decode(_bytes), do: {:error, :invalid_hint_metadata}

  defp remove_metadata_tmp(hint_path) do
    case Ferricstore.FS.rm(metadata_path(hint_path) <> ".tmp") do
      :ok -> :ok
      {:error, {:not_found, _reason}} -> :ok
      {:error, reason} -> {:error, {:metadata_remove_failed, reason}}
    end
  end

  defp remove_metadata_files(hint_path) do
    [metadata_path(hint_path), metadata_path(hint_path) <> ".tmp"]
    |> Enum.reduce_while(:ok, fn path, :ok ->
      case Ferricstore.FS.rm(path) do
        :ok -> {:cont, :ok}
        {:error, {:not_found, _reason}} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:metadata_remove_failed, path, reason}}}
      end
    end)
  end
end
