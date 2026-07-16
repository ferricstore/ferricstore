defmodule Ferricstore.Store.HintFile do
  @moduledoc false

  alias Ferricstore.Store.{HintMetadata, SegmentLock}

  @page_size 256
  @temp_open_attempts 8

  @spec write_from_keydir(binary(), :ets.tid(), non_neg_integer()) :: :ok | {:error, term()}
  def write_from_keydir(hint_path, keydir, target_fid)
      when is_binary(hint_path) and is_integer(target_fid) and target_fid >= 0 do
    log_path = Path.rootname(hint_path, ".hint") <> ".log"
    shard_path = Path.dirname(hint_path)

    SegmentLock.with_lock(log_path, fn ->
      do_write_from_keydir(hint_path, log_path, shard_path, keydir, target_fid)
    end)
  end

  defp do_write_from_keydir(hint_path, log_path, shard_path, keydir, target_fid) do
    with {:ok, source_snapshot} <- HintMetadata.source_snapshot(log_path),
         :ok <- HintMetadata.prepare_publish(hint_path, shard_path),
         {:ok, temp_path} <- write_temp_file(hint_path, keydir, target_fid) do
      publish_temp(
        temp_path,
        hint_path,
        log_path,
        shard_path,
        target_fid,
        source_snapshot
      )
    end
  rescue
    error -> {:error, Exception.message(error)}
  end

  defp write_temp_file(hint_path, keydir, target_fid) do
    case open_temp_file(hint_path, @temp_open_attempts) do
      {:ok, io, temp_path} ->
        write_open_temp_file(io, temp_path, keydir, target_fid)

      {:error, _reason} = error ->
        error
    end
  end

  defp write_open_temp_file(io, temp_path, keydir, target_fid) do
    result =
      try do
        :ets.safe_fixtable(keydir, true)

        try do
          with :ok <- write_pages(io, :ets.select(keydir, match_spec(target_fid), @page_size)),
               :ok <- :file.sync(io) do
            :ok
          end
        after
          :ets.safe_fixtable(keydir, false)
        end
      after
        :file.close(io)
      end

    case result do
      :ok -> {:ok, temp_path}
      {:error, reason} -> cleanup_error(temp_path, reason)
    end
  rescue
    error -> cleanup_error(temp_path, Exception.message(error))
  catch
    kind, reason -> cleanup_error(temp_path, {kind, reason})
  end

  defp open_temp_file(_hint_path, 0), do: {:error, :temp_name_exhausted}

  defp open_temp_file(hint_path, attempts) do
    temp_path = unique_temp_path(hint_path)

    case :file.open(String.to_charlist(temp_path), [:write, :binary, :raw, :exclusive]) do
      {:ok, io} -> {:ok, io, temp_path}
      {:error, :eexist} -> open_temp_file(hint_path, attempts - 1)
      {:error, reason} -> {:error, {:temp_open_failed, reason}}
    end
  end

  defp unique_temp_path(hint_path) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
    hint_path <> ".tmp." <> suffix
  end

  defp publish_temp(
         temp_path,
         hint_path,
         log_path,
         shard_path,
         target_fid,
         source_snapshot
       ) do
    result =
      with :ok <- File.rename(temp_path, hint_path),
           :ok <-
             HintMetadata.publish(
               log_path,
               hint_path,
               target_fid,
               source_snapshot,
               shard_path
             ) do
        :ok
      end

    case result do
      :ok -> :ok
      {:error, reason} -> cleanup_error(temp_path, reason)
    end
  rescue
    error -> cleanup_error(temp_path, Exception.message(error))
  end

  defp write_pages(_io, :"$end_of_table"), do: :ok

  defp write_pages(io, {entries, continuation}) do
    with :ok <- write_entries(io, entries) do
      write_pages(io, :ets.select(continuation))
    end
  end

  defp write_entries(io, entries) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case encode_entry(entry) do
        {:ok, encoded} ->
          case :file.write(io, encoded) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, {:write_failed, reason}}}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp encode_entry({key, file_id, offset, value_size, expire_at_ms})
       when is_binary(key) and byte_size(key) <= 65_535 and is_integer(file_id) and file_id >= 0 and
              is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 and
              value_size <= 4_294_967_295 and is_integer(expire_at_ms) and expire_at_ms >= 0 do
    body =
      <<file_id::little-unsigned-64, offset::little-unsigned-64, value_size::little-unsigned-32,
        expire_at_ms::little-unsigned-64, byte_size(key)::little-unsigned-16, key::binary>>

    {:ok, [<<:erlang.crc32(body)::little-unsigned-32>>, body]}
  end

  defp encode_entry(entry), do: {:error, {:invalid_hint_entry, entry}}

  defp match_spec(target_fid) do
    [
      {{:"$1", :_, :"$2", :_, target_fid, :"$3", :"$4"}, [],
       [{{:"$1", target_fid, :"$3", :"$4", :"$2"}}]}
    ]
  end

  defp cleanup_error(temp_path, reason) do
    _ = File.rm(temp_path)
    {:error, reason}
  end
end
