defmodule Ferricstore.Store.CompactionPlan do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.{Hibernation, LMDB, Locator}
  alias Ferricstore.TermCodec

  @magic "FSCPLAN1"
  @header_bytes byte_size(@magic) + 8
  @record_header_bytes 8
  @max_record_bytes 16 * 1024 * 1024
  @default_page_size 512
  @compare_retries 3

  @type entry ::
          {:hot, binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
          | {:cold, binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(), binary(),
             map()}

  @type writer :: %{
          file: :file.io_device(),
          path: binary(),
          temp_path: binary(),
          shard_path: binary(),
          fid: non_neg_integer()
        }

  @spec path(binary(), non_neg_integer()) :: binary()
  def path(shard_path, fid) when is_binary(shard_path) and is_integer(fid) and fid >= 0,
    do: Path.join(shard_path, "compaction_plan_#{fid}.txn")

  @spec create(binary(), non_neg_integer()) :: {:ok, writer()} | {:error, term()}
  def create(shard_path, fid)
      when is_binary(shard_path) and is_integer(fid) and fid >= 0 do
    plan_path = path(shard_path, fid)
    temp_path = plan_path <> ".tmp"

    cond do
      File.exists?(plan_path) ->
        {:error, {:plan_exists, plan_path}}

      File.exists?(temp_path) ->
        {:error, {:plan_temp_exists, temp_path}}

      true ->
        case :file.open(temp_path, [:write, :binary, :raw, :exclusive]) do
          {:ok, file} ->
            case :file.write(file, <<@magic::binary, fid::unsigned-big-64>>) do
              :ok ->
                {:ok,
                 %{
                   file: file,
                   path: plan_path,
                   temp_path: temp_path,
                   shard_path: shard_path,
                   fid: fid
                 }}

              {:error, reason} ->
                :ok = :file.close(file)
                _ = File.rm(temp_path)
                {:error, {:plan_header_write_failed, reason}}
            end

          {:error, reason} ->
            {:error, {:plan_open_failed, reason}}
        end
    end
  end

  @spec append(writer(), [entry()]) :: :ok | {:error, term()}
  def append(%{file: file, fid: fid}, entries) when is_list(entries) do
    with {:ok, frames} <- encode_frames(entries, fid),
         :ok <- :file.write(file, frames) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec finish(writer()) :: {:ok, binary()} | {:error, term()}
  def finish(%{} = writer) do
    result =
      with :ok <- :file.sync(writer.file),
           :ok <- :file.close(writer.file),
           :ok <- rename(writer.temp_path, writer.path),
           :ok <- fsync_dir(writer.shard_path) do
        {:ok, writer.path}
      end

    case result do
      {:ok, _path} = ok ->
        ok

      {:error, _reason} = error ->
        _ = :file.close(writer.file)
        _ = File.rm(writer.temp_path)
        _ = File.rm(writer.path)
        error
    end
  end

  @spec abort(writer()) :: :ok
  def abort(%{} = writer) do
    _ = :file.close(writer.file)
    _ = File.rm(writer.temp_path)
    _ = File.rm(writer.path)
    :ok
  end

  @spec remove(binary()) :: :ok | {:error, term()}
  def remove(plan_path) when is_binary(plan_path) do
    case Ferricstore.FS.rm(plan_path) do
      :ok -> :ok
      {:error, {:not_found, _message}} -> :ok
      {:error, reason} -> {:error, {:plan_remove_failed, plan_path, reason}}
    end
  end

  @spec reduce_pages(binary(), pos_integer(), term(), ([entry()], term() -> term())) ::
          {:ok, term()} | {:error, term()}
  def reduce_pages(plan_path, page_size, acc, fun)
      when is_binary(plan_path) and is_integer(page_size) and page_size > 0 and
             is_function(fun, 2) do
    case open_plan_for_read(plan_path) do
      {:ok, file} ->
        try do
          with {:ok, fid} <- read_header(file) do
            reduce_file_pages(file, fid, page_size, acc, fun)
          end
        after
          :file.close(file)
        end

      {:error, {:plan_identity_changed, _path} = reason} ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:plan_open_failed, reason}}
    end
  end

  defp open_plan_for_read(plan_path) do
    modes = [:read, :binary, :raw]

    with {:ok, %{type: :regular} = expected_stat} <- File.lstat(plan_path),
         {:ok, file} <- open_plan_path(plan_path, modes) do
      case verify_plan_file_identity(file, plan_path, expected_stat) do
        :ok ->
          {:ok, file}

        {:error, _reason} = error ->
          _ = :file.close(file)
          error
      end
    else
      {:ok, %{type: type}} -> {:error, {:invalid_file_type, type}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp open_plan_path(plan_path, modes) do
    case Process.get(:ferricstore_compaction_plan_open_read_hook) do
      fun when is_function(fun, 2) -> fun.(plan_path, modes)
      _other -> :file.open(plan_path, modes)
    end
  end

  defp verify_plan_file_identity(file, plan_path, %File.Stat{
         major_device: major_device,
         minor_device: minor_device,
         inode: inode
       }) do
    case :file.read_file_info(file) do
      {:ok, info}
      when elem(info, 2) == :regular and elem(info, 9) == major_device and
             elem(info, 10) == minor_device and elem(info, 11) == inode ->
        :ok

      {:ok, _different_file} ->
        {:error, {:plan_identity_changed, plan_path}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec relocate_cold(binary(), binary(), :forward | :reverse, keyword()) ::
          :ok | {:error, term()}
  def relocate_cold(plan_path, lmdb_path, direction, opts \\ [])
      when is_binary(plan_path) and is_binary(lmdb_path) and direction in [:forward, :reverse] do
    page_size = Keyword.get(opts, :page_size, @default_page_size)
    get_many_fun = Keyword.get(opts, :get_many_fun, &LMDB.get_many/2)
    write_fun = Keyword.get(opts, :write_fun, &LMDB.write_batch/2)

    case reduce_pages(plan_path, page_size, :ok, fn page, :ok ->
           case relocate_cold_page(
                  lmdb_path,
                  page,
                  direction,
                  @compare_retries,
                  get_many_fun,
                  write_fun
                ) do
             :ok -> :ok
             {:error, _reason} = error -> error
           end
         end) do
      {:ok, :ok} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp encode_frames(entries, fid) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case validate_entry(entry, fid) do
        :ok ->
          payload = TermCodec.encode(entry)
          size = byte_size(payload)

          if size <= @max_record_bytes do
            crc = :erlang.crc32(payload)
            {:cont, {:ok, [[<<size::unsigned-big-32, crc::unsigned-big-32>>, payload] | acc]}}
          else
            {:halt, {:error, {:plan_record_too_large, size}}}
          end

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, frames} -> {:ok, Enum.reverse(frames)}
      {:error, _reason} = error -> error
    end
  end

  defp read_header(file) do
    case :file.read(file, @header_bytes) do
      {:ok, <<@magic::binary, fid::unsigned-big-64>>} -> {:ok, fid}
      {:ok, _invalid} -> {:error, :invalid_plan_header}
      :eof -> {:error, :truncated_plan_header}
      {:error, reason} -> {:error, {:plan_read_failed, reason}}
    end
  end

  defp reduce_file_pages(file, fid, page_size, acc, fun) do
    case read_page(file, fid, page_size, []) do
      {:ok, [], :eof} ->
        {:ok, acc}

      {:ok, page, status} ->
        case fun.(page, acc) do
          {:halt, next_acc} -> {:ok, next_acc}
          {:error, _reason} = error -> error
          next_acc when status == :eof -> {:ok, next_acc}
          next_acc -> reduce_file_pages(file, fid, page_size, next_acc, fun)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp read_page(_file, _fid, 0, acc), do: {:ok, Enum.reverse(acc), :more}

  defp read_page(file, fid, remaining, acc) do
    case read_record(file, fid) do
      {:ok, entry} -> read_page(file, fid, remaining - 1, [entry | acc])
      :eof -> {:ok, Enum.reverse(acc), :eof}
      {:error, _reason} = error -> error
    end
  end

  defp read_record(file, fid) do
    case :file.read(file, @record_header_bytes) do
      {:ok, <<size::unsigned-big-32, crc::unsigned-big-32>>} when size <= @max_record_bytes ->
        read_record_payload(file, fid, size, crc)

      {:ok, <<size::unsigned-big-32, _crc::unsigned-big-32>>} ->
        {:error, {:plan_record_too_large, size}}

      {:ok, _partial} ->
        {:error, :truncated_record_header}

      :eof ->
        :eof

      {:error, reason} ->
        {:error, {:plan_read_failed, reason}}
    end
  end

  defp read_record_payload(file, fid, size, expected_crc) do
    case :file.read(file, size) do
      {:ok, payload} when byte_size(payload) == size ->
        if :erlang.crc32(payload) == expected_crc do
          decode_entry(payload, fid)
        else
          {:error, :plan_checksum_mismatch}
        end

      {:ok, _partial} ->
        {:error, :truncated_record}

      :eof ->
        {:error, :truncated_record}

      {:error, reason} ->
        {:error, {:plan_read_failed, reason}}
    end
  end

  defp decode_entry(payload, fid) do
    with {:ok, entry} <- TermCodec.decode(payload),
         :ok <- validate_entry(entry, fid) do
      {:ok, entry}
    else
      {:error, :invalid_external_term} -> {:error, :invalid_plan_record}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_entry({:hot, key, old_offset, new_offset, new_size}, _fid)
       when is_binary(key) and is_integer(old_offset) and old_offset >= 0 and
              is_integer(new_offset) and new_offset >= 0 and is_integer(new_size) and
              new_size >= 0,
       do: :ok

  defp validate_entry(
         {:cold, state_key, old_offset, new_offset, new_size, park_key,
          %{locator: %Locator{kind: :state, file_id: fid, offset: old_offset}} = park},
         fid
       )
       when is_binary(state_key) and is_binary(park_key) and is_integer(old_offset) and
              old_offset >= 0 and is_integer(new_offset) and new_offset >= 0 and
              is_integer(new_size) and new_size >= 0 do
    if Map.get(park, :state_key) == state_key, do: :ok, else: {:error, :invalid_plan_record}
  end

  defp validate_entry(_entry, _fid), do: {:error, :invalid_plan_record}

  defp relocate_cold_page(
         lmdb_path,
         page,
         direction,
         retries_left,
         get_many_fun,
         write_fun
       ) do
    cold_entries = Enum.filter(page, &match?({:cold, _, _, _, _, _, _}, &1))
    park_keys = Enum.map(cold_entries, &elem(&1, 5))

    with {:ok, current_values} <- get_many_fun.(lmdb_path, park_keys),
         {:ok, ops} <- relocation_ops(cold_entries, current_values, direction) do
      case write_fun.(lmdb_path, ops) do
        :ok ->
          :ok

        {:error, {:compare_failed, _key}} when retries_left > 0 ->
          relocate_cold_page(
            lmdb_path,
            page,
            direction,
            retries_left - 1,
            get_many_fun,
            write_fun
          )

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, reason} -> {:error, reason}
      invalid -> {:error, {:invalid_cold_relocation_read, invalid}}
    end
  end

  defp relocation_ops(entries, values, direction) when length(entries) == length(values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce_while({:ok, []}, fn {entry, current}, {:ok, acc} ->
      with {:ok, source_row, target_row} <- relocation_rows(entry, direction),
           source_blob <- encoded_park(source_row),
           target_blob <- encoded_park(target_row) do
        cond do
          current == {:ok, target_blob} ->
            {:cont, {:ok, acc}}

          current == {:ok, source_blob} ->
            case Hibernation.cold_compaction_ops(source_row, target_row) do
              {:ok, row_ops} -> {:cont, {:ok, [row_ops | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end

          current == :not_found ->
            {:cont, {:ok, acc}}

          match?({:ok, _other}, current) ->
            {:cont, {:ok, acc}}

          match?({:error, _reason}, current) ->
            {:halt, current}

          true ->
            {:halt, {:error, {:invalid_lmdb_value, current}}}
        end
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, nested_ops} -> {:ok, nested_ops |> Enum.reverse() |> List.flatten()}
      {:error, _reason} = error -> error
    end
  end

  defp relocation_ops(_entries, _values, _direction), do: {:error, :lmdb_result_count_mismatch}

  defp relocation_rows(
         {:cold, _state_key, _old_offset, new_offset, new_size, park_key,
          %{locator: %Locator{} = old_locator} = park},
         direction
       ) do
    old_row = %{locator: old_locator, park: park, park_key: park_key}

    with {:ok, new_row} <-
           Hibernation.relocate_cold_row(old_row, offset: new_offset, value_size: new_size) do
      case direction do
        :forward -> {:ok, old_row, new_row}
        :reverse -> {:ok, new_row, old_row}
      end
    end
  end

  defp encoded_park(%{locator: %Locator{} = locator, park: park}) do
    LMDB.encode_cold_park(locator, Map.delete(park, :locator))
  end

  defp rename(from, to) do
    case Ferricstore.FS.rename(from, to) do
      :ok -> :ok
      {:error, reason} -> {:error, {:plan_rename_failed, from, to, reason}}
    end
  end

  defp fsync_dir(path) do
    case NIF.v2_fsync_dir(path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:plan_fsync_dir_failed, path, reason}}
    end
  end
end
