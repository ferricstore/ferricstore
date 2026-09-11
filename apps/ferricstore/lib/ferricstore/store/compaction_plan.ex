defmodule Ferricstore.Store.CompactionPlan do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.{Hibernation, Keys, LMDB, Locator}
  alias Ferricstore.Flow.Query.QueryRowCodec
  alias Ferricstore.TermCodec

  @magic "FSCPLAN2"
  @header_bytes byte_size(@magic) + 8
  @record_header_bytes 8
  @max_record_bytes 16 * 1024 * 1024
  @default_page_size 512
  @compare_retries 3

  @type entry ::
          {:hot, binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
          | {:flow, binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
             non_neg_integer(), non_neg_integer()}
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
      Ferricstore.FS.exists?(plan_path) ->
        {:error, {:plan_exists, plan_path}}

      Ferricstore.FS.exists?(temp_path) ->
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
                _ = Ferricstore.FS.rm(temp_path)
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
        _ = Ferricstore.FS.rm(writer.temp_path)
        _ = Ferricstore.FS.rm(writer.path)
        error
    end
  end

  @spec abort(writer()) :: :ok
  def abort(%{} = writer) do
    _ = :file.close(writer.file)
    _ = Ferricstore.FS.rm(writer.temp_path)
    _ = Ferricstore.FS.rm(writer.path)
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

  @spec relocate_flow_locators(binary(), binary(), :forward | :reverse, keyword()) ::
          :ok | {:error, term()}
  def relocate_flow_locators(plan_path, lmdb_path, direction, opts \\ [])
      when is_binary(plan_path) and is_binary(lmdb_path) and direction in [:forward, :reverse] do
    page_size = Keyword.get(opts, :page_size, @default_page_size)
    get_many_fun = Keyword.get(opts, :get_many_fun, &LMDB.get_many/2)
    write_fun = Keyword.get(opts, :write_fun, &LMDB.write_batch/2)

    with {:ok, :ok} <-
           reduce_pages(plan_path, page_size, :ok, fn page, :ok ->
             case relocate_flow_locator_page(
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
      :ok
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
         {:flow, state_key, fid, old_offset, old_size, new_offset, new_size},
         fid
       )
       when is_binary(state_key) and is_integer(old_offset) and old_offset >= 0 and
              is_integer(old_size) and old_size > 0 and is_integer(new_offset) and
              new_offset >= 0 and is_integer(new_size) and new_size == old_size do
    if Keys.state_key?(state_key), do: :ok, else: {:error, :invalid_plan_record}
  end

  defp validate_entry(
         {:cold, state_key, old_offset, new_offset, new_size, park_key,
          %{locator: %Locator{kind: :state, file_id: fid, offset: old_offset}} = park},
         fid
       )
       when is_binary(state_key) and is_binary(park_key) and is_integer(old_offset) and
              old_offset >= 0 and is_integer(new_offset) and new_offset >= 0 and
              is_integer(new_size) and new_size > 0 do
    if valid_cold_plan_identity?(state_key, park_key, park) and
         Locator.hydration_ready?(park.locator) and new_size == park.locator.value_size,
       do: :ok,
       else: {:error, :invalid_plan_record}
  end

  defp validate_entry(_entry, _fid), do: {:error, :invalid_plan_record}

  defp valid_cold_plan_identity?(state_key, park_key, park) do
    partition_key = Map.get(park, :partition_key)
    locator = Map.fetch!(park, :locator)

    Keys.state_key?(state_key) and Map.get(park, :state_key) == state_key and
      Keys.state_key(locator.flow_id, partition_key) == state_key and
      LMDB.cold_park_key_for_state_key(state_key) == park_key
  rescue
    _error -> false
  end

  defp relocate_flow_locator_page(
         lmdb_path,
         page,
         direction,
         retries_left,
         get_many_fun,
         write_fun
       ) do
    cold_entries = Enum.filter(page, &match?({:cold, _, _, _, _, _, _}, &1))
    flow_entries = Enum.filter(page, &match?({:flow, _, _, _, _, _, _}, &1))

    if cold_entries == [] and flow_entries == [] do
      :ok
    else
      park_keys = Enum.map(cold_entries, &elem(&1, 5))
      cold_state_keys = Enum.map(cold_entries, &elem(&1, 1))
      flow_state_keys = Enum.map(flow_entries, &elem(&1, 1))
      read_keys = park_keys ++ cold_state_keys ++ flow_state_keys

      with {:ok, current_values} <- get_many_fun.(lmdb_path, read_keys),
           true <- length(current_values) == length(read_keys),
           {park_values, query_rows} <- Enum.split(current_values, length(park_keys)),
           {cold_query_rows, flow_query_rows} <-
             Enum.split(query_rows, length(cold_state_keys)),
           {:ok, cold_ops} <-
             cold_relocation_ops(cold_entries, park_values, cold_query_rows, direction),
           {:ok, flow_ops} <- flow_relocation_ops(flow_entries, flow_query_rows, direction),
           ops = cold_ops ++ flow_ops do
        case if(ops == [], do: :ok, else: write_fun.(lmdb_path, ops)) do
          :ok ->
            :ok

          {:error, {:compare_failed, _key}} when retries_left > 0 ->
            relocate_flow_locator_page(
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
        false -> {:error, :lmdb_result_count_mismatch}
        invalid -> {:error, {:invalid_flow_relocation_read, invalid}}
      end
    end
  end

  defp cold_relocation_ops(entries, park_values, query_rows, direction)
       when length(entries) == length(park_values) and length(entries) == length(query_rows) do
    entries
    |> Enum.zip(Enum.zip(park_values, query_rows))
    |> Enum.reduce_while({:ok, []}, fn {entry, {current_park, current_query}}, {:ok, acc} ->
      with {:ok, source_row, target_row} <- relocation_rows(entry, direction),
           source_blob <- encoded_park(source_row),
           target_blob <- encoded_park(target_row) do
        cond do
          current_park == {:ok, target_blob} ->
            case query_row_matches?(current_query, target_row) do
              true -> {:cont, {:ok, acc}}
              false -> {:halt, {:error, :cold_query_locator_mismatch}}
              {:error, _reason} = error -> {:halt, error}
            end

          current_park == {:ok, source_blob} ->
            case current_query do
              {:ok, query_row} when is_binary(query_row) ->
                case Hibernation.cold_compaction_ops(source_row, target_row, query_row) do
                  {:ok, row_ops} -> {:cont, {:ok, [row_ops | acc]}}
                  {:error, reason} -> {:halt, {:error, reason}}
                end

              :not_found ->
                {:halt, {:error, :cold_query_row_missing}}

              {:error, _reason} = error ->
                {:halt, error}

              invalid ->
                {:halt, {:error, {:invalid_query_row_read, invalid}}}
            end

          current_park == :not_found ->
            {:cont, {:ok, acc}}

          match?({:ok, _other}, current_park) ->
            {:cont, {:ok, acc}}

          match?({:error, _reason}, current_park) ->
            {:halt, current_park}

          true ->
            {:halt, {:error, {:invalid_lmdb_value, current_park}}}
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

  defp cold_relocation_ops(_entries, _park_values, _query_rows, _direction),
    do: {:error, :lmdb_result_count_mismatch}

  defp flow_relocation_ops(entries, query_rows, direction)
       when length(entries) == length(query_rows) do
    entries
    |> Enum.zip(query_rows)
    |> Enum.reduce_while({:ok, []}, fn {entry, current_query_row}, {:ok, acc} ->
      case flow_relocation_entry_ops(entry, current_query_row, direction) do
        {:ok, []} -> {:cont, {:ok, acc}}
        {:ok, ops} -> {:cont, {:ok, [ops | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, nested_ops} -> {:ok, nested_ops |> Enum.reverse() |> List.flatten()}
      {:error, _reason} = error -> error
    end
  end

  defp flow_relocation_ops(_entries, _query_rows, _direction),
    do: {:error, :lmdb_result_count_mismatch}

  defp flow_relocation_entry_ops(
         {:flow, state_key, file_id, old_offset, old_size, new_offset, new_size},
         {:ok, query_row},
         direction
       )
       when is_binary(query_row) do
    {source_offset, source_size, target_offset, target_size} =
      case direction do
        :forward -> {old_offset, old_size, new_offset, new_size}
        :reverse -> {new_offset, new_size, old_offset, old_size}
      end

    with {:ok, %{locator: %Locator{} = current}} <- QueryRowCodec.decode(query_row, state_key) do
      cond do
        physical_location?(current, file_id, target_offset, target_size) ->
          {:ok, []}

        physical_location?(current, file_id, source_offset, source_size) ->
          with {:ok, relocated} <-
                 Locator.relocate(current,
                   file_id: file_id,
                   offset: target_offset,
                   value_size: target_size
                 ),
               {:ok, relocated_query_row} <-
                 QueryRowCodec.relocate(query_row, state_key, current, relocated) do
            {:ok, [{:compare, state_key, query_row}, {:put, state_key, relocated_query_row}]}
          end

        true ->
          {:ok, []}
      end
    else
      :error -> {:error, :invalid_query_row}
    end
  end

  defp flow_relocation_entry_ops(
         {:flow, _state_key, _file_id, _old_offset, _old_size, _new_offset, _new_size},
         :not_found,
         _direction
       ),
       do: {:ok, []}

  defp flow_relocation_entry_ops(
         {:flow, _state_key, _file_id, _old_offset, _old_size, _new_offset, _new_size},
         {:error, _reason} = error,
         _direction
       ),
       do: error

  defp flow_relocation_entry_ops(_entry, invalid, _direction),
    do: {:error, {:invalid_query_row_read, invalid}}

  defp physical_location?(%Locator{} = locator, file_id, offset, value_size) do
    locator.file_id == file_id and locator.offset == offset and locator.value_size == value_size
  end

  defp query_row_matches?({:ok, query_row}, %{locator: %Locator{} = locator, park: park})
       when is_binary(query_row) and is_map(park) do
    case Map.get(park, :state_key) do
      state_key when is_binary(state_key) ->
        case QueryRowCodec.decode(query_row, state_key) do
          {:ok, %{locator: current}} -> Locator.same_physical_record?(current, locator)
          :error -> {:error, :invalid_query_row}
        end

      _invalid ->
        {:error, :state_key_mismatch}
    end
  end

  defp query_row_matches?(:not_found, _row), do: {:error, :cold_query_row_missing}
  defp query_row_matches?({:error, _reason} = error, _row), do: error
  defp query_row_matches?(_invalid, _row), do: {:error, :invalid_query_row_read}

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
