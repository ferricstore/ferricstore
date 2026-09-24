defmodule Ferricstore.Flow.ColdDuePrecheck do
  @moduledoc false

  alias Ferricstore.Flow.LMDB

  @max_rows 32

  # A negative proof is only valid when the entire cold-due key range fits the
  # bounded read. Rows of another type cannot satisfy this claim; a matching
  # due row whose park record is gone cannot be promoted. Any uncertainty keeps
  # the caller on the authoritative replicated claim path.
  def empty_for_type?(path, type) when is_binary(path) and is_binary(type) do
    with true <- Ferricstore.FS.dir?(path),
         false <- LMDB.flush_in_progress?(path),
         {:ok, rows} <-
           LMDB.prefix_entries_initialized(path, LMDB.cold_due_prefix(), @max_rows + 1),
         true <- length(rows) <= @max_rows,
         true <- Enum.all?(rows, &irrelevant_or_orphaned?(&1, path, type)),
         false <- LMDB.flush_in_progress?(path) do
      true
    else
      _uncertain -> false
    end
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  def empty_for_type?(_path, _type), do: false

  defp irrelevant_or_orphaned?({key, park_key}, path, type)
       when is_binary(key) and is_binary(park_key) and park_key != "" do
    case cold_due_type(key) do
      {:ok, ^type} -> LMDB.get(path, park_key) == :not_found
      {:ok, _other_type} -> true
      :error -> false
    end
  end

  defp irrelevant_or_orphaned?(_row, _path, _type), do: false

  defp cold_due_type(key) do
    case :binary.split(key, ":", [:global]) do
      [
        "flow",
        "due",
        "v1",
        _bucket,
        encoded_type,
        _state,
        _partition,
        _priority,
        _due_at,
        _flow_id,
        _version
      ] ->
        Base.url_decode64(encoded_type, padding: false)
        |> case do
          {:ok, type} when type != "" -> {:ok, type}
          _invalid -> :error
        end

      _other ->
        :error
    end
  end
end
