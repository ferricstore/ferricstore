defmodule Ferricstore.Flow.HistoryProjector.Files do
  @moduledoc false

  # A shard's Flow history log is a series of files, history/<id>.log, that
  # recovery replays in id order. The projector appends to the file with the
  # highest id. Once that file holds max_bytes, the next one starts at the next
  # multiple of @slot, so a single file never grows without bound. The ids in
  # between are left free so that a closed file can be rewritten into the id
  # after it and keep its place in replay order.

  alias Ferricstore.Bitcask.NIF

  @slot 1_000
  @default_max_bytes 128 * 1_024 * 1_024
  @min_max_bytes 1_024

  @spec slot() :: pos_integer()
  def slot, do: @slot

  @doc "Size at which the projector starts a new history file."
  @spec max_bytes() :: pos_integer()
  def max_bytes do
    case Application.get_env(:ferricstore, :flow_history_log_max_bytes, @default_max_bytes) do
      bytes when is_integer(bytes) and bytes >= @min_max_bytes -> bytes
      _invalid -> @default_max_bytes
    end
  end

  @spec dir(binary()) :: binary()
  def dir(shard_data_path), do: Path.join(shard_data_path, "history")

  @spec path(binary(), non_neg_integer()) :: binary()
  def path(shard_data_path, file_id) when is_integer(file_id) and file_id >= 0 do
    Path.join(
      dir(shard_data_path),
      "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
    )
  end

  @doc "The ids of the history files, ascending. A missing directory has none."
  @spec list(binary()) :: {:ok, [non_neg_integer()]} | {:error, term()}
  def list(shard_data_path) do
    case Ferricstore.FS.ls(dir(shard_data_path)) do
      {:ok, names} -> {:ok, names |> Enum.flat_map(&file_id/1) |> Enum.sort()}
      {:error, {:not_found, _message}} -> {:ok, []}
      {:error, reason} -> {:error, {:history_dir_list_failed, reason}}
    end
  end

  defp file_id(name) do
    with ".log" <- Path.extname(name),
         digits = Path.rootname(name),
         true <- byte_size(digits) >= 5,
         {id, ""} <- Integer.parse(digits),
         true <- id >= 0 and name == Path.basename(path("", id)) do
      [id]
    else
      _other -> []
    end
  end

  @doc "The id of the file new history records go to: the highest, or 0 when none exists."
  @spec active(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def active(shard_data_path) do
    with {:ok, ids} <- list(shard_data_path) do
      {:ok, List.last(ids, 0)}
    end
  end

  @spec next_slot(non_neg_integer()) :: pos_integer()
  def next_slot(file_id), do: (div(file_id, @slot) + 1) * @slot

  @doc """
  The file to append to: the active one, or a new one at the next slot once
  the active one holds `max_bytes`. A new file is created and its directory
  synced before it is returned, so recovery finds it after a crash.
  """
  @spec append_target(binary(), pos_integer()) ::
          {:ok, non_neg_integer(), binary()}
          | {:rotated, non_neg_integer(), binary(), non_neg_integer()}
          | {:error, term()}
  def append_target(shard_data_path, max_bytes) do
    with {:ok, active_id} <- active(shard_data_path),
         active_path = path(shard_data_path, active_id),
         {:ok, %{type: :regular, size: size}} <- File.lstat(active_path) do
      if size >= max_bytes do
        rotate(shard_data_path, active_id, active_path)
      else
        {:ok, active_id, active_path}
      end
    else
      {:ok, %{type: type}} -> {:error, {:invalid_history_file_type, type}}
      {:error, _reason} = error -> error
    end
  end

  defp rotate(shard_data_path, active_id, active_path) do
    next_id = next_slot(active_id)
    next_path = path(shard_data_path, next_id)

    with :ok <- NIF.v2_fsync(active_path),
         :ok <- Ferricstore.FS.touch(next_path),
         :ok <- NIF.v2_fsync_dir(dir(shard_data_path)) do
      {:rotated, next_id, next_path, active_id}
    end
  end
end
