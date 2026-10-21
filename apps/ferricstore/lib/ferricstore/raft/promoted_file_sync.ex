defmodule Ferricstore.Raft.PromotedFileSync do
  @moduledoc """
  Lets a WARaft storage process write large collections' dedicated files
  without an fsync per write.

  Every acknowledged write is in the Raft log, so a dedicated-file write need
  not reach disk when it is applied, as long as recovery replays the log from
  before it. Recovery starts from the persisted replay position or from a
  segment projection checkpoint, and a log trim drops the entries before a
  checkpoint. So the storage process syncs the files it wrote (`sync/1`)
  before it persists a replay position, and a checkpoint or trim syncs them
  (`sync_tracked/1`) before it writes a checkpoint past those writes.

  The files written since the last sync are tracked in an ETS table owned by
  the storage process. Before the first untracked write, a marker file in the
  storage root is made durable; the owner removes it once every tracked file is
  synced. A storage that opens with the marker present lost its tracking (the
  node stopped before syncing), so it syncs every dedicated file before it
  replays the log.

  Writes are tracked by the process applying them, so a cross-shard
  transaction's writes into other shards' collections are synced before the
  applying shard persists past the transaction. A process that owns no table
  writes with an fsync, as every write did before.
  """

  alias Ferricstore.Bitcask.NIF
  # File operations here run in the WARaft apply path, so they go through NIFs
  # rather than wait on the shared Erlang file server.
  alias Ferricstore.FS

  @tracker_key {__MODULE__, :tracker}
  @marker_name "dedicated_unsynced"
  # Bitcask record header bytes; a put's record size is header + key + value.
  @record_header_size 26

  @type t :: %{table: :ets.tid(), marker: binary(), dedicated_root: binary()}

  @doc """
  Opens tracking for the storage at `root_dir` in the calling process.

  If a previous storage left its marker, every file under `data_dir`'s
  dedicated directory is synced first.
  """
  @spec open(binary(), binary()) :: {:ok, t()} | {:error, term()}
  def open(root_dir, data_dir) when is_binary(root_dir) and is_binary(data_dir) do
    marker = Path.join(root_dir, @marker_name)
    dedicated_root = Path.join(data_dir, "dedicated")

    with :ok <- recover_marker(marker, dedicated_root) do
      table = :ets.new(__MODULE__, [:set, :protected])
      tracker = %{table: table, marker: marker, dedicated_root: dedicated_root}
      :persistent_term.put(registry_key(root_dir), table)
      bind(tracker)
      {:ok, tracker}
    end
  end

  @doc "Makes `tracker` the calling process's tracker for dedicated-file writes."
  @spec bind(t() | nil) :: :ok
  def bind(nil) do
    Process.delete(@tracker_key)
    :ok
  end

  def bind(%{table: _table} = tracker) do
    Process.put(@tracker_key, tracker)
    :ok
  end

  @doc "Stops tracking for `root_dir` and drops the calling process's table."
  @spec close(binary(), t() | nil) :: :ok
  def close(root_dir, tracker) do
    case :persistent_term.get(registry_key(root_dir), nil) do
      table when is_map(tracker) and table == tracker.table ->
        :persistent_term.erase(registry_key(root_dir))

      _other ->
        :ok
    end

    if is_map(tracker) and owner?(tracker.table) do
      :ets.delete(tracker.table)
    end

    case Process.get(@tracker_key) do
      ^tracker -> Process.delete(@tracker_key)
      _other -> :ok
    end

    :ok
  end

  @doc "The tracking table registered for the storage at `root_dir`, if any."
  @spec registered_table(binary()) :: :ets.tid() | nil
  def registered_table(root_dir), do: :persistent_term.get(registry_key(root_dir), nil)

  defp registry_key(root_dir), do: {__MODULE__, Path.expand(root_dir)}

  @doc "Appends one record, unsynced and tracked when the caller has a tracker."
  @spec append_record(binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, term()} | term()
  def append_record(path, key, value, expire_at_ms) do
    case current() do
      nil ->
        NIF.v2_append_record(path, key, value, expire_at_ms)

      tracker ->
        tracked_append(tracker, path, fn ->
          case NIF.v2_append_ops_batch_nosync(path, [{:put, key, value, expire_at_ms}]) do
            {:ok, [{:put, offset, _value_size}]} ->
              {:ok, {offset, @record_header_size + byte_size(key) + byte_size(value)}}

            other ->
              other
          end
        end)
    end
  end

  @doc "Appends one tombstone, unsynced and tracked when the caller has a tracker."
  @spec append_tombstone(binary(), binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, term()} | term()
  def append_tombstone(path, key) do
    case current() do
      nil ->
        NIF.v2_append_tombstone(path, key)

      tracker ->
        tracked_append(tracker, path, fn ->
          case NIF.v2_append_ops_batch_nosync(path, [{:delete, key}]) do
            {:ok, [{:delete, offset, record_size}]} -> {:ok, {offset, record_size}}
            other -> other
          end
        end)
    end
  end

  @doc "Appends records, unsynced and tracked when the caller has a tracker."
  @spec append_batch(binary(), [{binary(), binary(), non_neg_integer()}]) ::
          {:ok, [{non_neg_integer(), non_neg_integer()}]} | {:error, term()} | term()
  def append_batch(path, entries) do
    case current() do
      nil ->
        NIF.v2_append_batch(path, entries)

      tracker ->
        tracked_append(tracker, path, fn -> NIF.v2_append_batch_nosync(path, entries) end)
    end
  end

  @doc """
  Syncs every tracked file and, once none is left, removes the marker.

  Only the owner untracks files, so a write that lands while another process
  syncs stays tracked.
  """
  @spec sync(t() | nil) :: :ok | {:error, term()}
  def sync(nil), do: :ok

  def sync(%{table: table, marker: marker} = tracker) do
    if owner?(table) do
      with :ok <- sync_rows(table, &:ets.delete_object(table, &1)) do
        remove_marker(table, marker)
      end
    else
      sync_tracked(tracker.table)
    end
  end

  @doc """
  Syncs the files tracked in `table` without untracking them, from any process.

  A table that no longer exists belonged to a storage that stopped before
  syncing, so this fails rather than vouch for its writes.
  """
  @spec sync_tracked(:ets.tid() | nil) :: :ok | {:error, term()}
  def sync_tracked(nil), do: :ok

  def sync_tracked(table) do
    if :ets.info(table, :owner) == :undefined do
      {:error, :promoted_file_sync_unavailable}
    else
      sync_rows(table, fn _row -> true end)
    end
  rescue
    ArgumentError -> {:error, :promoted_file_sync_unavailable}
  end

  @doc "True when `tracker` has no file written since its last sync."
  @spec synced?(t() | nil) :: boolean()
  def synced?(nil), do: true

  def synced?(%{table: table}) do
    :ets.match_object(table, {{:file, :_}, :_}, 1) == :"$end_of_table"
  rescue
    ArgumentError -> false
  end

  defp current do
    case Process.get(@tracker_key) do
      %{table: table} = tracker -> if owner?(table), do: tracker
      _none -> nil
    end
  end

  defp owner?(table), do: :ets.info(table, :owner) == self()

  defp tracked_append(%{table: table, marker: marker}, path, append) do
    with :ok <- ensure_marker(table, marker) do
      result = append.()
      :ets.insert(table, {{:file, path}, :erlang.unique_integer([:monotonic])})
      result
    end
  end

  defp ensure_marker(table, marker) do
    if :ets.member(table, :marker) do
      :ok
    else
      with :ok <- write_marker(marker) do
        :ets.insert(table, {:marker, true})
        :ok
      end
    end
  end

  defp write_marker(marker) do
    with :ok <- FS.touch(marker),
         :ok <- fsync_file(marker),
         :ok <- fsync_dir(Path.dirname(marker)) do
      :ok
    else
      {:error, reason} -> {:error, {:promoted_file_sync_marker_failed, reason}}
    end
  end

  defp remove_marker(table, marker) do
    if :ets.member(table, :marker) and synced?(%{table: table}) do
      case FS.rm(marker) do
        :ok -> :ets.delete(table, :marker)
        {:error, {:not_found, _message}} -> :ets.delete(table, :marker)
        {:error, _reason} -> true
      end
    end

    :ok
  end

  defp sync_rows(table, synced) do
    table
    |> :ets.match_object({{:file, :_}, :_})
    |> Enum.reduce_while(:ok, fn {{:file, path}, _version} = row, :ok ->
      case fsync_file(path) do
        :ok ->
          synced.(row)
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:promoted_file_fsync_failed, path, reason}}}
      end
    end)
  end

  defp recover_marker(marker, dedicated_root) do
    if FS.exists?(marker) do
      with :ok <- fsync_tree(dedicated_root) do
        case FS.rm(marker) do
          :ok -> :ok
          {:error, {:not_found, _message}} -> :ok
          {:error, reason} -> {:error, {:promoted_file_sync_marker_failed, reason}}
        end
      end
    else
      :ok
    end
  end

  defp fsync_tree(path) do
    cond do
      FS.dir?(path) ->
        case FS.ls(path) do
          {:ok, children} ->
            with :ok <-
                   Enum.reduce_while(children, :ok, fn child, :ok ->
                     case fsync_tree(Path.join(path, child)) do
                       :ok -> {:cont, :ok}
                       {:error, _reason} = error -> {:halt, error}
                     end
                   end) do
              fsync_dir(path)
            end

          {:error, {:not_found, _message}} ->
            :ok

          {:error, reason} ->
            {:error, {:promoted_file_fsync_failed, path, reason}}
        end

      FS.exists?(path) ->
        fsync_file(path)

      true ->
        :ok
    end
  end

  # A tracked file that is gone was merged by compaction, which syncs what it
  # keeps, or removed with its collection.
  defp fsync_file(path) do
    case call_fsync(:file, path) do
      :ok -> :ok
      {:error, reason} -> if FS.exists?(path), do: {:error, reason}, else: :ok
    end
  end

  defp fsync_dir(path) do
    case call_fsync(:dir, path) do
      :ok -> :ok
      {:error, reason} -> if FS.exists?(path), do: {:error, reason}, else: :ok
    end
  end

  if Mix.env() == :test do
    defp call_fsync(kind, path) do
      case Application.get_env(:ferricstore, :promoted_file_sync_fsync_hook) do
        hook when is_function(hook, 2) ->
          case hook.(kind, path) do
            :passthrough -> do_fsync(kind, path)
            result -> result
          end

        _missing ->
          do_fsync(kind, path)
      end
    end
  else
    defp call_fsync(kind, path), do: do_fsync(kind, path)
  end

  defp do_fsync(:file, path), do: normalize(NIF.v2_fsync(path))
  defp do_fsync(:dir, path), do: normalize(NIF.v2_fsync_dir(path))

  defp normalize(:ok), do: :ok
  defp normalize({:error, _reason} = error), do: error
  defp normalize(other), do: {:error, other}
end
