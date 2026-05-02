defmodule Ferricstore.Store.BitcaskWriter do
  @moduledoc """
  Background Bitcask writer for deferred persistence of small values.

  Each shard has one BitcaskWriter process. When the StateMachine applies a
  write for a small value (< hot_cache_max_value_size), it inserts the value
  into ETS immediately (for instant read availability) and sends a cast to
  this process with the key, value, and the active file path at the time of
  the write. The BitcaskWriter accumulates writes and flushes them to Bitcask
  in batches, then updates each ETS entry's file_id and offset via
  `:ets.update_element/3`.

  This decouples the ~50us synchronous NIF write from the ra_server process,
  allowing it to process the next Raft command immediately. The ETS entry is
  tagged with `file_id = :pending` until the background write completes.

  ## Batching strategy

  Writes are flushed when either:
    - The pending batch reaches 100 entries, OR
    - 1ms has elapsed since the first pending entry was queued

  ## File rotation handling

  The active file path is passed with each write cast, so the writer
  automatically handles file rotations -- writes to different paths are
  grouped and flushed separately.

  ## Invariants

    - A key with `file_id = :pending` in ETS has its value in ETS (non-nil).
      MemoryGuard must NOT evict these entries.
    - After the Bitcask write completes, the writer updates ETS positions
      5 (file_id), 6 (offset), and 7 (value_size) via `update_element`.
    - Large values (>= hot_cache_max_value_size) are NOT routed here -- they
      use the synchronous path in StateMachine because their ETS value is nil
      and cold reads need a valid disk offset immediately.
  """

  use GenServer

  require Logger

  alias Ferricstore.Bitcask.NIF

  @batch_size_threshold 2000
  @flush_interval_ms 1
  @error_retry_interval_ms 50

  @type flush_result :: :ok | {:error, {:flush_failed, pos_integer()}}

  @doc "Starts a BitcaskWriter for the given shard index."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: writer_name(shard_index))
  end

  @doc "Returns the registered name for a shard's BitcaskWriter."
  @spec writer_name(non_neg_integer()) :: atom()
  def writer_name(shard_index), do: :"Ferricstore.Store.BitcaskWriter.#{shard_index}"

  @doc """
  Queues a deferred Bitcask write for background processing.

  Called from `StateMachine.apply/3` for small values. The write is
  non-blocking (cast) so the ra_server process is not delayed.

  ## Parameters

    - `shard_index` -- zero-based shard index
    - `active_file_path` -- the active log file path at the time of the write
    - `active_file_id` -- the numeric file ID for the active log file
    - `ets_table` -- the ETS table name (keydir) to update after writing
    - `key` -- the key being written
    - `value` -- the value to persist (always a binary, always < 64KB)
    - `expire_at_ms` -- expiry timestamp in milliseconds (0 = no expiry)
  """
  @spec write(
          FerricStore.Instance.t() | nil,
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          atom(),
          binary(),
          binary(),
          non_neg_integer()
        ) :: :ok
  def write(
        instance_ctx,
        shard_index,
        active_file_path,
        active_file_id,
        ets_table,
        key,
        value,
        expire_at_ms
      ) do
    GenServer.cast(
      writer_name(shard_index),
      {:write, instance_ctx, active_file_path, active_file_id, ets_table, key, value,
       expire_at_ms}
    )
  end

  @spec write(
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          atom(),
          binary(),
          binary(),
          non_neg_integer()
        ) :: :ok
  def write(shard_index, active_file_path, active_file_id, ets_table, key, value, expire_at_ms) do
    write(nil, shard_index, active_file_path, active_file_id, ets_table, key, value, expire_at_ms)
  end

  @doc """
  Queues a batch of deferred Bitcask writes in a single cast.

  Same semantics as calling `write/7` for each entry, but sends one
  GenServer cast instead of N. Each entry is a tuple:
  `{active_file_path, active_file_id, ets_table, key, value, expire_at_ms}`.
  """
  @spec write_batch(non_neg_integer(), [
          {binary(), non_neg_integer(), atom(), binary(), binary(), non_neg_integer()}
        ]) :: :ok
  def write_batch(shard_index, entries) do
    GenServer.cast(
      writer_name(shard_index),
      {:write_batch, entries}
    )
  end

  @doc """
  Queues a deferred Bitcask tombstone (delete) for background processing.

  Called from `Router.async_delete/2` when raft is disabled. The tombstone
  is written in the same ordered batch as value writes, so a tombstone
  for key K will always appear after any preceding value write for K.
  The ETS entry has already been deleted by the caller before this cast.
  """
  @spec delete(non_neg_integer(), binary(), binary()) :: :ok
  def delete(shard_index, active_file_path, key) do
    delete(nil, shard_index, active_file_path, key)
  end

  @spec delete(FerricStore.Instance.t() | nil, non_neg_integer(), binary(), binary()) :: :ok
  def delete(instance_ctx, shard_index, active_file_path, key) do
    GenServer.cast(
      writer_name(shard_index),
      {:tombstone, instance_ctx, active_file_path, key}
    )
  end

  @doc """
  Synchronously flushes all pending writes to Bitcask and returns.

  Used in tests and before shard shutdown to ensure all deferred writes
  are persisted.
  """
  @spec flush(non_neg_integer()) :: flush_result()
  def flush(shard_index, timeout \\ 10_000) do
    try do
      GenServer.call(writer_name(shard_index), :flush, timeout)
    catch
      :exit, _ -> :ok
    end
  end

  @doc """
  Flushes all running BitcaskWriter processes.

  Iterates through shard indices 0..N-1 (default N=4) and flushes each
  writer that is alive. Silently ignores writers that are not running.
  Used in tests that need all background writes to be on disk before
  simulating eviction or verifying disk state.
  """
  @spec flush_all(non_neg_integer()) :: :ok
  def flush_all(shard_count \\ 4) do
    for i <- 0..(shard_count - 1) do
      try do
        flush(i)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  # -- Callbacks --

  @impl true
  def init(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)

    {:ok,
     %{
       shard_index: shard_index,
       pending: [],
       pending_count: 0,
       flush_timer: nil
     }}
  end

  @impl true
  def handle_cast({:write, instance_ctx, path, file_id, ets, key, value, expire_at_ms}, state) do
    entry = {:write, instance_ctx, path, file_id, ets, key, value, expire_at_ms}
    handle_entry(entry, state)
  end

  def handle_cast({:write, path, file_id, ets, key, value, expire_at_ms}, state) do
    entry = {:write, nil, path, file_id, ets, key, value, expire_at_ms}
    handle_entry(entry, state)
  end

  def handle_cast({:tombstone, instance_ctx, path, key}, state) do
    entry = {:tombstone, instance_ctx, path, key}
    handle_entry(entry, state)
  end

  def handle_cast({:tombstone, path, key}, state) do
    entry = {:tombstone, nil, path, key}
    handle_entry(entry, state)
  end

  def handle_cast({:write_batch, entries}, state) do
    new_pending =
      Enum.reduce(entries, state.pending, fn {path, fid, ets, key, value, exp}, acc ->
        [{:write, path, fid, ets, key, value, exp} | acc]
      end)

    new_count = state.pending_count + length(entries)

    if new_count >= @batch_size_threshold do
      {_result, state} = flush_pending_entries(new_pending, state)
      {:noreply, state}
    else
      timer =
        if state.flush_timer == nil do
          Process.send_after(self(), :flush_timer, @flush_interval_ms)
        else
          state.flush_timer
        end

      {:noreply, %{state | pending: new_pending, pending_count: new_count, flush_timer: timer}}
    end
  end

  # Drain the mailbox in one sweep so we amortize handle_cast overhead
  # across many messages. Under heavy load the mailbox had 270K+
  # backlogged casts and each round-trip through gen_server's dispatch
  # loop was pure overhead. Collecting all pending casts in a single
  # call lets us submit one large batch to the NIF per sweep.
  defp handle_entry(entry, state) do
    new_pending = [entry | state.pending]
    new_count = state.pending_count + 1

    {drained_pending, drained_count} = drain_mailbox(new_pending, new_count)

    if drained_count >= @batch_size_threshold do
      {_result, state} = flush_pending_entries(drained_pending, state)
      {:noreply, state}
    else
      timer =
        if state.flush_timer == nil do
          Process.send_after(self(), :flush_timer, @flush_interval_ms)
        else
          state.flush_timer
        end

      {:noreply,
       %{state | pending: drained_pending, pending_count: drained_count, flush_timer: timer}}
    end
  end

  # Pull all pending {:write, ...} / {:tombstone, ...} casts off the
  # mailbox and append to the batch. Stops when the mailbox is empty OR
  # a non-write message is at the head (handled normally by the next
  # dispatch).
  defp drain_mailbox(pending, count) do
    receive do
      {:"$gen_cast", {:write, instance_ctx, path, fid, ets, key, value, exp}} ->
        entry = {:write, instance_ctx, path, fid, ets, key, value, exp}
        drain_mailbox([entry | pending], count + 1)

      {:"$gen_cast", {:write, path, fid, ets, key, value, exp}} ->
        entry = {:write, nil, path, fid, ets, key, value, exp}
        drain_mailbox([entry | pending], count + 1)

      {:"$gen_cast", {:tombstone, instance_ctx, path, key}} ->
        drain_mailbox([{:tombstone, instance_ctx, path, key} | pending], count + 1)

      {:"$gen_cast", {:tombstone, path, key}} ->
        drain_mailbox([{:tombstone, nil, path, key} | pending], count + 1)
    after
      0 -> {pending, count}
    end
  end

  @impl true
  def handle_call(:flush, _from, %{pending: []} = state) do
    {:reply, :ok, state}
  end

  def handle_call(:flush, _from, state) do
    {result, state} = flush_pending_entries(state.pending, state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:flush_timer, %{pending: []} = state) do
    {:noreply, %{state | flush_timer: nil}}
  end

  def handle_info(:flush_timer, state) do
    {_result, state} = flush_pending_entries(state.pending, state)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Private --

  # Flushes all pending entries (writes and tombstones), grouped by file path
  # (handles file rotation mid-batch). For writes, uses v2_append_batch_nosync
  # and updates ETS entries with the real file_id and offset. For tombstones,
  # uses v2_append_tombstone. Entries within each path group are processed in
  # insertion order, preserving the write-then-delete invariant.
  defp do_flush(pending, shard_index) do
    # Reverse to preserve insertion order, then group by file path.
    entries = Enum.reverse(pending)

    failed =
      entries
      |> Enum.group_by(&entry_path/1)
      |> Enum.flat_map(fn {path, group} ->
        flush_path_group(path, group, shard_index)
      end)

    if failed == [], do: :ok, else: {:error, failed}
  end

  defp flush_pending_entries(pending, state) do
    cancel_timer(state.flush_timer)

    case do_flush(pending, state.shard_index) do
      :ok ->
        {:ok, %{state | pending: [], pending_count: 0, flush_timer: nil}}

      {:error, failed_entries} ->
        failed_count = length(failed_entries)
        timer = Process.send_after(self(), :flush_timer, @error_retry_interval_ms)

        {{:error, {:flush_failed, failed_count}},
         %{
           state
           | pending: Enum.reverse(failed_entries),
             pending_count: failed_count,
             flush_timer: timer
         }}
    end
  end

  defp entry_path({:write, _ctx, path, _fid, _ets, _key, _value, _exp}), do: path
  defp entry_path({:tombstone, _ctx, path, _key}), do: path
  # Legacy format (6-tuple without tag) for backward compatibility with
  # any in-flight casts that used the old format.
  defp entry_path({:write, path, _fid, _ets, _key, _value, _exp}), do: path
  defp entry_path({:tombstone, path, _key}), do: path
  defp entry_path({path, _fid, _ets, _key, _value, _exp}), do: path

  # Processes a group of entries for a single file path. Consecutive writes
  # are batched into a single v2_append_batch_nosync call for efficiency.
  # Tombstones are written individually via v2_append_tombstone.
  defp flush_path_group(path, group, shard_index) do
    # Split into contiguous runs of writes vs tombstones to maximize batching.
    group
    |> chunk_by_type()
    |> Enum.flat_map(fn
      {:writes, write_entries} ->
        flush_write_batch(path, write_entries, shard_index)

      {:tombstones, tombstone_entries} ->
        flush_tombstone_batch(path, tombstone_entries, shard_index)
    end)
  end

  # Chunks entries into contiguous runs of same-type entries.
  defp chunk_by_type(entries) do
    entries
    |> Enum.chunk_while(
      nil,
      fn entry, acc ->
        type = entry_type(entry)

        case acc do
          nil -> {:cont, {type, [entry]}}
          {^type, items} -> {:cont, {type, [entry | items]}}
          {other_type, items} -> {:cont, {other_type, Enum.reverse(items)}, {type, [entry]}}
        end
      end,
      fn
        nil -> {:cont, nil}
        {type, items} -> {:cont, {type, Enum.reverse(items)}, nil}
      end
    )
    |> Enum.map(fn {type, items} ->
      case type do
        :write -> {:writes, items}
        :tombstone -> {:tombstones, items}
      end
    end)
  end

  defp entry_type({:write, _, _, _, _, _, _, _}), do: :write
  defp entry_type({:write, _, _, _, _, _, _}), do: :write
  defp entry_type({:tombstone, _, _, _}), do: :tombstone
  defp entry_type({:tombstone, _, _}), do: :tombstone
  defp entry_type({_, _, _, _, _, _}), do: :write

  defp flush_write_batch(path, write_entries, shard_index) do
    batch =
      Enum.map(write_entries, fn
        {:write, _ctx, _path, _fid, _ets, key, value, exp} -> {key, to_binary(value), exp}
        {:write, _path, _fid, _ets, key, value, exp} -> {key, to_binary(value), exp}
        {_path, _fid, _ets, key, value, exp} -> {key, to_binary(value), exp}
      end)

    case NIF.v2_append_batch_nosync(path, batch) do
      {:ok, locations} ->
        mark_checkpoint_dirty(write_entries, shard_index)

        # Update ETS entries with real file_id and offset.
        Enum.zip(write_entries, locations)
        |> Enum.each(fn {entry, {offset, _record_size}} ->
          {_tag, _ctx, _path, file_id, ets, key, value, exp} = normalize_write_entry(entry)
          bin_value = to_binary(value)
          vsize = byte_size(bin_value)

          # Only update if the key still has the same pending value/expiry
          # this writer persisted. Newer pending writes for the same key must
          # not inherit this older disk location.
          try do
            :ets.select_replace(ets, [
              {
                {key, bin_value, exp, :"$1", :pending, :_, :_},
                [],
                [{{key, bin_value, exp, :"$1", file_id, offset, vsize}}]
              }
            ])
          rescue
            ArgumentError -> :ok
          end
        end)

        []

      {:error, reason} ->
        mark_disk_pressure(write_entries, shard_index)

        Logger.error(
          "BitcaskWriter shard_#{shard_index}: flush failed for #{path}: #{inspect(reason)} — retrying #{length(batch)} entries"
        )

        write_entries
    end
  end

  defp flush_tombstone_batch(path, tombstone_entries, shard_index) do
    {dirty_contexts, failed_entries} =
      Enum.reduce(tombstone_entries, {[], []}, fn entry, {dirty_acc, failed_acc} ->
        {instance_ctx, key} = normalize_tombstone_entry(entry)

        case NIF.v2_append_tombstone(path, key) do
          {:ok, _} ->
            {[instance_ctx | dirty_acc], failed_acc}

          {:error, reason} ->
            set_disk_pressure(instance_ctx, shard_index)

            Logger.error(
              "BitcaskWriter shard_#{shard_index}: tombstone failed for #{path} key=#{inspect(key)}: #{inspect(reason)}"
            )

            {dirty_acc, [entry | failed_acc]}
        end
      end)

    mark_checkpoint_dirty_contexts(dirty_contexts, shard_index)
    Enum.reverse(failed_entries)
  end

  defp normalize_write_entry({:write, ctx, path, fid, ets, key, value, exp}),
    do: {:write, ctx, path, fid, ets, key, value, exp}

  defp normalize_write_entry({:write, path, fid, ets, key, value, exp}),
    do: {:write, nil, path, fid, ets, key, value, exp}

  defp normalize_write_entry({path, fid, ets, key, value, exp}),
    do: {:write, nil, path, fid, ets, key, value, exp}

  defp normalize_tombstone_entry({:tombstone, ctx, _path, key}), do: {ctx, key}
  defp normalize_tombstone_entry({:tombstone, _path, key}), do: {nil, key}

  defp mark_disk_pressure(entries, shard_index) do
    entries
    |> Enum.map(fn entry ->
      {_tag, ctx, _path, _fid, _ets, _key, _value, _exp} = normalize_write_entry(entry)
      ctx
    end)
    |> Enum.uniq()
    |> Enum.each(&set_disk_pressure(&1, shard_index))
  end

  defp set_disk_pressure(nil, shard_index), do: Ferricstore.Store.DiskPressure.set(shard_index)

  defp set_disk_pressure(ctx, shard_index),
    do: Ferricstore.Store.DiskPressure.set(ctx, shard_index)

  defp mark_checkpoint_dirty(entries, shard_index) do
    entries
    |> Enum.map(fn entry ->
      {_tag, ctx, _path, _fid, _ets, _key, _value, _exp} = normalize_write_entry(entry)
      ctx
    end)
    |> mark_checkpoint_dirty_contexts(shard_index)
  end

  defp mark_checkpoint_dirty_contexts(contexts, shard_index) do
    contexts
    |> Enum.uniq()
    |> Enum.each(&mark_checkpoint_dirty_context(&1, shard_index))
  end

  defp mark_checkpoint_dirty_context(nil, _shard_index), do: :ok

  defp mark_checkpoint_dirty_context(%{checkpoint_flags: flags}, shard_index) do
    :atomics.put(flags, shard_index + 1, 1)
  rescue
    _ -> :ok
  end

  defp mark_checkpoint_dirty_context(_ctx, _shard_index), do: :ok

  defp to_binary(v) when is_binary(v), do: v
  defp to_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_binary(v) when is_float(v), do: Float.to_string(v)

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) do
    Process.cancel_timer(ref)
    # Flush any already-sent :flush_timer message from the mailbox.
    receive do
      :flush_timer -> :ok
    after
      0 -> :ok
    end
  end
end
