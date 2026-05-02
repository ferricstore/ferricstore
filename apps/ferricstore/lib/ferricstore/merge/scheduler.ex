defmodule Ferricstore.Merge.Scheduler do
  @moduledoc """
  Per-shard merge scheduler that triggers compaction when file rotation occurs.

  Each shard has its own `Scheduler` GenServer. Instead of polling every 30s
  with expensive `File.ls` calls that block the Shard GenServer, the scheduler
  is event-driven: the Shard notifies it on file rotation via `notify_rotation/2`.

  ## Merge modes

  The scheduler supports three merge modes per the spec (section 2E):

  * **Hot mode** -- Event-driven, can trigger anytime. When the file count
    reaches `min_files_for_merge` after a rotation, the scheduler attempts
    a merge (subject to the node-level semaphore). This is the default mode.

  * **Bulk mode** -- Only merges during a configurable time window (e.g.
    02:00-04:00). Outside the window, rotations are noted but no merge
    is triggered. Inside the window, file count is checked.

  * **Age mode** -- Like bulk mode but only merges files older than a
    configurable age threshold, within a time window.

  ## Merge lifecycle

  1. Shard rotates its active file and casts `{:file_rotated, file_count}`.
  2. Scheduler checks file count against `min_files_for_merge` and mode.
  3. If merge is needed, scheduler requests the node-level semaphore.
  4. If semaphore is acquired, scheduler writes a merge manifest.
  5. Scheduler selects non-active files for incremental merge.
  6. Scheduler calls the shard's `run_compaction` via GenServer.call.
  7. On completion, scheduler deletes the manifest and releases the semaphore.
  8. On failure, scheduler logs the error, deletes the manifest, releases semaphore.

  ## Configuration

  Configuration is passed via the `:merge` key in the application env:

      config :ferricstore, :merge,
        mode: :hot,
        min_files_for_merge: 2,
        max_files_per_merge: 10,
        merge_window: {2, 4},
        min_free_space_ratio: 0.1
  """

  use GenServer

  alias Ferricstore.Merge.{Manifest, Semaphore}
  alias Ferricstore.Store.Router

  require Logger

  # -------------------------------------------------------------------
  # Default configuration
  # -------------------------------------------------------------------

  @default_min_files_for_merge 2
  @default_max_files_per_merge 10
  @default_mode :hot
  @default_merge_window {2, 4}
  @default_min_free_space_ratio 0.1
  @default_fragmentation_threshold 0.5
  @default_dead_bytes_threshold 134_217_728
  @default_merge_cooldown_ms 60_000
  @default_small_file_threshold 10_485_760
  @default_merge_retry_interval_ms 5_000

  @type merge_mode :: :hot | :bulk | :age

  @type config :: %{
          mode: merge_mode(),
          min_files_for_merge: pos_integer(),
          max_files_per_merge: pos_integer(),
          merge_window: {non_neg_integer(), non_neg_integer()},
          min_free_space_ratio: float(),
          fragmentation_threshold: float(),
          dead_bytes_threshold: non_neg_integer(),
          merge_cooldown_ms: non_neg_integer(),
          small_file_threshold: non_neg_integer(),
          merge_retry_interval_ms: non_neg_integer()
        }

  defstruct [
    :shard_index,
    :config,
    :data_dir,
    :semaphore,
    merging: false,
    last_merge_at: nil,
    last_merge_completed_at: nil,
    last_merge_completed_mono_at: nil,
    merge_count: 0,
    total_bytes_reclaimed: 0,
    # Tracks the current file count from the last rotation notification.
    # Initialized to 0; updated by :file_rotated casts from the Shard.
    file_count: 0,
    # File IDs flagged by the shard as having high fragmentation.
    fragmentation_candidates: [],
    # Timer ref for retry after semaphore busy. Prevents stacking retries.
    retry_ref: nil
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a merge scheduler for the given shard.

  ## Options

    * `:shard_index` (required) -- zero-based shard index
    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:merge_config` -- override merge configuration (map)
    * `:semaphore` -- name or pid of the semaphore process (default: `Semaphore`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :shard_index)
    name = Keyword.get(opts, :name, scheduler_name(index))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the registered process name for the scheduler at `index`.
  """
  @spec scheduler_name(non_neg_integer()) :: atom()
  def scheduler_name(index), do: :"Ferricstore.Merge.Scheduler.#{index}"

  @doc """
  Called by the Shard when it rotates to a new active file.

  The `file_count` is the total number of log files (old + new active).
  This is the primary trigger for merge — no polling needed.
  """
  @spec notify_rotation(non_neg_integer(), non_neg_integer()) :: :ok
  def notify_rotation(shard_index, file_count) do
    name = scheduler_name(shard_index)

    try do
      GenServer.cast(name, {:file_rotated, file_count})
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @doc """
  Called by the Shard when per-file fragmentation exceeds thresholds.

  The `candidate_file_ids` are file IDs that have dead/total ratio above
  `fragmentation_threshold` AND dead bytes above `dead_bytes_threshold`.
  """
  @spec notify_fragmentation(non_neg_integer(), [non_neg_integer()], non_neg_integer()) :: :ok
  def notify_fragmentation(shard_index, candidate_file_ids, file_count) do
    name = scheduler_name(shard_index)

    try do
      GenServer.cast(name, {:fragmentation, candidate_file_ids, file_count})
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @doc """
  Returns the current status of the merge scheduler for observability.
  """
  @spec status(non_neg_integer() | GenServer.server()) :: map()
  def status(index_or_server) when is_integer(index_or_server) do
    GenServer.call(scheduler_name(index_or_server), :status)
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  @doc """
  Forces an immediate merge check, bypassing the event-driven trigger.
  Used in tests and for manual compaction via INFO/DEBUG commands.
  """
  @spec trigger_check(non_neg_integer() | GenServer.server()) :: :ok
  def trigger_check(index_or_server) when is_integer(index_or_server) do
    GenServer.call(scheduler_name(index_or_server), :trigger_check)
  end

  def trigger_check(server) do
    GenServer.call(server, :trigger_check)
  end

  @doc false
  @spec select_mergeable_file_ids([{non_neg_integer(), non_neg_integer()}], map(), [
          non_neg_integer()
        ]) ::
          {:ok, [non_neg_integer()]} | {:error, atom()}
  def select_mergeable_file_ids(file_sizes, config, frag_candidates \\ []) do
    pick_mergeable_files(file_sizes, config, frag_candidates)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  @impl true
  def init(opts) do
    index = Keyword.fetch!(opts, :shard_index)
    data_dir = Keyword.fetch!(opts, :data_dir)
    merge_config = Keyword.get(opts, :merge_config, %{})
    semaphore = Keyword.get(opts, :semaphore, Semaphore)

    config = build_config(merge_config)
    shard_data_dir = Ferricstore.DataDir.shard_data_path(data_dir, index)

    state = %__MODULE__{
      shard_index: index,
      config: config,
      data_dir: shard_data_dir,
      semaphore: semaphore
    }

    # Recover from any interrupted merge on startup.
    Manifest.recover_if_needed(shard_data_dir, index)

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      shard_index: state.shard_index,
      mode: state.config.mode,
      merging: state.merging,
      last_merge_at: state.last_merge_at,
      last_merge_completed_at: state.last_merge_completed_at,
      merge_count: state.merge_count,
      total_bytes_reclaimed: state.total_bytes_reclaimed,
      file_count: state.file_count,
      fragmentation_candidates: state.fragmentation_candidates,
      config: state.config
    }

    {:reply, status, state}
  end

  def handle_call(:trigger_check, _from, state) do
    new_state = maybe_merge(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:file_rotated, file_count}, state) do
    state = %{state | file_count: file_count}
    new_state = maybe_merge(state)
    {:noreply, new_state}
  end

  def handle_cast({:fragmentation, candidate_file_ids, file_count}, state) do
    state = %{state | fragmentation_candidates: candidate_file_ids, file_count: file_count}
    new_state = maybe_merge(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:retry_merge, state) do
    state = %{state | retry_ref: nil}
    new_state = maybe_merge(state)
    {:noreply, new_state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # -------------------------------------------------------------------
  # Private: merge decision logic
  # -------------------------------------------------------------------

  defp maybe_merge(%{merging: true} = state), do: state

  defp maybe_merge(state) do
    if should_merge?(state) do
      attempt_merge(state)
    else
      state
    end
  end

  defp should_merge?(state) do
    cooldown_ok =
      state.last_merge_completed_mono_at == nil or
        System.monotonic_time(:millisecond) - state.last_merge_completed_mono_at >=
          state.config.merge_cooldown_ms

    has_trigger =
      state.file_count >= state.config.min_files_for_merge or
        state.fragmentation_candidates != []

    cooldown_ok and has_trigger and mode_allows_merge?(state.config)
  end

  defp mode_allows_merge?(%{mode: :hot}), do: true

  defp mode_allows_merge?(%{mode: :bulk, merge_window: {start_hour, end_hour}}) do
    in_time_window?(start_hour, end_hour)
  end

  defp mode_allows_merge?(%{mode: :age, merge_window: {start_hour, end_hour}}) do
    in_time_window?(start_hour, end_hour)
  end

  defp in_time_window?(start_hour, end_hour) do
    {:ok, now} = DateTime.now("Etc/UTC")
    hour = now.hour

    if start_hour <= end_hour do
      hour >= start_hour and hour < end_hour
    else
      # Wraps around midnight, e.g. 22:00-04:00
      hour >= start_hour or hour < end_hour
    end
  end

  # -------------------------------------------------------------------
  # Private: merge execution
  # -------------------------------------------------------------------

  defp attempt_merge(state) do
    case Semaphore.acquire(state.shard_index, state.semaphore) do
      :ok ->
        state = cancel_retry(state)
        state = %{state | merging: true}
        do_merge(state)

      {:busy, _holder} ->
        Logger.debug(
          "Shard #{state.shard_index}: merge semaphore busy, scheduling retry"
        )

        schedule_retry(state)
    end
  end

  # Schedule a retry timer if one isn't already pending.
  defp schedule_retry(%{retry_ref: ref} = state) when ref != nil, do: state

  defp schedule_retry(state) do
    interval = state.config.merge_retry_interval_ms
    ref = Process.send_after(self(), :retry_merge, interval)
    %{state | retry_ref: ref}
  end

  # Cancel any pending retry timer.
  defp cancel_retry(%{retry_ref: nil} = state), do: state

  defp cancel_retry(%{retry_ref: ref} = state) do
    Process.cancel_timer(ref)
    %{state | retry_ref: nil}
  end

  defp do_merge(state) do
    ctx = FerricStore.Instance.get(:default)
    shard_name = Router.shard_name(ctx, state.shard_index)

    result =
      with {:ok, file_ids} <- select_files_for_merge(state, shard_name),
           :ok <- check_disk_space(state, shard_name, file_ids),
           :ok <- write_manifest(state, file_ids),
           {:ok, compaction_result} <- run_compaction(shard_name, file_ids) do
        {:ok, compaction_result, file_ids}
      end

    case result do
      {:ok, {written, dropped, reclaimed}, _file_ids} ->
        Logger.info(
          "Shard #{state.shard_index}: merge complete — " <>
            "#{written} records written, #{dropped} dropped, " <>
            "#{format_bytes(reclaimed)} reclaimed"
        )

        Manifest.delete(state.data_dir)
        Semaphore.release(state.shard_index, state.semaphore)

        now_ms = System.system_time(:millisecond)
        now_mono = System.monotonic_time(:millisecond)

        %{
          state
          | merging: false,
            last_merge_at: now_ms,
            last_merge_completed_at: now_ms,
            last_merge_completed_mono_at: now_mono,
            merge_count: state.merge_count + 1,
            total_bytes_reclaimed: state.total_bytes_reclaimed + reclaimed,
            fragmentation_candidates: []
        }

      {:error, reason} ->
        Logger.error(
          "Shard #{state.shard_index}: merge failed — #{inspect(reason)}"
        )

        Manifest.delete(state.data_dir)
        Semaphore.release(state.shard_index, state.semaphore)
        %{state | merging: false, fragmentation_candidates: []}
    end
  end

  defp select_files_for_merge(state, shard_name) do
    with {:ok, file_sizes} <- safe_call(shard_name, :file_sizes),
         {:ok, mergeable} <- pick_mergeable_files(file_sizes, state.config, state.fragmentation_candidates) do
      {:ok, mergeable}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp pick_mergeable_files([], _config, _frag_candidates), do: {:error, :no_files}

  defp pick_mergeable_files(file_sizes, config, frag_candidates) do
    {active_fid, _} = Enum.max_by(file_sizes, fn {fid, _size} -> fid end)

    non_active = Enum.reject(file_sizes, fn {fid, _size} -> fid == active_fid end)

    # Priority 1: files flagged by fragmentation
    frag_set = MapSet.new(frag_candidates)

    frag_files =
      non_active
      |> Enum.filter(fn {fid, _size} -> MapSet.member?(frag_set, fid) end)
      |> Enum.map(fn {fid, _size} -> fid end)

    # Priority 2: small files (below small_file_threshold) — always merge candidates
    small_files =
      non_active
      |> Enum.filter(fn {fid, size} ->
        not MapSet.member?(frag_set, fid) and size < config.small_file_threshold
      end)
      |> Enum.map(fn {fid, _size} -> fid end)

    # Priority 3: largest non-active files (existing logic)
    remaining_fids = MapSet.new(frag_files ++ small_files)

    by_size =
      non_active
      |> Enum.reject(fn {fid, _size} -> MapSet.member?(remaining_fids, fid) end)
      |> Enum.sort_by(fn {_fid, size} -> size end, :desc)
      |> Enum.map(fn {fid, _size} -> fid end)

    # Combine, dedup, cap at max_files_per_merge
    mergeable =
      (frag_files ++ small_files ++ by_size)
      |> Enum.uniq()
      |> Enum.take(config.max_files_per_merge)

    min_required =
      if frag_candidates != [] do
        # Fragmentation-triggered: merge even a single file
        1
      else
        # Rotation notifications count total log files, including the active
        # file. Selection never merges the active file, so require one fewer
        # non-active file here.
        max(config.min_files_for_merge - 1, 1)
      end

    if length(mergeable) >= min_required do
      {:ok, mergeable}
    else
      {:error, :not_enough_files}
    end
  end

  defp check_disk_space(state, shard_name, file_ids) do
    with {:ok, available} <- safe_call(shard_name, :available_disk_space),
         {:ok, file_sizes} <- safe_call(shard_name, :file_sizes) do
      # Sum the size of files being merged — worst case, the new merged file
      # is as large as all input files combined.
      input_bytes =
        file_sizes
        |> Enum.filter(fn {fid, _size} -> fid in file_ids end)
        |> Enum.reduce(0, fn {_fid, size}, acc -> acc + size end)

      if available > 0 and input_bytes / max(available, 1) > (1.0 - state.config.min_free_space_ratio) do
        Logger.warning(
          "Shard #{state.shard_index}: insufficient disk space for merge " <>
            "(need ~#{format_bytes(input_bytes)}, available #{format_bytes(available)})"
        )

        {:error, :insufficient_disk_space}
      else
        :ok
      end
    end
  end

  defp write_manifest(state, file_ids) do
    Manifest.write(state.data_dir, %{
      shard_index: state.shard_index,
      input_file_ids: file_ids
    })
  end

  defp run_compaction(shard_name, file_ids) do
    case safe_call(shard_name, {:run_compaction, file_ids}) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, {:compaction_failed, reason}}
    end
  end

  # -------------------------------------------------------------------
  # Private: helpers
  # -------------------------------------------------------------------

  defp build_config(overrides) do
    %{
      mode: Map.get(overrides, :mode, app_config(:mode, @default_mode)),
      min_files_for_merge:
        Map.get(
          overrides,
          :min_files_for_merge,
          app_config(:min_files_for_merge, @default_min_files_for_merge)
        ),
      max_files_per_merge:
        Map.get(
          overrides,
          :max_files_per_merge,
          app_config(:max_files_per_merge, @default_max_files_per_merge)
        ),
      merge_window:
        Map.get(overrides, :merge_window, app_config(:merge_window, @default_merge_window)),
      min_free_space_ratio:
        Map.get(
          overrides,
          :min_free_space_ratio,
          app_config(:min_free_space_ratio, @default_min_free_space_ratio)
        ),
      fragmentation_threshold:
        Map.get(
          overrides,
          :fragmentation_threshold,
          app_config(:fragmentation_threshold, @default_fragmentation_threshold)
        ),
      dead_bytes_threshold:
        Map.get(
          overrides,
          :dead_bytes_threshold,
          app_config(:dead_bytes_threshold, @default_dead_bytes_threshold)
        ),
      merge_cooldown_ms:
        Map.get(
          overrides,
          :merge_cooldown_ms,
          app_config(:merge_cooldown_ms, @default_merge_cooldown_ms)
        ),
      small_file_threshold:
        Map.get(
          overrides,
          :small_file_threshold,
          app_config(:small_file_threshold, @default_small_file_threshold)
        ),
      merge_retry_interval_ms:
        Map.get(
          overrides,
          :merge_retry_interval_ms,
          app_config(:merge_retry_interval_ms, @default_merge_retry_interval_ms)
        )
    }
  end

  defp app_config(key, default) do
    merge_config = Application.get_env(:ferricstore, :merge, [])

    case merge_config do
      config when is_list(config) -> Keyword.get(config, key, default)
      config when is_map(config) -> Map.get(config, key, default)
      _ -> default
    end
  end

  # Safe GenServer.call that catches exits (shard might be restarting).
  defp safe_call(name, msg) do
    GenServer.call(name, msg)
  catch
    :exit, reason ->
      {:error, {:shard_unavailable, reason}}
  end

  defp format_bytes(bytes) when bytes >= 1_073_741_824,
    do: "#{Float.round(bytes / 1_073_741_824, 2)} GB"

  defp format_bytes(bytes) when bytes >= 1_048_576,
    do: "#{Float.round(bytes / 1_048_576, 2)} MB"

  defp format_bytes(bytes) when bytes >= 1_024,
    do: "#{Float.round(bytes / 1_024, 2)} KB"

  defp format_bytes(bytes), do: "#{bytes} B"
end
