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
        min_file_age_ms: 3_600_000,
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
  @default_min_file_age_ms 3_600_000
  @default_small_file_threshold 10_485_760
  @default_merge_retry_interval_ms 5_000
  @default_compaction_call_timeout_ms :infinity
  @trigger_check_timeout_ms 300_000

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
          min_file_age_ms: non_neg_integer(),
          small_file_threshold: non_neg_integer(),
          merge_retry_interval_ms: non_neg_integer(),
          compaction_call_timeout_ms: pos_integer() | :infinity
        }

  defstruct [
    :shard_index,
    :config,
    :data_dir,
    :semaphore,
    :instance_ctx,
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
    retry_ref: nil,
    # Compaction runs outside the scheduler mailbox. The scheduler retains
    # completion accounting while the worker owns the node-level semaphore.
    merge_worker: nil,
    fragmentation_generation: 0
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
  Returns the scheduler name for an instance-scoped shard.

  The default instance keeps the historic global names; embedded instances use
  their module name so rotation/fragmentation notifications cannot hit the
  default instance's scheduler.
  """
  @spec scheduler_name(map() | nil, non_neg_integer()) :: atom()
  def scheduler_name(nil, index), do: scheduler_name(index)
  def scheduler_name(%{name: :default}, index), do: scheduler_name(index)
  def scheduler_name(%{name: name}, index), do: :"#{name}.Merge.Scheduler.#{index}"

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
    GenServer.call(scheduler_name(index_or_server), :trigger_check, @trigger_check_timeout_ms)
  end

  def trigger_check(server) do
    GenServer.call(server, :trigger_check, @trigger_check_timeout_ms)
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
    instance_ctx = Keyword.get(opts, :instance_ctx)

    with {:ok, config} <- build_config(merge_config) do
      shard_data_dir = Ferricstore.DataDir.shard_data_path(data_dir, index)

      # Recover from any interrupted merge on startup. Fail closed if recovery
      # cannot prove partial files were cleaned up; starting the scheduler with
      # an unknown merge state can make later compactions act on stale files.
      case Manifest.recover_if_needed(shard_data_dir, index) do
        :ok ->
          case count_existing_log_files(shard_data_dir) do
            {:ok, file_count} ->
              state = %__MODULE__{
                shard_index: index,
                config: config,
                data_dir: shard_data_dir,
                semaphore: semaphore,
                instance_ctx: instance_ctx,
                file_count: file_count
              }

              {:ok, state}

            {:error, reason} ->
              {:stop, {:merge_log_file_count_failed, index, reason}}
          end

        {:error, reason} ->
          {:stop, {:merge_manifest_recovery_failed, index, reason}}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
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
    state = %{
      state
      | fragmentation_candidates: candidate_file_ids,
        file_count: file_count,
        fragmentation_generation: state.fragmentation_generation + 1
    }

    new_state = maybe_merge(state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:retry_merge, state) do
    state = %{state | retry_ref: nil}
    new_state = maybe_merge(state)
    {:noreply, new_state}
  end

  def handle_info(
        {:merge_finished, work_ref, result},
        %{merge_worker: %{work_ref: work_ref} = worker} = state
      ) do
    Process.demonitor(worker.monitor_ref, [:flush])
    {:noreply, finish_merge(state, worker, result)}
  end

  def handle_info(
        {:DOWN, monitor_ref, :process, pid, reason},
        %{merge_worker: %{monitor_ref: monitor_ref, pid: pid}} = state
      ) do
    Logger.error("Shard #{state.shard_index}: merge worker failed — #{inspect(reason)}")

    state = %{state | merging: false, merge_worker: nil}

    # An unexpected worker exit can leave a manifest or partial output behind.
    # Restart the scheduler so init performs fail-closed manifest recovery
    # before another compaction is attempted.
    {:stop, {:merge_worker_failed, state.shard_index, reason}, state}
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
      maybe_schedule_deferred_merge(state)
    end
  end

  defp should_merge?(state) do
    cooldown_remaining_ms(state) == 0 and merge_triggered?(state) and
      mode_allows_merge?(state.config) and
      age_mode_has_mergeable_files?(state)
  end

  defp maybe_schedule_deferred_merge(state) do
    cond do
      not merge_triggered?(state) ->
        state

      cooldown_remaining_ms(state) > 0 ->
        schedule_retry_after(state, cooldown_remaining_ms(state))

      not mode_allows_merge?(state.config) ->
        schedule_retry(state)

      state.config.mode == :age ->
        schedule_age_retry(state)

      true ->
        state
    end
  end

  defp merge_triggered?(state) do
    state.file_count >= state.config.min_files_for_merge or
      state.fragmentation_candidates != []
  end

  defp cooldown_remaining_ms(%{last_merge_completed_mono_at: nil}), do: 0

  defp cooldown_remaining_ms(state) do
    elapsed_ms =
      System.monotonic_time(:millisecond) - state.last_merge_completed_mono_at

    max(state.config.merge_cooldown_ms - elapsed_ms, 0)
  end

  defp schedule_age_retry(state) do
    case next_age_retry_delay_ms(state) do
      {:ok, delay_ms} ->
        state
        |> cancel_retry()
        |> schedule_retry_after(delay_ms)

      {:error, _reason} ->
        schedule_retry(state)

      :none ->
        state
    end
  end

  defp next_age_retry_delay_ms(state) do
    with {:ok, file_mtimes} <- log_file_ids_with_mtime_ms(state.data_dir),
         [_ | _] <- file_mtimes do
      {active_fid, _mtime_ms} = Enum.max_by(file_mtimes, fn {fid, _mtime_ms} -> fid end)
      now_ms = System.system_time(:millisecond)

      delays =
        file_mtimes
        |> Enum.reject(fn {fid, _mtime_ms} -> fid == active_fid end)
        |> Enum.map(fn {_fid, mtime_ms} ->
          mtime_ms + state.config.min_file_age_ms - now_ms
        end)
        |> Enum.filter(&(&1 > 0))

      case delays do
        [] -> :none
        delays -> {:ok, max(Enum.min(delays), 1)}
      end
    else
      [] -> :none
      {:error, reason} -> {:error, reason}
    end
  end

  defp age_mode_has_mergeable_files?(%{config: %{mode: :age}} = state) do
    with {:ok, file_sizes} <- log_file_sizes(state.data_dir),
         {:ok, file_sizes} <- filter_age_mode_files(file_sizes, state.data_dir, state.config),
         {:ok, _mergeable} <-
           pick_mergeable_files(file_sizes, state.config, state.fragmentation_candidates) do
      true
    else
      _ -> false
    end
  end

  defp age_mode_has_mergeable_files?(_state), do: true

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
    state = cancel_retry(state)
    parent = self()
    work_ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result = run_merge_attempt(state)
        send(parent, {:merge_finished, work_ref, result})
      end)

    worker = %{
      pid: pid,
      monitor_ref: monitor_ref,
      work_ref: work_ref,
      file_count: state.file_count,
      fragmentation_generation: state.fragmentation_generation
    }

    %{state | merging: true, merge_worker: worker}
  end

  # Schedule a retry timer if one isn't already pending.
  defp schedule_retry(%{retry_ref: ref} = state) when ref != nil, do: state

  defp schedule_retry(state) do
    schedule_retry_after(state, state.config.merge_retry_interval_ms)
  end

  defp schedule_retry_after(%{retry_ref: ref} = state, _interval) when ref != nil, do: state

  defp schedule_retry_after(state, interval) do
    ref = Process.send_after(self(), :retry_merge, max(interval, 0))
    %{state | retry_ref: ref}
  end

  # Cancel any pending retry timer.
  defp cancel_retry(%{retry_ref: nil} = state), do: state

  defp cancel_retry(%{retry_ref: ref} = state) do
    Process.cancel_timer(ref)
    %{state | retry_ref: nil}
  end

  defp run_merge_attempt(state) do
    case Semaphore.acquire(state.shard_index, state.semaphore) do
      :ok ->
        try do
          result = merge_work(state)
          _ = Manifest.delete(state.data_dir)
          {:attempted, result}
        after
          _ = Semaphore.release(state.shard_index, state.semaphore)
        end

      {:busy, holder} ->
        {:busy, holder}
    end
  end

  defp merge_work(state) do
    ctx = state.instance_ctx || FerricStore.Instance.get(:default)
    shard_name = Router.shard_name(ctx, state.shard_index)

    with {:ok, file_ids} <- select_files_for_merge(state, shard_name),
         :ok <- check_disk_space(state, shard_name, file_ids),
         :ok <- write_manifest(state, file_ids),
         {:ok, compaction_result} <- run_compaction(state, shard_name, file_ids) do
      {:ok, compaction_result, file_ids}
    end
  end

  defp finish_merge(state, _worker, {:busy, _holder}) do
    Logger.debug("Shard #{state.shard_index}: merge semaphore busy, scheduling retry")

    state
    |> Map.put(:merging, false)
    |> Map.put(:merge_worker, nil)
    |> schedule_retry()
  end

  defp finish_merge(state, worker, {:attempted, result}) do
    case result do
      {:ok, {written, dropped, reclaimed}, _file_ids} ->
        Logger.info(
          "Shard #{state.shard_index}: merge complete — " <>
            "#{written} records written, #{dropped} dropped, " <>
            "#{format_bytes(reclaimed)} reclaimed"
        )

        now_ms = System.system_time(:millisecond)
        now_mono = System.monotonic_time(:millisecond)

        pending_fragmentation? =
          state.fragmentation_generation != worker.fragmentation_generation

        fragmentation_candidates =
          if pending_fragmentation?, do: state.fragmentation_candidates, else: []

        pending_rotation? = state.file_count != worker.file_count

        next_state = %{
          state
          | merging: false,
            merge_worker: nil,
            last_merge_at: now_ms,
            last_merge_completed_at: now_ms,
            last_merge_completed_mono_at: now_mono,
            merge_count: state.merge_count + 1,
            total_bytes_reclaimed: state.total_bytes_reclaimed + reclaimed,
            fragmentation_candidates: fragmentation_candidates
        }

        if pending_fragmentation? or pending_rotation? do
          schedule_retry_after(next_state, state.config.merge_cooldown_ms)
        else
          next_state
        end

      {:error, reason} ->
        Logger.error("Shard #{state.shard_index}: merge failed — #{inspect(reason)}")

        pending_fragmentation? =
          state.fragmentation_generation != worker.fragmentation_generation

        state = %{state | merging: false, merge_worker: nil}

        if retryable_merge_error?(reason) do
          schedule_retry(state)
        else
          state =
            if pending_fragmentation?, do: state, else: %{state | fragmentation_candidates: []}

          if pending_fragmentation? or state.file_count != worker.file_count do
            schedule_retry_after(state, state.config.merge_cooldown_ms)
          else
            state
          end
        end
    end
  end

  defp retryable_merge_error?(:no_files), do: false
  defp retryable_merge_error?(:not_enough_files), do: false
  defp retryable_merge_error?({:compaction_failed, {:no_compactable_files, _file_ids}}), do: false
  defp retryable_merge_error?(_reason), do: true

  defp select_files_for_merge(state, _shard_name) do
    with {:ok, file_sizes} <- log_file_sizes(state.data_dir),
         {:ok, file_sizes} <- filter_age_mode_files(file_sizes, state.data_dir, state.config),
         {:ok, mergeable} <-
           pick_mergeable_files(file_sizes, state.config, state.fragmentation_candidates) do
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
         {:ok, file_sizes} <- log_file_sizes(state.data_dir) do
      # Sum the size of files being merged — worst case, the new merged file
      # is as large as all input files combined.
      input_bytes =
        file_sizes
        |> Enum.filter(fn {fid, _size} -> fid in file_ids end)
        |> Enum.reduce(0, fn {_fid, size}, acc -> acc + size end)

      if available > 0 and
           input_bytes / max(available, 1) > 1.0 - state.config.min_free_space_ratio do
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

  defp run_compaction(state, shard_name, file_ids) do
    case safe_call(
           shard_name,
           {:run_compaction, file_ids},
           state.config.compaction_call_timeout_ms
         ) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, {:compaction_failed, reason}}
    end
  end

  # -------------------------------------------------------------------
  # Private: helpers
  # -------------------------------------------------------------------

  defp build_config(overrides) when is_map(overrides) do
    config = %{
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
      min_file_age_ms:
        Map.get(
          overrides,
          :min_file_age_ms,
          app_config(:min_file_age_ms, @default_min_file_age_ms)
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
        ),
      compaction_call_timeout_ms:
        Map.get(
          overrides,
          :compaction_call_timeout_ms,
          app_config(:compaction_call_timeout_ms, @default_compaction_call_timeout_ms)
        )
    }

    with :ok <- validate_config(config) do
      {:ok,
       %{
         config
         | min_free_space_ratio: config.min_free_space_ratio * 1.0,
           fragmentation_threshold: config.fragmentation_threshold * 1.0,
           compaction_call_timeout_ms:
             normalize_compaction_call_timeout(config.compaction_call_timeout_ms)
       }}
    end
  end

  defp build_config(overrides),
    do: {:error, {:invalid_merge_config, :merge_config, overrides}}

  defp validate_config(config) do
    with :ok <- valid_config_value(:mode, config.mode, &(&1 in [:hot, :bulk, :age])),
         :ok <-
           valid_config_value(
             :min_files_for_merge,
             config.min_files_for_merge,
             &(is_integer(&1) and &1 >= 2)
           ),
         :ok <-
           valid_config_value(
             :max_files_per_merge,
             config.max_files_per_merge,
             &(is_integer(&1) and &1 > 0)
           ),
         :ok <- valid_config_value(:merge_window, config.merge_window, &valid_merge_window?/1),
         :ok <-
           valid_config_value(
             :min_free_space_ratio,
             config.min_free_space_ratio,
             &valid_ratio?(&1, true)
           ),
         :ok <-
           valid_config_value(
             :fragmentation_threshold,
             config.fragmentation_threshold,
             &valid_ratio?(&1, false)
           ),
         :ok <-
           valid_config_value(
             :dead_bytes_threshold,
             config.dead_bytes_threshold,
             &non_negative_integer?/1
           ),
         :ok <-
           valid_config_value(
             :merge_cooldown_ms,
             config.merge_cooldown_ms,
             &non_negative_integer?/1
           ),
         :ok <-
           valid_config_value(
             :min_file_age_ms,
             config.min_file_age_ms,
             &non_negative_integer?/1
           ),
         :ok <-
           valid_config_value(
             :small_file_threshold,
             config.small_file_threshold,
             &non_negative_integer?/1
           ),
         :ok <-
           valid_config_value(
             :merge_retry_interval_ms,
             config.merge_retry_interval_ms,
             &(is_integer(&1) and &1 > 0)
           ),
         :ok <-
           valid_config_value(
             :compaction_call_timeout_ms,
             config.compaction_call_timeout_ms,
             &(&1 == :infinity or (is_integer(&1) and &1 > 0))
           ) do
      :ok
    end
  end

  defp valid_config_value(key, value, predicate) do
    if predicate.(value), do: :ok, else: {:error, {:invalid_merge_config, key, value}}
  end

  defp valid_merge_window?({start_hour, end_hour}) do
    is_integer(start_hour) and start_hour in 0..23 and is_integer(end_hour) and
      end_hour in 0..24 and start_hour != end_hour
  end

  defp valid_merge_window?(_window), do: false

  defp valid_ratio?(value, zero_allowed?) when is_number(value) do
    lower_bound_ok = if zero_allowed?, do: value >= 0, else: value > 0
    lower_bound_ok and value <= 1
  end

  defp valid_ratio?(_value, _zero_allowed?), do: false

  defp non_negative_integer?(value), do: is_integer(value) and value >= 0

  defp filter_age_mode_files(file_sizes, _data_dir, %{mode: mode}) when mode != :age,
    do: {:ok, file_sizes}

  defp filter_age_mode_files([], _data_dir, %{mode: :age}), do: {:ok, []}

  defp filter_age_mode_files(file_sizes, data_dir, %{mode: :age, min_file_age_ms: min_age_ms}) do
    {active_fid, _} = Enum.max_by(file_sizes, fn {fid, _size} -> fid end)

    with {:ok, eligible} <- age_eligible_file_ids(data_dir, min_age_ms) do
      {:ok,
       Enum.filter(file_sizes, fn {fid, _size} ->
         fid == active_fid or MapSet.member?(eligible, fid)
       end)}
    end
  end

  defp age_eligible_file_ids(shard_data_dir, min_age_ms) when min_age_ms <= 0 do
    with {:ok, file_mtimes} <- log_file_ids_with_mtime_ms(shard_data_dir) do
      {:ok, file_mtimes |> Enum.map(fn {fid, _mtime_ms} -> fid end) |> MapSet.new()}
    end
  end

  defp age_eligible_file_ids(shard_data_dir, min_age_ms) do
    cutoff_ms = System.system_time(:millisecond) - min_age_ms

    with {:ok, file_mtimes} <- log_file_ids_with_mtime_ms(shard_data_dir) do
      eligible =
        file_mtimes
        |> Enum.filter(fn {_fid, mtime_ms} -> mtime_ms <= cutoff_ms end)
        |> Enum.map(fn {fid, _mtime_ms} -> fid end)
        |> MapSet.new()

      {:ok, eligible}
    end
  end

  defp log_file_ids_with_mtime_ms(shard_data_dir) do
    case Ferricstore.FS.ls(shard_data_dir) do
      {:ok, entries} ->
        Enum.reduce_while(entries, {:ok, []}, fn name, {:ok, acc} ->
          if bitcask_log_file?(name) do
            {file_id, ""} = name |> Path.rootname() |> Integer.parse()
            path = Path.join(shard_data_dir, name)

            case File.stat(path, time: :posix) do
              {:ok, %{mtime: mtime_s}} ->
                {:cont, {:ok, [{file_id, mtime_s * 1_000} | acc]}}

              {:error, reason} ->
                {:halt, {:error, {:log_file_stat_failed, path, reason}}}
            end
          else
            {:cont, {:ok, acc}}
          end
        end)

      {:error, reason} ->
        {:error, {:log_file_list_failed, reason}}
    end
  end

  # A timed-out GenServer.call does not cancel work already running inside the
  # shard. Keeping this infinite preserves the node-level merge semaphore until
  # compaction actually finishes or the shard process exits.
  defp normalize_compaction_call_timeout(_timeout), do: :infinity

  defp app_config(key, default) do
    merge_config = Application.get_env(:ferricstore, :merge, [])

    case merge_config do
      config when is_list(config) -> Keyword.get(config, key, default)
      config when is_map(config) -> Map.get(config, key, default)
      _ -> default
    end
  end

  defp count_existing_log_files(shard_data_dir) do
    case Ferricstore.FS.ls(shard_data_dir) do
      {:ok, entries} ->
        {:ok, Enum.count(entries, &bitcask_log_file?/1)}

      {:error, reason} ->
        Logger.error(
          "Failed to list shard data directory for merge scheduler startup at #{shard_data_dir}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp log_file_sizes(shard_data_dir) do
    case Ferricstore.FS.ls(shard_data_dir) do
      {:ok, entries} ->
        Enum.reduce_while(entries, {:ok, []}, fn name, {:ok, acc} ->
          if bitcask_log_file?(name) do
            case log_file_size(shard_data_dir, name) do
              {:ok, file_size} -> {:cont, {:ok, [file_size | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          else
            {:cont, {:ok, acc}}
          end
        end)

      {:error, reason} ->
        {:error, {:log_file_list_failed, reason}}
    end
  end

  defp log_file_size(shard_data_dir, name) do
    {file_id, ""} = name |> Path.rootname() |> Integer.parse()
    path = Path.join(shard_data_dir, name)

    case File.stat(path) do
      {:ok, %{size: size}} -> {:ok, {file_id, size}}
      {:error, reason} -> {:error, {:log_file_stat_failed, path, reason}}
    end
  end

  defp bitcask_log_file?(name) do
    with true <- String.ends_with?(name, ".log"),
         {_, ""} <- name |> Path.rootname() |> Integer.parse() do
      true
    else
      _ -> false
    end
  end

  # Safe GenServer.call that catches exits (shard might be restarting).
  defp safe_call(name, msg, timeout \\ 5_000) do
    GenServer.call(name, msg, timeout)
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
