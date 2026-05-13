defmodule Ferricstore.Store.BlobGCSweeper do
  @moduledoc """
  Periodic conservative garbage collection for large-value blob storage.

  The sweeper asks `Router.sweep_blob_garbage/1` to build the live reference
  set from shard keydirs before deleting unreferenced legacy blob files and
  stale tmp files. Append-segment records are retained until segment compaction
  exists; the sweep is deliberately conservative so it never removes a segment
  that may still contain a live payload. To avoid scanning keydirs on idle
  systems, each tick first checks blob storage stats and skips the expensive
  sweep when no reclaimable legacy blob files or temporary files exist.
  """

  use GenServer

  require Logger

  alias Ferricstore.Store.{BlobStore, Router}

  @default_initial_delay_ms 60_000
  @default_interval_ms 600_000

  @zero_stats %{
    files: 0,
    bytes: 0,
    legacy_files: 0,
    legacy_bytes: 0,
    segment_files: 0,
    segment_bytes: 0,
    tmp_files: 0,
    tmp_bytes: 0
  }
  @zero_gc %{deleted_files: 0, deleted_bytes: 0, kept_files: 0}

  def start_link(opts \\ []) do
    if enabled?(opts) do
      name = Keyword.get(opts, :name, __MODULE__)
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      :ignore
    end
  end

  def info(name \\ __MODULE__) do
    case process_for(name) do
      nil ->
        %{
          enabled: enabled?([]),
          running: false,
          interval_ms: config_pos_int(:blob_gc_sweeper_interval_ms, @default_interval_ms),
          last_sweep: nil
        }

      pid ->
        GenServer.call(pid, :info)
    end
  catch
    :exit, _reason ->
      %{
        enabled: enabled?([]),
        running: false,
        interval_ms: config_pos_int(:blob_gc_sweeper_interval_ms, @default_interval_ms),
        last_sweep: nil
      }
  end

  @impl true
  def init(opts) do
    instance_ctx = Keyword.get(opts, :instance_ctx)

    state = %{
      interval_ms:
        opt_pos_int(opts, :interval_ms, :blob_gc_sweeper_interval_ms, @default_interval_ms),
      instance_ctx: instance_ctx,
      stats_fun: Keyword.get(opts, :stats_fun),
      sweep_fun: Keyword.get(opts, :sweep_fun),
      last_sweep: nil
    }

    initial_delay =
      opt_non_neg_int(
        opts,
        :initial_delay_ms,
        :blob_gc_sweeper_initial_delay_ms,
        @default_initial_delay_ms
      )

    schedule(initial_delay)
    {:ok, state}
  end

  @impl true
  def handle_call(:info, _from, state) do
    {:reply,
     %{
       enabled: true,
       running: true,
       interval_ms: state.interval_ms,
       last_sweep: state.last_sweep
     }, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    started = System.monotonic_time()
    {status, stats, gc_stats, reason} = run_sweep(state)
    duration_us = duration_us(started)
    emit_sweep(status, stats, gc_stats, reason, duration_us)
    maybe_emit_error(status, reason)

    last_sweep =
      stats
      |> Map.merge(gc_stats)
      |> Map.merge(%{
        status: status,
        reason: reason,
        duration_us: duration_us,
        finished_at_ms: System.system_time(:millisecond)
      })

    schedule(state.interval_ms)
    {:noreply, %{state | last_sweep: last_sweep}}
  end

  defp run_sweep(state) do
    with {:ok, stats} <- storage_stats(state) do
      if blob_work_present?(stats) do
        case sweep(state) do
          {:ok, %{skipped: true, reason: reason} = gc_stats} ->
            {:skipped, normalize_stats(stats), normalize_gc_stats(gc_stats), reason}

          {:ok, gc_stats} -> {:ok, normalize_stats(stats), normalize_gc_stats(gc_stats), :none}
          {:error, reason} -> {:error, normalize_stats(stats), @zero_gc, reason}
          other -> {:error, normalize_stats(stats), @zero_gc, other}
        end
      else
        {:skipped, normalize_stats(stats), @zero_gc, :no_blob_files}
      end
    else
      {:error, reason} -> {:error, @zero_stats, @zero_gc, reason}
      other -> {:error, @zero_stats, @zero_gc, other}
    end
  rescue
    error ->
      Logger.warning("Blob GC sweeper failed: #{Exception.message(error)}")
      {:error, @zero_stats, @zero_gc, error}
  end

  defp storage_stats(%{stats_fun: fun}) when is_function(fun, 0), do: fun.()

  defp storage_stats(%{instance_ctx: %{blob_side_channel_threshold_bytes: threshold}})
       when is_integer(threshold) and threshold <= 0,
       do: {:ok, @zero_stats}

  defp storage_stats(%{instance_ctx: %{data_dir: data_dir}}),
    do: BlobStore.storage_stats(data_dir)

  defp storage_stats(_state), do: {:error, :no_default_instance}

  defp sweep(%{sweep_fun: fun}) when is_function(fun, 0), do: fun.()
  defp sweep(%{instance_ctx: ctx}) when is_map(ctx), do: Router.sweep_blob_garbage(ctx)
  defp sweep(_state), do: {:error, :no_default_instance}

  defp blob_work_present?(stats) do
    legacy_files =
      if Map.has_key?(stats, :legacy_files) do
        Map.get(stats, :legacy_files, 0)
      else
        Map.get(stats, :files, 0)
      end

    legacy_files > 0 or Map.get(stats, :tmp_files, 0) > 0
  end

  defp normalize_stats(stats) do
    legacy_files =
      if Map.has_key?(stats, :legacy_files) do
        Map.get(stats, :legacy_files, 0)
      else
        Map.get(stats, :files, 0)
      end

    legacy_bytes =
      if Map.has_key?(stats, :legacy_bytes) do
        Map.get(stats, :legacy_bytes, 0)
      else
        Map.get(stats, :bytes, 0)
      end

    %{
      files: Map.get(stats, :files, 0),
      bytes: Map.get(stats, :bytes, 0),
      legacy_files: legacy_files,
      legacy_bytes: legacy_bytes,
      segment_files: Map.get(stats, :segment_files, 0),
      segment_bytes: Map.get(stats, :segment_bytes, 0),
      tmp_files: Map.get(stats, :tmp_files, 0),
      tmp_bytes: Map.get(stats, :tmp_bytes, 0)
    }
  end

  defp normalize_gc_stats(stats) do
    %{
      deleted_files: Map.get(stats, :deleted_files, 0),
      deleted_bytes: Map.get(stats, :deleted_bytes, 0),
      kept_files: Map.get(stats, :kept_files, 0),
      deleted_tmp_files: Map.get(stats, :deleted_tmp_files, 0),
      deleted_tmp_bytes: Map.get(stats, :deleted_tmp_bytes, 0),
      skipped: Map.get(stats, :skipped, false),
      reason: Map.get(stats, :reason)
    }
  end

  defp emit_sweep(status, stats, gc_stats, reason, duration_us) do
    :telemetry.execute(
      [:ferricstore, :blob, :gc_sweeper, :sweep],
      Map.merge(stats, Map.put(gc_stats, :duration_us, duration_us)),
      %{status: status, reason: reason}
    )
  end

  defp maybe_emit_error(:error, reason) do
    :telemetry.execute(
      [:ferricstore, :blob, :gc_sweeper, :error],
      %{count: 1},
      %{reason: reason}
    )
  end

  defp maybe_emit_error(_status, _reason), do: :ok

  defp schedule(delay_ms), do: Process.send_after(self(), :sweep, delay_ms)

  defp enabled?(opts) do
    Application.get_env(:ferricstore, :blob_gc_sweeper_enabled, true) == true and
      blob_side_channel_enabled?(Keyword.get(opts, :instance_ctx))
  end

  defp blob_side_channel_enabled?(%{blob_side_channel_threshold_bytes: threshold})
       when is_integer(threshold),
       do: threshold > 0

  defp blob_side_channel_enabled?(_ctx), do: true

  defp process_for(pid) when is_pid(pid), do: pid
  defp process_for(name) when is_atom(name), do: Process.whereis(name)
  defp process_for(_name), do: nil

  defp opt_pos_int(opts, key, env_key, default) do
    case Keyword.get(opts, key, Application.get_env(:ferricstore, env_key, default)) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp opt_non_neg_int(opts, key, env_key, default) do
    case Keyword.get(opts, key, Application.get_env(:ferricstore, env_key, default)) do
      value when is_integer(value) and value >= 0 -> value
      _ -> default
    end
  end

  defp config_pos_int(env_key, default) do
    case Application.get_env(:ferricstore, env_key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp duration_us(started) do
    System.monotonic_time()
    |> Kernel.-(started)
    |> System.convert_time_unit(:native, :microsecond)
  end
end
