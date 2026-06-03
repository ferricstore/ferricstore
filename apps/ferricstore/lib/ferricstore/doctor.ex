defmodule Ferricstore.Doctor do
  @moduledoc """
  Admin diagnostics and bounded repair jobs.

  Doctor is deliberately command-driven: production users can run it through the
  Redis protocol and the dashboard can call the same surface. The inline CHECK
  path only reads bounded metadata. Potentially expensive scans or repairs run
  as background jobs so they do not block a client connection or a dashboard
  request.
  """

  use GenServer

  alias Ferricstore.DataDir
  alias Ferricstore.FS
  alias Ferricstore.Flow.{LMDB, LMDBRebuilder, LMDBWriter}

  @default_scopes [:bitcask, :blob_refs, :flow_lmdb]
  @scope_names %{
    "ALL" => :all,
    "BITCASK" => :bitcask,
    "BLOB_REFS" => :blob_refs,
    "FLOW_LMDB" => :flow_lmdb
  }

  @type scope :: :bitcask | :blob_refs | :flow_lmdb

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  def handle_command(args, %FerricStore.Instance{} = ctx) when is_list(args) do
    case args do
      ["CHECK" | rest] ->
        with {:ok, scopes} <- parse_scopes(rest) do
          check(ctx, scopes)
        end

      ["START", "CHECK" | rest] ->
        with {:ok, scopes} <- parse_scopes(rest) do
          start_job(:check, ctx, scopes)
        end

      ["START", "REPAIR", "PROJECTIONS" | rest] ->
        with {:ok, scopes} <- parse_scopes(rest, [:flow_lmdb]),
             :ok <- validate_projection_repair_scopes(scopes) do
          start_job(:repair_projections, ctx, scopes)
        end

      ["STATUS", job_id] when is_binary(job_id) ->
        status(job_id)

      ["LIST"] ->
        list_jobs()

      ["CANCEL", job_id] when is_binary(job_id) ->
        cancel(job_id)

      [] ->
        wrong_arity()

      _ ->
        {:error, "ERR syntax error for 'ferricstore.doctor' command"}
    end
  end

  def handle_command(_args, _ctx),
    do: {:error, "ERR no default instance available for 'ferricstore.doctor' command"}

  @doc false
  def check(%FerricStore.Instance{} = ctx, scopes \\ @default_scopes) when is_list(scopes) do
    started_at = System.monotonic_time(:millisecond)
    maybe_run_check_hook()

    checks = Enum.map(normalize_scope_list(scopes), &run_scope_check(ctx, &1))

    %{
      "status" => aggregate_status(checks),
      "checks" => checks,
      "duration_ms" => max(0, System.monotonic_time(:millisecond) - started_at)
    }
  end

  @doc false
  def start_job(kind, %FerricStore.Instance{} = ctx, scopes)
      when kind in [:check, :repair_projections] do
    call_server({:start, kind, ctx, normalize_scope_list(scopes)})
  end

  @doc false
  def status(job_id) when is_binary(job_id), do: call_server({:status, job_id})

  @doc false
  def list_jobs, do: call_server(:list)

  @doc false
  def cancel(job_id) when is_binary(job_id), do: call_server({:cancel, job_id})

  @doc false
  def clear_for_test do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _pid -> GenServer.call(__MODULE__, :clear)
    end
  end

  @impl true
  def init(_opts) do
    {:ok, %{seq: 0, jobs: %{}, monitors: %{}}}
  end

  @impl true
  def handle_call({:start, kind, ctx, scopes}, _from, state) do
    {job_id, state} = next_job_id(state)
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        result = run_job(kind, ctx, scopes)
        send(parent, {:doctor_job_done, job_id, result})
      end)

    now = now_ms()

    job = %{
      id: job_id,
      kind: kind,
      scopes: scopes,
      status: :running,
      pid: pid,
      monitor: ref,
      created_at_ms: now,
      updated_at_ms: now,
      result: nil,
      error: nil
    }

    state = put_job(%{state | monitors: Map.put(state.monitors, ref, job_id)}, job)
    {:reply, job_summary(job), state}
  end

  def handle_call({:status, job_id}, _from, state) do
    {:reply, job_status(Map.get(state.jobs, job_id), job_id), state}
  end

  def handle_call(:list, _from, state) do
    jobs =
      state.jobs
      |> Map.values()
      |> Enum.sort_by(& &1.created_at_ms, :desc)
      |> Enum.map(&job_summary/1)

    {:reply, %{"jobs" => jobs}, state}
  end

  def handle_call({:cancel, job_id}, _from, state) do
    case Map.get(state.jobs, job_id) do
      %{status: :running, pid: pid, monitor: ref} = job ->
        Process.exit(pid, :kill)
        Process.demonitor(ref, [:flush])

        job =
          job
          |> Map.put(:status, :cancelled)
          |> Map.put(:pid, nil)
          |> Map.put(:monitor, nil)
          |> Map.put(:updated_at_ms, now_ms())

        state = %{state | monitors: Map.delete(state.monitors, ref)} |> put_job(job)
        {:reply, job_summary(job), state}

      nil ->
        {:reply, {:error, "ERR no such doctor job '#{job_id}'"}, state}

      job ->
        {:reply, job_summary(job), state}
    end
  end

  def handle_call(:clear, _from, state) do
    Enum.each(state.jobs, fn
      {_id, %{status: :running, pid: pid, monitor: ref}} when is_pid(pid) ->
        Process.exit(pid, :kill)
        Process.demonitor(ref, [:flush])

      _other ->
        :ok
    end)

    {:reply, :ok, %{seq: 0, jobs: %{}, monitors: %{}}}
  end

  @impl true
  def handle_info({:doctor_job_done, job_id, {:ok, result}}, state) do
    {:noreply, finish_job(state, job_id, :done, result, nil)}
  end

  def handle_info({:doctor_job_done, job_id, {:error, reason}}, state) do
    {:noreply, finish_job(state, job_id, :failed, nil, inspect(reason))}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {job_id, monitors} ->
        state = %{state | monitors: monitors}

        case Map.get(state.jobs, job_id) do
          %{status: :running} ->
            {:noreply, finish_job(state, job_id, :failed, nil, inspect(reason))}

          _other ->
            {:noreply, state}
        end
    end
  end

  defp call_server(message) do
    case Process.whereis(__MODULE__) do
      nil -> {:error, "ERR doctor service is not running"}
      _pid -> GenServer.call(__MODULE__, message, 5_000)
    end
  end

  defp next_job_id(%{seq: seq} = state) do
    next = seq + 1
    suffix = System.unique_integer([:positive, :monotonic])
    {"doctor-#{next}-#{suffix}", %{state | seq: next}}
  end

  defp put_job(state, %{id: id} = job), do: %{state | jobs: Map.put(state.jobs, id, job)}

  defp finish_job(state, job_id, status, result, error) do
    case Map.get(state.jobs, job_id) do
      %{status: :running, monitor: ref} = job ->
        job =
          job
          |> Map.put(:status, status)
          |> Map.put(:pid, nil)
          |> Map.put(:monitor, nil)
          |> Map.put(:result, result)
          |> Map.put(:error, error)
          |> Map.put(:updated_at_ms, now_ms())

        %{state | monitors: Map.delete(state.monitors, ref)}
        |> put_job(job)

      _other ->
        state
    end
  end

  defp run_job(:check, ctx, scopes), do: {:ok, check(ctx, scopes)}

  defp run_job(:repair_projections, ctx, scopes) do
    started_at = System.monotonic_time(:millisecond)
    results = Enum.map(scopes, &run_repair_scope(ctx, &1))

    result = %{
      "status" => aggregate_status(results),
      "checks" => results,
      "duration_ms" => max(0, System.monotonic_time(:millisecond) - started_at)
    }

    {:ok, result}
  rescue
    exception -> {:error, {exception.__struct__, Exception.message(exception)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp run_scope_check(ctx, :bitcask) do
    shard_metrics = Enum.map(shard_indexes(ctx), &bitcask_shard_metrics(ctx, &1))
    missing_keydirs = Enum.count(shard_metrics, &(&1["keydir_keys"] == nil))

    status = if missing_keydirs == 0, do: "ok", else: "error"

    %{
      "scope" => "bitcask",
      "status" => status,
      "message" => bitcask_message(status, missing_keydirs),
      "metrics" => %{
        "shards" => shard_metrics,
        "total_keydir_keys" => sum_metric(shard_metrics, "keydir_keys"),
        "total_keydir_binary_bytes" => sum_metric(shard_metrics, "keydir_binary_bytes"),
        "total_data_bytes" => sum_metric(shard_metrics, "data_bytes"),
        "total_data_files" => sum_metric(shard_metrics, "data_files")
      }
    }
  end

  defp run_scope_check(ctx, :blob_refs) do
    shard_metrics = Enum.map(shard_indexes(ctx), &blob_shard_metrics(ctx, &1))

    %{
      "scope" => "blob_refs",
      "status" => "ok",
      "message" => "blob segment metadata is readable",
      "metrics" => %{
        "shards" => shard_metrics,
        "total_segment_files" => sum_metric(shard_metrics, "segment_files"),
        "total_segment_bytes" => sum_metric(shard_metrics, "segment_bytes"),
        "protected_refs" => protected_blob_ref_count()
      }
    }
  end

  defp run_scope_check(ctx, :flow_lmdb) do
    shard_metrics = Enum.map(shard_indexes(ctx), &flow_lmdb_shard_metrics(ctx, &1))
    degraded = Enum.count(shard_metrics, &(&1["degraded"] == true))
    missing_dirs = Enum.count(shard_metrics, &(&1["lmdb_dir"] == false))

    status =
      cond do
        degraded > 0 -> "error"
        missing_dirs > 0 -> "warning"
        true -> "ok"
      end

    %{
      "scope" => "flow_lmdb",
      "status" => status,
      "message" => flow_lmdb_message(status, degraded, missing_dirs),
      "metrics" => %{
        "shards" => shard_metrics,
        "pending_ops" => sum_metric(shard_metrics, "pending_ops"),
        "max_oldest_pending_age_ms" => max_metric(shard_metrics, "oldest_pending_age_ms"),
        "degraded_shards" => degraded,
        "missing_dirs" => missing_dirs
      }
    }
  end

  defp run_repair_scope(ctx, :flow_lmdb) do
    flush_result = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)

    rebuild =
      ctx
      |> shard_indexes()
      |> Enum.map(&reconcile_flow_lmdb_shard(ctx, &1))

    status =
      cond do
        flush_result != :ok -> "error"
        Enum.any?(rebuild, &match?(%{"status" => "error"}, &1)) -> "error"
        true -> "ok"
      end

    %{
      "scope" => "flow_lmdb",
      "status" => status,
      "message" => "flow LMDB projection flushed and reconciled from durable Flow records",
      "metrics" => %{
        "flush" => inspect(flush_result),
        "shards" => rebuild
      }
    }
  end

  defp run_repair_scope(ctx, scope), do: run_scope_check(ctx, scope)

  defp reconcile_flow_lmdb_shard(ctx, shard_index) do
    shard_path = DataDir.shard_data_path(ctx.data_dir, shard_index)
    keydir = elem(ctx.keydir_refs, shard_index)

    # Do not mutate the hot Flow native index from an online doctor repair. The
    # durable source is the keydir/segment records; active indexes keep moving via
    # normal applies while LMDB is rebuilt as the cold projection.
    case LMDBRebuilder.reconcile_shard(shard_path, keydir, shard_index, ctx, nil, nil, nil, nil) do
      :ok ->
        %{"shard" => shard_index, "status" => "ok"}
    end
  rescue
    exception ->
      %{
        "shard" => shard_index,
        "status" => "error",
        "reason" => Exception.message(exception)
      }
  catch
    kind, reason ->
      %{"shard" => shard_index, "status" => "error", "reason" => inspect({kind, reason})}
  end

  defp bitcask_shard_metrics(ctx, shard_index) do
    keydir = elem(ctx.keydir_refs, shard_index)
    shard_path = DataDir.shard_data_path(ctx.data_dir, shard_index)

    %{
      "shard" => shard_index,
      "keydir_keys" => ets_size(keydir),
      "keydir_binary_bytes" => atomic_get(ctx.keydir_binary_bytes, shard_index),
      "data_files" => count_files(shard_path, ["*.log", "*.seg", "*.data", "*.hint"]),
      "data_bytes" => sum_file_bytes(shard_path, ["*.log", "*.seg", "*.data", "*.hint"])
    }
  end

  defp blob_shard_metrics(ctx, shard_index) do
    blob_path = DataDir.blob_shard_path(ctx.data_dir, shard_index)
    segment_path = Path.join(blob_path, "segments")

    %{
      "shard" => shard_index,
      "segment_files" => count_files(segment_path, ["*.bloblog"]),
      "segment_bytes" => sum_file_bytes(segment_path, ["*.bloblog"])
    }
  end

  defp flow_lmdb_shard_metrics(ctx, shard_index) do
    shard_path = DataDir.shard_data_path(ctx.data_dir, shard_index)
    lmdb_path = LMDB.path(shard_path)

    requested = atomic_get(ctx.flow_lmdb_replay_safe_requested_index, shard_index)
    durable = atomic_get(ctx.flow_lmdb_replay_safe_index, shard_index)

    %{
      "shard" => shard_index,
      "lmdb_dir" => FS.dir?(lmdb_path),
      "pending_ops" => atomic_get(ctx.flow_lmdb_writer_pending_ops, shard_index),
      "oldest_pending_age_ms" =>
        div(atomic_get(ctx.flow_lmdb_writer_oldest_pending_age_us, shard_index), 1_000),
      "replay_safe_requested_index" => requested,
      "replay_safe_durable_index" => durable,
      "replay_safe_lag" => max(0, requested - durable),
      "degraded" => atomic_get(ctx.flow_lmdb_mirror_degraded, shard_index) > 0
    }
  end

  defp parse_scopes(args, default \\ @default_scopes)

  defp parse_scopes([], default), do: {:ok, default}

  defp parse_scopes(["SCOPE", scope], _default) when is_binary(scope), do: parse_scope(scope)

  defp parse_scopes(["SCOPES", count | rest], _default) do
    with {n, ""} when n > 0 <- Integer.parse(count),
         true <- length(rest) == n,
         {:ok, scopes} <- parse_scope_list(rest) do
      {:ok, scopes}
    else
      _ -> {:error, "ERR syntax error for 'ferricstore.doctor' command"}
    end
  end

  defp parse_scopes(_args, _default),
    do: {:error, "ERR syntax error for 'ferricstore.doctor' command"}

  defp parse_scope(scope) do
    case Map.get(@scope_names, String.upcase(scope)) do
      :all -> {:ok, @default_scopes}
      nil -> {:error, "ERR unknown doctor scope '#{scope}'"}
      scope -> {:ok, [scope]}
    end
  end

  defp parse_scope_list(scopes) do
    Enum.reduce_while(scopes, {:ok, []}, fn scope, {:ok, acc} ->
      case parse_scope(scope) do
        {:ok, parsed} -> {:cont, {:ok, acc ++ parsed}}
        error -> {:halt, error}
      end
    end)
  end

  defp normalize_scope_list(scopes) do
    scopes
    |> Enum.flat_map(fn
      :all -> @default_scopes
      scope -> [scope]
    end)
    |> Enum.uniq()
  end

  defp validate_projection_repair_scopes(scopes) do
    case normalize_scope_list(scopes) do
      [:flow_lmdb] -> :ok
      _other -> {:error, "ERR doctor repair projections supports only FLOW_LMDB scope"}
    end
  end

  defp aggregate_status(checks) do
    statuses = Enum.map(checks, & &1["status"])

    cond do
      "error" in statuses -> "error"
      "warning" in statuses -> "warning"
      true -> "ok"
    end
  end

  defp job_status(nil, job_id), do: {:error, "ERR no such doctor job '#{job_id}'"}
  defp job_status(job, _job_id), do: job_summary(job)

  defp job_summary(job) do
    %{
      "job_id" => job.id,
      "kind" => Atom.to_string(job.kind),
      "status" => Atom.to_string(job.status),
      "scopes" => Enum.map(job.scopes, &scope_name/1),
      "created_at_ms" => job.created_at_ms,
      "updated_at_ms" => job.updated_at_ms,
      "result" => job.result,
      "error" => job.error
    }
  end

  defp scope_name(:bitcask), do: "bitcask"
  defp scope_name(:blob_refs), do: "blob_refs"
  defp scope_name(:flow_lmdb), do: "flow_lmdb"

  defp bitcask_message("ok", _missing), do: "keydir and storage metadata are readable"
  defp bitcask_message(_status, missing), do: "#{missing} shard keydir table(s) unavailable"

  defp flow_lmdb_message("ok", _degraded, _missing),
    do: "LMDB projection is enabled and writers are healthy"

  defp flow_lmdb_message(_status, degraded, missing),
    do: "#{degraded} degraded shard(s), #{missing} missing LMDB dir(s)"

  defp wrong_arity, do: {:error, "ERR wrong number of arguments for 'ferricstore.doctor' command"}

  defp maybe_run_check_hook do
    case Application.get_env(:ferricstore, :doctor_check_hook) do
      fun when is_function(fun, 0) -> fun.()
      _other -> :ok
    end
  end

  defp shard_indexes(%{shard_count: count}) when is_integer(count) and count > 0,
    do: 0..(count - 1)

  defp shard_indexes(_ctx), do: []

  defp ets_size(table) do
    case :ets.info(table, :size) do
      :undefined -> nil
      size when is_integer(size) -> size
      _other -> nil
    end
  rescue
    _ -> nil
  end

  defp atomic_get(ref, shard_index) when is_reference(ref) and is_integer(shard_index) do
    if shard_index < :atomics.info(ref).size do
      :atomics.get(ref, shard_index + 1)
    else
      0
    end
  rescue
    _ -> 0
  end

  defp atomic_get(_ref, _shard_index), do: 0

  defp protected_blob_ref_count do
    case :ets.info(:ferricstore_blob_store_protected_refs, :size) do
      :undefined -> 0
      n when is_integer(n) -> n
      _other -> 0
    end
  rescue
    _ -> 0
  end

  defp count_files(path, patterns), do: path |> matching_files(patterns) |> length()

  defp sum_file_bytes(path, patterns) do
    path
    |> matching_files(patterns)
    |> Enum.reduce(0, fn file, acc ->
      case File.stat(file) do
        {:ok, %{type: :regular, size: size}} -> acc + size
        _other -> acc
      end
    end)
  end

  defp matching_files(path, patterns) do
    if FS.dir?(path) do
      Enum.flat_map(patterns, fn pattern -> Path.wildcard(Path.join(path, pattern)) end)
    else
      []
    end
  end

  defp sum_metric(rows, key) do
    Enum.reduce(rows, 0, fn row, acc ->
      case Map.get(row, key) do
        value when is_integer(value) -> acc + value
        _other -> acc
      end
    end)
  end

  defp max_metric(rows, key) do
    rows
    |> Enum.map(&Map.get(&1, key, 0))
    |> Enum.filter(&is_integer/1)
    |> Enum.max(fn -> 0 end)
  end

  defp now_ms, do: System.system_time(:millisecond)
end
