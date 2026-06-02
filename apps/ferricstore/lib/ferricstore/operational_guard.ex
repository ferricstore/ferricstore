defmodule Ferricstore.OperationalGuard do
  @moduledoc """
  Proactive operational guard for memory/disk capacity.

  `MemoryGuard` protects the hot cache. `DiskPressure` protects individual shard
  writes after IO failures. This module bridges the operational gap: it watches
  real node/filesystem budgets and enters pressure mode before an ENOSPC or RSS
  collapse. Pressure accelerates retention/compaction; reject level gates writes
  cleanly through the existing write-path flags.
  """

  use GenServer

  alias Ferricstore.Store.DiskPressure

  @pt_key :ferricstore_operational_guard
  @disk_pressure_slot 1
  @disk_reject_slot 2
  @disk_panic_slot 3
  @rss_pressure_slot 4
  @rss_reject_slot 5
  @rss_panic_slot 6

  @default_interval_ms 1_000
  @default_lmdb_mmap_reclaim_interval_ms 10_000

  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts \\ []) do
    if enabled?() do
      name = Keyword.get(opts, :name, __MODULE__)
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      :ignore
    end
  end

  @spec pressure?() :: boolean()
  def pressure? do
    flag?(@disk_pressure_slot) or flag?(@rss_pressure_slot)
  end

  @spec reject_writes?() :: boolean()
  def reject_writes? do
    flag?(@disk_reject_slot) or flag?(@rss_reject_slot)
  end

  @spec reject_flow_creates?() :: boolean()
  def reject_flow_creates? do
    flag?(@disk_reject_slot) or flag?(@disk_panic_slot) or
      (flag?(@rss_panic_slot) and Ferricstore.Flow.Admission.reject_new_creates?())
  rescue
    _ -> flag?(@disk_reject_slot) or flag?(@disk_panic_slot) or flag?(@rss_panic_slot)
  catch
    _, _ -> flag?(@disk_reject_slot) or flag?(@disk_panic_slot) or flag?(@rss_panic_slot)
  end

  @spec info() :: map()
  def info do
    case Process.whereis(__MODULE__) do
      nil ->
        %{
          enabled: enabled?(),
          running: false,
          pressure?: pressure?(),
          reject_writes?: reject_writes?()
        }

      pid ->
        GenServer.call(pid, :info)
    end
  catch
    :exit, _ ->
      %{
        enabled: enabled?(),
        running: false,
        pressure?: pressure?(),
        reject_writes?: reject_writes?()
      }
  end

  @doc false
  def reset_for_test do
    ref = :atomics.new(6, signed: false)
    :persistent_term.put(@pt_key, ref)
    Ferricstore.Flow.Admission.clear_create_pause()
    :ok
  end

  @impl true
  def init(opts) do
    Ferricstore.Flow.Admission.init()

    ref = :atomics.new(6, signed: false)
    :persistent_term.put(@pt_key, ref)

    ctx = Keyword.get(opts, :instance_ctx)
    shard_count = Keyword.get(opts, :shard_count) || shard_count(ctx)
    data_dir = Keyword.get(opts, :data_dir) || data_dir(ctx)

    interval_ms =
      pos_int(
        Keyword.get(opts, :interval_ms),
        Application.get_env(:ferricstore, :operational_guard_interval_ms),
        @default_interval_ms
      )

    state = %{
      ctx: ctx,
      shard_count: shard_count,
      data_dir: data_dir,
      interval_ms: interval_ms,
      limits_fun: Keyword.get(opts, :limits_fun, &Ferricstore.OperationalLimits.snapshot/1),
      apply_disk_pressure_fun:
        Keyword.get(opts, :apply_disk_pressure_fun, &apply_disk_pressure/3),
      apply_memory_pressure_fun:
        Keyword.get(opts, :apply_memory_pressure_fun, &apply_memory_pressure/1),
      apply_flow_admission_fun:
        Keyword.get(opts, :apply_flow_admission_fun, &apply_flow_admission/1),
      memory_stats_fun: Keyword.get(opts, :memory_stats_fun, &memory_stats/0),
      lmdb_reclaim_fun:
        Keyword.get(opts, :lmdb_reclaim_fun, &Ferricstore.Flow.LMDB.release_all/0),
      telemetry_fun: Keyword.get(opts, :telemetry_fun, &:telemetry.execute/3),
      last_snapshot: nil,
      last_lmdb_mmap_reclaim_at: nil
    }

    send(self(), :check)
    {:ok, state}
  end

  @impl true
  def handle_call(:info, _from, state) do
    {:reply,
     %{
       enabled: true,
       running: true,
       pressure?: pressure?(),
       reject_writes?: reject_writes?(),
       interval_ms: state.interval_ms,
       last_snapshot: state.last_snapshot
     }, state}
  end

  @impl true
  def handle_info(:check, state) do
    snapshot =
      state.limits_fun.(
        data_dir: state.data_dir,
        shard_count: state.shard_count
      )

    snapshot = Map.put(snapshot, :active_memory, active_memory_snapshot(state))

    update_flags(snapshot)
    state.apply_disk_pressure_fun.(state.ctx, state.shard_count, snapshot.disk.level)
    state.apply_memory_pressure_fun.(snapshot.memory.level)
    state.apply_flow_admission_fun.(snapshot)
    emit_check(state.telemetry_fun, snapshot)
    state = maybe_reclaim_lmdb_mmap(state, snapshot)

    Process.send_after(self(), :check, state.interval_ms)
    {:noreply, %{state | last_snapshot: snapshot}}
  end

  defp enabled? do
    Application.get_env(:ferricstore, :operational_guard_enabled, true) == true
  end

  defp update_flags(snapshot) do
    set_flag(@disk_pressure_slot, pressured?(snapshot.disk.level))
    set_flag(@disk_reject_slot, reject?(snapshot.disk.level))
    set_flag(@disk_panic_slot, snapshot.disk.level == :panic)
    set_flag(@rss_pressure_slot, pressured?(snapshot.memory.level))
    set_flag(@rss_reject_slot, reject?(snapshot.memory.level))
    set_flag(@rss_panic_slot, snapshot.memory.level == :panic)
  end

  defp apply_disk_pressure(_ctx, shard_count, level) when level in [:reject, :panic] do
    Enum.each(0..(shard_count - 1), &DiskPressure.set_operational/1)
  end

  defp apply_disk_pressure(_ctx, shard_count, _level) do
    Enum.each(0..(shard_count - 1), &DiskPressure.clear_operational/1)
  end

  defp apply_memory_pressure(level) do
    Ferricstore.MemoryGuard.set_skip_promotion(pressured?(level))
    Ferricstore.MemoryGuard.set_reject_writes(level == :panic)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp apply_flow_admission(snapshot) do
    rss_ratio = snapshot.memory.rss_ratio
    disk_ratio = snapshot.disk.used_ratio
    admission = Ferricstore.Flow.Admission.status()
    paused? = admission.reject_new_creates?

    pause_rss_ratio =
      configured_ratio(:flow_create_pause_rss_ratio, 0.88)

    resume_rss_ratio =
      configured_ratio(:flow_create_resume_rss_ratio, 0.84)

    pause_disk_ratio =
      configured_ratio(:flow_create_pause_disk_ratio, 0.82)

    resume_disk_ratio =
      configured_ratio(:flow_create_resume_disk_ratio, 0.78)

    cond do
      rss_create_pause_needed?(snapshot, rss_ratio, pause_rss_ratio) ->
        Ferricstore.Flow.Admission.pause_creates(
          :rss_pressure,
          retry_after_ms(snapshot.memory.level)
        )

      ratio_reached?(disk_ratio, pause_disk_ratio) ->
        Ferricstore.Flow.Admission.pause_creates(
          :disk_pressure,
          retry_after_ms(snapshot.disk.level)
        )

      paused? and admission.reason == :rss_pressure and
          not rss_create_resume_safe?(snapshot, rss_ratio, resume_rss_ratio) ->
        :ok

      paused? and admission.reason == :disk_pressure and
          not ratio_below?(disk_ratio, resume_disk_ratio) ->
        :ok

      paused? and admission.reason not in [:rss_pressure, :disk_pressure] and
          (not ratio_below?(rss_ratio, resume_rss_ratio) or
             not ratio_below?(disk_ratio, resume_disk_ratio)) ->
        :ok

      true ->
        Ferricstore.Flow.Admission.clear_create_pause()
    end
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp rss_create_pause_needed?(snapshot, rss_ratio, pause_rss_ratio) do
    ratio_reached?(rss_ratio, pause_rss_ratio) and
      not active_memory_drained_for_flow_create?(snapshot)
  end

  defp rss_create_resume_safe?(snapshot, rss_ratio, resume_rss_ratio) do
    ratio_below?(rss_ratio, resume_rss_ratio) or sticky_rss_resume_safe?(snapshot, rss_ratio)
  end

  defp sticky_rss_resume_safe?(snapshot, rss_ratio) do
    Application.get_env(:ferricstore, :flow_create_resume_on_drained_active_memory, true) == true and
      ratio_below?(
        rss_ratio,
        configured_ratio(:flow_create_sticky_rss_resume_max_ratio, 0.99)
      ) and
      active_memory_drained_for_flow_create?(snapshot)
  end

  defp active_memory_drained_for_flow_create?(snapshot) do
    active = Map.get(snapshot, :active_memory, %{})

    active_total_ratio =
      number_or(active[:total_ratio], active[:ratio], 1.0)

    active_keydir_ratio =
      number_or(active[:keydir_ratio], 1.0)

    active_keydir_bytes =
      int_or(active[:keydir_bytes], 0)

    active_total_ratio <= configured_ratio(:flow_create_resume_active_memory_ratio, 0.25) and
      active_keydir_ratio <= configured_ratio(:flow_create_resume_keydir_ratio, 0.10) and
      active_keydir_bytes <=
        configured_pos_int(:flow_create_resume_keydir_bytes, 64 * 1024 * 1024)
  end

  defp active_memory_snapshot(state) do
    case state.memory_stats_fun.() do
      stats when is_map(stats) ->
        %{
          total_bytes: int_or(stats[:total_bytes], 0),
          total_ratio: number_or(stats[:ratio], 0.0),
          keydir_bytes: int_or(stats[:keydir_bytes], 0),
          keydir_ratio: number_or(stats[:keydir_ratio], 0.0)
        }

      _ ->
        %{}
    end
  rescue
    _ -> %{}
  catch
    _, _ -> %{}
  end

  defp memory_stats do
    Ferricstore.MemoryGuard.stats()
  rescue
    _ -> %{}
  catch
    _, _ -> %{}
  end

  defp maybe_reclaim_lmdb_mmap(state, snapshot) do
    if lmdb_mmap_reclaim_enabled?() and lmdb_mmap_reclaim_due?(state) and
         lmdb_mmap_reclaim_pressure?(snapshot) and lmdb_idle?(state.ctx, state.shard_count) do
      result = safe_lmdb_reclaim(state.lmdb_reclaim_fun)
      emit_lmdb_mmap_reclaim(state.telemetry_fun, snapshot, result)
      %{state | last_lmdb_mmap_reclaim_at: monotonic_ms()}
    else
      state
    end
  end

  defp lmdb_mmap_reclaim_enabled? do
    Application.get_env(:ferricstore, :flow_lmdb_mmap_reclaim_enabled, true) == true
  end

  defp lmdb_mmap_reclaim_due?(state) do
    case state.last_lmdb_mmap_reclaim_at do
      nil ->
        true

      last ->
        monotonic_ms() - last >=
          configured_pos_int(
            :flow_lmdb_mmap_reclaim_interval_ms,
            @default_lmdb_mmap_reclaim_interval_ms
          )
    end
  end

  defp lmdb_mmap_reclaim_pressure?(snapshot) do
    ratio_reached?(
      snapshot.memory.rss_ratio,
      configured_ratio(:flow_lmdb_mmap_reclaim_rss_ratio, 0.70)
    )
  end

  defp lmdb_idle?(nil, _shard_count), do: false

  defp lmdb_idle?(ctx, shard_count) when is_integer(shard_count) and shard_count > 0 do
    Enum.all?(0..(shard_count - 1), fn shard ->
      pending = atomic(ctx, :flow_lmdb_writer_pending_ops, shard)
      requested = atomic(ctx, :flow_lmdb_replay_safe_requested_index, shard)
      durable = atomic(ctx, :flow_lmdb_replay_safe_index, shard)

      pending == 0 and requested <= durable
    end)
  rescue
    _ -> false
  end

  defp lmdb_idle?(_ctx, _shard_count), do: false

  defp atomic(ctx, key, shard) do
    ref = Map.get(ctx, key)

    case ref do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size
        idx = shard + 1
        if idx <= size, do: :atomics.get(ref, idx), else: 0

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp safe_lmdb_reclaim(fun) do
    fun.()
  rescue
    reason -> {:error, reason}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp emit_lmdb_mmap_reclaim(telemetry_fun, snapshot, result) do
    {status, released, busy} =
      case result do
        {:ok, released} -> {:ok, released, 0}
        {:busy, busy} -> {:busy, 0, busy}
        {:error, _reason} -> {:error, 0, 0}
        _ -> {:error, 0, 0}
      end

    telemetry_fun.(
      [:ferricstore, :flow, :lmdb, :mmap_reclaim],
      %{released: released, busy: busy},
      %{status: status, rss_ratio: snapshot.memory.rss_ratio, rss_level: snapshot.memory.level}
    )
  end

  defp emit_check(telemetry_fun, snapshot) do
    telemetry_fun.(
      [:ferricstore, :operational, :guard],
      %{
        rss_bytes: snapshot.memory.rss_bytes || 0,
        memory_limit_bytes: snapshot.memory.limit_bytes,
        disk_used_bytes: snapshot.disk.used_bytes,
        disk_total_bytes: snapshot.disk.total_bytes
      },
      %{
        rss_level: snapshot.memory.level,
        disk_level: snapshot.disk.level,
        rss_ratio: snapshot.memory.rss_ratio,
        disk_ratio: snapshot.disk.used_ratio,
        data_dir: snapshot.data_dir,
        shard_count: snapshot.shard_count
      }
    )
  end

  defp pressured?(level), do: level in [:pressure, :reject, :panic]
  defp reject?(level), do: level in [:reject, :panic]

  defp retry_after_ms(:panic), do: configured_pos_int(:flow_create_panic_retry_after_ms, 5_000)
  defp retry_after_ms(:reject), do: configured_pos_int(:flow_create_reject_retry_after_ms, 2_000)
  defp retry_after_ms(_level), do: configured_pos_int(:flow_create_pressure_retry_after_ms, 2_000)

  defp ratio_reached?(ratio, threshold) when is_number(ratio), do: ratio >= threshold
  defp ratio_reached?(_ratio, _threshold), do: false

  defp ratio_below?(ratio, threshold) when is_number(ratio), do: ratio < threshold
  defp ratio_below?(_ratio, _threshold), do: true

  defp configured_ratio(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_number(value) and value >= 0.0 and value <= 1.0 -> value
      _ -> default
    end
  end

  defp configured_pos_int(key, default) do
    case Application.get_env(:ferricstore, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp number_or(value, _fallback) when is_number(value), do: value
  defp number_or(_value, fallback), do: fallback
  defp number_or(value, _alternate, _fallback) when is_number(value), do: value
  defp number_or(_value, alternate, _fallback) when is_number(alternate), do: alternate
  defp number_or(_value, _alternate, fallback), do: fallback

  defp int_or(value, _fallback) when is_integer(value), do: value
  defp int_or(_value, fallback), do: fallback

  defp monotonic_ms do
    System.monotonic_time(:millisecond)
  end

  defp flag?(slot) do
    case :persistent_term.get(@pt_key, nil) do
      nil -> false
      ref -> :atomics.get(ref, slot) == 1
    end
  rescue
    _ -> false
  end

  defp set_flag(slot, value) do
    case :persistent_term.get(@pt_key, nil) do
      nil -> :ok
      ref -> :atomics.put(ref, slot, if(value, do: 1, else: 0))
    end
  rescue
    _ -> :ok
  end

  defp pos_int(value1, value2, default) do
    cond do
      is_integer(value1) and value1 > 0 -> value1
      is_integer(value2) and value2 > 0 -> value2
      true -> default
    end
  end

  defp shard_count(%{shard_count: shard_count}) when is_integer(shard_count) and shard_count > 0,
    do: shard_count

  defp shard_count(_ctx) do
    case Application.get_env(:ferricstore, :shard_count, 0) do
      n when is_integer(n) and n > 0 -> n
      _ -> max(System.schedulers_online(), 1)
    end
  end

  defp data_dir(%{data_dir: data_dir}) when is_binary(data_dir), do: data_dir
  defp data_dir(_ctx), do: Application.get_env(:ferricstore, :data_dir, "data")
end
