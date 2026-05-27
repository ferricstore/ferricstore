Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule StartupRecoveryBench do
  @moduledoc false

  @events [
    [:ferricstore, :shard, :startup_phase],
    [:ferricstore, :waraft, :backend, :startup_phase],
    [:ferricstore, :waraft, :segment_log, :startup_phase],
    [:ferricstore, :waraft, :vendor, :startup_phase],
    [:ferricstore, :waraft, :storage, :startup_phase]
  ]

  def run do
    data_dir =
      System.get_env("DATA_DIR") ||
        raise "DATA_DIR is required for startup recovery benchmark"

    timeout_ms = env_int("STARTUP_TIMEOUT_MS", 180_000)
    shards = env_int("SHARDS", 16)
    parent = self()
    handler_id = "startup-recovery-bench-#{System.unique_integer([:positive])}"

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :shard_count, shards)
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :max_memory_bytes, 0)
    Application.put_env(:ferricstore, :memory_guard_interval_ms, 3_600_000)
    Application.put_env(:ferricstore, :flow_async_history, true)

    {:ok, _} = Application.ensure_all_started(:telemetry)

    Enum.each(@events, fn event ->
      :telemetry.attach(handler_id <> inspect(event), event, &__MODULE__.handle_event/4, parent)
    end)

    started_us = System.monotonic_time(:microsecond)

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore)
      app_started_us = System.monotonic_time(:microsecond)
      FerricStore.await_ready(timeout: timeout_ms, interval: 50)
      ready_at_us = System.monotonic_time(:microsecond)
      app_start_us = app_started_us - started_us
      await_ready_us = ready_at_us - app_started_us
      ready_us = ready_at_us - started_us
      ctx = FerricStore.Instance.get(:default)

      IO.puts(
        "startup_summary data_dir=#{data_dir} shards=#{shards} ready_ms=#{round_ms(ready_us)} " <>
          "app_start_ms=#{round_ms(app_start_us)} await_ready_ms=#{round_ms(await_ready_us)} " <>
          "dbsize=#{safe_dbsize(ctx)} keydir_entries=#{keydir_entries(ctx)} " <>
          "rss_mb=#{rss_mb()} total_mem_mb=#{mem_mb(:total)} binary_mem_mb=#{mem_mb(:binary)}"
      )

      print_phase_summary(drain_events())
    after
      Enum.each(@events, fn event -> :telemetry.detach(handler_id <> inspect(event)) end)
      stop_apps()
    end
  end

  def handle_event(event, measurements, metadata, parent) do
    send(parent, {:startup_event, event, measurements, metadata})
  end

  defp drain_events(acc \\ []) do
    receive do
      {:startup_event, event, measurements, metadata} ->
        drain_events([{event, measurements, metadata} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp print_phase_summary(events) do
    events
    |> Enum.reduce(%{}, fn
      {[:ferricstore, :shard, :startup_phase], %{duration_us: us}, metadata}, acc ->
        add_phase(acc, {:shard, metadata[:phase]}, metadata[:shard_index], us)

      {[:ferricstore, :waraft, :storage, :startup_phase], %{duration_us: us}, metadata}, acc ->
        add_phase(acc, {:waraft_storage, metadata[:phase]}, metadata[:shard_index], us)

      {[:ferricstore, :waraft, :backend, :startup_phase], %{duration_us: us}, metadata}, acc ->
        add_phase(acc, {:waraft_backend, metadata[:phase]}, metadata[:shard_index], us)

      {[:ferricstore, :waraft, :segment_log, :startup_phase], %{duration_us: us}, metadata},
      acc ->
        add_phase(acc, {:waraft_segment_log, metadata[:phase]}, metadata[:path], us)

      {[:ferricstore, :waraft, :vendor, :startup_phase], %{duration_us: us}, metadata}, acc ->
        add_phase(acc, {:waraft_vendor, metadata[:phase]}, metadata[:shard_index], us)

      _other, acc ->
        acc
    end)
    |> Enum.sort_by(fn {_phase, stats} -> -stats.total_us end)
    |> Enum.each(fn {phase, stats} ->
      IO.puts(
        "startup_phase kind=#{elem(phase, 0)} phase=#{elem(phase, 1)} count=#{stats.count} " <>
          "total_ms=#{round_ms(stats.total_us)} max_ms=#{round_ms(stats.max_us)} " <>
          "max_shard=#{inspect(stats.max_shard)}"
      )
    end)
  end

  defp add_phase(acc, phase, shard, us) when is_integer(us) do
    Map.update(acc, phase, %{count: 1, total_us: us, max_us: us, max_shard: shard}, fn stats ->
      %{
        count: stats.count + 1,
        total_us: stats.total_us + us,
        max_us: max(stats.max_us, us),
        max_shard: if(us > stats.max_us, do: shard, else: stats.max_shard)
      }
    end)
  end

  defp safe_dbsize(ctx) do
    Ferricstore.Store.Router.dbsize(ctx)
  rescue
    _ -> :unknown
  end

  defp keydir_entries(%{keydir_refs: refs}) when is_tuple(refs) do
    refs
    |> Tuple.to_list()
    |> Enum.map(fn table ->
      case :ets.info(table, :size) do
        size when is_integer(size) -> size
        _ -> 0
      end
    end)
    |> Enum.sum()
  rescue
    _ -> 0
  end

  defp keydir_entries(_ctx), do: 0

  defp mem_mb(kind) do
    kind
    |> :erlang.memory()
    |> Kernel./(1_048_576)
    |> Float.round(1)
  end

  defp rss_mb do
    pid = to_string(:os.getpid())

    case System.cmd("ps", ["-o", "rss=", "-p", pid], stderr_to_stdout: true) do
      {out, 0} ->
        out
        |> String.trim()
        |> Integer.parse()
        |> case do
          {kb, _rest} -> Float.round(kb / 1024, 1)
          :error -> 0.0
        end

      _other ->
        0.0
    end
  rescue
    _ -> 0.0
  end

  defp stop_apps do
    for app <- [:ferricstore_server, :ferricstore] do
      if List.keymember?(Application.started_applications(), app, 0) do
        Application.stop(app)
      end
    end
  end

  defp round_ms(us), do: Float.round(us / 1000, 1)

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

StartupRecoveryBench.run()
