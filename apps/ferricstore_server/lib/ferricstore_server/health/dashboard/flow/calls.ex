defmodule FerricstoreServer.Health.Dashboard.Flow.Calls do
  @moduledoc false

  @flow_dashboard_detail_fetch_timeout_ms 5_000
  @flow_dashboard_list_fetch_timeout_ms 5_000

  def flow_dashboard_detail_fetch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :flow_dashboard_detail_fetch_timeout_ms,
      @flow_dashboard_detail_fetch_timeout_ms
    )
  end

  def flow_dashboard_list_fetch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :flow_dashboard_list_fetch_timeout_ms,
      @flow_dashboard_list_fetch_timeout_ms
    )
  end

  def flow_dashboard_flow_get(id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun) do
      fun when is_function(fun, 2) -> fun.(id, opts)
      fun when is_function(fun, 1) -> fun.(id)
      _ -> FerricStore.flow_get(id, opts)
    end
  end

  def flow_dashboard_flow_history(id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun) do
      fun when is_function(fun, 2) -> fun.(id, opts)
      _ -> FerricStore.flow_history(id, opts)
    end
  end

  def flow_dashboard_flow_list(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_list_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_list(type, opts)
    end
  end

  def flow_dashboard_flow_search(opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_search_fun) do
      fun when is_function(fun, 1) -> fun.(opts)
      _ -> FerricStore.flow_search(opts)
    end
  end

  def flow_dashboard_flow_stats(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_stats_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_stats(type, opts)
    end
  end

  def flow_dashboard_flow_terminals(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_terminals_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_terminals(type, opts)
    end
  end

  def flow_dashboard_flow_failures(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_failures_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_failures(type, opts)
    end
  end

  def flow_dashboard_flow_stuck(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_stuck_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_stuck(type, opts)
    end
  end

  def flow_dashboard_flow_reclaim(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_reclaim_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_reclaim(type, opts)
    end
  end

  def flow_dashboard_flow_by_parent(parent_id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_by_parent_fun) do
      fun when is_function(fun, 2) -> fun.(parent_id, opts)
      _ -> FerricStore.flow_by_parent(parent_id, opts)
    end
  end

  def flow_dashboard_flow_by_root(root_id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_by_root_fun) do
      fun when is_function(fun, 2) -> fun.(root_id, opts)
      _ -> FerricStore.flow_by_root(root_id, opts)
    end
  end

  def flow_dashboard_flow_by_correlation(correlation_id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_by_correlation_fun) do
      fun when is_function(fun, 2) -> fun.(correlation_id, opts)
      _ -> FerricStore.flow_by_correlation(correlation_id, opts)
    end
  end

  def flow_dashboard_flow_value_mget(refs) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun) do
      fun when is_function(fun, 1) -> fun.(refs)
      _ -> FerricStore.flow_value_mget(refs)
    end
  end

  def bounded_dashboard_call(fun, timeout_ms, operation) when is_function(fun, 0) do
    started_at = System.monotonic_time()
    caller = self()
    reply_ref = make_ref()

    {worker, monitor_ref} =
      spawn_monitor(fn ->
        result =
          try do
            {:ok, fun.()}
          rescue
            exception -> {:error, {:exit, {exception, __STACKTRACE__}}}
          catch
            kind, reason -> {:error, {:exit, {{kind, reason}, __STACKTRACE__}}}
          end

        send(caller, {reply_ref, result})
      end)

    result =
      receive do
        {^reply_ref, result} ->
          Process.demonitor(monitor_ref, [:flush])
          result

        {:DOWN, ^monitor_ref, :process, ^worker, reason} ->
          {:error, {:exit, reason}}
      after
        timeout_ms ->
          Process.exit(worker, :kill)
          await_worker_down(monitor_ref, worker)
          flush_worker_reply(reply_ref)
          {:error, :timeout}
      end

    emit_dashboard_flow_lookup(operation, result, timeout_ms, started_at)
    result
  end

  defp await_worker_down(monitor_ref, worker) do
    receive do
      {:DOWN, ^monitor_ref, :process, ^worker, _reason} -> :ok
    end
  end

  defp flush_worker_reply(reply_ref) do
    receive do
      {^reply_ref, _result} -> :ok
    after
      0 -> :ok
    end
  end

  defp emit_dashboard_flow_lookup(operation, result, timeout_ms, started_at) do
    :telemetry.execute(
      [:ferricstore, :dashboard, :flow, :lookup],
      %{
        duration_us:
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond),
        timeout_ms: timeout_ms
      },
      %{operation: operation, result: dashboard_flow_lookup_result(result)}
    )
  end

  defp dashboard_flow_lookup_result({:ok, _value}), do: :ok
  defp dashboard_flow_lookup_result({:error, :timeout}), do: :timeout
  defp dashboard_flow_lookup_result({:error, {:exit, _reason}}), do: :exit
  defp dashboard_flow_lookup_result({:error, _reason}), do: :error
end
