defmodule Ferricstore.Flow.LMDBWriter.Control do
  @moduledoc false

  alias Ferricstore.Flow.LMDBWriter.Registry
  alias Ferricstore.Flow.LMDBWriter.Shards
  alias Ferricstore.Flow.LMDBWriter.Telemetry

  def flush_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    flush_all(:default, shard_count, 30_000)
  end

  def flush_all(shard_count, timeout)
      when is_integer(shard_count) and shard_count >= 0 and is_integer(timeout) do
    flush_all(:default, shard_count, timeout)
  end

  def flush_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    flush_all(instance_name, shard_count, 30_000)
  end

  def flush_all(instance_name, shard_count, timeout)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 and
             is_integer(timeout) do
    shard_count
    |> Shards.indexes()
    |> Task.async_stream(
      fn shard_index -> {shard_index, flush(instance_name, shard_index, timeout)} end,
      max_concurrency: Shards.flush_all_concurrency(shard_count),
      on_timeout: :kill_task,
      ordered: false,
      timeout: Shards.flush_all_task_timeout(timeout)
    )
    |> Enum.reduce(:ok, &Shards.merge_flush_all_result/2)
  end

  def flush(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    flush(:default, shard_index, 30_000)
  end

  def flush(shard_index, timeout)
      when is_integer(shard_index) and shard_index >= 0 and is_integer(timeout) do
    flush(:default, shard_index, timeout)
  end

  def flush(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    flush(instance_name, shard_index, 30_000)
  end

  def flush(instance_name, shard_index, timeout)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(timeout) do
    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) ->
        try do
          case GenServer.call(pid, :flush, timeout) do
            :ok -> :ok
            {:error, _reason} = error -> error
          end
        catch
          :exit, reason ->
            writer_unavailable(:flush, instance_name, shard_index, reason, 0)
        end

      nil ->
        writer_unavailable(:flush, instance_name, shard_index, :writer_not_started, 0)
    end
  end

  def suspend_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    suspend_all(:default, shard_count)
  end

  def suspend_all(shard_count, opts)
      when is_integer(shard_count) and shard_count >= 0 and is_list(opts) do
    suspend_all(:default, shard_count, opts)
  end

  def suspend_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    suspend_all(instance_name, shard_count, flush: true)
  end

  def suspend_all(instance_name, shard_count, opts)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 and
             is_list(opts) do
    mark_instance_suspended(instance_name)
    flush? = Keyword.get(opts, :flush, true)

    Enum.each(Shards.indexes(shard_count), fn shard_index ->
      _ =
        if flush? do
          suspend(instance_name, shard_index)
        else
          suspend_without_flush(instance_name, shard_index)
        end
    end)

    :ok
  end

  def suspend(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    mark_instance_suspended(instance_name)

    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) ->
        GenServer.call(pid, :suspend, 5_000)

      nil ->
        :ok
    end
  catch
    :exit, _reason -> :ok
  end

  def suspend_without_flush(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    mark_instance_suspended(instance_name)

    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) ->
        GenServer.cast(pid, :suspend_without_flush)
        :ok

      nil ->
        :ok
    end
  end

  def resume_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    resume_all(:default, shard_count)
  end

  def resume_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    clear_instance_suspended(instance_name)

    Enum.each(Shards.indexes(shard_count), fn shard_index ->
      case Process.whereis(name(instance_name, shard_index)) do
        pid when is_pid(pid) -> GenServer.cast(pid, :resume)
        nil -> :ok
      end
    end)

    :ok
  end

  def discard_all(shard_count) when is_integer(shard_count) and shard_count >= 0 do
    discard_all(:default, shard_count)
  end

  def discard_all(instance_name, shard_count)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    Enum.each(Shards.indexes(shard_count), fn shard_index ->
      _ = discard(instance_name, shard_index)
    end)

    :ok
  end

  def discard(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) -> GenServer.call(pid, :discard, 5_000)
      nil -> :ok
    end
  catch
    :exit, _reason -> :ok
  end

  def prepare_snapshot_install(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) ->
        with :ok <- GenServer.call(pid, :flush, 30_000) do
          GenServer.call(pid, :prepare_snapshot_install, 30_000)
        end

      nil ->
        :ok
    end
  catch
    :exit, reason -> {:error, {:lmdb_writer_snapshot_handoff_failed, reason}}
  end

  def resume_after_snapshot_install(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    snapshot_call(instance_name, shard_index, :resume_after_snapshot_install)
  end

  defp snapshot_call(instance_name, shard_index, request) do
    case Process.whereis(name(instance_name, shard_index)) do
      pid when is_pid(pid) -> GenServer.call(pid, request, 30_000)
      nil -> :ok
    end
  catch
    :exit, reason -> {:error, {:lmdb_writer_snapshot_handoff_failed, reason}}
  end

  defp name(instance_name, shard_index), do: Registry.name(instance_name, shard_index)

  defp mark_instance_suspended(instance_name) when is_atom(instance_name),
    do: Registry.mark_instance_suspended(instance_name)

  defp clear_instance_suspended(instance_name) when is_atom(instance_name),
    do: Registry.clear_instance_suspended(instance_name)

  defp writer_unavailable(operation, instance_name, shard_index, reason, op_count),
    do: Telemetry.writer_unavailable(operation, instance_name, shard_index, reason, op_count)
end
