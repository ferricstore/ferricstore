defmodule Ferricstore.Flow.HistoryProjector.RetryTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.HistoryProjector

  test "flush does not synchronously reappend a batch after publication failure" do
    unique = System.unique_integer([:positive, :monotonic])
    instance_name = :"history_projector_retry_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_projector_retry_#{unique}")
    keydir = :ets.new(:"history_projector_retry_keydir_#{unique}", [:set, :public])
    attempts = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

    Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn
      ^dir, 0, [_entry] ->
        :atomics.add(attempts, 1, 1)
        {:error, :injected_publish_failure}
    end)

    ctx = %{
      name: instance_name,
      keydir_refs: {keydir},
      data_dir: Path.dirname(dir),
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false),
      flow_history_projector_flush_failures: :atomics.new(1, signed: false)
    }

    {:ok, pid} =
      HistoryProjector.start_link(
        shard_index: 0,
        shard_data_path: dir,
        instance_ctx: ctx,
        recover_on_init: false
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)

      case previous_hook do
        nil -> Application.delete_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)
        hook -> Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, hook)
      end

      File.rm_rf!(dir)
    end)

    history_key = Ferricstore.Flow.Keys.history_key("retry-flow")
    event_id = "1000-1"

    entry = %{
      key: Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id),
      expire_at_ms: 0,
      history_key: history_key,
      event_id: event_id,
      event_ms: 1_000,
      version: 1,
      value: "history-value",
      ra_index: 10
    }

    assert :ok = HistoryProjector.enqueue(ctx, 0, [entry], 10)
    assert {:error, :flush_failed} = HistoryProjector.flush(ctx, 0)
    assert :atomics.get(attempts, 1) == 1

    Process.sleep(260)
    retry_attempts = :atomics.get(attempts, 1)
    assert retry_attempts <= 3

    GenServer.stop(pid)

    history_path = HistoryProjector.history_file_path(dir, 0)
    assert {:ok, records} = NIF.v2_scan_file(history_path)
    assert length(records) == retry_attempts
  end

  test "projected watermark persistence retries after a transient failure" do
    unique = System.unique_integer([:positive, :monotonic])
    instance_name = :"history_projector_marker_retry_#{unique}"
    dir = Path.join(System.tmp_dir!(), "ferricstore_history_marker_retry_#{unique}")
    keydir = :ets.new(:"history_projector_marker_retry_keydir_#{unique}", [:set, :public])

    ctx = %{
      name: instance_name,
      keydir_refs: {keydir},
      data_dir: Path.dirname(dir),
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false),
      flow_history_projector_flush_failures: :atomics.new(1, signed: false)
    }

    {:ok, pid} =
      HistoryProjector.start_link(
        shard_index: 0,
        shard_data_path: dir,
        instance_ctx: ctx,
        recover_on_init: false
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
      File.rm_rf!(dir)
    end)

    history_key = Ferricstore.Flow.Keys.history_key("marker-retry-flow")
    event_id = "2000-1"

    entry = %{
      key: Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id),
      expire_at_ms: 0,
      history_key: history_key,
      event_id: event_id,
      event_ms: 2_000,
      version: 1,
      value: "history-value",
      ra_index: 20
    }

    assert :ok = HistoryProjector.enqueue(ctx, 0, [entry], 20)
    assert :ok = HistoryProjector.flush(ctx, 0)
    refute HistoryProjector.durable?(ctx, 0, dir, 20)

    marker_path = Ferricstore.Flow.HistoryProjectedIndex.path(dir)
    File.rm!(marker_path)
    File.mkdir_p!(marker_path)

    assert :requested = HistoryProjector.request(ctx, 0, dir, 20)

    assert wait_until(fn ->
             :atomics.get(ctx.flow_history_projector_flush_failures, 1) > 0
           end)

    File.rm_rf!(marker_path)

    assert wait_until(fn -> HistoryProjector.durable?(ctx, 0, dir, 20) end)
  end

  defp wait_until(fun, attempts \\ 100)

  defp wait_until(_fun, 0), do: false

  defp wait_until(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end
end
