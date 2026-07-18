defmodule Ferricstore.Raft.WARaftStartupWriteFenceTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @starting_key {WARaftBackend, :starting}

  setup do
    ShardHelpers.wait_default_pipeline_ready()
    ctx = FerricStore.Instance.get(:default)
    shard_index = 0
    key = ShardHelpers.key_for_shard(shard_index)
    shard = Router.shard_name(ctx, shard_index)
    server = :wa_raft_server.registered_name(:ferricstore_waraft_backend, shard_index + 1)
    server_pid = Process.whereis(server)

    assert is_pid(server_pid)
    _ = Router.delete(ctx, key)

    previous_starting = :persistent_term.get(@starting_key, :missing)

    on_exit(fn ->
      restore_starting(previous_starting)
      resume_if_suspended(server_pid)
      _ = Router.delete(ctx, key)
    end)

    {:ok, key: key, shard: shard, server_pid: server_pid}
  end

  test "startup rejects shard-forwarded writes without blocking recovery", %{
    key: key,
    shard: shard,
    server_pid: server_pid
  } do
    :persistent_term.put(@starting_key, true)
    :ok = :sys.suspend(server_pid)

    write = Task.async(fn -> GenServer.call(shard, {:put, key, "value", 0}, 5_000) end)

    assert {:ok, {:error, :backend_unavailable}} = Task.yield(write, 500)

    assert {:error, :invalid_flush_shard_caller} =
             GenServer.call(shard, {:prepare_promoted_flush_from_raft, {1, 0}}, 500)

    :ok = :sys.resume(server_pid)
    assert nil == Router.get(FerricStore.Instance.get(:default), key)
  end

  test "startup rejects asynchronous writes before backend submission", %{key: key} do
    :persistent_term.put(@starting_key, true)
    reply_ref = make_ref()

    assert :ok = WARaftBackend.write_async(0, {:put, key, "value", 0}, {self(), reply_ref})
    assert_receive {^reply_ref, {:error, :backend_unavailable}}, 500
    assert nil == Router.get(FerricStore.Instance.get(:default), key)
  end

  test "startup rejects multi-command admission as one fenced operation", %{key: key} do
    :persistent_term.put(@starting_key, true)

    assert [
             {:error, :backend_unavailable},
             {:error, :backend_unavailable}
           ] ==
             WARaftBackend.write_many([
               {0, {:put, key, "value", 0}},
               {0, {:delete, key}}
             ])

    assert nil == Router.get(FerricStore.Instance.get(:default), key)
  end

  test "backend restart owns the write fence through recovery" do
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :waraft, :backend, :startup_phase],
        fn _event, _measurements, metadata, _config ->
          if metadata.phase == :ensure_storage_runtime do
            send(test_pid, {:startup_fence_observed, WARaftBackend.starting?()})
          end
        end,
        nil
      )

    try do
      assert :ok = WARaftBackend.start(FerricStore.Instance.get(:default))
      assert_receive {:startup_fence_observed, true}, 500
      refute WARaftBackend.starting?()
    after
      :telemetry.detach(handler_id)
    end
  end

  defp restore_starting(:missing), do: :persistent_term.erase(@starting_key)
  defp restore_starting(value), do: :persistent_term.put(@starting_key, value)

  defp resume_if_suspended(pid) do
    if Process.alive?(pid) do
      _ = :sys.resume(pid)
    end

    :ok
  end
end
