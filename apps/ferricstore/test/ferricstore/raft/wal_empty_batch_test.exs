defmodule Ferricstore.Raft.WalEmptyBatchTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    Process.sleep(100)
    :ok
  end

  test "non-write WAL batches do not schedule fdatasync" do
    wal_pid = Process.whereis(:ra_system.derive_names(Cluster.system_name()).wal)
    assert is_pid(wal_pid)

    parent = self()
    handler_id = {:wal_empty_batch_test, self()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :wal, :sync],
        &__MODULE__.handle_wal_sync/4,
        parent
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :gen_batch_server.cast(wal_pid, {:query, fn _state -> send(parent, :wal_query_seen) end})

    assert_receive :wal_query_seen, 1_000
    refute_receive :wal_sync, 100
  end

  def handle_wal_sync(_event, _measurements, _metadata, parent) do
    send(parent, :wal_sync)
  end
end
