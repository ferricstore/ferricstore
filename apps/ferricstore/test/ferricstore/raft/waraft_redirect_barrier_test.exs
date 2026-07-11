defmodule Ferricstore.Raft.WARaftRedirectBarrierTest do
  use ExUnit.Case, async: false

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.WARaftBackend

  test "redirected success fails closed when local apply cannot be proven" do
    unavailable_shard = 1_000_000

    assert ErrorReasons.write_timeout_unknown() ==
             WARaftBackend.__barrier_redirected_commit_for_test__(
               node(),
               unavailable_shard,
               :ok
             )
  end

  test "malformed redirected apply proofs fail closed without waiting for the barrier timeout" do
    task =
      Task.async(fn ->
        WARaftBackend.__barrier_redirected_commit_for_test__(
          node(),
          0,
          {:waraft_applied_at, :malformed_position, :ok}
        )
      end)

    result = Task.yield(task, 100) || Task.shutdown(task, :brutal_kill)
    assert {:ok, ErrorReasons.write_timeout_unknown()} == result
  end

  test "redirected apply proof rejects an older-term position even at a higher index" do
    refute WARaftBackend.__storage_position_satisfies_for_test__(
             {:raft_log_pos, 200, 4},
             {:raft_log_pos, 150, 5}
           )

    assert WARaftBackend.__storage_position_satisfies_for_test__(
             {:raft_log_pos, 150, 5},
             {:raft_log_pos, 150, 5}
           )

    assert WARaftBackend.__storage_position_satisfies_for_test__(
             {:raft_log_pos, 151, 5},
             {:raft_log_pos, 150, 5}
           )
  end
end
