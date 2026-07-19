defmodule Ferricstore.Raft.LmdbProjectionCommandContractTest do
  use ExUnit.Case, async: true

  @source_path Path.expand(
                 "../../../lib/ferricstore/raft/state_machine/sections/lmdb_projection.ex",
                 __DIR__
               )

  test "hibernation after-flush command uses the canonical beta contract" do
    source = File.read!(@source_path)

    refute source =~ ":hibernate_flow_evict_hot_v1"
    assert source =~ "{:hibernate_flow_evict_hot,"
  end

  test "pending hibernation keeps only the final candidate for each state key" do
    newest_a = {"state-a", %{version: 3}, "value-a-3"}
    only_b = {"state-b", %{version: 2}}
    oldest_a = {"state-a", %{version: 1}, "value-a-1"}

    assert [^only_b, ^newest_a] =
             Ferricstore.Raft.StateMachine.__coalesce_pending_flow_hibernation_candidates_for_test__(
               [newest_a, only_b, oldest_a]
             )
  end

  test "hibernation rejects a candidate superseded by a newer state value" do
    candidate = %{
      id: "flow",
      type: "job",
      state: "waiting",
      version: 1,
      state_enter_seq: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: 1_000,
      priority: 0,
      partition_key: "tenant",
      root_flow_id: "flow"
    }

    newer = %{candidate | version: 2, state: "ready"}
    candidate_value = Ferricstore.Flow.encode_record(candidate)
    newer_value = Ferricstore.Flow.encode_record(newer)

    assert Ferricstore.Raft.StateMachine.__flow_hibernation_candidate_current_for_test__(
             candidate_value,
             candidate_value,
             candidate
           )

    refute Ferricstore.Raft.StateMachine.__flow_hibernation_candidate_current_for_test__(
             newer_value,
             candidate_value,
             candidate
           )
  end
end
