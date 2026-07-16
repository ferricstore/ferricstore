defmodule Ferricstore.CrossShardOpTopologyGuardTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../lib/ferricstore/cross_shard_op.ex", __DIR__)
  @lib_root Path.expand("../../lib/ferricstore", __DIR__)

  test "durable cross-group execution returns CROSSSLOT before standalone limits" do
    source = File.read!(@source_path)

    durable_branch = string_index!(source, "Router.durable_context?(ctx) ->")
    key_limit_branch = string_index!(source, "length(keys_with_roles) > @max_cross_shard_keys ->")

    assert durable_branch < key_limit_branch
    assert source =~ "Router.durable_context?(ctx) ->\n          @crossslot_error"
  end

  test "standalone coordination is non-cancellable and owned by the coordinator shard" do
    source = File.read!(@source_path)

    assert Regex.match?(
             ~r/GenServer\.call\(\s*\{:standalone_cross_shard_execute,\s*participant_indices,\s*execute_fn\},\s*:infinity\s*\)/s,
             source
           )

    refute source =~ "@standalone_barrier_timeout"
    refute source =~ "standalone_cross_shard_barrier_acquire"
    refute source =~ "standalone_cross_shard_barrier_release"
  end

  test "removed Raft coordinator vocabulary is absent from production code" do
    source =
      @lib_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.map_join("\n", &File.read!/1)

    for removed <- [
          "cross_shard_intent",
          "cross_shard_intents",
          "IntentResolver",
          "renew_key_locks",
          "locked_put",
          "locked_delete",
          "unlock_keys_owned"
        ] do
      refute source =~ removed
    end
  end

  test "fetch-or-compute locks are exact-key state, not global coordinator gates" do
    source =
      [
        "raft/state_machine/sections/async_apply.ex",
        "raft/state_machine/sections/compound_apply.ex",
        "raft/state_machine/sections/flow_claim_due.ex",
        "raft/waraft_storage/sections/segment_project_commands.ex"
      ]
      |> Enum.map_join("\n", fn relative -> File.read!(Path.join(@lib_root, relative)) end)

    refute source =~ "cross_shard_locks"
    refute Regex.match?(~r/fetch_or_compute_locks[^\n]*(?:==|!=)\s*%\{\}/, source)
  end

  defp string_index!(source, needle) do
    {index, _length} = :binary.match(source, needle)
    index
  end
end
