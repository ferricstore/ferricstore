defmodule Ferricstore.Raft.CompoundShardRaftGuardTest do
  use ExUnit.Case, async: true

  @compound_path Path.expand(
                   "../../../lib/ferricstore/store/shard/compound.ex",
                   __DIR__
                 )

  test "Shard compound Raft handlers do not mutate promoted storage directly" do
    source = File.read!(@compound_path)

    # Dedicated Bitcask is only a storage placement detail. With Raft enabled,
    # every compound mutation must enter the replicated log first; otherwise a
    # promoted hash/set/zset can diverge across replicas.
    assert function_body(source, "handle_compound_put_raft") =~ "Ferricstore.Raft.Batcher.write("
    assert function_body(source, "handle_compound_put_raft") =~ "{:compound_put,"

    refute function_body(source, "handle_compound_put_raft") =~ "promoted_write("

    assert function_body(source, "handle_compound_delete_raft") =~
             "Ferricstore.Raft.Batcher.write("

    assert function_body(source, "handle_compound_delete_raft") =~ "{:compound_delete,"

    refute function_body(source, "handle_compound_delete_raft") =~ "promoted_tombstone("

    assert function_body(source, "handle_compound_delete_prefix_raft") =~
             "Ferricstore.Raft.Batcher.write("

    assert function_body(source, "handle_compound_delete_prefix_raft") =~
             "{:compound_delete_prefix,"

    refute function_body(source, "handle_compound_delete_prefix_raft") =~
             "Promotion.cleanup_promoted!("
  end

  defp function_body(source, function_name) do
    [_before, rest] = :binary.split(source, "defp #{function_name}")

    case :binary.split(rest, "\n  defp ") do
      [body, _after] -> body
      [body] -> body
    end
  end
end
