defmodule Ferricstore.ReviewR2.C2LocksPersistTest do
  @moduledoc """
  Verifies that CrossShardOp locks and intents survive shard restarts.
  Previously stored in process dictionary (lost on crash). Now in Raft state.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  @moduletag :shard_kill

  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
    :ok
  end

  defp unique_key(shard_idx) do
    base = ShardHelpers.key_for_shard(shard_idx)
    "#{base}_persist_#{:rand.uniform(9_999_999)}"
  end

  defp raft_result({:ok, {:applied_at, _, result}, _}), do: result
  defp raft_result({:error, _reason} = error), do: error

  describe "locks survive shard restart" do
    test "lock persists after shard kill + restart" do
      key = unique_key(0)
      shard_id = Cluster.shard_server_id(0)
      owner_ref = make_ref()
      expire_at = System.os_time(:millisecond) + 30_000

      # Lock the key
      {:ok, {:applied_at, _, :ok}, _} =
        Ferricstore.Raft.CommandClock.process_command(
          shard_id,
          {:lock_keys, [key], owner_ref, expire_at}
        )

      # Kill and restart shard
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(0)

      # After restart, try to lock same key with different owner — should FAIL
      # because the original lock should have survived
      other_ref = make_ref()
      other_expire = System.os_time(:millisecond) + 5000

      result =
        shard_id
        |> Ferricstore.Raft.CommandClock.process_command(
          {:lock_keys, [key], other_ref, other_expire}
        )
        |> raft_result()

      assert result == {:error, :keys_locked},
             "Lock should survive shard restart — got #{inspect(result)} instead of {:error, :keys_locked}"

      # Cleanup
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:unlock_keys, [key], owner_ref})
    end
  end

  describe "intents survive shard restart" do
    test "intent persists after shard kill + restart" do
      shard_id = Cluster.shard_server_id(0)
      owner_ref = make_ref()

      # Write intent
      Ferricstore.Raft.CommandClock.process_command(
        shard_id,
        {:cross_shard_intent, owner_ref,
         %{
           command: :smove,
           keys: %{source: "a", dest: "b"},
           status: :executing,
           created_at: System.os_time(:millisecond)
         }}
      )

      # Kill and restart shard
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(0)

      # After restart, intent should still be there
      {:ok, {:applied_at, _, intents}, _} =
        Ferricstore.Raft.CommandClock.process_command(shard_id, {:get_intents})

      assert Map.has_key?(intents, owner_ref),
             "Intent should survive shard restart"

      # Cleanup
      Ferricstore.Raft.CommandClock.process_command(shard_id, {:delete_intent, owner_ref})
    end
  end
end
