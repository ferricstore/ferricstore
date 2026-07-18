defmodule Ferricstore.Review.H7DedicatedCompactCleanupTest do
  @moduledoc """
  Verifies that leftover compact_*.log files in dedicated promoted
  directories are cleaned up on recovery and don't crash file parsing.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  @moduletag :shard_kill

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.{Promotion, Router}
  alias Ferricstore.Test.ShardHelpers

  @test_threshold 5

  setup do
    apply_context_snapshot =
      ShardHelpers.replace_default_apply_context(promotion_threshold: @test_threshold)

    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(apply_context_snapshot)
      ShardHelpers.wait_shards_alive()
    end)
  end

  defp real_store, do: ShardHelpers.router_store()

  defp ukey(base), do: "h7_#{base}_#{:rand.uniform(9_999_999)}"

  defp dedicated_dir(key) do
    shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    Promotion.dedicated_path(data_dir, shard_idx, :hash, key)
  end

  test "shard starts after compact_*.log left in dedicated dir" do
    store = real_store()
    key = ukey("compact_cleanup")

    # Promote a hash
    pairs = Enum.flat_map(1..(@test_threshold + 1), fn i -> ["f_#{i}", "v_#{i}"] end)
    Hash.handle("HSET", [key | pairs], store)

    dir = dedicated_dir(key)

    ShardHelpers.eventually(
      fn -> File.exists?(dir) end,
      "hash should be promoted before compact cleanup setup"
    )

    # Create a fake leftover compact file
    compact_file = Path.join(dir, "compact_0.log")
    File.write!(compact_file, "partial compaction garbage")
    assert File.exists?(compact_file)

    # Flush and kill shard
    ShardHelpers.flush_all_shards()
    shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    ShardHelpers.kill_shard_safely(shard_idx)

    # Shard should restart without crashing
    # compact file should be cleaned up
    refute File.exists?(compact_file)

    # Data should survive
    ShardHelpers.eventually(
      fn ->
        "v_1" == Hash.handle("HGET", [key, "f_1"], store)
      end,
      "promoted hash data should survive after compact cleanup"
    )
  end
end
