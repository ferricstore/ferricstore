defmodule Ferricstore.Review.C3CrossShardPendingWritesTest do
  @moduledoc """
  Proves transaction pending writes remain shard-local and independent Raft
  groups fail closed before any mutation.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag :shard_kill

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.PreparedTransactionCoordinator, as: Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)

    ctx = FerricStore.Instance.get(:default)
    uid = System.unique_integer([:positive])
    {k0, k1} = find_unique_keys_on_different_shards(ctx, uid)
    same0 = "{c3:#{uid}}:a"
    same1 = "{c3:#{uid}}:b"
    shard0 = Router.shard_for(ctx, k0)
    shard1 = Router.shard_for(ctx, k1)
    same_shard = Router.shard_for(ctx, same0)

    %{
      k0: k0,
      k1: k1,
      shard0: shard0,
      shard1: shard1,
      same0: same0,
      same1: same1,
      same_shard: same_shard
    }
  end

  defp find_unique_keys_on_different_shards(ctx, uid) do
    k0 = "c3_a_#{uid}"
    s0 = Router.shard_for(ctx, k0)

    k1 =
      Enum.find_value(0..1000, fn i ->
        candidate = "c3_b_#{uid}_#{i}"
        if Router.shard_for(ctx, candidate) != s0, do: candidate
      end)

    {k0, k1}
  end

  defp keys_on_disk(shard_index) do
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)

    case File.ls(shard_path) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.reject(&String.starts_with?(&1, "compact_"))
        |> Enum.flat_map(fn log_name ->
          log_path = Path.join(shard_path, log_name)

          case NIF.v2_scan_file(log_path) do
            {:ok, records} ->
              records
              |> Enum.reduce(%{}, fn {key, _off, _vs, _exp, is_tombstone}, acc ->
                Map.put(acc, key, is_tombstone)
              end)
              |> Enum.reject(fn {_key, is_tombstone} -> is_tombstone end)
              |> Enum.map(fn {key, _} -> key end)

            _ ->
              []
          end
        end)
        |> MapSet.new()

      _ ->
        MapSet.new()
    end
  end

  describe "transaction pending writes isolation" do
    test "same-slot writes land in their owning shard's Bitcask files",
         %{same0: same0, same1: same1, same_shard: same_shard} do
      ctx = FerricStore.Instance.get(:default)
      assert Router.shard_for(ctx, same1) == same_shard

      queue = [{"SET", [same0, "first"]}, {"SET", [same1, "second"]}]
      result = Coordinator.execute(queue, %{}, nil)
      assert result == [:ok, :ok]

      ShardHelpers.eventually(
        fn ->
          assert Router.get(ctx, same0) == "first"
          assert Router.get(ctx, same1) == "second"
        end,
        "same-slot values should be readable",
        10,
        100
      )

      ShardHelpers.flush_all_shards()

      # Wait for disk writes to settle
      ShardHelpers.eventually(
        fn ->
          disk_keys = keys_on_disk(same_shard)
          MapSet.member?(disk_keys, same0) and MapSet.member?(disk_keys, same1)
        end,
        "keys not on disk after flush",
        30,
        100
      )

      disk_keys = keys_on_disk(same_shard)
      assert MapSet.member?(disk_keys, same0)
      assert MapSet.member?(disk_keys, same1)
    end

    test "independent Raft groups are rejected without partial or stale writes",
         %{k0: k0, k1: k1, shard0: shard0, shard1: shard1} do
      assert shard0 != shard1

      ctx = FerricStore.Instance.get(:default)
      Router.put(ctx, k0, "before_0", 0)
      Router.put(ctx, k1, "before_1", 0)

      queue = [{"SET", [k0, "after_0"]}, {"SET", [k1, "after_1"]}]
      result = Coordinator.execute(queue, %{}, nil)
      assert result == {:error, "CROSSSLOT Keys in request don't hash to the same slot"}

      assert Router.get(ctx, k0) == "before_0"
      assert Router.get(ctx, k1) == "before_1"
    end
  end
end
