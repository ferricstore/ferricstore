defmodule Ferricstore.Review.C3CrossShardPendingWritesTest do
  @moduledoc """
  Proves cross-shard transaction pending writes land in the correct shard's
  Bitcask files.
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
    # Generate unique keys on different shards
    uid = System.unique_integer([:positive])
    {k0, k1} = find_unique_keys_on_different_shards(ctx, uid)
    shard0 = Router.shard_for(FerricStore.Instance.get(:default), k0)
    shard1 = Router.shard_for(FerricStore.Instance.get(:default), k1)

    %{k0: k0, k1: k1, shard0: shard0, shard1: shard1}
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

  describe "cross-shard tx pending writes isolation" do
    test "writes land in the correct shard's Bitcask files, not the anchor shard's",
         %{k0: k0, k1: k1, shard0: shard0, shard1: shard1} do
      assert shard0 != shard1

      queue = [{"SET", [k0, "val_shard0"]}, {"SET", [k1, "val_shard1"]}]
      result = Coordinator.execute(queue, %{}, nil)
      assert result == [:ok, :ok]

      ShardHelpers.eventually(
        fn ->
          assert Router.get(FerricStore.Instance.get(:default), k0) == "val_shard0"
          assert Router.get(FerricStore.Instance.get(:default), k1) == "val_shard1"
        end,
        "cross-shard values should be readable",
        10,
        100
      )

      ShardHelpers.flush_all_shards()

      # Wait for disk writes to settle
      ShardHelpers.eventually(
        fn ->
          MapSet.member?(keys_on_disk(shard0), k0) and MapSet.member?(keys_on_disk(shard1), k1)
        end,
        "keys not on disk after flush",
        30,
        100
      )

      disk_keys_shard0 = keys_on_disk(shard0)
      disk_keys_shard1 = keys_on_disk(shard1)

      assert MapSet.member?(disk_keys_shard0, k0),
             "key #{inspect(k0)} (shard #{shard0}) missing from shard #{shard0}'s Bitcask files"

      refute MapSet.member?(disk_keys_shard1, k0),
             "key #{inspect(k0)} was written to wrong shard"

      assert MapSet.member?(disk_keys_shard1, k1),
             "key #{inspect(k1)} (shard #{shard1}) missing from shard #{shard1}'s Bitcask files"

      refute MapSet.member?(disk_keys_shard0, k1),
             "key #{inspect(k1)} was written to wrong shard"
    end

    test "sm_pending_writes buffer is empty during cross-shard dispatch (no stale writes)",
         %{k0: k0, k1: k1, shard0: shard0, shard1: shard1} do
      assert shard0 != shard1

      Router.put(FerricStore.Instance.get(:default), k0, "before_0", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "before_1", 0)

      queue = [{"SET", [k0, "after_0"]}, {"SET", [k1, "after_1"]}]
      result = Coordinator.execute(queue, %{}, nil)
      assert result == [:ok, :ok]

      assert Router.get(FerricStore.Instance.get(:default), k0) == "after_0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "after_1"
    end
  end
end
