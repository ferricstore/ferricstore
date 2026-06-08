defmodule Ferricstore.Store.PromotionTest.Sections.SmallSetStaysInSharedBitcask do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog
      alias Ferricstore.Commands.{Hash, List, Set, SortedSet, Strings}
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.HLC
      alias Ferricstore.Store.{CompoundKey, Promotion, Router}
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Test.ShardHelpers

  describe "small set stays in shared Bitcask" do
    test "set with fewer members than threshold is not promoted" do
      store = real_store()
      key = ukey("small_set")

      populate_set(store, key, @test_threshold - 1)

      refute promoted?(key)
      assert @test_threshold - 1 == Set.handle("SCARD", [key], store)
    end

    test "set with exactly threshold members is not promoted (threshold is exclusive)" do
      store = real_store()
      key = ukey("exact_threshold_set")

      populate_set(store, key, @test_threshold)

      refute promoted?(key)
    end
  end

  # ---------------------------------------------------------------------------
  # Set exceeding threshold gets promoted
  # ---------------------------------------------------------------------------

  describe "set promotion on threshold crossing" do
    test "set crossing threshold gets promoted to dedicated Bitcask" do
      store = real_store()
      key = ukey("promote_set")

      # Insert members up to threshold (not yet promoted)
      populate_set(store, key, @test_threshold)
      refute promoted?(key)

      # Add one more member to cross the threshold
      Set.handle("SADD", [key, "extra_member"], store)

      assert promoted?(key)
    end

    test "promoted set has dedicated directory on disk" do
      store = real_store()
      key = ukey("promote_set_dir")

      populate_set(store, key, @test_threshold + 1)

      assert promoted?(key)

      # Verify the dedicated directory exists
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "set:#{hash}"])
      assert File.dir?(dedicated_path)
    end
  end

  # ---------------------------------------------------------------------------
  # SMEMBERS on promoted set
  # ---------------------------------------------------------------------------

  describe "SMEMBERS on promoted set" do
    test "SMEMBERS returns all members after promotion" do
      store = real_store()
      key = ukey("smembers_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      members = Set.handle("SMEMBERS", [key], store)
      assert length(members) == n

      expected = for i <- 1..n, do: "member_#{i}"
      assert Enum.sort(members) == Enum.sort(expected)
    end
  end

  # ---------------------------------------------------------------------------
  # SISMEMBER on promoted set
  # ---------------------------------------------------------------------------

  describe "SISMEMBER on promoted set" do
    test "SISMEMBER returns 1 for existing member in promoted set" do
      store = real_store()
      key = ukey("sismember_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Set.handle("SISMEMBER", [key, "member_1"], store)
    end

    test "SISMEMBER returns 0 for missing member in promoted set" do
      store = real_store()
      key = ukey("sismember_miss_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Set.handle("SISMEMBER", [key, "nonexistent"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SREM on promoted set
  # ---------------------------------------------------------------------------

  describe "SREM on promoted set" do
    test "SREM removes member from promoted set" do
      store = real_store()
      key = ukey("srem_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Set.handle("SREM", [key, "member_1"], store)
      assert 0 == Set.handle("SISMEMBER", [key, "member_1"], store)
    end

    test "SREM on missing member in promoted set returns 0" do
      store = real_store()
      key = ukey("srem_miss_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Set.handle("SREM", [key, "nonexistent"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SCARD on promoted set
  # ---------------------------------------------------------------------------

  describe "SCARD on promoted set" do
    test "SCARD returns correct count for promoted set" do
      store = real_store()
      key = ukey("scard_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      assert n == Set.handle("SCARD", [key], store)
    end

    test "SCARD updates after member addition on promoted set" do
      store = real_store()
      key = ukey("scard_add_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      Set.handle("SADD", [key, "extra"], store)
      assert n + 1 == Set.handle("SCARD", [key], store)
    end

    test "SCARD updates after member removal on promoted set" do
      store = real_store()
      key = ukey("scard_rem_promoted")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      Set.handle("SREM", [key, "member_1"], store)
      assert n - 1 == Set.handle("SCARD", [key], store)
    end
  end

  # ---------------------------------------------------------------------------
  # SADD on promoted set (adding after promotion)
  # ---------------------------------------------------------------------------

  describe "SADD on promoted set" do
    test "SADD adds new member to promoted set" do
      store = real_store()
      key = ukey("sadd_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 1 == Set.handle("SADD", [key, "new_member"], store)
      assert 1 == Set.handle("SISMEMBER", [key, "new_member"], store)
    end

    test "SADD of existing member in promoted set returns 0" do
      store = real_store()
      key = ukey("sadd_existing_promoted")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      assert 0 == Set.handle("SADD", [key, "member_1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL on promoted set cleans up dedicated instance
  # ---------------------------------------------------------------------------

  describe "DEL on promoted set" do
    test "DEL removes promoted set and cleans up dedicated Bitcask" do
      store = real_store()
      key = ukey("del_promoted_set")

      populate_set(store, key, @test_threshold + 1)
      assert promoted?(key)

      # DEL the key
      Strings.handle("DEL", [key], store)

      # Key should be gone
      refute promoted?(key)
      assert 0 == Set.handle("SISMEMBER", [key, "member_1"], store)
      assert 0 == Set.handle("SCARD", [key], store)

      # Verify the dedicated directory was cleaned up
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      hash = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
      dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}", "set:#{hash}"])
      refute File.dir?(dedicated_path)
    end
  end

  # ---------------------------------------------------------------------------
  # Set promotion is one-way
  # ---------------------------------------------------------------------------

  describe "set promotion is one-way" do
    test "set stays promoted even with few members remaining" do
      store = real_store()
      key = ukey("stays_promoted_set")
      n = @test_threshold + 1

      populate_set(store, key, n)
      assert promoted?(key)

      # Delete most members, keep 2
      for i <- 3..n do
        Set.handle("SREM", [key, "member_#{i}"], store)
      end

      assert promoted?(key)
      assert 2 == Set.handle("SCARD", [key], store)
      assert 1 == Set.handle("SISMEMBER", [key, "member_1"], store)
    end
  end

  # ===========================================================================
  # SORTED SET PROMOTION
  # ===========================================================================

  # Inserts `n` members into a sorted set and returns the key.
    end
  end
end
