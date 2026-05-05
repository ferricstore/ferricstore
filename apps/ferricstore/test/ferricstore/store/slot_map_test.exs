defmodule Ferricstore.Store.SlotMapTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.SlotMap

  @num_slots 1024

  describe "build_uniform/1" do
    test "creates 1024-element tuple" do
      map = SlotMap.build_uniform(4)
      assert tuple_size(map) == @num_slots
    end

    test "distributes slots evenly across 4 shards" do
      map = SlotMap.build_uniform(4)
      counts = count_per_shard(map, 4)
      assert counts == %{0 => 256, 1 => 256, 2 => 256, 3 => 256}
    end

    test "distributes slots evenly across 8 shards" do
      map = SlotMap.build_uniform(8)
      counts = count_per_shard(map, 8)

      for shard <- 0..7 do
        assert counts[shard] == 128
      end
    end

    test "single shard gets all 1024 slots" do
      map = SlotMap.build_uniform(1)

      for slot <- 0..(@num_slots - 1) do
        assert elem(map, slot) == 0
      end
    end

    test "128 shards gets 8 slots each" do
      map = SlotMap.build_uniform(128)
      counts = count_per_shard(map, 128)

      for shard <- 0..127 do
        assert counts[shard] == 8
      end
    end

    test "handles non-power-of-2 shard count (3 shards)" do
      map = SlotMap.build_uniform(3)
      counts = count_per_shard(map, 3)
      # 1024 / 3 = 341 remainder 1 => shards 0 gets 342, shards 1,2 get 341
      assert counts[0] == 342
      assert counts[1] == 341
      assert counts[2] == 341
    end

    test "all values are valid shard indices" do
      map = SlotMap.build_uniform(4)

      for slot <- 0..(@num_slots - 1) do
        assert elem(map, slot) in 0..3
      end
    end

    test "distribution is roughly even (no shard gets >2x average)" do
      for shard_count <- [3, 5, 7, 10, 16, 64, 128] do
        map = SlotMap.build_uniform(shard_count)
        average = @num_slots / shard_count
        counts = count_per_shard(map, shard_count)

        for {_shard, count} <- counts do
          assert count <= ceil(average) + 1,
                 "shard_count=#{shard_count}: shard got #{count} slots, average=#{average}"
        end
      end
    end
  end

  describe "slot_for_key/1" do
    test "returns value in 0..1023" do
      slot = SlotMap.slot_for_key("user:42")
      assert slot >= 0 and slot <= 1023
    end

    test "is deterministic" do
      assert SlotMap.slot_for_key("test") == SlotMap.slot_for_key("test")
    end

    test "hash tags co-locate keys" do
      assert SlotMap.slot_for_key("{tag}:a") == SlotMap.slot_for_key("{tag}:b")

      assert SlotMap.slot_for_key("{user:42}:session") ==
               SlotMap.slot_for_key("{user:42}:profile")
    end

    test "empty key returns valid slot" do
      slot = SlotMap.slot_for_key("")
      assert slot >= 0 and slot <= 1023
    end

    test "large key returns valid slot" do
      big_key = String.duplicate("x", 10_000)
      slot = SlotMap.slot_for_key(big_key)
      assert slot >= 0 and slot <= 1023
    end

    test "uses stable crc32 slots clients can compute outside BEAM" do
      assert SlotMap.slot_for_key("123456789") == 294
      assert SlotMap.slot_for_key("plain") == 719
      assert SlotMap.slot_for_key("{user:42}:session") == 390
      assert SlotMap.slot_for_key("{}empty") == 873
    end

    test "key distribution across 1024 slots is roughly uniform" do
      n = 100_000

      slot_counts =
        Enum.reduce(1..n, %{}, fn i, acc ->
          slot = SlotMap.slot_for_key("uniform_test_key_#{i}")
          Map.update(acc, slot, 1, &(&1 + 1))
        end)

      expected = n / @num_slots
      # Allow 3x deviation (generous for statistical test)
      for {_slot, count} <- slot_counts do
        assert count < expected * 3,
               "slot got #{count} keys, expected ~#{expected}"
      end

      # At least 90% of slots should be used
      used_slots = map_size(slot_counts)

      assert used_slots > @num_slots * 0.9,
             "only #{used_slots}/#{@num_slots} slots used"
    end
  end

  describe "reassign_slot/3" do
    test "changes slot assignment" do
      map = SlotMap.build_uniform(4)
      old_owner = elem(map, 500)
      new_owner = rem(old_owner + 1, 4)
      new_map = SlotMap.reassign_slot(map, 500, new_owner)
      assert elem(new_map, 500) == new_owner
    end

    test "does not affect other slots" do
      map = SlotMap.build_uniform(4)
      new_map = SlotMap.reassign_slot(map, 500, 3)

      for slot <- 0..(@num_slots - 1), slot != 500 do
        assert elem(new_map, slot) == elem(map, slot)
      end
    end

    test "tuple size is preserved" do
      map = SlotMap.build_uniform(4)
      new_map = SlotMap.reassign_slot(map, 0, 2)
      assert tuple_size(new_map) == @num_slots
    end
  end

  describe "shard_for_slot/2" do
    test "returns correct shard from map" do
      map = SlotMap.build_uniform(4)
      # First 256 slots should map to shard 0
      assert SlotMap.shard_for_slot(map, 0) == 0
      assert SlotMap.shard_for_slot(map, 255) == 0
      # Next 256 to shard 1
      assert SlotMap.shard_for_slot(map, 256) == 1
    end
  end

  describe "slot_count_for_shard/2" do
    test "returns correct count" do
      map = SlotMap.build_uniform(4)
      assert SlotMap.slot_count_for_shard(map, 0) == 256
      assert SlotMap.slot_count_for_shard(map, 3) == 256
    end

    test "returns 0 for non-existent shard" do
      map = SlotMap.build_uniform(4)
      assert SlotMap.slot_count_for_shard(map, 99) == 0
    end
  end

  describe "num_slots/0" do
    test "returns 1024" do
      assert SlotMap.num_slots() == 1024
    end
  end

  describe "slot_ranges/1" do
    test "returns contiguous ranges for uniform distribution" do
      map = SlotMap.build_uniform(4)
      ranges = SlotMap.slot_ranges(map)
      assert length(ranges) == 4
      assert {0, 255, 0} in ranges
      assert {256, 511, 1} in ranges
      assert {512, 767, 2} in ranges
      assert {768, 1023, 3} in ranges
    end

    test "returns single range for 1 shard" do
      map = SlotMap.build_uniform(1)
      ranges = SlotMap.slot_ranges(map)
      assert ranges == [{0, 1023, 0}]
    end
  end

  # --- helpers ---

  defp count_per_shard(map, shard_count) do
    Enum.reduce(0..(@num_slots - 1), Map.new(0..(shard_count - 1), &{&1, 0}), fn slot, acc ->
      shard = elem(map, slot)
      Map.update!(acc, shard, &(&1 + 1))
    end)
  end
end
