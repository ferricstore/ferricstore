defmodule Ferricstore.Commands.HyperLogLogTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.Set
  alias Ferricstore.Commands.HyperLogLog, as: HLLCmd
  alias Ferricstore.HyperLogLog, as: HLL
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # PFADD
  # ---------------------------------------------------------------------------

  describe "PFADD" do
    test "PFADD to new key creates sketch and returns 1" do
      store = MockStore.make()
      assert 1 == HLLCmd.handle("PFADD", ["mykey", "hello"], store)

      # The key now holds a valid HLL sketch
      sketch = store.get.("mykey")
      assert HLL.valid_sketch?(sketch)
    end

    test "PFADD treats a fully expired hash as a missing HLL key before TYPE cleanup" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      field_key = CompoundKey.hash_field("mykey", "field")
      store.compound_put.("mykey", field_key, "value", System.os_time(:millisecond) - 1)

      assert 1 == HLLCmd.handle("PFADD", ["mykey", "hello"], store)
      assert HLL.valid_sketch?(store.get.("mykey"))
    end

    test "PFADD same element twice — second returns 0" do
      store = MockStore.make()
      assert 1 == HLLCmd.handle("PFADD", ["mykey", "hello"], store)
      assert 0 == HLLCmd.handle("PFADD", ["mykey", "hello"], store)
    end

    test "PFADD multiple elements — returns 1 if any modified sketch" do
      store = MockStore.make()
      assert 1 == HLLCmd.handle("PFADD", ["mykey", "a", "b", "c"], store)
    end

    test "PFADD multiple elements all duplicates — returns 0" do
      store = MockStore.make()
      assert 1 == HLLCmd.handle("PFADD", ["mykey", "a", "b"], store)
      assert 0 == HLLCmd.handle("PFADD", ["mykey", "a", "b"], store)
    end

    test "PFADD new element to existing sketch returns 1" do
      store = MockStore.make()
      assert 1 == HLLCmd.handle("PFADD", ["mykey", "a"], store)
      assert 1 == HLLCmd.handle("PFADD", ["mykey", "b"], store)
    end

    test "PFADD with empty string element works" do
      store = MockStore.make()
      assert 1 == HLLCmd.handle("PFADD", ["mykey", ""], store)

      sketch = store.get.("mykey")
      assert HLL.valid_sketch?(sketch)
      assert HLL.count(sketch) >= 1
    end

    test "PFADD wrong number of args — error (no key)" do
      assert {:error, msg} = HLLCmd.handle("PFADD", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "PFADD with key but no elements creates an empty sketch" do
      store = MockStore.make()

      assert 1 == HLLCmd.handle("PFADD", ["mykey"], store)
      assert HLL.valid_sketch?(store.get.("mykey"))
      assert 0 == HLLCmd.handle("PFCOUNT", ["mykey"], store)
    end

    test "PFADD with key but no elements returns 0 when sketch already exists" do
      store = MockStore.make()
      assert 1 == HLLCmd.handle("PFADD", ["mykey"], store)

      assert 0 == HLLCmd.handle("PFADD", ["mykey"], store)
    end

    test "PFADD to key holding non-HLL value returns WRONGTYPE error" do
      store = MockStore.make(%{"mykey" => {"not-a-sketch", 0}})

      assert {:error, msg} = HLLCmd.handle("PFADD", ["mykey", "elem"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "PFADD with no elements on non-HLL value returns WRONGTYPE error" do
      store = MockStore.make(%{"mykey" => {"not-a-sketch", 0}})

      assert {:error, msg} = HLLCmd.handle("PFADD", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "PFADD rejects wrong-size cold value from metadata without loading it" do
      store = wrong_size_cold_store(%{"mykey" => 1_048_576})

      assert {:error, msg} = HLLCmd.handle("PFADD", ["mykey", "elem"], store)
      assert msg =~ "WRONGTYPE"
      refute_received {:loaded_cold_hll_candidate, "mykey"}
    end

    test "PFADD with no elements rejects wrong-size cold value without loading it" do
      store = wrong_size_cold_store(%{"mykey" => 1_048_576})

      assert {:error, msg} = HLLCmd.handle("PFADD", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
      refute_received {:loaded_cold_hll_candidate, "mykey"}
    end

    test "PFADD to a compound key returns WRONGTYPE and preserves it" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, msg} = HLLCmd.handle("PFADD", ["mykey", "elem"], store)
      assert msg =~ "WRONGTYPE"
      assert store.get.("mykey") == nil
      assert Set.handle("SMEMBERS", ["mykey"], store) == ["member"]
    end
  end

  # ---------------------------------------------------------------------------
  # PFCOUNT
  # ---------------------------------------------------------------------------

  describe "PFCOUNT" do
    test "PFCOUNT on non-existent key returns 0" do
      store = MockStore.make()
      assert 0 == HLLCmd.handle("PFCOUNT", ["nokey"], store)
    end

    test "PFCOUNT after adding single element returns 1" do
      store = MockStore.make()
      HLLCmd.handle("PFADD", ["mykey", "hello"], store)
      assert 1 == HLLCmd.handle("PFCOUNT", ["mykey"], store)
    end

    test "PFCOUNT after adding N=100 unique elements — within 10%" do
      store = MockStore.make()
      n = 100

      elements = for i <- 1..n, do: "element:#{i}"
      HLLCmd.handle("PFADD", ["mykey" | elements], store)

      count = HLLCmd.handle("PFCOUNT", ["mykey"], store)
      assert_in_delta count, n, n * 0.10
    end

    test "PFCOUNT after adding N=1000 unique elements — within 10%" do
      store = MockStore.make()
      n = 1000

      elements = for i <- 1..n, do: "element:#{i}"
      HLLCmd.handle("PFADD", ["mykey" | elements], store)

      count = HLLCmd.handle("PFCOUNT", ["mykey"], store)
      assert_in_delta count, n, n * 0.10
    end

    test "PFCOUNT multiple keys — merges in memory, returns combined estimate" do
      store = MockStore.make()

      # Add distinct elements to two separate keys
      HLLCmd.handle("PFADD", ["key1", "a", "b", "c"], store)
      HLLCmd.handle("PFADD", ["key2", "d", "e", "f"], store)

      count = HLLCmd.handle("PFCOUNT", ["key1", "key2"], store)
      # Combined cardinality should be approximately 6
      assert_in_delta count, 6, 3
    end

    test "PFCOUNT multiple keys with overlap" do
      store = MockStore.make()

      HLLCmd.handle("PFADD", ["key1", "a", "b", "c"], store)
      HLLCmd.handle("PFADD", ["key2", "b", "c", "d"], store)

      count = HLLCmd.handle("PFCOUNT", ["key1", "key2"], store)
      # Combined unique cardinality should be approximately 4 (a, b, c, d)
      assert_in_delta count, 4, 3
    end

    test "PFCOUNT multiple keys with non-existent key — treated as empty" do
      store = MockStore.make()
      HLLCmd.handle("PFADD", ["key1", "a", "b"], store)

      count = HLLCmd.handle("PFCOUNT", ["key1", "nonexistent"], store)
      # Should still return approximately 2
      assert_in_delta count, 2, 2
    end

    test "PFCOUNT multiple keys uses batch_get when the store provides it" do
      parent = self()
      sketch = HLL.new()

      store = %{
        batch_get: fn keys ->
          send(parent, {:batch_get, keys})

          Enum.map(keys, fn
            "key1" -> sketch
            "key2" -> nil
          end)
        end,
        get: fn key ->
          flunk("PFCOUNT should use batch_get, got per-key GET for #{inspect(key)}")
        end,
        compound_get: fn _redis_key, _compound_key -> nil end
      }

      assert 0 == HLLCmd.handle("PFCOUNT", ["key1", "key2"], store)
      assert_received {:batch_get, ["key1", "key2"]}
    end

    test "PFCOUNT wrong number of args — error" do
      assert {:error, msg} = HLLCmd.handle("PFCOUNT", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "PFCOUNT on key holding non-HLL value returns WRONGTYPE error" do
      store = MockStore.make(%{"mykey" => {"not-a-sketch", 0}})

      assert {:error, msg} = HLLCmd.handle("PFCOUNT", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "PFCOUNT rejects wrong-size cold value from metadata without batch loading it" do
      store = wrong_size_cold_store(%{"mykey" => 1_048_576})

      assert {:error, msg} = HLLCmd.handle("PFCOUNT", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
      refute_received {:batch_loaded_cold_hll_candidates, ["mykey"]}
    end

    test "PFCOUNT on a compound key returns WRONGTYPE" do
      store = MockStore.make()
      Set.handle("SADD", ["mykey", "member"], store)

      assert {:error, msg} = HLLCmd.handle("PFCOUNT", ["mykey"], store)
      assert msg =~ "WRONGTYPE"
    end
  end

  # ---------------------------------------------------------------------------
  # PFMERGE
  # ---------------------------------------------------------------------------

  describe "PFMERGE" do
    test "PFMERGE creates dest with merged sketch" do
      store = MockStore.make()

      HLLCmd.handle("PFADD", ["src1", "a", "b"], store)
      HLLCmd.handle("PFADD", ["src2", "c", "d"], store)

      assert :ok = HLLCmd.handle("PFMERGE", ["dest", "src1", "src2"], store)

      dest_sketch = store.get.("dest")
      assert HLL.valid_sketch?(dest_sketch)

      count = HLL.count(dest_sketch)
      assert_in_delta count, 4, 3
    end

    test "PFMERGE with non-existent source — treated as empty" do
      store = MockStore.make()

      HLLCmd.handle("PFADD", ["src1", "a"], store)

      assert :ok = HLLCmd.handle("PFMERGE", ["dest", "src1", "nonexistent"], store)

      count = HLLCmd.handle("PFCOUNT", ["dest"], store)
      assert_in_delta count, 1, 1
    end

    test "PFMERGE into existing dest — merges with existing" do
      store = MockStore.make()

      # Populate dest with some elements first
      HLLCmd.handle("PFADD", ["dest", "x", "y"], store)
      HLLCmd.handle("PFADD", ["src", "z"], store)

      assert :ok = HLLCmd.handle("PFMERGE", ["dest", "src"], store)

      count = HLLCmd.handle("PFCOUNT", ["dest"], store)
      # Should contain approximately x, y, z = 3
      assert_in_delta count, 3, 2
    end

    test "PFMERGE with all non-existent sources creates empty dest" do
      store = MockStore.make()

      assert :ok = HLLCmd.handle("PFMERGE", ["dest", "nokey1", "nokey2"], store)

      count = HLLCmd.handle("PFCOUNT", ["dest"], store)
      assert count == 0
    end

    test "PFMERGE reads destination and sources with batch_get when the store provides it" do
      parent = self()
      {:ok, pid} = Agent.start_link(fn -> %{"src1" => HLL.new(), "src2" => HLL.new()} end)

      store = %{
        batch_get: fn keys ->
          send(parent, {:batch_get, keys})
          Agent.get(pid, fn state -> Enum.map(keys, &Map.get(state, &1)) end)
        end,
        get: fn key ->
          flunk("PFMERGE should use batch_get, got per-key GET for #{inspect(key)}")
        end,
        put: fn key, value, _expire_at_ms ->
          Agent.update(pid, &Map.put(&1, key, value))
          :ok
        end,
        compound_get: fn _redis_key, _compound_key -> nil end
      }

      assert :ok = HLLCmd.handle("PFMERGE", ["dest", "src1", "src2"], store)
      assert_received {:batch_get, ["dest", "src1", "src2"]}
      assert HLL.valid_sketch?(Agent.get(pid, &Map.fetch!(&1, "dest")))
    end

    test "PFMERGE wrong number of args — error (no args)" do
      assert {:error, msg} = HLLCmd.handle("PFMERGE", [], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "PFMERGE wrong number of args — error (only destkey)" do
      assert {:error, msg} = HLLCmd.handle("PFMERGE", ["dest"], MockStore.make())
      assert msg =~ "wrong number of arguments"
    end

    test "PFMERGE with source holding non-HLL value returns WRONGTYPE error" do
      store = MockStore.make(%{"bad" => {"not-a-sketch", 0}})

      assert {:error, msg} = HLLCmd.handle("PFMERGE", ["dest", "bad"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "PFMERGE rejects wrong-size cold source from metadata without batch loading it" do
      store = wrong_size_cold_store(%{"dest" => HLL.num_registers(), "bad" => 1_048_576})

      assert {:error, msg} = HLLCmd.handle("PFMERGE", ["dest", "bad"], store)
      assert msg =~ "WRONGTYPE"
      refute_received {:batch_loaded_cold_hll_candidates, ["dest", "bad"]}
    end

    test "PFMERGE with dest holding non-HLL value returns WRONGTYPE error" do
      store = MockStore.make(%{"dest" => {"not-a-sketch", 0}})

      HLLCmd.handle("PFADD", ["src", "a"], store)

      assert {:error, msg} = HLLCmd.handle("PFMERGE", ["dest", "src"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "PFMERGE with compound destination returns WRONGTYPE and preserves destination" do
      store = MockStore.make()
      Set.handle("SADD", ["dest", "old-member"], store)
      HLLCmd.handle("PFADD", ["src", "a"], store)

      assert {:error, msg} = HLLCmd.handle("PFMERGE", ["dest", "src"], store)
      assert msg =~ "WRONGTYPE"
      assert store.get.("dest") == nil
      assert Set.handle("SMEMBERS", ["dest"], store) == ["old-member"]
    end
  end

  # ---------------------------------------------------------------------------
  # Accuracy test
  # ---------------------------------------------------------------------------

  describe "accuracy" do
    test "PFADD 10,000 unique elements — count within 2%" do
      store = MockStore.make()
      n = 10_000

      # Add in batches to avoid creating a huge argument list
      Enum.chunk_every(1..n, 500)
      |> Enum.each(fn batch ->
        elements = Enum.map(batch, &"item:#{&1}")
        HLLCmd.handle("PFADD", ["bigkey" | elements], store)
      end)

      count = HLLCmd.handle("PFCOUNT", ["bigkey"], store)
      assert_in_delta count, n, n * 0.02
    end
  end

  # ---------------------------------------------------------------------------
  # Dispatcher routing
  # ---------------------------------------------------------------------------

  describe "dispatcher routing" do
    alias Ferricstore.Commands.Dispatcher

    test "dispatches PFADD via dispatcher" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("PFADD", ["k", "v"], store)
    end

    test "dispatches pfadd via dispatcher (case insensitive)" do
      store = MockStore.make()
      assert 1 == Dispatcher.dispatch("pfadd", ["k", "v"], store)
    end

    test "dispatches PFCOUNT via dispatcher" do
      store = MockStore.make()
      assert 0 == Dispatcher.dispatch("PFCOUNT", ["nokey"], store)
    end

    test "dispatches PFMERGE via dispatcher" do
      store = MockStore.make()
      Dispatcher.dispatch("PFADD", ["src", "a"], store)
      assert :ok = Dispatcher.dispatch("PFMERGE", ["dest", "src"], store)
    end
  end

  defp wrong_size_cold_store(sizes) do
    parent = self()

    %{
      value_size: fn key -> Map.get(sizes, key) end,
      get: fn key ->
        send(parent, {:loaded_cold_hll_candidate, key})
        :binary.copy(<<0>>, Map.fetch!(sizes, key))
      end,
      batch_get: fn keys ->
        send(parent, {:batch_loaded_cold_hll_candidates, keys})

        Enum.map(keys, fn key ->
          case Map.get(sizes, key) do
            nil -> nil
            size -> :binary.copy(<<0>>, size)
          end
        end)
      end,
      put: fn _key, _value, _expire_at_ms -> :ok end,
      compound_get: fn _redis_key, _compound_key -> nil end
    }
  end
end
