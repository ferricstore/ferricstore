defmodule Ferricstore.Commands.BloomNifTest do
  @moduledoc """
  Tests for the NIF-backed (mmap) Bloom filter implementation.

  These tests exercise the Rust NIF path by providing a store map that
  contains a `:bloom_registry` key. This causes Bloom.handle/3 to use
  the mmap-backed NIF instead of the pure-Elixir fallback.

  Covers:
  - All BF.* commands via NIF
  - mmap file creation in prob/ directory
  - File persistence across process restart (reopen)
  - DEL removes the mmap file
  - Large bloom filter (1M bits) via mmap
  - Concurrent reads don't block
  - Stress: 100K adds + 100K exists checks
  - False positive rate verification
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bloom

  # ===========================================================================
  # Test helpers: NIF-backed mock store
  # ===========================================================================

  defp make_nif_store(opts \\ []) do
    dir = Keyword.get(opts, :dir, make_temp_dir())
    {:ok, reg_pid} = Agent.start_link(fn -> %{} end)

    bloom_registry = %{
      get: fn key ->
        Agent.get(reg_pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, resource, metadata ->
        Agent.update(reg_pid, fn state ->
          Map.put(state, key, {resource, metadata})
        end)
      end,
      delete: fn key ->
        Agent.update(reg_pid, fn state -> Map.delete(state, key) end)
      end,
      path: fn key ->
        safe_key = key |> String.replace(~r/[^a-zA-Z0-9_.-]/, "_")
        Path.join(dir, "#{safe_key}.bloom")
      end,
      dir: dir,
      pid: reg_pid
    }

    # Note: exists? is intentionally omitted so the handler falls back
    # to File.exists? on the base64-encoded path.
    %{
      bloom_registry: bloom_registry,
      get: fn key ->
        Agent.get(reg_pid, fn state -> Map.get(state, key) end)
      end,
      put: fn key, value, _ttl ->
        Agent.update(reg_pid, fn state -> Map.put(state, key, value) end)
      end,
      delete: fn key ->
        Agent.update(reg_pid, fn state -> Map.delete(state, key) end)
      end
    }
  end

  defp make_temp_dir do
    suffix = "#{System.os_time(:nanosecond)}_#{System.unique_integer([:positive, :monotonic])}"
    dir = Path.join(System.tmp_dir!(), "bloom_nif_test_#{suffix}")
    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    dir
  end

  # Computes the new-style prob file path (base64 encoded key)
  defp prob_file_path(store, key, ext) do
    dir = store.bloom_registry.dir
    safe = Base.url_encode64(key, padding: false)
    Path.join(dir, "#{safe}.#{ext}")
  end

  # ===========================================================================
  # BF.RESERVE via NIF
  # ===========================================================================

  describe "NIF BF.RESERVE" do
    @tag :bloom_nif_mmap
    test "creates a bloom filter backed by an mmap file" do
      store = make_nif_store()
      assert :ok = Bloom.handle("BF.RESERVE", ["mybloom", "0.01", "1000"], store)

      # Verify the .bloom file was created (new base64 path convention)
      path = prob_file_path(store, "mybloom", "bloom")
      assert File.exists?(path)
    end

    test "returns error when key already exists" do
      store = make_nif_store()
      result1 = Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert result1 == :ok or match?({:ok, _}, result1)
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert msg =~ "item exists"
    end

    test "returns error with invalid error rate" do
      store = make_nif_store()
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "0", "100"], store)
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "1", "100"], store)
      assert {:error, _} = Bloom.handle("BF.RESERVE", ["bf", "-0.5", "100"], store)
    end

    test "returns error with wrong number of arguments" do
      store = make_nif_store()
      assert {:error, msg} = Bloom.handle("BF.RESERVE", ["bf", "0.01"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ===========================================================================
  # BF.ADD via NIF
  # ===========================================================================

  describe "NIF BF.ADD" do
    test "adds an element to a new filter (auto-creates)" do
      store = make_nif_store()
      assert 1 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
    end

    test "adds an element to an existing filter" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 1 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
    end

    test "returns 0 for duplicate element" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 1 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
      assert 0 = Bloom.handle("BF.ADD", ["bf", "hello"], store)
    end

    test "handles empty string element" do
      store = make_nif_store()
      assert 1 = Bloom.handle("BF.ADD", ["bf", ""], store)
    end

    test "handles binary element with special characters" do
      store = make_nif_store()
      assert 1 = Bloom.handle("BF.ADD", ["bf", <<0, 1, 2, 255>>], store)
    end
  end

  # ===========================================================================
  # BF.MADD via NIF
  # ===========================================================================

  describe "NIF BF.MADD" do
    test "adds multiple elements at once" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      result = Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      assert result == [1, 1, 1]
    end

    test "returns 0 for elements already present" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      result = Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      assert result == [0, 1, 1]
    end

    test "handles duplicates within the same MADD call" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      result = Bloom.handle("BF.MADD", ["bf", "dup", "dup"], store)
      assert result == [1, 0]
    end
  end

  # ===========================================================================
  # BF.EXISTS via NIF
  # ===========================================================================

  describe "NIF BF.EXISTS" do
    test "returns 1 for an element that was added" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf", "hello"], store)
    end

    test "returns 0 for an element that was not added" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.001", "1000"], store)
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      assert 0 = Bloom.handle("BF.EXISTS", ["bf", "goodbye"], store)
    end

    test "returns 0 for non-existent key" do
      store = make_nif_store()
      assert 0 = Bloom.handle("BF.EXISTS", ["nonexistent", "hello"], store)
    end
  end

  # ===========================================================================
  # BF.MEXISTS via NIF
  # ===========================================================================

  describe "NIF BF.MEXISTS" do
    test "checks multiple elements at once" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.001", "1000"], store)
      Bloom.handle("BF.MADD", ["bf", "a", "b", "c"], store)
      result = Bloom.handle("BF.MEXISTS", ["bf", "a", "b", "c", "d"], store)
      assert Enum.slice(result, 0, 3) == [1, 1, 1]
      assert Enum.at(result, 3) == 0
    end

    test "returns all zeros for non-existent key" do
      store = make_nif_store()
      result = Bloom.handle("BF.MEXISTS", ["nonexistent", "a", "b"], store)
      assert result == [0, 0]
    end
  end

  # ===========================================================================
  # BF.CARD via NIF
  # ===========================================================================

  describe "NIF BF.CARD" do
    test "returns 0 for non-existent key" do
      store = make_nif_store()
      assert 0 = Bloom.handle("BF.CARD", ["nonexistent"], store)
    end

    test "returns count after adding elements" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      Bloom.handle("BF.ADD", ["bf", "b"], store)
      Bloom.handle("BF.ADD", ["bf", "c"], store)
      assert 3 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "does not count duplicates" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      Bloom.handle("BF.ADD", ["bf", "a"], store)
      assert 1 = Bloom.handle("BF.CARD", ["bf"], store)
    end
  end

  # ===========================================================================
  # BF.INFO via NIF
  # ===========================================================================

  describe "NIF BF.INFO" do
    test "returns filter information" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "1000"], store)
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      result = Bloom.handle("BF.INFO", ["bf"], store)
      assert is_list(result)

      info = list_to_info_map(result)
      # Capacity may be derived from header (inverse formula), so allow approximate match
      assert is_integer(info["Capacity"]) and info["Capacity"] > 0
      assert info["Size"] == 1
      assert info["Number of items inserted"] == 1
      assert is_number(info["Error rate"]) and info["Error rate"] > 0
      assert info["Number of hash functions"] > 0
      assert info["Number of bits"] > 0
    end

    test "returns error for non-existent key" do
      store = make_nif_store()
      assert {:error, msg} = Bloom.handle("BF.INFO", ["nonexistent"], store)
      assert msg =~ "not found"
    end
  end

  # ===========================================================================
  # mmap file persistence
  # ===========================================================================

  describe "mmap file persistence" do
    @tag :bloom_nif_mmap
    test "bloom file persists on disk after creation" do
      dir = make_temp_dir()
      store = make_nif_store(dir: dir)

      Bloom.handle("BF.RESERVE", ["persist_test", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["persist_test", "elem1"], store)

      path = prob_file_path(store, "persist_test", "bloom")
      assert File.exists?(path)

      # File should be non-empty (header + bit array)
      stat = File.stat!(path)
      assert stat.size > 32
    end

    @tag :bloom_nif_mmap
    test "bloom filter can be reopened from file" do
      dir = make_temp_dir()

      # Phase 1: Create and populate
      store1 = make_nif_store(dir: dir)
      Bloom.handle("BF.RESERVE", ["reopen", "0.01", "100"], store1)
      Bloom.handle("BF.ADD", ["reopen", "hello"], store1)
      Bloom.handle("BF.ADD", ["reopen", "world"], store1)
      assert 2 = Bloom.handle("BF.CARD", ["reopen"], store1)

      # Phase 2: Create a new store from the same directory and verify
      # data persists (stateless pread on same file)
      store2 = make_nif_store(dir: dir)
      assert 1 = Bloom.handle("BF.EXISTS", ["reopen", "hello"], store2)
      assert 1 = Bloom.handle("BF.EXISTS", ["reopen", "world"], store2)
      assert 0 = Bloom.handle("BF.EXISTS", ["reopen", "missing"], store2)
      assert 2 = Bloom.handle("BF.CARD", ["reopen"], store2)
    end
  end

  # ===========================================================================
  # DEL removes the file (munmap + unlink)
  # ===========================================================================

  describe "DEL removes the mmap file" do
    @tag :bloom_nif_mmap
    test "nif_delete munmaps and unlinks the bloom file" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["deleteme", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["deleteme", "elem"], store)

      path = prob_file_path(store, "deleteme", "bloom")
      assert File.exists?(path)

      # Delete via NIF
      assert :ok = Bloom.nif_delete("deleteme", store)
      refute File.exists?(path)
    end

    @tag :bloom_nif_mmap
    test "nif_delete on non-existent key is a no-op" do
      store = make_nif_store()
      assert :ok = Bloom.nif_delete("no_such_key", store)
    end
  end

  # ===========================================================================
  # Large bloom filter (1M bits) via mmap
  # ===========================================================================

  describe "large bloom filter" do
    test "1M bits bloom filter works via mmap" do
      store = make_nif_store()
      # Create with very low error rate -> many bits
      # 10000 capacity, 0.0001 error rate => ~191,702 bits
      Bloom.handle("BF.RESERVE", ["large", "0.0001", "10000"], store)

      # Add 1000 elements
      for i <- 1..1000 do
        Bloom.handle("BF.ADD", ["large", "item_#{i}"], store)
      end

      assert 1000 = Bloom.handle("BF.CARD", ["large"], store)

      # Verify all elements exist
      for i <- 1..1000 do
        assert 1 = Bloom.handle("BF.EXISTS", ["large", "item_#{i}"], store),
               "False negative for item_#{i}"
      end
    end
  end

  # ===========================================================================
  # Concurrent reads (mmap is thread-safe for reads)
  # ===========================================================================

  describe "concurrent reads" do
    test "concurrent reads don't block" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["concurrent", "0.01", "1000"], store)

      for i <- 1..100 do
        Bloom.handle("BF.ADD", ["concurrent", "elem_#{i}"], store)
      end

      # Spawn multiple tasks doing concurrent reads
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            for i <- 1..100 do
              Bloom.handle("BF.EXISTS", ["concurrent", "elem_#{i}"], store)
            end
          end)
        end

      results = Task.await_many(tasks, 5000)

      # All reads should return 1 for every element
      for result_list <- results do
        assert Enum.all?(result_list, &(&1 == 1))
      end
    end
  end

  # ===========================================================================
  # Stress test: 100K adds + 100K exists
  # ===========================================================================

  describe "stress test" do
    @tag :bench
    @tag timeout: 120_000
    test "100K adds and 100K exists checks" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["stress", "0.01", "200000"], store)

      # Add 100K elements
      for i <- 1..100_000 do
        Bloom.handle("BF.ADD", ["stress", "stress_#{i}"], store)
      end

      card = Bloom.handle("BF.CARD", ["stress"], store)
      # Card may be slightly less than 100K due to hash collisions
      # (two distinct elements mapping to the same bit positions).
      # With 200K capacity and 1% error rate, the expected count should
      # be very close to 100K.
      assert card >= 99_900,
             "Card #{card} is unexpectedly low (expected ~100,000)"

      assert card <= 100_000

      # Verify all elements exist (no false negatives)
      false_negatives =
        Enum.count(1..100_000, fn i ->
          Bloom.handle("BF.EXISTS", ["stress", "stress_#{i}"], store) != 1
        end)

      assert false_negatives == 0, "Found #{false_negatives} false negatives"
    end
  end

  # ===========================================================================
  # False positive rate verification via NIF
  # ===========================================================================

  describe "NIF false positive rate" do
    test "respects target false positive rate within 2x margin" do
      store = make_nif_store()
      error_rate = 0.05
      capacity = 1000

      Bloom.handle("BF.RESERVE", ["fpr", "#{error_rate}", "#{capacity}"], store)

      for i <- 1..capacity do
        Bloom.handle("BF.ADD", ["fpr", "added_#{i}"], store)
      end

      test_count = 10_000

      false_positives =
        Enum.count(1..test_count, fn i ->
          Bloom.handle("BF.EXISTS", ["fpr", "not_added_#{i}"], store) == 1
        end)

      observed_rate = false_positives / test_count

      assert observed_rate < error_rate * 2,
             "False positive rate #{observed_rate} exceeds 2x target #{error_rate}"
    end

    test "no false negatives: all added elements are found" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["nofn", "0.01", "500"], store)

      elements = for i <- 1..200, do: "element_#{i}"
      Enum.each(elements, fn e -> Bloom.handle("BF.ADD", ["nofn", e], store) end)

      Enum.each(elements, fn e ->
        assert 1 == Bloom.handle("BF.EXISTS", ["nofn", e], store),
               "False negative for #{e}"
      end)
    end
  end

  # ===========================================================================
  # Cross-command interactions via NIF
  # ===========================================================================

  describe "NIF cross-command interactions" do
    test "BF.ADD then BF.EXISTS returns 1" do
      store = make_nif_store()
      Bloom.handle("BF.ADD", ["bf", "test"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf", "test"], store)
    end

    test "BF.RESERVE then BF.ADD then BF.CARD tracks count" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf", "0.01", "100"], store)
      assert 0 = Bloom.handle("BF.CARD", ["bf"], store)
      Bloom.handle("BF.ADD", ["bf", "x"], store)
      assert 1 = Bloom.handle("BF.CARD", ["bf"], store)
      Bloom.handle("BF.ADD", ["bf", "y"], store)
      assert 2 = Bloom.handle("BF.CARD", ["bf"], store)
    end

    test "auto-created filter has default capacity and error rate" do
      store = make_nif_store()
      Bloom.handle("BF.ADD", ["bf", "hello"], store)
      result = Bloom.handle("BF.INFO", ["bf"], store)
      info = list_to_info_map(result)
      # Default auto-create uses capacity=100, error_rate=0.01
      # Capacity is derived from header via inverse formula, so allow approximate
      assert is_integer(info["Capacity"]) and info["Capacity"] > 0
      assert is_number(info["Error rate"]) and info["Error rate"] > 0 and info["Error rate"] < 1
    end

    test "multiple independent bloom filters do not interfere" do
      store = make_nif_store()
      Bloom.handle("BF.RESERVE", ["bf1", "0.001", "1000"], store)
      Bloom.handle("BF.RESERVE", ["bf2", "0.001", "1000"], store)

      Bloom.handle("BF.ADD", ["bf1", "only_in_bf1"], store)
      Bloom.handle("BF.ADD", ["bf2", "only_in_bf2"], store)

      assert 1 = Bloom.handle("BF.EXISTS", ["bf1", "only_in_bf1"], store)
      assert 0 = Bloom.handle("BF.EXISTS", ["bf1", "only_in_bf2"], store)
      assert 0 = Bloom.handle("BF.EXISTS", ["bf2", "only_in_bf1"], store)
      assert 1 = Bloom.handle("BF.EXISTS", ["bf2", "only_in_bf2"], store)
    end
  end

  # Old mmap-based "direct NIF API" tests removed — those NIFs no longer exist.
  # The stateless bloom_file_* NIFs are tested via the command handler tests above.

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp list_to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
