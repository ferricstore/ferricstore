defmodule Ferricstore.ProbAsyncEdgeCasesTest do
  @moduledoc """
  Edge case tests for async probabilistic read NIFs.

  Verifies correct behavior under stress: no memory leaks, proper timeout
  handling, concurrent reads, deleted files, null-byte elements, interleaved
  cross-type reads, large responses, and mailbox cleanup.

  All async NIFs use the Tokio spawn_blocking + OwnedEnv pattern and
  communicate results via {:tokio_complete, corr_id, :ok/:error, payload}.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    # Clean prob dirs across all shards
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    for i <- 0..(shard_count - 1) do
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)
      prob_dir = Path.join(shard_path, "prob")

      case File.ls(prob_dir) do
        {:ok, files} -> Enum.each(files, &File.rm(Path.join(prob_dir, &1)))
        _ -> :ok
      end
    end

    :ok
  end

  # ===========================================================================
  # Test helpers
  # ===========================================================================

  defp make_prob_dir(prefix) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "prob_async_#{prefix}_#{:os.getpid()}_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    dir
  end

  defp make_bloom_store do
    dir = make_prob_dir("bloom")

    %{
      bloom_registry: %{dir: dir},
      prob_dir: fn -> dir end,
      get: fn _key -> nil end,
      put: fn _key, _value, _ttl -> :ok end,
      delete: fn _key -> :ok end
    }
  end

  defp make_cms_store do
    dir = make_prob_dir("cms")

    %{
      cms_registry: %{dir: dir},
      prob_dir: fn -> dir end,
      get: fn _key -> nil end
    }
  end

  defp make_cuckoo_store do
    dir = make_prob_dir("cuckoo")

    %{
      cuckoo_registry: %{dir: dir},
      prob_dir: fn -> dir end
    }
  end

  defp make_topk_store do
    dir = make_prob_dir("topk")
    {:ok, pid} = Agent.start_link(fn -> %{} end)

    %{
      prob_dir: fn -> dir end,
      exists?: fn key -> Agent.get(pid, &Map.has_key?(&1, key)) end,
      put: fn key, value, ttl ->
        Agent.update(pid, &Map.put(&1, key, {value, ttl}))
        :ok
      end
    }
  end

  defp prob_file_path(dir, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    Path.join(dir, "#{safe}.#{ext}")
  end

  describe "cuckoo file write edge cases" do
    test "failed full insert does not mutate existing filter bytes" do
      dir = make_prob_dir("cuckoo_full")
      path = prob_file_path(dir, "full_filter", "cuckoo")

      on_exit(fn -> File.rm_rf(dir) end)

      assert {:ok, :ok} = NIF.cuckoo_file_create(path, 1, 2)
      assert {:ok, 1} = NIF.cuckoo_file_add(path, "seed_1")
      assert {:ok, 1} = NIF.cuckoo_file_add(path, "seed_2")

      before = File.read!(path)

      assert {:error, "filter is full"} = NIF.cuckoo_file_add(path, "probe")
      assert File.read!(path) == before
    end
  end

  # ===========================================================================
  # 1. Memory leak detection
  # ===========================================================================

  describe "memory leak detection" do
    @tag timeout: 120_000
    test "creating and reading 1000 bloom filters does not leak memory" do
      store = make_bloom_store()
      dir = store.prob_dir.()

      # Warm up: create some structures first to stabilize allocator
      for i <- 0..9 do
        key = "warmup_#{i}"
        Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)
        Bloom.handle("BF.ADD", [key, "elem"], store)
        Bloom.handle("BF.EXISTS", [key, "elem"], store)
      end

      :erlang.garbage_collect()
      Process.sleep(100)
      mem_before = :erlang.memory(:total)

      for round <- 0..9 do
        for i <- 0..99 do
          key = "leak_#{round}_#{i}"
          Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)

          for j <- 0..9 do
            Bloom.handle("BF.ADD", [key, "elem_#{j}"], store)
          end

          # Read back via async NIF
          for j <- 0..9 do
            Bloom.handle("BF.EXISTS", [key, "elem_#{j}"], store)
          end

          Bloom.handle("BF.CARD", [key], store)
          Bloom.handle("BF.INFO", [key], store)
        end

        :erlang.garbage_collect()
      end

      :erlang.garbage_collect()
      Process.sleep(100)
      mem_after = :erlang.memory(:total)

      # Allow up to 50MB growth (generous to avoid flaky tests on CI).
      # A real leak would grow unboundedly.
      growth = mem_after - mem_before
      assert growth < 50_000_000, "Memory grew by #{div(growth, 1_000_000)}MB — possible leak"

      # Cleanup
      case File.ls(dir) do
        {:ok, files} -> Enum.each(files, &File.rm(Path.join(dir, &1)))
        _ -> :ok
      end
    end
  end

  # ===========================================================================
  # 2. Timeout / error handling
  # ===========================================================================

  describe "timeout and error handling" do
    test "BF.EXISTS on non-existent path returns 0 (not hang)" do
      store = make_bloom_store()
      # Key has no backing file — async NIF should return enoent quickly
      result = Bloom.handle("BF.EXISTS", ["no_such_bloom", "x"], store)
      assert result == 0
    end

    test "BF.CARD on non-existent path returns 0" do
      store = make_bloom_store()
      result = Bloom.handle("BF.CARD", ["no_such_bloom"], store)
      assert result == 0
    end

    test "BF.INFO on non-existent path returns error" do
      store = make_bloom_store()
      assert {:error, "ERR not found"} = Bloom.handle("BF.INFO", ["no_such_bloom"], store)
    end

    test "CMS.QUERY on non-existent path returns error" do
      store = make_cms_store()
      assert {:error, msg} = CMS.handle("CMS.QUERY", ["no_such_cms", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "CF.EXISTS on non-existent path returns 0" do
      store = make_cuckoo_store()
      result = Cuckoo.handle("CF.EXISTS", ["no_such_cuckoo", "x"], store)
      assert result == 0
    end

    test "TOPK.QUERY on non-existent path returns error" do
      store = make_topk_store()
      assert {:error, msg} = TopK.handle("TOPK.QUERY", ["no_such_topk", "elem"], store)
      assert msg =~ "does not exist"
    end

    test "direct async NIF on non-existent file sends error to caller" do
      corr_id = System.unique_integer([:positive, :monotonic])

      :ok =
        NIF.bloom_file_exists_async(self(), corr_id, "/tmp/nonexistent_async_bloom.bloom", "x")

      assert_receive {:tokio_complete, ^corr_id, :error, "enoent"}, 5000
    end

    test "direct CMS async NIF on non-existent file sends error" do
      corr_id = System.unique_integer([:positive, :monotonic])
      :ok = NIF.cms_file_query_async(self(), corr_id, "/tmp/nonexistent_async_cms.cms", ["x"])

      assert_receive {:tokio_complete, ^corr_id, :error, "enoent"}, 5000
    end

    test "direct cuckoo async NIF on non-existent file sends error" do
      corr_id = System.unique_integer([:positive, :monotonic])

      :ok =
        NIF.cuckoo_file_exists_async(
          self(),
          corr_id,
          "/tmp/nonexistent_async_cuckoo.cuckoo",
          "x"
        )

      assert_receive {:tokio_complete, ^corr_id, :error, "enoent"}, 5000
    end

    test "direct topk async NIF on non-existent file sends error" do
      corr_id = System.unique_integer([:positive, :monotonic])

      :ok =
        NIF.topk_file_query_v2_async(
          self(),
          corr_id,
          "/tmp/nonexistent_async_topk.topk",
          ["x"]
        )

      assert_receive {:tokio_complete, ^corr_id, :error, "enoent"}, 5000
    end
  end

  # ===========================================================================
  # 3. Concurrent async reads
  # ===========================================================================

  describe "concurrent async reads" do
    test "100 concurrent BF.EXISTS on same bloom filter" do
      store = make_bloom_store()
      key = "concurrent_bloom"

      Bloom.handle("BF.RESERVE", [key, "0.001", "10000"], store)

      for i <- 0..49 do
        Bloom.handle("BF.ADD", [key, "elem_#{i}"], store)
      end

      tasks =
        for i <- 0..99 do
          Task.async(fn ->
            elem = "elem_#{rem(i, 50)}"
            Bloom.handle("BF.EXISTS", [key, elem], store)
          end)
        end

      results = Task.await_many(tasks, 30_000)

      assert length(results) == 100
      # All should return 1 (element exists)
      assert Enum.all?(results, &(&1 == 1)),
             "Some concurrent BF.EXISTS returned wrong result: #{inspect(Enum.uniq(results))}"
    end

    test "100 concurrent CMS.QUERY on same CMS" do
      store = make_cms_store()
      key = "concurrent_cms"

      CMS.handle("CMS.INITBYDIM", [key, "200", "7"], store)
      CMS.handle("CMS.INCRBY", [key, "elem", "42"], store)

      tasks =
        for _i <- 0..99 do
          Task.async(fn ->
            CMS.handle("CMS.QUERY", [key, "elem"], store)
          end)
        end

      results = Task.await_many(tasks, 30_000)

      assert length(results) == 100

      assert Enum.all?(results, fn
               [count] when is_integer(count) and count >= 42 -> true
               _ -> false
             end),
             "Some concurrent CMS.QUERY returned unexpected: #{inspect(Enum.frequencies(results))}"
    end

    test "100 concurrent CF.EXISTS on same cuckoo filter" do
      store = make_cuckoo_store()
      key = "concurrent_cuckoo"

      Cuckoo.handle("CF.RESERVE", [key, "2048"], store)
      Cuckoo.handle("CF.ADD", [key, "target"], store)

      tasks =
        for _i <- 0..99 do
          Task.async(fn ->
            Cuckoo.handle("CF.EXISTS", [key, "target"], store)
          end)
        end

      results = Task.await_many(tasks, 30_000)

      assert length(results) == 100
      assert Enum.all?(results, &(&1 == 1))
    end

    test "100 concurrent TOPK.QUERY on same TopK" do
      store = make_topk_store()
      key = "concurrent_topk"

      TopK.handle("TOPK.RESERVE", [key, "10"], store)

      for i <- 0..9 do
        TopK.handle("TOPK.ADD", [key, "elem_#{i}"], store)
      end

      tasks =
        for _i <- 0..99 do
          Task.async(fn ->
            TopK.handle("TOPK.QUERY", [key, "elem_0"], store)
          end)
        end

      results = Task.await_many(tasks, 30_000)

      assert length(results) == 100
      assert Enum.all?(results, &(&1 == [1]))
    end
  end

  # ===========================================================================
  # 4. Rapid sequential reads
  # ===========================================================================

  describe "rapid sequential reads" do
    test "10K BF.EXISTS calls on the same key — correct correlation_id routing" do
      dir = make_prob_dir("rapid_bloom")
      path = Path.join(dir, "rapid.bloom")
      {:ok, :ok} = NIF.bloom_file_create(path, 10_000, 7)
      {:ok, 1} = NIF.bloom_file_add(path, "target")

      for i <- 1..10_000 do
        corr_id = i
        :ok = NIF.bloom_file_exists_async(self(), corr_id, path, "target")

        receive do
          {:tokio_complete, ^corr_id, :ok, 1} ->
            :ok

          {:tokio_complete, ^corr_id, status, val} ->
            flunk("Iteration #{i}: expected {:ok, 1}, got {#{inspect(status)}, #{inspect(val)}}")
        after
          5000 -> flunk("Iteration #{i}: timeout waiting for async response")
        end
      end
    end

    test "10K sequential CMS.QUERY via command handler" do
      store = make_cms_store()
      key = "rapid_cms"
      CMS.handle("CMS.INITBYDIM", [key, "100", "5"], store)
      CMS.handle("CMS.INCRBY", [key, "elem", "7"], store)

      for _i <- 1..10_000 do
        result = CMS.handle("CMS.QUERY", [key, "elem"], store)
        assert [count] = result
        assert count >= 7
      end
    end
  end

  # ===========================================================================
  # 5. Async read on deleted file
  # ===========================================================================

  describe "async read on deleted file" do
    test "BF.EXISTS after file deletion returns 0" do
      store = make_bloom_store()
      key = "deleted_bloom"
      dir = store.prob_dir.()

      Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)
      Bloom.handle("BF.ADD", [key, "elem"], store)
      assert 1 == Bloom.handle("BF.EXISTS", [key, "elem"], store)

      # Delete the underlying file
      path = prob_file_path(dir, key, "bloom")
      assert File.exists?(path)
      File.rm!(path)
      refute File.exists?(path)

      # Should return 0 (enoent mapped to 0), not crash
      result = Bloom.handle("BF.EXISTS", [key, "elem"], store)
      assert result == 0
    end

    test "CMS.QUERY after file deletion returns error" do
      store = make_cms_store()
      key = "deleted_cms"
      dir = store.prob_dir.()

      CMS.handle("CMS.INITBYDIM", [key, "100", "5"], store)
      CMS.handle("CMS.INCRBY", [key, "elem", "5"], store)

      path = prob_file_path(dir, key, "cms")
      File.rm!(path)

      result = CMS.handle("CMS.QUERY", [key, "elem"], store)
      assert {:error, msg} = result
      assert msg =~ "does not exist"
    end

    test "CF.EXISTS after file deletion returns 0" do
      store = make_cuckoo_store()
      key = "deleted_cuckoo"
      dir = store.prob_dir.()

      Cuckoo.handle("CF.RESERVE", [key, "1024"], store)
      Cuckoo.handle("CF.ADD", [key, "elem"], store)
      assert 1 == Cuckoo.handle("CF.EXISTS", [key, "elem"], store)

      path = prob_file_path(dir, key, "cuckoo")
      File.rm!(path)

      result = Cuckoo.handle("CF.EXISTS", [key, "elem"], store)
      assert result == 0
    end

    test "TOPK.QUERY after file deletion returns error" do
      store = make_topk_store()
      key = "deleted_topk"
      dir = store.prob_dir.()

      TopK.handle("TOPK.RESERVE", [key, "5"], store)
      TopK.handle("TOPK.ADD", [key, "elem"], store)

      path = prob_file_path(dir, key, "topk")
      File.rm!(path)

      result = TopK.handle("TOPK.QUERY", [key, "elem"], store)
      assert {:error, msg} = result
      assert msg =~ "does not exist"
    end
  end

  # ===========================================================================
  # 6. Async read with empty/null-byte elements
  # ===========================================================================

  describe "async read with empty and null-byte elements" do
    test "BF.EXISTS with empty string element" do
      store = make_bloom_store()
      key = "empty_elem_bloom"
      Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)
      Bloom.handle("BF.ADD", [key, ""], store)

      result = Bloom.handle("BF.EXISTS", [key, ""], store)
      assert result == 1
    end

    test "BF.EXISTS with null-byte element" do
      store = make_bloom_store()
      key = "null_elem_bloom"
      element = <<0, 1, 0>>
      Bloom.handle("BF.RESERVE", [key, "0.01", "100"], store)
      Bloom.handle("BF.ADD", [key, element], store)

      result = Bloom.handle("BF.EXISTS", [key, element], store)
      assert result == 1
    end

    test "BF.MEXISTS with mixed empty and null-byte elements" do
      store = make_bloom_store()
      key = "mixed_elem_bloom"
      Bloom.handle("BF.RESERVE", [key, "0.01", "1000"], store)
      Bloom.handle("BF.ADD", [key, ""], store)
      Bloom.handle("BF.ADD", [key, <<0, 1, 0>>], store)
      Bloom.handle("BF.ADD", [key, "normal"], store)

      result = Bloom.handle("BF.MEXISTS", [key, "", <<0, 1, 0>>, "normal", "not_there"], store)
      assert [1, 1, 1, 0] = result
    end

    test "CMS.QUERY with empty element" do
      store = make_cms_store()
      key = "empty_elem_cms"
      CMS.handle("CMS.INITBYDIM", [key, "100", "5"], store)
      CMS.handle("CMS.INCRBY", [key, "", "3"], store)

      result = CMS.handle("CMS.QUERY", [key, ""], store)
      assert [3] = result
    end

    test "CF.EXISTS with empty element" do
      store = make_cuckoo_store()
      key = "empty_elem_cf"
      Cuckoo.handle("CF.RESERVE", [key, "1024"], store)
      Cuckoo.handle("CF.ADD", [key, ""], store)

      result = Cuckoo.handle("CF.EXISTS", [key, ""], store)
      assert result == 1
    end

    test "CF.EXISTS with null-byte element" do
      store = make_cuckoo_store()
      key = "null_elem_cf"
      element = <<0, 1, 0>>
      Cuckoo.handle("CF.RESERVE", [key, "1024"], store)
      Cuckoo.handle("CF.ADD", [key, element], store)

      result = Cuckoo.handle("CF.EXISTS", [key, element], store)
      assert result == 1
    end

    test "direct async NIF with empty binary" do
      dir = make_prob_dir("empty_bin")
      path = Path.join(dir, "test.bloom")
      {:ok, :ok} = NIF.bloom_file_create(path, 1000, 7)
      {:ok, _} = NIF.bloom_file_add(path, "")

      corr_id = System.unique_integer([:positive, :monotonic])
      :ok = NIF.bloom_file_exists_async(self(), corr_id, path, "")

      assert_receive {:tokio_complete, ^corr_id, :ok, 1}, 5000
    end

    test "direct async NIF with null bytes" do
      dir = make_prob_dir("null_bin")
      path = Path.join(dir, "test.bloom")
      {:ok, :ok} = NIF.bloom_file_create(path, 1000, 7)
      {:ok, _} = NIF.bloom_file_add(path, <<0, 1, 0>>)

      corr_id = System.unique_integer([:positive, :monotonic])
      :ok = NIF.bloom_file_exists_async(self(), corr_id, path, <<0, 1, 0>>)

      assert_receive {:tokio_complete, ^corr_id, :ok, 1}, 5000
    end
  end

  # ===========================================================================
  # 7. Interleaved async reads across types
  # ===========================================================================

  describe "interleaved async reads across types" do
    test "concurrent BF.EXISTS + CMS.QUERY + CF.EXISTS + TOPK.QUERY" do
      bloom_store = make_bloom_store()
      cms_store = make_cms_store()
      cuckoo_store = make_cuckoo_store()
      topk_store = make_topk_store()

      # Set up structures
      Bloom.handle("BF.RESERVE", ["interleave_bf", "0.01", "1000"], bloom_store)
      Bloom.handle("BF.ADD", ["interleave_bf", "bf_elem"], bloom_store)

      CMS.handle("CMS.INITBYDIM", ["interleave_cms", "100", "5"], cms_store)
      CMS.handle("CMS.INCRBY", ["interleave_cms", "cms_elem", "10"], cms_store)

      Cuckoo.handle("CF.RESERVE", ["interleave_cf", "1024"], cuckoo_store)
      Cuckoo.handle("CF.ADD", ["interleave_cf", "cf_elem"], cuckoo_store)

      TopK.handle("TOPK.RESERVE", ["interleave_tk", "5"], topk_store)
      TopK.handle("TOPK.ADD", ["interleave_tk", "tk_elem"], topk_store)

      # Fire all 4 concurrently
      tasks = [
        Task.async(fn ->
          for _ <- 1..50 do
            Bloom.handle("BF.EXISTS", ["interleave_bf", "bf_elem"], bloom_store)
          end
        end),
        Task.async(fn ->
          for _ <- 1..50 do
            CMS.handle("CMS.QUERY", ["interleave_cms", "cms_elem"], cms_store)
          end
        end),
        Task.async(fn ->
          for _ <- 1..50 do
            Cuckoo.handle("CF.EXISTS", ["interleave_cf", "cf_elem"], cuckoo_store)
          end
        end),
        Task.async(fn ->
          for _ <- 1..50 do
            TopK.handle("TOPK.QUERY", ["interleave_tk", "tk_elem"], topk_store)
          end
        end)
      ]

      [bf_results, cms_results, cf_results, tk_results] = Task.await_many(tasks, 30_000)

      # Verify bloom results
      assert length(bf_results) == 50
      assert Enum.all?(bf_results, &(&1 == 1)), "BF results cross-contaminated"

      # Verify CMS results
      assert length(cms_results) == 50

      assert Enum.all?(cms_results, fn
               [c] when c >= 10 -> true
               _ -> false
             end),
             "CMS results cross-contaminated"

      # Verify cuckoo results
      assert length(cf_results) == 50
      assert Enum.all?(cf_results, &(&1 == 1)), "CF results cross-contaminated"

      # Verify topk results
      assert length(tk_results) == 50
      assert Enum.all?(tk_results, &(&1 == [1])), "TopK results cross-contaminated"
    end

    test "interleaved async reads with non-existent keys mixed in" do
      bloom_store = make_bloom_store()
      cms_store = make_cms_store()

      Bloom.handle("BF.RESERVE", ["mixed_bf", "0.01", "100"], bloom_store)
      Bloom.handle("BF.ADD", ["mixed_bf", "exists"], bloom_store)

      CMS.handle("CMS.INITBYDIM", ["mixed_cms", "100", "5"], cms_store)
      CMS.handle("CMS.INCRBY", ["mixed_cms", "exists", "5"], cms_store)

      tasks =
        for i <- 0..99 do
          Task.async(fn ->
            if rem(i, 4) == 0 do
              # Existing bloom key
              {:bf, Bloom.handle("BF.EXISTS", ["mixed_bf", "exists"], bloom_store)}
            else
              if rem(i, 4) == 1 do
                # Non-existent bloom key
                {:bf_miss, Bloom.handle("BF.EXISTS", ["no_bf", "x"], bloom_store)}
              else
                if rem(i, 4) == 2 do
                  # Existing CMS key
                  {:cms, CMS.handle("CMS.QUERY", ["mixed_cms", "exists"], cms_store)}
                else
                  # Non-existent CMS key
                  {:cms_miss, CMS.handle("CMS.QUERY", ["no_cms", "x"], cms_store)}
                end
              end
            end
          end)
        end

      results = Task.await_many(tasks, 30_000)

      for result <- results do
        case result do
          {:bf, val} -> assert val == 1
          {:bf_miss, val} -> assert val == 0
          {:cms, [val]} -> assert val >= 5
          {:cms_miss, {:error, _}} -> :ok
        end
      end
    end
  end

  # ===========================================================================
  # 8. Large response handling
  # ===========================================================================

  describe "large response handling" do
    test "TOPK.LIST with k=100 after adding 100 elements" do
      store = make_topk_store()
      key = "large_topk"

      TopK.handle("TOPK.RESERVE", [key, "100", "32", "7", "0.9"], store)

      for i <- 0..99 do
        TopK.handle("TOPK.ADD", [key, "elem_#{i}"], store)
      end

      result = TopK.handle("TOPK.LIST", [key], store)
      assert is_list(result)
      assert length(result) == 100
    end

    test "TOPK.LIST WITHCOUNT with k=100" do
      store = make_topk_store()
      key = "large_topk_wc"

      TopK.handle("TOPK.RESERVE", [key, "100", "32", "7", "0.9"], store)

      for i <- 0..99 do
        # Add multiple times so each has count > 0
        for _ <- 1..3 do
          TopK.handle("TOPK.ADD", [key, "elem_#{i}"], store)
        end
      end

      result = TopK.handle("TOPK.LIST", [key, "WITHCOUNT"], store)
      assert is_list(result)
      # WITHCOUNT returns [elem, count, elem, count, ...] = 200 entries
      assert length(result) == 200
    end

    test "CMS.QUERY with 100 elements in a single call" do
      store = make_cms_store()
      key = "large_cms"
      CMS.handle("CMS.INITBYDIM", [key, "200", "7"], store)

      # Increment 100 elements
      for i <- 0..99 do
        CMS.handle("CMS.INCRBY", [key, "elem_#{i}", "#{i + 1}"], store)
      end

      elements = for i <- 0..99, do: "elem_#{i}"
      result = CMS.handle("CMS.QUERY", [key | elements], store)

      assert is_list(result)
      assert length(result) == 100

      # Each count should be >= expected (CMS can overcount but not undercount)
      Enum.with_index(result, fn count, i ->
        assert count >= i + 1,
               "CMS.QUERY elem_#{i}: expected >= #{i + 1}, got #{count}"
      end)
    end

    test "BF.MEXISTS with 100 elements" do
      store = make_bloom_store()
      key = "large_bloom"
      Bloom.handle("BF.RESERVE", [key, "0.001", "10000"], store)

      # Add 50 elements
      for i <- 0..49 do
        Bloom.handle("BF.ADD", [key, "elem_#{i}"], store)
      end

      # Check 100 elements (50 present, 50 absent)
      elements = for i <- 0..99, do: "elem_#{i}"
      result = Bloom.handle("BF.MEXISTS", [key | elements], store)

      assert is_list(result)
      assert length(result) == 100

      # First 50 should be 1 (present)
      assert Enum.all?(Enum.take(result, 50), &(&1 == 1))

      # Last 50 should be mostly 0 (with very low false positive rate at 0.001)
      absent_results = Enum.drop(result, 50)
      false_positives = Enum.count(absent_results, &(&1 == 1))

      assert false_positives < 5,
             "Too many false positives: #{false_positives}/50 at 0.001 error rate"
    end
  end

  # ===========================================================================
  # 9. Process mailbox cleanup
  # ===========================================================================

  describe "process mailbox cleanup" do
    test "no stale tokio_complete messages after BF.EXISTS" do
      store = make_bloom_store()
      Bloom.handle("BF.RESERVE", ["mailbox_bf", "0.01", "100"], store)
      Bloom.handle("BF.ADD", ["mailbox_bf", "elem"], store)

      # Do multiple async reads
      for _ <- 1..100 do
        Bloom.handle("BF.EXISTS", ["mailbox_bf", "elem"], store)
      end

      # Verify mailbox is clean
      {:message_queue_len, len} = Process.info(self(), :message_queue_len)
      assert len == 0, "Stale messages in mailbox: #{len}"
    end

    test "no stale tokio_complete messages after CMS.QUERY" do
      store = make_cms_store()
      CMS.handle("CMS.INITBYDIM", ["mailbox_cms", "100", "5"], store)
      CMS.handle("CMS.INCRBY", ["mailbox_cms", "elem", "5"], store)

      for _ <- 1..100 do
        CMS.handle("CMS.QUERY", ["mailbox_cms", "elem"], store)
      end

      {:message_queue_len, len} = Process.info(self(), :message_queue_len)
      assert len == 0, "Stale messages in mailbox: #{len}"
    end

    test "no stale tokio_complete messages after CF.EXISTS" do
      store = make_cuckoo_store()
      Cuckoo.handle("CF.RESERVE", ["mailbox_cf", "1024"], store)
      Cuckoo.handle("CF.ADD", ["mailbox_cf", "elem"], store)

      for _ <- 1..100 do
        Cuckoo.handle("CF.EXISTS", ["mailbox_cf", "elem"], store)
      end

      {:message_queue_len, len} = Process.info(self(), :message_queue_len)
      assert len == 0, "Stale messages in mailbox: #{len}"
    end

    test "no stale tokio_complete messages after TOPK.QUERY" do
      store = make_topk_store()
      TopK.handle("TOPK.RESERVE", ["mailbox_tk", "5"], store)
      TopK.handle("TOPK.ADD", ["mailbox_tk", "elem"], store)

      for _ <- 1..100 do
        TopK.handle("TOPK.QUERY", ["mailbox_tk", "elem"], store)
      end

      {:message_queue_len, len} = Process.info(self(), :message_queue_len)
      assert len == 0, "Stale messages in mailbox: #{len}"
    end

    test "no stale messages after mixed async operations" do
      bloom_store = make_bloom_store()
      cms_store = make_cms_store()
      cuckoo_store = make_cuckoo_store()
      topk_store = make_topk_store()

      Bloom.handle("BF.RESERVE", ["mix_bf", "0.01", "100"], bloom_store)
      Bloom.handle("BF.ADD", ["mix_bf", "e"], bloom_store)

      CMS.handle("CMS.INITBYDIM", ["mix_cms", "100", "5"], cms_store)
      CMS.handle("CMS.INCRBY", ["mix_cms", "e", "1"], cms_store)

      Cuckoo.handle("CF.RESERVE", ["mix_cf", "1024"], cuckoo_store)
      Cuckoo.handle("CF.ADD", ["mix_cf", "e"], cuckoo_store)

      TopK.handle("TOPK.RESERVE", ["mix_tk", "5"], topk_store)
      TopK.handle("TOPK.ADD", ["mix_tk", "e"], topk_store)

      for _ <- 1..50 do
        Bloom.handle("BF.EXISTS", ["mix_bf", "e"], bloom_store)
        CMS.handle("CMS.QUERY", ["mix_cms", "e"], cms_store)
        Cuckoo.handle("CF.EXISTS", ["mix_cf", "e"], cuckoo_store)
        TopK.handle("TOPK.QUERY", ["mix_tk", "e"], topk_store)
      end

      {:message_queue_len, len} = Process.info(self(), :message_queue_len)
      assert len == 0, "Stale messages after mixed operations: #{len}"
    end
  end

  # ===========================================================================
  # 10. Direct async NIF correlation_id correctness
  # ===========================================================================

  describe "correlation_id correctness" do
    test "multiple in-flight async NIFs return to correct correlation_ids" do
      dir = make_prob_dir("corr_id")
      path = Path.join(dir, "test.bloom")
      {:ok, :ok} = NIF.bloom_file_create(path, 10_000, 7)
      {:ok, 1} = NIF.bloom_file_add(path, "present")

      # Fire 100 async requests without waiting
      corr_ids =
        for i <- 1..100 do
          corr_id = i * 1_000_000 + :erlang.unique_integer([:positive])

          elem =
            if rem(i, 2) == 0, do: "present", else: "absent_#{i}"

          :ok = NIF.bloom_file_exists_async(self(), corr_id, path, elem)
          {corr_id, rem(i, 2) == 0}
        end

      # Collect all responses and verify correlation_id matching
      for {corr_id, should_exist} <- corr_ids do
        expected = if should_exist, do: 1, else: 0

        receive do
          {:tokio_complete, ^corr_id, :ok, ^expected} ->
            :ok

          {:tokio_complete, ^corr_id, :ok, other} ->
            # Only flunk for elements that should be absent (no false negative allowed for bloom)
            if should_exist do
              flunk("corr_id #{corr_id}: expected 1, got #{other}")
            end
        after
          5000 -> flunk("corr_id #{corr_id}: timeout")
        end
      end
    end

    test "mixed type async NIFs with distinct correlation_ids" do
      bloom_dir = make_prob_dir("corr_bloom")
      cms_dir = make_prob_dir("corr_cms")

      bloom_path = Path.join(bloom_dir, "test.bloom")
      cms_path = Path.join(cms_dir, "test.cms")

      {:ok, :ok} = NIF.bloom_file_create(bloom_path, 1000, 7)
      {:ok, 1} = NIF.bloom_file_add(bloom_path, "elem")

      {:ok, :ok} = NIF.cms_file_create(cms_path, 100, 5)
      {:ok, [7]} = NIF.cms_file_incrby(cms_path, [{"elem", 7}])

      bloom_corr = 111_111
      cms_corr = 222_222

      :ok = NIF.bloom_file_exists_async(self(), bloom_corr, bloom_path, "elem")
      :ok = NIF.cms_file_query_async(self(), cms_corr, cms_path, ["elem"])

      # Collect both — order doesn't matter
      results =
        for _ <- 1..2 do
          receive do
            {:tokio_complete, corr, status, val} -> {corr, status, val}
          after
            5000 -> flunk("timeout waiting for async response")
          end
        end

      bloom_result = Enum.find(results, fn {c, _, _} -> c == bloom_corr end)
      cms_result = Enum.find(results, fn {c, _, _} -> c == cms_corr end)

      assert {^bloom_corr, :ok, 1} = bloom_result
      assert {^cms_corr, :ok, [7]} = cms_result
    end
  end
end
