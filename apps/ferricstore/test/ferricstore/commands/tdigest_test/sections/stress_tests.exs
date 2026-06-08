defmodule Ferricstore.Commands.TDigestTest.Sections.StressTests do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Hash
      alias Ferricstore.Commands.TDigest, as: TDigestCmd
      alias Ferricstore.TDigest.Core
      alias Ferricstore.Test.MockStore

      describe "stress tests" do
        test "100K adds, quantile query < 100ms" do
          store = MockStore.make()
          :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

          # Add 100K values
          for chunk <- Enum.chunk_every(1..100_000, 1000) do
            values = Enum.map(chunk, &Integer.to_string/1)
            :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)
          end

          # Measure quantile query time
          start = System.monotonic_time(:millisecond)
          TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5", "0.95", "0.99"], store)
          elapsed = System.monotonic_time(:millisecond) - start

          assert elapsed < 100, "quantile query took #{elapsed}ms, expected < 100ms"
        end

        @tag :slow
        test "1M adds, memory < 100KB" do
          digest = Core.new(100)

          digest =
            Enum.reduce(1..1_000_000, digest, fn i, d ->
              Core.add(d, i / 1)
            end)

          info = Core.info(digest)

          assert info.memory_usage < 100_000,
                 "memory usage #{info.memory_usage} bytes, expected < 100KB"
        end

        test "sequential adds from 10 batches" do
          store = MockStore.make()
          :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

          # Each batch adds 100 values sequentially (the command handler does
          # a get-modify-put cycle so concurrent Tasks would race on the store)
          for i <- 0..9 do
            values = Enum.map((i * 100 + 1)..(i * 100 + 100), &Integer.to_string/1)
            :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest" | values], store)
          end

          # All 1000 values should be in the digest
          info = TDigestCmd.handle("TDIGEST.INFO", ["mydigest"], store)
          merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
          unmerged_weight = parse_float_str(find_info_field(info, "Unmerged weight"))
          total = merged_weight + unmerged_weight
          assert_in_delta total, 1000.0, 1.0
        end

        test "merge 100 digests" do
          store = MockStore.make()

          # Create 100 source digests
          for i <- 1..100 do
            key = "src_#{i}"
            :ok = TDigestCmd.handle("TDIGEST.CREATE", [key], store)
            values = Enum.map(((i - 1) * 10 + 1)..(i * 10), &Integer.to_string/1)
            :ok = TDigestCmd.handle("TDIGEST.ADD", [key | values], store)
          end

          src_keys = Enum.map(1..100, &"src_#{&1}")
          args = ["dst", "100" | src_keys]
          assert :ok = TDigestCmd.handle("TDIGEST.MERGE", args, store)

          info = TDigestCmd.handle("TDIGEST.INFO", ["dst"], store)
          merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
          assert_in_delta merged_weight, 1000.0, 1.0
        end

        test "repeated create/add/query cycle 1000 times" do
          store = MockStore.make()

          for i <- 1..1000 do
            key = "td_#{i}"
            :ok = TDigestCmd.handle("TDIGEST.CREATE", [key], store)
            :ok = TDigestCmd.handle("TDIGEST.ADD", [key, "#{i}.0"], store)
            [result] = TDigestCmd.handle("TDIGEST.QUANTILE", [key, "0.5"], store)
            assert parse_float_str(result) > 0
          end
        end
      end

      describe "dispatcher integration" do
        test "TDIGEST commands route through the dispatcher" do
          alias Ferricstore.Commands.Dispatcher
          store = MockStore.make()

          assert :ok = Dispatcher.dispatch("TDIGEST.CREATE", ["mydigest"], store)

          assert :ok =
                   Dispatcher.dispatch("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

          result = Dispatcher.dispatch("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
          assert is_list(result) and length(result) == 1

          info = Dispatcher.dispatch("TDIGEST.INFO", ["mydigest"], store)
          assert ["Compression", 100 | _] = info
        end

        test "TDIGEST commands are case-insensitive via dispatcher" do
          alias Ferricstore.Commands.Dispatcher
          store = MockStore.make()

          assert :ok = Dispatcher.dispatch("tdigest.create", ["mydigest"], store)
          assert :ok = Dispatcher.dispatch("tdigest.add", ["mydigest", "42.0"], store)
          result = Dispatcher.dispatch("tdigest.quantile", ["mydigest", "0.5"], store)
          assert is_list(result)
        end

        test "mixed case routing works" do
          alias Ferricstore.Commands.Dispatcher
          store = MockStore.make()

          assert :ok = Dispatcher.dispatch("Tdigest.Create", ["mydigest"], store)
          assert :ok = Dispatcher.dispatch("TDigest.Add", ["mydigest", "1.0"], store)
        end

        test "all TDIGEST commands are routable" do
          alias Ferricstore.Commands.Dispatcher
          store = MockStore.make()

          # Create and populate
          :ok = Dispatcher.dispatch("TDIGEST.CREATE", ["mydigest"], store)
          :ok = Dispatcher.dispatch("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

          # Read commands
          assert is_list(Dispatcher.dispatch("TDIGEST.QUANTILE", ["mydigest", "0.5"], store))
          assert is_list(Dispatcher.dispatch("TDIGEST.CDF", ["mydigest", "2.0"], store))
          assert is_list(Dispatcher.dispatch("TDIGEST.RANK", ["mydigest", "2.0"], store))
          assert is_list(Dispatcher.dispatch("TDIGEST.REVRANK", ["mydigest", "2.0"], store))
          assert is_list(Dispatcher.dispatch("TDIGEST.BYRANK", ["mydigest", "0"], store))
          assert is_list(Dispatcher.dispatch("TDIGEST.BYREVRANK", ["mydigest", "0"], store))

          assert is_binary(
                   Dispatcher.dispatch("TDIGEST.TRIMMED_MEAN", ["mydigest", "0.0", "1.0"], store)
                 )

          assert is_binary(Dispatcher.dispatch("TDIGEST.MIN", ["mydigest"], store))
          assert is_binary(Dispatcher.dispatch("TDIGEST.MAX", ["mydigest"], store))
          assert is_list(Dispatcher.dispatch("TDIGEST.INFO", ["mydigest"], store))

          # Mutation commands
          :ok = Dispatcher.dispatch("TDIGEST.CREATE", ["src"], store)
          :ok = Dispatcher.dispatch("TDIGEST.ADD", ["src", "5.0"], store)
          assert :ok = Dispatcher.dispatch("TDIGEST.MERGE", ["merged", "1", "src"], store)
          assert :ok = Dispatcher.dispatch("TDIGEST.RESET", ["mydigest"], store)
        end
      end

      describe "catalog integration" do
        test "catalog has entries for all TDIGEST commands" do
          alias Ferricstore.Commands.Catalog

          tdigest_cmds = ~w(tdigest.create tdigest.add tdigest.reset tdigest.quantile
        tdigest.cdf tdigest.rank tdigest.revrank tdigest.byrank tdigest.byrevrank
        tdigest.trimmed_mean tdigest.min tdigest.max tdigest.info tdigest.merge)

          for cmd <- tdigest_cmds do
            assert {:ok, entry} = Catalog.lookup(cmd),
                   "catalog should have entry for #{cmd}"

            assert entry.name == cmd
          end
        end

        test "catalog entries have correct key positions" do
          alias Ferricstore.Commands.Catalog

          for cmd <- ~w(tdigest.create tdigest.add tdigest.reset tdigest.quantile
                    tdigest.cdf tdigest.rank tdigest.revrank tdigest.byrank
                    tdigest.byrevrank tdigest.trimmed_mean tdigest.min tdigest.max
                    tdigest.info tdigest.merge) do
            {:ok, entry} = Catalog.lookup(cmd)
            assert entry.first_key == 1, "#{cmd} should have first_key=1"
          end
        end
      end

      describe "full lifecycle" do
        test "CREATE -> ADD 10K -> QUANTILE -> CDF -> MERGE -> RESET" do
          store = MockStore.make()

          # Create
          :ok = TDigestCmd.handle("TDIGEST.CREATE", ["td1"], store)
          :ok = TDigestCmd.handle("TDIGEST.CREATE", ["td2"], store)

          # ADD 10K values to each
          values1 = Enum.map(1..5000, &Integer.to_string/1)
          values2 = Enum.map(5001..10_000, &Integer.to_string/1)
          :ok = TDigestCmd.handle("TDIGEST.ADD", ["td1" | values1], store)
          :ok = TDigestCmd.handle("TDIGEST.ADD", ["td2" | values2], store)

          # QUANTILE on individual digests
          [p50_1] = TDigestCmd.handle("TDIGEST.QUANTILE", ["td1", "0.5"], store)
          [p50_2] = TDigestCmd.handle("TDIGEST.QUANTILE", ["td2", "0.5"], store)
          assert parse_float_str(p50_1) < parse_float_str(p50_2)

          # CDF check
          [cdf_result] = TDigestCmd.handle("TDIGEST.CDF", ["td1", "2500.0"], store)
          cdf_val = parse_float_str(cdf_result)
          assert_in_delta cdf_val, 0.5, 0.1

          # MERGE
          :ok = TDigestCmd.handle("TDIGEST.MERGE", ["combined", "2", "td1", "td2"], store)

          [p50_combined] = TDigestCmd.handle("TDIGEST.QUANTILE", ["combined", "0.5"], store)
          assert_in_delta parse_float_str(p50_combined), 5000.0, 5000.0 * 0.05

          # INFO check
          info = TDigestCmd.handle("TDIGEST.INFO", ["combined"], store)
          merged_weight = parse_float_str(find_info_field(info, "Merged weight"))
          assert_in_delta merged_weight, 10_000.0, 1.0

          # RESET
          :ok = TDigestCmd.handle("TDIGEST.RESET", ["combined"], store)
          assert ["nan"] = TDigestCmd.handle("TDIGEST.QUANTILE", ["combined", "0.5"], store)
        end

        test "persistence round-trip: ADD, store, reload, QUANTILE returns same result" do
          store = MockStore.make()

          :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)

          :ok =
            TDigestCmd.handle(
              "TDIGEST.ADD",
              ["mydigest" | Enum.map(1..1000, &Integer.to_string/1)],
              store
            )

          # Query before simulated reload
          [before] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)

          # "Reload" by reading raw value and putting it back (simulates persistence)
          raw = store.get.("mydigest")
          assert is_binary(raw)
          assert {:tdigest, _, _} = :erlang.binary_to_term(raw)

          store2 = MockStore.make()
          store2.put.("mydigest", raw, 0)

          # Query after simulated reload
          [after_reload] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store2)

          assert before == after_reload
        end

        test "CREATE -> ADD -> RESET -> ADD -> QUANTILE works correctly" do
          store = MockStore.make()

          :ok = TDigestCmd.handle("TDIGEST.CREATE", ["mydigest"], store)
          :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1000.0"], store)
          :ok = TDigestCmd.handle("TDIGEST.RESET", ["mydigest"], store)
          :ok = TDigestCmd.handle("TDIGEST.ADD", ["mydigest", "1.0", "2.0", "3.0"], store)

          [result] = TDigestCmd.handle("TDIGEST.QUANTILE", ["mydigest", "0.5"], store)
          val = parse_float_str(result)
          # Should reflect new data (1-3), not old data (1000)
          assert val >= 1.0 and val <= 3.0
        end
      end

      describe "Ferricstore.TDigest.Core direct tests" do
        test "new creates empty digest with default compression" do
          digest = Core.new()
          assert digest.compression == 100
          assert digest.centroids == []
          assert digest.count == 0
          assert digest.min == nil
          assert digest.max == nil
        end

        test "new creates empty digest with custom compression" do
          digest = Core.new(200)
          assert digest.compression == 200
        end

        test "add updates min/max" do
          digest = Core.new() |> Core.add(5.0)
          assert digest.min == 5.0
          assert digest.max == 5.0

          digest = Core.add(digest, 10.0)
          assert digest.min == 5.0
          assert digest.max == 10.0

          digest = Core.add(digest, 1.0)
          assert digest.min == 1.0
          assert digest.max == 10.0
        end

        test "add_many adds multiple values" do
          digest = Core.new() |> Core.add_many([1.0, 2.0, 3.0, 4.0, 5.0])
          assert digest.count == 5
          assert digest.min == 1.0
          assert digest.max == 5.0
        end

        test "compress reduces buffer to zero" do
          digest = Core.new() |> Core.add_many(Enum.map(1..10, &(&1 / 1)))
          compressed = Core.compress(digest)
          assert compressed.buffer == []
          assert compressed.buffer_size == 0
          assert compressed.centroids != []
        end

        test "reset preserves compression" do
          digest = Core.new(250) |> Core.add_many([1.0, 2.0, 3.0])
          reset = Core.reset(digest)
          assert reset.compression == 250
          assert reset.count == 0
          assert reset.centroids == []
        end

        test "info returns correct map" do
          digest = Core.new() |> Core.add_many(Enum.map(1..100, &(&1 / 1)))
          info = Core.info(digest)
          assert is_map(info)
          assert info.compression == 100
          assert info[:merged_nodes] >= 0
          assert info[:unmerged_nodes] >= 0
        end

        test "merge combines two digests" do
          d1 = Core.new() |> Core.add_many(Enum.map(1..50, &(&1 / 1)))
          d2 = Core.new() |> Core.add_many(Enum.map(51..100, &(&1 / 1)))
          merged = Core.merge(d1, d2)

          assert merged.count == 100
          assert merged.min == 1.0
          assert merged.max == 100.0
        end

        test "merge_many combines multiple digests" do
          digests =
            for i <- 0..4 do
              values = Enum.map((i * 20 + 1)..((i + 1) * 20), &(&1 / 1))
              Core.new() |> Core.add_many(values)
            end

          merged = Core.merge_many(digests, 100)
          assert merged.count == 100
          assert merged.min == 1.0
          assert merged.max == 100.0
        end

        test "quantile returns :nan for empty digest" do
          digest = Core.new()
          assert Core.quantile(digest, 0.5) == :nan
        end

        test "cdf returns :nan for empty digest" do
          digest = Core.new()
          assert Core.cdf(digest, 50.0) == :nan
        end

        test "rank returns -2 for empty digest" do
          digest = Core.new()
          assert Core.rank(digest, 50.0) == -2
        end

        test "by_rank returns :nan for empty digest" do
          digest = Core.new()
          assert Core.by_rank(digest, 0) == :nan
        end

        test "trimmed_mean returns :nan for empty digest" do
          digest = Core.new()
          assert Core.trimmed_mean(digest, 0.0, 1.0) == :nan
        end

        test "compression pass runs automatically when buffer is full" do
          # Buffer capacity = ceil(compression * 3) = 300 for default compression=100
          digest = Core.new()
          digest = Core.add_many(digest, Enum.map(1..300, &(&1 / 1)))

          # After adding 300 values (= buffer capacity), compression should have run
          assert digest.total_compressions >= 1
          assert digest.buffer_size < 300
        end

        test "centroids are always sorted by mean after compression" do
          digest = Core.new()
          # Add values in random order
          :rand.seed(:exsss, {1, 2, 3})
          values = Enum.shuffle(1..500) |> Enum.map(&(&1 / 1))
          digest = Core.add_many(digest, values) |> Core.compress()

          means = Enum.map(digest.centroids, fn {mean, _} -> mean end)
          assert means == Enum.sort(means)
        end

        test "total_compressions increments on each compress pass" do
          digest = Core.new()
          # Add enough to trigger multiple compressions
          digest = Core.add_many(digest, Enum.map(1..1000, &(&1 / 1)))
          assert digest.total_compressions >= 3
        end
      end

      describe "accuracy benchmarks" do
        @tag :slow
        test "uniform[0,1] with 100K samples: max quantile error < 0.02 at compression=100" do
          :rand.seed(:exsss, {100, 200, 300})
          values = for _ <- 1..100_000, do: :rand.uniform()

          digest = Core.new(100) |> Core.add_many(values)

          for q <- [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99] do
            estimated = Core.quantile(digest, q)
            error = abs(estimated - q)

            assert error < 0.02,
                   "q=#{q}: estimated=#{estimated}, error=#{error}, expected < 0.02"
          end
        end

        @tag :slow
        test "uniform[0,1] with 100K samples: max quantile error < 0.01 at compression=200" do
          :rand.seed(:exsss, {100, 200, 300})
          values = for _ <- 1..100_000, do: :rand.uniform()

          digest = Core.new(200) |> Core.add_many(values)

          for q <- [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99] do
            estimated = Core.quantile(digest, q)
            error = abs(estimated - q)

            assert error < 0.01,
                   "q=#{q}: estimated=#{estimated}, error=#{error}, expected < 0.01"
          end
        end

        @tag :slow
        test "merge accuracy: merge 10 digests of 10K vs single digest of 100K" do
          :rand.seed(:exsss, {42, 42, 42})
          all_values = for _ <- 1..100_000, do: :rand.uniform()

          # Single digest
          single = Core.new(100) |> Core.add_many(all_values)

          # 10 digests of 10K each
          chunks = Enum.chunk_every(all_values, 10_000)

          digests =
            Enum.map(chunks, fn chunk ->
              Core.new(100) |> Core.add_many(chunk)
            end)

          merged = Core.merge_many(digests, 100)

          for q <- [0.5, 0.95, 0.99] do
            single_val = Core.quantile(single, q)
            merged_val = Core.quantile(merged, q)
            diff = abs(single_val - merged_val)

            assert diff < 0.02,
                   "q=#{q}: single=#{single_val}, merged=#{merged_val}, diff=#{diff}"
          end
        end
      end
    end
  end
end
