defmodule Ferricstore.Commands.ProbBugHuntTest.Sections.CmsEdgeCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Bloom
      alias Ferricstore.Commands.Cuckoo
      alias Ferricstore.Commands.CMS
      alias Ferricstore.Commands.Strings
      alias Ferricstore.Commands.TopK
      alias Ferricstore.Test.MockStore

      describe "CMS edge cases" do
        test "CMS.INITBYDIM with width 0 returns error" do
          store = MockStore.make()
          assert {:error, _} = CMS.handle("CMS.INITBYDIM", ["sk", "0", "5"], store)
        end

        test "CMS.INITBYDIM with depth 0 returns error" do
          store = MockStore.make()
          assert {:error, _} = CMS.handle("CMS.INITBYDIM", ["sk", "100", "0"], store)
        end

        test "CMS.INCRBY with count 0 returns error" do
          store = MockStore.make()
          :ok = CMS.handle("CMS.INITBYDIM", ["sk", "100", "5"], store)
          assert {:error, _} = CMS.handle("CMS.INCRBY", ["sk", "elem", "0"], store)
        end

        test "CMS.INFO reflects total count after multiple increments" do
          store = MockStore.make()
          :ok = CMS.handle("CMS.INITBYDIM", ["sk", "100", "5"], store)

          CMS.handle("CMS.INCRBY", ["sk", "a", "10"], store)
          CMS.handle("CMS.INCRBY", ["sk", "b", "20"], store)
          CMS.handle("CMS.INCRBY", ["sk", "a", "5"], store)

          result = CMS.handle("CMS.INFO", ["sk"], store)
          assert ["width", 100, "depth", 5, "count", 35] = result
        end

        test "CMS.INITBYPROB rejects probability of 0" do
          store = MockStore.make()
          assert {:error, _} = CMS.handle("CMS.INITBYPROB", ["sk", "0.01", "0.0"], store)
        end

        test "CMS.INITBYPROB rejects probability of 1" do
          store = MockStore.make()
          assert {:error, _} = CMS.handle("CMS.INITBYPROB", ["sk", "0.01", "1.0"], store)
        end
      end

      describe "TopK edge cases" do
        test "TOPK.RESERVE with k=1 tracks single most frequent element" do
          store = MockStore.make()
          :ok = TopK.handle("TOPK.RESERVE", ["tk", "1"], store)

          TopK.handle("TOPK.INCRBY", ["tk", "a", "100"], store)
          assert ["a"] = TopK.handle("TOPK.LIST", ["tk"], store)

          # Higher frequency replaces it
          TopK.handle("TOPK.INCRBY", ["tk", "b", "200"], store)
          assert ["b"] = TopK.handle("TOPK.LIST", ["tk"], store)
          assert [0] = TopK.handle("TOPK.QUERY", ["tk", "a"], store)
        end

        test "TOPK.ADD returns eviction info per element" do
          store = MockStore.make()
          :ok = TopK.handle("TOPK.RESERVE", ["tk", "2"], store)

          # Fill the heap
          result1 = TopK.handle("TOPK.ADD", ["tk", "a", "b"], store)
          assert length(result1) == 2
          assert Enum.all?(result1, &is_nil/1)

          # Adding without exceeding min count should not evict
          result2 = TopK.handle("TOPK.ADD", ["tk", "c"], store)
          assert length(result2) == 1
        end

        test "TOPK.INCRBY with zero count returns error" do
          store = MockStore.make()
          :ok = TopK.handle("TOPK.RESERVE", ["tk", "5"], store)
          assert {:error, _} = TopK.handle("TOPK.INCRBY", ["tk", "elem", "0"], store)
        end

        test "TOPK.LIST on empty tracker returns empty list" do
          store = MockStore.make()
          :ok = TopK.handle("TOPK.RESERVE", ["tk", "5"], store)
          assert [] = TopK.handle("TOPK.LIST", ["tk"], store)
        end

        test "TOPK.INFO returns correct default dimensions" do
          store = MockStore.make()
          :ok = TopK.handle("TOPK.RESERVE", ["tk", "10"], store)

          result = TopK.handle("TOPK.INFO", ["tk"], store)
          assert ["k", 10, "width", 8, "depth", 7, "decay", 0.9] = result
        end
      end
    end
  end
end
