Code.require_file("prob_edge_cases_test/sections/bloom_edge_cases.exs", __DIR__)
Code.require_file("prob_edge_cases_test/sections/nif_level_edge_cases.exs", __DIR__)

defmodule Ferricstore.ProbEdgeCasesTest do
  @moduledoc """
  Comprehensive edge case tests for probabilistic data structures and MemoryGuard.

  Tests cover:
  1. Bloom filter command handler edge cases
  2. CMS command handler edge cases
  3. Cuckoo filter command handler edge cases
  4. TopK command handler edge cases
  5. MemoryGuard edge cases
  6. State machine prob-related edge cases
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.MemoryGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_keydir_full(false)
      MemoryGuard.set_skip_promotion(false)
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  # ===========================================================================
  # Test helpers
  # ===========================================================================

  use Ferricstore.ProbEdgeCasesTest.Sections.BloomEdgeCases

  use Ferricstore.ProbEdgeCasesTest.Sections.NifLevelEdgeCases

  defp make_prob_dir(prefix) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "prob_edge_#{prefix}_#{:os.getpid()}_#{:erlang.unique_integer([:positive])}"
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

  # ===========================================================================
  # 1. Bloom filter command handler edge cases
  # ===========================================================================

  # ===========================================================================
  # 2. CMS command handler edge cases
  # ===========================================================================

  # ===========================================================================
  # 3. Cuckoo filter command handler edge cases
  # ===========================================================================

  # ===========================================================================
  # 4. TopK command handler edge cases
  # ===========================================================================

  # ===========================================================================
  # 5. MemoryGuard edge cases
  # ===========================================================================

  # ===========================================================================
  # 6. State machine prob-related edge cases (via Router)
  # ===========================================================================

  # ===========================================================================
  # 7. NIF-level edge cases (direct NIF calls)
  # ===========================================================================

  # ===========================================================================
  # 8. Cross-cutting prob file cleanup
  # ===========================================================================

  # ===========================================================================
  # 9. Optimal sizing edge cases (Bloom)
  # ===========================================================================

  # ===========================================================================
  # 10. Input validation edge cases
  # ===========================================================================
end
