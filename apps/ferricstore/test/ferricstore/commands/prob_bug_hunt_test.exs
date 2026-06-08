Code.require_file("prob_bug_hunt_test/sections/probabilistic_direct_writes_surface_directory_fsync_failures.exs", __DIR__)
Code.require_file("prob_bug_hunt_test/sections/cms_edge_cases.exs", __DIR__)
defmodule Ferricstore.Commands.ProbBugHuntTest do
  @moduledoc """
  Bug-hunting tests for the four probabilistic data structure command
  modules: Bloom, Cuckoo, CMS, and TopK.

  Each test targets a specific behavioral contract drawn from the Redis
  documentation. Failures here indicate a bug in the command handler,
  not in the test.

  ## Bugs found

    1. **CMS.INITBYPROB depth formula is wrong.**
       The current code computes `depth = ceil(ln(1 / (1 - prob)))`.
       Redis defines the probability parameter as delta (the probability of
       the estimate exceeding the error bound), so the correct formula is
       `depth = ceil(ln(1 / prob))`.  With `prob = 0.01`, the code produces
       `depth = 1` instead of the correct `depth = 5`.

    2. **Bloom/Cuckoo `load_filter` crashes on wrong-type keys.**
       If a key holds a non-binary value (e.g. a CMS tuple), the two-clause
       `case` in `load_filter` raises `CaseClauseError` instead of returning
       a WRONGTYPE error.

    3. **BF.CARD over-counts when near capacity due to hash collisions.**
       The size counter increments whenever a new bit is set, but two
       distinct elements may set overlapping bit positions. If element B
       shares all its hash positions with previously-added elements, B is
       treated as a duplicate (size does not increment), leading to an
       undercount rather than an exact count. This is inherent to Bloom
       filters and not technically a bug, but the counter's accuracy
       degrades faster than expected.

  ## Test organisation

  Tests are grouped by data structure and then by the specific scenario
  under test. Every test uses `MockStore` for full isolation and runs
  `async: true`.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bloom
  alias Ferricstore.Commands.Cuckoo
  alias Ferricstore.Commands.CMS
  alias Ferricstore.Commands.Strings
  alias Ferricstore.Commands.TopK
  alias Ferricstore.Test.MockStore



  # ===========================================================================
  # Bloom filter (BF.*)
  # ===========================================================================






  # ===========================================================================
  # Cuckoo filter (CF.*)
  # ===========================================================================




  # ===========================================================================
  # Count-Min Sketch (CMS.*)
  # ===========================================================================




  # ===========================================================================
  # Top-K (TOPK.*)
  # ===========================================================================



  # ===========================================================================
  # All commands on non-existent key
  # ===========================================================================


  # ===========================================================================
  # Wrong-type key access
  # ===========================================================================



  # ===========================================================================
  # Additional edge cases
  # ===========================================================================





  # ===========================================================================
  # Helpers
  # ===========================================================================

  use Ferricstore.Commands.ProbBugHuntTest.Sections.ProbabilisticDirectWritesSurfaceDirectoryFsyncFailures

  use Ferricstore.Commands.ProbBugHuntTest.Sections.CmsEdgeCases

  defp to_info_map(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.into(%{}, fn [k, v] -> {k, v} end)
  end
end
