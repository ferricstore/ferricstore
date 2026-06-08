Code.require_file("tdigest_test/sections/tdigest_create.exs", __DIR__)
Code.require_file("tdigest_test/sections/tdigest_cdf.exs", __DIR__)
Code.require_file("tdigest_test/sections/stress_tests.exs", __DIR__)

defmodule Ferricstore.Commands.TDigestTest do
  @moduledoc """
  Comprehensive tests for the TDIGEST.* commands and the underlying
  Ferricstore.TDigest.Core data structure.

  Organized into sections:
    1.  TDIGEST.CREATE -- basic creation and validation
    2.  TDIGEST.ADD -- adding observations
    3.  TDIGEST.RESET -- clearing data
    4.  TDIGEST.INFO -- metadata retrieval
    5.  TDIGEST.MIN / TDIGEST.MAX -- extreme value tracking
    6.  Quantile accuracy -- statistical correctness
    7.  CDF tests -- cumulative distribution function
    8.  MERGE tests -- merging multiple digests
    9.  TRIMMED_MEAN tests -- quantile-bounded means
    10. RANK / REVRANK / BYRANK / BYREVRANK -- rank-based queries
    11. Edge cases -- boundary conditions and unusual inputs
    12. Stress tests -- performance and memory
    13. Integration -- dispatcher routing, catalog, full lifecycle
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.TDigest, as: TDigestCmd
  alias Ferricstore.TDigest.Core
  alias Ferricstore.Test.MockStore

  # ===========================================================================
  # 1. TDIGEST.CREATE
  # ===========================================================================

  use Ferricstore.Commands.TDigestTest.Sections.TdigestCreate
  use Ferricstore.Commands.TDigestTest.Sections.TdigestCdf
  use Ferricstore.Commands.TDigestTest.Sections.StressTests

defp parse_float_str("nan"), do: :nan
  defp parse_float_str("inf"), do: :infinity
  defp parse_float_str("-inf"), do: :neg_infinity

  defp parse_float_str(str) when is_binary(str) do
    case Float.parse(str) do
      {f, _} ->
        f

      :error ->
        case Integer.parse(str) do
          {i, _} -> i / 1
          :error -> raise "cannot parse #{inspect(str)} as float"
        end
    end
  end

  defp parse_float_str(val) when is_number(val), do: val / 1

  # Extract a value from the INFO flat list by field name
  defp find_info_field(info_list, field_name) do
    info_list
    |> Enum.chunk_every(2)
    |> Enum.find_value(fn
      [^field_name, value] -> value
      _ -> nil
    end)
  end
end

