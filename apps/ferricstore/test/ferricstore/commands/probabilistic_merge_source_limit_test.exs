defmodule Ferricstore.Commands.ProbabilisticMergeSourceLimitTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{CMS, TDigest}

  test "prepared merges reject excessive source lists before storage" do
    parent = self()
    store = %{get: fn key -> send(parent, {:store_get, key}) end}
    sources = List.duplicate("source", 10_001)

    assert {:error, "ERR CMS: too many source keys (maximum 10000)"} ==
             CMS.handle_ast({:cms_merge, "destination", sources, List.duplicate(1, 10_001)}, store)

    assert {:error, "ERR TDIGEST: too many source keys (maximum 10000)"} ==
             TDigest.handle_ast({:tdigest_merge, "destination", sources, []}, store)

    refute_received {:store_get, _key}
  end

  test "raw merges reject excessive declared source counts without storage" do
    parent = self()
    store = %{get: fn key -> send(parent, {:store_get, key}) end}

    assert {:error, "ERR CMS: too many source keys (maximum 10000)"} ==
             CMS.handle("CMS.MERGE", ["destination", "10001"], store)

    assert {:error, "ERR TDIGEST: too many source keys (maximum 10000)"} ==
             TDigest.handle("TDIGEST.MERGE", ["destination", "10001"], store)

    refute_received {:store_get, _key}
  end
end
