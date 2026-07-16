defmodule Ferricstore.Commands.ProbabilisticBatchLimitTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}

  test "probabilistic batch commands reject excessive work before storage" do
    parent = self()
    store = %{get: fn key -> send(parent, {:store_get, key}) end}
    elements = List.duplicate("item", 10_001)
    pairs = Enum.flat_map(elements, &[&1, "1"])

    assert {:error, "ERR bloom batch exceeds configured maximum batch size"} ==
             Bloom.handle("BF.MADD", ["bloom" | elements], store)

    assert {:error, "ERR bloom batch exceeds configured maximum batch size"} ==
             Bloom.handle("BF.MEXISTS", ["bloom" | elements], store)

    assert {:error, "ERR cuckoo batch exceeds maximum of 10000 items"} ==
             Cuckoo.handle("CF.MEXISTS", ["cuckoo" | elements], store)

    assert {:error, "ERR CMS: batch exceeds maximum of 10000 items"} ==
             CMS.handle("CMS.QUERY", ["cms" | elements], store)

    assert {:error, "ERR CMS: batch exceeds maximum of 10000 items"} ==
             CMS.handle("CMS.INCRBY", ["cms" | pairs], store)

    for command <- ~w(TOPK.ADD TOPK.QUERY TOPK.COUNT) do
      assert {:error, "ERR TopK batch exceeds maximum of 10000 items"} ==
               TopK.handle(command, ["topk" | elements], store)
    end

    assert {:error, "ERR TopK batch exceeds maximum of 10000 items"} ==
             TopK.handle("TOPK.INCRBY", ["topk" | pairs], store)

    refute_received {:store_get, _key}
  end
end
