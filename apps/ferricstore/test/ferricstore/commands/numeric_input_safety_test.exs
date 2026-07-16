defmodule Ferricstore.Commands.NumericInputSafetyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Blocking, Bloom, CMS, Geo, SortedSet, TDigest, TopK}
  alias Ferricstore.Test.MockStore

  test "float-taking command families reject values outside the VM float range" do
    huge = String.duplicate("9", 1_000)
    store = MockStore.make()

    commands = [
      {"BF.RESERVE", fn -> Bloom.handle("BF.RESERVE", ["key", huge, "100"], store) end},
      {"CMS.INITBYPROB", fn -> CMS.handle("CMS.INITBYPROB", ["key", huge, "0.5"], store) end},
      {"TOPK.RESERVE",
       fn -> TopK.handle("TOPK.RESERVE", ["key", "10", "8", "7", huge], store) end},
      {"TDIGEST.ADD", fn -> TDigest.handle("TDIGEST.ADD", ["key", huge], store) end},
      {"BLPOP", fn -> Blocking.handle("BLPOP", ["key", huge], store) end},
      {"GEOADD", fn -> Geo.handle("GEOADD", ["key", huge, "1", "member"], store) end},
      {"ZADD", fn -> SortedSet.handle("ZADD", ["key", huge, "member"], store) end}
    ]

    for {command, execute} <- commands do
      assert {:error, message} = execute.(), "expected #{command} to reject the value"
      assert is_binary(message)
    end
  end
end
