defmodule Ferricstore.Commands.ProbabilisticLongKeyPathTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.Test.MockStore

  @cases [
    {Bloom, "BF.RESERVE", ["0.01", "100"], ".bloom"},
    {Cuckoo, "CF.RESERVE", ["100"], ".cuckoo"},
    {CMS, "CMS.INITBYDIM", ["100", "5"], ".cms"},
    {TopK, "TOPK.RESERVE", ["10"], ".topk"}
  ]

  test "probabilistic sidecars support keys up to the store key-size contract" do
    key = String.duplicate("long-probabilistic-key", 1_000)

    for {module, command, args, extension} <- @cases do
      store = MockStore.make()
      prob_dir = store.prob_dir.()

      assert :ok = module.handle(command, [key | args], store)

      assert [filename] =
               prob_dir
               |> File.ls!()
               |> Enum.filter(&String.ends_with?(&1, extension))

      assert byte_size(filename) <= 255
    end
  end
end
