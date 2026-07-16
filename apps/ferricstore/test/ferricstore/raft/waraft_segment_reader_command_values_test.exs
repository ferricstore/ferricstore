defmodule Ferricstore.Raft.WARaftSegmentReader.CommandValuesTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.WARaftSegmentReader.CommandValues

  test "cold replay extracts values through accepted Raft command wrappers" do
    command =
      {:ferricstore_apply_context, :encoded_context,
       {:ferricstore_latency_trace, {:async, node(), {:put, "key", "value", 0}}}}

    entry = {7, {:default, {make_ref(), command}}}

    assert {:ok, "value"} = CommandValues.value_from_entry(entry, "key")
    assert %{"key" => "value"} = CommandValues.values_from_entry(entry, ["key"])
  end

  test "cold replay extracts the command that an origin-checked wrapper applied" do
    command =
      {:async, :origin@host,
       {:origin_checked, "key", {:put, "key", "value", 0}, "before", 0, "value", 0}}

    entry = {8, {make_ref(), command}}

    assert {:ok, "value"} = CommandValues.value_from_entry(entry, "key")
    assert %{"key" => "value"} = CommandValues.values_from_entry(entry, ["key"])
  end

  test "WARaft storage and cold reads share one replay command decoder" do
    source =
      Path.expand(
        "../../../lib/ferricstore/raft/waraft_storage/sections/apply_result.ex",
        __DIR__
      )
      |> File.read!()

    assert source =~ "CommandValues.decode_replay_command(command)"
    refute source =~ "CommandStamp.decode_ttb(binary)"
  end
end
