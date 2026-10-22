defmodule Ferricstore.Transaction.WatchIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.CommandStamp
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Transaction.WatchIndex

  test "a command writes the client key it names, however it reaches the log" do
    put = {:put, "k", "v", 0}

    assert WatchIndex.written(put) == {:keys, ["k"]}
    assert WatchIndex.written({put, %{hlc_ts: {5, 0}, wall_time_ms: 5}}) == {:keys, ["k"]}
    assert WatchIndex.written({:ferricstore_latency_trace, put}) == {:keys, ["k"]}

    {:ttb, encoded} = CommandStamp.to_ttb(put)
    assert WatchIndex.written({:ttb, encoded}) == {:keys, ["k"]}
  end

  test "compound storage keys resolve to their client key" do
    assert WatchIndex.written({:compound_put, CompoundKey.hash_field("h", "f"), "v", 0}) ==
             {:keys, ["h"]}

    assert WatchIndex.written({:compound_delete_prefix, CompoundKey.hash_prefix("h")}) ==
             {:keys, ["h"]}
  end

  test "multi-key commands and batches write every key they carry" do
    assert WatchIndex.written({:mset, [{"a", "1"}, {"b", "2"}]}) == {:keys, ["a", "b"]}
    assert WatchIndex.written({:delete_batch, ["a", "b", "a"]}) == {:keys, ["a", "b"]}
    assert WatchIndex.written({:key_lifecycle, {:rename, "a", "b"}}) == {:keys, ["a", "b"]}
    assert WatchIndex.written({:key_lifecycle, {:copy, "a", "b", false}}) == {:keys, ["b"]}

    assert {:keys, keys} = WatchIndex.written({:batch, [{:put, "a", "1", 0}, {:delete, "b"}]})
    assert Enum.sort(keys) == ["a", "b"]
  end

  test "a transaction writes its queue's write keys inside its namespace" do
    queue = [%{write_keys: ["a"], read_keys: ["r"]}, %{write_keys: [], read_keys: ["b"]}]

    assert WatchIndex.written({:tx_execute, queue, "ns:", %{}}) == {:keys, ["ns:a"]}
    assert WatchIndex.written({:tx_execute, queue, nil}) == {:keys, ["a"]}
  end

  test "Flow commands and expiry write no client key" do
    assert WatchIndex.written({:flow_create, "f:{f}:s:x", %{}}) == :none
    assert WatchIndex.written({:expire_if_batch, [{"k", 1}]}) == :none
    assert WatchIndex.written({:watch_tokens, ["k"]}) == :none
  end

  test "commands whose keys are only known as they apply write every key" do
    assert WatchIndex.written({:flush_shard, {1, 0}}) == :all
    assert WatchIndex.written({:delete_prefix, "p"}) == :all
    assert WatchIndex.written({:some_future_command, "k"}) == :all
    assert WatchIndex.written({:mset, [:not_an_entry]}) == :all
  end

  test "a command whose result shows it changed nothing writes no key" do
    for {opts, result} <- [
          {%{nx: true}, nil},
          {%{xx: true}, nil},
          {%{nx: true, get: true}, "old"},
          {%{xx: true, get: true}, nil}
        ] do
      assert WatchIndex.written({:set, "k", "v", 0, opts}, result) == :none
    end

    for {opts, result} <- [
          {%{}, :ok},
          {%{nx: true}, :ok},
          {%{nx: true, get: true}, nil},
          {%{xx: true, get: true}, "old"}
        ] do
      assert WatchIndex.written({:set, "k", "v", 0, opts}, result) == {:keys, ["k"]}
    end

    assert WatchIndex.written({:cas, "k", "a", "b", nil}, nil) == :none
    assert WatchIndex.written({:cas, "k", "a", "b", nil}, 1) == {:keys, ["k"]}
    assert WatchIndex.written({:sadd_single, "s", "m"}, 0) == :none
    assert WatchIndex.written({:sadd_single, "s", "m"}, 1) == {:keys, ["s"]}
    assert WatchIndex.written({:getdel, "k"}, nil) == :none
    assert WatchIndex.written({:put, "k", "v", 0}, {:error, :failed}) == {:keys, ["k"]}
  end

  test "a batch writes the keys of the commands that changed something" do
    batch = {:batch, [{:set, "a", "v", 0, %{nx: true}}, {:put, "b", "v", 0}]}

    assert WatchIndex.written(batch, {[nil, :ok], 2}) == {:keys, ["b"]}
    assert WatchIndex.written(batch, {:ok, [nil, :ok]}) == {:keys, ["b"]}
    assert {:keys, keys} = WatchIndex.written(batch, :unaligned)
    assert Enum.sort(keys) == ["a", "b"]
  end

  test "every state machine command is classified" do
    sections = Path.expand("../../../lib/ferricstore/raft/state_machine/sections", __DIR__)

    tags =
      sections
      |> Path.join("*.ex")
      |> Path.wildcard()
      |> Enum.flat_map(fn path ->
        Regex.scan(~r/def apply\(\s*_?meta,\s*\{:([a-z0-9_]+)/, File.read!(path),
          capture: :all_but_first
        )
      end)
      |> List.flatten()
      |> Enum.map(&String.to_atom/1)
      |> Enum.uniq()

    wrappers = [:ttb, :async, :ferricstore_latency_trace, :ferricstore_apply_context]

    unclassified =
      Enum.reject(tags, fn tag ->
        tag in WatchIndex.classified_tags() or tag in wrappers or
          String.starts_with?(Atom.to_string(tag), "flow_")
      end)

    assert unclassified == []
    assert length(tags) > 100
  end
end
