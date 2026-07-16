defmodule Ferricstore.Flow.OrderedIndexEarliestDueTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF

  test "native matcher returns one earliest score across positive due-count keys" do
    index = NIF.flow_index_new()

    assert :ok =
             NIF.flow_index_put_entries(index, [
               {"f:{flow}:d:email:queued:p1", "queued-later", 40.0},
               {"f:{flow}:d:email:queued:p1", "queued-first", 30.0},
               {"f:{flow}:d:email:retry:p1", "retry-first", 20.0},
               {"f:{flow}:da:email:p1", "any-state-first", 15.0},
               {"f:{flow}:d:email:queued:p2", "wrong-priority", 10.0},
               {"f:{flow}:d:sms:queued:p1", "wrong-type", 5.0},
               {"ordinary:index", "not-due", 1.0},
               {"f:{flow}:d:email:zero:p1", "ignored-zero-count", 2.0}
             ])

    assert :ok = NIF.flow_index_restore_count(index, "f:{flow}:d:email:zero:p1", 0)
    assert :ok = NIF.flow_index_restore_count(index, "f:{flow}:d:email:stale:p1", 1)

    assert 15.0 ==
             NIF.flow_index_earliest_due_score(
               index,
               ["f:{flow}:"],
               ["}:d:email:", "}:da:email:p"],
               [":p1"]
             )

    assert nil == NIF.flow_index_earliest_due_score(index, ["f:{missing}:"], [], [])
  end

  test "aggregate matcher is classified as dirty CPU work" do
    source =
      File.read!(Path.expand("../../../native/ferricstore_bitcask/src/flow_index.rs", __DIR__))

    assert source =~
             ~r/#\[rustler::nif\(schedule = "DirtyCpu"\)\]\s+pub fn flow_index_earliest_due_score\b/
  end
end
