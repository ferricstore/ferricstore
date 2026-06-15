defmodule Ferricstore.Flow.PipelineHistoryReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineHistoryRead

  test "results returns empty map for no history ops" do
    assert PipelineHistoryRead.results([], :ctx, %{}) == %{}
  end

  test "results sends cold or consistent ops through read callback" do
    op = {3, "flow-1", "tenant", "history-key", %{count: 10}, true, false, %{enabled?: false}}

    callbacks = %{
      read: fn :ctx,
               "flow-1",
               "tenant",
               "history-key",
               %{count: 10},
               true,
               false,
               %{enabled?: false} ->
        {:ok, [:cold]}
      end
    }

    assert PipelineHistoryRead.results([op], :ctx, callbacks) == %{3 => {:ok, [:cold]}}
  end

  test "results prepares consistent cold ops once before reading without per-op consistency" do
    parent = self()

    op1 = {3, "flow-1", "tenant", "history-key-1", %{count: 10}, true, true, %{enabled?: false}}
    op2 = {4, "flow-2", "tenant", "history-key-2", %{count: 10}, true, true, %{enabled?: false}}

    callbacks = %{
      prepare_consistent: fn :ctx, ops ->
        send(parent, {:prepare_consistent, ops})
        :ok
      end,
      read: fn :ctx, id, "tenant", history_key, %{count: 10}, true, false, %{enabled?: false} ->
        send(parent, {:read, id, history_key})
        {:ok, [id]}
      end
    }

    assert PipelineHistoryRead.results([op1, op2], :ctx, callbacks) == %{
             3 => {:ok, ["flow-1"]},
             4 => {:ok, ["flow-2"]}
           }

    assert_received {:prepare_consistent, [^op1, ^op2]}
    assert_received {:read, "flow-1", "history-key-1"}
    assert_received {:read, "flow-2", "history-key-2"}
  end

  test "results returns prepare error for consistent cold ops without reading" do
    op = {3, "flow-1", "tenant", "history-key", %{count: 10}, true, true, %{enabled?: false}}

    callbacks = %{
      prepare_consistent: fn :ctx, [^op] -> {:error, "ERR projector unavailable"} end,
      read: fn _ctx,
               _id,
               _partition_key,
               _history_key,
               _query,
               _include_cold?,
               _consistent?,
               _value_return ->
        flunk("read should not run when consistent preparation fails")
      end
    }

    assert PipelineHistoryRead.results([op], :ctx, callbacks) == %{
             3 => {:error, "ERR projector unavailable"}
           }
  end

  test "results batches hot rank requests and hydrates ranked events" do
    op = {1, "flow-1", "tenant", "history-key", %{count: 2}, false, false, %{enabled?: false}}

    callbacks = %{
      fetch_count: fn %{count: 2} -> 2 end,
      hot_range: fn :ctx, "flow-1", "tenant", "history-key", 2 -> {0, 1} end,
      rank_range_many: fn :ctx, [{"history-key", 0, 1, false}] ->
        {:ok, [[{"e-1", 1}, {"e-2", 2}]]}
      end,
      from_event_ids: fn :ctx,
                         "flow-1",
                         "tenant",
                         "history-key",
                         ["e-1", "e-2"],
                         %{enabled?: false} ->
        {:ok, [{"e-1", %{event: "created"}}, {"e-2", %{event: "completed"}}]}
      end,
      apply_query: fn events, %{count: 2} -> events end
    }

    assert PipelineHistoryRead.results([op], :ctx, callbacks) == %{
             1 => {:ok, [{"e-1", %{event: "created"}}, {"e-2", %{event: "completed"}}]}
           }
  end

  test "results batch fetches decode contexts for hot history entries" do
    parent = self()

    op1 = {1, "flow-1", "tenant", "history-key-1", %{count: 1}, false, false, %{enabled?: false}}

    op2 = {2, "flow-2", "tenant", "history-key-2", %{count: 1}, false, false, %{enabled?: false}}

    callbacks = %{
      fetch_count: fn %{count: 1} -> 1 end,
      hot_range: fn :ctx, _id, "tenant", _history_key, 1 -> {0, 0} end,
      rank_range_many: fn :ctx,
                          [{"history-key-1", 0, 0, false}, {"history-key-2", 0, 0, false}] ->
        {:ok, [[{"e-1", 1}], [{"e-2", 2}]]}
      end,
      decode_contexts: fn :ctx, flows ->
        send(parent, {:decode_contexts, flows})

        %{
          {"flow-1", "tenant"} => %{id: "flow-1", type: "email"},
          {"flow-2", "tenant"} => %{id: "flow-2", type: "sms"}
        }
      end,
      from_event_ids_with_context: fn :ctx,
                                      id,
                                      "tenant",
                                      history_key,
                                      event_ids,
                                      %{enabled?: false},
                                      context ->
        send(parent, {:from_event_ids_with_context, id, history_key, event_ids, context})
        {:ok, [{List.first(event_ids), context}]}
      end,
      from_event_ids: fn _ctx, _id, _partition_key, _history_key, _event_ids, _value_return ->
        flunk("per-history context lookup should not be used")
      end,
      apply_query: fn events, %{count: 1} -> events end
    }

    assert PipelineHistoryRead.results([op1, op2], :ctx, callbacks) == %{
             1 => {:ok, [{"e-1", %{id: "flow-1", type: "email"}}]},
             2 => {:ok, [{"e-2", %{id: "flow-2", type: "sms"}}]}
           }

    assert_received {:decode_contexts, [{"flow-1", "tenant"}, {"flow-2", "tenant"}]}

    assert_received {:from_event_ids_with_context, "flow-1", "history-key-1", ["e-1"],
                     %{id: "flow-1", type: "email"}}

    assert_received {:from_event_ids_with_context, "flow-2", "history-key-2", ["e-2"],
                     %{id: "flow-2", type: "sms"}}
  end

  test "results skips hot index and fallback scan when record disables hot history" do
    parent = self()
    op = {1, "flow-1", "tenant", "history-key", %{count: 10}, false, false, %{enabled?: false}}

    callbacks = %{
      fetch_count: fn %{count: 10} -> 10 end,
      decode_contexts: fn :ctx, [{"flow-1", "tenant"}] ->
        send(parent, :decoded_contexts)
        %{{"flow-1", "tenant"} => %{id: "flow-1", history_hot_max_events: 0}}
      end,
      hot_range: fn _ctx, _id, _partition_key, _history_key, _count ->
        flunk("hot range should not be read when hot history is disabled")
      end,
      rank_range_many: fn _ctx, _requests ->
        flunk("rank index should not be read when hot history is disabled")
      end,
      fallback: fn _ctx, _history_key, _query, _value_return ->
        flunk("fallback scan should not run when hot history is disabled")
      end
    }

    assert PipelineHistoryRead.results([op], :ctx, callbacks) == %{1 => {:ok, []}}
    assert_received :decoded_contexts
  end

  test "results falls back when hot rank index is unavailable or empty" do
    op = {1, "flow-1", "tenant", "history-key", %{count: 2}, false, false, %{enabled?: false}}

    callbacks = %{
      fetch_count: fn _query -> 2 end,
      hot_range: fn _ctx, _id, _partition_key, _history_key, _count -> {0, 1} end,
      rank_range_many: fn _ctx, _requests -> :unavailable end,
      fallback: fn :ctx, "history-key", %{count: 2}, %{enabled?: false} -> {:ok, [:fallback]} end
    }

    assert PipelineHistoryRead.results([op], :ctx, callbacks) == %{1 => {:ok, [:fallback]}}
  end
end
