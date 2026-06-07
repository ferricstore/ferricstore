defmodule Ferricstore.Flow.PipelineHistoryReadTest do
  use ExUnit.Case, async: true

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
