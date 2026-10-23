defmodule Ferricstore.Flow.HistoryOrderTest do
  @moduledoc """
  Events written in the same millisecond share their id's "<ms>-" prefix, so
  their order comes from the version alone: "1000-9" precedes "1000-10".
  """
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow
  alias Ferricstore.Flow.{HistoryAPI, HistoryProjector}
  alias Ferricstore.Test.IsolatedInstance

  @partition "history-order"
  @event_ids Enum.map(1..13, &"1000-#{&1}")

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  for {storage, create_opts, trim_opts} <- [
        {:cold, [], [history_max_events: 4]},
        {:hot, [history_hot_max_events: 16], [history_max_events: 4, history_hot_max_events: 4]}
      ] do
    describe "#{storage} history in one millisecond" do
      test "lists and filters events in version order", %{ctx: ctx} do
        id = write_same_ms_history!(ctx, unquote(create_opts))

        assert ids(history!(ctx, id)) == @event_ids
        assert ids(history!(ctx, id, count: 3)) == ["1000-11", "1000-12", "1000-13"]
        assert ids(history!(ctx, id, from_event: "1000-9")) == Enum.slice(@event_ids, 8..12)
        assert ids(history!(ctx, id, to_event: "1000-10")) == Enum.slice(@event_ids, 0..9)

        assert ids(history!(ctx, id, rev: true, count: 5)) ==
                 ["1000-13", "1000-12", "1000-11", "1000-10", "1000-9"]
      end

      test "pages through events in version order", %{ctx: ctx} do
        id = write_same_ms_history!(ctx, unquote(create_opts))

        assert pages(ctx, id, :asc) == Enum.chunk_every(@event_ids, 4)
        assert pages(ctx, id, :desc) == Enum.chunk_every(Enum.reverse(@event_ids), 4)
      end

      test "trimming keeps the newest events", %{ctx: ctx} do
        id = write_same_ms_history!(ctx, unquote(trim_opts))

        assert ids(history!(ctx, id)) == ["1000-10", "1000-11", "1000-12", "1000-13"]
      end
    end
  end

  defp write_same_ms_history!(ctx, create_opts) do
    id = "history-order-#{System.unique_integer([:positive])}"

    assert :ok =
             Flow.create(
               ctx,
               id,
               [type: "history-order", partition_key: @partition, now_ms: 1_000] ++ create_opts
             )

    last_state =
      Enum.reduce(0..11, "queued", fn step, from ->
        to = "step-#{step}"

        assert :ok =
                 Flow.transition(ctx, id, from, to,
                   partition_key: @partition,
                   fencing_token: 0,
                   now_ms: 1_000
                 )

        to
      end)

    assert last_state == "step-11"
    assert :ok = HistoryProjector.flush(ctx, 0, 30_000)
    id
  end

  defp history!(ctx, id, opts \\ []) do
    assert {:ok, events} = Flow.history(ctx, id, [partition_key: @partition] ++ opts)
    events
  end

  defp pages(ctx, id, direction, before_event \\ nil, acc \\ []) do
    assert {:ok, page} =
             HistoryAPI.history_page_resolved(
               ctx,
               id,
               @partition,
               %{},
               4,
               before_event,
               direction
             )

    acc = [ids(page.events) | acc]

    if page.has_more,
      do: pages(ctx, id, direction, page.continuation, acc),
      else: Enum.reverse(acc)
  end

  defp ids(events), do: Enum.map(events, &elem(&1, 0))
end
