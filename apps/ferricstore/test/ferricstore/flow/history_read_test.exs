defmodule Ferricstore.Flow.HistoryReadTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Flow.HistoryRead

  setup do
    previous = Application.get_env(:ferricstore, :flow_max_history_max_events)
    previous_sweep = Application.get_env(:ferricstore, :flow_lmdb_history_sweep_limit)
    Application.put_env(:ferricstore, :flow_max_history_max_events, 128)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_max_history_max_events)
        value -> Application.put_env(:ferricstore, :flow_max_history_max_events, value)
      end

      case previous_sweep do
        nil -> Application.delete_env(:ferricstore, :flow_lmdb_history_sweep_limit)
        value -> Application.put_env(:ferricstore, :flow_lmdb_history_sweep_limit, value)
      end
    end)
  end

  test "filtered reverse hot reads scan the bounded history window" do
    query = %{
      count: 1,
      rev?: true,
      from_event: nil,
      to_event: nil,
      from_ms: nil,
      to_ms: nil,
      from_version: nil,
      to_version: nil,
      event: "claimed",
      worker: nil
    }

    assert HistoryRead.query_fetch_count(query) == 128
    assert HistoryRead.query_fetch_count(%{query | event: nil}) == 1
  end

  test "history availability is not normalized to an empty result" do
    assert {:error, "ERR storage read failed"} =
             HistoryRead.__normalize_state_read_for_test__(:unavailable)

    assert {:error, "ERR storage read failed"} =
             HistoryRead.__normalize_hot_refs_for_test__(:unavailable)

    assert {:ok, false} = HistoryRead.__normalize_state_read_for_test__(nil)
    assert {:ok, []} = HistoryRead.__normalize_hot_refs_for_test__({:ok, []})
  end

  test "cold history batches require exact results and preserve storage failures" do
    decoder = fn
      "event-1", {:ok, "index-1"} -> {:ok, "value-1"}
      "event-2", :not_found -> :miss
      "event-3", {:ok, "index-3"} -> {:error, :eio}
    end

    assert {:ok, %{"event-1" => "value-1"}} =
             HistoryRead.__cold_values_by_event_results_for_test__(
               ["event-1", "event-2"],
               [{:ok, "index-1"}, :not_found],
               decoder
             )

    assert {:error, {:storage_read_failed, {:batch_result_mismatch, 2, 1}}} =
             HistoryRead.__cold_values_by_event_results_for_test__(
               ["event-1", "event-2"],
               [{:ok, "index-1"}],
               decoder
             )

    assert {:error, {:storage_read_failed, {:cold_value_read_failed, "event-3", :eio}}} =
             HistoryRead.__cold_values_by_event_results_for_test__(
               ["event-3"],
               [{:ok, "index-3"}],
               decoder
             )
  end

  test "history sweep limits stay bounded when mutable configuration is invalid" do
    Application.put_env(:ferricstore, :flow_lmdb_history_sweep_limit, "unbounded")
    assert HistoryRead.__history_lmdb_sweep_limit_for_test__() == 10_000

    Application.put_env(:ferricstore, :flow_lmdb_history_sweep_limit, -1)
    assert HistoryRead.__history_lmdb_sweep_limit_for_test__() == 10_000

    Application.put_env(:ferricstore, :flow_lmdb_history_sweep_limit, 2_000_000)
    assert HistoryRead.__history_lmdb_sweep_limit_for_test__() == 1_000_000
  end

  test "history projector failures do not expose internal reasons" do
    assert {:error, "ERR flow history projection unavailable"} =
             HistoryRead.__normalize_history_projector_flush_for_test__({
               :error,
               {:open_failed, "/private/shard/history.log"}
             })

    assert :ok = HistoryRead.__normalize_history_projector_flush_for_test__(:ok)
  end

  test "history decode context distinguishes missing state from corruption and read failures" do
    assert {:ok, %{id: "flow-1"}} =
             HistoryRead.__decode_context_read_for_test__(nil, "flow-1")

    assert {:error, "ERR invalid flow record"} =
             HistoryRead.__decode_context_read_for_test__("corrupt", "flow-1")

    assert {:error, "ERR storage read failed"} =
             HistoryRead.__decode_context_read_for_test__(:unavailable, "flow-1")

    assert {:error, "ERR storage read failed"} =
             HistoryRead.__decode_context_read_for_test__({:error, :eio}, "flow-1")
  end
end
