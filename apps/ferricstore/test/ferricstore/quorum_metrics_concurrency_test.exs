defmodule Ferricstore.QuorumMetricsConcurrencyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.QuorumMetrics

  @metric_key {
    :slot_flush_queue_wait_us_max,
    [shard_index: 63, write_path: :quorum]
  }

  setup do
    QuorumMetrics.reset()
    on_exit(&QuorumMetrics.reset/0)
    :ok
  end

  test "maximum gauges never regress under concurrent observations" do
    Enum.each(1..300, fn _round ->
      :ets.delete_all_objects(:ferricstore_quorum_metrics)
      tasks = Enum.map(1..128, &concurrent_observation/1)

      Enum.each(tasks, &send(&1.pid, :observe))
      Task.await_many(tasks, 5_000)

      assert :ets.lookup(:ferricstore_quorum_metrics, @metric_key) == [
               {@metric_key, 128}
             ]
    end)
  end

  defp concurrent_observation(value) do
    Task.async(fn ->
      receive do
        :observe ->
          QuorumMetrics.handle_event(
            [:ferricstore, :batcher, :slot_flush],
            %{batch_size: 0, caller_count: 0, queue_wait_us: value},
            %{shard_index: 63, write_path: :quorum},
            nil
          )
      end
    end)
  end
end
