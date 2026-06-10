defmodule Ferricstore.LatencyTraceTest do
  use ExUnit.Case, async: true

  alias Ferricstore.LatencyTrace

  test "span is transparent when tracing is disabled" do
    assert LatencyTrace.enabled?() == false
    assert LatencyTrace.span("stage_us", fn -> :ok end) == :ok
    assert LatencyTrace.enabled?() == false
  end

  test "span records integer microseconds and accumulates repeated stages" do
    previous = LatencyTrace.start(%{"existing_us" => 7})

    try do
      assert LatencyTrace.enabled?()

      assert LatencyTrace.span("stage_us", fn ->
               LatencyTrace.add("stage_us", 3)
               :done
             end) == :done

      trace = LatencyTrace.finish(previous)

      assert trace["existing_us"] == 7
      assert is_integer(trace["stage_us"])
      assert trace["stage_us"] >= 3
      assert LatencyTrace.enabled?() == false
    after
      if LatencyTrace.enabled?(), do: LatencyTrace.finish(previous)
    end
  end

  test "finish restores previous trace context" do
    outer = LatencyTrace.start(%{"outer_us" => 1})
    inner = LatencyTrace.start(%{"inner_us" => 2})

    assert LatencyTrace.finish(inner) == %{"inner_us" => 2}
    assert LatencyTrace.finish(outer) == %{"outer_us" => 1}
    assert LatencyTrace.enabled?() == false
  end

  test "command and result wrappers are removed while trace is merged" do
    previous = LatencyTrace.start(%{})

    try do
      assert LatencyTrace.maybe_wrap_command({:put, "k", "v", 0}) ==
               {:ferricstore_latency_trace, {:put, "k", "v", 0}}

      result =
        {:ok,
         [
           {:ferricstore_latency_trace_result, :ok,
            %{"server_apply_us" => 10, "server_bitcask_append_us" => 5}}
         ]}

      assert LatencyTrace.merge_result(result) == {:ok, [:ok]}

      trace = LatencyTrace.finish(previous)
      assert trace["server_apply_us"] == 10
      assert trace["server_bitcask_append_us"] == 5
    after
      if LatencyTrace.enabled?(), do: LatencyTrace.finish(previous)
    end
  end
end
