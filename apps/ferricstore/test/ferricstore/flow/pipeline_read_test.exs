defmodule Ferricstore.Flow.PipelineReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineRead

  test "batch fast path preserves same-partition get order directly" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, {:get, id} ->
          {:get, id, "p1", %{enabled?: false, max_bytes: 64 * 1024}}
      end,
      batch_get: fn :ctx, ["a", "b", "c"], "p1" -> ["a", nil, "c"] end,
      decode_get: fn
        nil -> {:ok, nil}
        value -> {:ok, %{id: value}}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "a"}, {:get, "b"}, {:get, "c"}] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [{:get, "a"}, {:get, "b"}, {:get, "c"}], callbacks) == [
             {:ok, %{id: "a"}},
             {:ok, nil},
             {:ok, %{id: "c"}}
           ]
  end

  test "batch fast path preserves get order without payload hydration" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, {:get, id, partition} ->
          {:get, id, partition, %{enabled?: false, max_bytes: 64 * 1024}}
      end,
      batch_get: fn
        :ctx, ["b", "c"], "p2" -> ["b", "c"]
        :ctx, ["a"], "p1" -> ["a"]
      end,
      decode_get: fn
        nil -> {:ok, nil}
        value -> {:ok, %{id: value}}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "b", "p2"}, {:get, "a", "p1"}, {:get, "c", "p2"}] -> :ok end
    }

    assert PipelineRead.batch(
             :ctx,
             [{:get, "b", "p2"}, {:get, "a", "p1"}, {:get, "c", "p2"}],
             callbacks
           ) == [
             {:ok, %{id: "b"}},
             {:ok, %{id: "a"}},
             {:ok, %{id: "c"}}
           ]
  end

  test "batch preserves order across history, other, and errors" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, :history ->
          {:history, "flow-1", "tenant", "history-key", %{count: 10}, false, false,
           %{enabled?: false}}

        _ctx, :other ->
          {:other, fn -> {:ok, :other} end}

        _ctx, :bad ->
          {:error, "ERR bad"}
      end,
      decode_get: fn nil -> {:ok, nil} end,
      history_results: fn [
                            {0, "flow-1", "tenant", "history-key", %{count: 10}, false, false,
                             %{enabled?: false}}
                          ],
                          :ctx ->
        %{0 => {:ok, :history}}
      end,
      observe: fn :started, [:history, :bad, :other] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [:history, :bad, :other], callbacks) == [
             {:ok, :history},
             {:error, "ERR bad"},
             {:ok, :other}
           ]
  end

  test "batch coalesces identical keyed reads while preserving output order" do
    parent = self()

    callbacks = %{
      start: fn -> :started end,
      command: fn _ctx, op ->
        {:other, {:read_once, op},
         fn ->
           send(parent, {:read_executed, op})
           {:ok, op}
         end}
      end,
      decode_get: fn nil -> {:ok, nil} end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [:a, :a, :b, :a] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [:a, :a, :b, :a], callbacks) == [
             {:ok, :a},
             {:ok, :a},
             {:ok, :b},
             {:ok, :a}
           ]

    assert_receive {:read_executed, :a}
    assert_receive {:read_executed, :b}
    refute_receive {:read_executed, _}
  end

  test "batch keeps unkeyed other reads independent" do
    parent = self()

    callbacks = %{
      start: fn -> :started end,
      command: fn _ctx, op ->
        {:other,
         fn ->
           send(parent, {:read_executed, op})
           {:ok, op}
         end}
      end,
      decode_get: fn nil -> {:ok, nil} end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [:a, :a] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [:a, :a], callbacks) == [{:ok, :a}, {:ok, :a}]

    assert_receive {:read_executed, :a}
    assert_receive {:read_executed, :a}
  end

  test "hydrate_get_results passes through non-record results and records without payload" do
    decoded = [
      {0, {:ok, %{id: "flow-1"}}, %{enabled?: false, max_bytes: 10}},
      {1, {:error, "ERR"}, %{enabled?: false, max_bytes: 10}},
      {2, {:ok, nil}, %{enabled?: false, max_bytes: 10}}
    ]

    assert PipelineRead.hydrate_get_results(decoded, :ctx) == [
             {2, {:ok, nil}},
             {1, {:error, "ERR"}},
             {0, {:ok, %{id: "flow-1"}}}
           ]
  end
end
