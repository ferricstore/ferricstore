defmodule Ferricstore.Flow.PipelineReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineRead

  @metadata %{}

  test "batch fast path preserves same-partition get order directly" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, {:get, id} ->
          {:get, id, "p1", %{enabled?: false, max_bytes: 64 * 1024}, @metadata}
      end,
      batch_get: fn :ctx, ["a", "b", "c"], "p1" -> ["a", nil, "c"] end,
      decode_get: fn
        nil, @metadata ->
          {:ok, nil}

        value, @metadata ->
          {:ok, %{id: value, partition_key: "p1", system_metadata: %{}}}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "a"}, {:get, "b"}, {:get, "c"}] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [{:get, "a"}, {:get, "b"}, {:get, "c"}], callbacks) == [
             {:ok, %{id: "a", partition_key: "p1"}},
             {:ok, nil},
             {:ok, %{id: "c", partition_key: "p1"}}
           ]
  end

  test "batch fast path projects internal record fields before returning" do
    callbacks = %{
      start: fn -> :started end,
      command: fn _ctx, {:get, id} ->
        {:get, id, "p1", %{enabled?: false, max_bytes: 64 * 1024}, @metadata}
      end,
      batch_get: fn :ctx, ["a"], "p1" -> [:encoded] end,
      decode_get: fn :encoded, @metadata ->
        {:ok,
         %{
           id: "a",
           partition_key: "p1",
           system_metadata: %{},
           state_enter_seq: 123
         }}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "a"}] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [{:get, "a"}], callbacks) == [
             {:ok, %{id: "a", partition_key: "p1"}}
           ]
  end

  test "batch metadata return uses the lightweight decoder and public metadata projection" do
    callbacks = %{
      start: fn -> :started end,
      command: fn _ctx, {:get, id} ->
        {:get, id, "p1", %{enabled?: false, max_bytes: 64 * 1024, record_return: :meta},
         @metadata}
      end,
      batch_get: fn :ctx, ["a"], "p1" -> [:encoded] end,
      decode_get: fn _value, _metadata -> flunk("full record decoder must not run") end,
      decode_get_meta: fn :encoded, @metadata ->
        {:ok,
         %{
           id: "a",
           type: "email",
           partition_key: "p1",
           system_metadata: %{},
           state_enter_seq: 123
         }}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "a"}] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [{:get, "a"}], callbacks) == [
             {:ok, %{id: "a", type: "email", partition_key: "p1"}}
           ]
  end

  test "batch fast path fails every read closed when a batch reply is short" do
    callbacks = %{
      start: fn -> :started end,
      command: fn _ctx, {:get, id} ->
        {:get, id, "p1", %{enabled?: false, max_bytes: 64 * 1024}, @metadata}
      end,
      batch_get: fn :ctx, ["a", "b"], "p1" -> ["a"] end,
      decode_get: fn value, @metadata ->
        {:ok, %{id: value, partition_key: "p2", system_metadata: %{}}}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "a"}, {:get, "b"}] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [{:get, "a"}, {:get, "b"}], callbacks) == [
             {:error, "ERR flow batch read result mismatch"},
             {:error, "ERR flow batch read result mismatch"}
           ]
  end

  test "batch fast path preserves get order without payload hydration" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, {:get, id, partition} ->
          {:get, id, partition, %{enabled?: false, max_bytes: 64 * 1024}, @metadata}
      end,
      batch_get: fn
        :ctx, ["b", "c"], "p2" -> ["b", "c"]
        :ctx, ["a"], "p1" -> ["a"]
      end,
      decode_get: fn
        nil, @metadata ->
          {:ok, nil}

        value, @metadata ->
          partition_key = if value == "a", do: "p1", else: "p2"
          {:ok, %{id: value, partition_key: partition_key, system_metadata: %{}}}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "b", "p2"}, {:get, "a", "p1"}, {:get, "c", "p2"}] -> :ok end
    }

    assert PipelineRead.batch(
             :ctx,
             [{:get, "b", "p2"}, {:get, "a", "p1"}, {:get, "c", "p2"}],
             callbacks
           ) == [
             {:ok, %{id: "b", partition_key: "p2"}},
             {:ok, %{id: "a", partition_key: "p1"}},
             {:ok, %{id: "c", partition_key: "p2"}}
           ]
  end

  test "batch fast path isolates a short reply to its partition group" do
    callbacks = %{
      start: fn -> :started end,
      command: fn _ctx, {:get, id, partition} ->
        {:get, id, partition, %{enabled?: false, max_bytes: 64 * 1024}, @metadata}
      end,
      batch_get: fn
        :ctx, ["a", "b"], "p1" -> ["a"]
        :ctx, ["c"], "p2" -> ["c"]
      end,
      decode_get: fn value, @metadata ->
        {:ok, %{id: value, partition_key: "p2", system_metadata: %{}}}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "a", "p1"}, {:get, "c", "p2"}, {:get, "b", "p1"}] ->
        :ok
      end
    }

    assert PipelineRead.batch(
             :ctx,
             [{:get, "a", "p1"}, {:get, "c", "p2"}, {:get, "b", "p1"}],
             callbacks
           ) == [
             {:error, "ERR flow batch read result mismatch"},
             {:ok, %{id: "c", partition_key: "p2"}},
             {:error, "ERR flow batch read result mismatch"}
           ]
  end

  test "batch generic path isolates a short get reply to its partition group" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, {:get, id, partition} ->
          {:get, id, partition, %{enabled?: false, max_bytes: 64 * 1024}, @metadata}

        _ctx, :other ->
          {:other, fn -> {:ok, :other} end}
      end,
      batch_get: fn
        :ctx, ["a", "b"], "p1" -> ["a"]
        :ctx, ["c"], "p2" -> ["c"]
      end,
      decode_get: fn value, @metadata ->
        {:ok, %{id: value, partition_key: "p2", system_metadata: %{}}}
      end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [{:get, "a", "p1"}, :other, {:get, "c", "p2"}, {:get, "b", "p1"}] ->
        :ok
      end
    }

    assert PipelineRead.batch(
             :ctx,
             [{:get, "a", "p1"}, :other, {:get, "c", "p2"}, {:get, "b", "p1"}],
             callbacks
           ) == [
             {:error, "ERR flow batch read result mismatch"},
             {:ok, :other},
             {:ok, %{id: "c", partition_key: "p2"}},
             {:error, "ERR flow batch read result mismatch"}
           ]
  end

  test "batch preserves order across history, other, and errors" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, :history ->
          {:history, "flow-1", "tenant", "history-key", %{count: 10}, false, false,
           %{enabled?: false}, @metadata}

        _ctx, :other ->
          {:other, fn -> {:ok, :other} end}

        _ctx, :bad ->
          {:error, "ERR bad"}
      end,
      decode_get: fn nil, @metadata -> {:ok, nil} end,
      history_results: fn [
                            {0, "flow-1", "tenant", "history-key", %{count: 10}, false, false,
                             %{enabled?: false}, @metadata}
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

  test "batch prepares each command once when the get fast path falls back" do
    parent = self()

    callbacks = %{
      start: fn -> :started end,
      command: fn _ctx, :other = op ->
        send(parent, {:prepared, op})
        {:other, fn -> {:ok, :other} end}
      end,
      decode_get: fn nil, @metadata -> {:ok, nil} end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [:other] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [:other], callbacks) == [{:ok, :other}]

    assert_received {:prepared, :other}
    refute_received {:prepared, _op}
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
      decode_get: fn nil, @metadata -> {:ok, nil} end,
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
      decode_get: fn nil, @metadata -> {:ok, nil} end,
      history_results: fn [], :ctx -> %{} end,
      observe: fn :started, [:a, :a] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [:a, :a], callbacks) == [{:ok, :a}, {:ok, :a}]

    assert_receive {:read_executed, :a}
    assert_receive {:read_executed, :a}
  end

  test "hydrate_get_results passes through non-record results and records without payload" do
    decoded = [
      {0, {:ok, %{id: "flow-1", partition_key: "p1"}}, %{enabled?: false, max_bytes: 10}},
      {1, {:error, "ERR"}, %{enabled?: false, max_bytes: 10}},
      {2, {:ok, nil}, %{enabled?: false, max_bytes: 10}}
    ]

    assert PipelineRead.hydrate_get_results(decoded, :ctx) == [
             {2, {:ok, nil}},
             {1, {:error, "ERR"}},
             {0, {:ok, %{id: "flow-1", partition_key: "p1"}}}
           ]
  end

  test "hydrate_get_results projects internal record fields" do
    decoded = [
      {0, {:ok, %{id: "flow-1", partition_key: "p1", state_enter_seq: 123}},
       %{enabled?: false, max_bytes: 10}}
    ]

    assert PipelineRead.hydrate_get_results(decoded, :ctx) == [
             {0, {:ok, %{id: "flow-1", partition_key: "p1"}}}
           ]
  end
end
