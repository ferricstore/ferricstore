defmodule Ferricstore.Flow.ProjectedValueReadTest do
  @moduledoc """
  Values the history projector has moved out of the keydir live in its history
  log. A batch read opens each log once rather than once per value.
  """
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.{HistoryProjector, ValueStore}
  alias Ferricstore.Store.Router

  test "a batch of projected values is read with one call per history log" do
    ctx = FerricStore.Instance.get(:default)
    partition = "projected-read-#{System.unique_integer([:positive])}"

    refs =
      for n <- 1..64 do
        id = "projected-read-#{n}-#{System.unique_integer([:positive])}"

        assert :ok =
                 FerricStore.flow_create(id,
                   type: "projected-read",
                   state: "queued",
                   partition_key: partition,
                   payload: %{"n" => n},
                   run_at_ms: 1_000,
                   now_ms: 1_000
                 )

        assert {:ok, %{payload_ref: ref}} = FerricStore.flow_get(id, partition_key: partition)
        ref
      end

    shard_index = Router.shard_for(ctx, hd(refs))
    projected!(ctx, shard_index, refs)

    # A process cannot trace itself, so the read runs in its own.
    test_pid = self()

    reader =
      spawn(fn ->
        receive do
          :read -> send(test_pid, {:values, ValueStore.raw_mget(ctx, refs)})
        end
      end)

    :erlang.trace(reader, true, [:call, {:tracer, self()}])
    :erlang.trace_pattern({HistoryProjector, :read_value, 3}, true, [:local])
    :erlang.trace_pattern({HistoryProjector, :read_values, 3}, true, [:local])
    send(reader, :read)
    assert_receive {:values, values}, 10_000
    calls = collect_calls([])

    assert Enum.map(values, &Ferricstore.Flow.Codec.decode_value/1) ==
             Enum.map(1..64, &%{"n" => &1})

    assert Enum.count(calls, &(&1 == :read_value)) == 0
    assert Enum.count(calls, &(&1 == :read_values)) in 1..2
  end

  # A value the mirror locates but cannot read is a storage failure, not a value
  # that is gone.
  test "an unreadable projected value is reported, not read as missing" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout()
    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)
    id = "projected-unreadable-read-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.Impl.flow_create(ctx, id,
               type: "projected-unreadable-read",
               state: "queued",
               payload: "kept payload",
               run_at_ms: 1_000,
               now_ms: 10
             )

    assert {:ok, %{payload_ref: ref}} = FerricStore.Impl.flow_get(ctx, id, [])
    shard_index = Router.shard_for(ctx, ref)
    projected!(ctx, shard_index, [ref])

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_index)
    assert {:ok, blob} = Ferricstore.Flow.LMDB.get(Ferricstore.Flow.LMDB.path(shard_path), ref)

    assert {:ok, {:flow_value_locator, 1, _expire, {:flow_history, file_id}, offset, _size}} =
             Ferricstore.TermCodec.decode(blob)

    log = Path.join([shard_path, "history", String.pad_leading("#{file_id}", 5, "0") <> ".log"])
    {:ok, file} = :file.open(log, [:read, :write, :binary])
    :ok = :file.pwrite(file, offset, :binary.copy(<<0xFF>>, 8))
    :ok = :file.close(file)

    assert {:error, "ERR storage read failed"} = ValueStore.value_mget(ctx, [ref])

    assert {:ok, %{payload_error: "ERR storage read failed"} = record} =
             FerricStore.Impl.flow_get(ctx, id, payload: true)

    refute Map.has_key?(record, :payload)
  end

  defp collect_calls(acc) do
    receive do
      {:trace, _pid, :call, {HistoryProjector, function, _args}} ->
        collect_calls([function | acc])
    after
      100 -> acc
    end
  end

  defp projected!(ctx, shard_index, refs, attempts \\ 500)

  defp projected!(_ctx, _shard_index, refs, 0),
    do: flunk("the projector kept #{length(refs)} values in the keydir")

  defp projected!(ctx, shard_index, refs, attempts) do
    assert :ok = HistoryProjector.flush(ctx, shard_index)
    keydir = elem(ctx.keydir_refs, shard_index)

    if Enum.any?(refs, &(:ets.lookup(keydir, &1) != [])) do
      Process.sleep(10)
      projected!(ctx, shard_index, refs, attempts - 1)
    end
  end
end
