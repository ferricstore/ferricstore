defmodule Ferricstore.Flow.LMDBWriter.ReplaySafeRequestTest do
  @moduledoc """
  Every Raft entry a shard applies can ask the LMDB writer to persist its
  replay-safe index, and each persist syncs the marker file twice. A writer
  that persisted once per request fell behind a burst of writes that queue no
  Flow ops, and kept the disk busy syncing for seconds after the burst.
  """
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.{LMDBReplaySafeIndex, LMDBWriter}

  @persist_event [:ferricstore, :flow, :lmdb_replay_safe_index, :persist]

  test "requests waiting for the writer persist the replay-safe index once" do
    %{ctx: ctx, shard_data_path: shard_data_path, writer: writer} = start_writer!()
    handler = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(handler, @persist_event, &__MODULE__.handle_persist/4, {self(), writer})

    on_exit(fn -> :telemetry.detach(handler) end)

    :ok = :sys.suspend(writer)

    for index <- 1..50,
        do: assert(:requested == LMDBWriter.request(ctx, 0, shard_data_path, index))

    :ok = :sys.resume(writer)

    assert Ferricstore.Test.ShardHelpers.eventually(fn ->
             LMDBWriter.durable_index(ctx, 0, shard_data_path) == 50
           end)

    assert 50 == LMDBReplaySafeIndex.read(shard_data_path)
    assert [50] == persisted_indexes()
  end

  def handle_persist(_event, %{index: index}, %{status: :ok}, {test, writer}) do
    if self() == writer, do: send(test, {:persisted, index})
  end

  def handle_persist(_event, _measurements, _metadata, _config), do: :ok

  defp persisted_indexes(acc \\ []) do
    receive do
      {:persisted, index} -> persisted_indexes([index | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp start_writer! do
    instance_name = :"lmdb_replay_safe_request_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(instance_name))
    on_exit(fn -> File.rm_rf!(data_dir) end)
    Ferricstore.DataDir.ensure_layout!(data_dir, 1)

    ctx = %{
      name: instance_name,
      flow_lmdb_replay_safe_index: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_requested_index: :atomics.new(1, signed: false),
      flow_lmdb_replay_safe_persist_failures: :atomics.new(1, signed: false),
      flow_lmdb_mirror_degraded: :atomics.new(1, signed: false),
      flow_lmdb_writer_flush_failures: :atomics.new(1, signed: false)
    }

    writer =
      start_supervised!(
        {LMDBWriter,
         shard_index: 0, data_dir: data_dir, instance_name: instance_name, instance_ctx: ctx}
      )

    %{ctx: ctx, shard_data_path: Ferricstore.DataDir.shard_data_path(data_dir, 0), writer: writer}
  end
end
