defmodule Ferricstore.Store.PublicationEpochTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.PublicationEpoch

  test "a reader blocked by a live writer does not busy-spin for the whole wait" do
    latch = :ets.new(:publication_epoch_wait_budget, [:set, :public])
    ctx = %{publication_epoch: :atomics.new(1, signed: false), latch_refs: {latch}}
    token = PublicationEpoch.begin_write(ctx, 0)
    parent = self()

    reader =
      Task.async(fn ->
        send(parent, :waiting_reader)
        PublicationEpoch.read(ctx, [0], fn -> :stable end)
      end)

    try do
      assert_receive :waiting_reader
      started = System.monotonic_time(:millisecond)
      {:reductions, before} = Process.info(reader.pid, :reductions)
      Process.sleep(50)
      elapsed = max(System.monotonic_time(:millisecond) - started, 1)
      {:reductions, after_count} = Process.info(reader.pid, :reductions)

      assert after_count - before <= 5_000 + elapsed * 250,
             "blocked reader consumed #{after_count - before} reductions in #{elapsed}ms"

      assert Task.yield(reader, 0) == nil
      PublicationEpoch.end_write(token)
      assert Task.await(reader) == :stable
    after
      PublicationEpoch.end_write(token)
      Task.shutdown(reader, :brutal_kill)
    end
  end

  test "closing an old token twice does not expose a newer partially published write" do
    latch = :ets.new(:publication_epoch_repeated_close, [:set, :public])
    rows = :ets.new(:publication_epoch_rows, [:set, :public])
    ctx = %{publication_epoch: :atomics.new(1, signed: false), latch_refs: {latch}}
    :ets.insert(rows, [{:first, :old}, {:second, :old}])

    previous = PublicationEpoch.begin_write(ctx, 0)
    :ok = PublicationEpoch.end_write(previous)
    current = PublicationEpoch.begin_write(ctx, 0)
    :ets.insert(rows, {:first, :new})
    :ok = PublicationEpoch.end_write_if_open(previous)

    parent = self()

    reader =
      Task.async(fn ->
        send(parent, :publication_reader_started)

        PublicationEpoch.read(ctx, [0], fn ->
          {:ets.lookup_element(rows, :first, 2), :ets.lookup_element(rows, :second, 2)}
        end)
      end)

    try do
      assert_receive :publication_reader_started
      assert Task.yield(reader, 50) == nil
      :ets.insert(rows, {:second, :new})
      :ok = PublicationEpoch.end_write(current)
      assert Task.await(reader) == {:new, :new}
    after
      PublicationEpoch.end_write(current)
      Task.shutdown(reader, :brutal_kill)
    end
  end

  test "a queued publisher does not busy-spin while another writer owns the latch" do
    latch = :ets.new(:publication_epoch_writer_wait_budget, [:set, :public])
    ctx = %{publication_epoch: :atomics.new(1, signed: false), latch_refs: {latch}}
    token = PublicationEpoch.begin_write(ctx, 0)
    parent = self()

    writer =
      Task.async(fn ->
        send(parent, :queued_writer)
        next = PublicationEpoch.begin_write(ctx, 0)
        PublicationEpoch.end_write(next)
      end)

    try do
      assert_receive :queued_writer
      started = System.monotonic_time(:millisecond)
      {:reductions, before} = Process.info(writer.pid, :reductions)
      Process.sleep(50)
      elapsed = max(System.monotonic_time(:millisecond) - started, 1)
      {:reductions, after_count} = Process.info(writer.pid, :reductions)

      assert after_count - before <= 5_000 + elapsed * 250,
             "queued publisher consumed #{after_count - before} reductions in #{elapsed}ms"

      assert Task.yield(writer, 0) == nil
      PublicationEpoch.end_write(token)
      assert Task.await(writer) == :ok
    after
      PublicationEpoch.end_write(token)
      Task.shutdown(writer, :brutal_kill)
    end
  end

  test "a reader repairs an epoch abandoned by a killed writer" do
    latch = :ets.new(:publication_epoch_latch, [:set, :public])

    ctx = %{
      publication_epoch: :atomics.new(1, signed: false),
      latch_refs: {latch}
    }

    parent = self()

    writer =
      spawn(fn ->
        _token = PublicationEpoch.begin_write(ctx, 0)
        send(parent, {:writer_open, self()})
        Process.sleep(:infinity)
      end)

    assert_receive {:writer_open, ^writer}, 1_000
    monitor = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^writer, :killed}, 1_000

    reader = Task.async(fn -> PublicationEpoch.read(ctx, [0], fn -> :consistent end) end)

    assert Task.await(reader, 1_000) == :consistent
    assert :atomics.get(ctx.publication_epoch, 1) == 2
  end

  test "a reader repairs an orphaned odd epoch with a missing latch entry" do
    latch = :ets.new(:publication_epoch_missing_latch, [:set, :public])
    epoch = :atomics.new(1, signed: false)
    :atomics.put(epoch, 1, 1)

    ctx = %{
      publication_epoch: epoch,
      latch_refs: {latch}
    }

    reader = Task.async(fn -> PublicationEpoch.read(ctx, [0], fn -> :consistent end) end)

    assert Task.await(reader, 1_000) == :consistent
    assert :atomics.get(epoch, 1) == 2
  end
end
