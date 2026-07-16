defmodule Ferricstore.Store.PublicationEpochTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.PublicationEpoch

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
end
