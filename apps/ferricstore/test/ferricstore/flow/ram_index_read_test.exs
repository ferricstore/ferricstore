defmodule Ferricstore.Flow.RAMIndexReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.RAMIndexRead

  defmodule UnavailableShard do
    use GenServer

    def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok)
    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:flow_index_rank_range, _key, _start, _stop, _reverse?}, _from, state),
      do: {:reply, :unavailable, state}

    def handle_call(
          {:flow_index_score_range_slice, _key, _min, _max, _reverse?, _offset, _count},
          _from,
          state
        ),
        do: {:reply, :unavailable, state}
  end

  test "bounds map nil to open score ranges and integers to inclusive ranges" do
    assert RAMIndexRead.min_bound(nil) == :neg_inf
    assert RAMIndexRead.max_bound(nil) == :pos_inf
    assert RAMIndexRead.min_bound(10) == {:inclusive, 10}
    assert RAMIndexRead.max_bound(20) == {:inclusive, 20}

    assert RAMIndexRead.max_bound(%{
             rev?: true,
             to_ms: 20,
             before_id: "flow-050"
           }) == {:cursor_before, 20, "flow-050"}
  end

  test "reverse helpers preserve previous query behavior" do
    assert RAMIndexRead.reverse?(%{rev?: true})
    refute RAMIndexRead.reverse?(%{rev?: false})
    refute RAMIndexRead.reverse?(nil)

    assert RAMIndexRead.maybe_reverse([1, 2, 3], true) == [3, 2, 1]
    assert RAMIndexRead.maybe_reverse([1, 2, 3], false) == [1, 2, 3]
  end

  test "FLOW.STUCK threads its count into the ordered index instead of requesting all rows" do
    assert Code.ensure_loaded?(Ferricstore.Flow.IndexZSet)
    assert function_exported?(Ferricstore.Flow.IndexZSet, :range_by_score, 5)
    refute function_exported?(Ferricstore.Flow.IndexZSet, :range_by_score, 4)

    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/index_zset.ex", __DIR__))

    refute source =~ ":all"
  end

  test "RAM index reads fail closed when the shard is unavailable" do
    shard = start_supervised!(UnavailableShard)

    ctx = %FerricStore.Instance{
      name: :ram_index_read_unavailable_test,
      data_dir: System.tmp_dir!(),
      shard_count: 1,
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple(),
      shard_names: {shard}
    }

    assert {:error, :flow_index_unavailable} = RAMIndexRead.rank_entries(ctx, "index", 1)

    assert {:error, :flow_index_unavailable} =
             RAMIndexRead.score_entries(
               ctx,
               "index",
               %{from_ms: nil, to_ms: nil, rev?: false},
               1
             )
  end
end
