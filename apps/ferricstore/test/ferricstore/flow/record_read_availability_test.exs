defmodule Ferricstore.Flow.RecordReadAvailabilityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.RecordRead

  defmodule UnavailableIndexShard do
    use GenServer

    def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok)
    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:flow_index_count_all_many, keys}, _from, state) do
      counts = [1 | List.duplicate(0, length(keys) - 1)]
      {:reply, {:ok, counts}, state}
    end

    def handle_call({:flow_index_rank_range_many, _requests}, _from, state),
      do: {:reply, :unavailable, state}
  end

  defmodule ShortCountIndexShard do
    use GenServer

    def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok)
    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:flow_index_count_all_many, _keys}, _from, state),
      do: {:reply, {:ok, []}, state}
  end

  defmodule ShortRankIndexShard do
    use GenServer

    def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok)
    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:flow_index_count_all_many, keys}, _from, state) do
      {:reply, {:ok, [1 | List.duplicate(0, length(keys) - 1)]}, state}
    end

    def handle_call({:flow_index_rank_range_many, _requests}, _from, state),
      do: {:reply, {:ok, []}, state}
  end

  test "auto hot reads fail closed when a counted index becomes unavailable" do
    ctx = context(start_supervised!(UnavailableIndexShard))

    assert {:error, :flow_index_unavailable} =
             list_one_auto_hot(ctx)
  end

  test "auto hot reads reject a short count result vector" do
    ctx = context(start_supervised!(ShortCountIndexShard))

    assert {:error, :flow_index_unavailable} = list_one_auto_hot(ctx)
  end

  test "auto hot reads reject a short rank result vector without retrying forever" do
    ctx = context(start_supervised!(ShortRankIndexShard))
    task = Task.async(fn -> list_one_auto_hot(ctx) end)

    result = Task.yield(task, 200) || Task.shutdown(task, :brutal_kill)

    assert {:ok, {:error, :flow_index_unavailable}} = result
  end

  defp context(shard_pid) do
    %FerricStore.Instance{
      name: :record_read_availability_test,
      data_dir: System.tmp_dir!(),
      shard_count: 1,
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple(),
      shard_names: {shard_pid}
    }
  end

  defp list_one_auto_hot(ctx) do
    RecordRead.list_records(
      ctx,
      "jobs",
      "queued",
      :auto,
      1,
      %{
        from_ms: nil,
        to_ms: nil,
        rev?: false,
        before_id: nil,
        terminal_only?: false
      },
      false,
      false,
      ["completed", "failed"],
      100
    )
  end
end
