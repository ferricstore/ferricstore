defmodule Ferricstore.Store.RouterBatchResultCardinalityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Router

  defmodule BatchReadReply do
    use GenServer

    def start_link(replies), do: GenServer.start_link(__MODULE__, replies)
    def init(replies), do: {:ok, replies}

    def handle_call(request, _from, replies) do
      {:reply, Map.fetch!(replies, elem(request, 0)), replies}
    end
  end

  test "exact batch results preserve per-command outcomes and count possible writes" do
    assert {[:ok, {:error, :rejected}, {:ok, 2}], 2} =
             Router.__normalize_batch_write_result_for_test__(
               {:ok, [:ok, {:error, :rejected}, {:ok, 2}]},
               3
             )
  end

  test "short and long batch results fail every slot closed as an unknown outcome" do
    unknown = ErrorReasons.write_timeout_unknown()

    assert {[^unknown, ^unknown], 2} =
             Router.__normalize_batch_write_result_for_test__({:ok, [:ok]}, 2)

    assert {[^unknown, ^unknown], 2} =
             Router.__normalize_batch_write_result_for_test__({:ok, [:ok, :ok, :ok]}, 2)
  end

  test "an explicit unknown outcome advances the possible-write count" do
    unknown = ErrorReasons.write_timeout_unknown()

    assert {[^unknown, ^unknown, ^unknown], 3} =
             Router.__normalize_batch_write_result_for_test__(unknown, 3)
  end

  test "compound batch write replies require exact cardinality" do
    unknown = ErrorReasons.write_timeout_unknown()

    assert :ok = Router.__normalize_compound_batch_write_result_for_test__([:ok, :ok], 2)

    assert {:error, :rejected} =
             Router.__normalize_compound_batch_write_result_for_test__(
               {:ok, [:ok, {:error, :rejected}]},
               2
             )

    assert ^unknown =
             Router.__normalize_compound_batch_write_result_for_test__({:ok, [:ok]}, 2)

    assert ^unknown =
             Router.__normalize_compound_batch_write_result_for_test__([:ok, :ok, :ok], 2)
  end

  test "shard batch readers propagate per-entry storage failures" do
    failure = ReadResult.failure(:segment_unavailable)

    server =
      start_supervised!({BatchReadReply, %{get_many: [failure], get_many_entries: [failure]}})

    ctx = %{name: :batch_read_failure_test, data_dir: "unused", shard_names: {server}}

    assert ^failure = Router.read_shard_values(ctx, 0, ["key"])
    assert ^failure = Router.read_shard_entries(ctx, 0, ["key"])
  end

  test "shard batch readers collapse per-entry deadline markers to unavailable" do
    server =
      start_supervised!(
        {BatchReadReply, %{get_many: [:unavailable], get_many_entries: [:unavailable]}}
      )

    ctx = %{name: :batch_read_timeout_test, data_dir: "unused", shard_names: {server}}

    assert :unavailable = Router.read_shard_values(ctx, 0, ["key"])
    assert :unavailable = Router.read_shard_entries(ctx, 0, ["key"])
  end
end
