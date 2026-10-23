defmodule Ferricstore.Flow.SignalIdempotencyLevelTest do
  @moduledoc """
  From feature level 5 a signal retried with the same idempotency key and the
  same content is applied once: the retry answers OK and changes nothing.
  Older releases apply it again (a new version and history event, or a state
  mismatch for a signal that already moved the Flow).
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    assert {:ok, %{active: 5}} = FeatureLevel.raise(FerricStore.Instance.get(:default), 5)
    :ok
  end

  test "a retried signal is applied once" do
    id = waiting_flow()

    opts = [
      partition_key: "tenant-idem",
      signal: "paid",
      idempotency_key: "evt-1",
      values: %{"amount" => "10"},
      now_ms: 1_100
    ]

    assert :ok = FerricStore.flow_signal(id, opts)
    assert {:ok, %{version: 2}} = FerricStore.flow_get(id, partition_key: "tenant-idem")

    assert :ok = FerricStore.flow_signal(id, Keyword.put(opts, :now_ms, 1_200))
    assert {:ok, %{version: 2}} = FerricStore.flow_get(id, partition_key: "tenant-idem")
    assert {:ok, [_created, _signaled]} = history(id)

    assert {:error, "ERR flow signal idempotency conflict"} =
             FerricStore.flow_signal(id, Keyword.put(opts, :values, %{"amount" => "11"}))
  end

  test "a retried transitioning signal answers OK after the Flow moved" do
    id = waiting_flow()

    opts = [
      partition_key: "tenant-idem",
      signal: "paid",
      idempotency_key: "evt-2",
      if_state: "waiting",
      transition_to: "verify",
      now_ms: 1_100
    ]

    assert :ok = FerricStore.flow_signal(id, opts)
    assert :ok = FerricStore.flow_signal(id, Keyword.put(opts, :now_ms, 1_200))

    assert {:ok, %{state: "verify", version: 2}} =
             FerricStore.flow_get(id, partition_key: "tenant-idem")
  end

  defp waiting_flow do
    id = "signal-idem-#{System.unique_integer([:positive])}"

    assert :ok =
             FerricStore.flow_create(id,
               type: "signal-idem",
               partition_key: "tenant-idem",
               state: "waiting",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    id
  end

  defp history(id), do: FerricStore.flow_history(id, partition_key: "tenant-idem", count: 10)
end
