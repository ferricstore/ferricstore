defmodule Ferricstore.Flow.SignalDigestLevelTest do
  @moduledoc """
  A signal's idempotency digest is stored and compared on every replica. From
  feature level 5 it is a `Ferricstore.CanonicalTerm` digest, which no OTP
  release changes; digests stored before still match their retries.
  """
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.CanonicalTerm
  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, isolated: isolated}
  end

  test "digests stored below level 5 still match their retries at level 5" do
    ctx = FerricStore.Instance.get(:default)
    assert {:ok, %{active: active}} = FeatureLevel.status(ctx)
    assert active < 5

    id = "signal-digest-#{System.unique_integer([:positive])}"
    assert :ok = create(id)
    legacy = signal_opts("evt-legacy", %{"payment" => "v1"})
    assert :ok = FerricStore.flow_signal(id, legacy)
    refute digest_row(ctx, id, "evt-legacy") =~ "c1:"

    legacy_parent = "group-digest-#{System.unique_integer([:positive])}"
    spawn_legacy = claimed_parent(legacy_parent, "legacy-child")
    assert :ok = spawn_legacy.()
    refute request_hash(legacy_parent) =~ "c1:"

    assert {:ok, %{active: 5}} = FeatureLevel.raise(ctx, 5)

    assert :ok = FerricStore.flow_signal(id, legacy)
    assert :ok = spawn_legacy.()

    assert {:error, "ERR flow signal idempotency conflict"} =
             FerricStore.flow_signal(
               id,
               signal_opts("evt-legacy", %{"payment" => "v2"}, override_values: ["payment"])
             )

    values = Map.new(1..40, &{"value-#{&1}", "v#{&1}"})
    current = signal_opts("evt-current", values)
    assert :ok = FerricStore.flow_signal(id, current)

    assert digest_row(ctx, id, "evt-current") ==
             "c1:" <>
               Base.encode16(
                 CanonicalTerm.digest(%{
                   signal: "paid",
                   if_state: nil,
                   transition_to: nil,
                   run_at_ms: nil,
                   values: values,
                   value_refs: nil,
                   drop_values: nil,
                   override_values: nil
                 }),
                 case: :lower
               )

    assert :ok = FerricStore.flow_signal(id, current)

    current_parent = "group-digest-#{System.unique_integer([:positive])}"
    spawn_current = claimed_parent(current_parent, "current-child")
    assert :ok = spawn_current.()
    assert "c1:" <> _hex = request_hash(current_parent)
    assert :ok = spawn_current.()
  end

  defp request_hash(parent) do
    assert {:ok, record} = FerricStore.flow_get(parent, partition_key: "tenant-digest")
    get_in(record, [:child_groups, "group", "request_hash"])
  end

  defp claimed_parent(parent, child) do
    assert :ok =
             FerricStore.flow_create(parent,
               type: "group-digest-parent",
               partition_key: "tenant-digest",
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due("group-digest-parent",
               partition_key: "tenant-digest",
               worker: "group-digest-worker",
               limit: 1,
               now_ms: 1_000
             )

    spawn = fn ->
      FerricStore.flow_spawn_children(
        parent,
        [%{id: child <> "-" <> parent, type: "group-digest-child"}],
        partition_key: "tenant-digest",
        lease_token: claimed.lease_token,
        fencing_token: claimed.fencing_token,
        group_id: "group",
        wait_state: "waiting",
        success: "completed",
        failure: "failed",
        now_ms: 1_100
      )
    end

    spawn
  end

  defp create(id) do
    FerricStore.flow_create(id,
      type: "signal-digest",
      partition_key: "tenant-digest",
      state: "waiting",
      run_at_ms: 1_000,
      now_ms: 1_000
    )
  end

  defp signal_opts(key, values, extra \\ []) do
    [
      partition_key: "tenant-digest",
      signal: "paid",
      idempotency_key: key,
      values: values,
      now_ms: 1_100
    ] ++ extra
  end

  defp digest_row(ctx, id, key),
    do: Router.get(ctx, Keys.signal_idempotency_key(id, key, "tenant-digest"))
end
