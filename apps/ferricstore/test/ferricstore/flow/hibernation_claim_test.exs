defmodule Ferricstore.Flow.HibernationClaimTest do
  @moduledoc """
  A Flow due more than the hot window away is hibernated: its record leaves
  memory for the LMDB mirror. A claim that finds too few hot Flows promotes the
  due cold ones and claims them. From feature level 7 that includes a claim
  without a partition key, which takes the automatic partitions.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @hot_window_ms 1_000

  setup_all do
    previous = Application.fetch_env(:ferricstore, :flow_hibernation_hot_window_ms)
    Application.put_env(:ferricstore, :flow_hibernation_hot_window_ms, @hot_window_ms)
    # The apply context reads the window when the instance starts, and feature
    # levels survive FLUSHDB, so both need a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    assert {:ok, %{active: 7}} = FerricStore.raise_cluster_feature_level(7)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, :flow_hibernation_hot_window_ms, value)
        :error -> Application.delete_env(:ferricstore, :flow_hibernation_hot_window_ms)
      end

      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    :ok
  end

  setup do
    Ferricstore.Flow.Admission.clear_create_pause()
    %{ctx: FerricStore.Instance.get(:default), unique: System.unique_integer([:positive])}
  end

  test "a default claim takes a due hibernated Flow created without a partition key",
       %{ctx: ctx, unique: unique} do
    type = "hibernated-auto-#{unique}"
    id = "flow-#{unique}"
    now_ms = System.system_time(:millisecond)
    due_ms = now_ms + 3 * @hot_window_ms

    assert :ok = FerricStore.flow_create(id, type: type, run_at_ms: due_ms, now_ms: now_ms)
    await_hibernated!(ctx, Keys.state_key(id, nil))

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type, worker: "w", limit: 1, now_ms: due_ms + 1)
  end

  test "a claim for its partition takes a due hibernated Flow created with a partition key",
       %{ctx: ctx, unique: unique} do
    type = "hibernated-partition-#{unique}"
    id = "flow-#{unique}"
    partition = "p-#{unique}"
    now_ms = System.system_time(:millisecond)
    due_ms = now_ms + 3 * @hot_window_ms

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               run_at_ms: due_ms,
               now_ms: now_ms
             )

    await_hibernated!(ctx, Keys.state_key(id, partition))

    assert {:ok, [%{id: ^id, state: "running"}]} =
             FerricStore.flow_claim_due(type,
               worker: "w",
               limit: 1,
               partition_key: partition,
               now_ms: due_ms + 1
             )
  end

  # Hibernation evicts the hot record once the LMDB writer has flushed its
  # cold copy.
  defp await_hibernated!(ctx, state_key) do
    shard = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 10_000)
                 not :ets.member(keydir, state_key)
               end,
               "the Flow should be hibernated",
               100,
               50
             )
  end
end

defmodule Ferricstore.Flow.HibernationClaimBelowLevel7Test do
  @moduledoc """
  Below feature level 7 a claim without a partition key leaves hibernated
  Flows alone, as older releases apply it, so replicas and replays agree.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @hot_window_ms 1_000

  setup_all do
    previous = Application.fetch_env(:ferricstore, :flow_hibernation_hot_window_ms)
    Application.put_env(:ferricstore, :flow_hibernation_hot_window_ms, @hot_window_ms)
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, :flow_hibernation_hot_window_ms, value)
        :error -> Application.delete_env(:ferricstore, :flow_hibernation_hot_window_ms)
      end

      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    :ok
  end

  test "a default claim leaves a due hibernated Flow in an automatic partition alone" do
    Ferricstore.Flow.Admission.clear_create_pause()
    ctx = FerricStore.Instance.get(:default)
    unique = System.unique_integer([:positive])
    type = "hibernated-level-6-#{unique}"
    id = "flow-#{unique}"
    now_ms = System.system_time(:millisecond)
    due_ms = now_ms + 3 * @hot_window_ms

    assert :ok = FerricStore.flow_create(id, type: type, run_at_ms: due_ms, now_ms: now_ms)

    state_key = Keys.state_key(id, nil)
    keydir = elem(ctx.keydir_refs, Router.shard_for(ctx, state_key))

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 10_000)
                 not :ets.member(keydir, state_key)
               end,
               "the Flow should be hibernated",
               100,
               50
             )

    assert {:ok, []} =
             FerricStore.flow_claim_due(type, worker: "w", limit: 1, now_ms: due_ms + 1)
  end
end
