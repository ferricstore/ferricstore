defmodule Ferricstore.FlowPolicyGenerationTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.{Keys, PolicyCommand, RetryPolicy}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    assert :ok = ShardHelpers.flush_all_keys()
    :ok
  end

  test "policy generations are always allocated and advance monotonically" do
    ctx = FerricStore.Instance.get(:default)
    type = "generation-always-on-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{generation: 1}} = FerricStore.flow_policy_set(type, max_active_ms: 1_000)
    assert_policy_generation_on_all_shards(ctx, type, 1)

    assert {:ok, %{generation: 2}} = FerricStore.flow_policy_set(type, max_active_ms: 2_000)
    assert_policy_generation_on_all_shards(ctx, type, 2)

    assert {:ok, %{generation: 2, max_active_ms: 2_000}} =
             FerricStore.flow_policy_get(type)
  end

  test "policy set rejects a stale expected generation without changing the policy" do
    type = "generation-cas-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{generation: 1}} =
             FerricStore.flow_policy_set(type, max_active_ms: 1_000)

    assert {:ok, %{generation: 2}} =
             FerricStore.flow_policy_set(type,
               expected_generation: 1,
               max_active_ms: 2_000
             )

    assert {:error, "ERR stale flow policy generation"} =
             FerricStore.flow_policy_set(type,
               expected_generation: 1,
               max_active_ms: 3_000
             )

    assert {:ok, %{generation: 2, max_active_ms: 2_000}} =
             FerricStore.flow_policy_get(type)
  end

  test "policy set uses the allocator shard generation when a routed replica is stale" do
    ctx = FerricStore.Instance.get(:default)
    type = type_routed_away_from_allocator(ctx, "generation-primary-read")
    key = Keys.policy_key(type)
    routed_shard = Router.shard_for(ctx, key)

    refute routed_shard == 0

    assert {:ok, %{generation: 1}} =
             FerricStore.flow_policy_set(type, max_active_ms: 1_000)

    assert :ok = Ferricstore.Raft.Backend.write(routed_shard, {:delete, key})
    assert {:ok, nil} = Router.read_shard_value(ctx, routed_shard, key)

    assert {:ok, %{generation: 2, max_active_ms: 2_000}} =
             FerricStore.flow_policy_set(type, max_active_ms: 2_000)

    assert_policy_generation_on_all_shards(ctx, type, 2)
  end

  test "policy reads use the allocator shard when a routed replica is stale" do
    ctx = FerricStore.Instance.get(:default)
    type = type_routed_away_from_allocator(ctx, "generation-canonical-get")
    key = Keys.policy_key(type)
    routed_shard = Router.shard_for(ctx, key)

    assert {:ok, %{generation: 1, max_active_ms: 1_000}} =
             FerricStore.flow_policy_set(type, max_active_ms: 1_000)

    assert :ok = Ferricstore.Raft.Backend.write(routed_shard, {:delete, key})
    assert {:ok, nil} = Router.read_shard_value(ctx, routed_shard, key)

    assert {:ok, %{generation: 1, max_active_ms: 1_000}} =
             FerricStore.flow_policy_get(type)
  end

  test "policy reads reject a value stored under a different type key" do
    type = "generation-mismatched-read-#{System.unique_integer([:positive, :monotonic])}"
    key = Keys.policy_key(type)

    {:ok, foreign_policy} =
      RetryPolicy.normalize_flow_policy("foreign-policy-type", max_active_ms: 9_000)

    value = RetryPolicy.encode_flow_policy(foreign_policy, 1)

    assert :ok =
             Ferricstore.Raft.Backend.write(
               0,
               {:put, key, value, 0}
             )

    assert {:error, "ERR flow policy is corrupt"} = FerricStore.flow_policy_get(type)
  end

  test "missing existing-flow targets are stamped with an absence guard" do
    ctx = FerricStore.Instance.get(:default)
    partition_key = "missing-policy-target-tenant"
    id = "missing-policy-target-#{System.unique_integer([:positive, :monotonic])}"
    state_key = Keys.state_key(id, partition_key)

    assert {:ok, {:flow_retry, ^state_key, stamped}} =
             PolicyCommand.stamp(
               ctx,
               {:flow_retry, state_key, %{id: id, partition_key: partition_key}}
             )

    assert stamped.policy_reference_captured
    assert stamped.policy_guard == %{state_key: state_key, absent: true}
  end

  test "existing-flow targets retain both their policy reference and incarnation guard" do
    ctx = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive, :monotonic])
    type = "generation-existing-target-#{suffix}"
    partition_key = "generation-existing-target-tenant-#{suffix}"
    id = "generation-existing-target-flow-#{suffix}"
    state_key = Keys.state_key(id, partition_key)

    assert {:ok, %{generation: 1}} =
             FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               payload: "payload",
               now_ms: 1_000,
               run_at_ms: 1_000
             )

    assert {:ok, {:flow_policy_fence, [_install], {:flow_retry, ^state_key, stamped}}} =
             PolicyCommand.stamp(
               ctx,
               {:flow_retry, state_key, %{id: id, partition_key: partition_key}}
             )

    assert %{type: ^type, generation: 1} = stamped.policy_ref

    assert %{
             type: ^type,
             state_key: ^state_key,
             incarnation: incarnation
           } = stamped.policy_guard

    assert is_integer(incarnation) and incarnation >= 0
  end

  test "policy set merges independent type and state patches" do
    type = "generation-merge-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{generation: 1}} =
             FerricStore.flow_policy_set(type,
               max_active_ms: 1_000,
               states: %{
                 "queued" => [mode: :fifo, retry: [max_retries: 7]]
               }
             )

    assert {:ok, %{generation: 2}} =
             FerricStore.flow_policy_set(type,
               states: %{
                 "queued" => [retry: [exhausted_to: "dead"]],
                 "review" => [mode: :fifo]
               }
             )

    assert {:ok,
            %{
              generation: 2,
              max_active_ms: 1_000,
              states: %{
                "queued" => %{
                  mode: :fifo,
                  retry: %{max_retries: 7, exhausted_to: "dead"}
                },
                "review" => %{mode: :fifo}
              }
            }} = FerricStore.flow_policy_get(type)
  end

  test "string-key state patches override the equivalent normalized setting" do
    type = "generation-string-patch-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{generation: 1}} =
             FerricStore.flow_policy_set(type,
               states: %{"queued" => [mode: :fifo, retry: [max_retries: 7]]}
             )

    assert {:ok, %{generation: 2}} =
             FerricStore.flow_policy_set(type,
               states: %{
                 "queued" => %{
                   "mode" => "parallel",
                   "retry" => %{"max_retries" => 9}
                 }
               }
             )

    assert {:ok,
            %{
              states: %{
                "queued" => %{mode: :parallel, retry: %{max_retries: 9}}
              }
            }} = FerricStore.flow_policy_get(type)
  end

  test "native state-pair patches merge with existing states" do
    type = "generation-native-state-patch-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{generation: 1}} =
             FerricStore.flow_policy_set(type,
               states: %{"queued" => [mode: :fifo]}
             )

    assert {:ok, %{generation: 2}} =
             FerricStore.flow_policy_set(type,
               states: [{"review", [mode: :fifo]}]
             )

    assert {:ok,
            %{
              states: %{
                "queued" => %{mode: :fifo},
                "review" => %{mode: :fifo}
              }
            }} = FerricStore.flow_policy_get(type)
  end

  test "policy set validates the optional response state" do
    type = "generation-state-validation-#{System.unique_integer([:positive, :monotonic])}"

    assert {:error, "ERR flow state must be a string"} =
             FerricStore.flow_policy_set(type, state: 1)
  end

  test "concurrent policy patches preserve both writers" do
    type = "generation-concurrent-#{System.unique_integer([:positive, :monotonic])}"
    parent = self()

    writers =
      for {state, mode} <- [{"queued", :fifo}, {"review", :parallel}] do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :write ->
              FerricStore.flow_policy_set(type, states: %{state => [mode: mode]})
          end
        end)
      end

    for writer <- writers do
      assert_receive {:ready, pid} when pid == writer.pid
    end

    Enum.each(writers, &send(&1.pid, :write))

    assert Enum.all?(Task.await_many(writers, 15_000), &match?({:ok, _policy}, &1))

    assert {:ok,
            %{
              generation: 2,
              states: %{
                "queued" => %{mode: :fifo},
                "review" => %{mode: :parallel}
              }
            }} = FerricStore.flow_policy_get(type)
  end

  test "a burst of concurrent policy patches preserves every writer" do
    type = "generation-concurrent-burst-#{System.unique_integer([:positive, :monotonic])}"
    parent = self()

    patches =
      for index <- 1..12 do
        state = "state-#{index}"
        mode = if rem(index, 2) == 0, do: :fifo, else: :parallel
        {state, mode}
      end

    writers =
      Enum.map(patches, fn {state, mode} ->
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :write ->
              FerricStore.flow_policy_set(type, states: %{state => [mode: mode]})
          end
        end)
      end)

    for writer <- writers do
      assert_receive {:ready, pid} when pid == writer.pid
    end

    Enum.each(writers, &send(&1.pid, :write))

    results = Task.await_many(writers, 30_000)
    assert Enum.all?(results, &match?({:ok, _policy}, &1)), inspect(results)

    assert {:ok, %{generation: 12, states: states}} = FerricStore.flow_policy_get(type)
    assert map_size(states) == length(patches)

    Enum.each(patches, fn {state, mode} ->
      assert %{mode: ^mode} = Map.fetch!(states, state)
    end)
  end

  test "replace resets fields omitted from an explicit full policy snapshot" do
    type = "generation-replace-#{System.unique_integer([:positive, :monotonic])}"

    assert {:ok, %{generation: 1}} =
             FerricStore.flow_policy_set(type,
               max_active_ms: 1_000,
               states: %{"queued" => [mode: :fifo]}
             )

    assert {:ok, %{generation: 2, max_active_ms: nil, states: %{}}} =
             FerricStore.flow_policy_set(type, replace: true)
  end

  defp assert_policy_generation_on_all_shards(ctx, type, expected_generation) do
    key = Keys.policy_key(type)

    for shard_index <- 0..(ctx.shard_count - 1) do
      assert {:ok, value} = Router.read_shard_value(ctx, shard_index, key)
      assert {:ok, {^expected_generation, policy}} = RetryPolicy.decode_flow_policy_entry(value)
      assert policy.type == type
    end
  end

  defp type_routed_away_from_allocator(ctx, prefix) do
    unique = System.unique_integer([:positive, :monotonic])

    Enum.find_value(0..1_024, fn suffix ->
      type = "#{prefix}-#{unique}-#{suffix}"

      if Router.shard_for(ctx, Keys.policy_key(type)) != 0, do: type
    end) || raise "could not find a Flow policy key routed away from shard 0"
  end
end
