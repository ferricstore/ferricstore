defmodule Ferricstore.Flow.PolicyCommandTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.PolicyCommand

  test "ordinary KV commands bypass policy stamping unchanged" do
    set = {:set, "key", "value", 0, %{}}
    delete = {:delete, "key"}

    assert {:ok, ^set} = PolicyCommand.stamp(:context_must_not_be_read, set)
    assert {:ok, ^delete} = PolicyCommand.stamp(:context_must_not_be_read, delete)
  end

  test "signal transitions are policy-sensitive commands" do
    assert PolicyCommand.requires_stamp?({:flow_signal, "state-key", %{}})
    assert PolicyCommand.requires_stamp?({:flow_signal_many, "state-key", %{records: []}})
  end

  test "ordinary KV batches return the original list without consulting context" do
    commands =
      Enum.map(1..10_000, fn index ->
        key = "key-#{index}"
        {key, {:set, key, "value", 0, %{}}}
      end)

    assert {:ok, stamped} = PolicyCommand.stamp_many(:context_must_not_be_read, commands)
    assert :erts_debug.same(commands, stamped)
  end

  test "malformed policy-sensitive commands fail before policy reads" do
    assert {:error, "ERR flow policy-sensitive command attrs must be a map"} =
             PolicyCommand.stamp(:context_must_not_be_read, {:flow_create, "state-key", :invalid})

    assert {:error, "ERR flow command must be a tuple"} =
             PolicyCommand.stamp(:context_must_not_be_read, :invalid)
  end

  test "malformed keyed command batches return errors instead of raising" do
    error = {:error, "ERR flow keyed command must be a {binary_key, tuple_command} pair"}

    assert ^error = PolicyCommand.stamp_many(:context_must_not_be_read, [:invalid])
    assert ^error = PolicyCommand.stamp_many(:context_must_not_be_read, [{"key", :invalid}])
    assert ^error = PolicyCommand.stamp_many(:context_must_not_be_read, [{:invalid, {:set, "k"}}])
  end

  test "existing-flow policy stamping fails closed while the state shard is unavailable" do
    keydir = :ets.new(:unavailable_policy_stamp, [:set, :public])
    data_dir = Path.join(System.tmp_dir!(), "policy-stamp-#{System.unique_integer([:positive])}")
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)
    on_exit(fn -> File.rm_rf(data_dir) end)

    ctx = %{
      data_dir: data_dir,
      keydir_refs: {keydir},
      name: :policy_stamp_unavailable_test,
      shard_names: {:policy_stamp_unavailable_shard_process},
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple()
    }

    partition_key = "tenant"
    id = "existing-flow"
    key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    command = {:flow_retry, key, %{id: id, partition_key: partition_key}}

    assert {:error, "ERR flow state shard not available"} =
             PolicyCommand.stamp(ctx, command)
  end

  test "existing hot flows are stamped without a shard process round trip" do
    keydir = :ets.new(:hot_policy_stamp, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    ctx = %{
      keydir_refs: {keydir},
      name: :hot_policy_stamp_test,
      shard_names: {:policy_stamp_hot_shard_process_must_not_exist},
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple()
    }

    partition_key = "tenant"
    id = "hot-flow"
    type = "hot-policy"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    policy_key = Ferricstore.Flow.Keys.policy_key(type)
    policy = %{type: type, version: "public-v1"}

    state_value =
      Ferricstore.Flow.encode_record(%{
        id: id,
        type: type,
        state: "queued",
        version: 1,
        attempts: 0,
        fencing_token: 0,
        created_at_ms: 1,
        updated_at_ms: 1,
        next_run_at_ms: nil,
        priority: 0,
        incarnation: 1,
        state_enter_seq: 1,
        partition_key: partition_key,
        root_flow_id: id
      })

    policy_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 7)

    for {key, value} <- [{state_key, state_value}, {policy_key, policy_value}] do
      :ets.insert(
        keydir,
        {key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
      )
    end

    command = {:flow_retry, state_key, %{id: id, partition_key: partition_key}}

    assert {:ok, fenced} = PolicyCommand.stamp(ctx, command)
    assert {[_install], {:flow_retry, ^state_key, stamped}} = unwrap_policy_fence(fenced)
    assert %{type: ^type, generation: 7, digest: digest} = stamped.policy_ref
    assert byte_size(digest) == 32
    assert stamped.policy_guard.incarnation == 1
  end

  test "single commands fence their compact reference with the exact coordinator policy" do
    keydir = :ets.new(:policy_fence_stamp, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    type = "fenced-policy"
    partition_key = "fenced-tenant"
    id = "fenced-flow"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    policy_key = Ferricstore.Flow.Keys.policy_key(type)
    policy = %{type: type, max_active_ms: 7_000}
    policy_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 4)

    :ets.insert(
      keydir,
      {policy_key, policy_value, 0, Ferricstore.Store.LFU.initial(), 0, 0,
       byte_size(policy_value)}
    )

    ctx = %{
      keydir_refs: {keydir},
      name: :policy_fence_stamp_test,
      shard_names: {:policy_fence_stamp_shard_must_not_run},
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple()
    }

    command =
      {:flow_create, state_key,
       %{id: id, type: type, state: "queued", partition_key: partition_key}}

    assert {:ok,
            {:flow_policy_fence, [{^policy_key, ^policy_value, 0}],
             {:flow_create, ^state_key, stamped}}} = PolicyCommand.stamp(ctx, command)

    assert %{type: ^type, generation: 4, digest: <<_::256>>} = stamped.policy_ref
    refute Map.has_key?(stamped.policy_ref, :encoded)
  end

  @tag :compact_policy_ref
  test "policy-sensitive commands carry a fixed-size reference and one exact fence" do
    keydir = :ets.new(:compact_policy_ref, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    type = "compact-policy-ref"
    policy = %{type: type, version: String.duplicate("v", 128 * 1024)}
    encoded = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 17)

    :ets.insert(
      keydir,
      {Ferricstore.Flow.Keys.policy_key(type), encoded, 0, Ferricstore.Store.LFU.initial(), 0, 0,
       byte_size(encoded)}
    )

    ctx = %{
      keydir_refs: {keydir},
      shard_names: {:policy_stamp_shard_process_must_not_exist}
    }

    key = Ferricstore.Flow.Keys.state_key("compact-policy-flow", "tenant")

    command =
      {:flow_create, key,
       %{id: "compact-policy-flow", type: type, state: "queued", partition_key: "tenant"}}

    assert {:ok, fenced} = PolicyCommand.stamp(ctx, command)

    assert {[{_policy_key, ^encoded, 0}], {:flow_create, ^key, stamped}} =
             unwrap_policy_fence(fenced)

    assert %{
             type: ^type,
             generation: 17,
             digest: digest
           } = stamped.policy_ref

    assert is_binary(digest) and byte_size(digest) == 32
    refute Map.has_key?(stamped, :policy_snapshot)
    assert :erlang.external_size(stamped.policy_ref) < 256
  end

  @tag :compact_policy_ref
  test "existing-flow stamps fence the observed type and incarnation" do
    keydir = :ets.new(:policy_target_guard, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    id = "policy-target-flow"
    type = "policy-target-type"
    partition_key = "tenant"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    record = %{
      id: id,
      type: type,
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: nil,
      priority: 0,
      incarnation: 44,
      state_enter_seq: 44,
      partition_key: partition_key,
      root_flow_id: id,
      child_groups: %{}
    }

    value = Ferricstore.Flow.encode_record(record)

    :ets.insert(
      keydir,
      {state_key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
    )

    ctx = %{
      keydir_refs: {keydir},
      name: :policy_target_guard_test,
      shard_names: {:policy_stamp_shard_process_must_not_exist},
      slot_map: List.duplicate(0, 1_024) |> List.to_tuple()
    }

    assert {:ok, fenced} =
             PolicyCommand.stamp(
               ctx,
               {:flow_retry, state_key, %{id: id, partition_key: partition_key}}
             )

    assert {[_install], {:flow_retry, ^state_key, stamped}} = unwrap_policy_fence(fenced)

    assert stamped.policy_guard == %{
             state_key: state_key,
             type: type,
             incarnation: 44
           }
  end

  test "policy and migration codecs expose one canonical beta wire shape" do
    type = "single-wire-policy"
    state_key = Ferricstore.Flow.Keys.state_key("single-wire-flow", "tenant")
    policy = %{type: type, version: "public-v1"}

    policy_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 1)

    assert {:ok, {1, ^policy}} =
             Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(policy_value)

    generationless_policy = :erlang.term_to_binary({:flow_policy_v1, policy})
    assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(generationless_policy)

    catalog = Ferricstore.Flow.PolicyMigration.encode_catalog(type, state_key, 1)
    assert <<"FCT", 1, _payload::binary>> = catalog

    embedded_type_catalog =
      <<"FCT", 1, 1::unsigned-big-64, byte_size(type)::unsigned-big-32,
        byte_size(state_key)::unsigned-big-32, type::binary, state_key::binary>>

    assert :error = Ferricstore.Flow.PolicyMigration.decode_catalog(embedded_type_catalog)

    job = Ferricstore.Flow.PolicyMigration.encode_job(type, 1, 7, "owner", :active)
    assert <<"FPM", 1, _payload::binary>> = job

    revisionless_job =
      <<"FPM", 1, 0, 1, 1::unsigned-big-64, byte_size(type)::unsigned-big-32,
        byte_size("owner")::unsigned-big-32, type::binary, "owner">>

    assert :error = Ferricstore.Flow.PolicyMigration.decode_job(revisionless_job)
  end

  test "outer Flow batches replicate compact references instead of repeated policies" do
    keydir = :ets.new(:outer_batch_policy_stamp, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    type = "outer-batch-policy"
    policy = %{type: type, version: String.duplicate("p", 64 * 1024)}
    value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 1)
    policy_key = Ferricstore.Flow.Keys.policy_key(type)

    :ets.insert(
      keydir,
      {policy_key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
    )

    ctx = %{
      keydir_refs: {keydir},
      shard_names: {:policy_stamp_shard_process_must_not_exist}
    }

    keyed_commands =
      Enum.map(1..100, fn index ->
        id = "outer-batch-flow-#{index}"
        key = Ferricstore.Flow.Keys.state_key(id, "outer-batch-tenant")

        {key,
         {:flow_create, key,
          %{
            id: id,
            type: type,
            state: "queued",
            partition_key: "outer-batch-tenant"
          }}}
      end)

    assert {:ok, stamped} = PolicyCommand.stamp_many(ctx, keyed_commands)
    assert length(stamped) == 100

    assert Enum.all?(stamped, fn {_key, command} ->
             {_installs, {_op, _state_key, attrs}} = unwrap_policy_fence(command)
             match?(%{type: ^type, generation: 1, digest: <<_::256>>}, attrs.policy_ref)
           end)

    assert 1 == Enum.count(stamped, fn {_key, command} -> fence_installs(command) != [] end)

    assert :erlang.external_size(stamped) < 256 * 1024
  end

  test "obsolete cross-shard envelopes bypass policy traversal unchanged" do
    command =
      {:cross_shard_tx, [{0, [{0, {:flow_create, "state-key", %{id: "id", type: "type"}}}], nil}]}

    assert {:ok, unchanged} = PolicyCommand.stamp(:context_must_not_be_read, command)
    assert :erts_debug.same(command, unchanged)
  end

  test "nested batches store each policy reference once" do
    keydir = :ets.new(:deduplicated_policy_stamp, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    type = "deduplicated-policy"
    policy_key = Ferricstore.Flow.Keys.policy_key(type)
    policy = %{type: type, version: String.duplicate("v", 16_384)}
    value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 5)

    :ets.insert(
      keydir,
      {policy_key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
    )

    ctx = %{
      keydir_refs: {keydir},
      shard_names: {:policy_stamp_shard_process_must_not_exist}
    }

    records =
      Enum.map(1..100, fn index ->
        %{
          id: "deduplicated-flow-#{index}",
          type: type,
          state: "queued",
          partition_key: "deduplicated-tenant"
        }
      end)

    key = Ferricstore.Flow.Keys.state_key("__batch__", "deduplicated-tenant")
    command = {:flow_create_many, key, %{records: records}}
    unstamped_size = :erlang.external_size(command)

    assert {:ok, fenced} = PolicyCommand.stamp(ctx, command)

    assert {[_install], {:flow_create_many, ^key, stamped_attrs} = stamped} =
             unwrap_policy_fence(fenced)

    assert %{^type => %{type: ^type, generation: 5, digest: <<_::256>>}} =
             stamped_attrs.policy_refs

    assert Enum.all?(stamped_attrs.records, fn attrs ->
             not Map.has_key?(attrs, :policy_ref)
           end)

    assert :erlang.external_size(stamped) < unstamped_size + 512
  end

  test "policy snapshot size has a deterministic inclusive boundary" do
    max_bytes = Ferricstore.Flow.RetryPolicy.max_policy_snapshot_bytes()
    too_large_error = "ERR flow policy snapshot exceeds #{max_bytes} bytes"
    payload_size = largest_valid_policy_payload(0, max_bytes)
    allowed = sized_policy(payload_size)
    rejected = sized_policy(payload_size + 1)

    assert :erlang.external_size(allowed) == max_bytes
    assert :ok = Ferricstore.Flow.RetryPolicy.validate_flow_policy_snapshot_size(allowed)

    encoded_allowed = Ferricstore.Flow.RetryPolicy.encode_flow_policy(allowed, 1)

    assert byte_size(encoded_allowed) <=
             Ferricstore.Flow.RetryPolicy.max_encoded_policy_bytes()

    assert {:ok, {1, ^allowed}} =
             Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(encoded_allowed)

    assert {:error, ^too_large_error} =
             Ferricstore.Flow.RetryPolicy.validate_flow_policy_snapshot_size(rejected)

    assert {:error, ^too_large_error} =
             Ferricstore.Flow.RetryPolicy.normalize_flow_policy("oversized-policy",
               version: String.duplicate("x", max_bytes)
             )
  end

  test "oversized, compressed, and trailing stored policy envelopes are rejected before decode" do
    max_encoded = Ferricstore.Flow.RetryPolicy.max_encoded_policy_bytes()

    oversized_policy = %{
      type: "oversized-envelope",
      version: String.duplicate("x", max_encoded)
    }

    oversized = :erlang.term_to_binary({:flow_policy_v1, 1, oversized_policy})
    assert byte_size(oversized) > max_encoded
    assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(oversized)

    compressed =
      :erlang.term_to_binary({:flow_policy_v1, 1, oversized_policy}, compressed: 9)

    assert <<131, 80, _rest::binary>> = compressed
    assert byte_size(compressed) < max_encoded
    assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(compressed)

    valid = Ferricstore.Flow.RetryPolicy.encode_flow_policy(%{type: "canonical"}, 1)
    assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(valid <> <<0>>)
  end

  test "policy generations stop at the exact migration score boundary" do
    max_generation = Ferricstore.Flow.RetryPolicy.max_policy_generation()
    policy = %{type: "generation-boundary", version: "public-v1"}
    encoded = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, max_generation)

    assert {:ok, {^max_generation, ^policy}} =
             Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(encoded)

    invalid = :erlang.term_to_binary({:flow_policy_v1, max_generation + 1, policy})
    assert :error = Ferricstore.Flow.RetryPolicy.decode_flow_policy_entry(invalid)

    assert {:ok, ^max_generation} =
             Ferricstore.Flow.PolicyMigration.next_generation(max_generation - 1)

    assert {:error, "ERR flow policy generation exhausted"} =
             Ferricstore.Flow.PolicyMigration.next_generation(max_generation)
  end

  test "multi-type commands enforce one total snapshot bundle limit" do
    keydir = :ets.new(:multi_type_policy_stamp, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    max_bytes = Ferricstore.Flow.RetryPolicy.max_policy_snapshot_bytes()
    too_large_error = "ERR flow policy snapshot exceeds #{max_bytes} bytes"
    first = %{type: "bundle-type-1", version: String.duplicate("a", 64 * 1024)}
    second = %{type: "bundle-type-2", version: String.duplicate("b", 64 * 1024)}

    third_payload =
      largest_valid_bundle_payload([first, second], "bundle-type-3", 0, max_bytes)

    third = %{type: "bundle-type-3", version: String.duplicate("c", third_payload)}

    assert :ok =
             Ferricstore.Flow.RetryPolicy.validate_flow_policy_snapshots_size([
               first,
               second,
               third
             ])

    Enum.each([first, second, third], fn policy ->
      key = Ferricstore.Flow.Keys.policy_key(policy.type)
      value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 1)

      :ets.insert(
        keydir,
        {key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
      )
    end)

    ctx = %{
      keydir_refs: {keydir},
      shard_names: {:policy_stamp_shard_process_must_not_exist}
    }

    records =
      Enum.map([first, second, third], fn policy ->
        %{
          id: "flow-#{policy.type}",
          type: policy.type,
          state: "queued",
          partition_key: "bundle-tenant"
        }
      end)

    key = Ferricstore.Flow.Keys.state_key("__batch__", "bundle-tenant")
    command = {:flow_create_many, key, %{records: records}}

    assert {:ok, fenced} = PolicyCommand.stamp(ctx, command)

    assert {installs, {:flow_create_many, ^key, %{policy_refs: refs}}} =
             unwrap_policy_fence(fenced)

    assert map_size(refs) == 3
    assert length(installs) == 3

    oversized_third = %{third | version: third.version <> "x"}
    oversized_value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(oversized_third, 2)

    :ets.insert(
      keydir,
      {Ferricstore.Flow.Keys.policy_key(third.type), oversized_value, 0,
       Ferricstore.Store.LFU.initial(), 0, 0, byte_size(oversized_value)}
    )

    assert {:error, ^too_large_error} =
             PolicyCommand.stamp(ctx, command)
  end

  test "command tag dispatch does not allocate tag strings" do
    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/policy_command.ex", __DIR__))

    refute source =~ "Atom.to_string"
    refute source =~ "String.starts_with?"
  end

  test "hot policy stamping does not call the shard process" do
    keydir = :ets.new(:hot_policy_stamp, [:set, :public])
    on_exit(fn -> if :ets.info(keydir) != :undefined, do: :ets.delete(keydir) end)

    type = "hot-policy-stamp"
    policy_key = Ferricstore.Flow.Keys.policy_key(type)

    {:ok, policy} =
      Ferricstore.Flow.RetryPolicy.normalize_flow_policy(type, max_active_ms: 1_000)

    value = Ferricstore.Flow.RetryPolicy.encode_flow_policy(policy, 7)

    :ets.insert(
      keydir,
      {policy_key, value, 0, Ferricstore.Store.LFU.initial(), 0, 0, byte_size(value)}
    )

    ctx = %{
      keydir_refs: {keydir},
      shard_names: {:policy_stamp_shard_process_must_not_exist}
    }

    attrs = %{
      id: "hot-policy-flow",
      type: type,
      state: "queued",
      partition_key: "hot-policy-tenant",
      policy_reference_captured: false,
      policy_snapshots: %{
        "injected-type" => %{
          generation: 999,
          policy: %{type: "injected-type", version: "spoofed"}
        }
      }
    }

    command =
      {:flow_create, Ferricstore.Flow.Keys.state_key(attrs.id, attrs.partition_key), attrs}

    assert {:ok, fenced} = PolicyCommand.stamp(ctx, command)
    assert {[_install], {:flow_create, _key, stamped}} = unwrap_policy_fence(fenced)
    assert %{type: ^type, generation: 7, digest: <<_::256>>} = stamped.policy_ref
    assert stamped.policy_reference_captured
    refute Map.has_key?(stamped, :policy_snapshots)
  end

  defp largest_valid_policy_payload(low, high) when low <= high do
    midpoint = div(low + high, 2)

    case Ferricstore.Flow.RetryPolicy.validate_flow_policy_snapshot_size(sized_policy(midpoint)) do
      :ok when low == high -> midpoint
      :ok -> largest_valid_policy_payload(midpoint + 1, high)
      {:error, _reason} -> largest_valid_policy_payload(low, midpoint - 1)
    end
  end

  defp largest_valid_policy_payload(low, high) when low > high, do: high

  defp largest_valid_bundle_payload(policies, type, low, high) when low <= high do
    midpoint = div(low + high, 2)
    candidate = %{type: type, version: String.duplicate("c", midpoint)}

    case Ferricstore.Flow.RetryPolicy.validate_flow_policy_snapshots_size(policies ++ [candidate]) do
      :ok when low == high -> midpoint
      :ok -> largest_valid_bundle_payload(policies, type, midpoint + 1, high)
      {:error, _reason} -> largest_valid_bundle_payload(policies, type, low, midpoint - 1)
    end
  end

  defp largest_valid_bundle_payload(_policies, _type, low, high) when low > high, do: high

  defp sized_policy(payload_size) do
    %{type: "snapshot-boundary", version: String.duplicate("x", payload_size)}
  end

  defp unwrap_policy_fence({:flow_policy_fence, installs, command}), do: {installs, command}
  defp unwrap_policy_fence(command), do: {[], command}

  defp fence_installs({:flow_policy_fence, installs, _command}), do: installs
  defp fence_installs(_command), do: []
end
