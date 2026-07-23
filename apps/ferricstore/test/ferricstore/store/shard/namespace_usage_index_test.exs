defmodule Ferricstore.Store.Shard.NamespaceUsageIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.NamespaceUsageIndex

  setup do
    suffix = System.unique_integer([:positive])
    usage = :"namespace_usage_test_#{suffix}"
    expiry = :"namespace_usage_expiry_test_#{suffix}"
    keydir = :ets.new(:namespace_usage_keydir, [:set, :public])

    NamespaceUsageIndex.ensure_tables!(usage, expiry)

    on_exit(fn ->
      delete_table(usage)
      delete_table(expiry)
      delete_table(keydir)
    end)

    %{usage: usage, expiry: expiry, keydir: keydir}
  end

  test "rebuilds exact logical key and byte aggregates for one namespace", ctx do
    scope = "tenant:a"
    plain_key = scope <> ":plain"
    hash_key = scope <> ":hash"
    type_key = CompoundKey.type_key(hash_key)
    field_key = CompoundKey.hash_field(hash_key, "field")

    put_keydir(ctx.keydir, plain_key, "value")
    put_keydir(ctx.keydir, type_key, CompoundKey.encode_type(:hash))
    put_keydir(ctx.keydir, field_key, "field-value")
    put_keydir(ctx.keydir, "tenant:b:ignored", "ignored")

    refute NamespaceUsageIndex.active?(ctx.usage)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert NamespaceUsageIndex.active?(ctx.usage)

    expected_bytes =
      entry_bytes(plain_key, "value") +
        entry_bytes(type_key, CompoundKey.encode_type(:hash)) +
        entry_bytes(field_key, "field-value")

    assert {:ok, %{keys: 2, bytes: ^expected_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_000)

    # Aggregate reads must not revisit the authoritative keydir.
    true = :ets.delete(ctx.keydir)

    assert {:ok, %{keys: 2, bytes: ^expected_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_000)
  end

  test "serializes a rebuild with a concurrent post-keydir replacement", ctx do
    scope = "tenant:rebuild-race"
    key = scope <> ":value"
    parent = self()

    put_keydir(ctx.keydir, key, "old")

    rebuild =
      Task.async(fn ->
        Process.put(:ferricstore_namespace_usage_rebuild_visit_hook, fn
          ^key ->
            send(parent, {:rebuild_observed, self()})

            receive do
              :continue_rebuild -> :ok
            end

          _other ->
            :ok
        end)

        NamespaceUsageIndex.rebuild_scope(
          ctx.usage,
          ctx.expiry,
          ctx.keydir,
          scope,
          now_ms: 1_000
        )
      end)

    assert_receive {:rebuild_observed, rebuild_pid}, 1_000
    put_keydir(ctx.keydir, key, "replacement")

    replacement =
      Task.async(fn ->
        NamespaceUsageIndex.put(ctx.usage, ctx.expiry, key, "replacement", 0)
      end)

    assert nil == Task.yield(replacement, 50)
    send(rebuild_pid, :continue_rebuild)
    assert :ok = Task.await(rebuild, 1_000)
    assert :ok = Task.await(replacement, 1_000)

    expected_bytes = entry_bytes(key, "replacement")

    assert {:ok, %{keys: 1, bytes: ^expected_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_000)
  end

  test "updates overlapping tracked scopes from only the affected storage row", ctx do
    parent = "tenant"
    child = "tenant:a"
    key = child <> ":value"

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               parent,
               now_ms: 1_000
             )

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               child,
               now_ms: 1_000
             )

    assert :ok = NamespaceUsageIndex.put(ctx.usage, ctx.expiry, key, "one", 0)

    assert {:ok, %{keys: 1, bytes: first_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, parent, 1_000)

    assert {:ok, %{keys: 1, bytes: ^first_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, child, 1_000)

    assert first_bytes == entry_bytes(key, "one")
    assert :ok = NamespaceUsageIndex.put(ctx.usage, ctx.expiry, key, "longer", 0)

    assert {:ok, %{keys: 1, bytes: replacement_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, child, 1_000)

    assert replacement_bytes == entry_bytes(key, "longer")
    assert :ok = NamespaceUsageIndex.delete(ctx.usage, ctx.expiry, key)

    assert {:ok, %{keys: 0, bytes: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, child, 1_000)
  end

  test "expires indexed rows once and permits a later replacement", ctx do
    scope = "tenant:expiring"
    key = scope <> ":value"

    put_keydir(ctx.keydir, key, "old", 1_010)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert {:ok, %{keys: 1}} = NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_009)

    assert {:ok, %{keys: 0, bytes: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_010)

    assert :ok = NamespaceUsageIndex.put(ctx.usage, ctx.expiry, key, "new", 0)

    assert {:ok, %{keys: 1, bytes: bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_011)

    assert bytes == entry_bytes(key, "new")
  end

  test "returns affected logical details without enumerating the namespace", ctx do
    scope = "tenant:details"
    hash_key = scope <> ":hash"
    other_key = scope <> ":other"
    type_key = CompoundKey.type_key(hash_key)
    field_key = CompoundKey.hash_field(hash_key, "field")

    put_keydir(ctx.keydir, type_key, CompoundKey.encode_type(:hash))
    put_keydir(ctx.keydir, field_key, "value")
    put_keydir(ctx.keydir, other_key, "other")

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert {:ok, details} =
             NamespaceUsageIndex.details(
               ctx.usage,
               ctx.expiry,
               scope,
               [hash_key],
               1_000
             )

    expected_hash_bytes =
      entry_bytes(type_key, CompoundKey.encode_type(:hash)) + entry_bytes(field_key, "value")

    assert details.keys == 2
    assert details.bytes > expected_hash_bytes
    assert details.bytes_by_key == %{hash_key => expected_hash_bytes}
    assert details.entries_by_key == %{hash_key => 2}
    assert details.plain_entries_by_key == %{hash_key => 0}
    assert details.internal_entries_by_key == %{hash_key => 2}
  end

  test "Shard ETS inserts, replacements, and deletes maintain a ready scope", ctx do
    scope = "tenant:shard-hook"
    key = scope <> ":value"
    state = usage_state(ctx)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert true = ShardETS.ets_insert(state, key, "one", 0)
    assert_usage(ctx, scope, 1, entry_bytes(key, "one"))

    assert true = ShardETS.ets_insert(state, key, "replacement", 0)
    assert_usage(ctx, scope, 1, entry_bytes(key, "replacement"))

    assert true = ShardETS.ets_delete_key(state, key)
    assert_usage(ctx, scope, 0, 0)
  end

  test "Shard ETS exact delete changes usage only when the observed row wins", ctx do
    scope = "tenant:exact-hook"
    key = scope <> ":value"
    state = usage_state(ctx)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert true = ShardETS.ets_insert_with_location(state, key, "old", 0, 1, 10, 3)
    [old_row] = :ets.lookup(ctx.keydir, key)
    assert true = ShardETS.ets_insert_with_location(state, key, "new", 0, 2, 20, 3)

    refute ShardETS.delete_exact_entry(state, old_row)
    assert_usage(ctx, scope, 1, entry_bytes(key, "new"))

    [current_row] = :ets.lookup(ctx.keydir, key)
    assert ShardETS.delete_exact_entry(state, current_row)
    assert_usage(ctx, scope, 0, 0)
  end

  test "Shard ETS fast fresh batch maintains usage for every inserted row", ctx do
    scope = "tenant:batch-hook"
    first = scope <> ":first"
    second = scope <> ":second"
    state = usage_state(ctx)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert {:ok, 2} =
             ShardETS.ets_insert_fresh_no_expiry_many_with_location(
               state,
               [{first, "one", 0}, {second, "two", 0}],
               {:waraft_segment, 1},
               100,
               64
             )

    assert_usage(
      ctx,
      scope,
      2,
      entry_bytes(first, "one") + entry_bytes(second, "two")
    )
  end

  test "reset preserves tracked scopes while clearing all contributions", ctx do
    scope = "tenant:reset"
    key = scope <> ":value"

    put_keydir(ctx.keydir, key, "value")

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert_usage(ctx, scope, 1, entry_bytes(key, "value"))
    assert :ok = NamespaceUsageIndex.reset(ctx.usage, ctx.expiry)
    assert NamespaceUsageIndex.active?(ctx.usage)
    assert_usage(ctx, scope, 0, 0)

    assert :ok = NamespaceUsageIndex.put(ctx.usage, ctx.expiry, key, "new", 0)
    assert_usage(ctx, scope, 1, entry_bytes(key, "new"))
  end

  test "rebuild tracked scopes restores each aggregate after keydir replacement", ctx do
    parent_scope = "tenant"
    child_scope = "tenant:recovered"
    key = child_scope <> ":value"

    for scope <- [parent_scope, child_scope] do
      assert :ok =
               NamespaceUsageIndex.rebuild_scope(
                 ctx.usage,
                 ctx.expiry,
                 ctx.keydir,
                 scope,
                 now_ms: 1_000
               )
    end

    put_keydir(ctx.keydir, key, "recovered")

    assert :ok =
             NamespaceUsageIndex.rebuild_tracked(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               now_ms: 1_000
             )

    expected = entry_bytes(key, "recovered")
    assert_usage(ctx, parent_scope, 1, expected)
    assert_usage(ctx, child_scope, 1, expected)
  end

  test "details return exact source footprint outside the tracked destination scope", ctx do
    scope = "tenant:destination"
    source = "tenant:source:hash"
    type_key = CompoundKey.type_key(source)

    rows = [
      {type_key, CompoundKey.encode_type(:hash)},
      {CompoundKey.hash_field(source, "small"), "1"},
      {CompoundKey.hash_field(source, "medium"), "12345"},
      {CompoundKey.hash_field(source, "large"), "123456789"},
      {CompoundKey.hash_field(source, "largest"), "1234567890123"}
    ]

    Enum.each(rows, fn {key, value} -> put_keydir(ctx.keydir, key, value) end)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert {:ok, details} =
             NamespaceUsageIndex.details(ctx.usage, ctx.expiry, scope, [source], 1_000)

    expected_bytes = Enum.sum(Enum.map(rows, fn {key, value} -> entry_bytes(key, value) end))

    transfer_sizes =
      rows
      |> Enum.map(fn {key, value} ->
        entry_bytes(key, value) - CompoundKey.encoded_redis_key_size(source)
      end)
      |> Enum.sort(:desc)

    [first, second, third | _rest] = transfer_sizes

    assert details.keys == 0
    assert details.bytes == 0
    assert details.counted_by_key == %{source => true}
    assert details.bytes_by_key == %{source => expected_bytes}
    assert details.entries_by_key == %{source => length(rows)}
    assert details.top_transfer_base_bytes_by_key == %{source => {first, second, third, 3}}

    largest_key = CompoundKey.hash_field(source, "largest")
    assert :ok = NamespaceUsageIndex.delete(ctx.usage, ctx.expiry, largest_key)

    assert {:ok, after_delete} =
             NamespaceUsageIndex.details(ctx.usage, ctx.expiry, scope, [source], 1_000)

    remaining_transfer_sizes =
      rows
      |> Enum.reject(&(elem(&1, 0) == largest_key))
      |> Enum.map(fn {key, value} ->
        entry_bytes(key, value) - CompoundKey.encoded_redis_key_size(source)
      end)
      |> Enum.sort(:desc)

    [next_first, next_second, next_third | _rest] = remaining_transfer_sizes

    assert after_delete.top_transfer_base_bytes_by_key == %{
             source => {next_first, next_second, next_third, 3}
           }
  end

  test "tracking a later scope never revisits the authoritative keydir", ctx do
    first_scope = "tenant:first"
    second_scope = "tenant:second"
    second_key = second_scope <> ":value"
    put_keydir(ctx.keydir, second_key, "value")

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               first_scope,
               now_ms: 1_000
             )

    Process.put(:ferricstore_namespace_usage_rebuild_visit_hook, fn key ->
      flunk("later scope rebuild revisited keydir row #{inspect(key)}")
    end)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               second_scope,
               now_ms: 1_000
             )

    assert_usage(ctx, second_scope, 1, entry_bytes(second_key, "value"))
  after
    Process.delete(:ferricstore_namespace_usage_rebuild_visit_hook)
  end

  test "an exact entry-byte refresh replaces sidecar growth without changing key count", ctx do
    scope = "tenant:external-refresh"
    key = scope <> ":prob"
    type_key = CompoundKey.type_key(key)
    marker = CompoundKey.encode_prob_type(:bloom, 1)
    put_keydir(ctx.keydir, type_key, marker)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    base_bytes = entry_bytes(type_key, marker)
    assert_usage(ctx, scope, 1, base_bytes)

    assert :ok =
             NamespaceUsageIndex.put_exact_bytes(
               ctx.usage,
               ctx.expiry,
               type_key,
               base_bytes + 10_000,
               0
             )

    assert_usage(ctx, scope, 1, base_bytes + 10_000)
  end

  test "invalidation fails scope readiness closed until an exact rebuild", ctx do
    scope = "tenant:invalidate"

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert NamespaceUsageIndex.scope_ready?(ctx.usage, scope)
    assert :ok = NamespaceUsageIndex.invalidate(ctx.usage, ctx.expiry)
    refute NamespaceUsageIndex.scope_ready?(ctx.usage, scope)
    assert :unavailable = NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_000)
  end

  test "rebuild counts live Flow state records by logical partition scope", ctx do
    scope = "tenant:flow"
    state_key = Keys.state_key("flow-1", scope <> ":orders")

    put_keydir(ctx.keydir, state_key, encoded_flow("flow-1", scope <> ":orders"))
    put_keydir(ctx.keydir, Keys.registry_key("flow-1", scope <> ":orders"), "1")

    put_keydir(
      ctx.keydir,
      Keys.state_key("flow-2", "tenant:other"),
      encoded_flow("flow-2", "tenant:other")
    )

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_000)
  end

  test "Flow transitions preserve their first partition mapping and delete removes it", ctx do
    first_scope = "tenant:first"
    second_scope = "tenant:second"
    state_key = Keys.state_key("flow-transition", first_scope)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               first_scope,
               now_ms: 1_000
             )

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               second_scope,
               now_ms: 1_000
             )

    assert :ok =
             NamespaceUsageIndex.put(
               ctx.usage,
               ctx.expiry,
               state_key,
               encoded_flow("flow-transition", first_scope),
               0
             )

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, first_scope, 1_000)

    # The state key fixes the partition. A transition does not decode or move the run.
    assert :ok =
             NamespaceUsageIndex.put(
               ctx.usage,
               ctx.expiry,
               state_key,
               encoded_flow("flow-transition", second_scope),
               0
             )

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, first_scope, 1_000)

    assert {:ok, %{flow_count: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, second_scope, 1_000)

    assert :ok = NamespaceUsageIndex.delete(ctx.usage, ctx.expiry, state_key)

    assert {:ok, %{flow_count: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, first_scope, 1_000)
  end

  test "Flow expiry removes the count exactly once", ctx do
    scope = "tenant:expiring-flow"
    state_key = Keys.state_key("flow-expiry", scope)

    put_keydir(ctx.keydir, state_key, encoded_flow("flow-expiry", scope), 1_010)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               scope,
               now_ms: 1_000
             )

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_009)

    assert {:ok, %{flow_count: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_010)

    assert {:ok, %{flow_count: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_011)
  end

  test "later Flow scope activation derives its count without revisiting keydir", ctx do
    first_scope = "tenant:first-flow"
    second_scope = "tenant:second-flow"
    state_key = Keys.state_key("flow-later-scope", second_scope)

    put_keydir(ctx.keydir, state_key, encoded_flow("flow-later-scope", second_scope))

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               first_scope,
               now_ms: 1_000
             )

    true = :ets.delete(ctx.keydir)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               second_scope,
               now_ms: 1_000
             )

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, second_scope, 1_000)
  end

  test "corrupt Flow state is conservatively counted in every activated scope", ctx do
    first_scope = "tenant:corrupt:first"
    second_scope = "tenant:corrupt:second"
    state_key = Keys.state_key("flow-corrupt", first_scope)

    put_keydir(ctx.keydir, state_key, "not-a-flow-record")

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               first_scope,
               now_ms: 1_000
             )

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               second_scope,
               now_ms: 1_000
             )

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, first_scope, 1_000)

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, second_scope, 1_000)

    assert :ok = NamespaceUsageIndex.delete(ctx.usage, ctx.expiry, state_key)

    assert {:ok, %{flow_count: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, first_scope, 1_000)

    assert {:ok, %{flow_count: 0}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, second_scope, 1_000)
  end

  test "wildcard Flow accounting includes partitioned runs but excludes unscoped runs", ctx do
    partitioned_key = Keys.state_key("flow-partitioned", "tenant:partitioned")
    unscoped_key = Keys.state_key("flow-unscoped")

    put_keydir(
      ctx.keydir,
      partitioned_key,
      encoded_flow("flow-partitioned", "tenant:partitioned")
    )

    put_keydir(ctx.keydir, unscoped_key, encoded_flow("flow-unscoped", nil))

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               "*",
               now_ms: 1_000
             )

    assert {:ok, %{flow_count: 1}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, "*", 1_000)
  end

  test "wildcard key accounting covers rebuilds and later writes", ctx do
    first_key = "tenant:wildcard:first"
    second_key = "unscoped-key"
    first_value = "first"
    second_value = "second"

    put_keydir(ctx.keydir, first_key, first_value)

    assert :ok =
             NamespaceUsageIndex.rebuild_scope(
               ctx.usage,
               ctx.expiry,
               ctx.keydir,
               "*",
               now_ms: 1_000
             )

    assert {:ok, %{keys: 1, bytes: first_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, "*", 1_000)

    assert first_bytes == byte_size(first_key) + byte_size(first_value)

    assert :ok =
             NamespaceUsageIndex.put(
               ctx.usage,
               ctx.expiry,
               second_key,
               second_value,
               0
             )

    assert {:ok, %{keys: 2, bytes: total_bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, "*", 1_000)

    assert total_bytes ==
             byte_size(first_key) + byte_size(first_value) + byte_size(second_key) +
               byte_size(second_value)
  end

  defp put_keydir(keydir, key, value, expire_at_ms \\ 0) do
    true =
      :ets.insert(
        keydir,
        {key, value, expire_at_ms, 0, :pending, 0, logical_value_size(value)}
      )
  end

  defp entry_bytes(key, value), do: byte_size(key) + logical_value_size(value)

  defp encoded_flow(id, partition_key) do
    Flow.encode_record(%{
      id: id,
      type: "quota-test",
      state: "ready",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: 1,
      updated_at_ms: 1,
      next_run_at_ms: nil,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      max_active_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: partition_key,
      payload_ref: nil,
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: id,
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      child_groups: %{}
    })
  end

  defp logical_value_size(value) when is_binary(value), do: byte_size(value)
  defp logical_value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
  defp logical_value_size(value) when is_float(value), do: byte_size(Float.to_string(value))

  defp usage_state(ctx) do
    %{
      keydir: ctx.keydir,
      index: 0,
      instance_ctx: %{
        hot_cache_max_value_size: 64,
        keydir_binary_bytes: :atomics.new(1, signed: true),
        shard_count: 1
      },
      namespace_usage_index: ctx.usage,
      namespace_usage_expiry: ctx.expiry
    }
  end

  defp assert_usage(ctx, scope, keys, bytes) do
    assert {:ok, %{keys: ^keys, bytes: ^bytes}} =
             NamespaceUsageIndex.usage(ctx.usage, ctx.expiry, scope, 1_000)
  end

  defp delete_table(table) do
    case :ets.info(table) do
      :undefined -> :ok
      _info -> :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
