defmodule Ferricstore.Store.NamespaceUsageTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.NamespaceUsage
  alias Ferricstore.Store.Shard.NamespaceUsageIndex

  setup do
    suffix = System.unique_integer([:positive])
    name = :"namespace_usage_store_#{suffix}"

    keydirs =
      for shard <- 0..1 do
        keydir = :ets.new(:namespace_usage_store_keydir, [:set, :public])
        {usage, expiry} = NamespaceUsageIndex.table_names(name, shard)
        NamespaceUsageIndex.ensure_tables!(usage, expiry)
        {keydir, usage, expiry}
      end

    on_exit(fn ->
      Enum.each(keydirs, fn {keydir, usage, expiry} ->
        Enum.each([keydir, usage, expiry], &delete_table/1)
      end)
    end)

    store = %{
      name: name,
      keydir_refs: keydirs |> Enum.map(&elem(&1, 0)) |> List.to_tuple(),
      blob_side_channel_threshold_bytes: 0
    }

    %{store: store, keydirs: keydirs}
  end

  test "activates once and aggregates exact details across shards", ctx do
    scope = "tenant:aggregate"
    first = scope <> ":first"
    second = scope <> ":second"
    put_keydir(ctx.keydirs |> Enum.at(0) |> elem(0), first, "one")
    put_keydir(ctx.keydirs |> Enum.at(1) |> elem(0), second, "two")

    assert :ok = NamespaceUsage.ensure_scope(ctx.store, scope, 1_000)

    assert {:ok, details} =
             NamespaceUsage.details(ctx.store, scope, [first, second], 1_000)

    assert details.keys == 2
    assert details.bytes == entry_bytes(first, "one") + entry_bytes(second, "two")
    assert details.counted_by_key == %{first => true, second => true}

    assert details.bytes_by_key == %{
             first => entry_bytes(first, "one"),
             second => entry_bytes(second, "two")
           }

    Enum.each(ctx.keydirs, fn {keydir, _usage, _expiry} -> :ets.delete_all_objects(keydir) end)

    # A ready scope is not rebuilt from later out-of-band keydir changes.
    assert :ok = NamespaceUsage.ensure_scope(ctx.store, scope, 1_001)
    assert {:ok, %{keys: 2}} = NamespaceUsage.usage(ctx.store, scope, 1_001)
  end

  test "aggregates Flow counts across shards", ctx do
    scope = "tenant:flows"

    put_keydir(
      ctx.keydirs |> Enum.at(0) |> elem(0),
      Keys.state_key("flow-a", scope <> ":a"),
      encoded_flow("flow-a", scope <> ":a")
    )

    put_keydir(
      ctx.keydirs |> Enum.at(1) |> elem(0),
      Keys.state_key("flow-b", scope <> ":b"),
      encoded_flow("flow-b", scope <> ":b")
    )

    assert :ok = NamespaceUsage.ensure_scope(ctx.store, scope, 1_000)
    assert {:ok, %{flow_count: 2}} = NamespaceUsage.usage(ctx.store, scope, 1_000)
  end

  defp put_keydir(keydir, key, value) do
    true = :ets.insert(keydir, {key, value, 0, 0, :pending, 0, byte_size(value)})
  end

  defp entry_bytes(key, value), do: byte_size(key) + byte_size(value)

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

  defp delete_table(table) do
    case :ets.info(table) do
      :undefined -> :ok
      _info -> :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
