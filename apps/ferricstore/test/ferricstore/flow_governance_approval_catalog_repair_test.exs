defmodule Ferricstore.FlowGovernanceApprovalCatalogRepairTest do
  use Ferricstore.Test.FlowCase

  alias FerricStore.Impl
  alias Ferricstore.Flow.Governance.{ApprovalCatalogRepair, Catalog}
  alias Ferricstore.Flow.Keys

  test "catalog bulk unregister removes one bounded page in one write" do
    ctx = FerricStore.Instance.get(:default)
    suffix = unique_suffix()
    catalog_key = scope_catalog("approval-bulk-unregister-#{suffix}")

    members =
      Enum.map(1..ApprovalCatalogRepair.page_size(), fn index ->
        <<1, "approval-bulk-unregister-", suffix::binary, "-",
          String.pad_leading(Integer.to_string(index), 3, "0")::binary>>
      end)

    assert :ok = Catalog.register_keys(ctx, catalog_key, members)
    assert {:ok, count} = Impl.zcard(ctx, catalog_key)
    assert count == length(members)

    assert_single_zrem(ctx, catalog_key, members)

    assert {:ok, 0} = Impl.zcard(ctx, catalog_key)
    assert :ok = Catalog.unregister_keys(ctx, catalog_key, [])
  end

  test "source repair restores membership created during a batched stale removal" do
    ctx = FerricStore.Instance.get(:default)
    suffix = unique_suffix()
    record_key = <<1, "approval-source-race-", suffix::binary>>
    source_catalog = Keys.governance_catalog_key(:approval)
    target_catalog = scope_catalog("approval-source-race-#{suffix}")
    calls = :atomics.new(1, [])

    source_targets = fn ^record_key ->
      case :atomics.add_get(calls, 1, 1) do
        1 -> :missing
        _later -> {:ok, [target_catalog]}
      end
    end

    assert :ok = Catalog.register(ctx, :approval, record_key)

    assert :ok =
             ApprovalCatalogRepair.step(
               ctx,
               target_catalog,
               fn ^record_key -> true end,
               source_targets
             )

    assert {:ok, "0.0"} = Impl.zscore(ctx, source_catalog, record_key)
    assert {:ok, "0.0"} = Impl.zscore(ctx, target_catalog, record_key)
    assert :atomics.get(calls, 1) == 2
  end

  test "target repair restores membership that becomes valid during batched removal" do
    ctx = FerricStore.Instance.get(:default)
    suffix = unique_suffix()
    record_key = <<1, "approval-target-race-", suffix::binary>>
    target_catalog = scope_catalog("approval-target-race-#{suffix}")
    calls = :atomics.new(1, [])
    matcher = fn ^record_key -> :atomics.add_get(calls, 1, 1) > 1 end

    assert :ok = Catalog.register_key(ctx, target_catalog, record_key)

    assert :ok =
             ApprovalCatalogRepair.step(
               ctx,
               target_catalog,
               matcher,
               fn _key -> :skip end
             )

    assert {:ok, "0.0"} = Impl.zscore(ctx, target_catalog, record_key)
    assert :atomics.get(calls, 1) == 2
  end

  test "source repair restores uncertain membership when post-delete validation fails" do
    ctx = FerricStore.Instance.get(:default)
    suffix = unique_suffix()
    record_key = <<1, "approval-source-error-", suffix::binary>>
    source_catalog = Keys.governance_catalog_key(:approval)
    target_catalog = scope_catalog("approval-source-error-#{suffix}")
    calls = :atomics.new(1, [])

    source_targets = fn ^record_key ->
      case :atomics.add_get(calls, 1, 1) do
        1 -> :missing
        _later -> {:error, "ERR transient source validation"}
      end
    end

    assert :ok = Catalog.register(ctx, :approval, record_key)

    assert {:error, "ERR transient source validation"} =
             ApprovalCatalogRepair.step(
               ctx,
               target_catalog,
               fn _key -> false end,
               source_targets
             )

    assert {:ok, "0.0"} = Impl.zscore(ctx, source_catalog, record_key)
  end

  test "target repair restores uncertain membership when post-delete validation fails" do
    ctx = FerricStore.Instance.get(:default)
    suffix = unique_suffix()
    record_key = <<1, "approval-target-error-", suffix::binary>>
    target_catalog = scope_catalog("approval-target-error-#{suffix}")
    calls = :atomics.new(1, [])

    matcher = fn ^record_key ->
      case :atomics.add_get(calls, 1, 1) do
        1 -> false
        _later -> {:error, "ERR transient target validation"}
      end
    end

    assert :ok = Catalog.register_key(ctx, target_catalog, record_key)

    assert {:error, "ERR transient target validation"} =
             ApprovalCatalogRepair.step(
               ctx,
               target_catalog,
               matcher,
               fn _key -> :skip end
             )

    assert {:ok, "0.0"} = Impl.zscore(ctx, target_catalog, record_key)
  end

  defp assert_single_zrem(ctx, catalog_key, members) do
    parent = self()
    reference = make_ref()

    worker =
      spawn(fn ->
        receive do
          :unregister ->
            send(parent, {reference, Catalog.unregister_keys(ctx, catalog_key, members)})
            receive do: (:stop -> :ok)
        end
      end)

    Code.ensure_loaded!(Impl)
    assert :erlang.trace(worker, true, [:call, {:tracer, self()}]) == 1
    assert :erlang.trace_pattern({Impl, :zrem, 3}, true, [:local]) > 0

    try do
      send(worker, :unregister)
      assert_receive {^reference, :ok}, 10_000
      trace_reference = :erlang.trace_delivered(worker)
      assert_receive {:trace_delivered, _, ^trace_reference}, 1_000
      assert traced_calls(Impl, :zrem, 3) == 1
    after
      :erlang.trace_pattern({Impl, :zrem, 3}, false, [:local])
      :erlang.trace(worker, false, [:call])
      send(worker, :stop)
    end
  end

  defp traced_calls(module, function, arity) do
    receive do
      {:trace, _pid, :call, {^module, ^function, args}} when length(args) == arity ->
        1 + traced_calls(module, function, arity)
    after
      0 -> 0
    end
  end

  defp scope_catalog(scope), do: Keys.governance_approval_scope_catalog_key(scope)
  defp unique_suffix, do: Integer.to_string(System.unique_integer([:positive]))
end
