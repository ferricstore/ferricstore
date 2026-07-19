defmodule Ferricstore.Raft.CommandBatchingTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.CommandBatching

  test "reply-expanding commands bypass namespace coalescing" do
    for command <- [
          {:batch, [{:delete, "first"}, {:delete, "second"}]},
          {:put_batch, [{"first", "1", nil}, {"second", "2", nil}]},
          {:put_blob_batch, [{"first", "1", nil, :value}, {"second", "2", nil, :value}]},
          {:delete_batch, ["first", "second"]}
        ] do
      refute CommandBatching.batchable?(command)
      refute CommandBatching.batchable?({:ferricstore_latency_trace, command})
    end

    assert CommandBatching.batchable?({:mset, [{"first", "1", nil}, {"second", "2", nil}]})
  end

  test "top-level-only commands bypass namespace coalescing" do
    for tag <- [
          :clear_key_locks,
          :expire_if_batch,
          :fetch_or_compute_fail,
          :fetch_or_compute_lock,
          :fetch_or_compute_publish,
          :fetch_or_compute_publish_blob_ref,
          :fetch_or_compute_release,
          :flow_governance_limit_catalog_outbox_ack,
          :flow_governance_release_outbox_ack,
          :flow_governance_release_outbox_mark_completed,
          :flow_policy_attribute_catalog_repair,
          :flow_policy_attribute_catalog_repair_request,
          :flow_policy_catalog_backfill_step,
          :flow_policy_migration_step,
          :flow_policy_patch_allocate,
          :flow_policy_put,
          :flush_shard,
          :key_lifecycle,
          :server_catalog_mutate,
          :server_catalog_replace,
          :ttb,
          :zadd_many_single
        ] do
      command = {tag, :payload}

      refute CommandBatching.batchable?(command), "#{inspect(tag)} must commit directly"

      refute CommandBatching.batchable?({:ferricstore_latency_trace, command}),
             "wrapped #{inspect(tag)} must commit directly"
    end

    assert CommandBatching.batchable?({:set, "key", "value", nil, []})
    assert CommandBatching.batchable?({:cms_incrby, "key", [{"item", 1}]})
  end
end
