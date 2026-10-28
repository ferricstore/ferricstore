defmodule Ferricstore.Flow.PolicyBackfillReadCacheTest do
  @moduledoc """
  A policy catalog backfill reads every Flow of a shard. A Flow whose record
  hibernation or memory pressure left on disk stays there.
  """
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Flow.{Keys, LMDBWriter, PolicyMigration}
  alias Ferricstore.Store.{Keydir, Router}
  alias Ferricstore.Test.ShardHelpers

  setup do
    old_sweeper = Application.get_env(:ferricstore, :flow_retention_sweeper_enabled)
    Application.put_env(:ferricstore, :flow_retention_sweeper_enabled, false)
    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      if old_sweeper == nil,
        do: Application.delete_env(:ferricstore, :flow_retention_sweeper_enabled),
        else: Application.put_env(:ferricstore, :flow_retention_sweeper_enabled, old_sweeper)

      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    :ok
  end

  test "a backfill page reads a cold Flow record without caching it" do
    id = "cold-backfill-#{System.unique_integer([:positive])}"
    partition = "cold-backfill-partition"
    now_ms = System.system_time(:millisecond)

    assert :ok =
             FerricStore.flow_create(id,
               type: "cold-backfill",
               partition_key: partition,
               run_at_ms: now_ms,
               now_ms: now_ms
             )

    ctx = FerricStore.Instance.get(:default)
    assert :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 45_000)
    state_key = Keys.state_key(id, partition)
    shard_index = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_index)

    [{^state_key, value, _exp, _lfu, _file_id, _offset, _size} = row] =
      :ets.lookup(keydir, state_key)

    assert is_binary(value)
    assert Keydir.drop_value_unchanged_source(keydir, row)

    page = backfill_page(ctx, shard_index)

    assert [%{record_value: ^value}] =
             Enum.filter(page.candidates, &match?(%{kind: :state, state_key: ^state_key}, &1))

    assert [{^state_key, nil, _, _, _, _, _}] = :ets.lookup(keydir, state_key)
  end

  defp backfill_page(ctx, shard_index) do
    {:ok, source_token} = PolicyMigration.source_token(ctx, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    step = &Router.flow_policy_catalog_backfill_step(ctx, shard_index, &1)
    base = %{run_token: run_token, source_token: source_token, candidates: [], done?: false}

    assert {:ok, _} =
             step.(
               Map.merge(base, %{expected_cursor: "", cursor: PolicyMigration.snapshot_cursor()})
             )

    :ok = PolicyMigration.snapshot_primary_keydir(ctx, shard_index, run_token, 256, 2_097_152)
    work_cursor = PolicyMigration.work_cursor(run_token)

    assert {:ok, _} =
             step.(
               Map.merge(base, %{
                 expected_cursor: PolicyMigration.snapshot_cursor(),
                 cursor: work_cursor
               })
             )

    assert {:ok, page} =
             PolicyMigration.backfill_page(ctx, shard_index, work_cursor, 256, 2_097_152)

    page
  end
end
