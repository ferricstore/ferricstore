Code.require_file("write_path_test/sections/set_via_router_goes_through_raft.exs", __DIR__)
Code.require_file("write_path_test/sections/list_op_lpush_through_raft_adds_element.exs", __DIR__)
Code.require_file("write_path_test/sections/ratelimit_add_through_raft.exs", __DIR__)

defmodule Ferricstore.Raft.WritePathTest do
  @moduledoc """
  Tests for the Raft-integrated write path.

  Verifies that all write operations (SET, DEL, INCR, MULTI/EXEC) route
  through the Raft Batcher and StateMachine before updating ETS and Bitcask,
  rather than writing directly from the Shard GenServer.
  """

  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    ShardHelpers.wait_default_pipeline_ready()

    # The application already started WARaft partitions and
    # batchers for shards 0-3. Reuse them.
    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Helper to generate unique keys
  defp ukey(base), do: "raft_wp_#{base}_#{:rand.uniform(9_999_999)}"

  defp keydir_for(key), do: :"keydir_#{Router.shard_for(FerricStore.Instance.get(:default), key)}"

  defp shard_pid_for(key) do
    name =
      Router.shard_name(
        FerricStore.Instance.get(:default),
        Router.shard_for(FerricStore.Instance.get(:default), key)
      )

    Process.whereis(name)
  end

  # ---------------------------------------------------------------------------
  # 1. SET via Router goes through Raft when enabled
  # ---------------------------------------------------------------------------

  use Ferricstore.Raft.WritePathTest.Sections.SetViaRouterGoesThroughRaft

  defp fresh_sm_state do
    suffix = System.unique_integer([:positive])
    dir = Path.join(System.tmp_dir!(), "wp_sm_#{suffix}")
    shard_path = Ferricstore.DataDir.shard_data_path(dir, 0)
    Ferricstore.DataDir.ensure_layout!(dir, 1)

    active_file_path = Path.join(shard_path, "00000.log")
    File.touch!(active_file_path)

    instance_ctx =
      FerricStore.Instance.build(:"wp_sm_#{suffix}",
        data_dir: dir,
        shard_count: 1,
        hot_cache_max_value_size: 65_536
      )

    keydir = elem(instance_ctx.keydir_refs, 0)
    :ets.new(keydir, [:set, :public, :named_table])

    compound_member_index =
      Ferricstore.Store.Shard.CompoundMemberIndex.table_name(instance_ctx.name, 0)

    Ferricstore.Store.Shard.CompoundMemberIndex.ensure_table!(compound_member_index)
    Ferricstore.Store.Shard.CompoundMemberIndex.reset(compound_member_index)
    Ferricstore.Store.ActiveFile.init(1)

    Ferricstore.Store.ActiveFile.publish(
      instance_ctx,
      0,
      0,
      active_file_path,
      shard_path
    )

    state =
      Ferricstore.Raft.StateMachine.init(%{
        shard_index: 0,
        shard_data_path: shard_path,
        data_dir: dir,
        active_file_id: 0,
        active_file_path: active_file_path,
        ets: keydir,
        instance_ctx: instance_ctx,
        instance_name: instance_ctx.name
      })

    {state, keydir, instance_ctx, dir}
  end

  defp cleanup_sm({_state, _ets, instance_ctx, dir}) do
    FerricStore.Instance.cleanup(instance_ctx.name)
    cleanup_sm_indexes(instance_ctx.name)
    File.rm_rf!(dir)
  end

  defp cleanup_sm_indexes(instance_name) do
    {logical_key_index, logical_key_slots} =
      Ferricstore.Store.Shard.LogicalKeyIndex.table_names(instance_name, 0)

    [
      Ferricstore.Store.Shard.CompoundMemberIndex.table_name(instance_name, 0),
      Ferricstore.Store.Shard.CompoundRevisionIndex.table_name(instance_name, 0),
      logical_key_index,
      logical_key_slots
    ]
    |> Enum.each(fn table ->
      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)
  end

  alias Ferricstore.Raft.StateMachine, as: SM

  # ---------------------------------------------------------------------------
  # 12. list_op — LPUSH through Raft adds element
  # ---------------------------------------------------------------------------

  use Ferricstore.Raft.WritePathTest.Sections.ListOpLpushThroughRaftAddsElement

  use Ferricstore.Raft.WritePathTest.Sections.RatelimitAddThroughRaft
end
