defmodule Ferricstore.Store.DedicatedCompactionTest do
  @moduledoc """
  Tests for dedicated promoted Bitcask compaction.

  Covers: manual promoted compaction, file rotation during compaction, crash
  recovery with multiple files, ETS offset correctness after compaction,
  old file cleanup, and edge cases.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.{CompoundKey, LFU, Promotion, Router}
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Test.ShardHelpers

  # Low threshold so we can trigger promotion in tests
  @test_threshold 5
  @compaction_churn 20

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    apply_context_snapshot =
      ShardHelpers.replace_default_apply_context(promotion_threshold: @test_threshold)

    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(apply_context_snapshot)
      ShardHelpers.flush_all_keys()
      ShardHelpers.wait_shards_alive()
    end)
  end

  defp real_store, do: ShardHelpers.router_store()

  defp ukey(base), do: "#{base}_#{:rand.uniform(9_999_999)}"

  defp promote_hash(store, key) do
    pairs =
      Enum.flat_map(1..(@test_threshold + 1), fn i ->
        ["field_#{i}", "value_#{i}"]
      end)

    Hash.handle("HSET", [key | pairs], store)

    ShardHelpers.eventually(
      fn -> promoted?(key) end,
      "hash should be promoted before dedicated compaction"
    )

    key
  end

  defp promoted?(key) do
    ctx = FerricStore.Instance.get(:default)
    shard_idx = Router.shard_for(ctx, key)
    GenServer.call(Router.shard_name(ctx, shard_idx), {:promoted?, key})
  end

  defp dedicated_dir(key) do
    shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
    data_dir = Application.get_env(:ferricstore, :data_dir, "data")
    Promotion.dedicated_path(data_dir, shard_idx, :hash, key)
  end

  defp promoted_path!(state, key) do
    case ShardCompound.promoted_store(state, key) do
      path when is_binary(path) ->
        path

      nil ->
        flunk("expected promoted path for #{inspect(key)}")
    end
  end

  defp compact_promoted_key!(key) do
    ctx = FerricStore.Instance.get(:default)
    shard_idx = Router.shard_for(ctx, key)
    shard = Router.shard_name(ctx, shard_idx)

    :sys.replace_state(shard, fn state ->
      dedicated_path = promoted_path!(state, key)
      ShardCompound.compact_dedicated(state, key, dedicated_path)
    end)
  end

  defp attach_dedicated_compaction_failed_handler do
    parent = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :dedicated, :compaction_failed],
        fn event, measurements, metadata, _config ->
          send(parent, {:dedicated_compaction_failed, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp log_file_count(dir) do
    case File.ls(dir) do
      {:ok, files} -> Enum.count(files, &String.ends_with?(&1, ".log"))
      _ -> 0
    end
  end

  defp log_files(dir) do
    case File.ls(dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.sort()

      _ ->
        []
    end
  end

  # ---------------------------------------------------------------------------
  # Promoted hash with offset-based reads
  # ---------------------------------------------------------------------------

  describe "promoted reads use offsets" do
    test "HGET on promoted hash returns correct value" do
      store = real_store()
      key = ukey("offset_read")
      promote_hash(store, key)

      assert "value_3" == Hash.handle("HGET", [key, "field_3"], store)
    end

    test "HSET on promoted hash stores offset in ETS" do
      store = real_store()
      key = ukey("offset_ets")
      promote_hash(store, key)

      Hash.handle("HSET", [key, "new_field", "new_value"], store)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{shard_idx}"
      compound_key = "H:#{key}\0new_field"

      case :ets.lookup(keydir, compound_key) do
        [{^compound_key, _val, _exp, _lfu, fid, offset, vsize}] ->
          assert is_integer(fid)
          assert offset > 0, "Expected non-zero offset, got #{offset}"
          assert vsize == byte_size("new_value")

        [] ->
          flunk("Compound key not found in ETS")
      end
    end

    test "HGETALL on promoted hash returns all fields" do
      store = real_store()
      key = ukey("offset_getall")
      promote_hash(store, key)

      pairs = Hash.handle("HGETALL", [key], store)
      assert is_list(pairs)
      assert length(pairs) == (@test_threshold + 1) * 2
    end

    test "HGET on promoted hash reads cold offset zero from recorded dedicated fid" do
      store = real_store()
      key = ukey("offset_zero_cold")
      promote_hash(store, key)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      keydir = :"keydir_#{shard_idx}"
      dir = dedicated_dir(key)
      compound_key = "H:#{key}\0cold_zero"
      value = "cold-offset-zero"

      old_file = Path.join(dir, "00099.log")
      active_file = Path.join(dir, "00100.log")

      shared_file =
        Ferricstore.DataDir.shard_data_path(
          FerricStore.Instance.get(:default).data_dir,
          shard_idx
        )
        |> Path.join("00099.log")

      {:ok, {0, record_size}} = NIF.v2_append_record(old_file, compound_key, value, 0)

      {:ok, {0, _shared_record_size}} =
        NIF.v2_append_record(shared_file, compound_key, "wrong", 0)

      File.touch!(active_file)

      :ets.insert(
        keydir,
        {compound_key, nil, 0, Ferricstore.Store.LFU.initial(), 99, 0, record_size}
      )

      assert value == Hash.handle("HGET", [key, "cold_zero"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Manual promoted compaction after write/delete churn
  # ---------------------------------------------------------------------------

  describe "manual promoted compaction" do
    test "manual compaction works after restart with cold promotion marker" do
      store = real_store()
      key = ukey("restart_compact")
      promote_hash(store, key)

      dir = dedicated_dir(key)
      assert log_files(dir) == ["00000.log"]

      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_for_key(key)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      shard = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)
      state = :sys.get_state(shard)
      marker_key = Promotion.marker_key(key)

      assert [{^marker_key, "hash", _exp, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(state.keydir, marker_key)

      :sys.replace_state(shard, fn state ->
        dedicated_path = promoted_path!(state, key)
        ShardCompound.compact_dedicated(state, key, dedicated_path)
      end)

      assert log_files(dir) == ["00001.log"]
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "dedicated compaction ignores non-numeric log-shaped files" do
      store = real_store()
      key = ukey("compact_stray_log")
      promote_hash(store, key)

      dir = dedicated_dir(key)
      File.write!(Path.join(dir, "notes.log"), "not a bitcask log")

      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, shard_idx)

      :sys.replace_state(shard, fn state ->
        dedicated_path = promoted_path!(state, key)
        ShardCompound.compact_dedicated(state, key, dedicated_path)
      end)

      assert File.exists?(Path.join(dir, "notes.log"))
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "manual compaction after many writes keeps data readable" do
      store = real_store()
      key = ukey("compact_trigger")
      promote_hash(store, key)

      dir = dedicated_dir(key)
      assert log_file_count(dir) >= 1

      for i <- 1..@compaction_churn do
        Hash.handle("HSET", [key, "churn_field", "value_#{i}"], store)
      end

      compact_promoted_key!(key)

      # All original fields still readable
      for i <- 1..(@test_threshold + 1) do
        expected = "value_#{i}"
        actual = Hash.handle("HGET", [key, "field_#{i}"], store)
        assert actual == expected, "field_#{i}: expected #{expected}, got #{inspect(actual)}"
      end

      # Churned field has latest value
      assert "value_#{@compaction_churn}" == Hash.handle("HGET", [key, "churn_field"], store)
    end

    test "writes after compaction still work" do
      store = real_store()
      key = ukey("post_compact")
      promote_hash(store, key)

      for i <- 1..@compaction_churn do
        Hash.handle("HSET", [key, "churn", "v#{i}"], store)
      end

      compact_promoted_key!(key)

      Hash.handle("HSET", [key, "after_compact", "new_value"], store)
      assert "new_value" == Hash.handle("HGET", [key, "after_compact"], store)
    end

    test "manual compaction after delete churn keeps original fields" do
      store = real_store()
      key = ukey("del_compact")
      promote_hash(store, key)

      for i <- 1..@compaction_churn do
        if rem(i, 2) == 0 do
          Hash.handle("HDEL", [key, "temp_field"], store)
        else
          Hash.handle("HSET", [key, "temp_field", "v#{i}"], store)
        end
      end

      compact_promoted_key!(key)

      # Original fields survive
      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "HLEN is correct after compaction" do
      store = real_store()
      key = ukey("hlen_compact")
      promote_hash(store, key)

      expected_count = @test_threshold + 1

      # Overwrites only — no new fields
      for i <- 1..@compaction_churn do
        Hash.handle("HSET", [key, "field_1", "overwrite_#{i}"], store)
      end

      compact_promoted_key!(key)

      count = Hash.handle("HLEN", [key], store)
      assert count == expected_count
    end

    test "concurrent promoted HSET wins over stale compaction snapshot" do
      store = real_store()
      key = ukey("compact_hset_race")
      promote_hash(store, key)

      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, shard_idx)
      state = :sys.get_state(shard)
      dedicated_path = promoted_path!(state, key)
      compound_key = CompoundKey.hash_field(key, "field_1")

      assert [{^compound_key, "value_1", 0, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(state.keydir, compound_key)

      test_pid = self()

      Process.put(:ferricstore_promoted_compaction_after_collect_hook, fn ^key, live_entries ->
        if Enum.any?(live_entries, fn {entry_key, value, _exp, _source_row} ->
             entry_key == compound_key and value == "value_1"
           end) do
          task =
            Task.async(fn ->
              Hash.handle("HSET", [key, "field_1", "newer_value"], store)
            end)

          send(test_pid, {:promoted_compaction_race_task, task})
          Process.sleep(50)
        end
      end)

      try do
        ShardCompound.compact_dedicated(state, key, dedicated_path)
      after
        Process.delete(:ferricstore_promoted_compaction_after_collect_hook)
      end

      assert_receive {:promoted_compaction_race_task, task}
      Task.await(task, 5_000)

      assert "newer_value" == Hash.handle("HGET", [key, "field_1"], store)
    end

    test "dedicated compaction aborts when a live cold entry cannot be read" do
      store = real_store()
      key = ukey("compact_cold_missing")
      promote_hash(store, key)

      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, shard_idx)
      state = :sys.get_state(shard)
      dedicated_path = promoted_path!(state, key)
      missing_compound_key = CompoundKey.hash_field(key, "missing_cold")

      refute File.exists?(Path.join(dedicated_path, "00099.log"))

      :ets.insert(
        state.keydir,
        {missing_compound_key, nil, 0, LFU.initial(), 99, 0, 128}
      )

      :ok = CompoundMemberIndex.put(state.compound_member_index, missing_compound_key)

      assert log_files(dedicated_path) == ["00000.log"]

      attach_dedicated_compaction_failed_handler()

      ShardCompound.compact_dedicated(state, key, dedicated_path)

      assert_receive {:dedicated_compaction_failed,
                      [:ferricstore, :dedicated, :compaction_failed], %{count: 1, error_count: 1},
                      %{
                        shard_index: ^shard_idx,
                        phase: :collect_live_entries,
                        reason: :cold_read_failed,
                        path: ^dedicated_path,
                        redis_key_hash: key_hash
                      }}

      assert is_integer(key_hash)

      assert File.exists?(Path.join(dedicated_path, "00000.log")),
             "old dedicated log must stay until every live cold row was copied"

      assert File.exists?(Path.join(dedicated_path, "00001.log")),
             "failed compaction must retain an already-published metadata page"
    end

    test "dedicated compaction reports old-log directory listing failures" do
      store = real_store()
      key = ukey("compact_cleanup_list_failure")
      promote_hash(store, key)

      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, shard_idx)
      state = :sys.get_state(shard)
      dedicated_path = promoted_path!(state, key)

      attach_dedicated_compaction_failed_handler()

      Process.put(:ferricstore_promoted_compaction_list_hook, fn ^dedicated_path ->
        {:error, {:permission_denied, "injected list failure"}}
      end)

      try do
        assert {:error, ^state} =
                 ShardCompound.compact_dedicated_result(state, key, dedicated_path)
      after
        Process.delete(:ferricstore_promoted_compaction_list_hook)
      end

      assert_receive {:dedicated_compaction_failed,
                      [:ferricstore, :dedicated, :compaction_failed], %{count: 1, error_count: 1},
                      %{
                        shard_index: ^shard_idx,
                        phase: :remove_old_logs,
                        reason: :list_old_logs_failed,
                        path: ^dedicated_path
                      }}

      assert log_files(dedicated_path) == ["00000.log", "00001.log"]
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery with multiple files (crash simulation)
  # ---------------------------------------------------------------------------

  describe "recovery with multiple files" do
    @describetag :shard_kill

    test "data survives shard restart after compaction" do
      store = real_store()
      key = ukey("recover_compact")
      promote_hash(store, key)

      for i <- 1..@compaction_churn do
        Hash.handle("HSET", [key, "churn", "v#{i}"], store)
      end

      compact_promoted_key!(key)

      assert "value_1" == Hash.handle("HGET", [key, "field_1"], store)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      ShardHelpers.eventually(
        fn ->
          "value_1" == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 should survive restart after compaction"
      )
    end

    test "crash leaves two files — recovery reads both" do
      store = real_store()
      key = ukey("crash_two")
      promote_hash(store, key)

      dir = dedicated_dir(key)

      # Simulate crash: manually create second file with extra entry
      new_file = Path.join(dir, "00001.log")
      File.touch!(new_file)
      NIF.v2_append_record(new_file, "H:#{key}\0crash_field", "crash_value", 0)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      # Old file entries survive
      ShardHelpers.eventually(
        fn ->
          "value_1" == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 from old file should survive"
      )

      # New file entries survive
      ShardHelpers.eventually(
        fn ->
          "crash_value" == Hash.handle("HGET", [key, "crash_field"], store)
        end,
        "crash_field from new file should survive"
      )
    end

    test "last-write-wins across files on recovery" do
      store = real_store()
      key = ukey("lww_recov")
      promote_hash(store, key)

      dir = dedicated_dir(key)

      # Write conflicting value in newer file
      new_file = Path.join(dir, "00001.log")
      File.touch!(new_file)
      NIF.v2_append_record(new_file, "H:#{key}\0field_1", "overwritten", 0)

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      ShardHelpers.eventually(
        fn ->
          "overwritten" == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 should have value from newer file"
      )
    end

    test "tombstone in newer file deletes entry from older file" do
      store = real_store()
      key = ukey("tomb_recov")
      promote_hash(store, key)

      dir = dedicated_dir(key)

      new_file = Path.join(dir, "00001.log")
      File.touch!(new_file)
      NIF.v2_append_tombstone(new_file, "H:#{key}\0field_1")

      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      ShardHelpers.flush_all_shards()
      ShardHelpers.kill_shard_safely(shard_idx)

      # field_1 deleted
      ShardHelpers.eventually(
        fn ->
          nil == Hash.handle("HGET", [key, "field_1"], store)
        end,
        "field_1 should be deleted by tombstone"
      )

      # Others survive
      ShardHelpers.eventually(
        fn ->
          "value_2" == Hash.handle("HGET", [key, "field_2"], store)
        end,
        "field_2 should survive"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    @tag :stale_promotion_marker_restart
    test "segment projection ignores a stale physical promotion marker on restart" do
      store = real_store()
      live_key = ukey("live_marker_restart")
      promote_hash(store, live_key)

      key = ukey("stale_marker_restart")
      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      marker_key = CompoundKey.promotion_marker_key(key)

      assert [] = :ets.lookup(elem(ctx.keydir_refs, shard_idx), marker_key)
      refute File.dir?(dedicated_dir(key))
      assert :ok = ShardHelpers.flush_all_shards()

      {_active_file_id, active_file_path, _shard_data_path} =
        Ferricstore.Store.ActiveFile.get(ctx, shard_idx)

      server_started? =
        Enum.any?(Application.started_applications(), fn {app, _description, _version} ->
          app == :ferricstore_server
        end)

      if server_started?, do: assert(:ok = Application.stop(:ferricstore_server))
      assert :ok = Application.stop(:ferricstore)
      _ = Ferricstore.Raft.WARaftBackend.stop()

      assert {:ok, _location} = NIF.v2_append_record(active_file_path, marker_key, "hash", 0)
      assert :ok = NIF.v2_fsync(active_file_path)

      assert {:ok, _started} = Application.ensure_all_started(:ferricstore)

      if server_started? do
        assert {:ok, _started} = Application.ensure_all_started(:ferricstore_server)
      end

      assert :ok = ShardHelpers.wait_default_pipeline_ready()
      assert {:ok, "none"} = FerricStore.type(key)
      assert "value_1" == Hash.handle("HGET", [live_key, "field_1"], real_store())
    end

    test "compaction with all fields deleted" do
      store = real_store()
      key = ukey("all_del")
      promote_hash(store, key)

      for i <- 1..(@test_threshold + 1) do
        Hash.handle("HDEL", [key, "field_#{i}"], store)
      end

      ctx = FerricStore.Instance.get(:default)
      shard_idx = Router.shard_for(ctx, key)
      shard = Router.shard_name(ctx, shard_idx)

      :sys.replace_state(shard, fn state ->
        dedicated_path = promoted_path!(state, key)
        ShardCompound.compact_dedicated(state, key, dedicated_path)
      end)

      # Should not crash
      dir = dedicated_dir(key)
      assert log_file_count(dir) >= 1
    end

    test "concurrent reads and writes on promoted hash" do
      store = real_store()
      key = ukey("concurrent")
      promote_hash(store, key)

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            if rem(i, 2) == 0 do
              Hash.handle("HSET", [key, "conc_field", "val_#{i}"], store)
            else
              Hash.handle("HGET", [key, "field_1"], store)
            end
          end)
        end

      results = Task.await_many(tasks, 10_000)
      # No crashes — all tasks completed
      assert length(results) == 50
    end
  end
end
