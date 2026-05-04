defmodule Ferricstore.ReviewR4.CompactionEtsOffsetTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias Ferricstore.Test.IsolatedInstance

  @moduletag :review_r4
  @moduletag :compaction_bug

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 0)

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{
      shard: elem(ctx.shard_names, 0),
      keydir: elem(ctx.keydir_refs, 0)
    }
  end

  describe "C2/C3: v2 compaction updates ETS offsets" do
    test "cold reads stay correct after compaction removes a dead middle record", %{
      shard: shard,
      keydir: keydir
    } do
      assert :ok = GenServer.call(shard, {:put, "c2_key_1", "value_1", 0})
      assert :ok = GenServer.call(shard, {:put, "c2_key_2", "value_2_deleted", 0})
      assert :ok = GenServer.call(shard, {:put, "c2_key_3", "value_3_survives", 0})
      assert :ok = GenServer.call(shard, :flush)

      assert :ok = GenServer.call(shard, {:delete, "c2_key_2"})

      force_rotate_active_file(shard)

      [{_, nil, _, _, 0, offset_1_before, _}] = :ets.lookup(keydir, "c2_key_1")
      [{_, nil, _, _, 0, offset_3_before, _}] = :ets.lookup(keydir, "c2_key_3")

      assert {:ok, {2, 0, reclaimed}} = GenServer.call(shard, {:run_compaction, [0]})
      assert reclaimed > 0

      assert "value_1" == GenServer.call(shard, {:get, "c2_key_1"})
      assert "value_3_survives" == GenServer.call(shard, {:get, "c2_key_3"})

      [{_, nil, _, _, 0, offset_1_after, _}] = :ets.lookup(keydir, "c2_key_1")
      [{_, nil, _, _, 0, offset_3_after, _}] = :ets.lookup(keydir, "c2_key_3")

      assert offset_1_after != offset_1_before or offset_3_after != offset_3_before
    end

    test "copy failure is returned as a compaction error instead of success", %{
      shard: shard,
      keydir: keydir
    } do
      missing_file_id = 999
      key = "compaction_missing_source"

      :ets.insert(keydir, {key, nil, 0, 0, missing_file_id, 0, 16})

      assert {:error, {:compaction_failed, failures}} =
               GenServer.call(shard, {:run_compaction, [missing_file_id]})

      assert [{^missing_file_id, {:copy_failed, _reason}}] = failures
      assert [{^key, nil, 0, 0, ^missing_file_id, 0, 16}] = :ets.lookup(keydir, key)
    end

    test "copy failure reports temp cleanup failure", %{shard: shard} do
      assert :ok = GenServer.call(shard, {:put, "copy_cleanup_key", "value_1", 0})
      assert :ok = GenServer.call(shard, :flush)

      force_rotate_active_file(shard)

      state = :sys.get_state(shard)
      compact_tmp_dir = Path.join(state.shard_data_path, "compact_0.log")
      File.mkdir!(compact_tmp_dir)

      log =
        capture_log(fn ->
          assert {:error, {:compaction_failed, [{0, {:copy_failed, _reason}}]}} =
                   GenServer.call(shard, {:run_compaction, [0]})
        end)

      assert log =~ "failed to remove compaction temp file"
      assert File.dir?(compact_tmp_dir)
    end

    test "directory fsync failure after namespace changes is returned as compaction error", %{
      shard: shard,
      keydir: keydir
    } do
      assert :ok = GenServer.call(shard, {:put, "fsync_fail_key", "value_1", 0})
      assert :ok = GenServer.call(shard, {:put, "fsync_fail_dead", "dead_value", 0})
      assert :ok = GenServer.call(shard, :flush)

      assert :ok = GenServer.call(shard, {:delete, "fsync_fail_dead"})
      force_rotate_active_file(shard)

      :sys.replace_state(shard, fn state ->
        Map.put(state, :compaction_fsync_dir_fun, fn _path -> {:error, :eio} end)
      end)

      assert {:error, {:compaction_failed, failures}} =
               GenServer.call(shard, {:run_compaction, [0]})

      assert [{:dir_fsync_failed, :eio}] = failures
      assert [{_, nil, 0, _, 0, _offset, _size}] = :ets.lookup(keydir, "fsync_fail_key")
    end
  end

  defp force_rotate_active_file(shard) do
    :sys.replace_state(shard, fn state ->
      new_id = state.active_file_id + 1
      sp = state.shard_data_path
      new_path = Ferricstore.Store.Shard.ETS.file_path(sp, new_id)

      Ferricstore.FS.touch!(new_path)
      Ferricstore.Store.ActiveFile.publish(state.instance_ctx, state.index, new_id, new_path, sp)

      %{
        state
        | active_file_id: new_id,
          active_file_path: new_path,
          active_file_size: 0,
          file_stats: Map.put(state.file_stats, new_id, {0, 0})
      }
    end)
  end
end
