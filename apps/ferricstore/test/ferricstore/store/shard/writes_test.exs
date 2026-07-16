defmodule Ferricstore.Store.Shard.WritesTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Shard.Writes
  alias Ferricstore.Store.Shard

  test "direct put returns append error and rolls back ETS when flush fails" do
    keydir = :ets.new(:"writes_test_#{System.unique_integer([:positive])}", [:set, :public])

    state = %{
      active_file_path: Path.join(System.tmp_dir!(), "missing/writes_test.log"),
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      index: 0,
      instance_ctx: %{
        checkpoint_flags: :atomics.new(1, signed: false),
        disk_pressure: :atomics.new(1, signed: false),
        hot_cache_max_value_size: 65_536,
        keydir_binary_bytes: :atomics.new(1, signed: true)
      },
      keydir: keydir,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: System.tmp_dir!(),
      write_version: 0
    }

    try do
      assert {:reply, {:error, _reason}, new_state} =
               Writes.handle_put("key", "value", 0, {self(), make_ref()}, state)

      assert [] == :ets.lookup(keydir, "key")
      assert new_state.pending == []
      assert new_state.pending_count == 0
    after
      :ets.delete(keydir)
    end
  end

  test "direct INCR returns append error and restores value, counters, and version" do
    keydir = :ets.new(:writes_incr_failure, [:set, :public])
    state = direct_state(keydir)

    Ferricstore.Store.Shard.ETS.ets_insert_with_location(
      state,
      "counter",
      1,
      0,
      0,
      0,
      1
    )

    assert {:hit, original_value, 0} =
             Ferricstore.Store.Shard.ETS.ets_lookup(state, "counter")

    try do
      assert {:reply, {:error, _reason}, failed_state} =
               Writes.handle_incr("counter", 1, {self(), make_ref()}, state)

      assert {:hit, ^original_value, 0} =
               Ferricstore.Store.Shard.ETS.ets_lookup(state, "counter")

      assert failed_state.pending == []
      assert failed_state.pending_count == 0
      assert failed_state.write_version == state.write_version
    after
      :ets.delete(keydir)
    end
  end

  test "direct prefix delete does not account dead bytes when its tombstone batch fails" do
    keydir = :ets.new(:writes_prefix_delete_failure, [:set, :public])
    state = direct_state(keydir)

    Ferricstore.Store.Shard.ETS.ets_insert_with_location(
      state,
      "prefix:key",
      "value",
      0,
      0,
      0,
      5
    )

    try do
      assert {:reply, {:error, _reason}, failed_state} =
               Writes.handle_delete_prefix("prefix:", state)

      assert {:hit, "value", 0} =
               Ferricstore.Store.Shard.ETS.ets_lookup(state, "prefix:key")

      assert failed_state.file_stats == state.file_stats
      assert failed_state.write_version == state.write_version
    after
      :ets.delete(keydir)
    end
  end

  test "transaction pending writes keep pending_count in sync after a failed drain" do
    keydir = :ets.new(:writes_tx_pending_count, [:set, :public])
    state = direct_state(keydir)

    try do
      assert {:noreply, new_state} =
               Shard.handle_info({:tx_pending_write, "key", "value", 0}, state)

      assert new_state.pending == [{"key", "value", 0}]
      assert new_state.pending_count == 1
      assert new_state.write_version == state.write_version + 1
    after
      :ets.delete(keydir)
    end
  end

  test "file size inspection ignores nonnumeric filenames and numeric symlinks" do
    dir =
      Path.join(System.tmp_dir!(), "writes_file_sizes_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "00000.log"), "valid")
    File.write!(Path.join(dir, "not-a-segment.log"), "noise")

    external =
      Path.join(System.tmp_dir!(), "writes_external_#{System.unique_integer([:positive])}")

    File.write!(external, "external-segment")
    File.ln_s!(external, Path.join(dir, "00001.log"))

    state = %{
      flush_in_flight: nil,
      index: 0,
      instance_ctx: nil,
      pending: [],
      shard_data_path: dir
    }

    try do
      assert {:reply, {:ok, [{0, 5}]}, ^state} = Shard.handle_call(:file_sizes, nil, state)
    after
      File.rm_rf!(dir)
      File.rm(external)
    end
  end

  test "shard stats use tracked dead bytes instead of a file-count heuristic" do
    dir =
      Path.join(System.tmp_dir!(), "writes_shard_stats_#{System.unique_integer([:positive])}")

    keydir = :ets.new(:writes_shard_stats, [:set, :public])
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "00000.log"), :binary.copy("a", 100))
    File.write!(Path.join(dir, "00001.log"), :binary.copy("b", 50))
    File.write!(Path.join(dir, "compact_0.log"), :binary.copy("x", 200))

    state = %{
      file_stats: %{0 => {100, 40}, 1 => {50, 10}},
      flush_in_flight: nil,
      index: 0,
      instance_ctx: nil,
      keydir: keydir,
      pending: [],
      shard_data_path: dir
    }

    try do
      assert {:reply, {:ok, {150, 100, 50, 2, 0, fragmentation}}, ^state} =
               Shard.handle_call(:shard_stats, nil, state)

      assert_in_delta fragmentation, 1.0 / 3.0, 1.0e-12
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  defp direct_state(keydir) do
    %{
      active_file_path: Path.join(System.tmp_dir!(), "missing/writes_test.log"),
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      index: 0,
      instance_ctx: %{
        checkpoint_flags: :atomics.new(1, signed: false),
        disk_pressure: :atomics.new(1, signed: false),
        hot_cache_max_value_size: 65_536,
        keydir_binary_bytes: :atomics.new(1, signed: true)
      },
      keydir: keydir,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: System.tmp_dir!(),
      write_version: 11
    }
  end
end
