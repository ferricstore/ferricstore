defmodule Ferricstore.Store.ActiveFileTest do
  @moduledoc """
  Tests for `Ferricstore.Store.ActiveFile` — the active file registry that
  replaces persistent_term to avoid global GC on file rotation.

  Also tests the configurable `max-active-file-size` setting.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Store.ActiveFile
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    # Save original active file state for shard 0 so we can restore it
    orig_af = ActiveFile.get(0)

    on_exit(fn ->
      {fid, path, data_path} = orig_af
      ActiveFile.publish(0, fid, path, data_path)
      ShardHelpers.flush_all_keys()
    end)

    %{orig_af: orig_af}
  end

  # ---------------------------------------------------------------------------
  # ActiveFile registry basics
  # ---------------------------------------------------------------------------

  describe "ActiveFile registry" do
    test "get/1 returns valid file metadata for each shard" do
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        {file_id, file_path, shard_data_path} = ActiveFile.get(i)

        assert is_integer(file_id)
        assert file_id >= 0
        assert is_binary(file_path)
        assert is_binary(shard_data_path)
      end
    end

    test "publish/4 updates the value returned by get/1", %{
      orig_af: {_orig_fid, _, orig_data_path}
    } do
      new_path = Path.join(orig_data_path, "99_999.log")
      ActiveFile.publish(0, 99_999, new_path, orig_data_path)

      {file_id, file_path, _} = ActiveFile.get(0)
      assert file_id == 99_999
      assert file_path == new_path
    end

    test "process dictionary cache is invalidated on publish", %{orig_af: {_, _, data_path}} do
      # First read caches in process dictionary
      _cached = ActiveFile.get(0)

      # Publish a new value
      new_path = Path.join(data_path, "88_888.log")
      ActiveFile.publish(0, 88_888, new_path, data_path)

      # Next read should see the new value (cache invalidated)
      {fid2, path2, _} = ActiveFile.get(0)
      assert fid2 == 88_888
      assert path2 == new_path
    end

    test "get/1 from different processes sees published values", %{orig_af: {_, _, data_path}} do
      new_path = Path.join(data_path, "77_777.log")
      ActiveFile.publish(0, 77_777, new_path, data_path)

      # Read from a different process (no cached value)
      {fid, path, _} =
        Task.async(fn -> ActiveFile.get(0) end)
        |> Task.await(5000)

      assert fid == 77_777
      assert path == new_path
    end

    test "custom instance active file does not overwrite default shard entry",
         %{orig_af: orig_af} do
      dir =
        Path.join(System.tmp_dir!(), "active_file_scope_#{System.unique_integer([:positive])}")

      data_path = Path.join(dir, "data/shard_0")
      file_path = Path.join(data_path, "00000.log")
      File.mkdir_p!(data_path)
      File.touch!(file_path)

      ctx =
        FerricStore.Instance.build(:active_file_scope_test,
          data_dir: dir,
          shard_count: 1,
          raft_enabled: false
        )

      on_exit(fn ->
        FerricStore.Instance.cleanup(ctx.name)
        File.rm_rf(dir)
      end)

      ActiveFile.publish(ctx, 0, 123, file_path, data_path)

      assert ActiveFile.get(0) == orig_af
      assert ActiveFile.get(ctx, 0) == {123, file_path, data_path}
    end
  end

  # ---------------------------------------------------------------------------
  # Configurable max-active-file-size (init-time only, via config.exs)
  # ---------------------------------------------------------------------------

  describe "max-active-file-size config" do
    test "default is 256MB" do
      assert :persistent_term.get(:ferricstore_max_active_file_size) == 256 * 1024 * 1024
    end

    test "shard reads max_active_file_size from persistent_term at init" do
      # Shards cache this in state at init. Verify persistent_term is the source.
      val = :persistent_term.get(:ferricstore_max_active_file_size)
      assert is_integer(val)
      assert val >= 1_048_576
    end
  end
end
