defmodule Ferricstore.Store.ActiveFileTest do
  @moduledoc """
  Tests for `Ferricstore.Store.ActiveFile` — the active file registry that
  replaces persistent_term to avoid global GC on file rotation.

  Also tests the configurable `max-active-file-size` setting.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

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
    test "uses one instance-qualified key shape for the default instance" do
      ActiveFile.publish(0, 77_001, "/tmp/default-active.log", "/tmp")

      assert [
               {{:default, 0}, _generation, 77_001, "/tmp/default-active.log", "/tmp"}
             ] = :ets.lookup(:ferricstore_active_files, {:default, 0})

      assert [] = :ets.lookup(:ferricstore_active_files, 0)
    end

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

    @tag :active_file_scoped_generation
    test "publishing another shard does not invalidate this shard's process cache" do
      ctx = %{name: :active_file_scoped_generation, shard_count: 2}
      first_path = "/tmp/active-file-scoped-0.log"
      second_path = "/tmp/active-file-scoped-1.log"

      try do
        ActiveFile.publish(ctx, 0, 0, first_path, "/tmp")
        ActiveFile.publish(ctx, 1, 0, second_path, "/tmp")
        assert {0, ^first_path, "/tmp"} = ActiveFile.get(ctx, 0)

        cache_key = {:active_file_cache, {ctx.name, 0}}
        cached_first = Process.get(cache_key)

        ActiveFile.publish(ctx, 1, 1, second_path <> ".next", "/tmp")
        assert {0, ^first_path, "/tmp"} = ActiveFile.get(ctx, 0)
        assert Process.get(cache_key) == cached_first
      after
        ActiveFile.cleanup_instance(ctx)
      end
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
          shard_count: 1
        )

      on_exit(fn ->
        FerricStore.Instance.cleanup(ctx.name)
        File.rm_rf(dir)
      end)

      ActiveFile.publish(ctx, 0, 123, file_path, data_path)

      assert ActiveFile.get(0) == orig_af
      assert ActiveFile.get(ctx, 0) == {123, file_path, data_path}
    end

    test "custom instance cleanup removes active file rows and process cache" do
      dir =
        Path.join(
          System.tmp_dir!(),
          "active_file_cleanup_#{System.unique_integer([:positive])}"
        )

      data_path = Path.join(dir, "data/shard_0")
      file_path = Path.join(data_path, "00000.log")
      File.mkdir_p!(data_path)
      File.touch!(file_path)

      ctx =
        FerricStore.Instance.build(:active_file_cleanup_test,
          data_dir: dir,
          shard_count: 1
        )

      on_exit(fn ->
        FerricStore.Instance.cleanup(ctx.name)
        File.rm_rf(dir)
      end)

      ActiveFile.publish(ctx, 0, 456, file_path, data_path)

      assert ActiveFile.get(ctx, 0) == {456, file_path, data_path}

      FerricStore.Instance.cleanup(ctx.name)

      assert_raise MatchError, fn ->
        ActiveFile.get(ctx, 0)
      end
    end

    test "process cache is pruned when active-file generation changes" do
      try do
        for i <- 1..5 do
          ctx = %{name: :"active_file_cache_#{i}", shard_count: 1}
          path = "/tmp/active-file-cache-#{i}.log"

          ActiveFile.publish(ctx, 0, 0, path, "/tmp")
          assert {0, ^path, "/tmp"} = ActiveFile.get(ctx, 0)
          ActiveFile.cleanup_instance(ctx)
        end

        assert active_file_cache_size() <= 1
      after
        clear_active_file_cache()
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Configurable max-active-file-size (init-time only, via config.exs)
  # ---------------------------------------------------------------------------

  describe "max-active-file-size config" do
    test "default is 8GiB to avoid frequent high-throughput rollover" do
      assert :persistent_term.get(:ferricstore_max_active_file_size) == 8 * 1024 * 1024 * 1024
    end

    test "shard reads max_active_file_size from persistent_term at init" do
      # Shards cache this in state at init. Verify persistent_term is the source.
      val = :persistent_term.get(:ferricstore_max_active_file_size)
      assert is_integer(val)
      assert val >= 1_048_576
    end
  end

  defp active_file_cache_size do
    Process.get()
    |> Enum.count(fn
      {{:active_file_cache, _key}, _value} -> true
      _ -> false
    end)
  end

  defp clear_active_file_cache do
    Process.get()
    |> Enum.each(fn
      {{:active_file_cache, _key} = key, _value} -> Process.delete(key)
      {:active_file_cache_generation, _value} -> Process.delete(:active_file_cache_generation)
      _ -> :ok
    end)
  end
end
