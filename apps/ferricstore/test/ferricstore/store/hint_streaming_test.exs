defmodule Ferricstore.Store.HintStreamingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.HintBuilder
  alias Ferricstore.Store.HintFile
  alias Ferricstore.Store.SegmentLock
  alias Ferricstore.Store.Shard.Flush
  alias Ferricstore.Store.Shard.Lifecycle

  test "active hint writing streams the keydir and filters by file id" do
    dir = temp_dir("active")
    keydir = :ets.new(:hint_streaming_active, [:set, :public])
    hint_path = Path.join(dir, "00000.hint")
    File.touch!(Path.join(dir, "00000.log"))

    :ets.insert(keydir, [
      {"first", nil, 0, 0, 0, 10, 5},
      {"second", nil, 123, 0, 0, 41, 6},
      {"other-file", nil, 0, 0, 1, 0, 4}
    ])

    try do
      assert :ok = HintFile.write_from_keydir(hint_path, keydir, 0)

      assert {:ok,
              [
                {"first", 0, 10, 5, 0},
                {"second", 0, 41, 6, 123}
              ]} = NIF.v2_read_hint_file(hint_path)

      source = File.read!("lib/ferricstore/store/shard/flush.ex")
      [_before, hint_body] = String.split(source, "def write_hint_for_file", parts: 2)
      refute hint_body =~ ":ets.foldl"
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "active hint writing does not reuse a predictable symlinkable temp path" do
    dir = temp_dir("temp-symlink")
    keydir = :ets.new(:hint_streaming_temp_symlink, [:set, :public])
    hint_path = Path.join(dir, "00000.hint")
    predictable_temp = hint_path <> ".tmp"
    victim = Path.join(dir, "victim")
    File.touch!(Path.join(dir, "00000.log"))
    File.write!(victim, "protected")
    File.ln_s!(victim, predictable_temp)
    :ets.insert(keydir, {"key", nil, 0, 0, 0, 10, 5})

    try do
      assert :ok = HintFile.write_from_keydir(hint_path, keydir, 0)
      assert File.read!(victim) == "protected"
      assert File.lstat!(predictable_temp).type == :symlink
      assert {:ok, [{"key", 0, 10, 5, 0}]} = NIF.v2_read_hint_file(hint_path)
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "hint recovery NIF returns bounded advancing pages" do
    dir = temp_dir("pages")
    hint_path = Path.join(dir, "00000.hint")

    entries =
      for index <- 0..4 do
        {"key-#{index}", 0, index * 100, 10 + index, index}
      end

    assert :ok = NIF.v2_write_hint_file(hint_path, entries)

    assert {:ok, first, first_offset, false} =
             NIF.v2_read_hint_file_page(hint_path, 0, 2, 1_024)

    assert length(first) == 2
    assert first_offset > 0

    assert {:ok, second, second_offset, false} =
             NIF.v2_read_hint_file_page(hint_path, first_offset, 2, 1_024)

    assert length(second) == 2
    assert second_offset > first_offset

    last_entry = List.last(entries)

    assert {:ok, [^last_entry], final_offset, true} =
             NIF.v2_read_hint_file_page(hint_path, second_offset, 2, 1_024)

    assert final_offset > second_offset
    File.rm_rf!(dir)
  end

  test "sealed log hint builder streams puts in log order" do
    dir = temp_dir("sealed")
    log_path = Path.join(dir, "00000.log")
    hint_path = Path.join(dir, "00000.hint")

    assert {:ok, [_]} = NIF.v2_append_batch(log_path, [{"key", "old", 0}])
    assert {:ok, _} = NIF.v2_append_tombstone(log_path, "key")
    assert {:ok, [_]} = NIF.v2_append_batch(log_path, [{"key", "new", 0}])

    assert :ok = HintBuilder.build_now(log_path, hint_path, 0, dir)
    assert {:ok, entries} = NIF.v2_read_hint_file(hint_path)
    assert Enum.map(entries, &elem(&1, 0)) == ["key", "key"]
    assert Enum.map(entries, &elem(&1, 3)) == [3, 3]
    File.rm_rf!(dir)
  end

  test "hint recovery does not let an older tombstone erase a newer live value" do
    dir = temp_dir("tombstone-before-live")
    log_path = Path.join(dir, "00000.log")
    hint_path = Path.join(dir, "00000.hint")
    keydir = :ets.new(:hint_tombstone_before_live_keydir, [:set, :public])

    try do
      assert {:ok, [_]} = NIF.v2_append_batch(log_path, [{"key", "old", 0}])
      assert {:ok, _} = NIF.v2_append_tombstone(log_path, "key")
      assert {:ok, [{new_offset, new_size}]} = NIF.v2_append_batch(log_path, [{"key", "new", 0}])
      assert :ok = HintBuilder.build_now(log_path, hint_path, 0, dir)

      assert :ok = Lifecycle.recover_keydir(dir, keydir, 0)

      assert [{"key", nil, 0, _lfu, 0, ^new_offset, ^new_size}] =
               :ets.lookup(keydir, "key")
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "recovery replays a delayed append after a sealed segment hint" do
    dir = temp_dir("sealed-tail")
    log0 = Path.join(dir, "00000.log")
    hint0 = Path.join(dir, "00000.hint")
    log1 = Path.join(dir, "00001.log")
    keydir = :ets.new(:hint_sealed_tail_keydir, [:set, :public])

    try do
      assert {:ok, [_]} = NIF.v2_append_batch(log0, [{"before-hint", "one", 0}])
      assert :ok = HintBuilder.build_now(log0, hint0, 0, dir)
      assert {:ok, [_]} = NIF.v2_append_batch(log0, [{"delayed", "two", 0}])
      assert {:ok, [_]} = NIF.v2_append_batch(log1, [{"active", "three", 0}])

      assert :ok = Lifecycle.recover_keydir(dir, keydir, 0)
      assert [{"delayed", nil, 0, _lfu, 0, _offset, 3}] = :ets.lookup(keydir, "delayed")
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  @tag :hint_generation
  test "recovery rejects a hint after its numeric log id is replaced" do
    dir = temp_dir("generation")
    log_path = Path.join(dir, "00000.log")
    replacement_path = Path.join(dir, "replacement.log")
    hint_path = Path.join(dir, "00000.hint")
    keydir = :ets.new(:hint_generation_keydir, [:set, :public])

    try do
      assert {:ok, [_]} = NIF.v2_append_batch(log_path, [{"old", "one", 0}])
      assert :ok = HintBuilder.build_now(log_path, hint_path, 0, dir)

      assert {:ok, [_]} = NIF.v2_append_batch(replacement_path, [{"new", "two", 0}])
      assert :ok = File.rename(replacement_path, log_path)

      assert :ok = Lifecycle.recover_keydir(dir, keydir, 0)
      assert [{"new", nil, 0, _lfu, 0, 0, 3}] = :ets.lookup(keydir, "new")
      assert [] = :ets.lookup(keydir, "old")
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "hint generation and compaction publication share the segment lock" do
    builder_source = File.read!("lib/ferricstore/store/hint_builder.ex")
    compaction_source = File.read!("lib/ferricstore/store/shard/compaction.ex")

    assert builder_source =~ "SegmentLock.with_lock(log_path"
    assert compaction_source =~ "SegmentLock.with_lock(source"
  end

  test "segment lock excludes a second process for the same path" do
    parent = self()
    path = Path.join(temp_dir("lock"), "00000.log")

    holder =
      Task.async(fn ->
        SegmentLock.with_lock(path, fn ->
          send(parent, :holder_entered)

          receive do
            :release_holder -> :ok
          end
        end)
      end)

    assert_receive :holder_entered

    waiter =
      Task.async(fn ->
        SegmentLock.with_lock(path, fn -> send(parent, :waiter_entered) end)
      end)

    refute_receive :waiter_entered, 100
    send(holder.pid, :release_holder)
    assert :ok = Task.await(holder, 5_000)
    assert :waiter_entered = Task.await(waiter, 5_000)
    assert_receive :waiter_entered

    File.rm_rf!(Path.dirname(path))
  end

  test "rotation queues a hint for the sealed file" do
    Ferricstore.Store.ActiveFile.init(1)
    dir = temp_dir("rotation")
    active_path = Path.join(dir, "00000.log")
    assert {:ok, [_]} = NIF.v2_append_batch(active_path, [{"key", "value", 0}])

    ctx = %{
      name: :"hint_rotation_#{System.unique_integer([:positive])}",
      disk_pressure: :atomics.new(1, signed: false)
    }

    {:ok, builder} = HintBuilder.start_link(index: 0, instance_ctx: ctx)

    state = %{
      active_file_id: 0,
      active_file_path: active_path,
      active_file_size: 10_000,
      file_stats: %{0 => {10_000, 0}},
      index: 0,
      instance_ctx: ctx,
      max_active_file_size: 1_024,
      shard_data_path: dir
    }

    try do
      assert Flush.maybe_rotate_file(state).active_file_id == 1
      assert_eventually(fn -> File.exists?(Path.join(dir, "00000.hint")) end)
    after
      if Process.alive?(builder), do: GenServer.stop(builder)
      File.rm_rf!(dir)
    end
  end

  test "lifecycle recovers hints through the paged API" do
    source = File.read!("lib/ferricstore/store/shard/lifecycle.ex")
    [_before, body] = String.split(source, "defp recover_from_hint", parts: 2)
    [body | _after] = String.split(body, "defp replay_hinted_tails", parts: 2)

    assert body =~ "NIF.v2_read_hint_file_page"
    refute body =~ "NIF.v2_read_hint_file("
  end

  defp temp_dir(suffix) do
    dir =
      Path.join(
        System.tmp_dir!(),
        "hint_streaming_#{suffix}_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    dir
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition did not become true")

      true ->
        Process.sleep(10)
        assert_eventually(fun, attempts - 1)
    end
  end
end
