defmodule Ferricstore.Raft.RewriteOffsetCleanupTest do
  use ExUnit.Case, async: false

  alias :ferricstore_waraft_spike_segment_log, as: Provider
  alias Ferricstore.Test.SegmentRuntimeFixture
  @registry :ferricstore_waraft_segment_offset_registry

  setup do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)
    root = Path.join(System.tmp_dir!(), "rewrite-offset-#{System.pid()}-#{suffix}")
    dir = Path.join(root, "segment_log")

    previous =
      Map.new(
        [
          :waraft_segment_log_offset_registry_max_entries,
          :waraft_segment_log_sync_dir_hook,
          :waraft_segment_log_file_sync_hook,
          :waraft_segment_log_rewrite_hook
        ],
        fn key ->
          {key, Application.fetch_env(:ferricstore, key)}
        end
      )

    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 8)

    on_exit(fn ->
      for {key, saved} <- previous do
        case saved do
          {:ok, value} -> Application.put_env(:ferricstore, key, value)
          :error -> Application.delete_env(:ferricstore, key)
        end
      end

      SegmentRuntimeFixture.cleanup(root)
    end)

    %{root: root, dir: dir}
  end

  test "repeated failed rewrite stages release their offset rows and preserve live records", %{
    root: root,
    dir: dir
  } do
    seed(root)
    before = entries(dir)

    for _ <- 1..4 do
      # Config is synced by stage preparation and writer initialization; fail
      # the final staging sync after offsets have been registered.
      Application.put_env(
        :ferricstore,
        :waraft_segment_log_sync_dir_hook,
        {:fail_on_count, 3, self()}
      )

      assert {:error, _} =
               Provider.write_projection(
                 to_charlist(root),
                 {:raft_log_pos, 200, 0},
                 for(i <- 1..32, do: {"new-#{i}", "value", 0})
               )

      assert temporary_entries(root) == []
      assert entries(dir) == before
      assert {:ok, _} = Provider.read_disk(to_charlist(root), 1)
    end

    refute Enum.any?(File.ls!(root), &String.contains?(&1, ".rewrite.staging."))
  end

  test "successful replacement retires staging offsets without deleting live sidecars", %{
    root: root,
    dir: dir
  } do
    seed(root)

    assert :ok =
             Provider.write_projection(to_charlist(root), {:raft_log_pos, 300, 0}, [
               {"replacement", "new", 0}
             ])

    assert temporary_entries(root) == []
    assert File.regular?(Path.join(dir, "0.idx"))

    assert {:ok, {0, {:ferricstore_segment_projection_header, {:raft_log_pos, 300, 0}, 1}}} =
             Provider.read_disk(to_charlist(root), 0)

    assert {:ok, {0, {:ferricstore_segment_projection_entry, "replacement", "new", 0}}} =
             Provider.read_disk(to_charlist(root), 1)

    assert length(entries(dir)) == 4
  end

  test "full-cap failed stages create 8194 entries and repeated cleanup leaves none", %{
    root: root,
    dir: dir
  } do
    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 8_192)

    Provider.write_projection_batches_sync(
      to_charlist(root),
      for(index <- 1..9_000, do: {{:raft_log_pos, index, 0}, [{"k", "v", 0}]})
    )

    before = entries(dir)
    assert length(before) == 8_194
    parent = self()

    Application.put_env(:ferricstore, :waraft_segment_log_sync_dir_hook, fn path ->
      rows = entries(path)

      if String.contains?(path, ".rewrite.staging.") and length(rows) == 8_194 do
        send(parent, {:full_stage_offsets, path, length(rows)})
        {:error, :injected_full_stage_sync}
      else
        :ok
      end
    end)

    for _ <- 1..3 do
      assert {:error, _} =
               Provider.write_projection(
                 to_charlist(root),
                 {:raft_log_pos, 9_001, 0},
                 for(i <- 1..9_000, do: {"field-#{i}", "v", 0})
               )

      assert_receive {:full_stage_offsets, _path, 8_194}
      assert temporary_entries(root) == []
      assert entries(dir) == before
    end
  end

  test "reclaims 23 abandoned full-cap indexes matching the reported growth", %{
    root: root,
    dir: dir
  } do
    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 8_192)

    assert :ok =
             Provider.write_projection_batches_sync(
               to_charlist(root),
               for(index <- 1..9_000, do: {{:raft_log_pos, index, 0}, [{"k", "v", 0}]})
             )

    live = entries(dir)
    assert length(live) == 8_194
    for suffix <- 1..23, do: copy_entries(live, dir <> ".rewrite.staging.#{suffix}")
    assert length(temporary_entries(root)) == 188_462
    assert {:ok, stats} = Provider.reclaim_abandoned_rewrite_indexes()
    assert stats.offset_entries >= 188_462
    assert temporary_entries(root) == []
    assert entries(dir) == live
  end

  test "reclaims old missing temporary indexes but keeps live, malformed and canonical paths", %{
    root: root,
    dir: dir
  } do
    seed(root)
    live = entries(dir)
    missing_stage = dir <> ".rewrite.staging.111"
    missing_backup = dir <> ".rewrite.backup.222"
    active_stage = dir <> ".rewrite.staging.333"
    symlink_stage = dir <> ".rewrite.staging.334"
    malformed = dir <> ".rewrite.staging.not-a-suffix"
    absent_canonical = Path.join(root, "unrelated/segment_log")
    File.mkdir_p!(active_stage)
    File.ln_s!(dir, symlink_stage)

    for path <- [
          missing_stage,
          missing_backup,
          active_stage,
          symlink_stage,
          malformed,
          absent_canonical
        ],
        do: copy_entries(live, path)

    cache_key =
      {:ferricstore_waraft_spike_segment_log, :records_per_segment, to_charlist(missing_stage)}

    :persistent_term.put(cache_key, 64)
    File.write!(Path.join(active_stage, "keep"), "active")
    index_bytes = File.read!(Path.join(dir, "0.idx"))

    assert {:ok, stats} = Provider.reclaim_abandoned_rewrite_indexes()
    assert stats.directories >= 2
    assert stats.offset_entries >= length(live) * 2
    assert entries(missing_stage) == []
    assert entries(missing_backup) == []
    assert :persistent_term.get(cache_key, :missing) == :missing

    for path <- [active_stage, symlink_stage, malformed, absent_canonical],
        do: assert(entries(path) != [])

    assert entries(dir) == live
    assert File.read!(Path.join(dir, "0.idx")) == index_bytes
    assert File.read!(Path.join(active_stage, "keep")) == "active"
  end

  test "an in-flight writer protects a missing temporary path from reclamation", %{
    root: root,
    dir: dir
  } do
    seed(root)
    temp = dir <> ".rewrite.staging.444"
    copy_entries(entries(dir), temp)
    writer_key = {self(), to_charlist(Path.join(temp, "0.seg"))}
    writer = {writer_key, to_charlist(temp), :file_fd_writing, :test_handle, 0}
    table = :ferricstore_waraft_segment_writer_registry
    :ets.insert(table, writer)

    try do
      assert {:ok, _stats} = Provider.reclaim_abandoned_rewrite_indexes()
      assert entries(temp) != []
    after
      :ets.delete(table, writer_key)
    end

    assert {:ok, _stats} = Provider.reclaim_abandoned_rewrite_indexes()
    assert entries(temp) == []
  end

  test "paged reclamation also retires generations, metadata and dead writer rows", %{
    root: root,
    dir: dir
  } do
    seed(root)

    assert :ok =
             Provider.write_projection(to_charlist(root), {:raft_log_pos, 500, 0}, [{"k", "v", 0}])

    live = entries(dir)
    generations = :ferricstore_waraft_segment_offset_index_generations
    writers = :ferricstore_waraft_segment_writer_registry
    paths = for suffix <- 1..129, do: dir <> ".rewrite.backup.#{suffix}"

    for path <- paths do
      copy_entries(live, path)
      :ets.insert(generations, {to_charlist(path), make_ref()})

      for kind <- [:records_per_segment, :trim_floor, :latest_config] do
        :persistent_term.put({Provider, kind, to_charlist(path)}, :fixture)
      end

      :persistent_term.put({Provider, :offset_index_untrusted, path}, true)
      Process.put({Provider, :offset_index_pruned_before, to_charlist(path)}, 1)
    end

    {owner, monitor} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}
    path = hd(paths)
    writer_key = {owner, to_charlist(Path.join(path, "0.seg"))}
    :ets.insert(writers, {writer_key, to_charlist(path), :file_fd, :closed_fixture_fd, 0})

    assert {:ok, stats} = Provider.reclaim_abandoned_rewrite_indexes()
    assert stats.directories >= 129
    assert stats.offset_entries >= 129 * length(live)
    assert temporary_entries(root) == []
    assert :ets.lookup(writers, writer_key) == []
    assert entries(dir) == live

    for path <- paths do
      assert :ets.lookup(generations, to_charlist(path)) == []

      for kind <- [:records_per_segment, :trim_floor, :latest_config] do
        assert :persistent_term.get({Provider, kind, to_charlist(path)}, :missing) == :missing
      end

      assert :persistent_term.get({Provider, :offset_index_untrusted, path}, :missing) == :missing
      assert Process.get({Provider, :offset_index_pruned_before, to_charlist(path)}) == nil
    end
  end

  test "a live cached writer and legacy writer also protect missing temporary paths", %{
    root: root,
    dir: dir
  } do
    seed(root)
    table = :ferricstore_waraft_segment_writer_registry

    for {suffix, kind} <- [{445, :file_fd}, {446, :legacy}] do
      temp = dir <> ".rewrite.staging.#{suffix}"
      copy_entries(entries(dir), temp)
      key = {self(), to_charlist(Path.join(temp, "0.seg"))}

      row =
        if kind == :legacy,
          do: {key, to_charlist(temp), :test_handle, 0},
          else: {key, to_charlist(temp), kind, :test_handle, 0}

      :ets.insert(table, row)

      try do
        assert {:ok, _} = Provider.reclaim_abandoned_rewrite_indexes()
        assert entries(temp) != []
        assert :ets.lookup(table, key) == [row]
      after
        :ets.delete_object(table, row)
      end

      assert {:ok, _} = Provider.reclaim_abandoned_rewrite_indexes()
      assert entries(temp) == []
    end
  end

  test "maintenance preserves a real paused writer even when its staging directory disappears", %{
    root: root,
    dir: dir
  } do
    seed(root)
    live = entries(dir)
    Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})

    writer =
      Task.async(fn ->
        Provider.write_projection(to_charlist(root), {:raft_log_pos, 600, 0}, [
          {"replacement", "new", 0}
        ])
      end)

    try do
      assert_receive {:waraft_segment_log_file_sync_blocked, path, _method, owner, ref}, 5_000
      assert owner == writer.pid
      temp = Path.dirname(path)
      assert String.starts_with?(temp, dir <> ".rewrite.staging.")
      copy_entries(live, temp)
      File.rm_rf!(temp)
      assert {:ok, _} = Provider.reclaim_abandoned_rewrite_indexes()
      assert entries(temp) == entries_for_path(live, temp) |> Enum.sort()
      assert entries(dir) == live
      send(owner, {ref, {:error, :injected_missing_stage}})
      assert {:error, _} = Task.await(writer, 5_000)
      assert entries(temp) == []
      assert {:ok, _} = Provider.read_disk(to_charlist(root), 1)
    after
      Task.shutdown(writer, :brutal_kill)
    end
  end

  test "rollback after moving the live directory preserves its sidecars and retires staging state",
       %{root: root, dir: dir} do
    seed(root)
    segment = File.read!(Path.join(dir, "0.seg"))
    sidecar = File.read!(Path.join(dir, "0.idx"))
    assert {:ok, expected} = Provider.read_disk(to_charlist(root), 1)

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_rewrite_hook,
      {:fail_once_after_live_backup, self()}
    )

    assert {:error, _} =
             Provider.write_projection(to_charlist(root), {:raft_log_pos, 700, 0}, [
               {"replacement", "new", 0}
             ])

    assert_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}
    assert temporary_entries(root) == []
    assert File.read!(Path.join(dir, "0.seg")) == segment
    assert File.read!(Path.join(dir, "0.idx")) == sidecar
    assert {:ok, ^expected} = Provider.read_disk(to_charlist(root), 1)
    refute File.exists?(dir <> ".rewrite.term")
  end

  test "a reader in another process reopens descriptors after live directory replacement", %{
    root: root
  } do
    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 1)
    seed(root)
    parent = self()

    reader =
      Task.async(fn ->
        assert {:ok, old} = Provider.location_for_index(to_charlist(root), 1)
        send(parent, {:cached_old_location, old})

        receive do
          :read_replacement ->
            assert {:ok, {_, offset, size}} = Provider.location_for_index(to_charlist(root), 1)
            Provider.read_disk_at(to_charlist(root), 1, offset, size)
        end
      end)

    try do
      assert_receive {:cached_old_location, _}, 5_000

      assert :ok =
               Provider.write_projection(to_charlist(root), {:raft_log_pos, 777, 0}, [
                 {"replacement", "new", 0}
               ])

      send(reader.pid, :read_replacement)

      assert {:ok, {0, {:ferricstore_segment_projection_entry, "replacement", "new", 0}}} =
               Task.await(reader, 5_000)
    after
      Task.shutdown(reader, :brutal_kill)
    end
  end

  test "rollback retains the untrusted-sidecar fence for repeated projection indexes", %{
    root: root
  } do
    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 1)
    projection = Path.join(root, "apply_projection_log")
    dir = Path.join(projection, "segment_log")

    assert :ok =
             Provider.write_projection_batches_sync(to_charlist(projection), [
               {{:raft_log_pos, 1, 0}, [{"k", "old", 0}]},
               {{:raft_log_pos, 2, 0}, [{"tail", "v", 0}]}
             ])

    sidecar = Path.join(dir, "0.idx")
    old_slots = File.read!(sidecar)

    assert :ok =
             Provider.write_projection_batches_sync(to_charlist(projection), [
               {{:raft_log_pos, 1, 0}, [{"k", "new", 0}]}
             ])

    # A missed derived update can leave a CRC-valid slot pointing to an older
    # frame. The directory fence, not the slot's CRC alone, protects this read.
    File.write!(sidecar, old_slots)
    fence = {Provider, :offset_index_untrusted, dir}
    :persistent_term.put(fence, true)
    assert {:ok, {_, offset, size}} = Provider.location_for_index(to_charlist(projection), 1)
    assert {:ok, expected} = Provider.read_disk_at(to_charlist(projection), 1, offset, size)
    assert {0, {:ferricstore_segment_apply_projection_batch, _, [{"k", "new", 0}]}} = expected

    # The verified lookup can repair a derived slot. Restore the stale slot so
    # the rollback itself must preserve the fence rather than inherit that fix.
    File.write!(sidecar, old_slots)
    :ets.delete(@registry, {dir, 1})

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_rewrite_hook,
      {:fail_once_after_live_backup, self()}
    )

    assert {:error, _} = Provider.compact_apply_projection(to_charlist(projection), 1, [])
    assert_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}
    assert :persistent_term.get(fence, false)

    assert {:ok, {_, next_offset, next_size}} =
             Provider.location_for_index(to_charlist(projection), 1)

    assert {:ok, ^expected} =
             Provider.read_disk_at(to_charlist(projection), 1, next_offset, next_size)

    assert temporary_entries(root) == []
  end

  defp seed(root) do
    assert :ok =
             Provider.write_projection_batches_sync(
               to_charlist(root),
               for(
                 index <- 1..32,
                 do: {{:raft_log_pos, index, 0}, [{"key-#{index}", "value", 0}]}
               )
             )
  end

  defp entries(dir) do
    :ets.match_object(@registry, {{dir, :_}, :_, :_, :_}) |> Enum.sort()
  end

  defp copy_entries(rows, dir) do
    :ets.insert(@registry, entries_for_path(rows, dir))
  end

  defp entries_for_path(rows, dir) do
    Enum.map(rows, fn {{_, index}, ordinal, offset, size} ->
      {{dir, index}, ordinal, offset, size}
    end)
  end

  defp temporary_entries(root) do
    :ets.tab2list(@registry)
    |> Enum.filter(fn {{dir, _}, _, _, _} ->
      is_binary(dir) and String.starts_with?(dir, root) and String.contains?(dir, ".rewrite.")
    end)
  end
end
