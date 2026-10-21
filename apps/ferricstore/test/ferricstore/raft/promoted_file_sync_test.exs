defmodule Ferricstore.Raft.PromotedFileSyncTest do
  @moduledoc """
  Writes to large collections' dedicated files skip their fsync under WARaft
  and are synced before the storage persists a replay position or writes a
  projection checkpoint past them.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Raft.PromotedFileSync
  alias Ferricstore.Store.Router

  setup do
    base =
      Path.join(System.tmp_dir!(), "promoted_file_sync_#{System.unique_integer([:positive])}")

    root = Path.join(base, "waraft")
    data_dir = Path.join(base, "data")
    File.mkdir_p!(root)
    File.mkdir_p!(Path.join([data_dir, "dedicated", "shard_0", "set_a"]))

    test_pid = self()

    Application.put_env(:ferricstore, :promoted_file_sync_fsync_hook, fn kind, path ->
      send(test_pid, {:fsync, kind, path, self()})
      :passthrough
    end)

    on_exit(fn ->
      Application.delete_env(:ferricstore, :promoted_file_sync_fsync_hook)
      File.rm_rf!(base)
    end)

    %{
      root: root,
      data_dir: data_dir,
      log_file: Path.join([data_dir, "dedicated", "shard_0", "set_a", "00000.log"])
    }
  end

  describe "tracking" do
    test "the owner's writes skip fsync until it syncs, and the marker covers them meanwhile",
         %{root: root, data_dir: data_dir, log_file: file} do
      assert {:ok, tracker} = PromotedFileSync.open(root, data_dir)
      marker = Path.join(root, "dedicated_unsynced")
      refute File.exists?(marker)

      assert {:ok, {0, size}} = PromotedFileSync.append_record(file, "k", "v", 0)
      assert size == 26 + 2
      tombstone_size = 26 + byte_size("kk")
      assert {:ok, {^size, ^tombstone_size}} = PromotedFileSync.append_tombstone(file, "kk")
      assert {:ok, [{_offset, 1}]} = PromotedFileSync.append_batch(file, [{"b", "x", 0}])

      assert File.exists?(marker)
      assert_received {:fsync, :file, ^marker, _pid}
      refute_received {:fsync, :file, ^file, _pid}
      refute PromotedFileSync.synced?(tracker)

      assert :ok = PromotedFileSync.sync(tracker)
      assert_received {:fsync, :file, ^file, _pid}
      assert PromotedFileSync.synced?(tracker)
      refute File.exists?(marker)

      assert {:ok, %{"k" => _}} = read_keys(file)
    end

    test "another process writes with fsync and is not tracked", %{
      root: root,
      data_dir: data_dir,
      log_file: file
    } do
      assert {:ok, tracker} = PromotedFileSync.open(root, data_dir)

      assert {:ok, {0, _size}} =
               Task.async(fn ->
                 PromotedFileSync.bind(tracker)
                 PromotedFileSync.append_record(file, "k", "v", 0)
               end)
               |> Task.await()

      assert PromotedFileSync.synced?(tracker)
      refute File.exists?(Path.join(root, "dedicated_unsynced"))
    end

    test "a sync from another process leaves the files tracked", %{
      root: root,
      data_dir: data_dir,
      log_file: file
    } do
      assert {:ok, tracker} = PromotedFileSync.open(root, data_dir)
      assert {:ok, _location} = PromotedFileSync.append_record(file, "k", "v", 0)
      table = PromotedFileSync.registered_table(root)

      {result, pid} =
        Task.async(fn -> {PromotedFileSync.sync_tracked(table), self()} end) |> Task.await()

      assert :ok == result
      assert_received {:fsync, :file, ^file, ^pid}
      refute PromotedFileSync.synced?(tracker)
    end

    test "a vanished table cannot vouch for its writes", %{root: root, data_dir: data_dir} do
      table =
        Task.async(fn ->
          {:ok, _tracker} = PromotedFileSync.open(root, data_dir)
          PromotedFileSync.registered_table(root)
        end)
        |> Task.await()

      assert {:error, :promoted_file_sync_unavailable} = PromotedFileSync.sync_tracked(table)
    end

    test "a failed fsync keeps the file tracked and the marker on disk", %{
      root: root,
      data_dir: data_dir,
      log_file: file
    } do
      assert {:ok, tracker} = PromotedFileSync.open(root, data_dir)
      assert {:ok, _location} = PromotedFileSync.append_record(file, "k", "v", 0)

      Application.put_env(:ferricstore, :promoted_file_sync_fsync_hook, fn
        :file, ^file -> {:error, :eio}
        _kind, _path -> :passthrough
      end)

      assert {:error, {:promoted_file_fsync_failed, ^file, :eio}} = PromotedFileSync.sync(tracker)
      refute PromotedFileSync.synced?(tracker)
      assert File.exists?(Path.join(root, "dedicated_unsynced"))
    end

    test "a tracked file removed by compaction counts as synced", %{
      root: root,
      data_dir: data_dir,
      log_file: file
    } do
      assert {:ok, tracker} = PromotedFileSync.open(root, data_dir)
      assert {:ok, _location} = PromotedFileSync.append_record(file, "k", "v", 0)
      File.rm!(file)

      assert :ok = PromotedFileSync.sync(tracker)
      assert PromotedFileSync.synced?(tracker)
    end

    test "opening after a stop that left the marker syncs every dedicated file first", %{
      root: root,
      data_dir: data_dir,
      log_file: file
    } do
      File.write!(file, "pending")
      other = Path.join([data_dir, "dedicated", "shard_3", "hash_b", "00002.log"])
      File.mkdir_p!(Path.dirname(other))
      File.write!(other, "pending")
      File.write!(Path.join(root, "dedicated_unsynced"), "1")

      assert {:ok, tracker} = PromotedFileSync.open(root, data_dir)

      assert_received {:fsync, :file, ^file, _pid}
      assert_received {:fsync, :file, ^other, _pid}
      refute File.exists?(Path.join(root, "dedicated_unsynced"))
      assert PromotedFileSync.synced?(tracker)
    end
  end

  describe "WARaft storage" do
    setup do
      previous = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)
      ctx = FerricStore.Instance.get(:default)
      key = "{promoted-sync:#{System.unique_integer([:positive])}}"
      Dispatcher.dispatch("SADD", [key | Enum.map(1..150, &"seed-#{&1}")], ctx)
      wait_promoted(ctx, key)

      on_exit(fn ->
        case previous do
          nil ->
            Application.delete_env(:ferricstore, :waraft_storage_metadata_persist_every)

          value ->
            Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, value)
        end
      end)

      %{ctx: ctx, key: key, table: storage_table(ctx, key)}
    end

    test "a write to a large set skips its fsync and is tracked", %{
      ctx: ctx,
      key: key,
      table: table
    } do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1_000_000)

      path =
        touched_file(table, fn -> Dispatcher.dispatch("SADD", [key, "tracked-member"], ctx) end)

      assert path =~ "/dedicated/"
    end

    test "the storage syncs the large set's file before it persists its replay position", %{
      ctx: ctx,
      key: key,
      table: table
    } do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1_000_000)
      path = touched_file(table, fn -> Dispatcher.dispatch("SADD", [key, "first"], ctx) end)

      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
      assert 1 == Dispatcher.dispatch("SADD", [key, "second"], ctx)

      assert :ok == await_file_sync(ctx, key, path)
      assert :ok == wait_untracked(table)
      assert ["1", "1"] == Enum.map(["first", "second"], &sismember(ctx, key, &1))
    end
  end

  describe "WARaft storage checkpoints and trims" do
    setup do
      ctx = FerricStore.Instance.get(:default)
      key = "{promoted-sync-cp:#{System.unique_integer([:positive])}}"
      Dispatcher.dispatch("SADD", [key | Enum.map(1..150, &"seed-#{&1}")], ctx)
      wait_promoted(ctx, key)

      settings =
        for name <- [
              :waraft_storage_metadata_persist_every,
              :waraft_segment_projection_checkpoint_every,
              :waraft_segment_projection_checkpoint_min_interval_ms
            ],
            do: {name, Application.get_env(:ferricstore, name)}

      on_exit(fn ->
        :telemetry.detach("promoted-file-sync-checkpoint")

        for {name, value} <- settings do
          if value == nil,
            do: Application.delete_env(:ferricstore, name),
            else: Application.put_env(:ferricstore, name, value)
        end
      end)

      %{ctx: ctx, key: key, table: storage_table(ctx, key)}
    end

    test "a log trim syncs the storage's tracked files first and refuses if it cannot", %{
      ctx: ctx,
      key: key,
      table: table
    } do
      # The segment log lives in <partition>/segment_log; trim passes its parent.
      trim_root = ctx |> partition_root(key) |> Path.join("segment_log") |> Path.dirname()

      assert table == PromotedFileSync.registered_table(trim_root)

      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1_000_000)
      path = touched_file(table, fn -> Dispatcher.dispatch("SADD", [key, "before-trim"], ctx) end)

      Application.put_env(:ferricstore, :promoted_file_sync_fsync_hook, fn
        :file, ^path -> {:error, :eio}
        _kind, _path -> :passthrough
      end)

      assert {:error, {:promoted_file_fsync_failed, ^path, :eio}} =
               Ferricstore.Raft.WARaftStorage.prepare_segment_projection_for_trim(trim_root, 0)
    end

    test "a projection checkpoint syncs the large set's file before it is written", %{
      ctx: ctx,
      key: key,
      table: table
    } do
      Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1_000_000)

      path =
        touched_file(table, fn -> Dispatcher.dispatch("SADD", [key, "before-checkpoint"], ctx) end)

      storage = :ets.info(table, :owner)

      test_pid = self()

      :telemetry.attach(
        "promoted-file-sync-checkpoint",
        [:ferricstore, :waraft, :segment_projection_checkpoint, :start],
        fn _event, _measurements, _metadata, _config ->
          send(test_pid, {:checkpoint_started, self()})
        end,
        nil
      )

      Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)
      Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_min_interval_ms, 0)
      assert 1 == Dispatcher.dispatch("SADD", [key, "starts-checkpoint"], ctx)

      assert_receive {:checkpoint_started, checkpoint}, 10_000
      assert_receive {:fsync, :file, ^path, ^checkpoint}, 10_000
      refute checkpoint == storage
    end
  end

  # The file a write tracked: the one row it added or re-versioned.
  defp touched_file(table, write) do
    before = Map.new(:ets.match_object(table, {{:file, :_}, :_}))
    assert 1 == write.()

    [{{:file, path}, _version}] =
      table
      |> :ets.match_object({{:file, :_}, :_})
      |> Enum.reject(fn {file, version} -> Map.get(before, file) == version end)

    path
  end

  defp sismember(ctx, key, member),
    do: Dispatcher.dispatch("SISMEMBER", [key, member], ctx) |> to_string()

  # The tracking of the default backend's storage for `key`'s shard, found the
  # way a log trim finds it (other instances in the same VM register too).
  defp storage_table(ctx, key) do
    table = ctx |> partition_root(key) |> PromotedFileSync.registered_table()
    assert table != nil and :ets.info(table, :owner) != :undefined
    table
  end

  defp partition_root(ctx, key) do
    :ferricstore_waraft_backend
    |> :wa_raft_part_sup.registered_partition_path(Router.shard_for(ctx, key) + 1)
    |> to_string()
  end

  # The storage persists its position, syncing the file first, once the
  # entry's replay dependencies are durable; until then it waits for a later
  # write to carry the persist. Small writes on the same shard supply them
  # without touching the large set's file.
  defp await_file_sync(ctx, key, path, tries \\ 50) do
    receive do
      {:fsync, :file, ^path, _storage_pid} -> :ok
    after
      200 ->
        if tries == 0 do
          :timeout
        else
          :ok = Dispatcher.dispatch("SET", [key <> ":nudge", Integer.to_string(tries)], ctx)
          await_file_sync(ctx, key, path, tries - 1)
        end
    end
  end

  defp wait_untracked(table, tries \\ 200) do
    cond do
      :ets.match_object(table, {{:file, :_}, :_}) == [] ->
        :ok

      tries == 0 ->
        {:still_tracked, :ets.match_object(table, {{:file, :_}, :_})}

      true ->
        Process.sleep(10)
        wait_untracked(table, tries - 1)
    end
  end

  defp wait_promoted(ctx, key, tries \\ 200) do
    promoted? =
      GenServer.call(Router.shard_name(ctx, Router.shard_for(ctx, key)), {:promoted?, key})

    cond do
      promoted? ->
        :ok

      tries == 0 ->
        flunk("expected #{inspect(key)} to be promoted")

      true ->
        Process.sleep(25)
        wait_promoted(ctx, key, tries - 1)
    end
  end

  defp read_keys(file) do
    case Ferricstore.Bitcask.NIF.v2_scan_file(file) do
      {:ok, records} -> {:ok, Map.new(records, fn record -> {elem(record, 0), record} end)}
      other -> other
    end
  end
end
