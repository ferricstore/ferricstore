defmodule FerricstoreServer.Health.Dashboard.OperationalDataTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Health.Dashboard.Data.Operational
  alias FerricstoreServer.Health.Dashboard.StorageSnapshotCache

  setup do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-dashboard-storage-#{System.unique_integer([:positive, :monotonic])}"
      )

    previous_data_dir = Application.get_env(:ferricstore, :data_dir)
    previous_ttl = Application.get_env(:ferricstore, :dashboard_storage_summary_ttl_ms)

    previous_scan_observer =
      Application.get_env(:ferricstore, :dashboard_storage_scan_observer)

    File.mkdir_p!(data_dir)
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :dashboard_storage_summary_ttl_ms, 60_000)

    on_exit(fn ->
      restore_env(:data_dir, previous_data_dir)
      restore_env(:dashboard_storage_summary_ttl_ms, previous_ttl)
      restore_env(:dashboard_storage_scan_observer, previous_scan_observer)
      File.rm_rf!(data_dir)
    end)

    {:ok, data_dir: data_dir}
  end

  test "storage summary reuses a recent filesystem snapshot", %{data_dir: data_dir} do
    file = Path.join(data_dir, "000001.log")
    File.write!(file, "one")

    assert %{total_disk_bytes: 3} = Operational.collect_storage_summary()

    File.write!(file, "one-two-three")

    assert %{total_disk_bytes: 3} = Operational.collect_storage_summary()
  end

  test "storage summary refreshes after the configured ttl", %{data_dir: data_dir} do
    Application.put_env(:ferricstore, :dashboard_storage_summary_ttl_ms, 0)
    file = Path.join(data_dir, "000001.log")
    File.write!(file, "one")

    assert %{total_disk_bytes: 3} = Operational.collect_storage_summary()

    File.write!(file, "one-two-three")

    assert %{total_disk_bytes: 13} = Operational.collect_storage_summary()
  end

  test "storage refresh replaces one supervised ETS cache entry", %{data_dir: data_dir} do
    Application.put_env(:ferricstore, :dashboard_storage_summary_ttl_ms, 0)
    file = Path.join(data_dir, "000001.log")
    File.write!(file, "one")

    assert %{total_disk_bytes: 3} = Operational.collect_storage_summary()

    table = StorageSnapshotCache
    owner = Process.whereis(StorageSnapshotCache)

    assert is_pid(owner)
    assert :ets.info(table, :owner) == owner
    assert :ets.info(table, :protection) == :protected

    assert [
             {:snapshot, {^data_dir, _shard_count}, _cached_at_ms, %{total_disk_bytes: 3}}
           ] = :ets.tab2list(table)

    File.write!(file, "one-two-three")

    assert %{total_disk_bytes: 13} = Operational.collect_storage_summary()

    assert [
             {:snapshot, {^data_dir, _shard_count}, _cached_at_ms, %{total_disk_bytes: 13}}
           ] = :ets.tab2list(table)
  end

  test "storage refresh visits shard files once", %{data_dir: data_dir} do
    Application.put_env(:ferricstore, :dashboard_storage_summary_ttl_ms, 0)
    test_pid = self()

    Application.put_env(:ferricstore, :dashboard_storage_scan_observer, fn event ->
      send(test_pid, event)
    end)

    shard_dir = Path.join([data_dir, "data", "shard_0"])
    shard_file = Path.join(shard_dir, "000001.log")
    File.mkdir_p!(shard_dir)
    File.write!(shard_file, "shard-data")

    assert %{total_disk_bytes: 10, shards: [%{index: 0, disk_bytes: 10} | _]} =
             Operational.collect_storage_page()

    events = drain_scan_events([])

    assert Enum.count(events, &(&1 == {:path, shard_file})) == 1
  end

  test "storage scans do not follow symlinks outside the data directory", %{
    data_dir: data_dir
  } do
    outside_dir = data_dir <> "-outside"
    File.mkdir_p!(outside_dir)
    File.write!(Path.join(outside_dir, "unrelated.log"), "outside")
    File.ln_s!(outside_dir, Path.join(data_dir, "linked"))
    on_exit(fn -> File.rm_rf!(outside_dir) end)

    assert Operational.scan_storage_tree(data_dir) == {0, 0, 0}
  end

  test "concurrent storage callers share one refresh", %{data_dir: data_dir} do
    test_pid = self()

    Application.put_env(:ferricstore, :dashboard_storage_scan_observer, fn
      {:scan_started, ^data_dir} ->
        send(test_pid, {:scan_started, self()})

        receive do
          :continue_storage_scan -> :ok
        end

      _event ->
        :ok
    end)

    first = Task.async(&Operational.collect_storage_summary/0)
    assert_receive {:scan_started, scan_pid}, 1_000

    rest = Enum.map(1..7, fn _index -> Task.async(&Operational.collect_storage_summary/0) end)
    refute_receive {:scan_started, _other_pid}, 100

    send(scan_pid, :continue_storage_scan)

    assert %{total_disk_bytes: 0} = Task.await(first)

    for task <- rest do
      assert %{total_disk_bytes: 0} = Task.await(task)
    end

    refute_receive {:scan_started, _other_pid}, 0
  end

  defp drain_scan_events(events) do
    receive do
      event -> drain_scan_events([event | events])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
