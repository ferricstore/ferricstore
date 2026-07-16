defmodule Ferricstore.Cluster.TargetDataProbeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Cluster.Manager.Target
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.SharedRefBackfill

  setup do
    root = Path.join(System.tmp_dir!(), "target_data_probe_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root, log: Path.join(root, "00000.log")}
  end

  test "bootstrap-only Bitcask records do not make a fresh join target stateful", %{
    root: root,
    log: log
  } do
    assert {:ok, _locations} =
             NIF.v2_append_batch(log, [
               {Keys.shared_value_ref_backfill_key(0), <<1>>, 0},
               {SharedRefBackfill.progress_key(0), "complete", 0},
               {Keys.policy_catalog_backfill_key(0), "done", 0}
             ])

    assert {:ok, false} = Target.probe_target_log_files(node(), root, ["00000.log"])
  end

  test "a user record still makes the target stateful", %{root: root, log: log} do
    assert {:ok, _locations} =
             NIF.v2_append_batch(log, [
               {Keys.shared_value_ref_backfill_key(0), <<1>>, 0},
               {"user-key", "value", 0}
             ])

    assert {:ok, true} = Target.probe_target_log_files(node(), root, ["00000.log"])
  end

  test "Flow state is user data even though its key is internal", %{root: root, log: log} do
    state_key = Keys.state_key("flow-id", "tenant")
    assert {:ok, _locations} = NIF.v2_append_batch(log, [{state_key, "state", 0}])

    assert {:ok, true} = Target.probe_target_log_files(node(), root, ["00000.log"])
  end

  test "file-tree probes reject symlink roots without traversing them", %{root: root} do
    outside = root <> "_outside"
    link = Path.join(root, "dedicated")

    on_exit(fn -> File.rm_rf!(outside) end)

    File.mkdir_p!(outside)
    File.write!(Path.join(outside, "payload"), "outside")
    File.ln_s!(outside, link)

    assert {:error, {:target_data_probe_failed, target, {:symlink, ^link}}} =
             Target.probe_target_file_tree(node(), link)

    assert target == node()
  end

  test "Bitcask probes reject symlink shard directories before listing them", %{root: root} do
    outside = root <> "_bitcask_outside"
    link = Path.join(root, "data")

    on_exit(fn -> File.rm_rf!(outside) end)

    File.mkdir_p!(outside)
    File.write!(Path.join(outside, "00000.log"), "not-a-bitcask-log")
    File.ln_s!(outside, link)

    assert {:error, {:target_data_probe_failed, target, {:symlink, ^link}}} =
             Target.probe_target_bitcask_logs(node(), link)

    assert target == node()
  end

  test "cluster joins expose only the WARaft snapshot transfer path" do
    refute File.exists?("lib/ferricstore/cluster/data_sync.ex")

    manager_source = File.read!("lib/ferricstore/cluster/manager.ex")
    target_source = File.read!("lib/ferricstore/cluster/manager/target.ex")

    refute manager_source =~ "sync data FIRST"
    refute target_source =~ "Cluster.DataSync"
    refute target_source =~ "wal_bridgeable"
    refute target_source =~ "extract_direct_sync_indices"
    refute target_source =~ "read_target_indices"
    refute manager_source =~ "{:add_node, node, role, opts}, 120_000"
    refute manager_source =~ "{:remove_node, node}, 30_000"
    refute manager_source =~ "GenServer.call(__MODULE__, :leave, 30_000)"

    assert manager_source =~
             "GenServer.call(__MODULE__, {:node_status, membership_timeout}, :infinity)"

    refute target_source =~ "{:add_node, target_node, role}, 120_000"
    assert manager_source =~ "WARaft snapshot replication"
  end
end
