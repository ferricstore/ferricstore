defmodule Ferricstore.Store.KeydirRuntimeSupervisorTest do
  use ExUnit.Case, async: false

  test "keydir owner failure restarts the dependent shard subtree" do
    unique = System.unique_integer([:positive])
    name = :"keydir_runtime_#{unique}"
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_#{name}")

    {:ok, instance_supervisor} =
      FerricStore.Instance.Supervisor.start_link(name,
        data_dir: data_dir,
        shard_count: 1,
        flow_shared_ref_backfill?: false
      )

    Process.unlink(instance_supervisor)

    on_exit(fn ->
      if Process.alive?(instance_supervisor), do: Supervisor.stop(instance_supervisor)
      FerricStore.Instance.cleanup(name)
      File.rm_rf!(data_dir)
    end)

    ctx = FerricStore.Instance.get(name)
    owner_name = :"#{name}.KeydirTableOwner"
    shard_supervisor_name = :"#{name}.ShardSupervisor"
    shard_name = elem(ctx.shard_names, 0)

    old_owner = Process.whereis(owner_name)
    old_shard_supervisor = Process.whereis(shard_supervisor_name)
    old_shard = Process.whereis(shard_name)

    assert is_pid(old_owner)
    assert is_pid(old_shard_supervisor)
    assert is_pid(old_shard)

    shard_monitor = Process.monitor(old_shard)
    Process.exit(old_owner, :kill)

    assert_receive {:DOWN, ^shard_monitor, :process, ^old_shard, _reason}, 5_000

    assert await_replacement(owner_name, old_owner, 200)
    assert await_replacement(shard_supervisor_name, old_shard_supervisor, 200)
    assert await_replacement(shard_name, old_shard, 200)
  end

  defp await_replacement(_name, _old_pid, 0), do: false

  defp await_replacement(name, old_pid, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != old_pid ->
        true

      _missing_or_old ->
        Process.sleep(10)
        await_replacement(name, old_pid, attempts - 1)
    end
  end
end
