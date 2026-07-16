defmodule Ferricstore.Store.BlobStoreTableOwnerTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.BlobStore
  alias Ferricstore.Store.BlobStore.TableOwner

  @table_heir Ferricstore.Store.BlobStore.TableHeir

  test "ensure_tables never starts an owner outside supervision" do
    assert :ok = Supervisor.terminate_child(Ferricstore.Supervisor, TableOwner)
    assert Process.whereis(TableOwner) == nil

    try do
      assert {:error, :table_owner_unavailable} = TableOwner.ensure_tables()
      assert Process.whereis(TableOwner) == nil
    after
      case Process.whereis(TableOwner) do
        pid when is_pid(pid) -> GenServer.stop(pid)
        nil -> :ok
      end

      assert {:ok, _pid} = Supervisor.restart_child(Ferricstore.Supervisor, TableOwner)
    end
  end

  test "blob table owner crashes preserve in-flight protection state" do
    BlobStore.init_tables()
    table = :ferricstore_blob_store_hardened_protections
    table_tid = :ets.whereis(table)
    owner = Process.whereis(TableOwner)
    id = make_ref()
    row = {id, "owner-crash", 0, ["segments/0.bloblog"], 1, %{}}
    :ets.insert(table, row)

    on_exit(fn ->
      if :ets.whereis(table) != :undefined, do: :ets.delete(table, id)
    end)

    monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, 5_000
    assert table_tid == :ets.whereis(table)
    assert [^row] = :ets.lookup(table, id)
    restarted_owner = await_restarted_owner(owner, 100)
    assert is_pid(restarted_owner)
    assert_owner_eventually(table, restarted_owner, 100)
  end

  test "a restarted table heir is rearmed before a later owner crash" do
    BlobStore.init_tables()
    table = :ferricstore_blob_store_hardened_protections
    table_tid = :ets.whereis(table)
    owner = Process.whereis(TableOwner)
    old_heir = Process.whereis(@table_heir)
    id = make_ref()
    row = {id, "heir-then-owner-crash", 0, ["segments/1.bloblog"], 1, %{}}
    :ets.insert(table, row)

    on_exit(fn ->
      if :ets.whereis(table) != :undefined, do: :ets.delete(table, id)

      current_heir = Process.whereis(@table_heir)

      if is_pid(current_heir) and :ets.info(table, :heir) != current_heir do
        current_owner = Process.whereis(TableOwner)
        if is_pid(current_owner), do: Process.exit(current_owner, :kill)
      end
    end)

    heir_monitor = Process.monitor(old_heir)
    Process.exit(old_heir, :kill)
    assert_receive {:DOWN, ^heir_monitor, :process, ^old_heir, :killed}, 5_000

    restarted_heir = await_restarted_process(@table_heir, old_heir, 100)
    assert is_pid(restarted_heir)
    assert_heir_eventually(table, restarted_heir, 100)

    owner_monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, 5_000

    assert table_tid == :ets.whereis(table)
    assert [^row] = :ets.lookup(table, id)
    restarted_owner = await_restarted_owner(owner, 100)
    assert is_pid(restarted_owner)
    assert_owner_eventually(table, restarted_owner, 100)
  end

  defp await_restarted_owner(_old_owner, 0), do: nil

  defp await_restarted_owner(old_owner, attempts) do
    case Process.whereis(TableOwner) do
      pid when is_pid(pid) and pid != old_owner ->
        pid

      _missing ->
        Process.sleep(10)
        await_restarted_owner(old_owner, attempts - 1)
    end
  end

  defp await_restarted_process(_name, _old_pid, 0), do: nil

  defp await_restarted_process(name, old_pid, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != old_pid ->
        pid

      _missing ->
        Process.sleep(10)
        await_restarted_process(name, old_pid, attempts - 1)
    end
  end

  defp assert_owner_eventually(table, expected_owner, 0) do
    assert :ets.info(table, :owner) == expected_owner
  end

  defp assert_owner_eventually(table, expected_owner, attempts) do
    if :ets.info(table, :owner) == expected_owner do
      :ok
    else
      Process.sleep(10)
      assert_owner_eventually(table, expected_owner, attempts - 1)
    end
  end

  defp assert_heir_eventually(table, expected_heir, 0) do
    assert :ets.info(table, :heir) == expected_heir
  end

  defp assert_heir_eventually(table, expected_heir, attempts) do
    if :ets.info(table, :heir) == expected_heir do
      :ok
    else
      Process.sleep(10)
      assert_heir_eventually(table, expected_heir, attempts - 1)
    end
  end
end
