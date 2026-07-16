defmodule FerricstoreServer.Native.AdmissionTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Stats
  alias FerricstoreServer.Native.Admission

  test "capacity is global and reclaimed when an owner is killed" do
    name = :"native_admission_#{System.unique_integer([:positive])}"
    table = :"native_admission_leases_#{System.unique_integer([:positive])}"
    :ok = Admission.init_table(table)
    baseline = Stats.active_connections()
    {:ok, admission} = Admission.start_link(name: name, table: table, max_connections: 2)

    owner1 = spawn(fn -> Process.sleep(:infinity) end)
    owner2 = spawn(fn -> Process.sleep(:infinity) end)
    owner3 = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Enum.each([owner1, owner2, owner3], &Process.exit(&1, :kill))
      stop_if_alive(admission)
      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)

    assert {:ok, _token1} = Admission.acquire(name, owner1)
    assert {:ok, _token2} = Admission.acquire(name, owner2)
    assert {:error, :max_connections} = Admission.acquire(name, owner3)
    assert Admission.count(name) == 2
    assert Stats.active_connections() == baseline + 2

    Process.exit(owner1, :kill)
    assert eventually(fn -> Admission.count(name) == 1 end)
    assert {:ok, token3} = Admission.acquire(name, owner3)
    assert :ok = Admission.release(name, token3)
    assert Admission.count(name) == 1
  end

  test "a restart preserves live leases and accepts their original release tokens" do
    name = :"native_admission_restart_#{System.unique_integer([:positive])}"
    table = :"native_admission_restart_leases_#{System.unique_integer([:positive])}"
    :ok = Admission.init_table(table)
    baseline = Stats.active_connections()
    owner1 = spawn(fn -> Process.sleep(:infinity) end)
    owner2 = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      name |> Process.whereis() |> stop_if_alive()

      Enum.each([owner1, owner2], &Process.exit(&1, :kill))
      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)

    {:ok, admission} = Admission.start_link(name: name, table: table, max_connections: 1)
    assert {:ok, token1} = Admission.acquire(name, owner1)
    assert Stats.active_connections() == baseline + 1

    :ok = GenServer.stop(admission)
    {:ok, _restarted} = Admission.start_link(name: name, table: table, max_connections: 1)

    assert Admission.count(name) == 1
    assert {:error, :max_connections} = Admission.acquire(name, owner2)
    assert Stats.active_connections() == baseline + 1

    assert :ok = Admission.release(name, token1)
    assert Admission.count(name) == 0
    assert Stats.active_connections() == baseline
  end

  defp eventually(fun, attempts \\ 50)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp stop_if_alive(nil), do: :ok

  defp stop_if_alive(pid) when is_pid(pid) do
    GenServer.stop(pid)
  catch
    :exit, _reason -> :ok
  end
end
