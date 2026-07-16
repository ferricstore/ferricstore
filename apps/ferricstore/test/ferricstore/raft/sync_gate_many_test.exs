defmodule Ferricstore.Raft.SyncGateManyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftBackend.SyncGate

  test "concurrent entrants receive distinct exactly-once admission tokens" do
    shard_index = System.unique_integer([:positive]) + 100_000
    parent = self()

    entrants =
      for _ <- 1..256 do
        spawn_link(fn ->
          receive do
            :enter ->
              {:ok, token} = SyncGate.enter(shard_index)
              send(parent, {:entered, self(), token})

              receive do
                :leave -> SyncGate.leave(token)
              end
          end
        end)
      end

    Enum.each(entrants, &send(&1, :enter))

    tokens =
      for _ <- entrants do
        assert_receive {:entered, _pid, token}, 5_000
        token
      end

    assert tokens
           |> Enum.map(&elem(&1, 1))
           |> MapSet.new()
           |> MapSet.size() == length(tokens)

    Enum.each(entrants, &send(&1, :leave))
  end

  test "multi-shard pause publishes every gate before waiting for active writers" do
    indexes = [30_000, 30_001]
    SyncGate.init_shards(0)

    on_exit(fn -> SyncGate.resume_many(indexes, 1_000) end)

    assert {:ok, held_token} = SyncGate.enter(30_001)
    assert {:ok, pauses} = SyncGate.pause_many(indexes)
    assert Enum.map(pauses, &elem(&1, 0)) == indexes
    assert SyncGate.paused?(30_000)
    assert SyncGate.paused?(30_001)

    parent = self()

    blocked =
      spawn(fn ->
        result = SyncGate.enter(30_000)
        send(parent, {:blocked_enter_result, result})
      end)

    refute_receive {:blocked_enter_result, _result}, 50
    assert Process.alive?(blocked)

    SyncGate.leave(held_token)
    assert :ok = SyncGate.await_many_drained(pauses, 1_000)
    assert :ok = SyncGate.resume_many(indexes, 1_000)
    assert_receive {:blocked_enter_result, {:ok, token}}, 1_000
    SyncGate.leave(token)
  end

  test "multi-shard admission releases every claim before waiting on a paused shard" do
    first = System.unique_integer([:positive]) + 200_000
    second = first + 1

    on_exit(fn -> SyncGate.resume_many([first, second], 1_000) end)

    assert {:ok, second_pause} = SyncGate.pause_many([second])
    assert :ok = SyncGate.await_many_drained(second_pause, 1_000)

    entrant =
      Task.async(fn ->
        SyncGate.enter_many([first, second])
      end)

    refute Task.yield(entrant, 50)

    assert {:ok, first_pause} = SyncGate.pause_many([first])
    assert :ok = SyncGate.await_many_drained(first_pause, 1_000)

    assert :ok = SyncGate.resume_many([second], 1_000)
    refute Task.yield(entrant, 50)

    assert :ok = SyncGate.resume_many([first], 1_000)
    assert {:ok, {:ok, tokens}} = Task.yield(entrant, 1_000)
    Enum.each(tokens, &SyncGate.leave/1)
  end

  test "owner death releases only that pause lease and explicit resume is idempotent" do
    shard_index = System.unique_integer([:positive]) + 300_000
    owner = spawn(fn -> Process.sleep(:infinity) end)
    owner_lease = {owner, make_ref()}
    nested_lease = {self(), make_ref()}

    on_exit(fn -> SyncGate.force_resume(shard_index, 1_000) end)

    assert {:ok, gate_pid} = SyncGate.pause(shard_index, owner_lease)
    assert {:ok, ^gate_pid} = SyncGate.pause(shard_index, nested_lease)
    assert {:ok, ^gate_pid} = SyncGate.pause(shard_index, nested_lease)

    Process.exit(owner, :kill)

    assert eventually(fn -> SyncGate.paused?(shard_index) end)
    assert :ok = SyncGate.resume(shard_index, owner_lease, 1_000)
    assert :ok = SyncGate.resume(shard_index, owner_lease, 1_000)
    assert SyncGate.paused?(shard_index)

    assert :ok = SyncGate.resume(shard_index, nested_lease, 1_000)
    assert eventually(fn -> not SyncGate.paused?(shard_index) end)
  end

  test "sole pause owner death admits blocked writers" do
    shard_index = System.unique_integer([:positive]) + 400_000
    parent = self()

    owner =
      spawn(fn ->
        lease = {self(), make_ref()}
        send(parent, {:sole_pause_acquired, self(), SyncGate.pause(shard_index, lease)})
        Process.sleep(:infinity)
      end)

    assert_receive {:sole_pause_acquired, ^owner, {:ok, _gate_pid}}, 1_000

    blocked =
      Task.async(fn ->
        SyncGate.enter(shard_index)
      end)

    refute Task.yield(blocked, 50)
    Process.exit(owner, :kill)

    assert {:ok, {:ok, token}} = Task.yield(blocked, 1_000)
    SyncGate.leave(token)
  end

  test "explicit multi-shard pause unwinds earlier holds after partial acquisition failure" do
    shard_index = System.unique_integer([:positive]) + 500_000
    pause_lease = {self(), make_ref()}

    assert {:error, {:sync_pause_many_failed, -1, _reason}} =
             SyncGate.pause_many([shard_index, -1], pause_lease)

    refute SyncGate.paused?(shard_index)
    assert :ok = SyncGate.resume(shard_index, pause_lease, 1_000)
  end

  test "unscoped pause survives the acquiring process and resumes from another process" do
    shard_index = System.unique_integer([:positive]) + 600_000
    parent = self()

    on_exit(fn -> SyncGate.force_resume(shard_index, 1_000) end)

    {pause_pid, pause_monitor} =
      spawn_monitor(fn ->
        send(parent, {:unscoped_pause_result, SyncGate.pause(shard_index)})
      end)

    assert_receive {:unscoped_pause_result, {:ok, _gate_pid}}, 1_000
    assert_receive {:DOWN, ^pause_monitor, :process, ^pause_pid, :normal}, 1_000
    Process.sleep(20)
    assert SyncGate.paused?(shard_index)

    resume_task =
      Task.async(fn ->
        SyncGate.resume(shard_index, 1_000)
      end)

    assert :ok = Task.await(resume_task, 1_000)
    assert eventually(fn -> not SyncGate.paused?(shard_index) end)
  end

  test "releasing a gate fails drain waiters while admitted writers remain" do
    shard_index = System.unique_integer([:positive]) + 700_000
    pause_lease = {self(), make_ref()}

    assert {:ok, active_token} = SyncGate.enter(shard_index)
    assert {:ok, gate_pid} = SyncGate.pause(shard_index, pause_lease)

    drain_waiter =
      Task.async(fn ->
        SyncGate.await_drained(gate_pid, 1_000)
      end)

    refute Task.yield(drain_waiter, 50)
    assert :ok = SyncGate.resume(shard_index, pause_lease, 1_000)

    assert {:ok, {:error, :sync_pause_released}} = Task.yield(drain_waiter, 1_000)
    SyncGate.leave(active_token)
  end

  test "admission owner death cannot strand a shard pause" do
    shard_index = System.unique_integer([:positive]) + 800_000
    pause_lease = {self(), make_ref()}
    parent = self()

    owner =
      spawn(fn ->
        {:ok, token} = SyncGate.enter(shard_index)
        send(parent, {:admission_owned, self(), token})
        Process.sleep(:infinity)
      end)

    assert_receive {:admission_owned, ^owner, _token}, 1_000
    Process.exit(owner, :kill)

    assert {:ok, gate_pid} = SyncGate.pause(shard_index, pause_lease)
    assert :ok = SyncGate.await_drained(gate_pid, 1_000)
    assert :ok = SyncGate.resume(shard_index, pause_lease, 1_000)
  end

  test "transferred admission remains active after its original owner exits" do
    shard_index = System.unique_integer([:positive]) + 900_000
    pause_lease = {self(), make_ref()}
    parent = self()

    holder =
      spawn(fn ->
        receive do
          {:hold_admission, token} ->
            send(parent, {:admission_held, self()})

            receive do
              :release_admission -> SyncGate.leave(token)
            end
        end
      end)

    {owner, owner_monitor} =
      spawn_monitor(fn ->
        {:ok, token} = SyncGate.enter(shard_index)
        :ok = SyncGate.transfer(token, holder)
        send(holder, {:hold_admission, token})
      end)

    assert_receive {:admission_held, ^holder}, 1_000
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}, 1_000
    assert {:ok, gate_pid} = SyncGate.pause(shard_index, pause_lease)

    drain =
      Task.async(fn ->
        SyncGate.await_drained(gate_pid, 1_000)
      end)

    refute Task.yield(drain, 50)
    send(holder, :release_admission)
    assert {:ok, :ok} = Task.yield(drain, 1_000)
    assert :ok = SyncGate.resume(shard_index, pause_lease, 1_000)
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
