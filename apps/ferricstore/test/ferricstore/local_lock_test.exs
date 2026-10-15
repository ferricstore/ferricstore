defmodule Ferricstore.LocalLockTest do
  # Timing assertions below need the schedulers to themselves.
  use ExUnit.Case, async: false

  alias Ferricstore.LocalLock

  defp lock_id(resource), do: {{__MODULE__, resource}, self()}

  test "a waiter takes the lock soon after the holder releases it" do
    # :global.trans sleeps a random 0.25-8 s between attempts, so a waiter
    # usually finds the lock long after it was freed.
    delays =
      for _trial <- 1..12 do
        resource = make_ref()
        parent = self()

        holder =
          spawn_link(fn ->
            LocalLock.trans(lock_id(resource), fn ->
              send(parent, :held)
              receive do: (:release -> :ok)
              send(parent, {:released, System.monotonic_time(:microsecond)})
            end)
          end)

        assert_receive :held

        waiter =
          Task.async(fn ->
            LocalLock.trans(lock_id(resource), fn -> System.monotonic_time(:microsecond) end)
          end)

        Process.sleep(20)
        send(holder, :release)
        assert_receive {:released, released_at}

        div(Task.await(waiter, 30_000) - released_at, 1_000)
      end

    assert Enum.max(delays) < 100, "lock handoff delays (ms): #{inspect(delays)}"
  end

  test "only one process holds a lock at a time" do
    resource = make_ref()
    counter = :atomics.new(2, [])

    1..16
    |> Enum.map(fn _ ->
      Task.async(fn ->
        for _ <- 1..5 do
          LocalLock.trans(lock_id(resource), fn ->
            inside = :atomics.add_get(counter, 1, 1)
            :atomics.put(counter, 2, max(:atomics.get(counter, 2), inside))
            Process.sleep(1)
            :atomics.sub(counter, 1, 1)
          end)
        end
      end)
    end)
    |> Task.await_many(30_000)

    assert :atomics.get(counter, 2) == 1
  end

  test "returns the function result and releases the lock when it raises" do
    resource = make_ref()

    assert :done = LocalLock.trans(lock_id(resource), fn -> :done end)

    assert_raise RuntimeError, fn ->
      LocalLock.trans(lock_id(resource), fn -> raise "boom" end)
    end

    assert :again =
             Task.async(fn -> LocalLock.trans(lock_id(resource), fn -> :again end) end)
             |> Task.await(1_000)
  end

  test "a holder that dies releases its lock" do
    resource = make_ref()
    parent = self()

    {holder, monitor} =
      spawn_monitor(fn ->
        LocalLock.trans(lock_id(resource), fn ->
          send(parent, :held)
          Process.sleep(:infinity)
        end)
      end)

    assert_receive :held
    waiter = Task.async(fn -> LocalLock.trans(lock_id(resource), fn -> :acquired end) end)

    Process.exit(holder, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^holder, :killed}
    assert :acquired = Task.await(waiter, 5_000)
  end

  test "node-local locks do not wait out :global.trans backoff sleeps" do
    sources = [
      "lib/ferricstore/store/standalone_tx_log.ex",
      "lib/ferricstore/store/segment_lock.ex",
      "lib/ferricstore/flow/history_projected_index.ex",
      "lib/ferricstore/flow/lmdb_replay_safe_index.ex",
      "lib/ferricstore/flow/hibernation.ex",
      "lib/ferricstore/raft/replay_safe_index.ex",
      "lib/ferricstore/raft/waraft_storage/sections/recovery.ex"
    ]

    calls =
      for source <- sources,
          {:ok, ast} = source |> File.read!() |> Code.string_to_quoted(),
          {_ast, lines} =
            Macro.prewalk(ast, [], fn
              {{:., meta, [:global, :trans]}, _call_meta, args} = node, acc
              when length(args) == 3 ->
                {node, [meta[:line] | acc]}

              node, acc ->
                {node, acc}
            end),
          line <- Enum.reverse(lines),
          do: "#{source}:#{line}"

    assert calls == []
  end

  test "different resources do not wait for each other" do
    parent = self()
    held = make_ref()

    holder =
      spawn_link(fn ->
        LocalLock.trans(lock_id(held), fn ->
          send(parent, :held)
          receive do: (:release -> :ok)
        end)
      end)

    assert_receive :held
    assert :other = LocalLock.trans(lock_id(make_ref()), fn -> :other end)
    send(holder, :release)
  end
end
