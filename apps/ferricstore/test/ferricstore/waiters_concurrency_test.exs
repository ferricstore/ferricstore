defmodule Ferricstore.WaitersConcurrencyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Waiters

  for direction <- [:lpush, :rpush], api <- [:default, :instance] do
    @direction direction
    @api api
    test "embedded #{@api} #{@direction} notifies blocked callers" do
      key = "waiters:embedded:#{System.unique_integer([:positive])}"
      parent = self()

      waiter =
        spawn(fn ->
          :ok = Waiters.register(key, self(), 0)
          send(parent, :embedded_waiter_ready)
          forward_notifications(parent, key)
        end)

      on_exit(fn ->
        Process.exit(waiter, :kill)
        Waiters.cleanup(waiter)
        FerricStore.del(key)
      end)

      assert_receive :embedded_waiter_ready, 1_000

      result =
        case @api do
          :default ->
            apply(FerricStore, @direction, [key, ["value"]])

          :instance ->
            apply(FerricStore.Impl, @direction, [
              FerricStore.Instance.get(:default),
              key,
              ["value"]
            ])
        end

      assert {:ok, 1} = result
      assert_receive {:waiter_notified, ^waiter, ^key}, 1_000
    end
  end

  for direction <- [:lpushx, :rpushx] do
    @direction direction
    test "#{direction} starts the waiter wake-up chain" do
      key = "waiters:batch-push:#{System.unique_integer([:positive])}"
      ctx = FerricStore.Instance.get(:default)

      assert {:ok, 1} = FerricStore.rpush(key, ["existing"])

      parent = self()

      waiter =
        spawn(fn ->
          :ok = Waiters.register(key, self(), 0)
          send(parent, :batch_waiter_ready)
          forward_notifications(parent, key)
        end)

      on_exit(fn ->
        Process.exit(waiter, :kill)
        Waiters.cleanup(waiter)
        FerricStore.del(key)
      end)

      assert_receive :batch_waiter_ready, 1_000

      assert 3 ==
               Ferricstore.Commands.List.handle_ast({@direction, [key, "one", "two"]}, ctx)

      assert_receive {:waiter_notified, ^waiter, ^key}, 1_000
    end
  end

  test "waiter death cleanup survives a monitor process restart" do
    key = "waiters:restart:#{System.unique_integer([:positive])}"
    parent = self()

    waiter =
      spawn(fn ->
        :ok = Waiters.register(key, self(), 0)
        send(parent, :restart_waiter_registered)
        Process.sleep(:infinity)
      end)

    on_exit(fn ->
      Process.exit(waiter, :kill)
      Waiters.cleanup(waiter)
      Supervisor.restart_child(Ferricstore.Supervisor, Waiters.Monitor)
    end)

    assert_receive :restart_waiter_registered, 1_000
    assert Waiters.count(key) == 1
    assert :ok = Supervisor.terminate_child(Ferricstore.Supervisor, Waiters.Monitor)
    assert {:ok, _pid} = Supervisor.restart_child(Ferricstore.Supervisor, Waiters.Monitor)
    Process.exit(waiter, :kill)
    Ferricstore.Test.ShardHelpers.eventually(fn -> Waiters.count(key) == 0 end)
  end

  test "concurrent push notifications claim each waiter at most once" do
    key = "waiters:claim:#{System.unique_integer([:positive])}"
    parent = self()

    waiters =
      for _ <- 1..64 do
        spawn(fn ->
          :ok = Waiters.register(key, self(), 0)
          send(parent, :waiter_registered)
          forward_notifications(parent, key)
        end)
      end

    on_exit(fn ->
      Enum.each(waiters, &Process.exit(&1, :kill))
      Enum.each(waiters, &Waiters.cleanup/1)
    end)

    for _ <- waiters, do: assert_receive(:waiter_registered)

    callers =
      for _ <- 1..256 do
        spawn(fn ->
          receive do
            :go -> send(parent, {:notify_done, Waiters.notify_push(key)})
          end
        end)
      end

    Enum.each(callers, &send(&1, :go))
    for _ <- callers, do: assert_receive({:notify_done, _result})

    notified = collect_notifications(key, [])

    assert length(notified) == length(waiters)
    assert MapSet.new(notified) == MapSet.new(waiters)
    assert Waiters.count(key) == 0
  end

  defp forward_notifications(parent, key) do
    receive do
      {:waiter_notify, ^key} ->
        send(parent, {:waiter_notified, self(), key})
        forward_notifications(parent, key)
    end
  end

  defp collect_notifications(key, acc) do
    receive do
      {:waiter_notified, pid, ^key} -> collect_notifications(key, [pid | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end
end
