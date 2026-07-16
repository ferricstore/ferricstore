defmodule Ferricstore.WaitersConcurrencyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Waiters

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
