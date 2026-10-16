defmodule Ferricstore.Api.ListWaiterNotificationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Waiters

  for api <- [:public, :instance], operation <- [:lpush, :rpush] do
    test "#{api} #{operation} wakes an already registered list waiter" do
      key = "api:list-wake:#{System.unique_integer([:positive, :monotonic])}"
      waiter = self()

      on_exit(fn ->
        Waiters.unregister(key, waiter)
        FerricStore.del(key)
      end)

      assert :ok = Waiters.register(key, waiter, 0)
      assert {:ok, 1} = push(unquote(api), unquote(operation), key, ["value"])
      assert_receive {:waiter_notify, ^key}, 100
      assert {:ok, "value"} = FerricStore.lpop(key)
    end
  end

  for api <- [:public, :instance] do
    test "#{api} failed push does not wake a waiter" do
      key = "api:list-wake-error:#{System.unique_integer([:positive, :monotonic])}"
      waiter = self()

      on_exit(fn ->
        Waiters.unregister(key, waiter)
        FerricStore.del(key)
      end)

      assert :ok = FerricStore.set(key, "not a list")
      assert :ok = Waiters.register(key, waiter, 0)
      assert {:error, _} = push(unquote(api), :rpush, key, ["value"])
      refute_receive {:waiter_notify, ^key}, 50
    end

    test "#{api} multi-element push starts one FIFO waiter" do
      key = "api:list-wake-many:#{System.unique_integer([:positive, :monotonic])}"
      parent = self()

      waiters =
        for _ <- 1..3 do
          pid =
            spawn(fn -> receive do: ({:waiter_notify, ^key} -> send(parent, {:woke, self()})) end)

          :ok = Waiters.register(key, pid, 0)
          pid
        end

      on_exit(fn ->
        Enum.each(waiters, fn pid ->
          Waiters.unregister(key, pid)
          Process.exit(pid, :kill)
        end)

        FerricStore.del(key)
      end)

      assert {:ok, 2} = push(unquote(api), :rpush, key, ["a", "b"])
      assert_receive {:woke, first}, 100
      assert first in waiters
      refute_receive {:woke, _}, 50
    end
  end

  defp push(:public, operation, key, values), do: apply(FerricStore, operation, [key, values])

  defp push(:instance, operation, key, values),
    do: apply(FerricStore.Impl, operation, [FerricStore.API.Store.default_ctx(), key, values])
end
