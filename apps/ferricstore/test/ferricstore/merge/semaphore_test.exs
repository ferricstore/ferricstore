defmodule Ferricstore.Merge.SemaphoreTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Merge.Semaphore

  setup do
    {:ok, pid} = Semaphore.start_link(name: :"semaphore_#{:erlang.unique_integer([:positive])}")
    %{semaphore: pid}
  end

  describe "acquire/2" do
    test "succeeds when semaphore is free", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(0, sem)
    end

    test "returns busy when another shard holds it", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(0, sem)
      assert {:busy, 0} = Semaphore.acquire(1, sem)
    end

    test "returns busy with holder shard index", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(3, sem)
      assert {:busy, 3} = Semaphore.acquire(0, sem)
      assert {:busy, 3} = Semaphore.acquire(1, sem)
    end
  end

  describe "release/2" do
    test "releases the semaphore so another shard can acquire", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(0, sem)
      assert :ok = Semaphore.release(0, sem)
      assert :ok = Semaphore.acquire(1, sem)
    end

    test "returns error when non-holder tries to release", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(0, sem)
      assert {:error, :not_holder} = Semaphore.release(1, sem)
    end

    test "does not let another process release the holder's shard", %{semaphore: sem} do
      parent = self()

      holder =
        spawn(fn ->
          :ok = Semaphore.acquire(7, sem)
          send(parent, :holder_acquired)

          receive do
            :release -> send(parent, {:holder_release, Semaphore.release(7, sem)})
          end
        end)

      assert_receive :holder_acquired
      assert {:error, :not_holder} = Semaphore.release(7, sem)
      assert {:held, 7} = Semaphore.status(sem)

      send(holder, :release)
      assert_receive {:holder_release, :ok}
      assert :free = Semaphore.status(sem)
    end

    test "returns error when nothing is held", %{semaphore: sem} do
      assert {:error, :not_holder} = Semaphore.release(0, sem)
    end
  end

  describe "status/1" do
    test "returns :free when no merge is running", %{semaphore: sem} do
      assert :free = Semaphore.status(sem)
    end

    test "returns {:held, shard_index} when acquired", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(2, sem)
      assert {:held, 2} = Semaphore.status(sem)
    end

    test "returns :free after release", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(0, sem)
      assert :ok = Semaphore.release(0, sem)
      assert :free = Semaphore.status(sem)
    end
  end

  describe "auto-release on process crash" do
    test "releases semaphore when holder process dies", %{semaphore: sem} do
      test_pid = self()

      # Spawn a process that acquires the semaphore then exits.
      spawn(fn ->
        :ok = Semaphore.acquire(5, sem)
        send(test_pid, :acquired)
        # Exit immediately.
      end)

      assert_receive :acquired, 2000

      # Poll until the semaphore is free (DOWN message processed).
      # Under heavy load the scheduler may delay message processing,
      # so we poll instead of relying on a fixed sleep.
      assert poll_until(fn -> Semaphore.status(sem) == :free end, 2000),
             "Semaphore did not auto-release within 2s after holder process exited"
    end

    test "another shard can acquire after holder crashes", %{semaphore: sem} do
      test_pid = self()

      spawn(fn ->
        :ok = Semaphore.acquire(0, sem)
        send(test_pid, :acquired)
      end)

      assert_receive :acquired, 2000

      # Poll until the semaphore is free before attempting acquire.
      assert poll_until(fn -> Semaphore.status(sem) == :free end, 2000),
             "Semaphore did not auto-release within 2s"

      # Now we can acquire from this process.
      assert :ok = Semaphore.acquire(1, sem)
    end
  end

  # Polls a predicate function every 20ms until it returns true or timeout.
  defp poll_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.reduce_while(Stream.repeatedly(fn -> :ok end), false, fn _, _ ->
      if fun.() do
        {:halt, true}
      else
        if System.monotonic_time(:millisecond) > deadline do
          {:halt, false}
        else
          Process.sleep(20)
          {:cont, false}
        end
      end
    end)
  end

  describe "acquire-release cycle" do
    test "multiple acquire-release cycles work correctly", %{semaphore: sem} do
      for i <- 0..9 do
        assert :ok = Semaphore.acquire(i, sem)
        assert {:held, ^i} = Semaphore.status(sem)
        assert :ok = Semaphore.release(i, sem)
        assert :free = Semaphore.status(sem)
      end
    end

    test "same shard cannot acquire twice", %{semaphore: sem} do
      assert :ok = Semaphore.acquire(0, sem)
      assert {:busy, 0} = Semaphore.acquire(0, sem)
    end
  end
end
