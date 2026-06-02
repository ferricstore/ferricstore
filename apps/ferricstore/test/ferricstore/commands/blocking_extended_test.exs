defmodule Ferricstore.Commands.BlockingExtendedTest do
  @moduledoc """
  Extended tests for blocking list commands: BLMOVE, BLMPOP, BRPOPLPUSH.

  Tests cover:
    - BLMOVE: immediate move, blocking wake, timeout, direction combinations
    - BLMPOP: pop from first non-empty list, blocking wake, timeout, numkeys edge cases
    - BRPOPLPUSH: immediate move, blocking wake, timeout (deprecated compat command)
    - Stress: 50 concurrent BLMOVE waiters each receive a unique element
    - Edge cases: direction combinations, numkeys=0
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Blocking, List}
  alias Ferricstore.Test.MockStore
  alias Ferricstore.Waiters

  @moduletag timeout: 30_000

  # ===========================================================================
  # Setup -- ensure the waiter ETS table exists
  # ===========================================================================

  setup do
    if :ets.whereis(:ferricstore_waiters) == :undefined do
      Waiters.init()
    end

    :ets.delete_all_objects(:ferricstore_waiters)

    :ok
  end

  # ===========================================================================
  # BLMOVE -- immediate (non-blocking) behavior
  # ===========================================================================

  describe "BLMOVE moves element when source has data" do
    test "BLMOVE LEFT RIGHT moves leftmost to right end of destination" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store)
      assert result == "a"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["b", "c"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["a"]
    end

    test "BLMOVE RIGHT LEFT moves rightmost to left end of destination" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "RIGHT", "LEFT", "0"], store)
      assert result == "c"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["a", "b"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["c"]
    end

    test "BLMOVE appends to existing destination" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store)
      assert result == "a"

      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["x", "y", "a"]
    end

    test "BLMOVE with same source and destination rotates the list" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = Blocking.handle("BLMOVE", ["mylist", "mylist", "LEFT", "RIGHT", "0"], store)
      assert result == "a"

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["b", "c", "a"]
    end
  end

  # ===========================================================================
  # BLMOVE -- blocking with notification
  # ===========================================================================

  describe "BLMOVE blocks and returns when element pushed to source" do
    test "waiter wakes up when source receives a push" do
      source_key = "blmove_wake_#{:erlang.unique_integer([:positive])}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(source_key, self(), 0)

          receive do
            {:waiter_notify, ^source_key} ->
              send(me, {:blmove_woke, source_key})
          after
            2000 ->
              Waiters.unregister(source_key, self())
              send(me, :blmove_timeout)
          end
        end)

      Process.sleep(20)
      assert Waiters.count(source_key) == 1

      Waiters.notify_push(source_key)

      assert_receive {:blmove_woke, ^source_key}, 500
    end

    test "multiple sequential BLMOVE operations work correctly" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)

      assert Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store) == "a"
      assert Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store) == "b"
      assert Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store) == "c"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == []
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["a", "b", "c"]
    end
  end

  # ===========================================================================
  # BLMOVE -- timeout
  # ===========================================================================

  describe "BLMOVE timeout returns nil" do
    test "returns nil when source is empty (non-blocking handle)" do
      store = MockStore.make()

      result = Blocking.handle("BLMOVE", ["empty_src", "dst", "LEFT", "RIGHT", "5"], store)
      assert result == nil
    end

    test "waiter times out when no push arrives" do
      source_key = "blmove_to_#{:erlang.unique_integer([:positive])}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(source_key, self(), 0)

          receive do
            {:waiter_notify, ^source_key} ->
              send(me, :blmove_unexpected_wake)
          after
            100 ->
              Waiters.unregister(source_key, self())
              send(me, :blmove_timed_out)
          end
        end)

      assert_receive :blmove_timed_out, 500
      refute_receive :blmove_unexpected_wake, 100
    end
  end

  # ===========================================================================
  # BLMPOP -- pop from first non-empty list
  # ===========================================================================

  describe "BLMPOP pops from first non-empty list" do
    test "skips empty lists and pops from first non-empty one" do
      store = MockStore.make()
      # k1, k2 are empty; k3 has data
      List.handle("RPUSH", ["k3", "val1", "val2"], store)

      result = Blocking.handle("BLMPOP", ["0", "3", "k1", "k2", "k3", "LEFT"], store)
      assert result == ["k3", ["val1"]]
    end

    test "pops from first key when multiple have data" do
      store = MockStore.make()
      List.handle("RPUSH", ["k1", "from_k1"], store)
      List.handle("RPUSH", ["k2", "from_k2"], store)

      result = Blocking.handle("BLMPOP", ["0", "2", "k1", "k2", "LEFT"], store)
      assert result == ["k1", ["from_k1"]]
    end

    test "LEFT direction pops from left" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "LEFT"], store)
      assert result == ["mylist", ["a"]]

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["b", "c"]
    end

    test "RIGHT direction pops from right" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "RIGHT"], store)
      assert result == ["mylist", ["c"]]

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["a", "b"]
    end

    test "COUNT pops multiple elements" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "LEFT", "COUNT", "2"], store)
      assert result == ["mylist", ["a", "b"]]

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["c", "d"]
    end

    test "returns WRONGTYPE instead of skipping a non-list key" do
      store = MockStore.make()
      :ok = store.put.("string_key", "not-a-list", 0)
      List.handle("RPUSH", ["list_key", "value"], store)

      assert {:error, msg} =
               Blocking.handle("BLMPOP", ["0", "2", "string_key", "list_key", "LEFT"], store)

      assert msg =~ "WRONGTYPE"
      assert List.handle("LRANGE", ["list_key", "0", "-1"], store) == ["value"]
    end
  end

  # ===========================================================================
  # BLMPOP -- blocking with empty lists
  # ===========================================================================

  describe "BLMPOP with all empty lists blocks then returns" do
    test "returns nil when all keys are empty (non-blocking handle)" do
      store = MockStore.make()

      result = Blocking.handle("BLMPOP", ["5", "2", "empty1", "empty2", "LEFT"], store)
      assert result == nil
    end

    test "waiter on multiple keys wakes when one receives data" do
      key1 = "blmpop_w1_#{:erlang.unique_integer([:positive])}"
      key2 = "blmpop_w2_#{:erlang.unique_integer([:positive])}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(key1, self(), 0)
          Waiters.register(key2, self(), 0)

          receive do
            {:waiter_notify, notified_key} ->
              send(me, {:blmpop_woke, notified_key})
          after
            2000 ->
              Waiters.unregister(key1, self())
              Waiters.unregister(key2, self())
              send(me, :blmpop_timeout)
          end
        end)

      Process.sleep(20)

      # Push to key2 -- waiter should wake
      Waiters.notify_push(key2)

      assert_receive {:blmpop_woke, ^key2}, 500
    end

    test "waiter times out when no push arrives on any key" do
      key1 = "blmpop_to1_#{:erlang.unique_integer([:positive])}"
      key2 = "blmpop_to2_#{:erlang.unique_integer([:positive])}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(key1, self(), 0)
          Waiters.register(key2, self(), 0)

          receive do
            {:waiter_notify, _} ->
              send(me, :blmpop_unexpected_wake)
          after
            100 ->
              Waiters.unregister(key1, self())
              Waiters.unregister(key2, self())
              send(me, :blmpop_timed_out)
          end
        end)

      assert_receive :blmpop_timed_out, 500
      refute_receive :blmpop_unexpected_wake, 100
    end
  end

  # ===========================================================================
  # BRPOPLPUSH -- argument parsing
  # ===========================================================================

  describe "BRPOPLPUSH argument parsing" do
    test "parses valid BRPOPLPUSH args" do
      assert {:ok, "src", "dst", 5000} =
               Blocking.parse_brpoplpush_args(["src", "dst", "5"])
    end

    test "parses timeout 0 as block forever" do
      assert {:ok, "src", "dst", 0} =
               Blocking.parse_brpoplpush_args(["src", "dst", "0"])
    end

    test "parses float timeout" do
      assert {:ok, "src", "dst", 1500} =
               Blocking.parse_brpoplpush_args(["src", "dst", "1.5"])
    end

    test "rejects negative timeout" do
      assert {:error, _} = Blocking.parse_brpoplpush_args(["src", "dst", "-1"])
    end

    test "rejects non-numeric timeout" do
      assert {:error, _} = Blocking.parse_brpoplpush_args(["src", "dst", "abc"])
    end

    test "rejects wrong number of arguments" do
      assert {:error, _} = Blocking.parse_brpoplpush_args(["src", "dst"])
      assert {:error, _} = Blocking.parse_brpoplpush_args(["src"])
      assert {:error, _} = Blocking.parse_brpoplpush_args([])
      assert {:error, _} = Blocking.parse_brpoplpush_args(["a", "b", "c", "d"])
    end
  end

  # ===========================================================================
  # BRPOPLPUSH -- works like RPOPLPUSH with blocking
  # ===========================================================================

  describe "BRPOPLPUSH works like RPOPLPUSH with blocking" do
    test "pops from right of source and pushes to left of destination" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)

      result = Blocking.handle("BRPOPLPUSH", ["src", "dst", "0"], store)
      assert result == "c"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["a", "b"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["c"]
    end

    test "returns nil when source is empty" do
      store = MockStore.make()

      result = Blocking.handle("BRPOPLPUSH", ["empty_src", "dst", "5"], store)
      assert result == nil
    end

    test "pushes to existing destination at left" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)

      result = Blocking.handle("BRPOPLPUSH", ["src", "dst", "0"], store)
      assert result == "b"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["a"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["b", "x", "y"]
    end

    test "same source and destination rotates (RPOP then LPUSH)" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = Blocking.handle("BRPOPLPUSH", ["mylist", "mylist", "0"], store)
      assert result == "c"

      # "c" was popped from right and pushed to left
      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["c", "a", "b"]
    end

    test "waiter wakes up on push to source key" do
      source_key = "brpoplpush_wake_#{:erlang.unique_integer([:positive])}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(source_key, self(), 0)

          receive do
            {:waiter_notify, ^source_key} ->
              send(me, {:brpoplpush_woke, source_key})
          after
            2000 ->
              Waiters.unregister(source_key, self())
              send(me, :brpoplpush_timeout)
          end
        end)

      Process.sleep(20)
      assert Waiters.count(source_key) == 1

      Waiters.notify_push(source_key)

      assert_receive {:brpoplpush_woke, ^source_key}, 500
    end

    test "waiter times out when no push arrives" do
      source_key = "brpoplpush_to_#{:erlang.unique_integer([:positive])}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(source_key, self(), 0)

          receive do
            {:waiter_notify, ^source_key} ->
              send(me, :brpoplpush_unexpected_wake)
          after
            100 ->
              Waiters.unregister(source_key, self())
              send(me, :brpoplpush_timed_out)
          end
        end)

      assert_receive :brpoplpush_timed_out, 500
      refute_receive :brpoplpush_unexpected_wake, 100
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()

      assert {:error, _} = Blocking.handle("BRPOPLPUSH", ["src", "dst"], store)
    end

    test "returns error for invalid timeout" do
      store = MockStore.make()

      assert {:error, _} = Blocking.handle("BRPOPLPUSH", ["src", "dst", "notanumber"], store)
    end
  end

  # ===========================================================================
  # RPOPLPUSH -- non-blocking (in List module)
  # ===========================================================================

  describe "RPOPLPUSH non-blocking" do
    test "pops from right of source and pushes to left of destination" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)

      result = List.handle("RPOPLPUSH", ["src", "dst"], store)
      assert result == "c"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["a", "b"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["c"]
    end

    test "returns nil when source is empty" do
      store = MockStore.make()

      result = List.handle("RPOPLPUSH", ["empty_src", "dst"], store)
      assert result == nil
    end

    test "same source and destination rotates" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = List.handle("RPOPLPUSH", ["mylist", "mylist"], store)
      assert result == "c"

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["c", "a", "b"]
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()

      assert {:error, _} = List.handle("RPOPLPUSH", ["only_one"], store)
      assert {:error, _} = List.handle("RPOPLPUSH", [], store)
      assert {:error, _} = List.handle("RPOPLPUSH", ["a", "b", "c"], store)
    end
  end

  # ===========================================================================
  # Stress: 50 concurrent BLMOVE waiters, all get unique elements
  # ===========================================================================

  describe "stress: 50 concurrent BLMOVE waiters all get unique elements" do
    test "50 waiters on same source key each receive exactly one unique element" do
      source_key = "stress_blmove_#{:erlang.unique_integer([:positive])}"
      me = self()
      n = 50

      # Spawn 50 waiter processes, each registers on the source key
      waiters =
        for i <- 1..n do
          spawn(fn ->
            Waiters.register(source_key, self(), 0)

            receive do
              {:waiter_notify, ^source_key} ->
                # Simulate: waiter would pop from the list.
                # We just report back which waiter was woken.
                send(me, {:woken, i})
            after
              5000 ->
                Waiters.unregister(source_key, self())
                send(me, {:timeout, i})
            end
          end)
        end

      # Give all waiters time to register
      Process.sleep(50)
      assert Waiters.count(source_key) == n

      # Push 50 elements (simulate via 50 notify_push calls)
      # Each notify_push wakes the oldest waiter (FIFO)
      notified_pids =
        for _ <- 1..n do
          Waiters.notify_push(source_key)
        end

      # All notified pids should be distinct
      assert length(Enum.uniq(notified_pids)) == n

      # All 50 waiters should have been woken
      woken_ids =
        for _ <- 1..n do
          receive do
            {:woken, id} -> id
            {:timeout, id} -> {:timeout, id}
          after
            2000 -> :missing
          end
        end

      # Assert all were woken (no timeouts)
      assert Enum.all?(woken_ids, &is_integer/1),
             "Some waiters timed out or were missing: #{inspect(woken_ids)}"

      # Assert all IDs are unique (each waiter woken exactly once)
      assert length(Enum.uniq(woken_ids)) == n

      # No leftover waiters
      assert Waiters.count(source_key) == 0

      # Clean up waiter processes
      Enum.each(waiters, fn pid ->
        if Process.alive?(pid), do: Process.exit(pid, :kill)
      end)
    end
  end

  # ===========================================================================
  # Edge: BLMOVE LEFT RIGHT vs RIGHT LEFT
  # ===========================================================================

  describe "edge: BLMOVE direction combinations" do
    test "LEFT LEFT: pop left, push left" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "LEFT", "0"], store)
      assert result == "a"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["b", "c"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["a", "x", "y"]
    end

    test "RIGHT RIGHT: pop right, push right" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "RIGHT", "RIGHT", "0"], store)
      assert result == "c"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["a", "b"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["x", "y", "c"]
    end

    test "LEFT RIGHT: pop left, push right" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store)
      assert result == "a"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["b", "c"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["x", "y", "a"]
    end

    test "RIGHT LEFT: pop right, push left" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)
      List.handle("RPUSH", ["dst", "x", "y"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "RIGHT", "LEFT", "0"], store)
      assert result == "c"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["a", "b"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["c", "x", "y"]
    end

    test "case-insensitive direction parsing" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "left", "right", "0"], store)
      assert result == "a"
    end
  end

  # ===========================================================================
  # Edge: BLMPOP with numkeys=0
  # ===========================================================================

  describe "edge: BLMPOP with numkeys=0" do
    test "numkeys=0 returns error" do
      store = MockStore.make()

      # numkeys=0 means no keys, which is invalid
      result = Blocking.handle("BLMPOP", ["0", "0", "LEFT"], store)
      assert {:error, _} = result
    end
  end
end
