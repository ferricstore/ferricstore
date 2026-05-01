defmodule Ferricstore.Commands.BlockingTest do
  @moduledoc """
  Unit tests for blocking list commands and the waiter registry.

  Tests cover:
  - `Ferricstore.Commands.Blocking` (argument parsing)
  - `Ferricstore.Waiters` (ETS-based waiter registry)
  - Blocking behavior via BEAM process messaging
  - BLMOVE non-blocking (immediate) behavior
  - BLMPOP non-blocking (immediate) behavior

  Most tests are async since they use isolated mock stores and process-local
  waiter registrations.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Blocking, List}
  alias Ferricstore.Test.MockStore
  alias Ferricstore.Waiters

  # ===========================================================================
  # Setup -- ensure the waiter ETS table exists
  # ===========================================================================

  setup do
    # The ETS table may already exist from a previous test or application start.
    # Create it only if it does not exist yet.
    if :ets.whereis(:ferricstore_waiters) == :undefined do
      Waiters.init()
    end

    # Clean up any leftover waiters from prior tests
    :ets.delete_all_objects(:ferricstore_waiters)

    :ok
  end

  # ===========================================================================
  # Blocking argument parsing -- BLPOP / BRPOP
  # ===========================================================================

  describe "parse_blpop_args/1" do
    test "parses single key with integer timeout" do
      assert {:ok, ["mylist"], 5000} = Blocking.parse_blpop_args(["mylist", "5"])
    end

    test "parses multiple keys with timeout" do
      assert {:ok, ["a", "b", "c"], 10_000} = Blocking.parse_blpop_args(["a", "b", "c", "10"])
    end

    test "parses timeout 0 as block forever" do
      assert {:ok, ["mylist"], 0} = Blocking.parse_blpop_args(["mylist", "0"])
    end

    test "parses float timeout" do
      assert {:ok, ["mylist"], 100} = Blocking.parse_blpop_args(["mylist", "0.1"])
    end

    test "rejects negative timeout" do
      assert {:error, _} = Blocking.parse_blpop_args(["mylist", "-1"])
    end

    test "rejects non-numeric timeout" do
      assert {:error, _} = Blocking.parse_blpop_args(["mylist", "abc"])
    end

    test "rejects empty args" do
      assert {:error, _} = Blocking.parse_blpop_args([])
    end

    test "rejects single arg (no timeout)" do
      assert {:error, _} = Blocking.parse_blpop_args(["mylist"])
    end
  end

  # ===========================================================================
  # Blocking argument parsing -- BLMOVE
  # ===========================================================================

  describe "parse_blmove_args/1" do
    test "parses valid BLMOVE args" do
      assert {:ok, "src", "dst", :left, :right, 5000} =
               Blocking.parse_blmove_args(["src", "dst", "LEFT", "RIGHT", "5"])
    end

    test "direction is case-insensitive" do
      assert {:ok, "src", "dst", :right, :left, 0} =
               Blocking.parse_blmove_args(["src", "dst", "right", "left", "0"])
    end

    test "rejects invalid direction" do
      assert {:error, _} = Blocking.parse_blmove_args(["src", "dst", "UP", "DOWN", "5"])
    end

    test "rejects wrong number of args" do
      assert {:error, _} = Blocking.parse_blmove_args(["src", "dst", "LEFT"])
      assert {:error, _} = Blocking.parse_blmove_args([])
    end
  end

  # ===========================================================================
  # Blocking argument parsing -- BLMPOP
  # ===========================================================================

  describe "parse_blmpop_args/1" do
    test "parses valid BLMPOP args with single key" do
      assert {:ok, ["mylist"], :left, 1, 5000} =
               Blocking.parse_blmpop_args(["5", "1", "mylist", "LEFT"])
    end

    test "parses multiple keys" do
      assert {:ok, ["a", "b"], :right, 1, 0} =
               Blocking.parse_blmpop_args(["0", "2", "a", "b", "RIGHT"])
    end

    test "parses with COUNT option" do
      assert {:ok, ["mylist"], :left, 3, 5000} =
               Blocking.parse_blmpop_args(["5", "1", "mylist", "LEFT", "COUNT", "3"])
    end

    test "rejects wrong number of args" do
      assert {:error, _} = Blocking.parse_blmpop_args(["5"])
      assert {:error, _} = Blocking.parse_blmpop_args([])
    end

    test "rejects invalid direction" do
      assert {:error, _} = Blocking.parse_blmpop_args(["5", "1", "mylist", "UP"])
    end
  end

  # ===========================================================================
  # Waiters registry
  # ===========================================================================

  describe "Waiters" do
    test "register and notify wakes the process" do
      key = "test_key_#{:erlang.unique_integer()}"
      me = self()

      # Spawn a waiter process
      waiter =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, :woken)
          after
            1000 -> send(me, :timeout)
          end
        end)

      # Give the waiter time to register
      Process.sleep(10)

      # Notify and check it woke up
      assert Waiters.notify_push(key) == waiter

      assert_receive :woken, 500
    end

    test "FIFO ordering: oldest waiter is notified first" do
      key = "test_fifo_#{:erlang.unique_integer()}"
      me = self()

      # Spawn two waiters with a small delay between them
      waiter1 =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, {:woken, 1})
          after
            1000 -> send(me, {:timeout, 1})
          end
        end)

      Process.sleep(5)

      _waiter2 =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} -> send(me, {:woken, 2})
          after
            1000 -> send(me, {:timeout, 2})
          end
        end)

      Process.sleep(10)

      # Notify once -- only waiter1 should wake
      notified = Waiters.notify_push(key)
      assert notified == waiter1

      assert_receive {:woken, 1}, 500
      refute_receive {:woken, 2}, 100
    end

    test "notify_push returns nil when no waiters" do
      key = "test_empty_#{:erlang.unique_integer()}"
      assert Waiters.notify_push(key) == nil
    end

    test "unregister removes waiter" do
      key = "test_unreg_#{:erlang.unique_integer()}"
      Waiters.register(key, self(), 0)
      assert Waiters.count(key) == 1

      Waiters.unregister(key, self())
      assert Waiters.count(key) == 0
    end

    test "cleanup removes all waiters for a pid" do
      key1 = "test_clean1_#{:erlang.unique_integer()}"
      key2 = "test_clean2_#{:erlang.unique_integer()}"

      Waiters.register(key1, self(), 0)
      Waiters.register(key2, self(), 0)

      assert Waiters.count(key1) == 1
      assert Waiters.count(key2) == 1

      Waiters.cleanup(self())

      assert Waiters.count(key1) == 0
      assert Waiters.count(key2) == 0
    end

    test "multiple waiters on same key" do
      key = "test_multi_#{:erlang.unique_integer()}"
      pid1 = spawn(fn -> Process.sleep(5000) end)
      pid2 = spawn(fn -> Process.sleep(5000) end)

      Waiters.register(key, pid1, 0)
      Waiters.register(key, pid2, 0)

      assert Waiters.count(key) == 2

      # First notify wakes oldest
      Waiters.notify_push(key)
      assert Waiters.count(key) == 1

      # Second notify wakes the remaining one
      Waiters.notify_push(key)
      assert Waiters.count(key) == 0

      Process.exit(pid1, :kill)
      Process.exit(pid2, :kill)
    end
  end

  # ===========================================================================
  # Blocking behavior integration (process-level)
  # ===========================================================================

  describe "blocking pop behavior" do
    test "BLPOP returns immediately when list has data" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)

      assert ["mylist", "a"] == Blocking.handle("BLPOP", ["mylist", "5"], store)
      assert ["b"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "BRPOP returns immediately from the right side when list has data" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b"], store)

      assert ["mylist", "b"] == Blocking.handle("BRPOP", ["mylist", "5"], store)
      assert ["a"] == List.handle("LRANGE", ["mylist", "0", "-1"], store)
    end

    test "waiter notification triggers pop" do
      key = "blpop_test_#{:erlang.unique_integer()}"
      me = self()

      # Spawn a process that simulates blocking on the key
      _waiter =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} ->
              send(me, {:got_notification, key})
          after
            2000 ->
              send(me, :timeout)
          end
        end)

      Process.sleep(10)

      # Simulate a push to the key
      Waiters.notify_push(key)

      assert_receive {:got_notification, ^key}, 500
    end

    test "blocking with timeout expires when no push arrives" do
      key = "timeout_test_#{:erlang.unique_integer()}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(key, self(), 0)

          receive do
            {:waiter_notify, ^key} ->
              send(me, :unexpected_wake)
          after
            100 ->
              Waiters.unregister(key, self())
              send(me, :timed_out)
          end
        end)

      assert_receive :timed_out, 500
      refute_receive :unexpected_wake, 100
    end
  end

  # ===========================================================================
  # BLMOVE non-blocking (immediate) behavior
  # ===========================================================================

  describe "BLMOVE immediate (non-blocking handle/3)" do
    test "returns moved element when source is non-empty" do
      store = MockStore.make()
      # Populate source list: [a, b, c]
      List.handle("RPUSH", ["src", "a", "b", "c"], store)

      # BLMOVE src dst LEFT RIGHT 0
      result = Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "0"], store)
      assert result == "a"

      # Verify source lost the element
      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["b", "c"]
      # Verify destination received it
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["a"]
    end

    test "returns nil when source is empty" do
      store = MockStore.make()

      result = Blocking.handle("BLMOVE", ["src", "dst", "LEFT", "RIGHT", "5"], store)
      assert result == nil
    end

    test "moves from RIGHT to LEFT" do
      store = MockStore.make()
      List.handle("RPUSH", ["src", "a", "b", "c"], store)

      result = Blocking.handle("BLMOVE", ["src", "dst", "RIGHT", "LEFT", "0"], store)
      assert result == "c"

      assert List.handle("LRANGE", ["src", "0", "-1"], store) == ["a", "b"]
      assert List.handle("LRANGE", ["dst", "0", "-1"], store) == ["c"]
    end

    test "same source and destination rotates the list" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = Blocking.handle("BLMOVE", ["mylist", "mylist", "LEFT", "RIGHT", "0"], store)
      assert result == "a"

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["b", "c", "a"]
    end

    test "returns error for invalid direction" do
      store = MockStore.make()

      assert {:error, _} =
               Blocking.handle("BLMOVE", ["src", "dst", "UP", "DOWN", "5"], store)
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()

      assert {:error, _} = Blocking.handle("BLMOVE", ["src", "dst"], store)
    end
  end

  # ===========================================================================
  # BLMOVE waiter behavior (process-level)
  # ===========================================================================

  describe "BLMOVE waiter behavior" do
    test "waiter on source key wakes when push arrives" do
      source_key = "blmove_src_#{:erlang.unique_integer()}"
      me = self()

      _waiter =
        spawn(fn ->
          Waiters.register(source_key, self(), 0)

          receive do
            {:waiter_notify, ^source_key} ->
              send(me, {:got_blmove_notification, source_key})
          after
            2000 ->
              send(me, :blmove_timeout)
          end
        end)

      Process.sleep(10)

      Waiters.notify_push(source_key)

      assert_receive {:got_blmove_notification, ^source_key}, 500
    end

    test "waiter times out when no push arrives" do
      source_key = "blmove_timeout_#{:erlang.unique_integer()}"
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
  # BLMPOP non-blocking (immediate) behavior
  # ===========================================================================

  describe "BLMPOP immediate (non-blocking handle/3)" do
    test "returns [key, [element]] from first non-empty key" do
      store = MockStore.make()
      # key1 is empty, key2 has data
      List.handle("RPUSH", ["key2", "val1", "val2"], store)

      # BLMPOP 0 2 key1 key2 LEFT
      result = Blocking.handle("BLMPOP", ["0", "2", "key1", "key2", "LEFT"], store)
      assert result == ["key2", ["val1"]]
    end

    test "returns nil when all keys are empty" do
      store = MockStore.make()

      result = Blocking.handle("BLMPOP", ["5", "2", "empty1", "empty2", "LEFT"], store)
      assert result == nil
    end

    test "direction LEFT pops from left" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "LEFT"], store)
      assert result == ["mylist", ["a"]]

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["b", "c"]
    end

    test "direction RIGHT pops from right" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "RIGHT"], store)
      assert result == ["mylist", ["c"]]

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["a", "b"]
    end

    test "COUNT pops multiple elements" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "a", "b", "c", "d"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "LEFT", "COUNT", "3"], store)
      assert result == ["mylist", ["a", "b", "c"]]

      assert List.handle("LRANGE", ["mylist", "0", "-1"], store) == ["d"]
    end

    test "COUNT larger than list length returns all elements" do
      store = MockStore.make()
      List.handle("RPUSH", ["mylist", "x", "y"], store)

      result = Blocking.handle("BLMPOP", ["0", "1", "mylist", "LEFT", "COUNT", "10"], store)
      assert result == ["mylist", ["x", "y"]]
    end

    test "prefers first key in order when multiple keys have data" do
      store = MockStore.make()
      List.handle("RPUSH", ["k1", "from_k1"], store)
      List.handle("RPUSH", ["k2", "from_k2"], store)

      result = Blocking.handle("BLMPOP", ["0", "2", "k1", "k2", "LEFT"], store)
      assert result == ["k1", ["from_k1"]]
    end

    test "returns error for invalid direction" do
      store = MockStore.make()

      assert {:error, _} = Blocking.handle("BLMPOP", ["0", "1", "mylist", "UP"], store)
    end

    test "returns error for wrong number of arguments" do
      store = MockStore.make()

      assert {:error, _} = Blocking.handle("BLMPOP", ["5"], store)
    end
  end

  # ===========================================================================
  # BLMPOP waiter behavior (process-level)
  # ===========================================================================

  describe "BLMPOP waiter behavior" do
    test "registers waiters on all keys and wakes on notification" do
      key1 = "blmpop_k1_#{:erlang.unique_integer()}"
      key2 = "blmpop_k2_#{:erlang.unique_integer()}"
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
              send(me, :blmpop_timeout)
          end
        end)

      Process.sleep(10)

      # Push to key2 -- waiter should wake
      Waiters.notify_push(key2)

      assert_receive {:blmpop_woke, ^key2}, 500
    end

    test "waiter times out when no push arrives on any key" do
      key1 = "blmpop_to1_#{:erlang.unique_integer()}"
      key2 = "blmpop_to2_#{:erlang.unique_integer()}"
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
end
