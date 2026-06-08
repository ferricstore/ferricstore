defmodule Ferricstore.Bugs.IncrOverflowTest do
  @moduledoc """
  Bug #1: INCR/DECR Integer Overflow Not Detected

  Elixir integers are arbitrary precision, so INCR past int64 max
  (9223372036854775807) returns 9223372036854775808 instead of Redis's
  "ERR increment or decrement would overflow".

  File: state_machine.ex (do_incr) / ferricstore.ex (incr_by)
  Affects: INCR, DECR, INCRBY, DECRBY
  """

  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state
  @moduletag timeout: 30_000

  @int64_max 9_223_372_036_854_775_807
  @int64_min -9_223_372_036_854_775_808

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  describe "INCR overflow past int64 max" do
    test "INCR at int64 max - 1 reaches max" do
      FerricStore.set("ovf:incr", Integer.to_string(@int64_max - 1))
      assert {:ok, @int64_max} = FerricStore.incr("ovf:incr")
    end

    test "INCR past int64 max should return overflow error" do
      FerricStore.set("ovf:incr2", Integer.to_string(@int64_max))
      result = FerricStore.incr("ovf:incr2")

      # Redis returns: (error) ERR increment or decrement would overflow
      # BUG: Currently returns {:ok, 9223372036854775808} (no bounds check)
      assert {:error, msg} = result,
             "INCR past int64 max should return error, got: #{inspect(result)}"

      assert msg =~ "overflow"
    end
  end

  describe "DECR overflow past int64 min" do
    test "DECR at int64 min + 1 reaches min" do
      FerricStore.set("ovf:decr", Integer.to_string(@int64_min + 1))
      assert {:ok, @int64_min} = FerricStore.decr("ovf:decr")
    end

    test "DECR past int64 min should return overflow error" do
      FerricStore.set("ovf:decr2", Integer.to_string(@int64_min))
      result = FerricStore.decr("ovf:decr2")

      # Redis returns: (error) ERR increment or decrement would overflow
      # BUG: Currently returns {:ok, -9223372036854775809}
      assert {:error, msg} = result,
             "DECR past int64 min should return error, got: #{inspect(result)}"

      assert msg =~ "overflow"
    end
  end

  describe "INCRBY overflow with large delta" do
    test "INCRBY that would exceed int64 max should return overflow error" do
      FerricStore.set("ovf:incrby", Integer.to_string(@int64_max - 5))
      result = FerricStore.incr_by("ovf:incrby", 10)

      # Redis returns: (error) ERR increment or decrement would overflow
      # BUG: Currently returns {:ok, 9223372036854775812}
      assert {:error, msg} = result,
             "INCRBY exceeding int64 max should return error, got: #{inspect(result)}"

      assert msg =~ "overflow"
    end

    test "DECRBY that would go below int64 min should return overflow error" do
      FerricStore.set("ovf:decrby", Integer.to_string(@int64_min + 5))
      result = FerricStore.decr_by("ovf:decrby", 10)

      # Redis returns: (error) ERR increment or decrement would overflow
      # BUG: Currently returns {:ok, -9223372036854775813}
      assert {:error, msg} = result,
             "DECRBY below int64 min should return error, got: #{inspect(result)}"

      assert msg =~ "overflow"
    end
  end
end
