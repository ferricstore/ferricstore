defmodule Ferricstore.ReviewR3.EmbeddedApiIssuesTest do
  @moduledoc """
  Tests proving/disproving Round 3 embedded API issues.

  R3-H1: Set operations don't propagate errors (WRONGTYPE swallowed by {:ok, _} wrapping)
  R3-H2: Sorted set operations don't propagate errors (same pattern)
  R3-M4: del() silently swallows errors (always returns :ok, ignores result)
  R3-M5: sismember returns bare boolean (not {:ok, boolean()})
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  # ===========================================================================
  # R3-H1: Set operations don't propagate WRONGTYPE errors
  # ===========================================================================

  describe "R3-H1: set operations error propagation" do
    test "sadd on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      # Store a plain string value under the key
      :ok = FerricStore.set("r3h1_str", "hello")

      # sadd on a string-typed key should return a WRONGTYPE error.
      # BUG: if sadd blindly wraps result in {:ok, result}, we get
      # {:ok, {:error, "WRONGTYPE ..."}} instead of {:error, "WRONGTYPE ..."}
      result = FerricStore.sadd("r3h1_str", ["member1"])

      # Assert the error is NOT double-wrapped
      refute match?({:ok, {:error, _}}, result),
             "sadd wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      # The correct return should be {:error, _}
      assert match?({:error, _}, result),
             "sadd should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "srem on a sorted-set key returns {:error, _}, not {:ok, {:error, _}}" do
      # Create a sorted set
      {:ok, 1} = FerricStore.zadd("r3h1_zset", [{1.0, "member1"}])

      # srem on a zset-typed key should produce WRONGTYPE
      result = FerricStore.srem("r3h1_zset", ["member1"])

      refute match?({:ok, {:error, _}}, result),
             "srem wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "srem should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "smembers on a sorted-set key returns {:error, _}, not {:ok, {:error, _}}" do
      {:ok, 1} = FerricStore.zadd("r3h1_zset2", [{1.0, "m"}])

      result = FerricStore.smembers("r3h1_zset2")

      refute match?({:ok, {:error, _}}, result),
             "smembers wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "smembers should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "scard on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h1_str2", "val")

      result = FerricStore.scard("r3h1_str2")

      refute match?({:ok, {:error, _}}, result),
             "scard wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "scard should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # R3-H2: Sorted set operations don't propagate WRONGTYPE errors
  # ===========================================================================

  describe "R3-H2: sorted set operations error propagation" do
    test "zadd on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h2_str", "hello")

      result = FerricStore.zadd("r3h2_str", [{1.0, "member1"}])

      refute match?({:ok, {:error, _}}, result),
             "zadd wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "zadd should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zadd on a set key returns {:error, _}, not {:ok, {:error, _}}" do
      {:ok, 1} = FerricStore.sadd("r3h2_set", ["member1"])

      result = FerricStore.zadd("r3h2_set", [{1.0, "member1"}])

      refute match?({:ok, {:error, _}}, result),
             "zadd wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "zadd should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zcard on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h2_str2", "val")

      result = FerricStore.zcard("r3h2_str2")

      refute match?({:ok, {:error, _}}, result),
             "zcard wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "zcard should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zrem on a set key returns {:error, _}, not {:ok, {:error, _}}" do
      {:ok, 1} = FerricStore.sadd("r3h2_set2", ["member1"])

      result = FerricStore.zrem("r3h2_set2", ["member1"])

      refute match?({:ok, {:error, _}}, result),
             "zrem wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "zrem should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zrange on a string key returns {:error, _}, not {:ok, {:error, _}}" do
      :ok = FerricStore.set("r3h2_str3", "val")

      result = FerricStore.zrange("r3h2_str3", 0, -1)

      refute match?({:ok, {:error, _}}, result),
             "zrange wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "zrange should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end

    test "zscore on a set key returns {:error, _} without crashing" do
      {:ok, 1} = FerricStore.sadd("r3h2_set3", ["member1"])

      # zscore does `case handle(...) do nil -> ... ; score_str -> Float.parse(score_str) end`
      # If WRONGTYPE {:error, _} is returned, it won't match either clause and will crash
      # with a FunctionClauseError or MatchError.
      result =
        try do
          FerricStore.zscore("r3h2_set3", "member1")
        rescue
          e -> {:rescued, e.__struct__}
        end

      refute match?({:rescued, _}, result),
             "zscore crashed on wrong type: #{inspect(result)}"

      refute match?({:ok, {:error, _}}, result),
             "zscore wraps WRONGTYPE error inside {:ok, _} — error is not propagated"

      assert match?({:error, _}, result),
             "zscore should return {:error, _} on type mismatch, got: #{inspect(result)}"
    end
  end

  # ===========================================================================
  # R3-M4: del() silently swallows errors / always returns :ok
  # ===========================================================================

  describe "R3-M4: del() return value" do
    test "del returns {:ok, 0} for non-existent key" do
      result = FerricStore.del("r3m4_nonexistent")
      assert {:ok, 0} = result
    end

    test "del returns {:ok, 1} for existing key" do
      :ok = FerricStore.set("r3m4_exists", "value")
      result = FerricStore.del("r3m4_exists")
      assert {:ok, 1} = result

      # Verify the key is actually gone
      assert {:ok, nil} = FerricStore.get("r3m4_exists")
    end

    test "del returns {:ok, count} like other write operations" do
      :ok = FerricStore.set("r3m4_count", "value")
      result = FerricStore.del("r3m4_count")
      assert {:ok, _count} = result
    end

    test "del with list of keys returns count of deleted" do
      :ok = FerricStore.set("r3m4_multi_a", "a")
      :ok = FerricStore.set("r3m4_multi_b", "b")

      result = FerricStore.del(["r3m4_multi_a", "r3m4_multi_b", "r3m4_multi_c"])
      assert {:ok, 2} = result

      # All deleted
      assert {:ok, nil} = FerricStore.get("r3m4_multi_a")
      assert {:ok, nil} = FerricStore.get("r3m4_multi_b")
    end

    test "del on a set key returns {:ok, count}" do
      {:ok, 2} = FerricStore.sadd("r3m4_set", ["a", "b"])

      # del now returns {:ok, count}
      result = FerricStore.del("r3m4_set")
      assert {:ok, _count} = result
    end
  end

  # ===========================================================================
  # R3-M5: sismember returns bare boolean instead of {:ok, boolean()}
  # ===========================================================================

  describe "R3-M5: sismember return type" do
    test "sismember returns {:ok, true} for existing member" do
      {:ok, 1} = FerricStore.sadd("r3m5_set", ["member1"])

      assert {:ok, true} = FerricStore.sismember("r3m5_set", "member1")
    end

    test "sismember returns {:ok, false} for missing member" do
      {:ok, 1} = FerricStore.sadd("r3m5_set2", ["member1"])

      assert {:ok, false} = FerricStore.sismember("r3m5_set2", "nonexistent")
    end

    test "sismember returns {:ok, false} for non-existent key" do
      assert {:ok, false} = FerricStore.sismember("r3m5_nonexistent", "anything")
    end

    test "sismember return type consistent with other set operations" do
      {:ok, 2} = FerricStore.sadd("r3m5_set3", ["a", "b"])
      {:ok, _members} = FerricStore.smembers("r3m5_set3")
      {:ok, _count} = FerricStore.scard("r3m5_set3")
      {:ok, _is_member} = FerricStore.sismember("r3m5_set3", "a")
    end
  end
end
