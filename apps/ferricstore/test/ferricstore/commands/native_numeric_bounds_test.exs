defmodule Ferricstore.Commands.NativeNumericBoundsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Native

  @max_int64 9_223_372_036_854_775_807
  @max_ttl_ms 9_223_090_561_878_065_152
  @max_window_ms div(@max_ttl_ms, 2)
  @integer_error {:error, "ERR value is not an integer or out of range"}

  test "CAS EX is case-insensitive" do
    store = %{
      cas: fn key, expected, value, ttl -> {:cas, key, expected, value, ttl} end
    }

    assert {:cas, "key", "old", "new", 2_000} ==
             Native.handle("CAS", ["key", "old", "new", "ex", "2"], store)
  end

  test "raw native durations and rate-limit integers are bounded before storage" do
    parent = self()
    store = recording_store(parent)

    too_many_seconds = Integer.to_string(div(@max_ttl_ms, 1_000) + 1)
    too_large_ttl = Integer.to_string(@max_ttl_ms + 1)
    too_large_window = Integer.to_string(@max_window_ms + 1)
    too_large_counter = Integer.to_string(@max_int64 + 1)

    assert @integer_error ==
             Native.handle("CAS", ["key", "old", "new", "EX", too_many_seconds], store)

    assert @integer_error == Native.handle("LOCK", ["key", "owner", too_large_ttl], store)
    assert @integer_error == Native.handle("EXTEND", ["key", "owner", too_large_ttl], store)

    assert @integer_error ==
             Native.handle("RATELIMIT.ADD", ["key", too_large_window, "10", "1"], store)

    assert @integer_error ==
             Native.handle("RATELIMIT.ADD", ["key", "1000", too_large_counter, "1"], store)

    assert @integer_error ==
             Native.handle("RATELIMIT.ADD", ["key", "1000", "10", too_large_counter], store)

    refute_received {:store_call, _operation, _args}
  end

  test "prepared native durations and rate-limit integers are bounded before storage" do
    parent = self()
    store = recording_store(parent)

    invalid_asts = [
      {:cas, "key", "old", "new", @max_ttl_ms + 1},
      {:lock, "key", "owner", @max_ttl_ms + 1},
      {:extend, "key", "owner", @max_ttl_ms + 1},
      {:ratelimit_add, "key", @max_window_ms + 1, 10, 1},
      {:ratelimit_add, "key", 1_000, @max_int64 + 1, 1},
      {:ratelimit_add, "key", 1_000, 10, @max_int64 + 1},
      {:fetch_or_compute, "key", @max_ttl_ms + 1, "hint"},
      {:fetch_or_compute_result, "key", "token", "value", @max_ttl_ms + 1}
    ]

    for ast <- invalid_asts do
      assert @integer_error == Native.handle_ast(ast, store)
    end

    refute_received {:store_call, _operation, _args}
  end

  defp recording_store(parent) do
    %{
      cas: fn key, expected, value, ttl ->
        send(parent, {:store_call, :cas, {key, expected, value, ttl}})
      end,
      lock: fn key, owner, ttl -> send(parent, {:store_call, :lock, {key, owner, ttl}}) end,
      extend: fn key, owner, ttl -> send(parent, {:store_call, :extend, {key, owner, ttl}}) end,
      ratelimit_add: fn key, window, max, count ->
        send(parent, {:store_call, :ratelimit_add, {key, window, max, count}})
      end
    }
  end
end
