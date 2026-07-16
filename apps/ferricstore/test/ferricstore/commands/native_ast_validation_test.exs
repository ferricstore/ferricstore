defmodule Ferricstore.Commands.NativeAstValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Native

  @integer_error {:error, "ERR value is not an integer or out of range"}

  test "prepared AST numeric invariants are enforced before invoking storage" do
    parent = self()

    store = %{
      cas: fn key, expected, value, ttl ->
        send(parent, {:store_call, :cas, key, expected, value, ttl})
      end,
      lock: fn key, owner, ttl -> send(parent, {:store_call, :lock, key, owner, ttl}) end,
      extend: fn key, owner, ttl -> send(parent, {:store_call, :extend, key, owner, ttl}) end,
      ratelimit_add: fn key, window, max, count ->
        send(parent, {:store_call, :ratelimit_add, key, window, max, count})
      end
    }

    invalid_asts = [
      {:cas, "key", "old", "new", 0},
      {:cas, "key", "old", "new", "1000"},
      {:lock, "key", "owner", 0},
      {:lock, "key", "owner", "1000"},
      {:extend, "key", "owner", -1},
      {:extend, "key", "owner", 1.5},
      {:ratelimit_add, "key", 0, 10, 1},
      {:ratelimit_add, "key", 1_000, 0, 1},
      {:ratelimit_add, "key", 1_000, 10, 0},
      {:ratelimit_add, "key", 1_000, 10, "1"}
    ]

    Enum.each(invalid_asts, fn ast ->
      assert @integer_error == Native.handle_ast(ast, store)
    end)

    refute_received {:store_call, _, _, _, _, _}
    refute_received {:store_call, _, _, _, _}
  end

  test "prepared fetch-or-compute AST rejects invalid TTLs before resolving context" do
    assert @integer_error ==
             Native.handle_ast({:fetch_or_compute, "key", 0, "hint"}, %{})

    assert @integer_error ==
             Native.handle_ast({:fetch_or_compute, "key", "1000", "hint"}, %{})

    assert @integer_error ==
             Native.handle_ast({:fetch_or_compute_result, "key", "token", "value", -1}, %{})

    assert @integer_error ==
             Native.handle_ast(
               {:fetch_or_compute_result, "key", "token", "value", "1000"},
               %{}
             )
  end
end
