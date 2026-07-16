defmodule Ferricstore.Commands.GenericScanPagingTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Generic

  test "SCAN delegates opaque cursors and filters to the store pager" do
    test_pid = self()

    store = %{
      keys: fn -> flunk("paged SCAN must not enumerate the whole keyspace") end,
      scan_keys_page: fn cursor, count, match_pattern, type_filter ->
        send(test_pid, {:scan_page, cursor, count, match_pattern, type_filter})

        case cursor do
          "0" -> {:ok, {"opaque-next", ["user:1"]}}
          "opaque-next" -> {:ok, {"0", ["user:2"]}}
        end
      end
    }

    assert ["opaque-next", ["user:1"]] =
             Generic.handle(
               "SCAN",
               ["0", "MATCH", "user:*", "COUNT", "7", "TYPE", "hash"],
               store
             )

    assert_receive {:scan_page, "0", 7, "user:*", "hash"}

    assert ["0", ["user:2"]] =
             Generic.handle("SCAN", ["opaque-next", "COUNT", "7"], store)

    assert_receive {:scan_page, "opaque-next", 7, nil, nil}
  end

  test "SCAN rejects COUNT values above the bounded work budget" do
    store = %{
      keys: fn -> flunk("invalid COUNT must fail before touching the store") end
    }

    assert {:error, "ERR value is not an integer or out of range"} =
             Generic.handle("SCAN", ["0", "COUNT", "10001"], store)
  end

  test "RANDOMKEY uses the store sampler without enumerating all keys" do
    store = %{
      keys: fn -> flunk("RANDOMKEY must not enumerate the whole keyspace") end,
      random_key: fn -> {:ok, "sampled"} end
    }

    assert "sampled" = Generic.handle("RANDOMKEY", [], store)
  end
end
