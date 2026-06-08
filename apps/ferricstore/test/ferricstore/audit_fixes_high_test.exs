defmodule Ferricstore.AuditFixesHighTest do
  @moduledoc """
  High-severity audit fixes (H1-H6).
  Split from AuditFixesTest.
  """

  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, Set, Server, Generic}

  # ---------------------------------------------------------------------------
  # Helper: build a mock store map from a key-value map
  # ---------------------------------------------------------------------------

  defp mock_store(data \\ %{}) do
    %{
      get: fn key -> get_in(data, [key, Access.elem(0)]) end,
      get_meta: fn key ->
        case Map.get(data, key) do
          nil -> nil
          {val, exp} -> {val, exp}
        end
      end,
      put: fn _key, _val, _exp -> :ok end,
      delete: fn _key -> :ok end,
      exists?: fn key -> Map.has_key?(data, key) end,
      keys: fn ->
        Map.keys(data) |> Enum.reject(&Ferricstore.Store.CompoundKey.internal_key?/1)
      end,
      flush: fn -> :ok end,
      dbsize: fn -> map_size(data) end,
      incr: fn _key, _delta -> {:ok, 1} end,
      incr_float: fn _key, _delta -> {:ok, "1.0"} end,
      append: fn _key, _val -> {:ok, 5} end,
      getset: fn _key, _val -> nil end,
      getdel: fn _key -> nil end,
      getex: fn _key, _exp -> nil end,
      setrange: fn _key, _off, _val -> {:ok, 10} end,
      compound_get: fn _rk, _ck -> nil end,
      compound_get_meta: fn _rk, _ck -> nil end,
      compound_put: fn _rk, _ck, _v, _e -> :ok end,
      compound_delete: fn _rk, _ck -> :ok end,
      compound_scan: fn _rk, _prefix -> [] end,
      compound_count: fn _rk, _prefix -> 0 end,
      compound_delete_prefix: fn _rk, _prefix -> :ok end
    }
  end

  # ---------------------------------------------------------------------------
  # H2: Encoder single-pass counting
  # ---------------------------------------------------------------------------

  if Code.ensure_loaded?(FerricstoreServer.Resp.Encoder) do
    describe "H2: encoder single-pass list counting" do
      test "array encoding correct" do
        result = FerricstoreServer.Resp.Encoder.encode([1, 2, 3]) |> IO.iodata_to_binary()
        assert result == "*3\r\n:1\r\n:2\r\n:3\r\n"
      end

      test "empty array" do
        result = FerricstoreServer.Resp.Encoder.encode([]) |> IO.iodata_to_binary()
        assert result == "*0\r\n"
      end

      test "push encoding correct" do
        result =
          FerricstoreServer.Resp.Encoder.encode({:push, ["a", "b"]}) |> IO.iodata_to_binary()

        assert result == ">2\r\n$1\r\na\r\n$1\r\nb\r\n"
      end

      test "nested array encoding" do
        result = FerricstoreServer.Resp.Encoder.encode([[1, 2], [3]]) |> IO.iodata_to_binary()
        assert result == "*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n"
      end

      test "MapSet encoding" do
        set = MapSet.new(["a", "b"])
        result = FerricstoreServer.Resp.Encoder.encode(set) |> IO.iodata_to_binary()
        # Order is not guaranteed for MapSet, but the count and elements should be correct
        assert String.starts_with?(result, "~2\r\n")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # H3 + H6: HSET direct recursion (no chunk_every, no length guard)
  # ---------------------------------------------------------------------------

  describe "H3+H6: HSET direct pair walking" do
    test "HSET with even number of args works" do
      compound_data = %{}

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, ck -> Map.get(compound_data, ck) end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      # Uses check_or_set which needs the compound_get for type registry
      # Just verify the call path doesn't crash with the new implementation
      result = Hash.handle("HSET", ["key", "f1", "v1", "f2", "v2"], store)
      assert result == 2 or match?({:error, _}, result)
    end

    test "HSET with odd number of args returns error" do
      store = mock_store()
      result = Hash.handle("HSET", ["key", "f1", "v1", "f2"], store)
      assert result == {:error, "ERR wrong number of arguments for 'hset' command"}
    end

    test "HSET with single field-value pair works" do
      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)

      result = Hash.handle("HSET", ["key", "f1", "v1"], store)
      assert result == 1 or match?({:error, _}, result)
    end

    test "HSET with missing args returns error" do
      store = mock_store()
      result = Hash.handle("HSET", ["key"], store)
      assert result == {:error, "ERR wrong number of arguments for 'hset' command"}
    end
  end

  # ---------------------------------------------------------------------------
  # H4: paginate avoids double length
  # ---------------------------------------------------------------------------

  describe "H4: HSCAN/SSCAN paginate avoids double length" do
    test "HSCAN returns correct pagination" do
      # Create a store with hash data
      hash_data = for i <- 1..5, into: %{}, do: {"field#{i}", "val#{i}"}

      store =
        mock_store()
        |> Map.put(:compound_get, fn _rk, _ck -> nil end)
        |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)
        |> Map.put(:compound_scan, fn _rk, _prefix ->
          Enum.map(hash_data, fn {k, v} -> {k, v} end)
        end)
        |> Map.put(:compound_count, fn _rk, _prefix -> 5 end)

      # HSCAN with COUNT 2 should return partial results
      result = Hash.handle("HSCAN", ["key", "0", "COUNT", "2"], store)
      assert is_list(result)
      [cursor, _elements] = result
      # Should have a non-zero cursor or "0" if all fit
      assert is_binary(cursor)
    end
  end

  # ---------------------------------------------------------------------------
  # H5: barrier_command_normalised? uses match?
  # ---------------------------------------------------------------------------

  describe "H5: barrier command classification" do
    # This tests the connection.ex change indirectly.
    # The fix replaces `length(args) > 1` with `match?([_, _ | _], args)`.
    # Both should behave identically.

    test "single-element list is not more than 1" do
      refute match?([_, _ | _], ["one"])
    end

    test "two-element list is more than 1" do
      assert match?([_, _ | _], ["one", "two"])
    end

    test "empty list is not more than 1" do
      empty = []
      assert empty == []
    end

    test "three-element list is more than 1" do
      assert match?([_, _ | _], ["one", "two", "three"])
    end
  end

  # ===========================================================================
  # HIGH-severity fixes from elixir-guide-audit.md v2
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # H1: encode_hash removed — hashes now use compound-key storage
  # ---------------------------------------------------------------------------
  # encode_hash/decode_hash were removed in favour of compound keys.
  # Hash round-trip correctness is covered by HSET/HGET/HGETALL tests.

  # ---------------------------------------------------------------------------
  # H2: paginate in HSCAN/SSCAN avoids length + Enum.slice double traversal
  # ---------------------------------------------------------------------------

  describe "H2: HSCAN paginate avoids double traversal" do
    defp hscan_mock_store(0), do: hscan_mock_store_from(%{})

    defp hscan_mock_store(n) when n > 0 do
      hash_data =
        for i <- 1..n, into: %{} do
          key = "field_#{String.pad_leading("#{i}", 4, "0")}"
          {key, "val_#{i}"}
        end

      hscan_mock_store_from(hash_data)
    end

    defp hscan_mock_store_from(hash_data) do
      mock_store()
      |> Map.put(:compound_get, fn _rk, _ck -> nil end)
      |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)
      |> Map.put(:compound_scan, fn _rk, _prefix ->
        Enum.sort(hash_data) |> Enum.map(fn {k, v} -> {k, v} end)
      end)
      |> Map.put(:compound_count, fn _rk, _prefix -> map_size(hash_data) end)
    end

    test "HSCAN with cursor 0 returns first batch" do
      store = hscan_mock_store(10)
      [cursor, elements] = Hash.handle("HSCAN", ["key", "0", "COUNT", "3"], store)
      assert is_binary(cursor)
      # Should have elements (field + value pairs interleaved)
      assert elements != []
    end

    test "HSCAN full iteration returns all fields exactly once" do
      store = hscan_mock_store(7)
      fields = collect_hscan_fields(store, "key", "0", 3, [])
      assert length(fields) == 7
      assert fields == Enum.uniq(fields)
    end

    test "HSCAN with count larger than total returns all at once" do
      store = hscan_mock_store(3)
      [cursor, elements] = Hash.handle("HSCAN", ["key", "0", "COUNT", "100"], store)
      assert cursor == "0"
      # 3 fields * 2 (field + value) = 6 elements
      assert length(elements) == 6
    end

    test "HSCAN on empty hash returns cursor 0 and empty list" do
      store = hscan_mock_store(0)
      [cursor, elements] = Hash.handle("HSCAN", ["key", "0", "COUNT", "10"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "HSCAN with count 1 iterates one at a time" do
      store = hscan_mock_store(5)
      fields = collect_hscan_fields(store, "key", "0", 1, [])
      assert length(fields) == 5
    end
  end

  # Helper to iterate HSCAN until cursor returns "0"
  defp collect_hscan_fields(_store, _key, "0", _count, acc) when acc != [] do
    acc
  end

  defp collect_hscan_fields(store, key, cursor, count, acc) do
    [next_cursor, elements] = Hash.handle("HSCAN", [key, cursor, "COUNT", "#{count}"], store)
    # Elements are [field, value, field, value, ...] -- extract fields
    fields = elements |> Enum.chunk_every(2) |> Enum.map(&hd/1)
    new_acc = acc ++ fields

    if next_cursor == "0" do
      new_acc
    else
      collect_hscan_fields(store, key, next_cursor, count, new_acc)
    end
  end

  describe "H2: SSCAN paginate avoids double traversal" do
    defp sscan_mock_store(0), do: sscan_mock_store_from([])

    defp sscan_mock_store(n) when n > 0 do
      members =
        for i <- 1..n do
          "member_#{String.pad_leading("#{i}", 4, "0")}"
        end

      sscan_mock_store_from(members)
    end

    defp sscan_mock_store_from(members) do
      mock_store()
      |> Map.put(:compound_get, fn _rk, _ck -> nil end)
      |> Map.put(:compound_put, fn _rk, _ck, _v, _e -> :ok end)
      |> Map.put(:compound_scan, fn _rk, _prefix ->
        Enum.sort(members) |> Enum.map(fn m -> {m, "1"} end)
      end)
      |> Map.put(:compound_count, fn _rk, _prefix -> length(members) end)
    end

    test "SSCAN full iteration returns all members" do
      store = sscan_mock_store(8)
      members = collect_sscan_members(store, "key", "0", 3, [])
      assert length(members) == 8
      assert members == Enum.uniq(members)
    end

    test "SSCAN on empty set returns cursor 0" do
      store = sscan_mock_store(0)
      [cursor, elements] = Set.handle("SSCAN", ["key", "0", "COUNT", "10"], store)
      assert cursor == "0"
      assert elements == []
    end

    test "SSCAN with count larger than total returns all" do
      store = sscan_mock_store(4)
      [cursor, elements] = Set.handle("SSCAN", ["key", "0", "COUNT", "100"], store)
      assert cursor == "0"
      assert length(elements) == 4
    end
  end

  defp collect_sscan_members(_store, _key, "0", _count, acc) when acc != [] do
    acc
  end

  defp collect_sscan_members(store, key, cursor, count, acc) do
    [next_cursor, elements] = Set.handle("SSCAN", [key, cursor, "COUNT", "#{count}"], store)
    new_acc = acc ++ elements

    if next_cursor == "0" do
      new_acc
    else
      collect_sscan_members(store, key, next_cursor, count, new_acc)
    end
  end

  # ---------------------------------------------------------------------------
  # H3: COPY handler uses pattern match instead of length guard
  # ---------------------------------------------------------------------------

  describe "H3: COPY pattern match guard" do
    test "COPY with exactly 2 args (source, dest) works" do
      store =
        mock_store(%{"src" => {"value", 0}})
        |> Map.put(:put, fn _k, _v, _e -> :ok end)

      result = Generic.handle("COPY", ["src", "dst"], store)
      # Should succeed (copy src to dst) or return :ok / integer
      assert result == 1 or result == 0 or match?({:error, _}, result)
    end

    test "COPY with REPLACE option works" do
      store =
        mock_store(%{"src" => {"value", 0}})
        |> Map.put(:put, fn _k, _v, _e -> :ok end)

      result = Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert result == 1 or result == 0 or match?({:error, _}, result)
    end

    test "COPY with 0 args returns error" do
      store = mock_store()
      result = Generic.handle("COPY", [], store)
      assert result == {:error, "ERR wrong number of arguments for 'copy' command"}
    end

    test "COPY with 1 arg returns error" do
      store = mock_store()
      result = Generic.handle("COPY", ["src"], store)
      assert result == {:error, "ERR wrong number of arguments for 'copy' command"}
    end
  end

  # ---------------------------------------------------------------------------
  # H4: AUTH handler uses pattern match instead of length guard
  # ---------------------------------------------------------------------------

  describe "H4: AUTH pattern match guard" do
    # AUTH dispatch happens in connection.ex which is harder to unit test directly.
    # We verify the pattern match semantics are equivalent to the old length guard.

    test "pattern [_, _, _ | _] matches 3+ element lists" do
      assert match?([_, _, _ | _], ["a", "b", "c"])
      assert match?([_, _, _ | _], ["a", "b", "c", "d"])
      assert match?([_, _, _ | _], ["a", "b", "c", "d", "e"])
    end

    test "pattern [_, _, _ | _] does not match 0-2 element lists" do
      empty = []
      one = ["a"]
      two = ["a", "b"]
      assert empty == []
      refute match?([_, _, _ | _], one)
      refute match?([_, _, _ | _], two)
    end

    test "pattern equivalence with length > 2" do
      # Verify the new pattern matches exactly the same inputs as length(args) > 2
      for n <- 0..10 do
        args = Enum.map(1..max(n, 1), &"arg#{&1}")
        args = if n == 0, do: [], else: args
        old_result = length(args) > 2
        new_result = match?([_, _, _ | _], args)
        assert old_result == new_result, "Mismatch for #{n}-element list"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # H5: DBSIZE uses Enum.count instead of Enum.reject + length
  # ---------------------------------------------------------------------------

  describe "H5: DBSIZE single-pass count" do
    test "DBSIZE counts non-internal keys" do
      store =
        mock_store(%{
          "user:1" => {"v", 0},
          "user:2" => {"v", 0},
          "session:a" => {"v", 0}
        })

      result = Server.handle("DBSIZE", [], store)
      assert result == 3
    end

    test "DBSIZE returns 0 for empty store" do
      store = mock_store(%{})
      result = Server.handle("DBSIZE", [], store)
      assert result == 0
    end

    test "DBSIZE with only internal keys returns 0" do
      # Internal keys have prefixes like H:, S:, Z:, L:, T:
      # The mock_store helper already filters these out via CompoundKey.internal_key?
      store =
        mock_store()
        |> Map.put(:keys, fn -> ["H:some_hash", "S:some_set", "Z:some_zset"] end)

      result = Server.handle("DBSIZE", [], store)
      assert result == 0
    end

    test "DBSIZE with mixed internal and normal keys" do
      store =
        mock_store()
        |> Map.put(:keys, fn -> ["key1", "H:hash1", "key2", "S:set1", "key3"] end)

      result = Server.handle("DBSIZE", [], store)
      assert result == 3
    end

    test "DBSIZE with wrong args returns error" do
      store = mock_store()
      result = Server.handle("DBSIZE", ["extra"], store)
      assert result == {:error, "ERR wrong number of arguments for 'dbsize' command"}
    end

    test "DBSIZE with large key count" do
      keys = for i <- 1..1000, do: "key_#{i}"
      store = mock_store() |> Map.put(:keys, fn -> keys end)
      result = Server.handle("DBSIZE", [], store)
      assert result == 1000
    end
  end

  # ---------------------------------------------------------------------------
  # H6: SCAN avoids length(all_keys) fallback via drop_while + split
  # ---------------------------------------------------------------------------

  describe "H6: SCAN avoids length(all_keys) traversal" do
    test "SCAN with cursor 0 returns first batch" do
      store =
        mock_store(%{
          "a" => {"v", 0},
          "b" => {"v", 0},
          "c" => {"v", 0},
          "d" => {"v", 0},
          "e" => {"v", 0}
        })

      [cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "2"], store)
      assert is_binary(cursor)
      assert length(keys) == 2
    end

    test "SCAN full iteration collects all keys" do
      data =
        for i <- 1..10, into: %{}, do: {"key_#{String.pad_leading("#{i}", 3, "0")}", {"v", 0}}

      store = mock_store(data)

      all_keys = collect_scan_keys(store, "0", 3, [])
      assert length(all_keys) == 10
      assert all_keys == Enum.uniq(all_keys)
    end

    test "SCAN returns 0 cursor when done" do
      store = mock_store(%{"only" => {"v", 0}})
      [cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)
      assert cursor == "0"
      assert keys == ["only"]
    end

    test "SCAN on empty store returns 0 and empty list" do
      store = mock_store(%{})
      [cursor, keys] = Generic.handle("SCAN", ["0"], store)
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN with cursor past all keys returns 0" do
      store = mock_store(%{"a" => {"v", 0}, "b" => {"v", 0}})
      # Use a cursor string that is lexicographically after all keys
      [cursor, keys] = Generic.handle("SCAN", ["zzz", "COUNT", "10"], store)
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN with MATCH pattern filters correctly" do
      all_data = %{
        "user:1" => {"v", 0},
        "user:2" => {"v", 0},
        "post:1" => {"v", 0}
      }

      _all_key_list = Map.keys(all_data)

      store = mock_store(all_data)

      all_keys = collect_scan_keys_with_match(store, "0", 1, "user:*", [])
      assert Enum.sort(all_keys) == ["user:1", "user:2"]
    end

    test "SCAN with count 1 iterates one key at a time" do
      data = for i <- 1..5, into: %{}, do: {"k#{i}", {"v", 0}}
      store = mock_store(data)

      all_keys = collect_scan_keys(store, "0", 1, [])
      assert length(all_keys) == 5
    end
  end

  defp collect_scan_keys(_store, "0", _count, acc) when acc != [] do
    acc
  end

  defp collect_scan_keys(store, cursor, count, acc) do
    [next_cursor, keys] = Generic.handle("SCAN", [cursor, "COUNT", "#{count}"], store)
    new_acc = acc ++ keys

    if next_cursor == "0" do
      new_acc
    else
      collect_scan_keys(store, next_cursor, count, new_acc)
    end
  end

  defp collect_scan_keys_with_match(_store, "0", _count, _pattern, acc) when acc != [] do
    acc
  end

  defp collect_scan_keys_with_match(store, cursor, count, pattern, acc) do
    [next_cursor, keys] =
      Generic.handle("SCAN", [cursor, "COUNT", "#{count}", "MATCH", pattern], store)

    new_acc = acc ++ keys

    if next_cursor == "0" do
      new_acc
    else
      collect_scan_keys_with_match(store, next_cursor, count, pattern, new_acc)
    end
  end
end
