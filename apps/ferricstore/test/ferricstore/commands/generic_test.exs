defmodule Ferricstore.Commands.GenericTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Generic, Hash, Stream}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.LFU
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # TYPE
  # ---------------------------------------------------------------------------

  describe "TYPE" do
    test "TYPE returns 'string' for existing key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:simple, "string"} == Generic.handle("TYPE", ["k"], store)
    end

    test "TYPE returns 'none' for missing key" do
      store = MockStore.make()
      assert {:simple, "none"} == Generic.handle("TYPE", ["missing"], store)
    end

    test "TYPE returns 'none' for expired key" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert {:simple, "none"} == Generic.handle("TYPE", ["k"], store)
    end

    test "TYPE uses existence check without loading string value" do
      store = %{
        compound_get: fn _redis_key, _compound_key -> nil end,
        exists?: fn "cold_string" -> true end,
        get: fn _key -> flunk("TYPE should not read the value to classify a string key") end
      }

      assert {:simple, "string"} == Generic.handle("TYPE", ["cold_string"], store)
    end

    test "TYPE with no args returns error" do
      assert {:error, _} = Generic.handle("TYPE", [], MockStore.make())
    end

    test "TYPE with too many args returns error" do
      assert {:error, _} = Generic.handle("TYPE", ["a", "b"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # UNLINK
  # ---------------------------------------------------------------------------

  describe "UNLINK" do
    test "UNLINK existing key returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Generic.handle("UNLINK", ["k"], store)
    end

    test "UNLINK missing key returns 0" do
      store = MockStore.make()
      assert 0 == Generic.handle("UNLINK", ["missing"], store)
    end

    test "UNLINK multiple keys returns count of deleted" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert 2 == Generic.handle("UNLINK", ["a", "b", "c"], store)
    end

    test "UNLINK actually removes the key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      Generic.handle("UNLINK", ["k"], store)
      assert nil == store.get.("k")
    end

    test "UNLINK with no args returns error" do
      assert {:error, _} = Generic.handle("UNLINK", [], MockStore.make())
    end

    test "UNLINK returns 0 for expired key" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Generic.handle("UNLINK", ["k"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # RENAME
  # ---------------------------------------------------------------------------

  describe "RENAME" do
    test "RENAME renames key" do
      store = MockStore.make(%{"old" => {"v", 0}})
      assert :ok = Generic.handle("RENAME", ["old", "new"], store)
      assert nil == store.get.("old")
      assert "v" == store.get.("new")
    end

    test "RENAME returns destination write errors and keeps source" do
      source = "{rename}:old"
      destination = "{rename}:new"
      base = MockStore.make(%{source => {"v", 0}})

      store =
        Map.put(base, :put, fn
          ^destination, "v", 0 -> {:error, :disk_full}
          key, value, expire_at_ms -> base.put.(key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} == Generic.handle("RENAME", [source, destination], store)
      assert "v" == store.get.(source)
      assert nil == store.get.(destination)
    end

    test "RENAME preserves TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"old" => {"v", future}})
      assert :ok = Generic.handle("RENAME", ["old", "new"], store)
      {_value, expire_at_ms} = store.get_meta.("new")
      assert expire_at_ms == future
    end

    test "RENAME errors when source doesn't exist" do
      store = MockStore.make()
      assert {:error, "ERR no such key"} = Generic.handle("RENAME", ["missing", "new"], store)
    end

    test "RENAME treats a fully expired compound source as missing before TYPE cleanup" do
      store = stale_hash_store("hash")

      assert {:error, "ERR no such key"} = Generic.handle("RENAME", ["hash", "dst"], store)
      assert nil == store.compound_get.("dst", CompoundKey.type_key("dst"))
    end

    test "RENAME overwrites existing destination" do
      store = MockStore.make(%{"src" => {"new_val", 0}, "dst" => {"old_val", 0}})
      assert :ok = Generic.handle("RENAME", ["src", "dst"], store)
      assert nil == store.get.("src")
      assert "new_val" == store.get.("dst")
    end

    test "RENAME preserves plain destination when plain write fails" do
      base = MockStore.make(%{"src" => {"new_val", 0}, "dst" => {"old_val", 0}})

      store =
        base
        |> Map.put(:delete, fn
          "dst" ->
            flunk("RENAME should not delete a plain destination before a plain overwrite")

          key ->
            base.delete.(key)
        end)
        |> Map.put(:put, fn
          "dst", "new_val", 0 -> {:error, :disk_full}
          key, value, expire_at_ms -> base.put.(key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} == Generic.handle("RENAME", ["src", "dst"], store)
      assert "new_val" == base.get.("src")
      assert "old_val" == base.get.("dst")
    end

    test "RENAME same key to itself is a no-op" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert :ok = Generic.handle("RENAME", ["k", "k"], store)
      assert "v" == store.get.("k")
    end

    test "RENAME same cold key to itself does not load source value" do
      store = metadata_only_key_store(%{"cold" => 0})

      assert :ok = Generic.handle("RENAME", ["cold", "cold"], store)
    end

    test "RENAME preserves stream metadata for ID monotonicity" do
      Stream.ensure_meta_table()
      store = MockStore.make()
      source = "stream_src_#{System.unique_integer([:positive])}"
      destination = "stream_dst_#{System.unique_integer([:positive])}"

      assert "5-0" == Stream.handle("XADD", [source, "5-0", "f", "v"], store)
      assert :ok = Generic.handle("RENAME", [source, destination], store)

      assert {:error, msg} = Stream.handle("XADD", [destination, "4-0", "f", "stale"], store)
      assert msg =~ "equal or smaller"

      assert "6-0" == Stream.handle("XADD", [destination, "6-0", "f", "new"], store)

      assert [["5-0", "f", "v"], ["6-0", "f", "new"]] ==
               Stream.handle("XRANGE", [destination, "-", "+"], store)
    end

    test "RENAME preserves stream consumer groups" do
      Stream.ensure_meta_table()
      store = MockStore.make()
      source = "stream_src_groups_#{System.unique_integer([:positive])}"
      destination = "stream_dst_groups_#{System.unique_integer([:positive])}"

      assert "1-0" == Stream.handle("XADD", [source, "1-0", "f", "v"], store)
      assert :ok == Stream.handle("XGROUP", ["CREATE", source, "workers", "0"], store)

      assert :ok = Generic.handle("RENAME", [source, destination], store)

      info = Stream.handle("XINFO", ["STREAM", destination], store)
      assert info["groups"] == 1

      assert [[^destination, [["1-0", "f", "v"]]]] =
               Stream.handle(
                 "XREADGROUP",
                 ["GROUP", "workers", "c1", "STREAMS", destination, ">"],
                 store
               )
    end

    test "RENAME with no args returns error" do
      assert {:error, _} = Generic.handle("RENAME", [], MockStore.make())
    end

    test "RENAME with one arg returns error" do
      assert {:error, _} = Generic.handle("RENAME", ["k"], MockStore.make())
    end

    test "RENAME with too many args returns error" do
      assert {:error, _} = Generic.handle("RENAME", ["a", "b", "c"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # RENAMENX
  # ---------------------------------------------------------------------------

  describe "RENAMENX" do
    test "RENAMENX returns 1 when destination doesn't exist" do
      store = MockStore.make(%{"old" => {"v", 0}})
      assert 1 == Generic.handle("RENAMENX", ["old", "new"], store)
      assert nil == store.get.("old")
      assert "v" == store.get.("new")
    end

    test "RENAMENX returns 0 when destination exists" do
      store = MockStore.make(%{"old" => {"v1", 0}, "new" => {"v2", 0}})
      assert 0 == Generic.handle("RENAMENX", ["old", "new"], store)
      # Source should still exist, destination unchanged
      assert "v1" == store.get.("old")
      assert "v2" == store.get.("new")
    end

    test "RENAMENX existing destination does not load cold source value" do
      store = metadata_only_key_store(%{"cold_src" => 0, "dst" => 0})

      assert 0 == Generic.handle("RENAMENX", ["cold_src", "dst"], store)
    end

    test "RENAMENX treats a fully expired compound destination as missing before TYPE cleanup" do
      store = stale_hash_store("dst", %{"src" => {"v", 0}})

      assert 1 == Generic.handle("RENAMENX", ["src", "dst"], store)
      assert nil == store.get.("src")
      assert "v" == store.get.("dst")
    end

    test "RENAMENX preserves TTL when renamed" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"old" => {"v", future}})
      assert 1 == Generic.handle("RENAMENX", ["old", "new"], store)
      {_value, expire_at_ms} = store.get_meta.("new")
      assert expire_at_ms == future
    end

    test "RENAMENX errors when source doesn't exist" do
      store = MockStore.make()
      assert {:error, "ERR no such key"} = Generic.handle("RENAMENX", ["missing", "new"], store)
    end

    test "RENAMENX same key returns 0" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 0 == Generic.handle("RENAMENX", ["k", "k"], store)
      assert "v" == store.get.("k")
    end

    test "RENAMENX with no args returns error" do
      assert {:error, _} = Generic.handle("RENAMENX", [], MockStore.make())
    end

    test "RENAMENX with one arg returns error" do
      assert {:error, _} = Generic.handle("RENAMENX", ["k"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # COPY
  # ---------------------------------------------------------------------------

  describe "COPY" do
    test "COPY copies value to new key" do
      store = MockStore.make(%{"src" => {"v", 0}})
      assert 1 == Generic.handle("COPY", ["src", "dst"], store)
      assert "v" == store.get.("src")
      assert "v" == store.get.("dst")
    end

    test "COPY returns destination write errors" do
      base = MockStore.make(%{"src" => {"v", 0}})

      store =
        Map.put(base, :put, fn
          "dst", "v", 0 -> {:error, :disk_full}
          key, value, expire_at_ms -> base.put.(key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} == Generic.handle("COPY", ["src", "dst"], store)
      assert "v" == store.get.("src")
      assert nil == store.get.("dst")
    end

    test "COPY batches compound writes" do
      base = MockStore.make()
      assert 2 == Hash.handle("HSET", ["hash", "a", "1", "b", "2"], base)

      type_key = CompoundKey.type_key("copy")
      field_a = CompoundKey.hash_field("copy", "a")
      field_b = CompoundKey.hash_field("copy", "b")
      parent = self()

      store =
        base
        |> Map.put(:compound_put, fn _redis_key, compound_key, _value, _expire_at_ms ->
          flunk("COPY should batch compound writes, got #{inspect(compound_key)}")
        end)
        |> Map.put(:compound_batch_put, fn "copy", entries ->
          send(parent, {:compound_batch_put, entries})
          :ok
        end)

      assert 1 == Generic.handle("COPY", ["hash", "copy"], store)

      assert_receive {:compound_batch_put, entries}

      entries_by_key =
        Map.new(entries, fn {compound_key, value, exp} -> {compound_key, {value, exp}} end)

      assert %{
               ^type_key => {"hash", 0},
               ^field_a => {"1", 0},
               ^field_b => {"2", 0}
             } = entries_by_key

      assert map_size(entries_by_key) == 3
      refute_receive {:compound_batch_put, _}
    end

    test "COPY preserves stream consumer groups" do
      Stream.ensure_meta_table()
      store = MockStore.make()
      source = "stream_copy_src_groups_#{System.unique_integer([:positive])}"
      destination = "stream_copy_dst_groups_#{System.unique_integer([:positive])}"

      assert "1-0" == Stream.handle("XADD", [source, "1-0", "f", "v"], store)
      assert :ok == Stream.handle("XGROUP", ["CREATE", source, "workers", "0"], store)

      assert 1 = Generic.handle("COPY", [source, destination], store)

      info = Stream.handle("XINFO", ["STREAM", destination], store)
      assert info["groups"] == 1

      assert [[^destination, [["1-0", "f", "v"]]]] =
               Stream.handle(
                 "XREADGROUP",
                 ["GROUP", "workers", "c1", "STREAMS", destination, ">"],
                 store
               )
    end

    test "COPY preserves TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"src" => {"v", future}})
      assert 1 == Generic.handle("COPY", ["src", "dst"], store)
      {_val, src_exp} = store.get_meta.("src")
      {_val, dst_exp} = store.get_meta.("dst")
      assert src_exp == dst_exp
    end

    test "COPY without REPLACE returns 0 if destination exists" do
      store = MockStore.make(%{"src" => {"v1", 0}, "dst" => {"v2", 0}})
      assert 0 == Generic.handle("COPY", ["src", "dst"], store)
      assert "v2" == store.get.("dst")
    end

    test "COPY without REPLACE existing destination does not load cold source value" do
      store = metadata_only_key_store(%{"cold_src" => 0, "dst" => 0})

      assert 0 == Generic.handle("COPY", ["cold_src", "dst"], store)
    end

    test "COPY destination existence check does not load destination value" do
      store = %{
        get: fn _key -> flunk("COPY should not load values through get") end,
        get_meta: fn
          "src" -> {"value", 0}
          "dst" -> flunk("COPY should not read destination value to check existence")
        end,
        compound_get: fn _redis_key, _compound_key -> nil end,
        exists?: fn
          "dst" -> true
          _key -> false
        end
      }

      assert 0 == Generic.handle("COPY", ["src", "dst"], store)
    end

    test "COPY with REPLACE overwrites destination" do
      store = MockStore.make(%{"src" => {"v1", 0}, "dst" => {"v2", 0}})
      assert 1 == Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert "v1" == store.get.("dst")
    end

    test "COPY with REPLACE preserves plain destination when plain write fails" do
      base = MockStore.make(%{"src" => {"v1", 0}, "dst" => {"v2", 0}})

      store =
        base
        |> Map.put(:delete, fn "dst" ->
          flunk("COPY REPLACE should not delete a plain destination before a plain overwrite")
        end)
        |> Map.put(:put, fn
          "dst", "v1", 0 -> {:error, :disk_full}
          key, value, expire_at_ms -> base.put.(key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} == Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert "v1" == base.get.("src")
      assert "v2" == base.get.("dst")
    end

    test "COPY with REPLACE preserves compound destination when compound write fails" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["src", "field", "new"], base)
      assert 1 == Hash.handle("HSET", ["dst", "field", "old"], base)

      store =
        Map.put(base, :compound_batch_put, fn
          "dst", entries ->
            if Enum.any?(entries, fn {_compound_key, value, _expire_at_ms} -> value == "new" end) do
              {:error, :disk_full}
            else
              base.compound_batch_put.("dst", entries)
            end

          key, entries ->
            base.compound_batch_put.(key, entries)
        end)

      assert {:error, :disk_full} == Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      assert "new" == Hash.handle("HGET", ["src", "field"], base)
      assert "old" == Hash.handle("HGET", ["dst", "field"], base)
    end

    test "COPY with REPLACE overwrites plain destination without loading destination value" do
      {:ok, deleted} = Agent.start_link(fn -> false end)
      {:ok, put_value} = Agent.start_link(fn -> nil end)

      store = %{
        get: fn _key -> flunk("COPY should not load destination through get") end,
        get_meta: fn
          "src" -> {"value", 0}
          "dst" -> flunk("COPY REPLACE should not read destination value before delete")
        end,
        put: fn "dst", "value", 0 ->
          Agent.update(put_value, fn _ -> "value" end)
          :ok
        end,
        delete: fn "dst" ->
          Agent.update(deleted, fn _ -> true end)
          :ok
        end,
        exists?: fn
          "dst" -> true
          _key -> false
        end,
        compound_get: fn _redis_key, _compound_key -> nil end,
        compound_scan: fn _redis_key, _prefix -> [] end,
        prob_write: fn _command -> :ok end
      }

      assert 1 == Generic.handle("COPY", ["src", "dst", "REPLACE"], store)
      refute Agent.get(deleted, & &1)
      assert "value" == Agent.get(put_value, & &1)
    end

    test "COPY with lowercase replace option works" do
      store = MockStore.make(%{"src" => {"v1", 0}, "dst" => {"v2", 0}})
      assert 1 == Generic.handle("COPY", ["src", "dst", "replace"], store)
      assert "v1" == store.get.("dst")
    end

    test "COPY returns 0 when source doesn't exist" do
      store = MockStore.make()
      assert 0 == Generic.handle("COPY", ["missing", "dst"], store)
    end

    test "COPY treats a fully expired compound source as missing before TYPE cleanup" do
      store = stale_hash_store("hash")

      assert 0 == Generic.handle("COPY", ["hash", "dst"], store)
      assert nil == store.compound_get.("dst", CompoundKey.type_key("dst"))
    end

    test "COPY source to itself without REPLACE returns 0" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 0 == Generic.handle("COPY", ["k", "k"], store)
      assert "v" == store.get.("k")
    end

    test "COPY source to itself with REPLACE succeeds" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Generic.handle("COPY", ["k", "k", "REPLACE"], store)
      assert "v" == store.get.("k")
    end

    test "COPY with no args returns error" do
      assert {:error, _} = Generic.handle("COPY", [], MockStore.make())
    end

    test "COPY with one arg returns error" do
      assert {:error, _} = Generic.handle("COPY", ["src"], MockStore.make())
    end

    test "COPY with invalid option returns syntax error" do
      store = MockStore.make(%{"src" => {"v", 0}})
      assert {:error, _} = Generic.handle("COPY", ["src", "dst", "BOGUS"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # RANDOMKEY
  # ---------------------------------------------------------------------------

  describe "RANDOMKEY" do
    test "RANDOMKEY returns nil when DB is empty" do
      store = MockStore.make()
      assert nil == Generic.handle("RANDOMKEY", [], store)
    end

    test "RANDOMKEY returns a key when DB has keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}, "c" => {"3", 0}})
      key = Generic.handle("RANDOMKEY", [], store)
      assert key in ["a", "b", "c"]
    end

    test "RANDOMKEY returns the only key when DB has one key" do
      store = MockStore.make(%{"only" => {"v", 0}})
      assert "only" == Generic.handle("RANDOMKEY", [], store)
    end

    test "RANDOMKEY returns logical names instead of internal compound keys" do
      store = MockStore.make(%{CompoundKey.type_key("hash") => {"hash", 0}})
      assert "hash" == Generic.handle("RANDOMKEY", [], store)
    end

    test "RANDOMKEY with args returns error" do
      assert {:error, _} = Generic.handle("RANDOMKEY", ["extra"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # SCAN
  # ---------------------------------------------------------------------------

  describe "SCAN" do
    test "SCAN 0 returns all keys when count >= total keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}, "c" => {"3", 0}})
      [next_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "100"], store)
      assert next_cursor == "0"
      assert Enum.sort(keys) == ["a", "b", "c"]
    end

    test "SCAN full iteration visits all keys" do
      data =
        for i <- 1..25, into: %{} do
          {"key:#{String.pad_leading(Integer.to_string(i), 2, "0")}", {"v#{i}", 0}}
        end

      store = MockStore.make(data)

      {all_keys, _} =
        iterate_scan(store, "0", [], "COUNT", "10")

      assert length(all_keys) == 25
      assert all_keys == Enum.uniq(all_keys)
    end

    test "SCAN with MATCH pattern filters keys" do
      store =
        MockStore.make(%{
          "user:1" => {"v", 0},
          "user:2" => {"v", 0},
          "order:1" => {"v", 0}
        })

      [_cursor, keys] = Generic.handle("SCAN", ["0", "MATCH", "user:*", "COUNT", "100"], store)
      assert Enum.sort(keys) == ["user:1", "user:2"]
    end

    test "SCAN with MATCH ? matches single character keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "ab" => {"2", 0}, "b" => {"3", 0}})
      [_cursor, keys] = Generic.handle("SCAN", ["0", "MATCH", "?", "COUNT", "100"], store)
      assert Enum.sort(keys) == ["a", "b"]
    end

    test "SCAN with COUNT hint limits batch size" do
      data = for i <- 1..20, into: %{}, do: {"k#{String.pad_leading("#{i}", 2, "0")}", {"v", 0}}
      store = MockStore.make(data)
      [next_cursor, keys] = Generic.handle("SCAN", ["0", "COUNT", "5"], store)
      assert length(keys) == 5
      assert next_cursor != "0"
    end

    test "SCAN with TYPE string returns only string keys" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      [_cursor, keys] = Generic.handle("SCAN", ["0", "TYPE", "string", "COUNT", "100"], store)
      assert Enum.sort(keys) == ["a", "b"]
    end

    test "SCAN with TYPE list returns empty when no list keys exist" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      [cursor, keys] = Generic.handle("SCAN", ["0", "TYPE", "list", "COUNT", "100"], store)
      assert keys == []
      assert cursor == "0"
    end

    test "SCAN on empty DB returns cursor 0 and empty list" do
      store = MockStore.make()
      [cursor, keys] = Generic.handle("SCAN", ["0"], store)
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN with no args returns error" do
      assert {:error, _} = Generic.handle("SCAN", [], MockStore.make())
    end

    test "SCAN with invalid COUNT returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Generic.handle("SCAN", ["0", "COUNT", "abc"], store)
    end

    test "SCAN with COUNT 0 returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Generic.handle("SCAN", ["0", "COUNT", "0"], store)
    end

    test "SCAN with negative COUNT returns error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Generic.handle("SCAN", ["0", "COUNT", "-1"], store)
    end

    test "SCAN with invalid option returns syntax error" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert {:error, _} = Generic.handle("SCAN", ["0", "BOGUS"], store)
    end

    test "SCAN case-insensitive options" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      [_cursor, keys] = Generic.handle("SCAN", ["0", "match", "*", "count", "100"], store)
      assert Enum.sort(keys) == ["a", "b"]
    end
  end

  # ---------------------------------------------------------------------------
  # EXPIRETIME
  # ---------------------------------------------------------------------------

  describe "EXPIRETIME" do
    test "EXPIRETIME returns -2 for missing key" do
      store = MockStore.make()
      assert -2 == Generic.handle("EXPIRETIME", ["missing"], store)
    end

    test "EXPIRETIME returns -1 for key with no expiry" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Generic.handle("EXPIRETIME", ["k"], store)
    end

    test "EXPIRETIME returns Unix timestamp in seconds" do
      expire_at_ms = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"v", expire_at_ms}})
      result = Generic.handle("EXPIRETIME", ["k"], store)
      expected = div(expire_at_ms, 1_000)
      assert result == expected
    end

    test "EXPIRETIME with no args returns error" do
      assert {:error, _} = Generic.handle("EXPIRETIME", [], MockStore.make())
    end

    test "EXPIRETIME with too many args returns error" do
      assert {:error, _} = Generic.handle("EXPIRETIME", ["a", "b"], MockStore.make())
    end

    test "EXPIRETIME returns -2 for expired key" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert -2 == Generic.handle("EXPIRETIME", ["k"], store)
    end

    test "EXPIRETIME returns -2 for a fully expired compound key before TYPE cleanup" do
      store = stale_hash_store("hash")

      assert -2 == Generic.handle("EXPIRETIME", ["hash"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # PEXPIRETIME
  # ---------------------------------------------------------------------------

  describe "PEXPIRETIME" do
    test "PEXPIRETIME returns -2 for missing key" do
      store = MockStore.make()
      assert -2 == Generic.handle("PEXPIRETIME", ["missing"], store)
    end

    test "PEXPIRETIME returns -1 for key with no expiry" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert -1 == Generic.handle("PEXPIRETIME", ["k"], store)
    end

    test "PEXPIRETIME returns Unix timestamp in milliseconds" do
      expire_at_ms = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"k" => {"v", expire_at_ms}})
      result = Generic.handle("PEXPIRETIME", ["k"], store)
      assert result == expire_at_ms
    end

    test "PEXPIRETIME reads expiry without loading the value" do
      expire_at_ms = System.os_time(:millisecond) + 60_000

      store = %{
        expire_at_ms: fn "cold_ttl" -> expire_at_ms end,
        get_meta: fn _key -> flunk("PEXPIRETIME should not load the value") end,
        compound_get_meta: fn _redis_key, _compound_key -> nil end
      }

      assert expire_at_ms == Generic.handle("PEXPIRETIME", ["cold_ttl"], store)
    end

    test "PEXPIRETIME with no args returns error" do
      assert {:error, _} = Generic.handle("PEXPIRETIME", [], MockStore.make())
    end

    test "PEXPIRETIME with too many args returns error" do
      assert {:error, _} = Generic.handle("PEXPIRETIME", ["a", "b"], MockStore.make())
    end

    test "PEXPIRETIME returns -2 for expired key" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert -2 == Generic.handle("PEXPIRETIME", ["k"], store)
    end

    test "PEXPIRETIME returns -2 for a fully expired compound key before TYPE cleanup" do
      store = stale_hash_store("hash")

      assert -2 == Generic.handle("PEXPIRETIME", ["hash"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # OBJECT ENCODING
  # ---------------------------------------------------------------------------

  describe "OBJECT ENCODING" do
    test "OBJECT ENCODING returns 'embstr' for short string key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "embstr" == Generic.handle("OBJECT", ["ENCODING", "k"], store)
    end

    test "OBJECT ENCODING returns 'int' for canonical integer strings" do
      store =
        MockStore.make(%{
          "positive" => {"42", 0},
          "negative" => {"-7", 0},
          "max" => {"9223372036854775807", 0},
          "min" => {"-9223372036854775808", 0}
        })

      assert "int" == Generic.handle("OBJECT", ["ENCODING", "positive"], store)
      assert "int" == Generic.handle("OBJECT", ["ENCODING", "negative"], store)
      assert "int" == Generic.handle("OBJECT", ["ENCODING", "max"], store)
      assert "int" == Generic.handle("OBJECT", ["ENCODING", "min"], store)
    end

    test "OBJECT ENCODING does not return 'int' for out-of-range integer strings" do
      store = MockStore.make(%{"too_big" => {"9223372036854775808", 0}})

      assert "embstr" == Generic.handle("OBJECT", ["ENCODING", "too_big"], store)
    end

    test "OBJECT ENCODING classifies large strings from metadata without loading value" do
      store = %{
        exists?: fn "cold_large" -> true end,
        compound_get: fn "cold_large", _compound_key -> nil end,
        value_size: fn "cold_large" -> 10_000 end,
        get: fn _key -> flunk("OBJECT ENCODING should not load a large cold string value") end
      }

      assert "raw" == Generic.handle("OBJECT", ["ENCODING", "cold_large"], store)
    end

    test "OBJECT ENCODING returns error for missing key" do
      store = MockStore.make()

      assert {:error, "ERR no such key"} =
               Generic.handle("OBJECT", ["ENCODING", "missing"], store)
    end

    test "OBJECT encoding is case-insensitive" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "embstr" == Generic.handle("OBJECT", ["encoding", "k"], store)
    end

    test "OBJECT ENCODING detects hash keys stored only as compound entries" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["hash", "field", "value"], store)

      assert "hashtable" == Generic.handle("OBJECT", ["ENCODING", "hash"], store)
    end

    test "OBJECT REFCOUNT detects hash keys stored only as compound entries" do
      store = MockStore.make()
      assert 1 == Hash.handle("HSET", ["hash", "field", "value"], store)

      assert 1 == Generic.handle("OBJECT", ["REFCOUNT", "hash"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # OBJECT HELP
  # ---------------------------------------------------------------------------

  describe "OBJECT HELP" do
    test "OBJECT HELP returns a list of help strings" do
      result = Generic.handle("OBJECT", ["HELP"], MockStore.make())
      assert is_list(result)
      assert result != []
      assert Enum.all?(result, &is_binary/1)
    end

    test "OBJECT HELP mentions all subcommands" do
      result = Generic.handle("OBJECT", ["HELP"], MockStore.make())
      text = Enum.join(result, " ")
      assert text =~ "ENCODING"
      assert text =~ "FREQ"
      assert text =~ "HELP"
      assert text =~ "IDLETIME"
      assert text =~ "REFCOUNT"
    end

    test "OBJECT help is case-insensitive" do
      result = Generic.handle("OBJECT", ["help"], MockStore.make())
      assert is_list(result)
    end
  end

  # ---------------------------------------------------------------------------
  # OBJECT FREQ
  # ---------------------------------------------------------------------------

  describe "OBJECT FREQ" do
    test "OBJECT FREQ returns 0 for existing key (stub)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 0 == Generic.handle("OBJECT", ["FREQ", "k"], store)
    end

    test "OBJECT FREQ uses injected LFU metadata instead of the default instance" do
      store = %{
        compound_get: fn _redis_key, _compound_key -> nil end,
        exists?: fn "custom_cold" -> true end,
        object_lfu: fn "custom_cold" -> LFU.pack(LFU.now_minutes(), 37) end,
        get: fn _key -> flunk("OBJECT FREQ should not load the value") end
      }

      assert 37 == Generic.handle("OBJECT", ["FREQ", "custom_cold"], store)
    end

    test "OBJECT FREQ returns error for missing key" do
      store = MockStore.make()
      assert {:error, "ERR no such key"} = Generic.handle("OBJECT", ["FREQ", "missing"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # OBJECT IDLETIME
  # ---------------------------------------------------------------------------

  describe "OBJECT IDLETIME" do
    test "OBJECT IDLETIME returns 0 for existing key (stub)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 0 == Generic.handle("OBJECT", ["IDLETIME", "k"], store)
    end

    test "OBJECT IDLETIME uses injected LFU metadata instead of the default instance" do
      ldt = Bitwise.band(LFU.now_minutes() - 2, 0xFFFF)

      store = %{
        compound_get: fn _redis_key, _compound_key -> nil end,
        exists?: fn "custom_idle" -> true end,
        object_lfu: fn "custom_idle" -> LFU.pack(ldt, 1) end,
        get: fn _key -> flunk("OBJECT IDLETIME should not load the value") end
      }

      assert Generic.handle("OBJECT", ["IDLETIME", "custom_idle"], store) in 120..180
    end

    test "OBJECT IDLETIME returns error for missing key" do
      store = MockStore.make()

      assert {:error, "ERR no such key"} =
               Generic.handle("OBJECT", ["IDLETIME", "missing"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # OBJECT REFCOUNT
  # ---------------------------------------------------------------------------

  describe "OBJECT REFCOUNT" do
    test "OBJECT REFCOUNT returns 1 for existing key" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Generic.handle("OBJECT", ["REFCOUNT", "k"], store)
    end

    test "OBJECT REFCOUNT returns error for missing key" do
      store = MockStore.make()

      assert {:error, "ERR no such key"} =
               Generic.handle("OBJECT", ["REFCOUNT", "missing"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # OBJECT -- error cases
  # ---------------------------------------------------------------------------

  describe "OBJECT error cases" do
    test "OBJECT with no subcommand returns error" do
      assert {:error, _} = Generic.handle("OBJECT", [], MockStore.make())
    end

    test "OBJECT with unknown subcommand returns error" do
      assert {:error, msg} = Generic.handle("OBJECT", ["BOGUS", "key"], MockStore.make())
      assert msg =~ "unknown subcommand"
      assert msg =~ "bogus"
    end
  end

  # ---------------------------------------------------------------------------
  # WAIT
  # ---------------------------------------------------------------------------

  describe "WAIT" do
    test "WAIT returns 0 immediately (no replication)" do
      store = MockStore.make()
      assert 0 == Generic.handle("WAIT", ["1", "0"], store)
    end

    test "WAIT with timeout returns 0" do
      store = MockStore.make()
      assert 0 == Generic.handle("WAIT", ["3", "5000"], store)
    end

    test "WAIT with no args returns error" do
      assert {:error, _} = Generic.handle("WAIT", [], MockStore.make())
    end

    test "WAIT with one arg returns error" do
      assert {:error, _} = Generic.handle("WAIT", ["1"], MockStore.make())
    end

    test "WAIT with too many args returns error" do
      assert {:error, _} = Generic.handle("WAIT", ["1", "0", "extra"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-command edge cases
  # ---------------------------------------------------------------------------

  describe "cross-command edge cases" do
    test "RENAME then TYPE on old key returns 'none'" do
      store = MockStore.make(%{"old" => {"v", 0}})
      Generic.handle("RENAME", ["old", "new"], store)
      assert {:simple, "none"} == Generic.handle("TYPE", ["old"], store)
      assert {:simple, "string"} == Generic.handle("TYPE", ["new"], store)
    end

    test "COPY then UNLINK source preserves destination" do
      store = MockStore.make(%{"src" => {"v", 0}})
      Generic.handle("COPY", ["src", "dst"], store)
      Generic.handle("UNLINK", ["src"], store)
      assert nil == store.get.("src")
      assert "v" == store.get.("dst")
    end

    test "RENAME expired source returns error" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"old" => {"v", past}})
      assert {:error, "ERR no such key"} = Generic.handle("RENAME", ["old", "new"], store)
    end

    test "COPY expired source returns 0" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"src" => {"v", past}})
      assert 0 == Generic.handle("COPY", ["src", "dst"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- SCAN iteration helper
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Edge cases: SCAN non-zero cursor, OBJECT arity, COPY edge cases
  # ---------------------------------------------------------------------------

  describe "SCAN cursor edge cases" do
    test "SCAN with non-zero initial cursor starts from that position" do
      data = for i <- 1..20, into: %{}, do: {"k#{String.pad_leading("#{i}", 2, "0")}", {"v", 0}}
      store = MockStore.make(data)
      # First scan
      [cursor1, keys1] = Generic.handle("SCAN", ["0", "COUNT", "5"], store)
      assert length(keys1) == 5
      # Continue from cursor
      [_cursor2, keys2] = Generic.handle("SCAN", [cursor1, "COUNT", "5"], store)
      assert length(keys2) == 5
      # No overlap between batches
      assert MapSet.disjoint?(MapSet.new(keys1), MapSet.new(keys2))
    end

    test "SCAN with MATCH and COUNT combined works" do
      data = %{
        "user:1" => {"v", 0},
        "user:2" => {"v", 0},
        "user:3" => {"v", 0},
        "order:1" => {"v", 0},
        "order:2" => {"v", 0}
      }

      store = MockStore.make(data)
      [_cursor, keys] = Generic.handle("SCAN", ["0", "MATCH", "order:*", "COUNT", "100"], store)
      assert Enum.sort(keys) == ["order:1", "order:2"]
    end
  end

  describe "OBJECT arity edge cases" do
    test "OBJECT ENCODING with no key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Generic.handle("OBJECT", ["ENCODING"], store)
      assert msg =~ "unknown subcommand or wrong number"
    end

    test "OBJECT FREQ with no key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Generic.handle("OBJECT", ["FREQ"], store)
      assert msg =~ "unknown subcommand or wrong number"
    end

    test "OBJECT IDLETIME with no key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Generic.handle("OBJECT", ["IDLETIME"], store)
      assert msg =~ "unknown subcommand or wrong number"
    end

    test "OBJECT REFCOUNT with no key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Generic.handle("OBJECT", ["REFCOUNT"], store)
      assert msg =~ "unknown subcommand or wrong number"
    end
  end

  describe "COPY edge cases" do
    test "COPY with extra args after REPLACE returns syntax error" do
      store = MockStore.make(%{"src" => {"v", 0}})
      assert {:error, msg} = Generic.handle("COPY", ["src", "dst", "REPLACE", "extra"], store)
      assert msg =~ "syntax error"
    end

    test "COPY with two extra args returns syntax error" do
      store = MockStore.make(%{"src" => {"v", 0}})
      assert {:error, msg} = Generic.handle("COPY", ["src", "dst", "a", "b"], store)
      assert msg =~ "syntax error"
    end
  end

  describe "RENAME arity edge cases" do
    test "RENAME with extra args returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, msg} = Generic.handle("RENAME", ["old", "new", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "RENAMENX with extra args returns error" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert {:error, msg} = Generic.handle("RENAMENX", ["old", "new", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "WAIT arity edge cases" do
    test "WAIT accepts string args that represent non-integer but still returns 0" do
      # WAIT doesn't validate its args because no replication - just returns 0
      store = MockStore.make()
      assert 0 == Generic.handle("WAIT", ["abc", "xyz"], store)
    end
  end

  defp iterate_scan(store, cursor, acc, count_opt, count_val) do
    [next_cursor, keys] = Generic.handle("SCAN", [cursor, count_opt, count_val], store)

    all_keys = acc ++ keys

    if next_cursor == "0" do
      {all_keys, next_cursor}
    else
      iterate_scan(store, next_cursor, all_keys, count_opt, count_val)
    end
  end

  defp stale_hash_store(key, initial \\ %{}) do
    store = MockStore.make(initial)
    Hash.handle("HSET", [key, "field", "value"], store)
    store.compound_put.(key, CompoundKey.hash_field(key, "field"), "value", expired_at_ms())
    store
  end

  defp metadata_only_key_store(expiries) do
    %{
      get: fn key -> flunk("no-op generic command should not load value for #{inspect(key)}") end,
      get_meta: fn key ->
        flunk("no-op generic command should not load value metadata for #{inspect(key)}")
      end,
      expire_at_ms: fn key -> Map.get(expiries, key) end,
      exists?: fn key -> Map.has_key?(expiries, key) end
    }
  end

  defp expired_at_ms, do: System.os_time(:millisecond) - 1
end
