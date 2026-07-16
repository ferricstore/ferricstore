defmodule Ferricstore.Commands.StringsTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Stream, Strings}
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  # ---------------------------------------------------------------------------
  # SET
  # ---------------------------------------------------------------------------

  describe "SET" do
    test "SET key value returns :ok" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "value"], store)
      assert "value" == store.get.("key")
    end

    test "executes Rust SET AST without reparsing option binaries" do
      store = MockStore.make()

      assert nil ==
               Strings.handle_ast({:set, "key", "value", [{:ex, 10}, :nx, :get]}, store)

      assert "value" == store.get.("key")
      assert nil == Strings.handle_ast({:set, "key", "new", [:nx]}, store)
      assert "value" == store.get.("key")
    end

    test "returns Rust SET AST option errors unchanged" do
      assert {:error, "ERR syntax error"} =
               Strings.handle_ast(
                 {:set, "key", "value", {:error, "ERR syntax error"}},
                 MockStore.make()
               )
    end

    test "Rust SET AST rejects conflicting options before touching storage" do
      store =
        MockStore.make()
        |> Map.put(:put, fn _key, _value, _expire_at_ms ->
          flunk("invalid SET options must not write")
        end)
        |> Map.put(:set, fn _key, _value, _opts ->
          flunk("invalid SET options must not write")
        end)

      assert {:error, "ERR XX and NX options at the same time are not compatible"} =
               Strings.handle_ast({:set, "key", "value", [:nx, :xx]}, store)

      assert {:error, "ERR syntax error"} =
               Strings.handle_ast({:set, "key", "value", [{:ex, 1}, {:px, 1}]}, store)

      assert {:error, "ERR syntax error"} =
               Strings.handle_ast({:set, "key", "value", [:keepttl, {:ex, 1}]}, store)
    end

    test "Rust SET AST rejects nonpositive expiry values before touching storage" do
      store =
        MockStore.make()
        |> Map.put(:put, fn _key, _value, _expire_at_ms ->
          flunk("invalid SET expiry must not write")
        end)
        |> Map.put(:set, fn _key, _value, _opts ->
          flunk("invalid SET expiry must not write")
        end)

      for option <- [{:ex, 0}, {:px, -1}, {:exat, 0}, {:pxat, -1}] do
        assert {:error, "ERR invalid expire time in 'set' command"} =
                 Strings.handle_ast({:set, "key", "value", [option]}, store)
      end
    end

    test "SET with EX sets expiry in seconds" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "value", "EX", "10"], store)
      # key should be accessible right after set (not expired yet)
      assert "value" == store.get.("key")
    end

    test "SET with PX sets expiry in milliseconds" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "value", "PX", "5000"], store)
      assert "value" == store.get.("key")
    end

    test "SET with NX succeeds when key is absent" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["newkey", "val", "NX"], store)
      assert "val" == store.get.("newkey")
    end

    test "SET options are case-insensitive" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["newkey", "val", "ex", "10", "nx"], store)
      assert "val" == store.get.("newkey")
    end

    test "SET with NX returns nil when key already present" do
      store = MockStore.make(%{"key" => {"existing", 0}})
      assert nil == Strings.handle("SET", ["key", "new_val", "NX"], store)
      assert "existing" == store.get.("key")
    end

    test "SET with XX returns :ok when key exists" do
      store = MockStore.make(%{"key" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["key", "new", "XX"], store)
      assert "new" == store.get.("key")
    end

    test "SET with XX returns nil when key absent" do
      store = MockStore.make()
      assert nil == Strings.handle("SET", ["key", "val", "XX"], store)
      assert nil == store.get.("key")
    end

    test "SET with no args returns error" do
      assert {:error, _} = Strings.handle("SET", [], MockStore.make())
    end

    test "SET with only key returns error" do
      assert {:error, _} = Strings.handle("SET", ["key"], MockStore.make())
    end

    test "SET with EX 0 returns error" do
      assert {:error, msg} = Strings.handle("SET", ["key", "val", "EX", "0"], MockStore.make())
      assert msg =~ "invalid expire"
    end

    test "SET with EX -1 returns error" do
      assert {:error, msg} = Strings.handle("SET", ["key", "val", "EX", "-1"], MockStore.make())
      assert msg =~ "invalid expire"
    end

    test "SET with EX non-integer returns error" do
      assert {:error, _} = Strings.handle("SET", ["key", "val", "EX", "abc"], MockStore.make())
    end

    test "SET with PX 0 returns error" do
      assert {:error, msg} = Strings.handle("SET", ["key", "val", "PX", "0"], MockStore.make())
      assert msg =~ "invalid expire"
    end

    test "SET overwrites existing key" do
      store = MockStore.make(%{"key" => {"old", 0}})
      assert :ok = Strings.handle("SET", ["key", "new"], store)
      assert "new" == store.get.("key")
    end

    test "SET returns compound cleanup errors before overwriting hash" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["key", "field", "value"], base)

      store =
        base
        |> Map.put(:compound_delete_prefix, fn "key", _prefix -> {:error, :disk_full} end)
        |> Map.put(:put, fn "key", _value, _expire_at_ms ->
          flunk("SET should not write string when compound cleanup fails")
        end)

      assert {:error, :disk_full} = Strings.handle("SET", ["key", "string"], store)
      assert "value" == Hash.handle("HGET", ["key", "field"], base)
    end

    test "SET returns type marker cleanup errors before overwriting compound key" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["key", "field", "value"], base)
      type_key = CompoundKey.type_key("key")

      store =
        base
        |> Map.put(:compound_delete, fn
          "key", ^type_key -> {:error, :disk_full}
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)
        |> Map.put(:put, fn "key", _value, _expire_at_ms ->
          flunk("SET should not write string when type cleanup fails")
        end)

      assert {:error, :disk_full} = Strings.handle("SET", ["key", "string"], store)
      assert "hash" == base.compound_get.("key", type_key)
      assert "value" == Hash.handle("HGET", ["key", "field"], base)
    end

    test "SET with EX and NX combined works when key absent" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["key", "val", "EX", "10", "NX"], store)
      assert "val" == store.get.("key")
    end

    test "SET with EX and NX combined returns nil when key present" do
      store = MockStore.make(%{"key" => {"old", 0}})
      assert nil == Strings.handle("SET", ["key", "val", "EX", "10", "NX"], store)
      assert "old" == store.get.("key")
    end

    test "SET KEEPTTL without GET reads expiry without loading a cold value" do
      expire_at_ms = System.os_time(:millisecond) + 60_000
      {:ok, seen_expire} = Agent.start_link(fn -> nil end)

      store = %{
        get: fn _key -> flunk("SET KEEPTTL should not load the value") end,
        get_meta: fn _key -> flunk("SET KEEPTTL should not load value metadata") end,
        expire_at_ms: fn "cold" -> expire_at_ms end,
        put: fn "cold", "new", exp ->
          Agent.update(seen_expire, fn _ -> exp end)
          :ok
        end,
        compound_get: fn _redis_key, _compound_key -> nil end
      }

      assert :ok == Strings.handle("SET", ["cold", "new", "KEEPTTL"], store)
      assert expire_at_ms == Agent.get(seen_expire, & &1)
    end
  end

  describe "GETEX prepared AST validation" do
    test "rejects nonpositive expiry values before touching storage" do
      store =
        MockStore.make(%{"key" => {"value", 0}})
        |> Map.put(:getex, fn _key, _expire_at_ms ->
          flunk("invalid GETEX expiry must not mutate storage")
        end)

      for option <- [{:ex, 0}, {:px, -1}, {:exat, 0}, {:pxat, -1}] do
        assert {:error, "ERR invalid expire time in 'getex' command"} =
                 Strings.handle_ast({:getex, "key", option}, store)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # GET
  # ---------------------------------------------------------------------------

  describe "GET" do
    test "GET existing key returns value" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert "v" == Strings.handle("GET", ["k"], store)
    end

    test "GET missing key returns nil" do
      assert nil == Strings.handle("GET", ["missing"], MockStore.make())
    end

    test "GET expired key returns nil" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert nil == Strings.handle("GET", ["k"], store)
    end

    test "GET with no args returns error" do
      assert {:error, _} = Strings.handle("GET", [], MockStore.make())
    end

    test "GET with too many args returns error" do
      assert {:error, _} = Strings.handle("GET", ["a", "b"], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # DEL
  # ---------------------------------------------------------------------------

  describe "DEL" do
    test "DEL existing key returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Strings.handle("DEL", ["k"], store)
    end

    test "DEL missing key returns 0" do
      assert 0 == Strings.handle("DEL", ["missing"], MockStore.make())
    end

    test "DEL multiple keys returns count of deleted" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert 2 == Strings.handle("DEL", ["a", "b", "c"], store)
    end

    test "DEL no args returns error" do
      assert {:error, _} = Strings.handle("DEL", [], MockStore.make())
    end

    test "DEL handles stream type metadata without crashing" do
      store = MockStore.make()
      store.compound_put.("s", CompoundKey.type_key("s"), "stream", 0)

      assert 1 == Strings.handle("DEL", ["s"], store)
      assert nil == store.compound_get.("s", CompoundKey.type_key("s"))
    end

    test "DEL removes stream entries and metadata" do
      Stream.ensure_meta_table()
      store = MockStore.make()
      key = "stream_del_#{System.unique_integer([:positive])}"

      id = Stream.handle("XADD", [key, "*", "f", "v"], store)
      assert is_binary(id)
      assert 1 == Stream.handle("XLEN", [key], store)

      assert 1 == Strings.handle("DEL", [key], store)
      assert 0 == Stream.handle("XLEN", [key], store)
      assert [] == store.compound_scan.(key, "X:#{key}" <> <<0>>)
    end

    test "DEL clears stream index so stale entries do not consume range count" do
      Stream.ensure_meta_table()
      store = MockStore.make()
      key = "stream_del_#{System.unique_integer([:positive])}"

      assert "1-0" == Stream.handle("XADD", [key, "1-0", "f", "old"], store)

      assert [["1-0", "f", "old"]] ==
               Stream.handle("XRANGE", [key, "-", "+", "COUNT", "1"], store)

      assert 1 == Strings.handle("DEL", [key], store)
      assert "2-0" == Stream.handle("XADD", [key, "2-0", "f", "new"], store)

      assert [["2-0", "f", "new"]] ==
               Stream.handle("XRANGE", [key, "-", "+", "COUNT", "1"], store)
    end

    test "DEL returns compound prefix delete errors before removing type metadata" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["h", "f", "v"], base)
      type_key = CompoundKey.type_key("h")

      store =
        base
        |> Map.put(:compound_delete_prefix, fn "h", _prefix -> {:error, :disk_full} end)
        |> Map.put(:compound_delete, fn
          "h", ^type_key ->
            flunk("DEL must not remove type metadata after compound prefix delete failure")

          key, compound_key ->
            base.compound_delete.(key, compound_key)
        end)

      assert {:error, :disk_full} == Strings.handle("DEL", ["h"], store)
      assert "hash" == base.compound_get.("h", type_key)
      assert "v" == Hash.handle("HGET", ["h", "f"], base)
    end

    test "DEL returns stream entry delete errors before cleaning stream metadata" do
      Stream.ensure_meta_table()
      base = MockStore.make()

      id = Stream.handle("XADD", ["s", "*", "f", "v"], base)
      assert is_binary(id)
      assert 1 == Stream.handle("XLEN", ["s"], base)

      store =
        Map.put(base, :compound_delete_prefix, fn "s", "X:s" <> <<0>> -> {:error, :disk_full} end)

      assert {:error, :disk_full} == Strings.handle("DEL", ["s"], store)
      assert 1 == Stream.handle("XLEN", ["s"], base)
      assert [_] = base.compound_scan.("s", "X:s" <> <<0>>)
    end

    @tag :transaction_stream_cache_commit
    test "DEL defers stream cache cleanup for transactional stores" do
      Stream.ensure_meta_table()
      base = MockStore.make()
      key = "stream_deferred_del_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        if :ets.whereis(Ferricstore.Stream.Meta) != :undefined do
          :ets.delete(Ferricstore.Stream.Meta, key)
        end
      end)

      assert "1-0" == Stream.handle("XADD", [key, "1-0", "f", "v"], base)
      assert [{^key, 1, "1-0", "1-0", 1, 0}] = :ets.lookup(Ferricstore.Stream.Meta, key)

      test_pid = self()

      store =
        Map.put(base, :defer_stream_cleanup, fn deferred_key ->
          send(test_pid, {:stream_cleanup_deferred, deferred_key})
          :ok
        end)

      assert 1 == Strings.handle("DEL", [key], store)
      assert_receive {:stream_cleanup_deferred, ^key}
      assert [{^key, 1, "1-0", "1-0", 1, 0}] = :ets.lookup(Ferricstore.Stream.Meta, key)
    end
  end

  # ---------------------------------------------------------------------------
  # EXISTS
  # ---------------------------------------------------------------------------

  describe "EXISTS" do
    test "EXISTS present key returns 1" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Strings.handle("EXISTS", ["k"], store)
    end

    test "EXISTS absent key returns 0" do
      assert 0 == Strings.handle("EXISTS", ["missing"], MockStore.make())
    end

    test "EXISTS multiple keys returns sum" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert 2 == Strings.handle("EXISTS", ["a", "b", "c"], store)
    end

    test "EXISTS same key twice counts twice" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 2 == Strings.handle("EXISTS", ["k", "k"], store)
    end

    test "EXISTS no args returns error" do
      assert {:error, _} = Strings.handle("EXISTS", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # MGET
  # ---------------------------------------------------------------------------

  describe "MGET" do
    test "MGET multiple keys returns array of values with nils for missing" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}})
      assert ["1", "2", nil] == Strings.handle("MGET", ["a", "b", "c"], store)
    end

    test "MGET single key returns single-element list" do
      store = MockStore.make(%{"a" => {"1", 0}})
      assert ["1"] == Strings.handle("MGET", ["a"], store)
    end

    test "MGET uses batch_get when the store provides it" do
      store = %{
        batch_get: fn keys ->
          Enum.map(keys, fn
            "a" -> "1"
            "b" -> "2"
            _ -> nil
          end)
        end,
        get: fn key -> flunk("MGET should use batch_get, got per-key GET for #{inspect(key)}") end
      }

      assert ["1", "2", nil] == Strings.handle("MGET", ["a", "b", "c"], store)
    end

    test "MGET no args returns error" do
      assert {:error, _} = Strings.handle("MGET", [], MockStore.make())
    end
  end

  # ---------------------------------------------------------------------------
  # MSET
  # ---------------------------------------------------------------------------

  describe "MSET" do
    test "MSET key val pairs returns :ok" do
      store = MockStore.make()
      assert :ok = Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
      assert "v1" == store.get.("k1")
      assert "v2" == store.get.("k2")
    end

    test "MSET odd number of args returns error" do
      assert {:error, _} = Strings.handle("MSET", ["k1", "v1", "k2"], MockStore.make())
    end

    test "MSET no args returns error" do
      assert {:error, _} = Strings.handle("MSET", [], MockStore.make())
    end

    test "MSET returns put error instead of discarding it" do
      store =
        MockStore.make()
        |> Map.put(:put, fn _key, _value, _expire_at_ms -> {:error, "ERR disk write failed"} end)

      assert {:error, "ERR disk write failed"} =
               Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
    end

    test "MSET uses batch_put when the store provides it" do
      parent = self()

      store = %{
        batch_put: fn kv_pairs ->
          send(parent, {:batch_put, kv_pairs})
          :ok
        end,
        put: fn key, _value, _expire_at_ms ->
          flunk("MSET should use batch_put, got per-key PUT for #{inspect(key)}")
        end,
        compound_get: fn _redis_key, _compound_key -> nil end
      }

      assert :ok == Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
      assert_received {:batch_put, [{"k1", "v1"}, {"k2", "v2"}]}
    end

    test "MSET propagates batch_put errors without per-key fallback" do
      store = %{
        batch_put: fn [{"k1", "v1"}, {"k2", "v2"}] -> {:error, "ERR disk write failed"} end,
        put: fn key, _value, _expire_at_ms ->
          flunk("MSET should not retry per-key after batch_put failure for #{inspect(key)}")
        end,
        compound_get: fn _redis_key, _compound_key -> nil end
      }

      assert {:error, "ERR disk write failed"} ==
               Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
    end

    test "MSET falls back to cleanup path when replacing compound data" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["k1", "field", "value"], base)

      store =
        base
        |> Map.put(:batch_put, fn kv_pairs ->
          flunk("MSET must not use blind batch_put for compound cleanup: #{inspect(kv_pairs)}")
        end)
        |> Map.put(:compound_delete_prefix, fn "k1", _prefix -> {:error, :disk_full} end)
        |> Map.put(:put, fn "k1", _value, _expire_at_ms ->
          flunk("MSET should not write string when compound cleanup fails")
        end)

      assert {:error, :disk_full} == Strings.handle("MSET", ["k1", "v1"], store)
      assert "value" == Hash.handle("HGET", ["k1", "field"], base)
    end

    test "MSET preserves earlier compound keys when a later replacement fails" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["k1", "field", "old1"], base)
      assert 1 == Hash.handle("HSET", ["k2", "field", "old2"], base)

      store =
        base
        |> Map.put(:batch_put, fn kv_pairs ->
          flunk("MSET must not use blind batch_put for compound cleanup: #{inspect(kv_pairs)}")
        end)
        |> Map.put(:compound_delete_prefix, fn
          "k2", _prefix -> {:error, :disk_full}
          key, prefix -> base.compound_delete_prefix.(key, prefix)
        end)

      assert {:error, :disk_full} == Strings.handle("MSET", ["k1", "v1", "k2", "v2"], store)
      assert nil == base.get.("k1")
      assert "old1" == Hash.handle("HGET", ["k1", "field"], base)
      assert "old2" == Hash.handle("HGET", ["k2", "field"], base)
    end

    test "MSET aborts before mutation when rollback metadata cannot be read" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["k1", "field", "old"], base)
      failure = Ferricstore.Store.ReadResult.failure(:missing_file)

      store =
        base
        |> Map.put(:get_meta, fn _key -> failure end)
        |> Map.put(:put, fn _key, _value, _expire_at_ms ->
          flunk("MSET must not write without a complete rollback snapshot")
        end)
        |> Map.put(:compound_delete, fn _redis_key, _compound_key ->
          flunk("MSET must not delete without a complete rollback snapshot")
        end)
        |> Map.put(:compound_delete_prefix, fn _redis_key, _prefix ->
          flunk("MSET must not delete without a complete rollback snapshot")
        end)

      assert {:error, "ERR storage read failed"} == Strings.handle("MSET", ["k1", "new"], store)
      assert "old" == Hash.handle("HGET", ["k1", "field"], base)
    end

    test "MSET with single pair stores the pair" do
      store = MockStore.make()
      assert :ok = Strings.handle("MSET", ["k", "v"], store)
      assert "v" == store.get.("k")
    end

    test "MSET overwrites existing keys" do
      store = MockStore.make(%{"k" => {"old", 0}})
      assert :ok = Strings.handle("MSET", ["k", "new"], store)
      assert "new" == store.get.("k")
    end
  end

  # ---------------------------------------------------------------------------
  # SET — additional edge cases
  # ---------------------------------------------------------------------------

  describe "SET edge cases" do
    test "SET aborts when the compound type marker cannot be read" do
      base = MockStore.make()
      assert 1 == Hash.handle("HSET", ["key", "field", "old"], base)
      failure = Ferricstore.Store.ReadResult.failure(:missing_file)

      store =
        base
        |> Map.put(:compound_get, fn _redis_key, _compound_key -> failure end)
        |> Map.put(:put, fn _key, _value, _expire_at_ms ->
          flunk("SET must not write after a failed type-marker read")
        end)
        |> Map.put(:compound_delete, fn _redis_key, _compound_key ->
          flunk("SET must not delete after a failed type-marker read")
        end)
        |> Map.put(:compound_delete_prefix, fn _redis_key, _prefix ->
          flunk("SET must not delete after a failed type-marker read")
        end)

      assert {:error, "ERR storage read failed"} == Strings.handle("SET", ["key", "new"], store)
      assert "old" == Hash.handle("HGET", ["key", "field"], base)
    end

    test "SET with PX -1 returns error" do
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "PX", "-1"], MockStore.make())

      assert msg =~ "invalid expire"
    end

    test "SET with both NX and XX returns error (mutually exclusive flags)" do
      # Redis rejects NX+XX regardless of key state
      store_empty = MockStore.make()

      assert {:error, "ERR XX and NX options at the same time are not compatible"} =
               Strings.handle("SET", ["key", "val", "NX", "XX"], store_empty)

      store_present = MockStore.make(%{"key" => {"old", 0}})

      assert {:error, "ERR XX and NX options at the same time are not compatible"} =
               Strings.handle("SET", ["key", "val", "NX", "XX"], store_present)
    end

    test "SET with EX and PX both specified returns syntax error" do
      store = MockStore.make()

      # Conflicting expiry options are rejected (Redis 7+ behaviour)
      assert {:error, "ERR syntax error"} =
               Strings.handle("SET", ["k", "v", "EX", "1", "PX", "60000"], store)
    end

    test "SET stores binary value with null bytes" do
      store = MockStore.make()
      value = <<0, 1, 2, 3>>
      assert :ok = Strings.handle("SET", ["key", value], store)
      assert value == store.get.("key")
    end

    test "SET stores key with null bytes" do
      store = MockStore.make()
      key = <<0, 1, 2>>
      assert :ok = Strings.handle("SET", [key, "val"], store)
      assert "val" == store.get.(key)
    end

    test "SET with very long key (10KB)" do
      store = MockStore.make()
      long_key = String.duplicate("k", 10_000)
      assert :ok = Strings.handle("SET", [long_key, "v"], store)
      assert "v" == store.get.(long_key)
    end

    test "SET with very long value (100KB)" do
      store = MockStore.make()
      long_value = String.duplicate("v", 100_000)
      assert :ok = Strings.handle("SET", ["key", long_value], store)
      assert long_value == store.get.("key")
    end

    test "SET with unrecognized option returns syntax error" do
      assert {:error, msg} =
               Strings.handle("SET", ["key", "val", "BOGUS"], MockStore.make())

      assert msg =~ "syntax error"
    end
  end

  # ---------------------------------------------------------------------------
  # GET — additional edge cases
  # ---------------------------------------------------------------------------

  describe "GET edge cases" do
    test "GET returns exact binary value with null bytes (not decoded)" do
      value = <<0, 1, 2>>
      store = MockStore.make(%{"k" => {value, 0}})
      assert ^value = Strings.handle("GET", ["k"], store)
    end

    test "GET after SET with PX returns value before expiry" do
      store = MockStore.make()
      assert :ok = Strings.handle("SET", ["k", "v", "PX", "5000"], store)
      assert "v" == Strings.handle("GET", ["k"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # DEL — additional edge cases
  # ---------------------------------------------------------------------------

  describe "DEL edge cases" do
    test "DEL returns 0 for expired key" do
      past = System.os_time(:millisecond) - 1000
      store = MockStore.make(%{"k" => {"v", past}})
      assert 0 == Strings.handle("DEL", ["k"], store)
    end

    test "DEL same key twice returns 1 (key gone after first delete)" do
      store = MockStore.make(%{"k" => {"v", 0}})
      assert 1 == Strings.handle("DEL", ["k", "k"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases: arity validation, empty key, key too large, error messages
  # ---------------------------------------------------------------------------

  describe "arity and key validation edge cases" do
    test "GET with empty key returns ERR empty key" do
      store = MockStore.make()
      assert {:error, "ERR empty key"} = Strings.handle("GET", [""], store)
    end

    test "GET with key exceeding 65535 bytes returns ERR key too large" do
      store = MockStore.make()
      huge_key = String.duplicate("x", 65_536)
      assert {:error, "ERR key too large"} = Strings.handle("GET", [huge_key], store)
    end

    test "SET with empty key returns ERR empty key" do
      store = MockStore.make()
      assert {:error, "ERR empty key"} = Strings.handle("SET", ["", "val"], store)
    end

    test "SET with key exceeding 65535 bytes returns ERR key too large" do
      store = MockStore.make()
      huge_key = String.duplicate("x", 65_536)
      assert {:error, "ERR key too large"} = Strings.handle("SET", [huge_key, "val"], store)
    end

    test "MSET with empty key in one of the pairs returns error" do
      store = MockStore.make()
      assert {:error, _} = Strings.handle("MSET", ["good", "v1", "", "v2"], store)
    end

    test "MSET with single arg (odd count) returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("MSET", ["only_key"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases: INCRBYFLOAT with inf/nan/edge floats
  # ---------------------------------------------------------------------------

  describe "INCRBYFLOAT edge cases" do
    test "INCRBYFLOAT rejects integer-shaped values outside the float range without raising" do
      store =
        MockStore.make()
        |> Map.put(:incr_float, fn _key, _delta ->
          flunk("an invalid float must not reach storage")
        end)

      huge = String.duplicate("9", 1_000)

      assert {:error, "ERR value is not a valid float"} =
               Strings.handle("INCRBYFLOAT", ["k", huge], store)
    end

    test "INCRBYFLOAT with 'inf' string returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("INCRBYFLOAT", ["k", "inf"], store)
      assert msg =~ "not a valid float"
    end

    test "INCRBYFLOAT with '-inf' string returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("INCRBYFLOAT", ["k", "-inf"], store)
      assert msg =~ "not a valid float"
    end

    test "INCRBYFLOAT with 'nan' string returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("INCRBYFLOAT", ["k", "nan"], store)
      assert msg =~ "not a valid float"
    end

    test "INCRBYFLOAT with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("INCRBYFLOAT", ["k", "1.0", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases: INCRBY / DECRBY with extra args
  # ---------------------------------------------------------------------------

  describe "INCRBY/DECRBY arity edge cases" do
    test "INCRBY with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("INCRBY", ["k", "5", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "DECRBY with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("DECRBY", ["k", "5", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "INCRBY with float delta string returns ERR not an integer" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:error, msg} = Strings.handle("INCRBY", ["k", "1.5"], store)
      assert msg =~ "not an integer"
    end

    test "DECRBY with float delta string returns ERR not an integer" do
      store = MockStore.make(%{"k" => {"10", 0}})
      assert {:error, msg} = Strings.handle("DECRBY", ["k", "1.5"], store)
      assert msg =~ "not an integer"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases: SETEX/PSETEX with extra or missing args
  # ---------------------------------------------------------------------------

  describe "SETEX/PSETEX arity edge cases" do
    test "SETEX with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SETEX", ["k", "10", "val", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "PSETEX with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("PSETEX", ["k", "5000", "val", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases: MSETNX with extra args
  # ---------------------------------------------------------------------------

  describe "MSETNX edge cases" do
    test "MSETNX with single arg (odd) returns error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("MSETNX", ["k"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "MSETNX rejects empty keys before writing" do
      store = MockStore.make()

      assert {:error, "ERR key too large or empty"} =
               Strings.handle("MSETNX", ["", "v", "k", "v2"], store)

      assert nil == store.get.("")
      assert nil == store.get.("k")
    end

    test "MSETNX rejects oversized keys before writing" do
      store = MockStore.make()
      oversized_key = String.duplicate("k", 65_536)

      assert {:error, "ERR key too large or empty"} =
               Strings.handle("MSETNX", [oversized_key, "v"], store)

      assert nil == store.get.(oversized_key)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases: GETRANGE boundary conditions
  # ---------------------------------------------------------------------------

  describe "GETRANGE boundary edge cases" do
    test "GETRANGE with start=0 end=0 on single-byte string returns the byte" do
      store = MockStore.make(%{"k" => {"X", 0}})
      assert "X" = Strings.handle("GETRANGE", ["k", "0", "0"], store)
    end

    test "GETRANGE on empty string value returns empty string" do
      store = MockStore.make(%{"k" => {"", 0}})
      assert "" = Strings.handle("GETRANGE", ["k", "0", "-1"], store)
    end

    test "GETRANGE returns empty out-of-range slice without loading cold value" do
      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 10_000 end,
        get: fn _key -> flunk("GETRANGE should not load a cold value for an empty range") end
      }

      assert "" == Strings.handle("GETRANGE", ["cold", "10000", "10010"], store)
      assert "" == Strings.handle("GETRANGE", ["cold", "-1", "-2"], store)
    end

    test "GETRANGE with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("GETRANGE", ["k", "0", "5", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases: SETRANGE boundary conditions
  # ---------------------------------------------------------------------------

  describe "SETRANGE boundary edge cases" do
    test "SETRANGE with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Strings.handle("SETRANGE", ["k", "0", "val", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end
end
