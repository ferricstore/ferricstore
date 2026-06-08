defmodule Ferricstore.Commands.GenericTest.Sections.ObjectIdletime do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Generic, Hash, Stream}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.LFU
      alias Ferricstore.Test.MockStore

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
    end
  end
end
