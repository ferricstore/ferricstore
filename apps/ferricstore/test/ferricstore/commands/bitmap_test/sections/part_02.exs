defmodule Ferricstore.Commands.BitmapTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.Bitmap
      alias Ferricstore.Commands.Hash
      alias Ferricstore.Commands.Set
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

  describe "BITOP XOR" do
    test "two keys" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF, 0x00>>, 0},
          "b" => {<<0xFF, 0xFF>>, 0}
        })

      assert 2 == Bitmap.handle("BITOP", ["XOR", "dest", "a", "b"], store)
      assert <<0x00, 0xFF>> == store.get.("dest")
    end

    test "XOR with itself produces zeros" do
      store =
        MockStore.make(%{
          "a" => {<<0xAB, 0xCD>>, 0}
        })

      assert 2 == Bitmap.handle("BITOP", ["XOR", "dest", "a", "a"], store)
      assert <<0x00, 0x00>> == store.get.("dest")
    end
  end
  describe "BITOP NOT" do
    test "single key" do
      store = MockStore.make(%{"a" => {<<0xFF, 0x00, 0xAA>>, 0}})
      assert 3 == Bitmap.handle("BITOP", ["NOT", "dest", "a"], store)
      assert <<0x00, 0xFF, 0x55>> == store.get.("dest")
    end

    test "empty string" do
      store = MockStore.make(%{"a" => {<<>>, 0}})
      assert 0 == Bitmap.handle("BITOP", ["NOT", "dest", "a"], store)
      assert <<>> == store.get.("dest")
    end

    test "error with multiple source keys" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF>>, 0},
          "b" => {<<0x00>>, 0}
        })

      assert {:error, msg} = Bitmap.handle("BITOP", ["NOT", "dest", "a", "b"], store)
      assert msg =~ "BITOP NOT requires one and only one key"
    end
  end
  describe "BITOP edge cases" do
    test "with non-existent source keys (treated as empty strings)" do
      store = MockStore.make(%{"a" => {<<0xFF>>, 0}})
      # AND with empty (zero-padded) -> all zeros
      assert 1 == Bitmap.handle("BITOP", ["AND", "dest", "a", "missing"], store)
      assert <<0x00>> == store.get.("dest")
    end

    test "AND with a missing source avoids reading large cold sources" do
      parent = self()

      store = %{
        value_size: fn
          "cold" -> 1_048_576
          "missing" -> nil
          "dest" -> nil
        end,
        batch_get: fn keys ->
          send(parent, {:batch_loaded_sources, keys})
          [<<1>>, nil]
        end,
        put: fn "dest", value, 0 ->
          prefix_size = min(byte_size(value), 4)
          send(parent, {:wrote_result, byte_size(value), binary_part(value, 0, prefix_size)})
          :ok
        end,
        compound_get: fn _redis_key, _compound_key -> nil end,
        compound_delete: fn _redis_key, _compound_key -> :ok end,
        compound_delete_prefix: fn _redis_key, _prefix -> :ok end
      }

      assert 1_048_576 == Bitmap.handle("BITOP", ["AND", "dest", "cold", "missing"], store)
      assert_received {:wrote_result, 1_048_576, <<0, 0, 0, 0>>}
      refute_received {:batch_loaded_sources, ["cold", "missing"]}
    end

    test "with all non-existent source keys" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("BITOP", ["OR", "dest", "missing1", "missing2"], store)
      assert <<>> == store.get.("dest")
    end

    test "returns destination write errors" do
      store = %{
        batch_get: fn ["a", "b"] -> [<<0xF0>>, <<0x0F>>] end,
        get: fn _key -> flunk("BITOP should use batch_get") end,
        put: fn "dest", <<0xFF>>, 0 -> {:error, :disk_full} end,
        compound_get: fn _redis_key, _compound_key -> nil end,
        compound_delete: fn _redis_key, _compound_key -> :ok end,
        compound_delete_prefix: fn _redis_key, _prefix -> :ok end
      }

      assert {:error, :disk_full} == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
    end

    test "preserves compound destination when result write fails" do
      base = MockStore.make(%{"a" => {<<0xF0>>, 0}, "b" => {<<0x0F>>, 0}})
      assert 1 == Hash.handle("HSET", ["dest", "field", "old"], base)

      store =
        Map.put(base, :put, fn
          "dest", <<0xFF>>, 0 -> {:error, :disk_full}
          key, value, expire_at_ms -> base.put.(key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
      assert "old" == Hash.handle("HGET", ["dest", "field"], base)
    end

    test "returns compound cleanup errors before overwriting destination" do
      type_key = CompoundKey.type_key("dest")

      store = %{
        batch_get: fn ["a", "b"] -> [<<0xF0>>, <<0x0F>>] end,
        get: fn _key -> flunk("BITOP should use batch_get") end,
        put: fn "dest", _value, 0 -> flunk("BITOP should not write after cleanup failure") end,
        compound_get: fn
          "dest", ^type_key -> "set"
          _redis_key, _compound_key -> nil
        end,
        compound_delete: fn
          "dest", ^type_key -> {:error, :disk_full}
          "dest", _compound_key -> :ok
        end,
        compound_delete_prefix: fn _redis_key, _prefix -> :ok end
      }

      assert {:error, :disk_full} == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
    end

    test "preserves compound destination when cleanup fails after metadata delete" do
      base = MockStore.make(%{"a" => {<<0xF0>>, 0}, "b" => {<<0x0F>>, 0}})
      assert 1 == Hash.handle("HSET", ["dest", "field", "old"], base)
      type_key = CompoundKey.type_key("dest")

      store =
        base
        |> Map.put(:compound_delete, fn
          "dest", ^type_key -> base.compound_delete.("dest", type_key)
          key, compound_key -> base.compound_delete.(key, compound_key)
        end)
        |> Map.put(:compound_delete_prefix, fn "dest", _prefix -> {:error, :disk_full} end)
        |> Map.put(:put, fn "dest", _value, 0 ->
          flunk("BITOP should not write after cleanup failure")
        end)

      assert {:error, :disk_full} == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
      assert "hash" == base.compound_get.("dest", type_key)
      assert "old" == Hash.handle("HGET", ["dest", "field"], base)
    end

    test "returns result length" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF, 0xFF, 0xFF>>, 0},
          "b" => {<<0x00>>, 0}
        })

      assert 3 == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
    end

    test "case insensitive operation name" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF>>, 0},
          "b" => {<<0x0F>>, 0}
        })

      assert 1 == Bitmap.handle("BITOP", ["and", "dest", "a", "b"], store)
      assert <<0x0F>> == store.get.("dest")
    end

    test "error with unknown operation" do
      store = MockStore.make(%{"a" => {<<0xFF>>, 0}})
      assert {:error, _} = Bitmap.handle("BITOP", ["NAND", "dest", "a"], store)
    end

    test "error with no source keys" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("BITOP", ["AND", "dest"], store)
    end

    test "error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("BITOP", [], store)
    end
  end
  describe "SETBIT edge cases" do
    test "SETBIT with non-0/1 string value returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bitmap.handle("SETBIT", ["k", "0", "abc"], store)
      assert msg =~ "bit is not an integer"
    end

    test "SETBIT with value '2' returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bitmap.handle("SETBIT", ["k", "0", "2"], store)
      assert msg =~ "bit is not an integer"
    end

    test "SETBIT with float offset returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bitmap.handle("SETBIT", ["k", "1.5", "1"], store)
      assert msg =~ "not an integer"
    end

    test "SETBIT at very large offset works (creates large binary)" do
      store = MockStore.make()
      # Offset 31 -> byte 3 (4 bytes total)
      assert 0 == Bitmap.handle("SETBIT", ["k", "31", "1"], store)
      assert byte_size(store.get.("k")) == 4
    end
  end
  describe "GETBIT edge cases" do
    test "GETBIT treats ETF-looking plain binaries as strings without type marker" do
      encoded = :erlang.term_to_binary({:list, ["a", "b"]})
      store = MockStore.make(%{"plain" => {encoded, 0}})

      assert 1 == Bitmap.handle("GETBIT", ["plain", "0"], store)
    end

    test "GETBIT with non-integer offset returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bitmap.handle("GETBIT", ["k", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "GETBIT with float offset returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bitmap.handle("GETBIT", ["k", "1.5"], store)
      assert msg =~ "not an integer"
    end

    test "GETBIT with extra args returns arity error" do
      store = MockStore.make()
      assert {:error, msg} = Bitmap.handle("GETBIT", ["k", "0", "extra"], store)
      assert msg =~ "wrong number of arguments"
    end
  end
  describe "BITPOS edge cases" do
    test "BITPOS with invalid bit value returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITPOS", ["k", "2"], store)
      assert msg =~ "bit is not an integer"
    end

    test "BITPOS with non-integer bit value returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITPOS", ["k", "abc"], store)
      assert msg =~ "bit is not an integer"
    end

    test "BITPOS with non-integer start returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITPOS", ["k", "1", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "BITPOS with non-integer end returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITPOS", ["k", "1", "0", "abc"], store)
      assert msg =~ "not an integer"
    end

    test "BITPOS with invalid mode returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITPOS", ["k", "1", "0", "7", "BOGUS"], store)
      assert msg =~ "syntax error"
    end
  end
  describe "BITCOUNT edge cases" do
    test "BITCOUNT treats ETF-looking plain binaries as strings without type marker" do
      encoded = :erlang.term_to_binary({:hash, %{"f" => "v"}})
      store = MockStore.make(%{"plain" => {encoded, 0}})

      assert is_integer(Bitmap.handle("BITCOUNT", ["plain"], store))
    end

    test "BITCOUNT with invalid mode returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITCOUNT", ["k", "0", "0", "BOGUS"], store)
      assert msg =~ "syntax error"
    end

    test "BITCOUNT with non-integer start returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITCOUNT", ["k", "abc", "0"], store)
      assert msg =~ "not an integer"
    end

    test "BITCOUNT with non-integer end returns error" do
      store = MockStore.make(%{"k" => {<<0xFF>>, 0}})
      assert {:error, msg} = Bitmap.handle("BITCOUNT", ["k", "0", "abc"], store)
      assert msg =~ "not an integer"
    end
  end
  describe "BITOP error message edge cases" do
    test "BITOP source treats ETF-looking plain binaries as strings without type marker" do
      encoded = :erlang.term_to_binary({:set, MapSet.new(["a"])})
      store = MockStore.make(%{"plain" => {encoded, 0}})

      assert byte_size(encoded) == Bitmap.handle("BITOP", ["NOT", "dest", "plain"], store)
      assert byte_size(store.get.("dest")) == byte_size(encoded)
    end

    test "bitmap read path does not infer type from ETF-looking string payloads" do
      source = File.read!(app_path("lib/ferricstore/commands/bitmap.ex"))

      refute source =~ "encoded_non_string_type?"
      refute source =~ "extract_etf_atom_name"
    end

    test "BITOP NOT with no source key returns error" do
      store = MockStore.make()
      assert {:error, msg} = Bitmap.handle("BITOP", ["NOT", "dest"], store)
      assert msg =~ "wrong number of arguments"
    end

    test "BITOP with lowercase operation" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF>>, 0},
          "b" => {<<0x0F>>, 0}
        })

      assert 1 == Bitmap.handle("BITOP", ["xor", "dest", "a", "b"], store)
      assert <<0xF0>> == store.get.("dest")
    end

    test "BITOP OR with single key copies it" do
      store = MockStore.make(%{"a" => {<<0xAB>>, 0}})
      assert 1 == Bitmap.handle("BITOP", ["OR", "dest", "a"], store)
      assert <<0xAB>> == store.get.("dest")
    end

    test "BITOP multi-source operations use batch_get when the store provides it" do
      parent = self()
      {:ok, pid} = Agent.start_link(fn -> %{"a" => <<0xF0>>, "b" => <<0x0F>>} end)

      store = %{
        batch_get: fn keys ->
          send(parent, {:batch_get, keys})
          Agent.get(pid, fn state -> Enum.map(keys, &Map.get(state, &1)) end)
        end,
        get: fn key ->
          flunk("BITOP should use batch_get, got per-key GET for #{inspect(key)}")
        end,
        put: fn key, value, _expire_at_ms ->
          Agent.update(pid, &Map.put(&1, key, value))
          :ok
        end,
        compound_get: fn _redis_key, _compound_key -> nil end,
        compound_delete: fn _redis_key, _compound_key -> :ok end,
        compound_delete_prefix: fn _redis_key, _prefix -> :ok end
      }

      assert 1 == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
      assert_received {:batch_get, ["a", "b"]}
      assert <<0xFF>> == Agent.get(pid, &Map.fetch!(&1, "dest"))
    end
  end
  describe "cross-command integration" do
    test "SETBIT then BITCOUNT" do
      store = MockStore.make()
      Bitmap.handle("SETBIT", ["mykey", "0", "1"], store)
      Bitmap.handle("SETBIT", ["mykey", "3", "1"], store)
      Bitmap.handle("SETBIT", ["mykey", "7", "1"], store)
      assert 3 == Bitmap.handle("BITCOUNT", ["mykey"], store)
    end

    test "SETBIT then BITPOS" do
      store = MockStore.make()
      Bitmap.handle("SETBIT", ["mykey", "10", "1"], store)
      assert 10 == Bitmap.handle("BITPOS", ["mykey", "1"], store)
    end

    test "BITPOS 0 with start beyond string length returns length*8 (virtual zero)" do
      # Redis compat: virtual bits past the string are all zeros
      store = MockStore.make(%{"allones" => {<<0xFF, 0xFF>>, 0}})

      # 2 bytes = 16 bits, all 1s. Searching for 0 starting at byte 5.
      # Start is past the string, so the first virtual 0 is at bit 16.
      assert 16 == Bitmap.handle("BITPOS", ["allones", "0", "5"], store)
    end

    test "BITPOS 0 with start beyond known cold size avoids loading value" do
      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 2 end,
        get: fn _key ->
          flunk("BITPOS should not load a cold value when metadata proves the result")
        end
      }

      assert 16 == Bitmap.handle("BITPOS", ["cold", "0", "5"], store)
    end

    test "BITPOS 1 with start beyond string length returns -1" do
      store = MockStore.make(%{"short" => {<<0xFF>>, 0}})

      # Looking for 1 past the string — no virtual 1 bits exist
      assert -1 == Bitmap.handle("BITPOS", ["short", "1", "5"], store)
    end

    test "BITPOS 1 with start beyond known cold size avoids loading value" do
      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 1 end,
        get: fn _key ->
          flunk("BITPOS should not load a cold value when metadata proves the result")
        end
      }

      assert -1 == Bitmap.handle("BITPOS", ["cold", "1", "5"], store)
    end

    test "BITPOS 0 with explicit end beyond string length returns -1" do
      # With explicit end range, no virtual bits — return -1
      store = MockStore.make(%{"allones" => {<<0xFF, 0xFF>>, 0}})
      assert -1 == Bitmap.handle("BITPOS", ["allones", "0", "5", "10"], store)
    end

    test "BITPOS with explicit byte range past known cold size avoids loading value" do
      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 2 end,
        get: fn _key ->
          flunk("BITPOS should not load a cold value when metadata proves the result")
        end
      }

      assert -1 == Bitmap.handle("BITPOS", ["cold", "0", "5", "10"], store)
    end

    test "BITPOS with explicit bit range past known cold size avoids loading value" do
      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 2 end,
        get: fn _key ->
          flunk("BITPOS should not load a cold value when metadata proves the result")
        end
      }

      assert -1 == Bitmap.handle("BITPOS", ["cold", "0", "16", "24", "BIT"], store)
    end

    test "BITOP result can be read with GETBIT" do
      store =
        MockStore.make(%{
          "a" => {<<0xF0>>, 0},
          "b" => {<<0x0F>>, 0}
        })

      Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
      # dest should be 0xFF — all bits set
      assert 1 == Bitmap.handle("GETBIT", ["dest", "0"], store)
      assert 1 == Bitmap.handle("GETBIT", ["dest", "7"], store)
    end
  end
    end
  end
end
