defmodule Ferricstore.Commands.BitmapTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bitmap
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Commands.Set
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.MockStore

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  # ---------------------------------------------------------------------------
  # SETBIT
  # ---------------------------------------------------------------------------

  describe "SETBIT" do
    test "on non-existent key creates string and returns 0" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "7", "1"], store)
      # Bit 7 is the LSB of byte 0 => byte value should be 1
      assert <<1>> == store.get.("mykey")
    end

    test "treats a fully expired hash as a missing bitmap key before TYPE cleanup" do
      store = MockStore.make()
      Hash.handle("HSET", ["mykey", "field", "value"], store)
      field_key = CompoundKey.hash_field("mykey", "field")
      store.compound_put.("mykey", field_key, "value", System.os_time(:millisecond) - 1)

      assert 0 == Bitmap.handle("SETBIT", ["mykey", "0", "1"], store)
      assert <<128>> == store.get.("mykey")
    end

    test "returns old bit value when overwriting" do
      # Set bit 7 first (LSB of byte 0 => byte value 1)
      store = MockStore.make(%{"mykey" => {<<1>>, 0}})
      # Now set bit 7 to 0 — old value should be 1
      assert 1 == Bitmap.handle("SETBIT", ["mykey", "7", "0"], store)
      assert <<0>> == store.get.("mykey")
    end

    test "preserves existing TTL" do
      future = System.os_time(:millisecond) + 60_000
      store = MockStore.make(%{"mykey" => {<<0>>, future}})

      assert 0 == Bitmap.handle("SETBIT", ["mykey", "0", "1"], store)
      assert {<<128>>, ^future} = store.get_meta.("mykey")
    end

    test "returns 0 when setting a bit that was already 0" do
      store = MockStore.make(%{"mykey" => {<<0>>, 0}})
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "0", "1"], store)
      # Bit 0 is MSB of byte 0 => byte value should be 128
      assert <<128>> == store.get.("mykey")
    end

    test "setting same bit twice returns 1 on second call" do
      store = MockStore.make(%{"mykey" => {<<0>>, 0}})
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "0", "1"], store)
      # Bit is now set, so setting it again should return old value = 1
      assert 1 == Bitmap.handle("SETBIT", ["mykey", "0", "1"], store)
    end

    test "at offset 0 sets MSB of byte 0" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "0", "1"], store)
      # Bit 0 = MSB of byte 0 = 0b10000000 = 128
      assert <<128>> == store.get.("mykey")
    end

    test "at offset 7 sets LSB of byte 0" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "7", "1"], store)
      # Bit 7 = LSB of byte 0 = 0b00000001 = 1
      assert <<1>> == store.get.("mykey")
    end

    test "at offset 8 sets MSB of byte 1" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "8", "1"], store)
      # Should create 2 bytes: byte 0 = 0, byte 1 = 128
      assert <<0, 128>> == store.get.("mykey")
    end

    test "auto-extends string with zero bytes" do
      store = MockStore.make()
      # Bit 23 = LSB of byte 2
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "23", "1"], store)
      assert <<0, 0, 1>> == store.get.("mykey")
    end

    test "preserves existing bits when extending" do
      store = MockStore.make(%{"mykey" => {<<255>>, 0}})
      assert 0 == Bitmap.handle("SETBIT", ["mykey", "15", "1"], store)
      assert <<255, 1>> == store.get.("mykey")
    end

    test "clearing a bit with value 0" do
      store = MockStore.make(%{"mykey" => {<<255>>, 0}})
      # Clear bit 0 (MSB of byte 0): 0xFF -> 0x7F
      assert 1 == Bitmap.handle("SETBIT", ["mykey", "0", "0"], store)
      assert <<0x7F>> == store.get.("mykey")
    end

    test "error with non-0/1 value" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("SETBIT", ["mykey", "0", "2"], store)
    end

    test "error with negative offset" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("SETBIT", ["mykey", "-1", "1"], store)
    end

    test "error with non-integer offset" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("SETBIT", ["mykey", "abc", "1"], store)
    end

    test "error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("SETBIT", ["mykey", "0"], store)
      assert {:error, _} = Bitmap.handle("SETBIT", ["mykey"], store)
      assert {:error, _} = Bitmap.handle("SETBIT", [], store)
    end
  end

  # ---------------------------------------------------------------------------
  # GETBIT
  # ---------------------------------------------------------------------------

  describe "GETBIT" do
    test "on non-existent key returns 0" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("GETBIT", ["missing", "0"], store)
    end

    test "beyond string length returns 0" do
      store = MockStore.make(%{"mykey" => {<<255>>, 0}})
      assert 0 == Bitmap.handle("GETBIT", ["mykey", "8"], store)
      assert 0 == Bitmap.handle("GETBIT", ["mykey", "100"], store)
    end

    test "beyond known cold value size returns 0 without loading value" do
      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 1 end,
        get: fn _key -> flunk("GETBIT should not load a cold value for out-of-range offset") end
      }

      assert 0 == Bitmap.handle("GETBIT", ["cold", "8"], store)
    end

    test "in-range known cold value reads only the target byte" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 2 end,
        getrange: fn "cold", 1, 1 ->
          send(test_pid, :range_reader_called)
          <<0b0100_0000>>
        end,
        get: fn _key -> flunk("GETBIT should read only the target byte") end
      }

      assert 1 == Bitmap.handle("GETBIT", ["cold", "9"], store)
      assert_received :range_reader_called
    end

    test "AST in-range known cold value reads only the target byte" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 2 end,
        getrange: fn "cold", 1, 1 ->
          send(test_pid, :range_reader_called)
          <<0b0100_0000>>
        end,
        get: fn _key -> flunk("GETBIT AST should read only the target byte") end
      }

      assert 1 == Bitmap.handle_ast({:getbit, "cold", 9}, store)
      assert_received :range_reader_called
    end

    test "returns correct bit at offset 0 (MSB)" do
      store = MockStore.make(%{"mykey" => {<<128>>, 0}})
      assert 1 == Bitmap.handle("GETBIT", ["mykey", "0"], store)
    end

    test "returns correct bit at offset 7 (LSB)" do
      store = MockStore.make(%{"mykey" => {<<1>>, 0}})
      assert 1 == Bitmap.handle("GETBIT", ["mykey", "7"], store)
    end

    test "returns 0 for unset bit" do
      store = MockStore.make(%{"mykey" => {<<128>>, 0}})
      assert 0 == Bitmap.handle("GETBIT", ["mykey", "1"], store)
    end

    test "reads bits across multiple bytes" do
      # Byte 0 = 0xFF (all 1s), Byte 1 = 0x00 (all 0s)
      store = MockStore.make(%{"mykey" => {<<255, 0>>, 0}})
      assert 1 == Bitmap.handle("GETBIT", ["mykey", "0"], store)
      assert 1 == Bitmap.handle("GETBIT", ["mykey", "7"], store)
      assert 0 == Bitmap.handle("GETBIT", ["mykey", "8"], store)
      assert 0 == Bitmap.handle("GETBIT", ["mykey", "15"], store)
    end

    test "roundtrip with SETBIT" do
      store = MockStore.make()
      Bitmap.handle("SETBIT", ["mykey", "13", "1"], store)
      assert 1 == Bitmap.handle("GETBIT", ["mykey", "13"], store)
      assert 0 == Bitmap.handle("GETBIT", ["mykey", "12"], store)
      assert 0 == Bitmap.handle("GETBIT", ["mykey", "14"], store)
    end

    test "error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("GETBIT", ["mykey"], store)
      assert {:error, _} = Bitmap.handle("GETBIT", [], store)
    end

    test "error with negative offset" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("GETBIT", ["mykey", "-1"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # BITCOUNT
  # ---------------------------------------------------------------------------

  describe "BITCOUNT" do
    test "on empty/non-existent key returns 0" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("BITCOUNT", ["missing"], store)
    end

    test "on zero-byte string returns 0" do
      store = MockStore.make(%{"mykey" => {<<0, 0, 0>>, 0}})
      assert 0 == Bitmap.handle("BITCOUNT", ["mykey"], store)
    end

    test "counts all set bits in string" do
      # 0xFF = 8 bits, 0x0F = 4 bits -> total 12
      store = MockStore.make(%{"mykey" => {<<0xFF, 0x0F>>, 0}})
      assert 12 == Bitmap.handle("BITCOUNT", ["mykey"], store)
    end

    test "known cold value counts all bits in bounded range chunks" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 4 end,
        getrange: fn
          "cold", 0, 3 ->
            send(test_pid, {:range_reader_called, 0, 3})
            <<0xFF, 0x00, 0x0F, 0xF0>>
        end,
        get: fn _key -> flunk("BITCOUNT should not load the full cold value") end
      }

      assert 16 == Bitmap.handle("BITCOUNT", ["cold"], store)
      assert_received {:range_reader_called, 0, 3}
    end

    test "with byte range" do
      # Three bytes: 0xFF (8 bits), 0x00 (0 bits), 0xFF (8 bits)
      store = MockStore.make(%{"mykey" => {<<0xFF, 0x00, 0xFF>>, 0}})
      # Only count byte 0
      assert 8 == Bitmap.handle("BITCOUNT", ["mykey", "0", "0"], store)
      # Count bytes 0-1
      assert 8 == Bitmap.handle("BITCOUNT", ["mykey", "0", "1"], store)
      # Count bytes 1-2
      assert 8 == Bitmap.handle("BITCOUNT", ["mykey", "1", "2"], store)
      # Count all bytes
      assert 16 == Bitmap.handle("BITCOUNT", ["mykey", "0", "2"], store)
    end

    test "with negative byte indices" do
      # 0xFF (8 bits), 0x00 (0 bits), 0x0F (4 bits)
      store = MockStore.make(%{"mykey" => {<<0xFF, 0x00, 0x0F>>, 0}})
      # -1 = last byte (0x0F)
      assert 4 == Bitmap.handle("BITCOUNT", ["mykey", "-1", "-1"], store)
      # -2 = second byte (0x00), -1 = last byte (0x0F)
      assert 4 == Bitmap.handle("BITCOUNT", ["mykey", "-2", "-1"], store)
      # -3 = first byte (0xFF)
      assert 12 == Bitmap.handle("BITCOUNT", ["mykey", "-3", "-1"], store)
    end

    test "with BIT mode" do
      # 0xFF = 11111111
      store = MockStore.make(%{"mykey" => {<<0xFF>>, 0}})
      # Count bits 0-3 (first 4 bits)
      assert 4 == Bitmap.handle("BITCOUNT", ["mykey", "0", "3", "BIT"], store)
      # Count bits 0-7 (all 8 bits)
      assert 8 == Bitmap.handle("BITCOUNT", ["mykey", "0", "7", "BIT"], store)
    end

    test "with BIT mode and mixed values" do
      # 0xAA = 10101010
      store = MockStore.make(%{"mykey" => {<<0xAA>>, 0}})
      # Bits: 1,0,1,0,1,0,1,0
      # Bits 0-3: 1,0,1,0 -> 2
      assert 2 == Bitmap.handle("BITCOUNT", ["mykey", "0", "3", "BIT"], store)
      # Bits 1-4: 0,1,0,1 -> 2
      assert 2 == Bitmap.handle("BITCOUNT", ["mykey", "1", "4", "BIT"], store)
    end

    test "with BYTE mode explicit" do
      store = MockStore.make(%{"mykey" => {<<0xFF, 0x00>>, 0}})
      assert 8 == Bitmap.handle("BITCOUNT", ["mykey", "0", "0", "BYTE"], store)
    end

    test "out-of-range byte indices return 0" do
      store = MockStore.make(%{"mykey" => {<<0xFF>>, 0}})
      assert 0 == Bitmap.handle("BITCOUNT", ["mykey", "5", "10"], store)
    end

    test "out-of-range cold value indices return 0 without loading value" do
      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 1 end,
        get: fn _key -> flunk("BITCOUNT should not load a cold value for out-of-range range") end
      }

      assert 0 == Bitmap.handle("BITCOUNT", ["cold", "1", "10"], store)
      assert 0 == Bitmap.handle("BITCOUNT", ["cold", "8", "80", "BIT"], store)
    end

    test "in-range cold byte range counts only the requested byte slice" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 4 end,
        getrange: fn "cold", 1, 2 ->
          send(test_pid, {:range_reader_called, 1, 2})
          <<0x0F, 0xF0>>
        end,
        get: fn _key -> flunk("BITCOUNT should read only the requested cold byte range") end
      }

      assert 8 == Bitmap.handle("BITCOUNT", ["cold", "1", "2"], store)
      assert_received {:range_reader_called, 1, 2}
    end

    test "in-range cold bit range counts only covering bytes" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 4 end,
        getrange: fn "cold", 1, 2 ->
          send(test_pid, {:range_reader_called, 1, 2})
          <<0b1111_0000, 0b1111_0000>>
        end,
        get: fn _key -> flunk("BITCOUNT BIT should read only the covering cold bytes") end
      }

      assert 4 == Bitmap.handle("BITCOUNT", ["cold", "12", "19", "BIT"], store)
      assert_received {:range_reader_called, 1, 2}
    end

    test "reversed range (start > end) returns 0" do
      store = MockStore.make(%{"mykey" => {<<0xFF, 0xFF>>, 0}})
      assert 0 == Bitmap.handle("BITCOUNT", ["mykey", "1", "0"], store)
    end

    test "error with only start (no end)" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("BITCOUNT", ["mykey", "0"], store)
    end

    test "error with no arguments" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("BITCOUNT", [], store)
    end
  end

  # ---------------------------------------------------------------------------
  # BITPOS
  # ---------------------------------------------------------------------------

  describe "BITPOS" do
    test "find first 1 bit" do
      # 0x00 0xFF = 00000000 11111111
      store = MockStore.make(%{"mykey" => {<<0x00, 0xFF>>, 0}})
      assert 8 == Bitmap.handle("BITPOS", ["mykey", "1"], store)
    end

    test "find first 0 bit" do
      # 0xFF 0x00 = 11111111 00000000
      store = MockStore.make(%{"mykey" => {<<0xFF, 0x00>>, 0}})
      assert 8 == Bitmap.handle("BITPOS", ["mykey", "0"], store)
    end

    test "find first 1 bit in all-zero string returns -1" do
      store = MockStore.make(%{"mykey" => {<<0, 0, 0>>, 0}})
      assert -1 == Bitmap.handle("BITPOS", ["mykey", "1"], store)
    end

    test "known cold value finds first 1 bit with bounded range chunks" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 4 end,
        getrange: fn
          "cold", 0, 3 ->
            send(test_pid, {:range_reader_called, 0, 3})
            <<0x00, 0x00, 0x08, 0xFF>>
        end,
        get: fn _key -> flunk("BITPOS should not load the full cold value") end
      }

      assert 20 == Bitmap.handle("BITPOS", ["cold", "1"], store)
      assert_received {:range_reader_called, 0, 3}
    end

    test "find first 0 bit in all-ones string returns position past end" do
      # Redis returns position just past the end when looking for 0 in all-1s
      store = MockStore.make(%{"mykey" => {<<0xFF, 0xFF>>, 0}})
      assert 16 == Bitmap.handle("BITPOS", ["mykey", "0"], store)
    end

    test "known all-ones cold value finds virtual zero without loading full value" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 2 end,
        getrange: fn
          "cold", 0, 1 ->
            send(test_pid, {:range_reader_called, 0, 1})
            <<0xFF, 0xFF>>
        end,
        get: fn _key -> flunk("BITPOS 0 should not load the full cold value") end
      }

      assert 16 == Bitmap.handle("BITPOS", ["cold", "0"], store)
      assert_received {:range_reader_called, 0, 1}
    end

    test "find first 1 bit at offset 0" do
      store = MockStore.make(%{"mykey" => {<<0x80>>, 0}})
      assert 0 == Bitmap.handle("BITPOS", ["mykey", "1"], store)
    end

    test "with byte range start" do
      # Byte 0 = 0x00, Byte 1 = 0xFF
      store = MockStore.make(%{"mykey" => {<<0x00, 0xFF>>, 0}})
      # Start scanning from byte 1
      assert 8 == Bitmap.handle("BITPOS", ["mykey", "1", "1"], store)
    end

    test "known cold byte range start scans large tail in bounded chunks" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 70_000 end,
        getrange: fn
          "cold", 0, 65_535 ->
            send(test_pid, {:range_reader_called, 0, 65_535})
            :binary.copy(<<0x00>>, 65_536)

          "cold", 65_536, 69_999 ->
            send(test_pid, {:range_reader_called, 65_536, 69_999})
            <<0x40, :binary.copy(<<0x00>>, 4_463)::binary>>

          "cold", 0, 69_999 ->
            flunk("BITPOS should not read the full cold tail in one range")
        end,
        get: fn _key -> flunk("BITPOS should not load the full cold value") end
      }

      assert 524_289 == Bitmap.handle("BITPOS", ["cold", "1", "0"], store)
      assert_received {:range_reader_called, 0, 65_535}
      assert_received {:range_reader_called, 65_536, 69_999}
    end

    test "with byte range start and end" do
      # Byte 0 = 0x00, Byte 1 = 0x00, Byte 2 = 0xFF
      store = MockStore.make(%{"mykey" => {<<0x00, 0x00, 0xFF>>, 0}})
      # Scan bytes 0-1 only — no 1 bit found
      assert -1 == Bitmap.handle("BITPOS", ["mykey", "1", "0", "1"], store)
      # Scan bytes 0-2 — found at bit 16
      assert 16 == Bitmap.handle("BITPOS", ["mykey", "1", "0", "2"], store)
    end

    test "in-range cold byte range scans only the requested byte slice" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 4 end,
        getrange: fn "cold", 1, 2 ->
          send(test_pid, {:range_reader_called, 1, 2})
          <<0x00, 0x08>>
        end,
        get: fn _key -> flunk("BITPOS should read only the requested cold byte range") end
      }

      assert 20 == Bitmap.handle("BITPOS", ["cold", "1", "1", "2"], store)
      assert_received {:range_reader_called, 1, 2}
    end

    test "with BIT mode range" do
      # 0xF0 = 11110000
      store = MockStore.make(%{"mykey" => {<<0xF0>>, 0}})
      # Find first 0 in bit range 0-7 => bit 4
      assert 4 == Bitmap.handle("BITPOS", ["mykey", "0", "0", "7", "BIT"], store)
      # Find first 1 in bit range 4-7 => none (bits 4-7 are 0)
      assert -1 == Bitmap.handle("BITPOS", ["mykey", "1", "4", "7", "BIT"], store)
      # Find first 1 in bit range 0-3 => bit 0
      assert 0 == Bitmap.handle("BITPOS", ["mykey", "1", "0", "3", "BIT"], store)
    end

    test "in-range cold bit range scans only covering bytes" do
      test_pid = self()

      store = %{
        compound_get: fn "cold", _compound_key -> nil end,
        value_size: fn "cold" -> 4 end,
        getrange: fn "cold", 1, 2 ->
          send(test_pid, {:range_reader_called, 1, 2})
          <<0b0000_0000, 0b0001_0000>>
        end,
        get: fn _key -> flunk("BITPOS BIT should read only the covering cold bytes") end
      }

      assert 19 == Bitmap.handle("BITPOS", ["cold", "1", "12", "20", "BIT"], store)
      assert_received {:range_reader_called, 1, 2}
    end

    test "on non-existent key looking for 1 returns -1" do
      store = MockStore.make()
      assert -1 == Bitmap.handle("BITPOS", ["missing", "1"], store)
    end

    test "on non-existent key looking for 0 returns 0" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("BITPOS", ["missing", "0"], store)
    end

    test "with negative byte range" do
      # 0x00 0x00 0xFF
      store = MockStore.make(%{"mykey" => {<<0x00, 0x00, 0xFF>>, 0}})
      # -1 = last byte (0xFF)
      assert 16 == Bitmap.handle("BITPOS", ["mykey", "1", "-1"], store)
    end

    test "error with wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Bitmap.handle("BITPOS", [], store)
      assert {:error, _} = Bitmap.handle("BITPOS", ["mykey"], store)
    end
  end

  # ---------------------------------------------------------------------------
  # BITOP
  # ---------------------------------------------------------------------------

  describe "BITOP AND" do
    test "two keys with same length" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF, 0x0F>>, 0},
          "b" => {<<0x0F, 0xFF>>, 0}
        })

      assert 2 == Bitmap.handle("BITOP", ["AND", "dest", "a", "b"], store)
      assert <<0x0F, 0x0F>> == store.get.("dest")
    end

    test "three keys" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF>>, 0},
          "b" => {<<0x0F>>, 0},
          "c" => {<<0x03>>, 0}
        })

      assert 1 == Bitmap.handle("BITOP", ["AND", "dest", "a", "b", "c"], store)
      assert <<0x03>> == store.get.("dest")
    end

    test "with different-length strings (zero padding)" do
      store =
        MockStore.make(%{
          "a" => {<<0xFF, 0xFF>>, 0},
          "b" => {<<0xFF>>, 0}
        })

      # b is padded with 0x00 -> AND with 0xFF gives 0x00 for byte 1
      assert 2 == Bitmap.handle("BITOP", ["AND", "dest", "a", "b"], store)
      assert <<0xFF, 0x00>> == store.get.("dest")
    end
  end

  describe "BITOP OR" do
    test "two keys" do
      store =
        MockStore.make(%{
          "a" => {<<0xF0, 0x00>>, 0},
          "b" => {<<0x0F, 0x00>>, 0}
        })

      assert 2 == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
      assert <<0xFF, 0x00>> == store.get.("dest")
    end

    test "with different-length strings" do
      store =
        MockStore.make(%{
          "a" => {<<0xF0>>, 0},
          "b" => {<<0x0F, 0xAA>>, 0}
        })

      # a is padded: <<0xF0, 0x00>>, b: <<0x0F, 0xAA>>
      # OR: <<0xFF, 0xAA>>
      assert 2 == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
      assert <<0xFF, 0xAA>> == store.get.("dest")
    end

    test "overwrites compound metadata on the destination" do
      store =
        MockStore.make(%{
          "a" => {<<0xF0>>, 0},
          "b" => {<<0x0F>>, 0}
        })

      Set.handle("SADD", ["dest", "old-member"], store)

      assert 1 == Bitmap.handle("BITOP", ["OR", "dest", "a", "b"], store)
      assert <<0xFF>> == store.get.("dest")
      assert store.compound_get.("dest", CompoundKey.type_key("dest")) == nil
      assert {:error, message} = Set.handle("SMEMBERS", ["dest"], store)
      assert message =~ "WRONGTYPE"
    end
  end

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

    test "with all non-existent source keys" do
      store = MockStore.make()
      assert 0 == Bitmap.handle("BITOP", ["OR", "dest", "missing1", "missing2"], store)
      assert <<>> == store.get.("dest")
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

  # ---------------------------------------------------------------------------
  # Cross-command integration
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Edge cases: arity, invalid values, error messages
  # ---------------------------------------------------------------------------

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
