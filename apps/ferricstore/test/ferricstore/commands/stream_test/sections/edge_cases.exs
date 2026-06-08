defmodule Ferricstore.Commands.StreamTest.Sections.EdgeCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Expiry, Stream, Strings}
      alias Ferricstore.Test.MockStore

      describe "edge cases" do
        test "multiple streams are independent" do
          store = MockStore.make()
          key1 = ustream()
          key2 = ustream()

          Stream.handle("XADD", [key1, "1-0", "a", "1"], store)
          Stream.handle("XADD", [key2, "1-0", "b", "2"], store)

          assert 1 == Stream.handle("XLEN", [key1], store)
          assert 1 == Stream.handle("XLEN", [key2], store)

          entries1 = Stream.handle("XRANGE", [key1, "-", "+"], store)
          entries2 = Stream.handle("XRANGE", [key2, "-", "+"], store)

          assert hd(hd(entries1)) == "1-0"
          assert hd(hd(entries2)) == "1-0"

          # Different field values.
          [_id1 | fields1] = hd(entries1)
          [_id2 | fields2] = hd(entries2)
          assert fields1 == ["a", "1"]
          assert fields2 == ["b", "2"]
        end

        test "XADD then XDEL then XADD maintains monotonic IDs" do
          store = MockStore.make()
          key = ustream()

          id1 = Stream.handle("XADD", [key, "100-0", "a", "1"], store)
          assert id1 == "100-0"

          Stream.handle("XDEL", [key, "100-0"], store)

          # Must still reject IDs <= 100-0.
          result = Stream.handle("XADD", [key, "50-0", "b", "2"], store)
          assert {:error, _} = result

          # New entry must be > 100-0.
          id2 = Stream.handle("XADD", [key, "101-0", "c", "3"], store)
          assert id2 == "101-0"
        end

        test "XADD with many entries and XRANGE COUNT pagination" do
          store = MockStore.make()
          key = ustream()

          for i <- 1..20, do: Stream.handle("XADD", [key, "#{i}-0", "n", "#{i}"], store)

          assert 20 == Stream.handle("XLEN", [key], store)

          page1 = Stream.handle("XRANGE", [key, "-", "+", "COUNT", "5"], store)
          assert length(page1) == 5
          assert hd(hd(page1)) == "1-0"
          last_id = hd(List.last(page1))
          assert last_id == "5-0"

          # Next page starts after last_id.
          page2 = Stream.handle("XRANGE", [key, "6-0", "+", "COUNT", "5"], store)
          assert length(page2) == 5
          assert hd(hd(page2)) == "6-0"
        end

        test "consumer groups with multiple consumers" do
          store = MockStore.make()
          key = ustream()

          for i <- 1..4, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)
          Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

          # Consumer 1 reads 2.
          r1 =
            Stream.handle(
              "XREADGROUP",
              ["GROUP", "g1", "c1", "COUNT", "2", "STREAMS", key, ">"],
              store
            )

          [[^key, entries1]] = r1
          assert length(entries1) == 2

          # Consumer 2 reads the remaining 2.
          r2 =
            Stream.handle(
              "XREADGROUP",
              ["GROUP", "g1", "c2", "COUNT", "2", "STREAMS", key, ">"],
              store
            )

          [[^key, entries2]] = r2
          assert length(entries2) == 2

          # All 4 entries delivered across consumers.
          all_ids = Enum.map(entries1 ++ entries2, &hd/1)
          assert Enum.sort(all_ids) == ["1-0", "2-0", "3-0", "4-0"]
        end

        test "XTRIM MAXLEN updates XINFO metadata" do
          store = MockStore.make()
          key = ustream()

          for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

          Stream.handle("XTRIM", [key, "MAXLEN", "3"], store)

          info = Stream.handle("XINFO", ["STREAM", key], store)
          assert info["length"] == 3
          assert info["first-entry"] != nil
          first_id = hd(info["first-entry"])
          assert first_id == "3-0"
        end

        test "XADD followed by XREVRANGE with COUNT 1 returns latest" do
          store = MockStore.make()
          key = ustream()

          for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

          entries = Stream.handle("XREVRANGE", [key, "+", "-", "COUNT", "1"], store)
          assert length(entries) == 1
          assert hd(hd(entries)) == "5-0"
        end

        test "XRANGE with no matching range returns empty list" do
          store = MockStore.make()
          key = ustream()

          Stream.handle("XADD", [key, "5-0", "f", "v"], store)

          entries = Stream.handle("XRANGE", [key, "10-0", "20-0"], store)
          assert entries == []
        end

        test "XRANGE with invalid start ID returns error" do
          store = MockStore.make()
          key = ustream()
          Stream.handle("XADD", [key, "1-0", "f", "v"], store)
          assert {:error, msg} = Stream.handle("XRANGE", [key, "abc", "+"], store)
          assert msg =~ "Invalid stream ID"
        end

        test "XRANGE with invalid end ID returns error" do
          store = MockStore.make()
          key = ustream()
          Stream.handle("XADD", [key, "1-0", "f", "v"], store)
          assert {:error, msg} = Stream.handle("XRANGE", [key, "-", "abc"], store)
          assert msg =~ "Invalid stream ID"
        end

        test "XRANGE with invalid COUNT value returns error" do
          store = MockStore.make()
          key = ustream()
          Stream.handle("XADD", [key, "1-0", "f", "v"], store)
          assert {:error, msg} = Stream.handle("XRANGE", [key, "-", "+", "COUNT", "abc"], store)
          assert msg =~ "not an integer"
        end

        test "XRANGE with invalid COUNT option returns error" do
          store = MockStore.make()
          key = ustream()
          Stream.handle("XADD", [key, "1-0", "f", "v"], store)
          assert {:error, msg} = Stream.handle("XRANGE", [key, "-", "+", "BOGUS"], store)
          assert msg =~ "syntax error"
        end

        test "XREVRANGE wrong number of arguments" do
          store = MockStore.make()
          assert {:error, _} = Stream.handle("XREVRANGE", ["key", "+"], store)
          assert {:error, _} = Stream.handle("XREVRANGE", ["key"], store)
          assert {:error, _} = Stream.handle("XREVRANGE", [], store)
        end

        test "XREAD without STREAMS keyword returns error" do
          store = MockStore.make()
          result = Stream.handle("XREAD", ["key1", "0"], store)
          assert {:error, msg} = result
          assert msg =~ "syntax error"
        end

        test "XREAD with no args returns error" do
          store = MockStore.make()
          result = Stream.handle("XREAD", [], store)
          assert {:error, msg} = result
          assert msg =~ "syntax error"
        end

        test "XREADGROUP without GROUP prefix returns error" do
          store = MockStore.make()
          result = Stream.handle("XREADGROUP", ["STREAMS", "key", ">"], store)
          assert {:error, msg} = result
          assert msg =~ "syntax error"
        end

        test "XTRIM with invalid strategy returns error" do
          store = MockStore.make()
          key = ustream()
          Stream.handle("XADD", [key, "1-0", "f", "v"], store)
          assert {:error, _} = Stream.handle("XTRIM", [key, "BOGUS", "5"], store)
        end

        test "XADD sequence number wraps correctly within same millisecond" do
          store = MockStore.make()
          key = ustream()

          # Add many entries at the same millisecond.
          for seq <- 0..9 do
            id = Stream.handle("XADD", [key, "1000-#{seq}", "n", "#{seq}"], store)
            assert id == "1000-#{seq}"
          end

          assert 10 == Stream.handle("XLEN", [key], store)
        end
      end
    end
  end
end
