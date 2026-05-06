defmodule Ferricstore.Commands.StreamTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Stream, Strings}
  alias Ferricstore.Test.MockStore

  # Each test gets a unique stream key to avoid interference.
  defp ustream, do: "stream_#{:rand.uniform(999_999)}"
  defp ids(entries), do: Enum.map(entries, &hd/1)
  defp stream_entry_key(key, id), do: "X:#{key}" <> <<0>> <> id

  defp corrupt_stream_entry(store, key, id) do
    store.compound_put.(key, stream_entry_key(key, id), <<131, 100, 0, 12, "made_up_atom">>, 0)
  end

  # Clean up ETS tables between tests to prevent state leaking.
  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups, Ferricstore.Stream.Index] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  # ===========================================================================
  # XADD
  # ===========================================================================

  describe "XADD" do
    test "XADD with auto-ID returns a valid stream ID" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "*", "field1", "val1"], store)
      assert is_binary(id)
      assert id =~ ~r/^\d+-\d+$/
    end

    test "SET overwrites a stream and removes stale stream entries" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "1-0", "field", "value"], store)
      assert [[^id, "field", "value"]] = Stream.handle("XRANGE", [key, "-", "+"], store)

      assert :ok == Strings.handle("SET", [key, "string"], store)
      assert "string" == Strings.handle("GET", [key], store)
      assert {:error, "WRONGTYPE" <> _} = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert [] == store.compound_scan.(key, "X:" <> key <> <<0>>)
    end

    test "XADD with multiple field-value pairs" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "*", "f1", "v1", "f2", "v2"], store)
      assert is_binary(id)

      # Verify fields stored correctly via XRANGE.
      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert length(entries) == 1
      [entry] = entries
      assert entry == [id, "f1", "v1", "f2", "v2"]
    end

    test "XADD returns entry write errors before updating metadata" do
      key = ustream()
      base = MockStore.make()

      store =
        Map.put(base, :compound_put, fn
          ^key, "X:" <> _rest, _encoded, 0 ->
            {:error, :disk_full}

          redis_key, compound_key, value, expire_at_ms ->
            base.compound_put.(redis_key, compound_key, value, expire_at_ms)
        end)

      assert {:error, :disk_full} = Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      assert 0 == Stream.handle("XLEN", [key], store)
    end

    test "XRANGE skips corrupt persisted stream entries" do
      store = MockStore.make()
      key = ustream()

      bad_id = Stream.handle("XADD", [key, "1-0", "bad", "value"], store)
      good_id = Stream.handle("XADD", [key, "2-0", "good", "value"], store)
      corrupt_stream_entry(store, key, bad_id)

      assert [[^good_id, "good", "value"]] = Stream.handle("XRANGE", [key, "-", "+"], store)
    end

    test "XINFO STREAM treats corrupt first/last entries as missing entries" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "1-0", "bad", "value"], store)
      corrupt_stream_entry(store, key, id)

      info = Stream.handle("XINFO", ["STREAM", key], store)

      assert info["first-entry"] == nil
      assert info["last-entry"] == nil
    end

    test "XADD auto-IDs are monotonically increasing" do
      store = MockStore.make()
      key = ustream()

      id1 = Stream.handle("XADD", [key, "*", "a", "1"], store)
      id2 = Stream.handle("XADD", [key, "*", "b", "2"], store)
      id3 = Stream.handle("XADD", [key, "*", "c", "3"], store)

      [ms1, seq1] = id1 |> String.split("-") |> Enum.map(&String.to_integer/1)
      [ms2, seq2] = id2 |> String.split("-") |> Enum.map(&String.to_integer/1)
      [ms3, seq3] = id3 |> String.split("-") |> Enum.map(&String.to_integer/1)

      assert {ms1, seq1} < {ms2, seq2}
      assert {ms2, seq2} < {ms3, seq3}
    end

    test "XADD with explicit ID" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "100-0", "f", "v"], store)
      assert id == "100-0"
    end

    test "XADD with malformed explicit ID returns error" do
      store = MockStore.make()
      key = ustream()

      for bad_id <- ["abc-def", "abc", "123-abc", "1-2-3"] do
        assert {:error, msg} = Stream.handle("XADD", [key, bad_id, "f", "v"], store)
        assert msg =~ "Invalid stream ID"
      end
    end

    test "XADD with explicit ID must be greater than last" do
      store = MockStore.make()
      key = ustream()

      _id1 = Stream.handle("XADD", [key, "100-0", "f", "v"], store)
      result = Stream.handle("XADD", [key, "50-0", "f", "v"], store)
      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end

    test "XADD with equal explicit ID fails" do
      store = MockStore.make()
      key = ustream()

      _id1 = Stream.handle("XADD", [key, "100-0", "f", "v"], store)
      result = Stream.handle("XADD", [key, "100-0", "g", "w"], store)
      assert {:error, msg} = result
      assert msg =~ "equal or smaller"
    end

    test "XADD with partial ID (ms only)" do
      store = MockStore.make()
      key = ustream()

      id = Stream.handle("XADD", [key, "200", "f", "v"], store)
      assert id == "200-0"

      # Same ms -> seq increments.
      id2 = Stream.handle("XADD", [key, "200", "g", "w"], store)
      assert id2 == "200-1"
    end

    test "XADD wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XADD", ["key", "*"], store)
      assert {:error, _} = Stream.handle("XADD", ["key"], store)
      assert {:error, _} = Stream.handle("XADD", [], store)
    end

    test "XADD with odd number of field-value pairs returns error" do
      store = MockStore.make()
      key = ustream()
      assert {:error, _} = Stream.handle("XADD", [key, "*", "f1", "v1", "f2"], store)
    end

    test "XADD NOMKSTREAM returns nil when stream does not exist" do
      store = MockStore.make()
      key = ustream()

      result = Stream.handle("XADD", [key, "NOMKSTREAM", "*", "f", "v"], store)
      assert result == nil
    end

    test "XADD NOMKSTREAM works when stream exists" do
      store = MockStore.make()
      key = ustream()

      # Create the stream first.
      _id1 = Stream.handle("XADD", [key, "*", "f", "v"], store)

      id2 = Stream.handle("XADD", [key, "NOMKSTREAM", "*", "g", "w"], store)
      assert is_binary(id2)
    end

    test "XADD with MAXLEN trims oldest entries" do
      store = MockStore.make()
      key = ustream()

      _id1 = Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      _id2 = Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      _id3 = Stream.handle("XADD", [key, "MAXLEN", "2", "3-0", "c", "3"], store)

      # Only 2 entries should remain.
      assert Stream.handle("XLEN", [key], store) == 2
      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert length(entries) == 2
      # Oldest should be trimmed.
      ids = Enum.map(entries, &hd/1)
      assert "2-0" in ids
      assert "3-0" in ids
    end

    test "XADD with MAXLEN ~ (approximate) trims" do
      store = MockStore.make()
      key = ustream()

      _id1 = Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      _id2 = Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      _id3 = Stream.handle("XADD", [key, "MAXLEN", "~", "2", "3-0", "c", "3"], store)

      assert Stream.handle("XLEN", [key], store) == 2
    end
  end

  # ===========================================================================
  # XLEN
  # ===========================================================================

  describe "XLEN" do
    test "XLEN returns 0 for nonexistent stream" do
      store = MockStore.make()
      assert 0 == Stream.handle("XLEN", ["nonexistent"], store)
    end

    test "XLEN returns WRONGTYPE for a string key" do
      store = MockStore.make(%{"plain" => {"value", 0}})

      assert {:error, msg} = Stream.handle("XLEN", ["plain"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "XLEN returns correct count after adding entries" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "*", "f", "v"], store)
      assert 1 == Stream.handle("XLEN", [key], store)

      Stream.handle("XADD", [key, "*", "g", "w"], store)
      assert 2 == Stream.handle("XLEN", [key], store)

      Stream.handle("XADD", [key, "*", "h", "x"], store)
      assert 3 == Stream.handle("XLEN", [key], store)
    end

    test "XLEN wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XLEN", [], store)
      assert {:error, _} = Stream.handle("XLEN", ["a", "b"], store)
    end
  end

  # ===========================================================================
  # XRANGE
  # ===========================================================================

  describe "XRANGE" do
    test "XRANGE - + returns all entries" do
      store = MockStore.make()
      key = ustream()

      id1 = Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      id2 = Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      id3 = Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert length(entries) == 3
      assert Enum.map(entries, &hd/1) == [id1, id2, id3]
    end

    test "XRANGE with specific start and end" do
      store = MockStore.make()
      key = ustream()

      _id1 = Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      id2 = Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      _id3 = Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      entries = Stream.handle("XRANGE", [key, "2-0", "2-0"], store)
      assert length(entries) == 1
      assert hd(hd(entries)) == id2
    end

    test "XRANGE with COUNT limits results" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      entries = Stream.handle("XRANGE", [key, "-", "+", "COUNT", "3"], store)
      assert length(entries) == 3
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0", "3-0"]
    end

    test "XRANGE uses stream prefix scan without enumerating unrelated keys" do
      parent = self()
      {:ok, pid} = Agent.start_link(fn -> %{} end)
      key = ustream()

      store = %{
        put: fn entry_key, value, _expire_at_ms ->
          Agent.update(pid, &Map.put(&1, entry_key, value))
          :ok
        end,
        keys: fn ->
          flunk("XRANGE should use compound_scan, not a full keyspace scan")
        end,
        compound_scan: fn ^key, prefix ->
          send(parent, {:compound_scan, key, prefix})

          Agent.get(pid, fn state ->
            state
            |> Enum.filter(fn {entry_key, _value} -> String.starts_with?(entry_key, prefix) end)
            |> Enum.map(fn {entry_key, value} ->
              {String.replace_prefix(entry_key, prefix, ""), value}
            end)
          end)
        end,
        get: fn entry_key ->
          flunk("XRANGE should use compound_scan, got per-entry GET for #{inspect(entry_key)}")
        end
      }

      assert "1-0" == Stream.handle("XADD", [key, "1-0", "f", "v1"], store)
      assert "2-0" == Stream.handle("XADD", [key, "2-0", "f", "v2"], store)

      assert [["1-0", "f", "v1"], ["2-0", "f", "v2"]] ==
               Stream.handle("XRANGE", [key, "-", "+"], store)

      assert_received {:compound_scan, ^key, "X:" <> _}
    end

    test "XRANGE uses ordered stream index without repeated prefix scans" do
      parent = self()
      {:ok, pid} = Agent.start_link(fn -> %{} end)
      key = ustream()

      store = %{
        put: fn entry_key, value, _expire_at_ms ->
          Agent.update(pid, &Map.put(&1, entry_key, value))
          :ok
        end,
        compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
          Agent.update(pid, &Map.put(&1, compound_key, {value, expire_at_ms}))
          :ok
        end,
        compound_scan: fn ^key, prefix ->
          send(parent, {:compound_scan, key, prefix})

          Agent.get(pid, fn state ->
            state
            |> Enum.filter(fn {entry_key, _value} -> String.starts_with?(entry_key, prefix) end)
            |> Enum.map(fn
              {entry_key, {value, _expire_at_ms}} ->
                {String.replace_prefix(entry_key, prefix, ""), value}

              {entry_key, value} ->
                {String.replace_prefix(entry_key, prefix, ""), value}
            end)
          end)
        end,
        compound_batch_get: fn _redis_key, compound_keys ->
          Agent.get(pid, fn state ->
            Enum.map(compound_keys, fn compound_key ->
              case Map.get(state, compound_key) do
                {value, _expire_at_ms} -> value
                value -> value
              end
            end)
          end)
        end,
        compound_get: fn _redis_key, compound_key ->
          Agent.get(pid, fn state ->
            case Map.get(state, compound_key) do
              {value, _expire_at_ms} -> value
              value -> value
            end
          end)
        end,
        compound_delete: fn _redis_key, compound_key ->
          Agent.update(pid, &Map.delete(&1, compound_key))
          :ok
        end,
        exists?: fn compound_key -> Agent.get(pid, &Map.has_key?(&1, compound_key)) end,
        get: fn entry_key -> Agent.get(pid, &Map.get(&1, entry_key)) end
      }

      for i <- 1..20, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      assert Enum.map(Stream.handle("XRANGE", [key, "-", "+", "COUNT", "3"], store), &hd/1) ==
               ["1-0", "2-0", "3-0"]

      refute_received {:compound_scan, ^key, "X:" <> _}

      assert Enum.map(Stream.handle("XRANGE", [key, "10-0", "+", "COUNT", "3"], store), &hd/1) ==
               ["10-0", "11-0", "12-0"]

      refute_received {:compound_scan, ^key, "X:" <> _}
    end

    test "ordered stream index tracks XADD and XDEL after warm build" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..3, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)
      assert ["1-0", "2-0", "3-0"] = ids(Stream.handle("XRANGE", [key, "-", "+"], store))

      assert 1 == Stream.handle("XDEL", [key, "2-0"], store)
      assert "4-0" == Stream.handle("XADD", [key, "4-0", "f", "4"], store)

      assert ["1-0", "3-0", "4-0"] = ids(Stream.handle("XRANGE", [key, "-", "+"], store))
      assert ["4-0", "3-0", "1-0"] = ids(Stream.handle("XREVRANGE", [key, "+", "-"], store))
    end

    test "XLEN fallback counts by stream prefix without enumerating unrelated keys" do
      parent = self()
      {:ok, pid} = Agent.start_link(fn -> %{} end)
      key = ustream()

      store = %{
        put: fn entry_key, value, _expire_at_ms ->
          Agent.update(pid, &Map.put(&1, entry_key, value))
          :ok
        end,
        keys: fn ->
          flunk("XLEN fallback should use compound_count, not a full keyspace scan")
        end,
        compound_count: fn ^key, prefix ->
          send(parent, {:compound_count, key, prefix})

          Agent.get(pid, fn state ->
            Enum.count(state, fn {entry_key, _value} ->
              String.starts_with?(entry_key, prefix)
            end)
          end)
        end
      }

      assert "1-0" == Stream.handle("XADD", [key, "1-0", "f", "v1"], store)
      assert "2-0" == Stream.handle("XADD", [key, "2-0", "f", "v2"], store)
      :ets.delete(Ferricstore.Stream.Meta, key)

      assert 2 == Stream.handle("XLEN", [key], store)
      assert_received {:compound_count, ^key, "X:" <> _}
    end

    test "XRANGE on nonexistent stream returns empty list" do
      store = MockStore.make()
      assert [] == Stream.handle("XRANGE", ["nonexistent", "-", "+"], store)
    end

    test "XRANGE returns WRONGTYPE for a string key" do
      store = MockStore.make(%{"plain" => {"value", 0}})

      assert {:error, msg} = Stream.handle("XRANGE", ["plain", "-", "+"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "XRANGE entries contain correct field-value pairs" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "name", "alice", "age", "30"], store)

      [[id | fields]] = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert id == "1-0"
      assert fields == ["name", "alice", "age", "30"]
    end

    test "XRANGE wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XRANGE", ["key", "-"], store)
      assert {:error, _} = Stream.handle("XRANGE", ["key"], store)
      assert {:error, _} = Stream.handle("XRANGE", [], store)
    end

    test "XRANGE with ms-only IDs" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      # Start from ms=2 (implies seq=0).
      entries = Stream.handle("XRANGE", [key, "2", "+"], store)
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["2-0", "3-0"]
    end
  end

  # ===========================================================================
  # XREVRANGE
  # ===========================================================================

  describe "XREVRANGE" do
    test "XREVRANGE + - returns all entries in reverse" do
      store = MockStore.make()
      key = ustream()

      id1 = Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      id2 = Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      id3 = Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      entries = Stream.handle("XREVRANGE", [key, "+", "-"], store)
      assert length(entries) == 3
      assert Enum.map(entries, &hd/1) == [id3, id2, id1]
    end

    test "XREVRANGE with COUNT limits results from end" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      entries = Stream.handle("XREVRANGE", [key, "+", "-", "COUNT", "2"], store)
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["5-0", "4-0"]
    end

    test "XREVRANGE on nonexistent stream returns empty list" do
      store = MockStore.make()
      assert [] == Stream.handle("XREVRANGE", ["nonexistent", "+", "-"], store)
    end

    test "XREVRANGE with specific range" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      entries = Stream.handle("XREVRANGE", [key, "3-0", "1-0"], store)
      assert length(entries) == 3
      assert Enum.map(entries, &hd/1) == ["3-0", "2-0", "1-0"]
    end
  end

  # ===========================================================================
  # XREAD
  # ===========================================================================

  describe "XREAD" do
    test "XREAD returns entries after given ID" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      result = Stream.handle("XREAD", ["STREAMS", key, "1-0"], store)
      assert is_list(result)
      assert length(result) == 1
      [[^key, entries]] = result
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["2-0", "3-0"]
    end

    test "XREAD with 0-0 returns all entries" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)

      result = Stream.handle("XREAD", ["STREAMS", key, "0-0"], store)
      [[^key, entries]] = result
      assert length(entries) == 2
    end

    test "XREAD with COUNT limits results" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      result = Stream.handle("XREAD", ["COUNT", "2", "STREAMS", key, "0"], store)
      [[^key, entries]] = result
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0"]
    end

    test "XREAD with multiple streams" do
      store = MockStore.make()
      key1 = ustream()
      key2 = ustream()

      Stream.handle("XADD", [key1, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key2, "1-0", "b", "2"], store)

      result = Stream.handle("XREAD", ["STREAMS", key1, key2, "0", "0"], store)
      assert length(result) == 2
    end

    test "XREAD returns empty list when no new entries" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)

      result = Stream.handle("XREAD", ["STREAMS", key, "1-0"], store)
      assert result == []
    end

    test "XREAD with nonexistent stream returns empty list" do
      store = MockStore.make()
      result = Stream.handle("XREAD", ["STREAMS", "nonexistent", "0"], store)
      assert result == []
    end

    test "XREAD with unbalanced streams returns error" do
      store = MockStore.make()
      result = Stream.handle("XREAD", ["STREAMS", "key1", "key2", "0"], store)
      assert {:error, msg} = result
      assert msg =~ "Unbalanced"
    end
  end

  # ===========================================================================
  # XTRIM
  # ===========================================================================

  describe "XTRIM" do
    test "XTRIM MAXLEN removes oldest entries" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)
      assert 5 == Stream.handle("XLEN", [key], store)

      deleted = Stream.handle("XTRIM", [key, "MAXLEN", "3"], store)
      assert deleted == 2
      assert 3 == Stream.handle("XLEN", [key], store)

      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert Enum.map(entries, &hd/1) == ["3-0", "4-0", "5-0"]
    end

    test "XTRIM MAXLEN returns delete errors before updating metadata" do
      base = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], base)

      store =
        Map.put(base, :compound_delete, fn ^key, "X:" <> _rest -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Stream.handle("XTRIM", [key, "MAXLEN", "3"], store)
      assert 5 == Stream.handle("XLEN", [key], base)

      entries = Stream.handle("XRANGE", [key, "-", "+"], base)
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0", "3-0", "4-0", "5-0"]
    end

    test "XTRIM MAXLEN 0 removes all entries" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..3, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      deleted = Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)
      assert deleted == 3
      assert 0 == Stream.handle("XLEN", [key], store)
    end

    test "XTRIM MAXLEN with nothing to trim returns 0" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..3, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      deleted = Stream.handle("XTRIM", [key, "MAXLEN", "10"], store)
      assert deleted == 0
    end

    test "XTRIM MINID removes entries below the given ID" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      deleted = Stream.handle("XTRIM", [key, "MINID", "3-0"], store)
      assert deleted == 2

      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert Enum.map(entries, &hd/1) == ["3-0", "4-0", "5-0"]
    end

    test "XTRIM MINID returns delete errors before updating metadata" do
      base = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], base)

      store =
        Map.put(base, :compound_delete, fn ^key, "X:" <> _rest -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Stream.handle("XTRIM", [key, "MINID", "3-0"], store)
      assert 5 == Stream.handle("XLEN", [key], base)

      entries = Stream.handle("XRANGE", [key, "-", "+"], base)
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0", "3-0", "4-0", "5-0"]
    end

    test "XTRIM on nonexistent stream returns 0" do
      store = MockStore.make()
      assert 0 == Stream.handle("XTRIM", ["nonexistent", "MAXLEN", "0"], store)
    end

    test "XTRIM wrong arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XTRIM", [], store)
    end

    test "XTRIM with approximate flag" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      deleted = Stream.handle("XTRIM", [key, "MAXLEN", "~", "3"], store)
      assert deleted == 2
    end
  end

  # ===========================================================================
  # XDEL
  # ===========================================================================

  describe "XDEL" do
    test "XDEL removes specific entries" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      deleted = Stream.handle("XDEL", [key, "2-0"], store)
      assert deleted == 1

      entries = Stream.handle("XRANGE", [key, "-", "+"], store)
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["1-0", "3-0"]
    end

    test "XDEL multiple entries" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)

      deleted = Stream.handle("XDEL", [key, "1-0", "3-0", "5-0"], store)
      assert deleted == 3
      assert 2 == Stream.handle("XLEN", [key], store)
    end

    test "XDEL nonexistent entry returns 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)

      deleted = Stream.handle("XDEL", [key, "99-0"], store)
      assert deleted == 0
    end

    test "XDEL returns entry delete errors before updating metadata" do
      base = MockStore.make()
      key = ustream()
      Stream.handle("XADD", [key, "1-0", "a", "1"], base)

      store =
        Map.put(base, :compound_delete, fn ^key, "X:" <> _rest -> {:error, :disk_full} end)

      assert {:error, :disk_full} = Stream.handle("XDEL", [key, "1-0"], store)
      assert 1 == Stream.handle("XLEN", [key], base)
      assert [[_, "a", "1"]] = Stream.handle("XRANGE", [key, "-", "+"], base)
    end

    test "XDEL mixed existing and nonexistent entries" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)

      deleted = Stream.handle("XDEL", [key, "1-0", "99-0"], store)
      assert deleted == 1
    end

    test "XDEL wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XDEL", ["key"], store)
      assert {:error, _} = Stream.handle("XDEL", [], store)
    end

    test "XDEL all entries leaves stream with length 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)

      Stream.handle("XDEL", [key, "1-0", "2-0"], store)
      assert 0 == Stream.handle("XLEN", [key], store)
    end

    test "XDEL updates metadata correctly" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XADD", [key, "3-0", "c", "3"], store)

      # Delete the first entry.
      Stream.handle("XDEL", [key, "1-0"], store)

      # XINFO should reflect the updated first entry.
      info = Stream.handle("XINFO", ["STREAM", key], store)
      assert info["length"] == 2
    end
  end

  # ===========================================================================
  # XINFO STREAM
  # ===========================================================================

  describe "XINFO STREAM" do
    test "XINFO STREAM returns stream metadata" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "name", "alice"], store)
      Stream.handle("XADD", [key, "2-0", "name", "bob"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)
      assert is_map(info)
      assert info["length"] == 2
      assert info["first-entry"] == ["1-0", "name", "alice"]
      assert info["last-entry"] == ["2-0", "name", "bob"]
      assert info["last-generated-id"] == "2-0"
      assert info["groups"] == 0
    end

    test "XINFO STREAM batches first and last entry reads" do
      parent = self()
      {:ok, pid} = Agent.start_link(fn -> %{} end)
      key = ustream()

      store = %{
        put: fn entry_key, value, _expire_at_ms ->
          Agent.update(pid, &Map.put(&1, entry_key, value))
          :ok
        end,
        keys: fn -> Agent.get(pid, &Map.keys/1) end,
        batch_get: fn keys ->
          send(parent, {:batch_get, keys})
          Agent.get(pid, fn state -> Enum.map(keys, &Map.get(state, &1)) end)
        end,
        get: fn entry_key ->
          flunk("XINFO STREAM should use batch_get, got per-entry GET for #{inspect(entry_key)}")
        end
      }

      assert "1-0" == Stream.handle("XADD", [key, "1-0", "name", "alice"], store)
      assert "2-0" == Stream.handle("XADD", [key, "2-0", "name", "bob"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)

      assert info["first-entry"] == ["1-0", "name", "alice"]
      assert info["last-entry"] == ["2-0", "name", "bob"]
      assert_received {:batch_get, [_first, _last]}
    end

    test "XINFO STREAM on nonexistent stream returns error" do
      store = MockStore.make()
      result = Stream.handle("XINFO", ["STREAM", "nonexistent"], store)
      assert {:error, "ERR no such key"} = result
    end

    test "XINFO STREAM returns WRONGTYPE for a string key" do
      store = MockStore.make(%{"plain" => {"value", 0}})

      assert {:error, msg} = Stream.handle("XINFO", ["STREAM", "plain"], store)
      assert msg =~ "WRONGTYPE"
    end

    test "XINFO STREAM with consumer groups shows group count" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g2", "0"], store)

      info = Stream.handle("XINFO", ["STREAM", key], store)
      assert info["groups"] == 2
    end

    test "XINFO wrong arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XINFO", [], store)
      assert {:error, _} = Stream.handle("XINFO", ["BADSUBCMD"], store)
    end
  end

  # ===========================================================================
  # XGROUP CREATE
  # ===========================================================================

  describe "XGROUP CREATE" do
    test "XGROUP CREATE on existing stream" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)

      result = Stream.handle("XGROUP", ["CREATE", key, "mygroup", "0"], store)
      assert :ok == result
    end

    test "XGROUP CREATE on nonexistent stream without MKSTREAM returns error" do
      store = MockStore.make()
      key = ustream()

      result = Stream.handle("XGROUP", ["CREATE", key, "mygroup", "0"], store)
      assert {:error, msg} = result
      assert msg =~ "requires the key to exist"
    end

    test "XGROUP CREATE with MKSTREAM creates stream" do
      store = MockStore.make()
      key = ustream()

      result = Stream.handle("XGROUP", ["CREATE", key, "mygroup", "0", "MKSTREAM"], store)
      assert :ok == result

      # Stream should exist now (even though empty).
      assert 0 == Stream.handle("XLEN", [key], store)
    end

    test "XGROUP CREATE with $ delivers only new messages" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "old", "data"], store)
      Stream.handle("XGROUP", ["CREATE", key, "mygroup", "$"], store)

      # Reading with > should return nothing (no new entries after $).
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "mygroup", "consumer1", "STREAMS", key, ">"],
          store
        )

      assert result == []
    end

    test "XGROUP wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XGROUP", ["CREATE", "key"], store)
      assert {:error, _} = Stream.handle("XGROUP", [], store)
    end
  end

  # ===========================================================================
  # XREADGROUP
  # ===========================================================================

  describe "XREADGROUP" do
    test "XREADGROUP delivers new messages to consumer" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "g1", "c1", "STREAMS", key, ">"],
          store
        )

      assert is_list(result)
      assert length(result) == 1
      [[^key, entries]] = result
      assert length(entries) == 2
      assert Enum.map(entries, &hd/1) == ["1-0", "2-0"]
    end

    test "XREADGROUP moves last_delivered_id forward" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      # First read delivers entry 1-0.
      Stream.handle(
        "XREADGROUP",
        ["GROUP", "g1", "c1", "STREAMS", key, ">"],
        store
      )

      # Add another entry.
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)

      # Second read should only deliver 2-0.
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "g1", "c1", "STREAMS", key, ">"],
          store
        )

      [[^key, entries]] = result
      assert length(entries) == 1
      assert hd(hd(entries)) == "2-0"
    end

    test "XREADGROUP with COUNT limits results" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "g1", "c1", "COUNT", "2", "STREAMS", key, ">"],
          store
        )

      [[^key, entries]] = result
      assert length(entries) == 2
    end

    test "XREADGROUP with nonexistent group returns error" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "nogroup", "c1", "STREAMS", key, ">"],
          store
        )

      assert {:error, msg} = result
      assert msg =~ "NOGROUP"
    end

    test "XREADGROUP with 0 returns pending entries for consumer" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      # Deliver to consumer.
      Stream.handle(
        "XREADGROUP",
        ["GROUP", "g1", "c1", "STREAMS", key, ">"],
        store
      )

      # Query pending entries.
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "g1", "c1", "STREAMS", key, "0"],
          store
        )

      [[^key, entries]] = result
      assert length(entries) == 2
    end

    test "XREADGROUP returns empty when no new messages" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      # First read delivers all.
      Stream.handle(
        "XREADGROUP",
        ["GROUP", "g1", "c1", "STREAMS", key, ">"],
        store
      )

      # Second read should have nothing new.
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "g1", "c1", "STREAMS", key, ">"],
          store
        )

      assert result == []
    end
  end

  # ===========================================================================
  # XACK
  # ===========================================================================

  describe "XACK" do
    test "XACK acknowledges pending entries" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      Stream.handle(
        "XREADGROUP",
        ["GROUP", "g1", "c1", "STREAMS", key, ">"],
        store
      )

      acked = Stream.handle("XACK", [key, "g1", "1-0"], store)
      assert acked == 1
    end

    test "XACK multiple entries" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..3, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      Stream.handle(
        "XREADGROUP",
        ["GROUP", "g1", "c1", "STREAMS", key, ">"],
        store
      )

      acked = Stream.handle("XACK", [key, "g1", "1-0", "2-0", "3-0"], store)
      assert acked == 3
    end

    test "XACK already acknowledged entry returns 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      Stream.handle(
        "XREADGROUP",
        ["GROUP", "g1", "c1", "STREAMS", key, ">"],
        store
      )

      Stream.handle("XACK", [key, "g1", "1-0"], store)
      acked = Stream.handle("XACK", [key, "g1", "1-0"], store)
      assert acked == 0
    end

    test "XACK nonexistent group returns 0" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)

      acked = Stream.handle("XACK", [key, "nogroup", "1-0"], store)
      assert acked == 0
    end

    test "XACK removes entries from pending list" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "a", "1"], store)
      Stream.handle("XADD", [key, "2-0", "b", "2"], store)
      Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], store)

      Stream.handle(
        "XREADGROUP",
        ["GROUP", "g1", "c1", "STREAMS", key, ">"],
        store
      )

      # Ack one entry.
      Stream.handle("XACK", [key, "g1", "1-0"], store)

      # Query pending -- should only show 2-0.
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "g1", "c1", "STREAMS", key, "0"],
          store
        )

      [[^key, entries]] = result
      assert length(entries) == 1
      assert hd(hd(entries)) == "2-0"
    end

    test "XACK wrong number of arguments" do
      store = MockStore.make()
      assert {:error, _} = Stream.handle("XACK", ["key", "group"], store)
      assert {:error, _} = Stream.handle("XACK", ["key"], store)
      assert {:error, _} = Stream.handle("XACK", [], store)
    end
  end

  # ===========================================================================
  # Dispatcher routing
  # ===========================================================================

  describe "Dispatcher routes stream commands" do
    alias Ferricstore.Commands.Dispatcher

    test "XADD dispatched through Dispatcher" do
      store = MockStore.make()
      key = ustream()

      id = Dispatcher.dispatch("XADD", [key, "*", "f", "v"], store)
      assert is_binary(id)
      assert id =~ ~r/^\d+-\d+$/
    end

    test "XLEN dispatched through Dispatcher" do
      store = MockStore.make()
      key = ustream()

      Dispatcher.dispatch("XADD", [key, "*", "f", "v"], store)
      assert 1 == Dispatcher.dispatch("XLEN", [key], store)
    end

    test "XRANGE dispatched through Dispatcher" do
      store = MockStore.make()
      key = ustream()

      Dispatcher.dispatch("XADD", [key, "1-0", "f", "v"], store)
      entries = Dispatcher.dispatch("XRANGE", [key, "-", "+"], store)
      assert length(entries) == 1
    end

    test "stream commands are case-insensitive" do
      store = MockStore.make()
      key = ustream()

      id = Dispatcher.dispatch("xadd", [key, "*", "f", "v"], store)
      assert is_binary(id)

      len = Dispatcher.dispatch("xlen", [key], store)
      assert len == 1
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

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
