defmodule Ferricstore.Commands.StreamTest.Sections.Xadd do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Expiry, Stream, Strings}
      alias Ferricstore.Test.MockStore

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

        test "SET notifies stream waiters only after the replacement is visible" do
          base = MockStore.make()
          key = ustream()
          parent = self()

          assert "1-0" = Stream.handle("XADD", [key, "1-0", "field", "old"], base)
          assert :ok = Stream.register_stream_waiter(key, self(), "1-0", base)

          store =
            Map.put(base, :put, fn ^key, value, expire_at_ms ->
              send(parent, {:replacement_put_waiting, self()})

              receive do
                :continue_replacement_put -> base.put.(key, value, expire_at_ms)
              end
            end)

          replacement = Task.async(fn -> Strings.handle("SET", [key, "string"], store) end)
          assert_receive {:replacement_put_waiting, put_pid}
          refute_receive {:stream_waiter_notify, ^key}

          send(put_pid, :continue_replacement_put)
          assert :ok = Task.await(replacement)
          assert_receive {:stream_waiter_notify, ^key}
          assert "string" = base.get.(key)
        end

        test "failed SET restores the stream and keeps its waiter registered" do
          base = MockStore.make()
          key = ustream()

          assert "1-0" = Stream.handle("XADD", [key, "1-0", "field", "old"], base)
          assert :ok = Stream.register_stream_waiter(key, self(), "1-0", base)

          store =
            Map.put(base, :put, fn ^key, "string", 0 ->
              {:error, :disk_full}
            end)

          assert {:error, :disk_full} = Strings.handle("SET", [key, "string"], store)
          refute_receive {:stream_waiter_notify, ^key}
          assert 1 = Stream.stream_waiter_count(key, base)
          assert [["1-0", "field", "old"]] = Stream.handle("XRANGE", [key, "-", "+"], base)
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

        test "XADD validates MINID before creating an entry" do
          store = MockStore.make()
          key = ustream()

          assert {:error, "ERR Invalid stream ID specified as stream command argument"} =
                   Stream.handle(
                     "XADD",
                     [key, "MINID", "not-an-id", "1-0", "field", "value"],
                     store
                   )

          assert 0 == Stream.handle("XLEN", [key], store)
          assert [] == Stream.handle("XRANGE", [key, "-", "+"], store)
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

        test "XLEN treats an expired stream as missing even when local metadata exists" do
          store = MockStore.make()
          key = ustream()

          Stream.handle("XADD", [key, "1-0", "f", "v"], store)
          assert 1 == Stream.handle("XLEN", [key], store)

          assert 1 == Expiry.handle("PEXPIRE", [key, "1"], store)
          Process.sleep(5)

          assert 0 == Stream.handle("XLEN", [key], store)
          assert [] == Stream.handle("XRANGE", [key, "-", "+"], store)
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
                |> Enum.filter(fn {entry_key, _value} ->
                  String.starts_with?(entry_key, prefix)
                end)
                |> Enum.map(fn {entry_key, value} ->
                  {String.replace_prefix(entry_key, prefix, ""), value}
                end)
              end)
            end,
            get: fn entry_key ->
              flunk(
                "XRANGE should use compound_scan, got per-entry GET for #{inspect(entry_key)}"
              )
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
                |> Enum.filter(fn {entry_key, _value} ->
                  String.starts_with?(entry_key, prefix)
                end)
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

        test "indexed stream reads decode batch results without zip or flat_map" do
          source =
            File.read!(
              Path.expand("../../../lib/ferricstore/commands/stream/entries.ex", __DIR__)
            )

          [decode_source] =
            Regex.run(
              ~r/def decode_indexed\(index_entries, stream_key, store\).*?^  end/ms,
              source
            )

          assert decode_source =~ "indexed_keys_and_ids(index_entries, [], [])"
          assert decode_source =~ "decode_indexed_raw(ids, raw_values, [])"
          refute decode_source =~ "Enum.zip"
          refute decode_source =~ "Enum.flat_map"
        end
      end

      # ===========================================================================
      # XREAD
      # ===========================================================================

      describe "XREAD" do
        test "XREAD builds ordered results in one pass" do
          source = File.read!(Path.expand("../../../lib/ferricstore/commands/stream.ex", __DIR__))

          [xread_source] =
            Regex.run(~r/defp do_xread\(stream_ids, count, store\).*?^  end/ms, source)

          assert xread_source =~ "xread_results(stream_ids, count, store, [])"
          refute xread_source =~ "Enum.find(results"
          refute xread_source =~ "Enum.reject(results"
        end

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
            Map.put(base, :compound_batch_delete, fn ^key, ["X:" <> _rest | _] ->
              {:error, :disk_full}
            end)

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
            Map.put(base, :compound_batch_delete, fn ^key, ["X:" <> _rest | _] ->
              {:error, :disk_full}
            end)

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
    end
  end
end
