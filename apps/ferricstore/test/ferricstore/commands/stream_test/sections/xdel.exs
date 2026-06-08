defmodule Ferricstore.Commands.StreamTest.Sections.Xdel do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Expiry, Stream, Strings}
      alias Ferricstore.Test.MockStore

      describe "XDEL" do
        test "stream deletes use batch get/delete on the hot path" do
          source =
            File.read!(
              Path.expand("../../../lib/ferricstore/commands/stream/mutations.ex", __DIR__)
            )

          [xdel_source] = Regex.run(~r/def xdel\(key, ids, store\).*?^  end/ms, source)

          [delete_ids_source] =
            Regex.run(~r/defp delete_stream_ids\(key, ids, store\).*?^  end/ms, source)

          assert xdel_source =~ "Entries.batch_get"
          assert xdel_source =~ "existing_ids"
          assert xdel_source =~ "Entries.existing_ids"
          assert xdel_source =~ "delete_stream_ids"
          refute xdel_source =~ "stream_entry_exists?"
          refute xdel_source =~ "Enum.zip"

          assert delete_ids_source =~ "Entries.delete_keys"
          assert delete_ids_source =~ "Index.delete_ids"
          refute delete_ids_source =~ "delete_stream_entry(store"
          refute delete_ids_source =~ "delete_stream_index_entry"

          entries_source =
            File.read!(
              Path.expand("../../../lib/ferricstore/commands/stream/entries.ex", __DIR__)
            )

          assert entries_source =~ "def existing_ids("
        end

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
            Map.put(base, :compound_batch_delete, fn ^key, ["X:" <> _rest | _] ->
              {:error, :disk_full}
            end)

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
              flunk(
                "XINFO STREAM should use batch_get, got per-entry GET for #{inspect(entry_key)}"
              )
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

        test "XINFO STREAM rebuilds empty MKSTREAM metadata after local state clear" do
          store = MockStore.make()
          key = ustream()

          assert :ok ==
                   Stream.handle("XGROUP", ["CREATE", key, "workers", "0", "MKSTREAM"], store)

          Stream.clear_local_state()

          info = Stream.handle("XINFO", ["STREAM", key], store)
          assert info["length"] == 0
          assert info["last-generated-id"] == "0-0"
          assert info["groups"] == 1
        end

        test "empty stream keeps last generated id after local state clear" do
          store = MockStore.make()
          key = ustream()

          assert "500-0" == Stream.handle("XADD", [key, "500-0", "f", "v"], store)
          assert 1 == Stream.handle("XTRIM", [key, "MAXLEN", "0"], store)
          Stream.clear_local_state()

          assert {:error, _} = Stream.handle("XADD", [key, "100-0", "old", "id"], store)
          new_id = Stream.handle("XADD", [key, "*", "new", "value"], store)
          [new_ms, _new_seq] = new_id |> String.split("-") |> Enum.map(&String.to_integer/1)
          assert new_ms >= 500
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
        test "XREADGROUP builds ordered results in one pass" do
          source = File.read!(Path.expand("../../../lib/ferricstore/commands/stream.ex", __DIR__))

          [xreadgroup_source] =
            Regex.run(
              ~r/defp do_xreadgroup\(group, consumer, stream_ids, count, store\).*?^  end/ms,
              source
            )

          assert xreadgroup_source =~
                   "xreadgroup_results(group, consumer, stream_ids, count, store, [])"

          refute xreadgroup_source =~ "Enum.find(results"
          refute xreadgroup_source =~ "Enum.reject(results"
        end

        test "XREADGROUP pending replay parses pending ids once before sorting" do
          source = File.read!(Path.expand("../../../lib/ferricstore/commands/stream.ex", __DIR__))

          assert source =~ "xreadgroup_pending_ids(pending, consumer, pending_start, count)"
          assert source =~ "defp xreadgroup_pending_ids("

          [pending_source] =
            Regex.run(
              ~r/defp xreadgroup_pending_ids\(pending, consumer, pending_start, count\).*?^  end/ms,
              source
            )

          refute pending_source =~ "|> Enum.filter",
                 "pending replay should filter and parse in one pass before sorting"
        end

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

        test "XREADGROUP pending replay batches stream entry reads" do
          base = MockStore.make()
          key = ustream()
          parent = self()

          for i <- 1..3, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], base)
          Stream.handle("XGROUP", ["CREATE", key, "g1", "0"], base)

          Stream.handle(
            "XREADGROUP",
            ["GROUP", "g1", "c1", "STREAMS", key, ">"],
            base
          )

          store =
            base
            |> Map.put(:compound_batch_get, fn redis_key, compound_keys ->
              send(parent, {:pending_batch_get, redis_key, compound_keys})
              base.compound_batch_get.(redis_key, compound_keys)
            end)
            |> Map.put(:compound_get, fn redis_key, compound_key ->
              send(parent, {:pending_single_get, redis_key, compound_key})
              base.compound_get.(redis_key, compound_key)
            end)

          result =
            Stream.handle(
              "XREADGROUP",
              ["GROUP", "g1", "c1", "COUNT", "2", "STREAMS", key, "0"],
              store
            )

          [[^key, entries]] = result
          assert Enum.map(entries, &hd/1) == ["1-0", "2-0"]

          assert_receive {:pending_batch_get, ^key, compound_keys}
          assert length(compound_keys) == 2
          assert Enum.all?(compound_keys, &String.starts_with?(&1, "X:#{key}" <> <<0>>))

          refute_receive {:pending_single_get, ^key, "X:" <> _rest}
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
    end
  end
end
