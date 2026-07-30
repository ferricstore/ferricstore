defmodule Ferricstore.Commands.StreamAtomicAppendTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Commands.Stream.{AtomicAppend, Groups, Index, Meta}
  alias Ferricstore.Store.{CompoundKey, Ops}
  alias Ferricstore.TermCodec

  @max_u64 18_446_744_073_709_551_615

  test "typed one-shot XRANGE preserves live, missing, expired, and wrong-type behavior" do
    live_key = unique_key("typed-range-live")
    expired_key = unique_key("typed-range-expired")
    wrong_type_key = unique_key("typed-range-wrongtype")
    missing_key = unique_key("typed-range-missing")
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      Enum.each([live_key, expired_key, wrong_type_key, missing_key], &FerricStore.del/1)
    end)

    for {key, value} <- [{live_key, "live"}, {expired_key, "expired"}] do
      assert "1-0" = Stream.handle("XADD", [key, "1-0", "field", value], ctx)
    end

    assert [["1-0", "field", "live"]] =
             Stream.handle("XRANGE", [live_key, "-", "+", "COUNT", "1"], ctx)

    assert [] = Stream.handle("XRANGE", [missing_key, "-", "+", "COUNT", "1"], ctx)

    assert :ok = FerricStore.set(wrong_type_key, "string")

    assert {:error, "WRONGTYPE" <> _rest} =
             Stream.handle("XRANGE", [wrong_type_key, "-", "+", "COUNT", "1"], ctx)

    assert {:ok, true} = FerricStore.pexpire(expired_key, 1)
    Process.sleep(5)
    assert [] = Stream.handle("XRANGE", [expired_key, "-", "+", "COUNT", "1"], ctx)
  end

  test "raw typed range reads preserve forward, reverse, missing, and wrong-type semantics" do
    key = unique_key("typed-raw-range")
    missing_key = unique_key("typed-raw-range-missing")
    wrong_type_key = unique_key("typed-raw-range-wrongtype")
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      Enum.each([key, missing_key, wrong_type_key], &FerricStore.del/1)
    end)

    for id <- 1..3 do
      stream_id = "#{id}-0"

      assert stream_id ==
               Stream.handle("XADD", [key, stream_id, "field", Integer.to_string(id)], ctx)
    end

    assert {:ok, [{"1-0", first_raw}, {"2-0", second_raw}]} =
             Stream.handle_raw_range_ast({:xrange, key, :min, :max, 2}, ctx)

    assert {:ok, ["field", "1"]} = Stream.Entries.decode_fields(first_raw)
    assert {:ok, ["field", "2"]} = Stream.Entries.decode_fields(second_raw)

    assert {:ok, [{"3-0", _third_raw}, {"2-0", _second_raw}]} =
             Stream.handle_raw_range_ast({:xrevrange, key, :min, :max, 2}, ctx)

    assert {:ok, []} =
             Stream.handle_raw_range_ast({:xrange, missing_key, :min, :max, 2}, ctx)

    assert :ok = FerricStore.set(wrong_type_key, "string")

    assert {:error, "WRONGTYPE" <> _rest} =
             Stream.handle_raw_range_ast({:xrange, wrong_type_key, :min, :max, 2}, ctx)
  end

  test "compact append planning is read-only and carries explicit publication data" do
    key = unique_key("plan")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    assert {:ok, %AtomicAppend.Plan{} = plan} =
             AtomicAppend.plan_many_auto(
               key,
               [["field", "one"], ["field", "two"]],
               ctx
             )

    assert length(plan.results) == 2
    assert length(plan.entries) == 4
    assert length(plan.member_entries) == 2
    assert plan.member_prefix == CompoundKey.stream_prefix(key)

    planned_member_keys = Enum.map(plan.member_entries, &elem(&1, 1))

    durable_member_keys =
      plan.entries
      |> Enum.map(&elem(&1, 0))
      |> Enum.filter(&String.starts_with?(&1, plan.member_prefix))

    assert planned_member_keys == durable_member_keys

    assert plan.results ==
             Enum.map(plan.member_entries, fn {{_prefix, {ms, seq}}, _key} -> "#{ms}-#{seq}" end)

    assert %AtomicAppend.Publication{
             cache_entry: {cache_key, 2, first, last, ms, seq},
             member_prefix: member_prefix,
             member_count: 2,
             member_entries: member_entries
           } = AtomicAppend.publication(plan, ctx)

    assert cache_key == {ctx.name, key}
    assert {:append, 2, ^first, ^last, ^ms, ^seq, _id, true, []} = plan.latest_update

    assert member_prefix == plan.member_prefix
    assert member_entries == plan.member_entries
    assert {:ok, 0} = FerricStore.xlen(key)
  end

  test "compact append cursor does not consume an ID for a rejected value" do
    key = unique_key("plan-rejected-value")

    store = %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      validate_value: fn encoded ->
        case Stream.Entries.decode_fields(encoded) do
          {:ok, ["field", "reject"]} -> {:error, :rejected_for_test}
          {:ok, _fields} -> :ok
          _not_term_encoded -> :ok
        end
      end
    }

    assert {:ok, %AtomicAppend.Plan{} = plan} =
             AtomicAppend.plan_many_auto(
               key,
               [["field", "one"], ["field", "reject"], ["field", "two"]],
               store
             )

    assert [first_id, {:error, :rejected_for_test}, second_id] = plan.results
    assert {:ok, {ms, seq}} = Stream.ID.parse_full_id(first_id)
    assert {:ok, {^ms, next_seq}} = Stream.ID.parse_full_id(second_id)
    assert next_seq == seq + 1
    assert length(plan.member_entries) == 2
  end

  test "compact append accepts one stamped apply time for grouped topic planning" do
    key = unique_key("plan-stamped-time")

    store = %{
      stream_apply_now_ms: 123,
      compound_get: fn _redis_key, _compound_key -> nil end
    }

    assert {:ok, %AtomicAppend.Plan{results: ["123-0", "123-1"]}} =
             AtomicAppend.plan_many_auto(
               key,
               [["field", "one"], ["field", "two"]],
               store
             )
  end

  test "compact append validates a new topic from one observed type marker" do
    key = unique_key("plan-single-type-read")
    type_key = CompoundKey.type_key(key)
    reads = :counters.new(1, [])

    store = %{
      compound_get: fn
        ^key, ^type_key ->
          :counters.add(reads, 1, 1)
          nil

        _redis_key, _compound_key ->
          nil
      end
    }

    assert {:ok, %AtomicAppend.Plan{results: [_id]}} =
             AtomicAppend.plan_many_auto(key, [["field", "value"]], store)

    assert :counters.get(reads, 1) == 1
  end

  test "compact append retains WRONGTYPE validation from the observed marker" do
    key = unique_key("plan-observed-wrongtype")
    type_key = CompoundKey.type_key(key)

    store = %{
      compound_get: fn
        ^key, ^type_key -> "hash"
        _redis_key, _compound_key -> nil
      end,
      compound_count: fn _redis_key, _prefix -> 1 end
    }

    assert {:ok, %AtomicAppend.Plan{entries: [], results: [{:error, error}]}} =
             AtomicAppend.plan_many_auto(key, [["field", "value"]], store)

    assert error == "WRONGTYPE Operation against a key holding the wrong kind of value"
  end

  test "compact append uses the explicit production value limit without callback dispatch" do
    key = unique_key("plan-direct-value-limit")
    fields = ["field", "value"]
    encoded_size = fields |> Stream.Entries.encode_fields() |> byte_size()

    store = %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      max_value_size: encoded_size - 1,
      validate_value: fn _value -> flunk("explicit value limit should bypass the callback") end
    }

    assert {:ok, %AtomicAppend.Plan{} = plan} =
             AtomicAppend.plan_many_auto(key, [fields], store)

    error =
      "ERR value too large (#{encoded_size} bytes, max #{encoded_size - 1} bytes)"

    assert [{:error, ^error}] = plan.results

    assert plan.entries == []
    assert plan.member_entries == []
  end

  test "terminal compact planning reuses published metadata after confirming the type marker" do
    key = unique_key("plan-cached-meta")
    scope = {:cached_meta_test, System.unique_integer([:positive])}
    type_key = CompoundKey.type_key(key)
    meta_key = CompoundKey.stream_meta_key(key)
    store = %{cache_scope: scope, stream_meta_cache_safe: true}

    assert true = Meta.put_local(key, 3, "100-0", "100-2", 100, 2, store)
    on_exit(fn -> Meta.cleanup_local(key, store) end)

    store =
      Map.put(store, :compound_get, fn
        ^key, ^type_key -> "stream"
        ^key, ^meta_key -> flunk("published metadata should satisfy the terminal plan")
        _redis_key, _compound_key -> nil
      end)

    assert {:ok, %AtomicAppend.Plan{} = plan} =
             AtomicAppend.plan_many_auto(key, [["field", "value"]], store)

    assert {:append, 4, "100-0", last, ms, seq, result_id, true, []} = plan.latest_update
    assert result_id == last
    assert last == "#{ms}-#{seq}"

    durable_meta =
      TermCodec.encode({:stream_meta, 2, 7, "200-0", "200-6", 200, 6})

    sequential_store =
      store
      |> Map.delete(:stream_meta_cache_safe)
      |> Map.put(:compound_get, fn
        ^key, ^type_key -> "stream"
        ^key, ^meta_key -> durable_meta
        _redis_key, _compound_key -> nil
      end)

    assert {:ok, %AtomicAppend.Plan{latest_update: {:append, 8, "200-0", _, _, _, _, true, []}}} =
             AtomicAppend.plan_many_auto(key, [["field", "value"]], sequential_store)
  end

  test "compact append cursor fails closed after the maximum Stream ID" do
    key = unique_key("plan-id-exhaustion")
    last_id = "#{@max_u64}-#{@max_u64 - 1}"
    type_key = CompoundKey.type_key(key)
    meta_key = CompoundKey.stream_meta_key(key)

    encoded_meta =
      TermCodec.encode({:stream_meta, 2, 1, last_id, last_id, @max_u64, @max_u64 - 1})

    store = %{
      compound_get: fn
        ^key, ^type_key -> "stream"
        ^key, ^meta_key -> encoded_meta
        _redis_key, _compound_key -> nil
      end
    }

    assert {:ok, %AtomicAppend.Plan{} = plan} =
             AtomicAppend.plan_many_auto(
               key,
               [["field", "last"], ["field", "overflow"]],
               store
             )

    max_id = "#{@max_u64}-#{@max_u64}"
    assert [^max_id, {:error, error}] = plan.results
    assert error =~ "equal or smaller"
    assert length(plan.member_entries) == 1
  end

  test "compact append validates each durable value exactly once before terminal persistence" do
    key = unique_key("plan-validation")
    parent = self()

    store = %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      validate_value: fn value ->
        send(parent, {:validated, value})
        :ok
      end,
      terminal_stream_validated_batch_put: fn persisted_key, entries ->
        send(parent, {:persisted, persisted_key, entries})
        :ok
      end
    }

    assert {:ok, %AtomicAppend.Plan{} = plan} =
             AtomicAppend.plan_many_auto(
               key,
               [["field", "one"], ["field", "two"]],
               store
             )

    assert_receive {:validated, encoded_one}
    assert_receive {:validated, encoded_two}
    assert_receive {:validated, "stream"}
    assert_receive {:validated, encoded_meta}
    assert Stream.Entries.decode_fields(encoded_one) == {:ok, ["field", "one"]}
    assert Stream.Entries.decode_fields(encoded_two) == {:ok, ["field", "two"]}
    assert match?({:ok, {:stream_meta, 2, 2, _, _, _, _}}, TermCodec.decode(encoded_meta))
    refute_receive {:validated, _value}

    assert plan.results == AtomicAppend.commit_terminal_many_auto(plan, store)
    assert_receive {:persisted, ^key, persisted_entries}
    assert persisted_entries == plan.entries
    refute_receive {:validated, _value}
  end

  test "compact append validates generated Stream metadata before persistence" do
    key = unique_key("plan-generated-validation")

    store = %{
      compound_get: fn _redis_key, _compound_key -> nil end,
      validate_value: fn value ->
        case TermCodec.decode(value) do
          {:ok, {:stream_meta, 2, _len, _first, _last, _ms, _seq}} ->
            {:error, :generated_metadata_rejected}

          _other ->
            :ok
        end
      end
    }

    assert {:error, :generated_metadata_rejected} =
             AtomicAppend.plan_many_auto(key, [["field", "value"]], store)
  end

  test "grouped terminal commit stages validated topic projections once" do
    parent = self()

    store = %{
      stream_apply_now_ms: 123,
      compound_get: fn _redis_key, _compound_key -> nil end,
      terminal_stream_validated_batch_put: fn _key, _prefix, _entries ->
        flunk("grouped commit should use the grouped staging callback")
      end,
      terminal_stream_validated_grouped_batch_put: fn batches ->
        send(parent, {:grouped_batches, batches})
        :ok
      end
    }

    plans =
      for key <- ["topic:a", "topic:b"] do
        assert {:ok, %AtomicAppend.Plan{} = plan} =
                 AtomicAppend.plan_many_auto(key, [["field", key]], store)

        plan
      end

    assert :ok = AtomicAppend.commit_terminal_many_auto_group(plans, store)

    assert_receive {:grouped_batches,
                    [
                      {"topic:a", _prefix_a, entries_a},
                      {"topic:b", _prefix_b, entries_b}
                    ]}

    assert entries_a == Enum.at(plans, 0).entries
    assert entries_b == Enum.at(plans, 1).entries

    assert :ok =
             AtomicAppend.commit_terminal_many_auto_group_reversed(
               Enum.reverse(plans),
               store
             )

    assert_receive {:grouped_batches,
                    [
                      {"topic:a", _prefix_a, ^entries_a},
                      {"topic:b", _prefix_b, ^entries_b}
                    ]}
  end

  test "concurrent XADD replies correspond one-to-one with durable entries" do
    key = unique_key("auto")
    on_exit(fn -> FerricStore.del(key) end)

    results =
      1..128
      |> Task.async_stream(
        fn value -> FerricStore.xadd(key, ["field", Integer.to_string(value)]) end,
        max_concurrency: 64,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.to_list()

    ids = for {:ok, {:ok, id}} <- results, do: id

    assert length(ids) == 128
    assert MapSet.size(MapSet.new(ids)) == 128
    assert {:ok, 128} = FerricStore.xlen(key)
    assert {:ok, entries} = FerricStore.xrange(key, "-", "+")
    assert length(entries) == 128
    assert MapSet.new(Enum.map(entries, &hd/1)) == MapSet.new(ids)
  end

  test "concurrent explicit IDs are linearized" do
    key = unique_key("explicit")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    results =
      1..32
      |> Task.async_stream(
        fn _ -> Stream.handle("XADD", [key, "1-0", "field", "value"], ctx) end,
        max_concurrency: 32,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == "1-0")) == 1
    assert Enum.count(results, &match?({:error, _reason}, &1)) == 31
    assert Stream.handle("XLEN", [key], ctx) == 1
  end

  test "XADD MAXLEN commits entry, trimming, and metadata atomically" do
    key = unique_key("trim")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    for value <- 1..40 do
      assert is_binary(
               Stream.handle(
                 "XADD",
                 [key, "MAXLEN", "10", "*", "field", Integer.to_string(value)],
                 ctx
               )
             )
    end

    assert Stream.handle("XLEN", [key], ctx) == 10
    entries = Stream.handle("XRANGE", [key, "-", "+"], ctx)
    assert Enum.map(entries, &List.last/1) == Enum.map(31..40, &Integer.to_string/1)
  end

  test "XADD MAXLEN 0 keeps an empty live range without losing ID monotonicity" do
    key = unique_key("maxlen-zero")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    assert Stream.handle("XADD", [key, "MAXLEN", "0", "5-0", "field", "value"], ctx) ==
             "5-0"

    assert Stream.handle("XLEN", [key], ctx) == 0
    assert Stream.handle("XRANGE", [key, "-", "+"], ctx) == []

    info = Stream.handle("XINFO", ["STREAM", key], ctx)
    assert info["length"] == 0
    assert info["first-entry"] == nil
    assert info["last-entry"] == nil
    assert info["last-generated-id"] == "5-0"
    assert {:error, _reason} = Stream.handle("XADD", [key, "4-0", "field", "old"], ctx)
    assert Stream.handle("XADD", [key, "6-0", "field", "new"], ctx) == "6-0"
  end

  test "atomic XADD incrementally updates a warmed range index" do
    key = unique_key("warm-index")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    assert Stream.handle("XADD", [key, "1-0", "field", "one"], ctx) == "1-0"

    assert Stream.handle("XREVRANGE", [key, "1-0", "1-0"], ctx) == [
             ["1-0", "field", "one"]
           ]

    assert Index.ready?(key, ctx)

    assert Stream.handle("XADD", [key, "2-0", "field", "two"], ctx) == "2-0"
    assert Index.ready?(key, ctx)

    assert Stream.handle("XRANGE", [key, "-", "+"], ctx) == [
             ["1-0", "field", "one"],
             ["2-0", "field", "two"]
           ]

    assert Stream.handle("XREVRANGE", [key, "2-0", "2-0"], ctx) == [
             ["2-0", "field", "two"]
           ]
  end

  test "XDEL preserves last-generated-id while updating the live tail" do
    key = unique_key("delete-tail")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    for id <- ["1-0", "2-0", "3-0"] do
      assert Stream.handle("XADD", [key, id, "field", id], ctx) == id
    end

    assert Stream.handle("XRANGE", [key, "-", "+"], ctx) |> length() == 3
    refute Index.ready?(key, ctx)
    assert Stream.handle("XDEL", [key, "3-0"], ctx) == 1
    assert Index.ready?(key, ctx)

    info = Stream.handle("XINFO", ["STREAM", key], ctx)
    assert info["length"] == 2
    assert hd(info["last-entry"]) == "2-0"
    assert info["last-generated-id"] == "3-0"
    assert {:error, _reason} = Stream.handle("XADD", [key, "2-1", "field", "old"], ctx)
    assert Stream.handle("XADD", [key, "4-0", "field", "new"], ctx) == "4-0"
  end

  test "XTRIM commits all deletes and metadata in one mutation" do
    key = unique_key("atomic-trim")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    for id <- 1..30 do
      assert Stream.handle("XADD", [key, "#{id}-0", "field", "#{id}"], ctx) == "#{id}-0"
    end

    assert Stream.handle("XRANGE", [key, "-", "+"], ctx) |> length() == 30
    refute Index.ready?(key, ctx)
    assert Stream.handle("XTRIM", [key, "MAXLEN", "7"], ctx) == 23
    assert Index.ready?(key, ctx)
    assert Stream.handle("XLEN", [key], ctx) == 7

    assert Stream.handle("XRANGE", [key, "-", "+"], ctx)
           |> Enum.map(&hd/1) == Enum.map(24..30, &"#{&1}-0")
  end

  test "concurrent delete and append operations retain every new entry" do
    key = unique_key("delete-append")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    old_ids =
      Enum.map(1..64, fn id ->
        stream_id = "#{id}-0"
        assert Stream.handle("XADD", [key, stream_id, "old", "#{id}"], ctx) == stream_id
        stream_id
      end)

    operations =
      Enum.map(old_ids, &{:delete, &1}) ++ Enum.map(65..128, &{:append, &1})

    results =
      operations
      |> Task.async_stream(
        fn
          {:delete, id} -> Stream.handle("XDEL", [key, id], ctx)
          {:append, id} -> Stream.handle("XADD", [key, "*", "new", "#{id}"], ctx)
        end,
        max_concurrency: 64,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == 1)) == 64
    assert Enum.count(results, &is_binary/1) == 64
    assert Stream.handle("XLEN", [key], ctx) == 64

    assert Stream.handle("XRANGE", [key, "-", "+"], ctx)
           |> Enum.map(&List.last/1)
           |> Enum.map(&String.to_integer/1)
           |> Enum.sort() == Enum.to_list(65..128)
  end

  test "concurrent consumer-group reads deliver each new entry once" do
    key = unique_key("group-read")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    for id <- 1..96 do
      assert Stream.handle("XADD", [key, "#{id}-0", "field", "#{id}"], ctx) == "#{id}-0"
    end

    assert Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], ctx) == :ok

    delivered_ids =
      1..96
      |> Task.async_stream(
        fn consumer_id ->
          Stream.handle(
            "XREADGROUP",
            [
              "GROUP",
              "workers",
              "consumer-#{consumer_id}",
              "COUNT",
              "1",
              "STREAMS",
              key,
              ">"
            ],
            ctx
          )
        end,
        max_concurrency: 48,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, [[^key, [[id | _fields]]]]} -> id end)

    assert length(delivered_ids) == 96
    assert MapSet.size(MapSet.new(delivered_ids)) == 96

    assert Stream.handle(
             "XREADGROUP",
             ["GROUP", "workers", "consumer", "STREAMS", key, ">"],
             ctx
           ) == []
  end

  test "concurrent XACK acknowledges a pending entry exactly once" do
    key = unique_key("group-ack")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    assert Stream.handle("XADD", [key, "1-0", "field", "one"], ctx) == "1-0"
    assert Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], ctx) == :ok

    assert [[^key, [["1-0", "field", "one"]]]] =
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "consumer", "STREAMS", key, ">"],
               ctx
             )

    acknowledgements =
      1..32
      |> Task.async_stream(
        fn _ -> Stream.handle("XACK", [key, "workers", "1-0"], ctx) end,
        max_concurrency: 32,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(acknowledgements, &(&1 == 1)) == 1
    assert Enum.count(acknowledgements, &(&1 == 0)) == 31
  end

  test "new consumer groups persist bounded metadata and split pending entries" do
    key = unique_key("group-split")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    for id <- 1..3 do
      assert Stream.handle("XADD", [key, "#{id}-0", "field", "#{id}"], ctx) == "#{id}-0"
    end

    assert Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], ctx) == :ok

    assert [[^key, entries]] =
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "consumer", "COUNT", "3", "STREAMS", key, ">"],
               ctx
             )

    assert length(entries) == 3

    raw_group = Ops.compound_get(ctx, key, CompoundKey.stream_group(key, "workers"))
    assert {:ok, {:stream_group, 2, "3-0"}} = Ferricstore.TermCodec.decode(raw_group)

    assert length(Ops.compound_scan(ctx, key, CompoundKey.stream_pending_prefix(key))) == 3
    assert length(Ops.compound_scan(ctx, key, CompoundKey.stream_consumer_prefix(key))) == 1

    Groups.delete_group_local(ctx, key, "workers")

    assert {:ok, "3-0", %{"consumer" => seen_at_ms}, pending} =
             Groups.lookup(ctx, key, "workers")

    assert is_integer(seen_at_ms)
    assert Map.keys(pending) |> Enum.sort() == ["1-0", "2-0", "3-0"]
    assert Stream.handle("XACK", [key, "workers", "1-0", "1-0", "3-0"], ctx) == 2

    Groups.delete_group_local(ctx, key, "workers")

    assert {:ok, "3-0", _consumers, %{"2-0" => {"consumer", _timestamp}}} =
             Groups.lookup(ctx, key, "workers")

    assert Groups.delete(ctx, key, "workers") == :ok
    assert Ops.compound_scan(ctx, key, CompoundKey.stream_pending_prefix(key)) == []
    assert Ops.compound_scan(ctx, key, CompoundKey.stream_consumer_prefix(key)) == []
  end

  test "split consumer-group records preserve an empty consumer name" do
    key = unique_key("empty-consumer")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    assert Stream.handle("XADD", [key, "1-0", "field", "one"], ctx) == "1-0"
    assert Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], ctx) == :ok

    assert [[^key, [["1-0", "field", "one"]]]] =
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "", "STREAMS", key, ">"],
               ctx
             )

    Groups.delete_group_local(ctx, key, "workers")

    assert {:ok, "1-0", %{"" => seen_at_ms}, %{"1-0" => {"", delivered_at_ms}}} =
             Groups.lookup(ctx, key, "workers")

    assert is_integer(seen_at_ms)
    assert is_integer(delivered_at_ms)
    assert Stream.handle("XACK", [key, "workers", "1-0"], ctx) == 1
  end

  test "COPY and RENAME preserve split consumer-group state without orphaning source data" do
    slot = System.unique_integer([:positive, :monotonic])
    source = "stream:atomic:{group-lifecycle-#{slot}}:source"
    copied = "stream:atomic:{group-lifecycle-#{slot}}:copied"
    renamed = "stream:atomic:{group-lifecycle-#{slot}}:renamed"
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      FerricStore.del(source)
      FerricStore.del(copied)
      FerricStore.del(renamed)
    end)

    assert Stream.handle("XADD", [source, "1-0", "field", "one"], ctx) == "1-0"
    assert Stream.handle("XGROUP", ["CREATE", source, "workers", "0"], ctx) == :ok

    assert [[^source, [["1-0", "field", "one"]]]] =
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "consumer", "STREAMS", source, ">"],
               ctx
             )

    assert {:ok, true} = FerricStore.copy(source, copied)
    assert Stream.handle("XACK", [source, "workers", "1-0"], ctx) == 1

    assert :ok = FerricStore.rename(copied, renamed)
    assert Stream.handle("XACK", [renamed, "workers", "1-0"], ctx) == 1
    assert Stream.handle("XINFO", ["STREAM", renamed], ctx)["groups"] == 1
    assert Ops.compound_scan(ctx, copied, CompoundKey.stream_pending_prefix(copied)) == []
    assert Ops.compound_scan(ctx, copied, CompoundKey.stream_consumer_prefix(copied)) == []
  end

  test "legacy monolithic consumer groups remain readable and writable" do
    key = unique_key("legacy-group")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    assert Stream.handle("XADD", [key, "1-0", "field", "one"], ctx) == "1-0"

    legacy = Ferricstore.TermCodec.encode({:stream_group, 1, "0-0", %{}, %{}})
    assert Ops.compound_put(ctx, key, CompoundKey.stream_group(key, "workers"), legacy, 0) == :ok
    Groups.delete_group_local(ctx, key, "workers")

    assert [[^key, [["1-0", "field", "one"]]]] =
             Stream.handle(
               "XREADGROUP",
               ["GROUP", "workers", "consumer", "STREAMS", key, ">"],
               ctx
             )

    raw_group = Ops.compound_get(ctx, key, CompoundKey.stream_group(key, "workers"))

    assert {:ok,
            {:stream_group, 1, "1-0", %{"consumer" => seen_at_ms},
             %{"1-0" => {"consumer", delivered_at_ms}}}} =
             Ferricstore.TermCodec.decode(raw_group)

    assert is_integer(seen_at_ms)
    assert is_integer(delivered_at_ms)
    assert Stream.handle("XACK", [key, "workers", "1-0"], ctx) == 1
  end

  test "concurrent XGROUP CREATE has one winner" do
    key = unique_key("group-create")
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)
    assert Stream.handle("XADD", [key, "1-0", "field", "one"], ctx) == "1-0"

    results =
      1..24
      |> Task.async_stream(
        fn _ -> Stream.handle("XGROUP", ["CREATE", key, "workers", "0"], ctx) end,
        max_concurrency: 24,
        ordered: false,
        timeout: 30_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert Enum.count(results, &match?({:error, "BUSYGROUP " <> _}, &1)) == 23
  end

  defp unique_key(suffix) do
    "stream:atomic:#{suffix}:#{System.unique_integer([:positive, :monotonic])}"
  end
end
