defmodule Ferricstore.Commands.StreamBatchAppendTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.NamespaceUsage
  alias Ferricstore.Commands.Stream
  alias Ferricstore.Test.ShardHelpers

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    :ok
  end

  test "xadd_many commits same-shard appends in one Raft entry and preserves order" do
    tag = System.unique_integer([:positive, :monotonic])
    key = "stream:batch:append:{#{tag}}"
    ctx = FerricStore.Instance.get(:default)
    shard_index = Router.shard_for(ctx, key)
    before_position = raft_position(shard_index)
    on_exit(fn -> FerricStore.del(key) end)

    items =
      Enum.map(1..64, fn value ->
        {key, ["field", Integer.to_string(value)]}
      end)

    assert results = FerricStore.xadd_many(items)
    assert length(results) == 64

    ids = Enum.map(results, fn {:ok, id} -> id end)
    assert MapSet.size(MapSet.new(ids)) == 64
    assert ids == Enum.sort(ids, &stream_id_before?/2)
    assert raft_position(shard_index) == before_position + 1
    assert {:ok, 64} = FerricStore.xlen(key)

    assert {:ok, entries} = FerricStore.xrange(key, "-", "+")
    assert Enum.map(entries, &List.last/1) == Enum.map(1..64, &Integer.to_string/1)
  end

  test "xadd_many batches each shard independently and retains global result order" do
    tag = System.unique_integer([:positive, :monotonic])
    ctx = FerricStore.Instance.get(:default)
    key_a = "stream:batch:cross:#{tag}:a"
    shard_a = Router.shard_for(ctx, key_a)

    key_b =
      Enum.find_value(1..10_000, fn suffix ->
        candidate = "stream:batch:cross:#{tag}:b:#{suffix}"
        if Router.shard_for(ctx, candidate) != shard_a, do: candidate
      end)

    shard_b = Router.shard_for(ctx, key_b)
    refute shard_a == shard_b
    before_a = raft_position(shard_a)
    before_b = raft_position(shard_b)

    on_exit(fn -> FerricStore.del([key_a, key_b]) end)

    assert [{:ok, id_a1}, {:ok, id_b1}, {:ok, id_a2}, {:ok, id_b2}] =
             FerricStore.xadd_many([
               {key_a, ["value", "a1"]},
               {key_b, ["value", "b1"]},
               {key_a, ["value", "a2"]},
               {key_b, ["value", "b2"]}
             ])

    assert stream_id_before?(id_a1, id_a2)
    assert stream_id_before?(id_b1, id_b2)
    assert raft_position(shard_a) == before_a + 1
    assert raft_position(shard_b) == before_b + 1
    assert {:ok, 2} = FerricStore.xlen(key_a)
    assert {:ok, 2} = FerricStore.xlen(key_b)
  end

  test "multi-shard routing compacts only homogeneous auto-ID Stream groups" do
    assert {:ok, {:stream_append_many_auto, "stream:compact", [["value", "1"], ["value", "2"]]}} =
             Router.__compact_stream_append_many_auto_command_for_test__([
               {:stream_append, "stream:compact", :auto, ["value", "1"], nil, false},
               {:stream_append, "stream:compact", :auto, ["value", "2"], nil, false}
             ])

    assert :generic =
             Router.__compact_stream_append_many_auto_command_for_test__([
               {:stream_append, "stream:a", :auto, ["value", "1"], nil, false},
               {:stream_append, "stream:b", :auto, ["value", "2"], nil, false}
             ])

    mixed_commands = [
      {:stream_append, "stream:a", :auto, ["value", "1"], nil, false},
      {:stream_append, "stream:b", :auto, ["value", "2"], nil, false}
    ]

    assert {:direct_batch,
            {:stream_append_grouped_auto,
             [
               {"stream:a", [["value", "1"]], [0]},
               {"stream:b", [["value", "2"]], [1]}
             ], 2}} =
             Router.__stream_append_auto_batch_shape_for_test__(mixed_commands)

    assert :generic =
             Router.__compact_stream_append_many_auto_command_for_test__([
               {:stream_append, "stream:compact", {1, 0}, ["value", "1"], nil, false}
             ])

    assert :generic =
             Router.__compact_stream_append_many_auto_command_for_test__([
               {:stream_append, "stream:compact", :auto, ["value", "1"], {:maxlen, 10}, false}
             ])

    assert :generic =
             Router.__stream_append_auto_batch_shape_for_test__([
               {:stream_append, "stream:compact", :auto, ["value", "1"], nil, false},
               {:put, "unrelated", "value", 0}
             ])
  end

  test "xadd_many groups interleaved topics on one shard and restores input results" do
    tag = System.unique_integer([:positive, :monotonic])
    ctx = FerricStore.Instance.get(:default)
    first_key = "stream:batch:same-shard:#{tag}:0"
    shard = Router.shard_for(ctx, first_key)

    keys =
      Elixir.Stream.iterate(1, &(&1 + 1))
      |> Elixir.Stream.map(&"stream:batch:same-shard:#{tag}:#{&1}")
      |> Elixir.Stream.filter(&(Router.shard_for(ctx, &1) == shard))
      |> Enum.take(3)
      |> then(&[first_key | &1])

    before_position = raft_position(shard)
    on_exit(fn -> FerricStore.del(keys) end)

    items =
      Enum.map(0..63, fn index ->
        key = Enum.at(keys, rem(index, length(keys)))
        {key, ["value", Integer.to_string(index)]}
      end)

    assert results = FerricStore.xadd_many(items)
    assert length(results) == length(items)
    assert Enum.all?(results, &match?({:ok, id} when is_binary(id), &1))
    assert raft_position(shard) == before_position + 1

    Enum.each(keys, fn key ->
      expected_values =
        items
        |> Enum.filter(&(elem(&1, 0) == key))
        |> Enum.map(&(elem(&1, 1) |> List.last()))

      key_ids =
        items
        |> Enum.zip(results)
        |> Enum.filter(&(elem(elem(&1, 0), 0) == key))
        |> Enum.map(fn {_item, {:ok, id}} -> id end)

      assert key_ids == Enum.sort(key_ids, &stream_id_before?/2)
      assert {:ok, entries} = FerricStore.xrange(key, "-", "+")
      assert Enum.map(entries, &List.last/1) == expected_values
    end)
  end

  test "xadd_many preserves order and isolation across many topics on one shard" do
    tag = System.unique_integer([:positive, :monotonic])
    ctx = FerricStore.Instance.get(:default)
    first_key = "stream:batch:many-topics:#{tag}:0"
    shard = Router.shard_for(ctx, first_key)

    keys =
      Elixir.Stream.iterate(1, &(&1 + 1))
      |> Elixir.Stream.map(&"stream:batch:many-topics:#{tag}:#{&1}")
      |> Elixir.Stream.filter(&(Router.shard_for(ctx, &1) == shard))
      |> Enum.take(31)
      |> then(&[first_key | &1])

    before_position = raft_position(shard)
    on_exit(fn -> FerricStore.del(keys) end)

    items =
      Enum.map(0..255, fn index ->
        key = Enum.at(keys, rem(index, length(keys)))
        {key, ["value", Integer.to_string(index)]}
      end)

    assert results = FerricStore.xadd_many(items)
    assert length(results) == length(items)
    assert raft_position(shard) == before_position + 1

    item_results = Enum.zip(items, results)

    Enum.each(keys, fn key ->
      topic_results = Enum.filter(item_results, &(elem(elem(&1, 0), 0) == key))
      ids = Enum.map(topic_results, fn {_item, {:ok, id}} -> id end)

      expected_values =
        Enum.map(topic_results, fn {{_key, fields}, _result} -> List.last(fields) end)

      assert length(ids) == 8
      assert ids == Enum.sort(ids, &stream_id_before?/2)
      assert {:ok, 8} = FerricStore.xlen(key)
      assert {:ok, entries} = FerricStore.xrange(key, "-", "+")
      assert Enum.map(entries, &hd/1) == ids
      assert Enum.map(entries, &List.last/1) == expected_values
    end)
  end

  @tag timeout: 120_000
  test "mixed-topic xadd_many survives a full restart and continues each topic monotonically" do
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)

    tag = System.unique_integer([:positive, :monotonic])
    ctx = FerricStore.Instance.get(:default)
    key_a = "stream:batch:restart:#{tag}:a"
    shard_a = Router.shard_for(ctx, key_a)

    key_b =
      Enum.find_value(1..10_000, fn suffix ->
        candidate = "stream:batch:restart:#{tag}:b:#{suffix}"
        if Router.shard_for(ctx, candidate) != shard_a, do: candidate
      end)

    key_c =
      Enum.find_value(1..10_000, fn suffix ->
        candidate = "stream:batch:restart:#{tag}:c:#{suffix}"
        if Router.shard_for(ctx, candidate) == shard_a, do: candidate
      end)

    keys = [key_a, key_b, key_c]
    values = Enum.map(1..129, &Integer.to_string/1)

    items =
      values
      |> Enum.with_index()
      |> Enum.map(fn {value, index} ->
        {Enum.at(keys, rem(index, length(keys))), ["value", value]}
      end)

    assert results = FerricStore.xadd_many(items)
    assert length(results) == 129

    item_results = Enum.zip(items, results)

    before_by_key =
      Map.new(keys, fn key ->
        key_results = Enum.filter(item_results, &(elem(elem(&1, 0), 0) == key))
        ids = Enum.map(key_results, fn {_item, {:ok, id}} -> id end)

        expected_values =
          Enum.map(key_results, fn {{_key, fields}, _result} -> List.last(fields) end)

        assert length(ids) == 43
        assert ids == Enum.sort(ids, &stream_id_before?/2)
        assert {:ok, 43} = FerricStore.xlen(key)
        assert {:ok, entries} = FerricStore.xrange(key, "-", "+")
        assert Enum.map(entries, &hd/1) == ids
        assert Enum.map(entries, &List.last/1) == expected_values
        {key, {entries, List.last(ids)}}
      end)

    assert :ok = ShardHelpers.restart_current_data_dir(isolated)

    Enum.each(keys, fn key ->
      {before_entries, _last_id} = Map.fetch!(before_by_key, key)
      assert {:ok, 43} = FerricStore.xlen(key)
      assert {:ok, ^before_entries} = FerricStore.xrange(key, "-", "+")
    end)

    next_items = Enum.map(keys, &{&1, ["value", "after-restart"]})
    assert next_results = FerricStore.xadd_many(next_items)

    Enum.zip(keys, next_results)
    |> Enum.each(fn {key, {:ok, next_id}} ->
      {_before_entries, last_id} = Map.fetch!(before_by_key, key)
      assert stream_id_before?(last_id, next_id)
      assert {:ok, 44} = FerricStore.xlen(key)

      assert {:ok, [[^next_id, "value", "after-restart"]]} =
               FerricStore.xrevrange(key, next_id, next_id)
    end)
  end

  test "xadd_many keeps an explicitly warmed range index complete while compacting cache publication" do
    tag = System.unique_integer([:positive, :monotonic])
    key = "stream:batch:warmed-index:{#{tag}}"
    ctx = FerricStore.Instance.get(:default)
    on_exit(fn -> FerricStore.del(key) end)

    assert {:ok, seed_id} = FerricStore.xadd(key, ["value", "seed"])
    assert {:ok, [[^seed_id, "value", "seed"]]} = FerricStore.xrange(key, "-", "+")
    assert :ok = Ferricstore.Commands.Stream.Index.ensure(key, ctx)
    assert Ferricstore.Commands.Stream.Index.ready?(key, ctx)

    items =
      Enum.map(1..64, fn value ->
        {key, ["value", Integer.to_string(value)]}
      end)

    assert results = FerricStore.xadd_many(items)
    assert Enum.all?(results, &match?({:ok, id} when is_binary(id), &1))
    assert {:ok, 65} = FerricStore.xlen(key)
    assert {:ok, entries} = FerricStore.xrange(key, "-", "+")
    assert length(entries) == 65
    assert Enum.map(entries, &List.last/1) == ["seed" | Enum.map(1..64, &Integer.to_string/1)]

    {:ok, newest_id} = List.last(results)

    assert {:ok, [[^newest_id, "value", "64"]]} =
             FerricStore.xrevrange(key, newest_id, newest_id)
  end

  test "xadd_many keeps per-item errors without rolling back successful appends" do
    tag = System.unique_integer([:positive, :monotonic])
    stream_key = "stream:batch:partial:{#{tag}}"
    string_key = "string:batch:partial:{#{tag}}"
    on_exit(fn -> FerricStore.del([stream_key, string_key]) end)

    assert :ok = FerricStore.set(string_key, "not-a-stream")

    assert [{:ok, first_id}, {:error, wrongtype}, {:ok, second_id}] =
             FerricStore.xadd_many([
               {stream_key, ["value", "first"]},
               {string_key, ["value", "rejected"]},
               {stream_key, ["value", "second"]}
             ])

    assert wrongtype =~ "WRONGTYPE"
    assert stream_id_before?(first_id, second_id)
    assert {:ok, 2} = FerricStore.xlen(stream_key)
    assert {:ok, "not-a-stream"} = FerricStore.get(string_key)
  end

  test "xadd_many retains per-entry value-size enforcement in the validated plan" do
    snapshot = ShardHelpers.replace_default_apply_context(max_value_size: 160)
    on_exit(fn -> ShardHelpers.restore_default_apply_context(snapshot) end)

    key = "stream:batch:value-limit:{#{System.unique_integer([:positive, :monotonic])}}"
    on_exit(fn -> FerricStore.del(key) end)

    oversized = :binary.copy("x", 200)

    assert [{:ok, id}, {:error, reason}] =
             FerricStore.xadd_many([
               {key, ["payload", "small"]},
               {key, ["payload", oversized]}
             ])

    assert is_binary(id)
    assert reason =~ "value too large"
    assert {:ok, [[^id, "payload", "small"]]} = FerricStore.xrange(key, "-", "+")
  end

  test "xadd_many preserves active namespace accounting for every durable Stream row" do
    tag = System.unique_integer([:positive, :monotonic])
    scope = "stream-batch-usage-#{tag}"
    key_a = "#{scope}:events:a:{#{tag}}"
    key_b = "#{scope}:events:b:{#{tag}}"
    ctx = FerricStore.Instance.get(:default)
    now_ms = System.system_time(:millisecond)
    on_exit(fn -> FerricStore.del([key_a, key_b]) end)

    assert :ok = NamespaceUsage.ensure_scope(ctx, scope, now_ms)
    assert {:ok, %{keys: 0, bytes: 0}} = NamespaceUsage.usage(ctx, scope, now_ms)

    items =
      Enum.map(1..64, fn value ->
        key = if rem(value, 2) == 0, do: key_a, else: key_b
        {key, ["value", Integer.to_string(value)]}
      end)

    assert results = FerricStore.xadd_many(items)
    assert Enum.all?(results, &match?({:ok, id} when is_binary(id), &1))

    assert {:ok, %{keys: 2, bytes: bytes}} =
             NamespaceUsage.usage(ctx, scope, System.system_time(:millisecond))

    assert bytes > 0
    assert {:ok, 32} = FerricStore.xlen(key_a)
    assert {:ok, 32} = FerricStore.xlen(key_b)
  end

  test "xadd_many rejects malformed batches before applying any item" do
    tag = System.unique_integer([:positive, :monotonic])
    key = "stream:batch:invalid:{#{tag}}"
    on_exit(fn -> FerricStore.del(key) end)

    assert {:error, "ERR XADD_MANY" <> _} =
             FerricStore.xadd_many([
               {key, ["field", "valid"]},
               {key, ["missing-value"]}
             ])

    assert {:ok, 0} = FerricStore.xlen(key)
    assert [] = FerricStore.xadd_many([])
  end

  test "xadd_many validates the remainder after a batch changes streams" do
    tag = System.unique_integer([:positive, :monotonic])
    key_a = "stream:batch:mixed-invalid:#{tag}:a"
    key_b = "stream:batch:mixed-invalid:#{tag}:b"
    on_exit(fn -> FerricStore.del([key_a, key_b]) end)

    assert {:error, "ERR XADD_MANY" <> _} =
             FerricStore.xadd_many([
               {key_a, ["field", "valid-a"]},
               {key_b, ["field", "valid-b"]},
               {key_a, ["field", :not_binary]}
             ])

    assert {:ok, 0} = FerricStore.xlen(key_a)
    assert {:ok, 0} = FerricStore.xlen(key_b)
  end

  test "xadd_many preserves large-value externalization and waiter notification" do
    tag = System.unique_integer([:positive, :monotonic])
    key = "stream:batch:large:a:{#{tag}}"
    other_key = "stream:batch:large:b:{#{tag}}"
    large_value = :binary.copy("L", 300 * 1024)
    ctx = FerricStore.Instance.get(:default)

    on_exit(fn ->
      Stream.unregister_stream_waiter(key, self(), ctx)
      FerricStore.del([key, other_key])
    end)

    assert :ok = Stream.register_stream_waiter(key, self(), "0-0", ctx)

    assert [{:ok, first_id}, {:ok, other_id}, {:ok, second_id}] =
             FerricStore.xadd_many([
               {key, ["payload", large_value]},
               {other_key, ["payload", "other-topic"]},
               {key, ["payload", "small"]}
             ])

    assert_receive {:stream_waiter_notify, ^key}, 1_000
    assert stream_id_before?(first_id, second_id)

    assert {:ok,
            [
              [^first_id, "payload", ^large_value],
              [^second_id, "payload", "small"]
            ]} = FerricStore.xrange(key, "-", "+")

    assert {:ok, [[^other_id, "payload", "other-topic"]]} =
             FerricStore.xrange(other_key, "-", "+")
  end

  test "xadd_many cannot be used to write internal compound keys" do
    tag = System.unique_integer([:positive, :monotonic])
    key = "stream:batch:public:{#{tag}}"
    reserved = Ferricstore.Store.CompoundKey.type_key(key)
    on_exit(fn -> FerricStore.del(key) end)

    assert {:error, reason} =
             FerricStore.xadd_many([
               {key, ["field", "would-have-been-valid"]},
               {reserved, ["field", "forged"]}
             ])

    assert reason =~ "internal"
    assert {:ok, 0} = FerricStore.xlen(key)
  end

  test "xadd_many reports malformed input before reserved-key authorization" do
    key = "stream:batch:invalid-precedence"
    reserved = Ferricstore.Store.CompoundKey.type_key(key)

    assert {:error, "ERR XADD_MANY" <> _} =
             FerricStore.xadd_many([
               {reserved, ["field", "forged"]},
               {key, ["missing-value"]}
             ])
  end

  test "pending projection keeps only the final durable Stream metadata mutation" do
    meta = "XM:stream-batch-meta"

    assert [
             {:put, "T:stream-batch-meta", "stream", 0},
             {:put, "X:stream-batch-meta\0a", "entry-a", 0},
             {:put, "X:stream-batch-meta\0b", "entry-b", 0},
             {:put, ^meta, "final", 0}
           ] =
             Ferricstore.Raft.StateMachine.compact_pending_stream_meta_writes_for_test([
               {:put, "T:stream-batch-meta", "stream", 0},
               {:put, "X:stream-batch-meta\0a", "entry-a", 0},
               {:put, meta, "intermediate", 0},
               {:put, "X:stream-batch-meta\0b", "entry-b", 0},
               {:put, meta, "final", 0}
             ])
  end

  defp stream_id_before?(left, right) do
    {:ok, left_id} = Ferricstore.Commands.Stream.ID.parse_full_id(left)
    {:ok, right_id} = Ferricstore.Commands.Stream.ID.parse_full_id(right)
    left_id <= right_id
  end

  defp raft_position(shard_index) do
    {:ok, {:raft_log_pos, index, _term}} =
      Ferricstore.Raft.WARaftBackend.storage_position(shard_index)

    index
  end
end
