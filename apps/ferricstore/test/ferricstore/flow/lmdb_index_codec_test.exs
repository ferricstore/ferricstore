defmodule Ferricstore.Flow.LMDB.IndexCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB.IndexCodec
  alias Ferricstore.TermCodec

  test "terminal expiry rows derive their fields from the key and still read v1 values" do
    alias Ferricstore.Flow.Keys

    index_key = Keys.state_index_key("invoice.process", "completed", "tenant-a")
    state_key = Keys.state_key("order-1", "tenant-a")
    terminal_key = IndexCodec.terminal_index_key(index_key, "order-1", 1_791_750_926_967)
    count_key = IndexCodec.terminal_count_key(index_key)
    expire_key = IndexCodec.terminal_expire_key(1_792_355_726_967, terminal_key)

    compact = IndexCodec.encode_terminal_expire_value(terminal_key, state_key, count_key)
    legacy = TermCodec.encode({terminal_key, state_key, count_key})

    assert byte_size(compact) == 1

    for value <- [compact, legacy] do
      assert {:ok, {^terminal_key, ^state_key, ^count_key}} =
               IndexCodec.decode_terminal_expire_row(expire_key, value)
    end

    assert Enum.sort(IndexCodec.terminal_expire_values(terminal_key, state_key, count_key)) ==
             Enum.sort([compact, legacy])

    # Fields that the key cannot reproduce keep the self-describing v1 value.
    for {row_state_key, row_count_key} <- [
          {nil, count_key},
          {Keys.state_key("order-2", "tenant-a"), count_key},
          {state_key, IndexCodec.terminal_count_key(index_key <> "x")}
        ] do
      value = IndexCodec.encode_terminal_expire_value(terminal_key, row_state_key, row_count_key)
      assert value == TermCodec.encode({terminal_key, row_state_key, row_count_key})

      assert {:ok, {^terminal_key, ^row_state_key, ^row_count_key}} =
               IndexCodec.decode_terminal_expire_row(expire_key, value)

      assert IndexCodec.terminal_expire_values(terminal_key, row_state_key, row_count_key) == [
               value
             ]
    end

    long_id = String.duplicate("x", 300)
    long_terminal_key = IndexCodec.terminal_index_key(index_key, long_id, 1)
    long_state_key = Keys.state_key(long_id, "tenant-a")

    assert IndexCodec.encode_terminal_expire_value(long_terminal_key, long_state_key, count_key) ==
             TermCodec.encode({long_terminal_key, long_state_key, count_key})

    assert :error = IndexCodec.decode_terminal_expire_row("flow-terminal-expire:bad", compact)
    assert :error = IndexCodec.decode_terminal_expire_row(expire_key, <<0xC3, 0>>)
  end

  test "terminal index rows keep only the expiry when the key holds the other fields" do
    alias Ferricstore.Flow.Keys

    for partition_key <- ["tenant-a", nil] do
      index_key = Keys.state_index_key("invoice.process", "completed", partition_key)
      state_key = Keys.state_key("order-1", partition_key)
      terminal_key = IndexCodec.terminal_index_key(index_key, "order-1", 1_791_750_926_967)
      count_key = IndexCodec.terminal_count_key(index_key)

      compact =
        IndexCodec.encode_terminal_index_row(
          terminal_key,
          "order-1",
          1_791_750_926_967,
          1_792_355_726_967,
          state_key,
          count_key
        )

      legacy =
        IndexCodec.encode_terminal_index_value(
          "order-1",
          1_791_750_926_967,
          1_792_355_726_967,
          state_key,
          count_key
        )

      assert byte_size(compact) == 9

      for value <- [compact, legacy] do
        assert {:ok, {"order-1", 1_791_750_926_967, 1_792_355_726_967, ^state_key}} =
                 IndexCodec.decode_terminal_index_row(terminal_key, value)

        assert {:ok, {"order-1", 1_791_750_926_967, 1_792_355_726_967, ^state_key, ^count_key}} =
                 IndexCodec.decode_terminal_index_entry_row(terminal_key, value)

        assert {:ok, ^count_key} = IndexCodec.terminal_index_row_count_key(terminal_key, value)
      end

      # Fields the key cannot reproduce keep the self-describing v1 value.
      for {id, state_key_value} <- [{"order-1", nil}, {"order-2", state_key}] do
        assert IndexCodec.encode_terminal_index_row(
                 terminal_key,
                 id,
                 1,
                 2,
                 state_key_value,
                 count_key
               ) ==
                 IndexCodec.encode_terminal_index_value(id, 1, 2, state_key_value, count_key)
      end

      assert :error = IndexCodec.decode_terminal_index_row("flow-terminal-index:bad", compact)
      assert :error = IndexCodec.decode_terminal_index_row(terminal_key, <<0xC4, 1>>)
      assert :missing = IndexCodec.terminal_index_row_count_key(terminal_key, <<0xC4>>)
    end
  end

  test "all index value decoders reject trailing external-term bytes" do
    active_key = IndexCodec.active_index_key("index", "id", 10)

    values = [
      {IndexCodec.encode_history_expire_value("history-index"),
       &IndexCodec.decode_history_expire_value/1, :error},
      {IndexCodec.encode_history_flow_expire_value("history", 10),
       &IndexCodec.decode_history_flow_expire_value/1, :error},
      {IndexCodec.encode_active_index_value("index", "id", 10, 20, "state"),
       &IndexCodec.decode_active_index_value/1, :error},
      {IndexCodec.encode_active_index_reverse_value([active_key]),
       &IndexCodec.decode_active_index_reverse_value/1, :error},
      {IndexCodec.encode_terminal_expire_value("terminal", "state", "count"),
       &IndexCodec.decode_terminal_expire_value/1, :error},
      {IndexCodec.encode_count(1), &IndexCodec.decode_count/1, :error},
      {IndexCodec.encode_terminal_index_value("id", 10, 20, "state", "count"),
       &IndexCodec.decode_terminal_index_value/1, :error},
      {IndexCodec.encode_terminal_index_value("id", 10, 20, "state", "count"),
       &IndexCodec.decode_terminal_index_entry/1, :error},
      {IndexCodec.encode_query_index_value("index", "id", 10, 20, "state"),
       &IndexCodec.decode_query_index_value/1, :error},
      {IndexCodec.encode_history_index_value("event", 10, "compound", 20),
       &IndexCodec.decode_history_index_value/1, :error},
      {IndexCodec.encode_history_index_value("event", 10, "compound", 20),
       &IndexCodec.decode_history_index_location/1, :error},
      {IndexCodec.encode_terminal_index_value("id", 10, 20, "state", "count"),
       &IndexCodec.terminal_index_count_key/1, :missing}
    ]

    for {encoded, decoder, invalid} <- values do
      assert decoder.(encoded <> <<0>>) == invalid
    end
  end

  test "history index decoder rejects compressed external terms" do
    encoded =
      IndexCodec.encode_history_index_value(
        String.duplicate("event", 1_024),
        10,
        String.duplicate("compound", 1_024),
        20
      )

    compressed =
      encoded |> :erlang.binary_to_term([:safe]) |> :erlang.term_to_binary(compressed: 9)

    assert <<131, 80, _rest::binary>> = compressed
    assert :error = IndexCodec.decode_history_index_value(compressed)
  end

  test "active reverse index accepts only bounded unique active-index keys" do
    active_keys =
      for score <- 1..6 do
        IndexCodec.active_index_key("index", "id-#{score}", score)
      end

    encoded = IndexCodec.encode_active_index_reverse_value(active_keys)

    assert {:ok, {:flow_active_reverse, ^active_keys, nil}} = TermCodec.decode(encoded)
    assert {:ok, ^active_keys} = IndexCodec.decode_active_index_reverse_value(encoded)
    assert :missing = IndexCodec.decode_active_index_reverse_lane_value(encoded)

    assert :error =
             active_keys
             |> TermCodec.encode()
             |> IndexCodec.decode_active_index_reverse_value()

    malformed = TermCodec.encode([hd(active_keys), 42, List.last(active_keys)])
    assert :error = IndexCodec.decode_active_index_reverse_value(malformed)

    for invalid <- [
          ["flow-terminal-count:index"],
          [hd(active_keys), hd(active_keys)],
          active_keys ++ [IndexCodec.active_index_key("index", "overflow", 7)]
        ] do
      assert :error =
               invalid
               |> TermCodec.encode()
               |> IndexCodec.decode_active_index_reverse_value()

      assert_raise ArgumentError, fn ->
        IndexCodec.encode_active_index_reverse_value(invalid)
      end
    end
  end

  test "active reverse metadata accepts the full due-any running projection" do
    record = %{
      id: "flow-id",
      type: "job",
      state: "running",
      run_state: "queued",
      state_enter_seq: 7,
      updated_at_ms: 10,
      next_run_at_ms: 20,
      lease_deadline_ms: 20,
      lease_owner: "worker-a",
      created_at_ms: 1,
      max_active_ms: 100,
      priority: 0,
      partition_key: "tenant-a"
    }

    active_keys =
      record
      |> Ferricstore.Flow.LMDB.active_projection_entries(due_any?: true)
      |> Enum.map(fn {index_key, id, score} ->
        Ferricstore.Flow.LMDB.active_index_key(index_key, id, score)
      end)

    assert length(active_keys) == 6
    lane_entry = Ferricstore.Flow.FifoLane.index_entry(record)

    encoded = IndexCodec.encode_active_index_reverse_value(active_keys, lane_entry)
    assert {:ok, ^active_keys} = IndexCodec.decode_active_index_reverse_value(encoded)
  end

  test "terminal index decoding requires the exact counted schema" do
    count_less = TermCodec.encode({"id", 10, 20, "state"})
    encoded = IndexCodec.encode_terminal_index_value("id", 10, 20, "state", "count")

    assert {:ok, {"id", 10, 20, "state", "count"}} =
             IndexCodec.decode_terminal_index_entry(encoded)

    assert :error = IndexCodec.decode_terminal_index_value(count_less)
    assert :error = IndexCodec.decode_terminal_index_entry(count_less)
    assert :missing = IndexCodec.terminal_index_count_key(count_less)
  end

  test "index decoders reject negative stored time fields" do
    assert :error =
             TermCodec.encode({"history", -1})
             |> IndexCodec.decode_history_flow_expire_value()

    assert :error =
             TermCodec.encode({"index", "id", -1, 0, "state"})
             |> IndexCodec.decode_active_index_value()

    assert :error =
             TermCodec.encode({"id", -1, 0, "state", "count"})
             |> IndexCodec.decode_terminal_index_value()

    assert :error =
             TermCodec.encode({"id", 0, -1, "state"})
             |> IndexCodec.decode_query_index_value()

    assert :error =
             TermCodec.encode({"event", -1, 0, "compound"})
             |> IndexCodec.decode_history_index_value()
  end

  test "ordered index keys reject negative or partially parsed time values" do
    for build <- [
          fn -> IndexCodec.terminal_index_key("index", "id", -1) end,
          fn -> IndexCodec.active_index_key("index", "id", -1) end,
          fn -> IndexCodec.query_index_key("index", "id", -1) end,
          fn -> IndexCodec.query_index_key("index", "id", -1.5) end,
          fn -> IndexCodec.query_index_key("index", "id", "1.5ms") end,
          fn -> IndexCodec.history_index_key("history", "event", -1) end,
          fn -> IndexCodec.query_index_key("index", "id", 18_446_744_073_709_551_616) end
        ] do
      assert_raise ArgumentError, build
    end
  end

  test "query index values reject coercion and out-of-range timestamps" do
    for updated_at_ms <- [-1, 1.5, "10", "1.5ms", nil, 18_446_744_073_709_551_616] do
      assert_raise ArgumentError, fn ->
        IndexCodec.encode_query_index_value("index", "id", updated_at_ms, 0, "state")
      end
    end

    for expire_at_ms <- [-1, 1.5, "10", 18_446_744_073_709_551_616] do
      assert_raise ArgumentError, fn ->
        IndexCodec.encode_query_index_value("index", "id", 10, expire_at_ms, "state")
      end
    end
  end

  test "positive invalid expirations cannot silently become immortal index rows" do
    assert IndexCodec.terminal_expire_key(0, "terminal") == nil
    assert IndexCodec.history_expire_key(0, "history-index") == nil
    assert IndexCodec.history_flow_expire_key(0, "history") == nil

    for build <- [
          fn -> IndexCodec.terminal_expire_key(-1, "terminal") end,
          fn -> IndexCodec.history_expire_key(-1, "history-index") end,
          fn -> IndexCodec.history_flow_expire_key(-1, "history") end,
          fn ->
            IndexCodec.terminal_expire_key(18_446_744_073_709_551_616, "terminal")
          end
        ] do
      assert_raise ArgumentError, build
    end
  end

  test "terminal expiry encoding rejects state keys its decoder cannot read" do
    assert_raise ArgumentError, fn ->
      IndexCodec.encode_terminal_expire_value("terminal", 123, "count")
    end
  end

  test "persisted index decoders reject integers outside the unsigned 64-bit schema" do
    too_large = 18_446_744_073_709_551_616

    assert :error =
             TermCodec.encode({"index", "id", too_large, 0, "state"})
             |> IndexCodec.decode_active_index_value()

    assert :error =
             TermCodec.encode({"id", too_large, 0, "state", "count"})
             |> IndexCodec.decode_terminal_index_value()

    assert :error =
             TermCodec.encode({"id", 0, too_large, "state"})
             |> IndexCodec.decode_query_index_value()

    assert :error =
             TermCodec.encode({"event", too_large, 0, "compound"})
             |> IndexCodec.decode_history_index_value()

    assert :error = too_large |> TermCodec.encode() |> IndexCodec.decode_count()
  end
end
