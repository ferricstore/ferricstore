defmodule Ferricstore.Flow.LMDB.IndexCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB.IndexCodec
  alias Ferricstore.TermCodec

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

    assert :error = IndexCodec.decode_terminal_index_value(count_less)
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
