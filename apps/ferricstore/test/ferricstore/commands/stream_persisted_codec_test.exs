defmodule Ferricstore.Commands.StreamPersistedCodecTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Commands.Stream.{Entries, Groups, Meta}
  alias Ferricstore.Store.{CompoundKey, ReadResult}

  test "absent stream metadata and consumer groups remain missing" do
    store = %{compound_get: fn "stream", _compound_key -> nil end}

    assert nil == Meta.durable_entry("stream", store)
    assert :missing == Groups.lookup(store, "stream", "group")
  end

  test "stream entries reject compressed and trailing external terms" do
    fields = Enum.flat_map(1..200, &["field-#{&1}", :binary.copy("value", 20)])

    for raw <- [
          :erlang.term_to_binary(fields, compressed: 9),
          :erlang.term_to_binary(fields) <> <<0>>
        ] do
      assert :error = Entries.decode_fields(raw)
    end
  end

  test "stream entries reject invalid field-value shapes" do
    for fields <- [[], ["field"], [1, "value"], ["field", %{value: true}]] do
      assert :error = Entries.decode_fields(Ferricstore.TermCodec.encode(fields))
    end
  end

  test "durable stream metadata rejects compressed and trailing external terms" do
    metadata =
      {:stream_meta, 1, :binary.copy("1-0", 400), :binary.copy("1-0", 400), 1, 0}

    for raw <- [
          :erlang.term_to_binary(metadata, compressed: 9),
          :erlang.term_to_binary(metadata) <> <<0>>
        ] do
      store = %{compound_get: fn "stream", _compound_key -> raw end}

      assert ReadResult.failure(:invalid_stream_metadata) ==
               Meta.durable_entry("stream", store)
    end
  end

  test "durable stream metadata rejects inconsistent structural fields" do
    invalid_metadata = [
      {:stream_meta, 1, "not-an-id", "1-0", 1, 0},
      {:stream_meta, 1, "2-0", "1-0", 1, 0},
      {:stream_meta, 1, "1-0", "2-0", 1, 0},
      {:stream_meta, 0, "non-empty-first", "2-0", 2, 0}
    ]

    for metadata <- invalid_metadata do
      raw = Ferricstore.TermCodec.encode(metadata)
      store = %{compound_get: fn "stream", _compound_key -> raw end}

      assert ReadResult.failure(:invalid_stream_metadata) ==
               Meta.durable_entry("stream", store)
    end
  end

  test "metadata rebuild propagates corrupt durable metadata" do
    key = "stream-corrupt-meta-#{System.unique_integer([:positive])}"
    type_key = CompoundKey.type_key(key)
    meta_key = CompoundKey.stream_meta_key(key)
    raw = Ferricstore.TermCodec.encode({:stream_meta, 1, "not-an-id", "1-0", 1, 0})

    store = %{
      compound_get: fn
        ^key, ^type_key -> "stream"
        ^key, ^meta_key -> raw
      end,
      compound_scan: fn ^key, _prefix -> [] end
    }

    Meta.cleanup_local(key, store)
    on_exit(fn -> Meta.cleanup_local(key, store) end)

    assert ReadResult.failure(:invalid_stream_metadata) == Meta.entries(key, store)
  end

  test "consumer-group state rejects compressed and trailing external terms" do
    consumers = Map.new(1..200, &{"consumer-#{&1}", %{seen: &1}})
    state = {:stream_group, 1, "1-0", consumers, %{}}

    for raw <- [
          :erlang.term_to_binary(state, compressed: 9),
          :erlang.term_to_binary(state) <> <<0>>
        ] do
      key = "stream-#{System.unique_integer([:positive])}"
      store = %{compound_get: fn ^key, _compound_key -> raw end}
      assert {:error, "ERR storage read failed"} == Groups.lookup(store, key, "group")

      assert {:error, "ERR storage read failed"} ==
               Stream.handle_ast({:xack, key, "group", ["1-0"]}, store)
    end
  end

  test "consumer-group state rejects invalid IDs and map values" do
    invalid_states = [
      {:stream_group, 1, "not-an-id", %{}, %{}},
      {:stream_group, 1, "1-0", %{"consumer" => "not-a-timestamp"}, %{}},
      {:stream_group, 1, "1-0", %{}, %{"not-an-id" => {"consumer", 1}}},
      {:stream_group, 1, "1-0", %{}, %{"1-0" => {:not_a_consumer, 1}}}
    ]

    for state <- invalid_states do
      key = "stream-#{System.unique_integer([:positive])}"
      raw = Ferricstore.TermCodec.encode(state)
      store = %{compound_get: fn ^key, _compound_key -> raw end}
      assert {:error, "ERR storage read failed"} == Groups.lookup(store, key, "group")

      assert {:error, "ERR storage read failed"} ==
               Stream.handle_ast({:xack, key, "group", ["1-0"]}, store)
    end
  end

  test "pending growth bound covers deterministic consumer-group encoding" do
    consumer = String.duplicate("consumer", 16)
    timestamp = 18_446_744_073_709_551_615
    ids = Enum.map(1..32, &"18446744073709551615-#{&1}")

    before = Ferricstore.TermCodec.encode({:stream_group, 1, "0-0", %{}, %{}})

    pending = Map.new(ids, &{&1, {consumer, timestamp}})

    after_value =
      Ferricstore.TermCodec.encode(
        {:stream_group, 1, List.last(ids), %{consumer => timestamp}, pending}
      )

    assert byte_size(after_value) - byte_size(before) <=
             Groups.pending_growth_bound(consumer, length(ids))
  end
end
