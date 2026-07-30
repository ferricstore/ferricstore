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
    common_fields = ["field", "value"]
    assert :error = Entries.decode_fields(:erlang.term_to_binary(common_fields) <> <<0>>)

    large_fields = Enum.flat_map(1..200, &["field-#{&1}", :binary.copy("value", 20)])

    for raw <- [
          :erlang.term_to_binary(large_fields, compressed: 9),
          :erlang.term_to_binary(large_fields) <> <<0>>
        ] do
      assert :error = Entries.decode_fields(raw)
    end
  end

  test "stream field encoder preserves the existing deterministic ETF bytes" do
    for fields <- [
          ["field", "value"],
          ["field", "", <<0, 1, 2>>, :binary.copy("value", 32)]
        ] do
      encoded = Entries.encode_fields(fields)

      assert encoded == Ferricstore.TermCodec.encode(fields)
      assert {:ok, ^fields} = Entries.decode_fields(encoded)
    end
  end

  test "stream field decoder remains compatible with existing ETF rows" do
    fields = ["field", "value", "binary", <<0, 1, 2>>]
    assert {:ok, ^fields} = Entries.decode_fields(Ferricstore.TermCodec.encode(fields))
  end

  test "stream metadata encoder preserves the existing deterministic ETF bytes" do
    key = <<"stream%", 0, "metadata">>
    metadata = {:stream_meta, 2, 12, "100-0", "100-11", 100, 11}
    meta_key = CompoundKey.stream_meta_key(key)

    assert {^meta_key, encoded, 0} =
             Meta.serialized_put_entry_with_key(
               meta_key,
               12,
               "100-0",
               "100-11",
               100,
               11
             )

    assert encoded == Ferricstore.TermCodec.encode(metadata)
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
      {:stream_group, 1, "1-0", %{}, %{"1-0" => {:not_a_consumer, 1}}},
      {:stream_group, 2, "not-an-id"},
      {:stream_group, 2, "1-0", %{}}
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

  test "split consumer-group state rejects corrupt pending records" do
    key = "stream-split-corrupt-#{System.unique_integer([:positive])}"
    group = "workers"
    group_key = CompoundKey.stream_group(key, group)
    pending_root = CompoundKey.stream_pending_prefix(key)
    pending_key = CompoundKey.stream_pending(key, group, "1-0")
    pending_member = CompoundKey.extract_subkey(pending_key, pending_root)
    consumer_root = CompoundKey.stream_consumer_prefix(key)
    group_state = Ferricstore.TermCodec.encode({:stream_group, 2, "1-0"})
    corrupt_pending = Ferricstore.TermCodec.encode({:stream_pending, 1, :not_binary, 1})

    store = %{
      compound_get: fn
        ^key, ^group_key -> group_state
        ^key, ^pending_key -> corrupt_pending
        ^key, _other -> nil
      end,
      compound_scan: fn
        ^key, ^pending_root -> [{pending_member, corrupt_pending}]
        ^key, ^consumer_root -> []
      end
    }

    Groups.delete_group_local(store, key, group)
    assert {:error, "ERR storage read failed"} == Groups.lookup(store, key, group)
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
