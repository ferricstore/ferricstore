defmodule Ferricstore.Flow.Query.CoveringCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{CoveringCodec, Field}
  alias Ferricstore.TermCodec

  @id "run-cover"
  @version 42

  test "publishes one exact covering schema for every supported run builtin" do
    expected =
      Field.supported_external_names()
      |> Enum.reject(&String.contains?(&1, "<"))
      |> Enum.map(fn name ->
        assert {:ok, field} = Field.parse(name)
        field
      end)
      |> List.delete(:event_id)
      |> Enum.sort()

    assert CoveringCodec.supported_fields() |> Enum.sort() == expected
    assert Enum.all?(expected, &CoveringCodec.supported_field?/1)
    refute CoveringCodec.supported_field?(:event_id)
    assert CoveringCodec.supported_field?({:attribute, "tier"})
    assert CoveringCodec.supported_field?({:state_meta, "queued", "worker"})
  end

  test "round trips dynamic attributes and state metadata without creating atoms" do
    tier = {:attribute, "tier"}
    labels = {:attribute, "labels"}
    active = {:attribute, "active"}
    score = {:state_meta, "queued", "score"}
    nullable = {:state_meta, "queued", "nullable"}

    record = %{
      tier => "gold",
      labels => ["urgent", "finance"],
      active => true,
      score => 1.5,
      nullable => nil,
      id: @id,
      version: @version,
      partition_key: "tenant-a"
    }

    assert {:ok, encoded} = CoveringCodec.encode(record)
    assert {:ok, ^record} = CoveringCodec.decode(encoded, @id, @version)
  end

  test "preserves an unpartitioned run as an explicit null partition" do
    record = %{
      id: @id,
      version: @version,
      partition_key: nil,
      state: "queued"
    }

    assert {:ok, encoded} = CoveringCodec.encode(record)
    assert {:ok, ^record} = CoveringCodec.decode(encoded, @id, @version)
  end

  test "dynamic field encoding is deterministic across insertion order" do
    pairs = [
      {:id, @id},
      {:version, @version},
      {:partition_key, "tenant-a"},
      {{:attribute, "tier"}, "gold"},
      {{:state_meta, "queued", "worker"}, "worker-1"}
    ]

    first = Map.new(pairs)
    second = pairs |> Enum.reverse() |> Map.new()

    assert {:ok, encoded} = CoveringCodec.encode(first)
    assert {:ok, ^encoded} = CoveringCodec.encode(second)
  end

  test "round trips every supported field and injects authoritative identity" do
    record = %{
      id: @id,
      type: "invoice\0priority",
      state: "queued",
      version: @version,
      priority: -10,
      partition_key: "tenant-a",
      created_at_ms: 1,
      updated_at_ms: 2,
      next_run_at_ms: 3,
      lease_deadline_ms: nil,
      attempts: 4,
      run_state: "ready",
      max_active_ms: 5,
      parent_flow_id: nil,
      root_flow_id: "root",
      correlation_id: "correlation"
    }

    assert {:ok, encoded} = CoveringCodec.encode(record)
    assert {:ok, ^record} = CoveringCodec.decode(encoded, @id, @version)

    assert {:ok, decoded} = CoveringCodec.decode(encoded, "authoritative", 99)
    assert decoded.id == "authoritative"
    assert decoded.version == 99
  end

  test "encoding is deterministic across map insertion order" do
    pairs = [
      id: @id,
      version: @version,
      partition_key: "tenant-a",
      state: "queued",
      updated_at_ms: 100
    ]

    first = Map.new(pairs)
    second = pairs |> Enum.reverse() |> Map.new()

    assert {:ok, encoded} = CoveringCodec.encode(first)
    assert {:ok, ^encoded} = CoveringCodec.encode(second)
  end

  test "decoded large fields do not retain unrelated covering payload bytes" do
    record = %{
      id: @id,
      version: @version,
      partition_key: :binary.copy("p", 10_000),
      type: :binary.copy("t", 100)
    }

    assert {:ok, encoded} = CoveringCodec.encode(record)
    assert {:ok, decoded} = CoveringCodec.decode(encoded, @id, @version)

    assert :binary.referenced_byte_size(decoded.type) == byte_size(decoded.type)
    assert :binary.referenced_byte_size(decoded.partition_key) == byte_size(decoded.partition_key)
  end

  test "compact payload is smaller than the previous bounded ETF envelope" do
    record = %{
      id: @id,
      version: @version,
      partition_key: "tenant-a",
      state: "queued",
      updated_at_ms: 100
    }

    assert {:ok, compact} = CoveringCodec.encode(record)
    etf = TermCodec.encode({:flow_composite_covering, 1, record})
    assert byte_size(compact) * 2 < byte_size(etf)
  end

  test "accepts exactly the authoritative dynamic metadata value domain" do
    base = %{id: @id, version: @version, partition_key: "tenant-a"}
    attribute = {:attribute, "value"}
    state_meta = {:state_meta, "queued", "value"}

    for value <- [
          nil,
          false,
          true,
          -0x8000_0000_0000_0000,
          0x7FFF_FFFF_FFFF_FFFF,
          -0.0,
          1.5,
          "",
          "utf8-\u20ac\0",
          :binary.copy("x", 256)
        ],
        field <- [attribute, state_meta] do
      record = Map.put(base, field, value)
      assert {:ok, encoded} = CoveringCodec.encode(record)
      assert {:ok, ^record} = CoveringCodec.decode(encoded, @id, @version)
    end

    values = Enum.map(1..16, &"tag-#{&1}")
    record = Map.put(base, attribute, values)
    assert {:ok, encoded} = CoveringCodec.encode(record)
    assert {:ok, ^record} = CoveringCodec.decode(encoded, @id, @version)

    for {field, invalid} <- [
          {attribute, []},
          {attribute, ["duplicate", "duplicate"]},
          {attribute, ["valid", 1]},
          {attribute, Enum.map(1..17, &"tag-#{&1}")},
          {attribute, :binary.copy("x", 257)},
          {state_meta, ["not-scalar"]},
          {state_meta, :binary.copy("x", 257)}
        ] do
      assert :error = CoveringCodec.encode(Map.put(base, field, invalid))
    end
  end

  test "rejects unsupported fields and values before persistence" do
    base = %{id: @id, version: @version, partition_key: "tenant-a"}

    assert :error = CoveringCodec.encode(Map.put(base, :attributes, %{}))
    assert :error = CoveringCodec.encode(Map.put(base, :priority, 0x8000_0000_0000_0000))
    assert :error = CoveringCodec.encode(Map.put(base, :state, [:invalid]))
    assert :error = CoveringCodec.encode(%{partition_key: "tenant-a"})
  end

  test "rejects truncation, trailing data, unknown bitmap bits, and invalid identity" do
    assert {:ok, encoded} =
             CoveringCodec.encode(%{
               id: @id,
               version: @version,
               partition_key: "tenant-a",
               state: "queued"
             })

    for bytes <- 0..(byte_size(encoded) - 1) do
      assert :error = CoveringCodec.decode(binary_part(encoded, 0, bytes), @id, @version)
    end

    assert :error = CoveringCodec.decode(encoded <> <<0>>, @id, @version)
    assert :error = CoveringCodec.decode(<<1, 0xFFFF::unsigned-16>>, @id, @version)
    assert :error = CoveringCodec.decode(encoded, "", @version)
    assert :error = CoveringCodec.decode(encoded, @id, -1)
  end
end
