defmodule Ferricstore.Flow.Transfer.RetagTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Codec, Digest, Keys}
  alias Ferricstore.Flow.Transfer.Retag

  setup do
    previous = Digest.format()
    on_exit(fn -> Digest.put_format(previous) end)
    :ok
  end

  defp in_format(format, fun) do
    :ok = Digest.put_format(format)
    fun.()
  end

  defp keys_for(partition_key) do
    [
      Keys.state_key("run-1", partition_key),
      Keys.value_key("run-1", :payload, 3, partition_key),
      Keys.value_key("run-1", :result, 4, partition_key),
      Keys.value_key("shared-1", :shared, 1, partition_key),
      Keys.named_shared_value_key("run-1", "doc", 2, partition_key),
      Keys.shared_value_link_key("run-1", "doc", 2, partition_key),
      Keys.signal_idempotency_key("run-1", "nudge-1", partition_key)
    ]
  end

  test "format 1 keys retag to the keys format 2 derives from the same inputs" do
    for partition_key <- ["tenant-a", "tenant:with:colons", nil] do
      legacy = in_format(1, fn -> keys_for(partition_key) end)
      expected = in_format(2, fn -> keys_for(partition_key) end)

      assert Enum.map(legacy, &Retag.key(&1, 1, 2)) == Enum.map(expected, &{:ok, &1})
    end
  end

  test "keys stay as they are within one format" do
    key = in_format(1, fn -> Keys.state_key("run-1", "tenant-a") end)
    assert Retag.key(key, 1, 1) == {:ok, key}
  end

  test "format 2 keys cannot become format 1 keys, and a format 1 source must hold format 1 keys" do
    short = in_format(2, fn -> Keys.state_key("run-1", "tenant-a") end)
    assert {:error, _reason} = Retag.key(short, 2, 1)
    assert {:error, _reason} = Retag.key(short, 1, 2)
  end

  test "records retag their value refs" do
    {record, expected} =
      for format <- [1, 2] do
        in_format(format, fn ->
          %{
            id: "run-1",
            partition_key: "tenant-a",
            payload_ref: Keys.value_key("run-1", :payload, 1, "tenant-a"),
            result_ref: Keys.value_key("run-1", :result, 3, "tenant-a"),
            error_ref: nil,
            value_refs: %{
              "doc" => %{
                ref: Keys.named_shared_value_key("run-1", "doc", 2, "tenant-a"),
                version: 2
              },
              "shared" => %{ref: Keys.value_key("shared-9", :shared, 1, "tenant-a"), version: 1}
            },
            attributes: %{"note" => "f:{f:" <> String.duplicate("A", 43) <> "}:s:not-a-ref"}
          }
        end)
      end
      |> List.to_tuple()

    assert Retag.record(record, 1, 2) == {:ok, expected}
  end

  test "history snapshots retag stored refs and keep everything else byte for byte" do
    build = fn ->
      record = %{
        id: "run-1",
        type: "invoice",
        state: "completed",
        version: 4,
        partition_key: "tenant-a",
        created_at_ms: 1_000,
        updated_at_ms: 2_000,
        payload_ref: Keys.value_key("run-1", :payload, 1, "tenant-a"),
        result_ref: Keys.value_key("run-1", :result, 4, "tenant-a"),
        error_ref: Keys.value_key("run-1", :error, 3, "tenant-a"),
        value_refs: %{
          "doc" => %{ref: Keys.named_shared_value_key("run-1", "doc", 2, "tenant-a"), version: 2}
        },
        attributes: %{"region" => "eu"}
      }

      meta = %{"signal_payload_ref" => Keys.value_key("run-1", :payload, 2, "tenant-a")}
      Codec.encode_history_fields(record, "completed", 2_000, meta)
    end

    legacy = in_format(1, build)
    expected = in_format(2, build)

    assert {:ok, snapshot} = Codec.decode_history_snapshot(legacy)
    assert {:ok, retagged} = Retag.history_snapshot(snapshot, 1, 2)
    assert Codec.encode_history_snapshot(retagged) == expected
  end
end
