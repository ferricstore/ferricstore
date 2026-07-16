defmodule Ferricstore.Flow.LMDB.SegmentPinsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.TermCodec

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_segment_pins_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    %{path: path}
  end

  test "pin scans fail closed on compressed and trailing batch values", %{path: path} do
    file_id = {:waraft_segment, 1}
    entries = [{String.duplicate("value", 1_024), 0, 10, 20}]
    encoded = LMDB.encode_segment_value_pin_batch(file_id, entries)
    term = :erlang.binary_to_term(encoded, [:safe])
    compressed = :erlang.term_to_binary(term, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed
    {:put, pin_key, ^encoded} = pin_op([{elem(hd(entries), 0), 0, file_id, 10, 20}])

    for value <- [compressed, encoded <> <<0>>] do
      assert :ok = LMDB.write_batch(path, [{:put, pin_key, value}])

      assert {:error, {:corrupt_flow_segment_value_pin, ^pin_key}} =
               LMDB.segment_value_pin_entries_before(path, 2, 10)

      assert :ok = LMDB.write_batch(path, [{:delete, pin_key}])
    end
  end

  test "pin scans reject an entire batch when one entry is malformed", %{path: path} do
    file_id = {:waraft_segment, 1}

    {:put, pin_key, _valid} =
      pin_op([
        {"valid", 0, file_id, 10, 20},
        {"invalid", 0, file_id, 10, 20}
      ])

    value =
      TermCodec.encode(
        {:flow_segment_value_pin_batch, 1, file_id,
         [{"valid", 0, 10, 20}, {"invalid", -1, 10, 20}]}
      )

    assert :ok = LMDB.write_batch(path, [{:put, pin_key, value}])

    assert {:error, {:corrupt_flow_segment_value_pin, ^pin_key}} =
             LMDB.segment_value_pin_entries_before(path, 2, 10)
  end

  test "paged pin scans bound decoded pins rather than only LMDB rows", %{path: path} do
    file_id = {:waraft_segment, 1}
    entries = for index <- 1..3, do: {"value-#{index}", 0, index, 1}

    {:put, pin_key, value} =
      pin_op(
        Enum.map(entries, fn {key, expire, offset, size} ->
          {key, expire, file_id, offset, size}
        end)
      )

    assert :ok = LMDB.write_batch(path, [{:put, pin_key, value}])

    assert {:error, {:flow_segment_value_pin_scan_limit, 2}} =
             LMDB.segment_value_pin_entries_before_page(path, 2, <<>>, 2)
  end

  test "pin batch construction rejects invalid entries instead of dropping them" do
    assert_raise ArgumentError, fn ->
      LMDB.segment_value_pin_batch_put_ops([
        {"valid", 0, {:waraft_segment, 1}, 10, 20},
        {"invalid", -1, {:waraft_segment, 1}, 10, 20}
      ])
    end
  end

  test "future pins in one family do not hide older pins in the other family", %{path: path} do
    {:put, future_key, future_value} =
      pin_op([{"future", 0, {:waraft_apply_projection, 10}, 10, 20}])

    {:put, old_key, old_value} =
      pin_op([{"old", 0, {:waraft_segment, 1}, 30, 40}])

    assert :ok =
             LMDB.write_batch(path, [
               {:put, future_key, future_value},
               {:put, old_key, old_value}
             ])

    assert {:ok, [%{key: "old", file_id: {:waraft_segment, 1}}]} =
             LMDB.segment_value_pin_entries_before(path, 5, 10)

    assert {:ok, [%{key: "old"}], _cursor, true} =
             LMDB.segment_value_pin_entries_before_page(path, 5, <<>>, 10)
  end

  test "paged pin scans advance across both pin families exactly once", %{path: path} do
    {:put, apply_key, apply_value} =
      pin_op([{"apply", 0, {:waraft_apply_projection, 1}, 10, 20}])

    {:put, segment_key, segment_value} =
      pin_op([{"segment", 0, {:waraft_segment, 2}, 30, 40}])

    assert :ok =
             LMDB.write_batch(path, [
               {:put, apply_key, apply_value},
               {:put, segment_key, segment_value}
             ])

    assert {:ok, [first], cursor, false} =
             LMDB.segment_value_pin_entries_before_page(path, 5, <<>>, 1)

    assert {:ok, [second], _cursor, true} =
             LMDB.segment_value_pin_entries_before_page(path, 5, cursor, 1)

    assert MapSet.new([first.key, second.key]) == MapSet.new(["apply", "segment"])
  end

  defp pin_op(entries) do
    prefix = LMDB.segment_value_pin_prefix()

    entries
    |> LMDB.segment_value_pin_batch_put_ops()
    |> Enum.find(fn
      {:put, key, _value} -> String.starts_with?(key, prefix)
      _other -> false
    end)
  end
end
