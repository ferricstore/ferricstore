defmodule Ferricstore.Flow.HistoryValuesTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryValues
  alias Ferricstore.Store.ReadResult

  test "history hydration marks storage failures without exposing their reason" do
    failure = ReadResult.failure({:missing_file, "/private/secret/path"})

    assert %{"payload_error" => "ERR storage read failed"} =
             HistoryValues.__apply_value_result_for_test__(
               %{"payload_ref" => "ref"},
               "payload",
               "ref",
               failure,
               10
             )
  end

  test "history file-reference size is the exact stored byte count" do
    assert %{"payload_omitted" => true, "payload_size" => 100} =
             HistoryValues.__apply_value_result_for_test__(
               %{"payload_ref" => "ref"},
               "payload",
               "ref",
               {:file_ref, "/segment", 10, 100},
               10
             )
  end

  test "history hydration marks corrupted typed values" do
    corrupted = "FSV2" <> <<2>> <> "not-an-external-term"

    hydrated =
      HistoryValues.__apply_value_result_for_test__(
        %{"payload_ref" => "ref"},
        "payload",
        "ref",
        corrupted,
        1_024
      )

    assert hydrated["payload_error"] == "ERR invalid flow value"
    refute Map.has_key?(hydrated, "payload")
  end
end
