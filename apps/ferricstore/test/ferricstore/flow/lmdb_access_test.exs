defmodule Ferricstore.Flow.LMDB.AccessTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB.Access

  test "get_many normalization requires one valid result per key" do
    assert {:ok, [{:ok, "a"}, :not_found]} =
             Access.__normalize_get_many_result_for_test__(
               {:ok, [{:ok, "a"}, :not_found]},
               2
             )

    assert {:error, {:batch_result_mismatch, 2, 1}} =
             Access.__normalize_get_many_result_for_test__({:ok, [{:ok, "a"}]}, 2)

    assert {:error, {:invalid_batch_result, 1, nil}} =
             Access.__normalize_get_many_result_for_test__({:ok, [{:ok, "a"}, nil]}, 2)
  end

  test "get_many normalization preserves native errors and rejects invalid envelopes" do
    assert {:error, :busy} = Access.__normalize_get_many_result_for_test__({:error, :busy}, 1)

    assert {:error, {:invalid_batch_envelope, :bad_reply}} =
             Access.__normalize_get_many_result_for_test__(:bad_reply, 1)
  end

  test "get_many rejects malformed keys even when the environment is absent" do
    assert {:error, :badarg} = Access.get_many("missing-lmdb-path", ["valid", 123])
  end
end
