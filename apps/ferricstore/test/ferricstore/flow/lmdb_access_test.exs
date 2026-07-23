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

  test "prefix-bounded normalization requires truthful progress and byte accounting" do
    normalize = &Access.__normalize_get_many_prefix_bounded_result_for_test__(&1, 2, 10)

    assert {:ok, [{:ok, "abc"}], 3, false} = normalize.({:ok, [{:ok, "abc"}], 3, false})

    assert {:ok, [{:ok, "abc"}, :not_found], 3, true} =
             normalize.({:ok, [{:ok, "abc"}, :not_found], 3, true})

    for invalid <- [
          {:ok, [], 0, false},
          {:ok, [{:ok, "abc"}], 3, true},
          {:ok, [{:ok, "abc"}, :not_found], 3, false}
        ] do
      assert {:error, {:batch_result_mismatch, 2, _returned}} = normalize.(invalid)
    end

    assert {:error, :invalid_batch_value_bytes} =
             normalize.({:ok, [{:ok, "abc"}], 2, false})

    assert {:error, {:invalid_batch_result, 0, nil}} =
             normalize.({:ok, [nil], 0, false})
  end

  test "get_many rejects malformed keys even when the environment is absent" do
    assert {:error, :badarg} = Access.get_many("missing-lmdb-path", ["valid", 123])
  end
end
