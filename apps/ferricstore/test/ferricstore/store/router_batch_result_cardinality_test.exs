defmodule Ferricstore.Store.RouterBatchResultCardinalityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Store.Router

  test "exact batch results preserve per-command outcomes and count possible writes" do
    assert {[:ok, {:error, :rejected}, {:ok, 2}], 2} =
             Router.__normalize_batch_write_result_for_test__(
               {:ok, [:ok, {:error, :rejected}, {:ok, 2}]},
               3
             )
  end

  test "short and long batch results fail every slot closed as an unknown outcome" do
    unknown = ErrorReasons.write_timeout_unknown()

    assert {[^unknown, ^unknown], 2} =
             Router.__normalize_batch_write_result_for_test__({:ok, [:ok]}, 2)

    assert {[^unknown, ^unknown], 2} =
             Router.__normalize_batch_write_result_for_test__({:ok, [:ok, :ok, :ok]}, 2)
  end

  test "an explicit unknown outcome advances the possible-write count" do
    unknown = ErrorReasons.write_timeout_unknown()

    assert {[^unknown, ^unknown, ^unknown], 3} =
             Router.__normalize_batch_write_result_for_test__(unknown, 3)
  end

  test "compound batch write replies require exact cardinality" do
    unknown = ErrorReasons.write_timeout_unknown()

    assert :ok = Router.__normalize_compound_batch_write_result_for_test__([:ok, :ok], 2)

    assert {:error, :rejected} =
             Router.__normalize_compound_batch_write_result_for_test__(
               {:ok, [:ok, {:error, :rejected}]},
               2
             )

    assert ^unknown =
             Router.__normalize_compound_batch_write_result_for_test__({:ok, [:ok]}, 2)

    assert ^unknown =
             Router.__normalize_compound_batch_write_result_for_test__([:ok, :ok, :ok], 2)
  end
end
