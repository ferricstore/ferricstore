defmodule Ferricstore.Store.RouterBatchPutStatusTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Router

  test "batch result status returns ok when every item succeeded" do
    assert :ok = Router.__batch_result_status_for_test__([])
    assert :ok = Router.__batch_result_status_for_test__([:ok, {:ok, :ok}, "OK"])
  end

  test "batch result status returns the first item error" do
    assert {:error, :first} =
             Router.__batch_result_status_for_test__([:ok, {:error, :first}, {:error, :second}])
  end

  test "batch result status preserves non-list error results" do
    assert {:error, :timeout} = Router.__batch_result_status_for_test__({:error, :timeout})
  end
end
