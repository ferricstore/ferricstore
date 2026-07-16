defmodule Ferricstore.Store.BitcaskWriterBatchResultTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.BitcaskWriter

  test "write location batches must be exact and well shaped" do
    assert BitcaskWriter.__valid_write_locations_for_test__([{0, 12}, {12, 14}], 2)

    refute BitcaskWriter.__valid_write_locations_for_test__([{0, 12}], 2)
    refute BitcaskWriter.__valid_write_locations_for_test__([{0, 12}, {12, 14}, {26, 9}], 2)
    refute BitcaskWriter.__valid_write_locations_for_test__([{0, 12}, {:put, 12, 14}], 2)
    refute BitcaskWriter.__valid_write_locations_for_test__([{-1, 12}, {12, 14}], 2)
  end
end
