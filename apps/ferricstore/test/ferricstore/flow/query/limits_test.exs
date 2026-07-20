defmodule Ferricstore.Flow.Query.LimitsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Limits
  alias Ferricstore.Store.Router

  test "derives point-value limits from the generated storage key" do
    overhead = byte_size("f:{f:" <> String.duplicate("x", 43) <> "}:s:")

    assert Limits.max_partition_key_bytes() == Router.max_key_size()
    assert Limits.max_run_id_bytes() + overhead == Router.max_key_size()

    assert Limits.valid_partition_key?(String.duplicate("p", Limits.max_partition_key_bytes()))

    refute Limits.valid_partition_key?(
             String.duplicate("p", Limits.max_partition_key_bytes() + 1)
           )

    assert Limits.valid_run_id?(String.duplicate("r", Limits.max_run_id_bytes()))
    refute Limits.valid_run_id?(String.duplicate("r", Limits.max_run_id_bytes() + 1))
  end

  test "publishes the bounded query contract from one module" do
    assert Limits.max_query_bytes() == 16 * 1024
    assert Limits.max_query_tokens() == 256
    assert Limits.max_predicates() == 12
    assert Limits.max_in_values() == 20
    assert Limits.max_generated_ranges() == 32
    assert Limits.max_parameters() == 64
    assert Limits.max_order_fields() == 2
    assert Limits.max_results() == 100
  end
end
