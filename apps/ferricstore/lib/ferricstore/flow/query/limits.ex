defmodule Ferricstore.Flow.Query.Limits do
  @moduledoc false

  alias Ferricstore.Store.Router

  # Partitioned Flow state keys use a fixed SHA-256 routing tag.
  @partitioned_state_key_overhead byte_size("f:{f:" <> String.duplicate("x", 43) <> "}:s:")

  @max_query_bytes 16 * 1024
  @max_query_tokens 256
  @max_predicates 12
  @max_in_values 20
  @max_generated_ranges 32
  @max_parameters 64
  @max_order_fields 2
  @max_return_fields 32
  @max_results 100
  @min_cursor_bytes 16
  @max_cursor_bytes 4_096
  @max_sort_key_bytes 2_512
  @max_projection_page_records 64

  @spec max_query_bytes() :: pos_integer()
  def max_query_bytes, do: @max_query_bytes

  @spec max_query_tokens() :: pos_integer()
  def max_query_tokens, do: @max_query_tokens

  @spec max_predicates() :: pos_integer()
  def max_predicates, do: @max_predicates

  @spec max_in_values() :: pos_integer()
  def max_in_values, do: @max_in_values

  @spec max_generated_ranges() :: pos_integer()
  def max_generated_ranges, do: @max_generated_ranges

  @spec max_order_fields() :: pos_integer()
  def max_order_fields, do: @max_order_fields

  @spec max_return_fields() :: pos_integer()
  def max_return_fields, do: @max_return_fields

  @spec max_parameters() :: pos_integer()
  def max_parameters, do: @max_parameters

  @spec max_results() :: pos_integer()
  def max_results, do: @max_results

  @spec min_cursor_bytes() :: pos_integer()
  def min_cursor_bytes, do: @min_cursor_bytes

  @spec max_cursor_bytes() :: pos_integer()
  def max_cursor_bytes, do: @max_cursor_bytes

  @spec max_sort_key_bytes() :: pos_integer()
  def max_sort_key_bytes, do: @max_sort_key_bytes

  @spec max_projection_page_records() :: pos_integer()
  def max_projection_page_records, do: @max_projection_page_records

  @spec max_partition_key_bytes() :: pos_integer()
  def max_partition_key_bytes, do: Router.max_key_size()

  @spec max_state_key_bytes() :: pos_integer()
  def max_state_key_bytes, do: Router.max_key_size()

  @spec max_run_id_bytes() :: pos_integer()
  def max_run_id_bytes, do: Router.max_key_size() - @partitioned_state_key_overhead

  @spec valid_partition_key?(term()) :: boolean()
  def valid_partition_key?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= max_partition_key_bytes()

  @spec valid_run_id?(term()) :: boolean()
  def valid_run_id?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= max_run_id_bytes()
end
