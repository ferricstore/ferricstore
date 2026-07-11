defmodule Ferricstore.Flow.RetentionGuard do
  @moduledoc false

  def encode(%{version: version} = record) when is_integer(version) do
    :erlang.term_to_binary({version, identity(record)})
  end

  def identity(%{state_enter_seq: sequence}) when is_integer(sequence) and sequence >= 0,
    do: {:state_enter_seq, sequence}

  def identity(_record),
    do: raise(ArgumentError, "flow record requires a non-negative state_enter_seq")
end
