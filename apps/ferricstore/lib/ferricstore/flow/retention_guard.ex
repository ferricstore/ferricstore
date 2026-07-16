defmodule Ferricstore.Flow.RetentionGuard do
  @moduledoc false

  alias Ferricstore.TermCodec

  def encode(%{version: version} = record) when is_integer(version) and version >= 0,
    do: TermCodec.encode({version, identity(record)})

  def encode(%{version: _version}),
    do: raise(ArgumentError, "flow record requires a non-negative version")

  def identity(%{state_enter_seq: sequence}) when is_integer(sequence) and sequence >= 0,
    do: {:state_enter_seq, sequence}

  def identity(_record),
    do: raise(ArgumentError, "flow record requires a non-negative state_enter_seq")
end
