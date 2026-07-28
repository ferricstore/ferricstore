defmodule Ferricstore.Flow.InternalLimits do
  @moduledoc false

  @payload_return_max_bytes 64 * 1_024

  @spec payload_return_max_bytes() :: pos_integer()
  def payload_return_max_bytes, do: @payload_return_max_bytes
end
