defmodule Ferricstore.ErrorReasons do
  @moduledoc false

  @write_timeout_unknown {:error, {:timeout, :unknown_outcome}}

  @spec write_timeout_unknown() :: {:error, {:timeout, :unknown_outcome}}
  def write_timeout_unknown, do: @write_timeout_unknown
end
