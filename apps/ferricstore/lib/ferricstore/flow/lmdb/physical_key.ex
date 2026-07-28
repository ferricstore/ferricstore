defmodule Ferricstore.Flow.LMDB.PhysicalKey do
  @moduledoc false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @max_direct_key_bytes 511
  @physical_state_key_prefix <<0, "flsk:1:">>

  @spec derive(binary()) :: binary()
  def derive(logical_key) when is_binary(logical_key) do
    if compact?(logical_key),
      do: @physical_state_key_prefix <> :crypto.hash(:sha256, logical_key),
      else: logical_key
  end

  @spec compact?(term()) :: boolean()
  def compact?(logical_key) when is_binary(logical_key) do
    byte_size(logical_key) > @max_direct_key_bytes and
      byte_size(logical_key) <= Router.max_key_size() and
      (Keys.state_key?(logical_key) or Keys.value_key?(logical_key))
  end

  def compact?(_logical_key), do: false
end
