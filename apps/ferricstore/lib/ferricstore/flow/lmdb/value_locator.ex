defmodule Ferricstore.Flow.LMDB.ValueLocator do
  @moduledoc false

  def encode(expire_at_ms, file_id, offset, value_size) do
    :erlang.term_to_binary({:flow_value_locator, 1, expire_at_ms, file_id, offset, value_size})
  end
end
