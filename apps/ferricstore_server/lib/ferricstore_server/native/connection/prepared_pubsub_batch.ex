defmodule FerricstoreServer.Native.Connection.PreparedPubSubBatch do
  @moduledoc false

  @enforce_keys [:encoded_value]
  defstruct [:encoded_value]

  @type t :: %__MODULE__{encoded_value: binary()}
end
