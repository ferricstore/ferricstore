defmodule FerricstoreServer.Native.Connection.PreparedPubSubBatch do
  @moduledoc false

  @enforce_keys [:channel, :messages, :at_ms, :encoded_value]
  defstruct [
    :channel,
    :messages,
    :at_ms,
    :encoded_value,
    :bounded_response_bytes,
    :bounded_encoded_values
  ]

  @type t :: %__MODULE__{
          channel: binary(),
          messages: [binary()],
          at_ms: integer(),
          encoded_value: binary(),
          bounded_response_bytes: pos_integer() | nil,
          bounded_encoded_values: [binary()] | nil
        }
end
