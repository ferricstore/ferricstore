defmodule Ferricstore.Flow.Query.PreparedResponse do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Response, ResultCodec}

  @contract Response.contract()
  @tag ResultCodec.tag()

  @enforce_keys [:value, :codec, :payload]
  defstruct [:value, :codec, :payload]

  @type t :: %__MODULE__{
          value: map(),
          codec: :flow_query_result_v1,
          payload: binary()
        }

  @doc false
  @spec new(map(), binary()) :: {:ok, t()} | :error
  def new(value, payload) when is_map(value) and is_binary(payload) do
    prepared = %__MODULE__{
      value: value,
      codec: :flow_query_result_v1,
      payload: payload
    }

    if valid?(prepared), do: {:ok, prepared}, else: :error
  end

  def new(_value, _payload), do: :error

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{
        value: %{version: version, usage: %{response_bytes: response_bytes}},
        codec: :flow_query_result_v1,
        payload: payload
      })
      when version == @contract and is_integer(response_bytes) and response_bytes >= 0 and
             is_binary(payload) and byte_size(payload) == response_bytes and
             byte_size(payload) > 0 do
    :binary.at(payload, 0) == @tag
  end

  def valid?(_prepared), do: false
end
