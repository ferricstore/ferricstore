defmodule Ferricstore.Flow.Query.QueryRowReference do
  @moduledoc false

  alias Ferricstore.Flow.Locator

  @enforce_keys [:state_key, :flow_id, :version, :locator, :expire_at_ms]
  defstruct [:state_key, :flow_id, :version, :locator, :expire_at_ms]

  @type t :: %__MODULE__{
          state_key: binary(),
          flow_id: binary(),
          version: non_neg_integer(),
          locator: Locator.t(),
          expire_at_ms: non_neg_integer()
        }
end
