defmodule Ferricstore.Flow.Query.QueryRow do
  @moduledoc false

  alias Ferricstore.Flow.Locator

  @enforce_keys [:state_key, :record, :locator, :expire_at_ms]
  defstruct [:state_key, :record, :locator, :expire_at_ms, scope_prefix: nil]

  @type t :: %__MODULE__{
          state_key: binary(),
          record: map(),
          locator: Locator.t(),
          expire_at_ms: non_neg_integer(),
          scope_prefix: binary() | nil
        }
end
