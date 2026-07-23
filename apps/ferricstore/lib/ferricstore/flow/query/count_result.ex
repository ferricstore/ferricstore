defmodule Ferricstore.Flow.Query.CountResult do
  @moduledoc false

  @derive {Inspect, only: [:count, :usage, :quality]}
  @enforce_keys [:count, :usage, :quality]
  defstruct [:count, :usage, :quality]

  @type t :: %__MODULE__{
          count: non_neg_integer(),
          usage: map(),
          quality: map()
        }
end
