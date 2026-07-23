defmodule Ferricstore.Flow.Query.ExecutionResult do
  @moduledoc false

  @derive {Inspect, only: [:records, :has_more, :usage, :quality]}
  @enforce_keys [:records, :has_more, :usage, :quality]
  defstruct [:records, :has_more, :usage, :quality, :continuation]

  @type t :: %__MODULE__{
          records: [map()],
          has_more: boolean(),
          usage: map(),
          quality: map(),
          continuation: binary() | nil
        }
end
