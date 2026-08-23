defmodule FerricstoreHttp.Deadline do
  @moduledoc """
  One request deadline represented in monotonic and system time.

  Monotonic time safely bounds local work. The corresponding absolute system
  timestamp is forwarded to FerricStore so downstream execution cannot reset
  the caller's timeout budget.
  """

  @enforce_keys [:monotonic_ms, :system_ms]
  defstruct @enforce_keys

  @opaque t :: %__MODULE__{monotonic_ms: integer(), system_ms: integer()}

  @spec new(pos_integer()) :: t()
  def new(timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    %__MODULE__{
      monotonic_ms: System.monotonic_time(:millisecond) + timeout_ms,
      system_ms: System.system_time(:millisecond) + timeout_ms
    }
  end

  @spec remaining_ms(t()) :: {:ok, pos_integer()} | {:error, :deadline_exceeded}
  def remaining_ms(%__MODULE__{monotonic_ms: expires_at}) do
    case expires_at - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 -> {:ok, remaining}
      _expired -> {:error, :deadline_exceeded}
    end
  end

  @spec system_ms(t()) :: integer()
  def system_ms(%__MODULE__{system_ms: system_ms}), do: system_ms
end
