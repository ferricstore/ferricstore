defmodule Ferricstore.Flow.RetentionGuard do
  @moduledoc false

  alias Ferricstore.Flow

  def encode(%{version: version} = record) when is_integer(version) do
    :erlang.term_to_binary({version, identity(record)})
  end

  def identity(%{state_enter_seq: sequence}) when is_integer(sequence) and sequence >= 0,
    do: {:state_enter_seq, sequence}

  def identity(record) when is_map(record) do
    digest = record |> Flow.encode_record() |> then(&:crypto.hash(:sha256, &1))
    {:legacy_record, Map.get(record, :created_at_ms), digest}
  end
end
