defmodule Ferricstore.Raft.CommandStamp do
  @moduledoc false

  alias Ferricstore.HLC

  @type hlc_ts :: {non_neg_integer(), non_neg_integer()}
  @type stamped_command :: {term(), %{hlc_ts: hlc_ts()}}

  @spec stamp(term()) :: stamped_command()
  def stamp({command, %{hlc_ts: {physical_ms, logical}}} = stamped)
      when is_integer(physical_ms) and is_integer(logical) and physical_ms >= 0 and logical >= 0 and
             is_tuple(command) do
    stamped
  end

  def stamp(command) do
    {command, %{hlc_ts: HLC.now()}}
  end

  @spec to_ttb(term()) :: {:ttb, binary()}
  def to_ttb(command) do
    {:ttb, :erlang.term_to_binary(stamp(command))}
  end
end
