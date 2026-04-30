defmodule Ferricstore.Raft.CommandClock do
  @moduledoc """
  Stamps Raft commands with a leader-side HLC timestamp before they enter the log.

  The state machine uses the stamped physical millisecond for TTL and lock expiry
  decisions, making replay deterministic across replicas.
  """

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

  @spec process_command(term(), term()) :: term()
  def process_command(shard_id, command) do
    :ra.process_command(shard_id, stamp(command))
  end

  @spec process_command(term(), term(), term()) :: term()
  def process_command(shard_id, command, opts) do
    :ra.process_command(shard_id, stamp(command), opts)
  end

  @spec pipeline_command(term(), term(), reference() | integer(), atom()) :: term()
  def pipeline_command(shard_id, command, corr, priority) do
    :ra.pipeline_command(shard_id, stamp(command), corr, priority)
  end
end
