defmodule Ferricstore.Raft.CommandClock do
  @moduledoc """
  Stamps Raft commands with a leader-side HLC timestamp before they enter the log.

  The state machine uses the stamped physical millisecond for TTL and lock expiry
  decisions, making replay deterministic across replicas.
  """

  alias Ferricstore.HLC
  alias Ferricstore.Raft.Backend
  alias Ferricstore.Raft.WARaftBackend

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
    if Backend.waraft?() do
      process_waraft_command(shard_id, command)
    else
      :ra.process_command(shard_id, stamp(command))
    end
  end

  @spec process_command(term(), term(), term()) :: term()
  def process_command(shard_id, command, opts) do
    if Backend.waraft?() do
      process_waraft_command(shard_id, command)
    else
      :ra.process_command(shard_id, stamp(command), opts)
    end
  end

  @spec pipeline_command(term(), term(), reference() | integer(), atom()) :: term()
  def pipeline_command(shard_id, command, corr, priority) do
    if Backend.waraft?() do
      pipeline_waraft_command(shard_id, command, corr)
    else
      :ra.pipeline_command(shard_id, stamp(command), corr, priority)
    end
  end

  defp process_waraft_command(shard_id, command) do
    with {:ok, shard_index} <- shard_index_from_id(shard_id) do
      shard_index
      |> Backend.write(command)
      |> wrap_waraft_process_result(shard_index)
    end
  end

  defp pipeline_waraft_command(shard_id, command, corr) do
    with {:ok, shard_index} <- shard_index_from_id(shard_id) do
      case Backend.write(shard_index, command) do
        {:error, _reason} = error ->
          error

        result ->
          send(
            self(),
            {:ra_event, nil, {:applied, [{corr, waraft_applied(result, shard_index)}]}}
          )

          :ok
      end
    end
  end

  defp wrap_waraft_process_result({:error, _reason} = error, _shard_index), do: error

  defp wrap_waraft_process_result(result, shard_index) do
    {:ok, waraft_applied(result, shard_index), nil}
  end

  defp waraft_applied(result, shard_index) do
    {:applied_at, waraft_applied_index(shard_index), result}
  end

  defp waraft_applied_index(shard_index) do
    case WARaftBackend.storage_position(shard_index) do
      {:ok, {:raft_log_pos, index, _term}} when is_integer(index) -> index
      _other -> 0
    end
  end

  defp shard_index_from_id({name, _node}) when is_atom(name) do
    name
    |> Atom.to_string()
    |> parse_shard_index_name()
  end

  defp shard_index_from_id(other), do: {:error, {:unsupported_shard_id, other}}

  defp parse_shard_index_name("ferricstore_shard_" <> suffix), do: parse_zero_based_index(suffix)

  defp parse_shard_index_name("raft_server_ferricstore_waraft_backend_" <> suffix) do
    with {:ok, partition} <- parse_zero_based_index(suffix) do
      if partition > 0 do
        {:ok, partition - 1}
      else
        {:error, {:bad_waraft_partition, suffix}}
      end
    end
  end

  defp parse_shard_index_name(other), do: {:error, {:unsupported_shard_name, other}}

  defp parse_zero_based_index(suffix) do
    case Integer.parse(suffix) do
      {index, ""} when index >= 0 -> {:ok, index}
      _other -> {:error, {:bad_shard_index, suffix}}
    end
  end
end
