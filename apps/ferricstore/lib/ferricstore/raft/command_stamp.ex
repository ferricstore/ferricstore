defmodule Ferricstore.Raft.CommandStamp do
  @moduledoc false

  alias Ferricstore.HLC
  alias Ferricstore.TermCodec

  @max_physical_ms Bitwise.bsl(1, 48) - 1
  @max_logical Bitwise.bsl(1, 16) - 1

  defguardp valid_metadata(physical_ms, logical, wall_time_ms)
            when is_integer(physical_ms) and physical_ms >= 0 and
                   physical_ms <= @max_physical_ms and is_integer(logical) and logical >= 0 and
                   logical <= @max_logical and is_integer(wall_time_ms) and wall_time_ms >= 0 and
                   wall_time_ms <= physical_ms

  @type hlc_ts :: {non_neg_integer(), non_neg_integer()}
  @type stamped_command ::
          {term(), %{hlc_ts: hlc_ts(), wall_time_ms: non_neg_integer()}}

  @spec stamp(term()) :: stamped_command()
  def stamp({command, %{hlc_ts: {physical_ms, logical}, wall_time_ms: wall_time_ms}} = stamped)
      when valid_metadata(physical_ms, logical, wall_time_ms) and is_tuple(command) do
    stamped
  end

  def stamp({command, %{hlc_ts: _invalid_or_removed_stamp}}) when is_tuple(command) do
    raise ArgumentError, "invalid stamped command metadata"
  end

  def stamp(command) do
    {hlc_ts, wall_time_ms} = HLC.now_with_wall()
    {command, %{hlc_ts: hlc_ts, wall_time_ms: wall_time_ms}}
  end

  @spec to_ttb(term()) :: {:ttb, binary()}
  def to_ttb(command) do
    {:ttb, TermCodec.encode(stamp(command))}
  end

  @spec decode_ttb(term()) :: {:ok, stamped_command()} | {:error, :invalid_preencoded_command}
  def decode_ttb(binary) when is_binary(binary) do
    with {:ok, term} <- TermCodec.decode(binary) do
      case term do
        {command,
         %{
           hlc_ts: {physical_ms, logical},
           wall_time_ms: wall_time_ms
         }} = stamped
        when is_tuple(command) and valid_metadata(physical_ms, logical, wall_time_ms) ->
          {:ok, stamped}

        _invalid ->
          {:error, :invalid_preencoded_command}
      end
    else
      {:error, :invalid_external_term} -> {:error, :invalid_preencoded_command}
    end
  end

  def decode_ttb(_binary), do: {:error, :invalid_preencoded_command}
end
