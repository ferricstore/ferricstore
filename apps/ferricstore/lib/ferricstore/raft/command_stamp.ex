defmodule Ferricstore.Raft.CommandStamp do
  @moduledoc false

  alias Ferricstore.HLC
  alias Ferricstore.TermCodec

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
    {:ttb, TermCodec.encode(stamp(command))}
  end

  @spec decode_ttb(term()) :: {:ok, stamped_command()} | {:error, :invalid_preencoded_command}
  def decode_ttb(binary) when is_binary(binary) do
    with {:ok, term} <- TermCodec.decode(binary) do
      case term do
        {_command, %{hlc_ts: {physical_ms, logical}}} = stamped
        when is_integer(physical_ms) and physical_ms >= 0 and is_integer(logical) and
               logical >= 0 ->
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
