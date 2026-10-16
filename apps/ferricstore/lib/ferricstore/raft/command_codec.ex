defmodule Ferricstore.Raft.CommandCodec do
  @moduledoc false
  # The bytes of a Raft log command. In data format 2 a command is stored in a
  # compact form: atoms and the default apply context found in a frozen
  # dictionary become two-byte references (native `raft_command_codec.rs`).
  # Decoding restores the exact external-term bytes, so everything that reads
  # the log sees the same command. Format 1 keeps the plain external term
  # format that older releases read; every member of a cluster runs one format.

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.DataFormat

  @compact_magic 0xFC
  # Larger commands are mostly value bytes.
  @max_compact_bytes 64 * 1_024

  @spec encode(binary()) :: binary()
  def encode(external_term) when byte_size(external_term) <= @max_compact_bytes do
    if DataFormat.current() == 2 do
      case NIF.raft_command_compact(external_term) do
        {:ok, compact} -> compact
        :error -> external_term
      end
    else
      external_term
    end
  end

  def encode(external_term) when is_binary(external_term), do: external_term

  @spec decode(binary()) :: {:ok, binary()} | :error
  def decode(<<@compact_magic, _rest::binary>> = compact), do: NIF.raft_command_expand(compact)
  def decode(external_term) when is_binary(external_term), do: {:ok, external_term}
end
