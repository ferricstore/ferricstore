defmodule FerricstoreServer.Resp.ParserNif do
  @moduledoc """
  Rust NIF binding for RESP3 protocol parsing.

  Parses raw RESP3 wire bytes into Elixir terms. This is required by the server;
  there is no Elixir fallback parser.
  """
  use Rustler,
    otp_app: :ferricstore_server,
    crate: "resp_parser_nif",
    skip_compilation?: true,
    load_from: {:ferricstore_server, "priv/native/resp_parser_nif"}

  @spec parse(binary(), non_neg_integer()) :: {:ok, list(), binary()} | {:error, term()}
  def parse(_data, _max_value_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec parse_commands(binary(), non_neg_integer()) :: {:ok, list(), binary()} | {:error, term()}
  def parse_commands(_data, _max_value_size), do: :erlang.nif_error(:nif_not_loaded)
end
