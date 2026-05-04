defmodule FerricstoreServer.Resp.ParserNif do
  @moduledoc """
  Rust NIF binding for RESP3 protocol parsing.

  Parses raw RESP3 wire bytes into Elixir terms. This is required by the server;
  there is no Elixir fallback parser.
  """

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :ferricstore_server,
    crate: "resp_parser_nif",
    base_url: "https://github.com/yoavgeva/ferricstore/releases/download/v#{version}",
    version: version,
    nif_versions: ["2.16"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-musl
    ),
    force_build: System.get_env("FERRICSTORE_BUILD") in ["1", "true"]

  @spec parse(binary(), non_neg_integer()) :: {:ok, list(), binary()} | {:error, term()}
  def parse(_data, _max_value_size), do: :erlang.nif_error(:nif_not_loaded)

  @spec parse_commands(binary(), non_neg_integer()) :: {:ok, list(), binary()} | {:error, term()}
  def parse_commands(_data, _max_value_size), do: :erlang.nif_error(:nif_not_loaded)
end
