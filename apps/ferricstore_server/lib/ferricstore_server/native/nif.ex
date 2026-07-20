defmodule FerricstoreServer.Native.NIF do
  @moduledoc """
  Rust NIF bindings for pure native protocol wire-codec operations.

  This module must stay side-effect free. It may parse bytes and build frame
  binaries, but it must not own sockets, ACL state, lane scheduling, storage, or
  command execution.
  """

  version = Mix.Project.config()[:version]

  use RustlerPrecompiled,
    otp_app: :ferricstore_server,
    crate: "native_protocol_nif",
    base_url: "https://github.com/ferricstore/ferricstore/releases/download/v#{version}",
    version: version,
    nif_versions: ["2.16"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      x86_64-unknown-linux-gnu
    )

  @type frame ::
          {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), binary()}

  @spec parse_fql(binary()) ::
          {:ok, :execute | :explain, :point | :collection, [tuple()], [tuple()], pos_integer(),
           nil | tuple()}
          | {:error, atom()}
  def parse_fql(_query), do: :erlang.nif_error(:nif_not_loaded)

  @spec decode_frames(binary(), non_neg_integer()) ::
          {:ok, [frame()], binary(), :more | :done} | {:error, binary()}
  def decode_frames(_buffer, _max_frame_bytes), do: :erlang.nif_error(:nif_not_loaded)

  @spec encode_frame(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          boolean()
        ) :: binary()
  def encode_frame(_opcode, _lane_id, _request_id, _body, _flags, _response?),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec encode_compact_claim_jobs_response_frame(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          term()
        ) :: binary() | nil
  def encode_compact_claim_jobs_response_frame(_opcode, _lane_id, _request_id, _jobs),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec encode_compact_ok_list_response_frame(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          term()
        ) :: binary() | nil
  def encode_compact_ok_list_response_frame(_opcode, _lane_id, _request_id, _values),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec encode_compact_kv_get_response_frame(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          term()
        ) :: binary() | nil
  def encode_compact_kv_get_response_frame(_opcode, _lane_id, _request_id, _value),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec encode_compact_kv_mget_response_frame(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          term()
        ) :: binary() | nil
  def encode_compact_kv_mget_response_frame(_opcode, _lane_id, _request_id, _values),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec encode_compact_kv_mget(term()) :: binary() | nil
  def encode_compact_kv_mget(_values),
    do: :erlang.nif_error(:nif_not_loaded)
end
