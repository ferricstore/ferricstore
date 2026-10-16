defmodule Ferricstore.Flow.KeysDigestTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Keys

  @alphabet ~c"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

  # The definition the digest check must keep: unpadded URL-safe base64 that
  # decodes to 32 bytes and re-encodes to the same text.
  defp reference_digest?(digest) when byte_size(digest) == 43 do
    case Base.url_decode64(digest, padding: false) do
      {:ok, decoded} when byte_size(decoded) == 32 ->
        Base.url_encode64(decoded, padding: false) == digest

      _invalid ->
        false
    end
  end

  defp reference_digest?(_digest), do: false

  test "canonical_digest? accepts exactly the canonical 32-byte URL-safe base64 digests" do
    real =
      for i <- 1..2_000, do: :crypto.hash(:sha256, <<i::32>>) |> Base.url_encode64(padding: false)

    body = binary_part(hd(real), 0, 42)
    every_last_char = for c <- 0..255, do: body <> <<c>>

    every_inner_char =
      for c <- 0..255, position <- [0, 20, 41] do
        <<prefix::binary-size(^position), _::binary-size(1), rest::binary>> = hd(real)
        prefix <> <<c>> <> rest
      end

    :rand.seed(:exsss, {7, 11, 13})

    random_alphabet =
      for _ <- 1..5_000, do: for(_ <- 1..43, into: "", do: <<Enum.random(@alphabet)>>)

    lengths =
      for size <- [0, 1, 41, 42, 44, 45, 64], do: binary_part(hd(real) <> hd(real), 0, size)

    for digest <- real ++ every_last_char ++ every_inner_char ++ random_alphabet ++ lengths do
      assert Keys.canonical_digest?(digest) == reference_digest?(digest), inspect(digest)
    end

    assert Enum.all?(real, &Keys.canonical_digest?/1)
    refute Keys.canonical_digest?(nil)
  end

  test "classifying a Flow key does not decode and re-encode its digests" do
    member =
      Keys.type_catalog_member_key("invoice.process", Keys.state_key("order-1", "tenant-1"))

    assert Keys.type_catalog_member_key?(member)

    {:reductions, before} = :erlang.process_info(self(), :reductions)
    for _ <- 1..1_000, do: true = Keys.type_catalog_member_key?(member)
    {:reductions, after_calls} = :erlang.process_info(self(), :reductions)

    # Three digest checks per call; decode + re-encode cost ~375 reductions per call.
    assert (after_calls - before) / 1_000 < 220
  end
end
