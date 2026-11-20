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

  describe "type digests" do
    alias Ferricstore.Flow.Digest

    test "keys built from a Flow type hash the type once per process" do
      type = "type-digest-cache-#{System.unique_integer([:positive])}"
      state_key = Keys.state_key("order-1", "tenant-1")
      mfa = {:crypto, :hash, 2}
      :erlang.trace_pattern(mfa, true, [:call_count])
      :erlang.trace(self(), true, [:call])

      try do
        for _ <- 1..10 do
          Keys.type_catalog_descriptor_key(type)
          Keys.policy_migration_job_key(type)
          Keys.policy_migration_marker_key(type)
        end

        # One hash for the type; the state key is hashed by the member key.
        assert {:call_count, 1} = :erlang.trace_info(mfa, :call_count)
        _member = Keys.type_catalog_member_key(type, state_key)
        assert {:call_count, 2} = :erlang.trace_info(mfa, :call_count)
      after
        :erlang.trace(self(), false, [:call])
        :erlang.trace_pattern(mfa, false, [:call_count])
      end
    end

    test "cached type digests equal the digest of the type" do
      type = "invoice.process"
      expected = Digest.encode(type)

      for _ <- 1..2 do
        assert String.ends_with?(Keys.type_catalog_descriptor_key(type), expected)
        assert String.contains?(Keys.policy_migration_job_key(type), expected)
        assert String.contains?(Keys.policy_migration_marker_key(type), expected)
      end
    end

    test "the per-process type digest cache stays bounded" do
      for i <- 1..3_000, do: Keys.type_catalog_descriptor_key("bounded-type-#{i}")

      cached =
        Process.get_keys()
        |> Enum.map(&Process.get/1)
        |> Enum.filter(&is_map/1)
        |> Enum.map(&map_size/1)
        |> Enum.max(fn -> 0 end)

      assert cached <= 1_024
    end
  end
end
