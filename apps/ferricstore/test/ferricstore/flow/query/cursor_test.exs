defmodule Ferricstore.Flow.Query.CursorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Limits
  alias Ferricstore.Flow.Query.Cursor

  @key :binary.copy(<<0xA5>>, 32)

  test "issues an opaque authenticated cursor and verifies its exact binding" do
    binding = cursor_binding()
    continuation = <<1, 2, 3, 4, 0, 255>>

    assert {:ok, token} =
             Cursor.issue(binding, continuation,
               key: @key,
               now_ms: 1_000,
               ttl_ms: 5_000,
               nonce_fun: fn -> :binary.copy(<<7>>, 12) end
             )

    assert byte_size(token) <= 4_096
    refute token =~ binding.scope
    refute token =~ binding.index_id
    refute token =~ binding.index_build_id
    refute token =~ continuation

    assert {:ok, ^continuation} = Cursor.verify(binding, token, key: @key, now_ms: 5_999)
  end

  test "opens an authenticated plan claim and binds it to the original token" do
    binding = cursor_binding()

    assert {:ok, token} = Cursor.issue(binding, "seek-a", key: @key, now_ms: 1_000)
    assert {:ok, other_token} = Cursor.issue(binding, "seek-b", key: @key, now_ms: 1_000)

    request_binding = Map.drop(binding, [:index_id, :index_version, :index_build_id])

    assert {:ok, claim} = Cursor.open(request_binding, token, key: @key, now_ms: 1_001)
    assert claim.index_id == binding.index_id
    assert claim.index_version == binding.index_version
    assert claim.index_build_id == binding.index_build_id
    assert {:ok, "seek-a"} = Cursor.verify_claim(binding, token, claim)

    assert {:error, :query_cursor_invalid} = Cursor.verify_claim(binding, other_token, claim)

    assert {:error, :query_cursor_invalid} =
             Cursor.verify_claim(
               %{binding | index_version: binding.index_version + 1},
               token,
               claim
             )
  end

  test "rejects tampering and every security-sensitive binding mismatch" do
    binding = cursor_binding()
    assert {:ok, token} = Cursor.issue(binding, "seek", key: @key, now_ms: 100)

    <<prefix::binary-size(byte_size(token) - 1), last>> = token
    tampered = <<prefix::binary, Bitwise.bxor(last, 1)>>

    assert {:error, :query_cursor_invalid} =
             Cursor.verify(binding, tampered, key: @key, now_ms: 101)

    for changed <- [
          %{binding | instance: :other},
          %{binding | scope: "tenant-b"},
          %{binding | query_fingerprint: String.duplicate("b", 64)},
          %{binding | query_digest: :binary.copy(<<2>>, 32)},
          %{binding | index_id: "other-index"},
          %{binding | index_version: 8},
          %{binding | index_build_id: "replacement-build"},
          %{binding | order_by: [{:run_id, :desc}]}
        ] do
      assert {:error, :query_cursor_invalid} =
               Cursor.verify(changed, token, key: @key, now_ms: 101)
    end

    assert {:error, :query_cursor_invalid} =
             Cursor.verify(binding, token, key: :binary.copy(<<1>>, 32), now_ms: 101)
  end

  test "expires at the exact deadline and never trusts caller-controlled expiry" do
    binding = cursor_binding()

    assert {:ok, token} =
             Cursor.issue(binding, "seek", key: @key, now_ms: 10_000, ttl_ms: 250)

    assert {:ok, "seek"} = Cursor.verify(binding, token, key: @key, now_ms: 10_249)

    assert {:error, :query_cursor_expired} =
             Cursor.verify(binding, token, key: @key, now_ms: 10_250)
  end

  test "fails closed for malformed and oversized inputs without raising" do
    binding = cursor_binding()

    malformed = [nil, 1, "", "fqc1_", "fqc2_abcd", <<0, 255>>, String.duplicate("x", 4_097)]

    for token <- malformed do
      assert {:error, :query_cursor_invalid} =
               Cursor.verify(binding, token, key: @key, now_ms: 0)
    end

    assert {:error, :query_cursor_invalid} =
             Cursor.issue(binding, String.duplicate("x", 4_096), key: @key, now_ms: 0)

    assert {:error, :query_cursor_invalid} =
             Cursor.issue(binding, "seek", key: <<1, 2, 3>>, now_ms: 0)
  end

  test "rejects an oversized binding scope before encoding or hashing" do
    binding = %{
      cursor_binding()
      | scope: :binary.copy("x", Limits.max_partition_key_bytes() + 1)
    }

    assert {:error, :query_cursor_invalid} =
             Cursor.issue(binding, "seek", key: @key, now_ms: 0)
  end

  test "rejects order bindings outside the canonical query contract" do
    for order_by <- [
          [],
          [{:unknown, :asc}],
          [{{:attribute, "rank"}, :asc}],
          [{:updated_at_ms, :asc}, {:updated_at_ms, :desc}]
        ] do
      binding = %{cursor_binding() | order_by: order_by}

      assert {:error, :query_cursor_invalid} =
               Cursor.issue(binding, "seek", key: @key, now_ms: 0)
    end
  end

  test "uses a fresh nonce for every cursor by default" do
    binding = cursor_binding()

    assert {:ok, first} = Cursor.issue(binding, "same", key: @key, now_ms: 0)
    assert {:ok, second} = Cursor.issue(binding, "same", key: @key, now_ms: 0)
    refute first == second
  end

  test "accepts the canonical event history order" do
    binding = %{cursor_binding() | order_by: [{:event_id, :asc}]}

    assert {:ok, token} = Cursor.issue(binding, "seek", key: @key, now_ms: 0)
    assert {:ok, "seek"} = Cursor.verify(binding, token, key: @key, now_ms: 1)
  end

  defp cursor_binding do
    %{
      instance: :ferricstore,
      scope: "tenant-secret-a",
      query_fingerprint: String.duplicate("a", 64),
      query_digest: :binary.copy(<<1>>, 32),
      index_id: "runs-by-state-and-time",
      index_version: 7,
      index_build_id: "build-7-a",
      order_by: [{:updated_at_ms, :desc}, {:version, :desc}]
    }
  end
end
