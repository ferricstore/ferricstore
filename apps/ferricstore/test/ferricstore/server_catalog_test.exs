defmodule Ferricstore.ServerCatalogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ServerCatalog

  test "catalog keys keep subjects opaque and within the internal namespace" do
    subject = <<0, 255, ?:, ?/, ?{, ?}>>
    key = ServerCatalog.entry_key("acl", subject)

    assert String.starts_with?(key, "f:{__server__}:catalog:acl:")
    assert ServerCatalog.subject_from_key("acl", key) == {:ok, subject}
  end

  test "empty catalog subjects round-trip through the prefix key" do
    key = ServerCatalog.entry_key("acl", "")

    assert key == ServerCatalog.prefix("acl")
    assert ServerCatalog.subject_from_key("acl", key) == {:ok, ""}
  end

  test "entry envelopes are deterministic, bounded, and safe to decode" do
    value = %{enabled: true, password: "pbkdf2-sha256$hash"}
    encoded = ServerCatalog.encode_entry(42, value)

    assert {:ok, %{version: 42, value: ^value}} = ServerCatalog.decode_entry(encoded)
    assert encoded == ServerCatalog.encode_entry(42, value)
    assert {:error, :invalid_server_catalog_entry} = ServerCatalog.decode_entry(<<131, 100>>)
  end

  test "deletion replies remain versioned projection envelopes" do
    encoded = ServerCatalog.encode_entry(77, :deleted)

    assert {:ok, %{version: 77, value: :deleted}} = ServerCatalog.decode_entry(encoded)
  end

  test "live counts have a dedicated bounded envelope" do
    key = ServerCatalog.live_count_key("acl")
    encoded = ServerCatalog.encode_live_count(42)

    assert String.ends_with?(key, ":live_count")
    assert {:ok, 42} = ServerCatalog.decode_live_count(encoded)

    assert {:error, :invalid_server_catalog_live_count} =
             ServerCatalog.decode_live_count(:erlang.term_to_binary({:unknown, 42}))
  end

  test "catalog decoders reject compressed and trailing external-term envelopes" do
    entry = ServerCatalog.encode_entry(1, String.duplicate("catalog-value", 1_024))
    entry_term = :erlang.binary_to_term(entry, [:safe])
    compressed = :erlang.term_to_binary(entry_term, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed

    assert {:error, :invalid_server_catalog_entry} = ServerCatalog.decode_entry(compressed)
    assert {:error, :invalid_server_catalog_entry} = ServerCatalog.decode_entry(entry <> <<0>>)

    revision = ServerCatalog.encode_revision(1)
    live_count = ServerCatalog.encode_live_count(1)

    assert {:error, :invalid_server_catalog_revision} =
             ServerCatalog.decode_revision(revision <> <<0>>)

    assert {:error, :invalid_server_catalog_live_count} =
             ServerCatalog.decode_live_count(live_count <> <<0>>)
  end
end
