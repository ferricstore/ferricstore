defmodule Ferricstore.Store.BlobValueTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.{BlobRef, BlobValue}

  test "maybe_materialize_many loads duplicate encoded refs once" do
    parent = self()
    payload = :binary.copy("shared", 32)
    ref = BlobRef.from_segment(payload, 0, 48)
    encoded_ref = BlobRef.encode!(ref)

    loader = fn "root", 0, ^ref ->
      send(parent, :blob_loaded)
      {:ok, payload}
    end

    assert [
             {:ok, ^payload},
             {:ok, "inline"},
             {:ok, ^payload}
           ] =
             BlobValue.maybe_materialize_many(
               "root",
               0,
               1,
               [encoded_ref, "inline", encoded_ref],
               loader
             )

    assert_received :blob_loaded
    refute_received :blob_loaded
  end

  test "maybe_materialize_many fans duplicate load errors out consistently" do
    parent = self()
    payload = "missing"
    ref = BlobRef.from_segment(payload, 1, 48)
    encoded_ref = BlobRef.encode!(ref)

    loader = fn "root", 0, ^ref ->
      send(parent, :blob_loaded)
      {:error, :enoent}
    end

    assert [
             {:error, :enoent},
             {:error, :enoent}
           ] =
             BlobValue.maybe_materialize_many("root", 0, 1, [encoded_ref, encoded_ref], loader)

    assert_received :blob_loaded
    refute_received :blob_loaded
  end
end
