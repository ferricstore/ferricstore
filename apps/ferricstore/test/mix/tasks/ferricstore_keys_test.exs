defmodule Mix.Tasks.FerricstoreKeysTest do
  use ExUnit.Case, async: true

  alias Mix.Tasks.Ferricstore.Keys

  test "streams every page without accumulating or sorting the keyspace" do
    parent = self()

    scan_page = fn
      "0", 2, "user:*" -> {:ok, {"next", ["user:z", "user:a"]}}
      "next", 2, "user:*" -> {:ok, {"last", []}}
      "last", 2, "user:*" -> {:ok, {"0", ["user:m"]}}
    end

    emit = fn key -> send(parent, {:emitted, key}) end

    assert Keys.stream_keys(scan_page, emit, "user:*", 2) == 3
    assert_receive {:emitted, "user:z"}
    assert_receive {:emitted, "user:a"}
    assert_receive {:emitted, "user:m"}
  end

  test "raises instead of hiding scan failures" do
    scan_page = fn "0", 100, nil -> {:error, :catalog_unavailable} end

    assert_raise Mix.Error, ~r/failed to scan keys: :catalog_unavailable/, fn ->
      Keys.stream_keys(scan_page, fn _key -> :ok end, nil, 100)
    end
  end

  test "rejects a stalled nonterminal cursor" do
    scan_page = fn
      "0", 100, nil -> {:ok, {"next", []}}
      "next", 100, nil -> {:ok, {"next", []}}
    end

    assert_raise Mix.Error, ~r/scan cursor did not advance/, fn ->
      Keys.stream_keys(scan_page, fn _key -> :ok end, nil, 100)
    end
  end
end
