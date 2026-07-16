defmodule Ferricstore.Store.RouterScanCursorTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router

  setup do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)
    %{ctx: ctx}
  end

  test "logical SCAN rejects compressed and trailing external-term cursors", %{ctx: ctx} do
    term = {:ferricstore_scan_cursor, 1, 0, {:after, String.duplicate("cursor", 4_096)}}
    compressed = :erlang.term_to_binary(term, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed

    trailing =
      :erlang.term_to_binary({:ferricstore_scan_cursor, 1, 0, {:after, "cursor"}}, [
        :deterministic
      ]) <> <<0>>

    for payload <- [compressed, trailing] do
      cursor = Base.url_encode64(payload, padding: false)
      assert byte_size(cursor) <= 8_192

      assert {:error, "ERR invalid cursor"} =
               Router.scan_keys_page(ctx, cursor, 1, nil, nil)
    end
  end

  test "logical SCAN can consume cursors emitted for keys larger than eight KiB", %{ctx: ctx} do
    long_key = String.duplicate("a", 10_000)
    assert :ok = Router.put(ctx, long_key, "value", 0)
    assert :ok = Router.put(ctx, "z", "value", 0)

    assert {:ok, {cursor, [^long_key]}} = Router.scan_keys_page(ctx, "0", 1, nil, nil)
    assert byte_size(cursor) > 8_192

    assert {:ok, {"0", ["z"]}} = Router.scan_keys_page(ctx, cursor, 1, nil, nil)
  end
end
