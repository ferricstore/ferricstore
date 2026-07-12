defmodule Ferricstore.Flow.HistoryProjector.KeyCodecTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.KeyCodec

  test "history_entry_key builds parseable history log keys" do
    key = KeyCodec.history_entry_key("flow-history:1", "12345-7")

    assert key == "X:flow-history:1" <> <<0>> <> "12345-7"
    assert KeyCodec.parse_history_entry_key(key) == {:ok, "flow-history:1", "12345-7", 12_345}
  end

  test "parse_history_entry_key rejects malformed keys" do
    assert KeyCodec.parse_history_entry_key("flow-history:1" <> <<0>> <> "12345-7") == :error
    assert KeyCodec.parse_history_entry_key("X:flow-history:1") == :error
    assert KeyCodec.parse_history_entry_key("X:flow-history:1" <> <<0>> <> "bad-7") == :error
    assert KeyCodec.parse_history_entry_key("X:flow-history:1" <> <<0>> <> "12345") == :error
  end

  test "parse_event_ms parses only exact millisecond prefix" do
    assert KeyCodec.parse_event_ms("123-1") == {:ok, 123}
    assert KeyCodec.parse_event_ms("123x-1") == :error
    assert KeyCodec.parse_event_ms("123") == :error
    assert KeyCodec.parse_event_ms("123-1-extra") == {:ok, 123}
  end

  test "parse_event_version parses only exact version suffix" do
    assert KeyCodec.parse_event_version("123-4") == {:ok, 4}
    assert KeyCodec.parse_event_version("123-v4") == :error
    assert KeyCodec.parse_event_version("123") == :error
    assert KeyCodec.parse_event_version("123-4-extra") == :error
  end
end
