defmodule Ferricstore.Flow.HistoryProjector.KeyCodecTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.KeyCodec
  alias Ferricstore.Flow.Keys

  test "history_entry_key builds parseable history log keys" do
    history_key = Keys.history_key("flow-history:1")
    key = KeyCodec.history_entry_key(history_key, "12345-7")

    assert key == "X:" <> history_key <> <<0>> <> "12345-7"
    assert KeyCodec.parse_history_entry_key(key) == {:ok, history_key, "12345-7", 12_345}
  end

  test "parse_history_entry_key rejects malformed keys" do
    assert KeyCodec.parse_history_entry_key("flow-history:1" <> <<0>> <> "12345-7") == :error
    assert KeyCodec.parse_history_entry_key("X:flow-history:1") == :error
    assert KeyCodec.parse_history_entry_key("X:flow-history:1" <> <<0>> <> "bad-7") == :error
    assert KeyCodec.parse_history_entry_key("X:flow-history:1" <> <<0>> <> "12345") == :error

    history_key = Keys.history_key("flow-history:1")

    assert KeyCodec.parse_history_entry_key("X:" <> history_key <> <<0>> <> "12345-bad") ==
             :error

    assert KeyCodec.parse_history_entry_key("X:" <> history_key <> <<0>> <> "12345-7-extra") ==
             :error
  end

  test "parse_event_ms requires a canonical current-schema event id" do
    assert KeyCodec.parse_event_ms("123-1") == {:ok, 123}
    assert KeyCodec.parse_event_ms("123x-1") == :error
    assert KeyCodec.parse_event_ms("123") == :error
    assert KeyCodec.parse_event_ms("123-1-extra") == :error
    assert KeyCodec.parse_event_ms("0123-1") == :error
    assert KeyCodec.parse_event_ms("123-01") == :error
    assert KeyCodec.parse_event_ms("9007199254740992-1") == :error
  end

  test "parse_event_version requires a canonical current-schema event id" do
    assert KeyCodec.parse_event_version("123-4") == {:ok, 4}
    assert KeyCodec.parse_event_version("123-v4") == :error
    assert KeyCodec.parse_event_version("123") == :error
    assert KeyCodec.parse_event_version("123-4-extra") == :error
    assert KeyCodec.parse_event_version("0123-4") == :error
    assert KeyCodec.parse_event_version("123-04") == :error
    assert KeyCodec.parse_event_version("123-9007199254740992") == :error
  end
end
