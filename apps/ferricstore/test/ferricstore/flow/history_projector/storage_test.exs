defmodule Ferricstore.Flow.HistoryProjector.StorageTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.HistoryProjector.Storage

  test "history append location validation rejects malformed batch replies without raising" do
    assert Storage.validate_locations([%{key: "history"}], :invalid) ==
             {:error, {:invalid_history_locations, :invalid}}

    assert Storage.validate_locations([%{key: "history"}], [:invalid]) ==
             {:error, {:invalid_history_locations, [:invalid]}}
  end

  test "encoded history entries must match their physical key and event identity" do
    history_key = Ferricstore.Flow.Keys.history_key("validated-entry")
    event_id = "3000-2"

    entry = %{
      key: Ferricstore.Flow.Keys.stream_entry_key_from_history_key(history_key, event_id),
      history_key: history_key,
      event_id: event_id,
      event_ms: 3_000,
      version: 2,
      expire_at_ms: 0,
      value: "encoded-history"
    }

    assert Storage.validate_entries([entry]) == :ok

    assert Storage.validate_entries([%{entry | event_ms: 3_001}]) ==
             {:error, {:invalid_history_entry, 0, :event_identity_mismatch}}

    assert Storage.validate_entries([%{entry | key: entry.key <> "-forged"}]) ==
             {:error, {:invalid_history_entry, 0, :physical_key_mismatch}}
  end

  test "malformed entries return a controlled projection error instead of raising" do
    unique = System.unique_integer([:positive])
    dir = Path.join(System.tmp_dir!(), "history_projector_invalid_entry_#{unique}")
    keydir = :ets.new(:"history_projector_invalid_entry_#{unique}", [:set, :public])
    on_exit(fn -> File.rm_rf!(dir) end)

    ctx = %{
      name: :"history_projector_invalid_entry_#{unique}",
      keydir_refs: {keydir},
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false)
    }

    assert {:error, {:history_projection_exception, %FunctionClauseError{}}} =
             HistoryProjector.write_entries_sync(ctx, 0, dir, [%{invalid: true}], 1)
  end
end
