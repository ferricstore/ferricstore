defmodule Ferricstore.Commands.NativeContextAndReadFailureTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Native
  alias Ferricstore.Store.ReadResult

  test "KEY_INFO uses an instance_ctx map without consulting the default instance" do
    keydir = :ets.new(:native_key_info_context, [:set, :public])
    key = "native-context-key"
    value = "custom-instance-value"
    :ets.insert(keydir, {key, value, 0, 0, 0, 0, byte_size(value)})

    ctx = context(keydir)
    store = %{instance_ctx: ctx, exists?: fn ^key -> true end, get: fn ^key -> value end}

    result = Native.handle("KEY_INFO", [key], store)

    assert field(result, "type") == "string"
    assert field(result, "value_size") == Integer.to_string(byte_size(value))
  end

  test "KEY_INFO converts type-registry storage failures to a command error" do
    keydir = :ets.new(:native_key_info_failure, [:set, :public])
    key = "native-failed-key"

    store = %{
      instance_ctx: context(keydir),
      compound_get: fn ^key, _compound_key -> ReadResult.failure(:disk_error) end
    }

    assert {:error, "ERR storage read failed"} == Native.handle("KEY_INFO", [key], store)
  end

  defp context(keydir) do
    %FerricStore.Instance{
      name: :native_context_test,
      shard_count: 1,
      slot_map: Tuple.duplicate(0, 1_024),
      keydir_refs: {keydir}
    }
  end

  defp field(result, name) do
    result
    |> Enum.chunk_every(2)
    |> Map.new(fn [key, value] -> {key, value} end)
    |> Map.fetch!(name)
  end
end
