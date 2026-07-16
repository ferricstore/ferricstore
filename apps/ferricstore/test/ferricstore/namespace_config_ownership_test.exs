defmodule Ferricstore.NamespaceConfigOwnershipTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.NamespaceConfig

  setup do
    NamespaceConfig.reset_all()
    on_exit(&NamespaceConfig.reset_all/0)
    :ok
  end

  test "only the namespace config owner may mutate the ETS table" do
    result =
      Task.async(fn ->
        try do
          :ets.insert(:ferricstore_ns_config, {"injected", 99, 0, "attacker"})
          :wrote
        rescue
          ArgumentError -> :protected
        end
      end)
      |> Task.await()

    assert result == :protected
    assert NamespaceConfig.window_for("injected") == NamespaceConfig.default_window_ms()
  end
end
