defmodule Ferricstore.Config.LocalOwnershipTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Config.Local

  setup do
    original_level = Logger.level() |> Atom.to_string()
    Local.reset_all()

    on_exit(fn ->
      Local.set("log_level", original_level)
      Local.reset_all()
    end)

    :ok
  end

  test "settings outlive the connection process that writes them" do
    if :ets.info(:ferricstore_config_local, :owner) == self() do
      :ets.delete(:ferricstore_config_local)
    end

    Task.async(fn -> Local.set("log_level", "debug") end)
    |> Task.await()

    assert Local.get_all() == %{"log_level" => "debug"}
  end

  test "only the local config owner may mutate the ETS table" do
    result =
      Task.async(fn ->
        try do
          :ets.insert(:ferricstore_config_local, {"log_level", "debug"})
          :wrote
        rescue
          ArgumentError -> :protected
        end
      end)
      |> Task.await()

    assert result == :protected
  end
end
