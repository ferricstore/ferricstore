defmodule Ferricstore.Commands.ListTest.Sections.Part01 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Commands.{Dispatcher, Hash, List, Strings}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Test.MockStore

  describe "storage path guards" do
    test "list operations expose only the compound-key store API" do
      source = File.read!(app_path("lib/ferricstore/store/list_ops.ex"))

      # The closure-based API wrote whole lists as serialized plain values.
      # New list code must stay on the compound-key path so async/read flows
      # see one canonical representation.
      refute source =~ "def execute(get_fn"
      refute source =~ "legacy_execute_blob"
      refute source =~ "legacy_execute_lmove"
      refute source =~ "decode_stored"
      refute source =~ "encode_list"
    end
  end
    end
  end
end
