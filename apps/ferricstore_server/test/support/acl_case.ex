defmodule Ferricstore.Test.AclCase do
  @moduledoc """
  Shared setup for tests that mutate server ACL state.
  """

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: false

      @moduletag :acl

      import Ferricstore.Test.Eventually

      setup do
        FerricstoreServer.Acl.reset!()

        on_exit(fn ->
          FerricstoreServer.Acl.reset!()
          Application.delete_env(:ferricstore, :protected_mode)
          Application.delete_env(:ferricstore, :max_acl_users)
        end)

        :ok
      end
    end
  end
end
