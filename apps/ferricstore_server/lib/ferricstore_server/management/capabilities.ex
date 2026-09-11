defmodule FerricstoreServer.Management.Capabilities do
  @moduledoc false
  @behaviour FerricStore.ManagementCapabilities

  @impl true
  def capabilities(_opts \\ []) do
    FerricStore.ManagementCapabilities.default()
    |> Map.put(
      :acl_management,
      FerricStore.Management.ACL.implementation() == FerricstoreServer.Management.ACL
    )
  end
end
