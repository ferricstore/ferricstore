defmodule FerricstoreServer.Management.ACL do
  @moduledoc """
  Server-backed implementation of the core ACL management contract.

  The core `:ferricstore` application keeps ACL management pluggable so
  embedded deployments can opt in explicitly. The standalone server owns the
  network ACL table, so it wires this adapter during application startup.
  """

  @behaviour FerricStore.Management.ACL

  alias FerricstoreServer.Acl

  @impl true
  def set_user(username, rules, _opts) do
    Acl.set_user(username, rules)
  end

  @impl true
  def del_user(username, _opts) do
    case Acl.del_user(username) do
      :ok -> {:ok, 1}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get_user(username, _opts) do
    {:ok, Acl.get_user_info(username) || []}
  end

  @impl true
  def list_users(_opts) do
    {:ok, Acl.list_users()}
  end

  @impl true
  def save(_opts) do
    Acl.save()
  end
end
