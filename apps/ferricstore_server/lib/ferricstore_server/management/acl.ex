defmodule FerricstoreServer.Management.ACL do
  @moduledoc """
  Server-backed implementation of the core ACL management contract.

  The core `:ferricstore` application keeps ACL management pluggable so
  embedded deployments can opt in explicitly. The standalone server owns the
  network ACL table, so it wires this adapter during application startup.
  """

  @behaviour FerricStore.Management.ACL

  alias Ferricstore.Store.Router
  alias FerricstoreServer.Acl

  @impl true
  def set_user(username, rules, opts) do
    with {:ok, store} <- mutation_store(opts) do
      Router.server_command(store, {:acl_setuser, username, rules})
    end
  end

  @impl true
  def del_user(username, opts) do
    with {:ok, store} <- mutation_store(opts),
         result <- Router.server_command(store, {:acl_deluser, username}) do
      case result do
        :ok -> {:ok, 1}
        {:error, reason} -> {:error, reason}
      end
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

  defp mutation_store(opts) when is_list(opts) do
    case Keyword.fetch(opts, :store) do
      {:ok, %{shard_count: shard_count} = store}
      when is_integer(shard_count) and shard_count > 0 ->
        {:ok, store}

      _other ->
        {:error, "ERR ACL mutation requires a FerricStore instance"}
    end
  end

  defp mutation_store(_opts), do: {:error, "ERR ACL mutation requires a FerricStore instance"}
end
