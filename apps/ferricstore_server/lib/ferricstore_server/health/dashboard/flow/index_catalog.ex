defmodule FerricstoreServer.Health.Dashboard.Flow.IndexCatalog do
  @moduledoc false
  alias FerricstoreServer.Health.Dashboard.Access

  def collect(opts) do
    if Access.flow_command_allowed_for_acl?(
         "FLOW.QUERY.INDEXES",
         Access.keyspace_acl_username(opts)
       ) do
      case Ferricstore.Flow.Query.IndexStatus.fetch(FerricStore.Instance.get(:default)) do
        {:ok, snapshot} -> %{status: :ok, snapshot: snapshot}
        {:error, _} -> %{status: :unavailable}
      end
    else
      %{status: :forbidden}
    end
  rescue
    _ -> %{status: :unavailable}
  catch
    :exit, _ -> %{status: :unavailable}
  end
end
