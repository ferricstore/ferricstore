defmodule FerricstoreHttp do
  @moduledoc """
  Reusable in-process HTTP protocol server for FerricStore.

  The HTTP transport delegates authentication and command execution to the
  transport-neutral gateways in `ferricstore_server`. It does not open native
  TCP connections or implement FerricStore command semantics.
  """

  @doc "Returns a child specification for an explicitly configured HTTP server."
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    %{
      id: Keyword.get(opts, :name, FerricstoreHttp.Server),
      start: {FerricstoreHttp.Server, :start_link, [opts]},
      type: :supervisor
    }
  end
end
