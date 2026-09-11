defmodule FerricstoreHttp.Router do
  @moduledoc false

  alias FerricstoreHttp.Handlers

  @spec dispatch(FerricstoreHttp.Config.t()) :: :cowboy_router.dispatch_rules()
  def dispatch(config) do
    :cowboy_router.compile([
      {:_,
       [
         {"/health", Handlers.Health, config},
         {"/ready", Handlers.Readiness, config},
         {"/v1/commands", Handlers.Commands, config}
       ] ++ metrics_routes(config) ++ invocation_routes(config)}
    ])
  end

  defp metrics_routes(%{metrics_enabled: false}), do: []
  defp metrics_routes(config), do: [{"/metrics", Handlers.Metrics, config}]

  defp invocation_routes(%{invocations_enabled: false}), do: []

  defp invocation_routes(config) do
    [
      {"/v1/invocations/:id/result", Handlers.Invocations, config},
      {"/v1/invocations/:id/values/batch", Handlers.Values, %{config: config, action: :batch}},
      {"/v1/invocations/:id/values/:name/content", Handlers.Values,
       %{config: config, action: :content}},
      {"/v1/invocations/:id/values/:name", Handlers.Values, %{config: config, action: :one}},
      {"/v1/invocations/:id/values", Handlers.Values, %{config: config, action: :put}},
      {"/v1/invocations/:id", Handlers.Invocations, config}
    ]
  end
end
