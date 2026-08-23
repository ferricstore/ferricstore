defmodule FerricstoreHttp.Handlers.Metrics do
  @moduledoc false

  @behaviour :cowboy_handler

  @impl :cowboy_handler
  def init(req, config) do
    req =
      FerricstoreHttp.HTTP.reply_text(
        req,
        200,
        FerricstoreHttp.Metrics.render(),
        "text/plain; version=0.0.4"
      )

    {:ok, req, config}
  end
end
