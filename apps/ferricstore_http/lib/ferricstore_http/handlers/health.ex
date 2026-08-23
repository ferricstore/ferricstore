defmodule FerricstoreHttp.Handlers.Health do
  @moduledoc false

  @behaviour :cowboy_handler

  @impl :cowboy_handler
  def init(req, config) do
    req = FerricstoreHttp.HTTP.reply_command(req, 200, %{"status" => "ok"}, :json)
    {:ok, req, config}
  end
end
