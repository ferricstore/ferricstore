defmodule FerricstoreHttp.Handlers.Readiness do
  @moduledoc false

  @behaviour :cowboy_handler

  alias FerricstoreHttp.{HTTP, Readiness}

  @impl :cowboy_handler
  def init(req, config) do
    {status, body} =
      case Readiness.check(config) do
        :ok -> {200, %{"status" => "ready"}}
        {:error, :not_ready} -> {503, %{"status" => "not_ready"}}
      end

    req = HTTP.reply_command(req, status, body, :json)
    {:ok, req, config}
  end
end
