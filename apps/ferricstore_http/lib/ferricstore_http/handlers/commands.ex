defmodule FerricstoreHttp.Handlers.Commands do
  @moduledoc false

  @behaviour :cowboy_handler

  alias FerricstoreHttp.{Auth, CommandBatcher, CommandService, Deadline, ErrorResponse, HTTP}

  @impl :cowboy_handler
  def init(req, config) do
    format = HTTP.command_format(req)

    if :cowboy_req.method(req) == "POST" do
      handle_post(req, config, format)
    else
      reply_error(req, config, format, {:method_not_allowed, :post})
    end
  end

  defp handle_post(req, config, format) do
    deadline = Deadline.new(config.request_timeout_ms)
    authorization = :cowboy_req.header("authorization", req)
    peer = :cowboy_req.peer(req)
    headers = HTTP.request_context_headers(req)

    result =
      with {:ok, authenticated} <- Auth.resolve(authorization, peer, headers, config),
           {:ok, envelope, req} <- HTTP.read_command(req, config.max_body_bytes, deadline),
           {:ok, prepared} <- CommandService.prepare(envelope),
           {:ok, response} <- execute(prepared, authenticated, config, deadline) do
        {:ok, req, response}
      end

    case result do
      {:ok, req, response} ->
        req = HTTP.reply_command(req, 200, response, format)
        {:ok, req, config}

      {:error, reason, req} ->
        reply_error(req, config, format, reason)

      {:error, reason} ->
        reply_error(req, config, format, reason)
    end
  end

  defp execute(prepared, authenticated, config, deadline) do
    case CommandBatcher.execute(authenticated, prepared, config, deadline) do
      {:error, :reauthentication_required} = error ->
        :ok = Auth.invalidate(authenticated)
        error

      result ->
        result
    end
  end

  defp reply_error(req, config, format, {:method_not_allowed, _method}) do
    body = %{"error" => %{"code" => "method_not_allowed"}}

    req =
      HTTP.reply_command(req, 405, body, format, %{
        "allow" => "POST",
        "cache-control" => "no-store"
      })

    {:ok, req, config}
  end

  defp reply_error(req, config, format, reason) do
    {status, body, headers} = ErrorResponse.response(reason)
    req = HTTP.reply_command(req, status, body, format, headers)
    {:ok, req, config}
  end
end
