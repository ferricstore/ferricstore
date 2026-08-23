defmodule FerricstoreHttp.Handlers.Invocations do
  @moduledoc false

  @behaviour :cowboy_handler

  alias FerricstoreHttp.{Auth, Deadline, ErrorResponse, HTTP}
  alias FerricstoreHttp.Invocations.Service

  @impl :cowboy_handler
  def init(req, config) do
    case {:cowboy_req.method(req), result_path?(req)} do
      {"POST", false} -> create(req, config)
      {"GET", false} -> get(req, config)
      {"GET", true} -> result(req, config)
      _other -> method_not_allowed(req, config)
    end
  end

  defp create(req, config) do
    name = binding(req, :id)
    deadline = Deadline.new(config.request_timeout_ms)

    result =
      with {:ok, context} <- authenticate(req, config),
           {:ok, envelope, req} <- HTTP.read_json(req, config.max_body_bytes, deadline),
           {:ok, body} <-
             execute(context, fn ->
               Service.create(name, envelope, context, config,
                 idempotency_key: :cowboy_req.header("idempotency-key", req),
                 deadline: deadline
               )
             end) do
        {:ok, req, body}
      end

    case result do
      {:ok, req, body} -> reply_json(req, config, 202, body)
      {:error, reason, req} -> reply_error(req, config, reason)
      {:error, reason} -> reply_error(req, config, reason)
    end
  end

  defp get(req, config) do
    id = binding(req, :id)
    deadline = Deadline.new(config.request_timeout_ms)

    with {:ok, context} <- authenticate(req, config),
         {:ok, body} <-
           execute(context, fn -> Service.get(id, context, config, deadline: deadline) end) do
      reply_json(req, config, 200, body)
    else
      {:error, reason} -> reply_error(req, config, reason)
    end
  end

  defp result(req, config) do
    id = binding(req, :id)
    deadline = Deadline.new(config.request_timeout_ms)

    response =
      with {:ok, context} <- authenticate(req, config) do
        execute(context, fn -> Service.result(id, context, config, deadline: deadline) end)
      end

    case response do
      {:ok, body} -> reply_json(req, config, 200, body)
      {:pending, body} -> reply_json(req, config, 202, body)
      {:error, reason} -> reply_error(req, config, reason)
    end
  end

  defp authenticate(req, config) do
    Auth.resolve(
      :cowboy_req.header("authorization", req),
      :cowboy_req.peer(req),
      HTTP.request_context_headers(req),
      config
    )
  end

  defp execute(context, operation) do
    case operation.() do
      {:error, :reauthentication_required} = error ->
        :ok = Auth.invalidate(context)
        error

      result ->
        result
    end
  end

  defp result_path?(req), do: req |> :cowboy_req.path() |> String.ends_with?("/result")

  defp binding(req, name) do
    case :cowboy_req.binding(name, req, nil) do
      :undefined -> nil
      value -> value
    end
  end

  defp method_not_allowed(req, config) do
    req =
      HTTP.reply_json(
        req,
        405,
        %{"error" => %{"code" => "method_not_allowed"}},
        %{"cache-control" => "no-store"}
      )

    {:ok, req, config}
  end

  defp reply_json(req, config, status, body) do
    req = HTTP.reply_json(req, status, body)
    {:ok, req, config}
  end

  defp reply_error(req, config, reason) do
    {status, body, headers} = ErrorResponse.response(reason)
    req = HTTP.reply_json(req, status, body, headers)
    {:ok, req, config}
  end
end
