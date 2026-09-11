defmodule FerricstoreHttp.Handlers.Values do
  @moduledoc false

  @behaviour :cowboy_handler

  alias FerricstoreHttp.{Auth, Deadline, ErrorResponse, HTTP}
  alias FerricstoreHttp.Invocations.Values

  @impl :cowboy_handler
  def init(req, %{config: config, action: action} = state) do
    deadline = Deadline.new(config.request_timeout_ms)

    case authenticate(req, config) do
      {:ok, context} -> dispatch(action, :cowboy_req.method(req), req, state, context, deadline)
      {:error, reason} -> reply_error(req, state, reason)
    end
  end

  defp dispatch(:one, "GET", req, state, context, deadline) do
    result =
      execute(context, fn ->
        Values.get(binding(req, :id), binding(req, :name), context, state.config,
          deadline: deadline
        )
      end)

    case result do
      {:ok, body} -> reply_json(req, state, 200, body)
      {:error, reason} -> reply_error(req, state, reason)
    end
  end

  defp dispatch(:content, "GET", req, state, context, deadline) do
    result =
      execute(context, fn ->
        Values.content(binding(req, :id), binding(req, :name), context, state.config,
          deadline: deadline
        )
      end)

    case result do
      {:ok, body, content_type} ->
        req = HTTP.reply_binary(req, 200, body, content_type)
        {:ok, req, state}

      {:error, reason} ->
        reply_error(req, state, reason)
    end
  end

  defp dispatch(:batch, "POST", req, state, context, deadline) do
    with {:ok, %{"names" => names}, req} <-
           HTTP.read_json(req, state.config.max_body_bytes, deadline),
         {:ok, body} <-
           execute(context, fn ->
             Values.batch(binding(req, :id), names, context, state.config, deadline: deadline)
           end) do
      reply_json(req, state, 200, body)
    else
      {:error, reason, req} -> reply_error(req, state, reason)
      {:error, reason} -> reply_error(req, state, reason)
      _invalid -> reply_error(req, state, :malformed_json)
    end
  end

  defp dispatch(:put, "POST", req, state, context, deadline) do
    with {:ok, body, req} <- HTTP.read_json(req, state.config.max_body_bytes, deadline),
         {:ok, response} <-
           execute(context, fn ->
             Values.put(binding(req, :id), body, context, state.config, deadline: deadline)
           end) do
      reply_json(req, state, 201, response)
    else
      {:error, reason, req} -> reply_error(req, state, reason)
      {:error, reason} -> reply_error(req, state, reason)
    end
  end

  defp dispatch(_action, _method, req, state, _context, _deadline),
    do: method_not_allowed(req, state)

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

  defp binding(req, name) do
    case :cowboy_req.binding(name, req, nil) do
      :undefined -> nil
      value -> value
    end
  end

  defp method_not_allowed(req, state) do
    req =
      HTTP.reply_json(
        req,
        405,
        %{"error" => %{"code" => "method_not_allowed"}},
        %{"cache-control" => "no-store"}
      )

    {:ok, req, state}
  end

  defp reply_json(req, state, status, body) do
    req = HTTP.reply_json(req, status, body)
    {:ok, req, state}
  end

  defp reply_error(req, state, reason) do
    {status, body, headers} = ErrorResponse.response(reason)
    req = HTTP.reply_json(req, status, body, headers)
    {:ok, req, state}
  end
end
