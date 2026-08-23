defmodule FerricstoreHttp.HTTP do
  @moduledoc false

  alias FerricstoreHttp.Deadline

  @msgpack_content_type "application/vnd.ferricstore.commands+msgpack"
  @context_headers ~w(x-ferricstore-subject x-ferricstore-scopes)

  @type command_format :: :json | :msgpack

  @spec command_format(:cowboy_req.req()) :: command_format()
  def command_format(req) do
    "content-type"
    |> :cowboy_req.header(req, "application/json")
    |> String.split(";", parts: 2)
    |> hd()
    |> String.trim()
    |> String.downcase()
    |> then(fn content_type ->
      if content_type == @msgpack_content_type, do: :msgpack, else: :json
    end)
  end

  @spec request_context_headers(:cowboy_req.req()) :: map()
  def request_context_headers(req) do
    Map.new(@context_headers, fn name ->
      {name, :cowboy_req.header(name, req, nil)}
    end)
  end

  @spec read_command(:cowboy_req.req(), pos_integer(), Deadline.t()) ::
          {:ok, map(), :cowboy_req.req()} | {:error, atom(), :cowboy_req.req()}
  def read_command(req, max_body_bytes, deadline) do
    with :ok <- check_body_length(req, max_body_bytes),
         {:ok, body, req} <- read_body(req, max_body_bytes, deadline, [], 0) do
      decode(body, req, command_format(req))
    else
      {:error, reason, req} -> {:error, reason, req}
      {:error, reason} -> {:error, reason, req}
    end
  end

  @spec read_json(:cowboy_req.req(), pos_integer(), Deadline.t()) ::
          {:ok, map(), :cowboy_req.req()} | {:error, atom(), :cowboy_req.req()}
  def read_json(req, max_body_bytes, deadline) do
    with :ok <- check_body_length(req, max_body_bytes),
         {:ok, body, req} <- read_body(req, max_body_bytes, deadline, [], 0) do
      decode(body, req, :json)
    else
      {:error, reason, req} -> {:error, reason, req}
      {:error, reason} -> {:error, reason, req}
    end
  end

  @spec reply_command(:cowboy_req.req(), non_neg_integer(), map(), command_format(), map()) ::
          :cowboy_req.req()
  def reply_command(req, status, body, format, extra_headers \\ %{})

  def reply_command(req, status, body, :json, extra_headers) do
    headers = Map.put(extra_headers, "content-type", "application/json; charset=utf-8")
    :cowboy_req.reply(status, headers, Jason.encode!(body), req)
  end

  def reply_command(req, status, body, :msgpack, extra_headers) do
    headers = Map.put(extra_headers, "content-type", @msgpack_content_type)
    encoded = body |> messagepack_safe() |> Msgpax.pack!()
    :cowboy_req.reply(status, headers, encoded, req)
  end

  @spec reply_json(:cowboy_req.req(), non_neg_integer(), map(), map()) :: :cowboy_req.req()
  def reply_json(req, status, body, extra_headers \\ %{}) do
    reply_command(req, status, body, :json, extra_headers)
  end

  @spec reply_binary(:cowboy_req.req(), non_neg_integer(), binary(), binary()) ::
          :cowboy_req.req()
  def reply_binary(req, status, body, content_type) do
    :cowboy_req.reply(
      status,
      %{"cache-control" => "no-store", "content-type" => content_type},
      body,
      req
    )
  end

  @spec reply_text(:cowboy_req.req(), non_neg_integer(), binary(), binary()) :: :cowboy_req.req()
  def reply_text(req, status, body, content_type) do
    :cowboy_req.reply(status, %{"content-type" => content_type}, body, req)
  end

  defp check_body_length(req, max_body_bytes) do
    case :cowboy_req.body_length(req) do
      length when is_integer(length) and length > max_body_bytes -> {:error, :body_too_large}
      _unknown_or_allowed -> :ok
    end
  end

  defp read_body(req, max_body_bytes, deadline, chunks, bytes_read) do
    case Deadline.remaining_ms(deadline) do
      {:ok, timeout} ->
        remaining_capacity = max_body_bytes - bytes_read

        options = %{
          length: remaining_capacity + 1,
          period: max(timeout - 1, 0),
          timeout: timeout
        }

        try do
          case :cowboy_req.read_body(req, options) do
            {:ok, body, req} ->
              finish_body(body, req, chunks, bytes_read, max_body_bytes)

            {:more, body, req} ->
              continue_body(body, req, chunks, bytes_read, max_body_bytes, deadline)
          end
        catch
          :exit, :timeout -> {:error, :request_timeout, req}
        end

      {:error, :deadline_exceeded} ->
        {:error, :request_timeout, req}
    end
  end

  defp finish_body(body, req, chunks, bytes_read, max_body_bytes) do
    total = bytes_read + byte_size(body)

    if total <= max_body_bytes do
      {:ok, chunks |> Enum.reverse([body]) |> :erlang.iolist_to_binary(), req}
    else
      {:error, :body_too_large, req}
    end
  end

  defp continue_body(body, req, chunks, bytes_read, max_body_bytes, deadline) do
    total = bytes_read + byte_size(body)

    if total <= max_body_bytes do
      read_body(req, max_body_bytes, deadline, [body | chunks], total)
    else
      {:error, :body_too_large, req}
    end
  end

  defp decode(body, req, :json) do
    case Jason.decode(body) do
      {:ok, %{} = envelope} -> {:ok, envelope, req}
      _invalid -> {:error, :malformed_json, req}
    end
  end

  defp decode(body, req, :msgpack) do
    case Msgpax.unpack(body) do
      {:ok, %{} = envelope} -> {:ok, envelope, req}
      _invalid -> {:error, :malformed_msgpack, req}
    end
  end

  defp messagepack_safe(%Msgpax.Bin{} = binary), do: binary

  defp messagepack_safe(binary) when is_binary(binary) do
    if String.valid?(binary), do: binary, else: Msgpax.Bin.new(binary)
  end

  defp messagepack_safe(list) when is_list(list), do: Enum.map(list, &messagepack_safe/1)

  defp messagepack_safe(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {messagepack_safe(key), messagepack_safe(value)} end)
  end

  defp messagepack_safe(value), do: value
end
