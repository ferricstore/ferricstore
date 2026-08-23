defmodule FerricstoreHttp.Targets.HttpEndpoint do
  @moduledoc "Generic HTTP target adapter for asynchronous invocations."

  require Logger

  @behaviour FerricstoreHttp.Target

  alias FerricstoreHttp.Targets.EgressPolicy

  @blocked_headers ~w(connection content-length host transfer-encoding)
  @max_response_header_bytes 65_536

  @impl FerricstoreHttp.Target
  def invoke(target, request, opts \\ [])

  def invoke(%{"kind" => "http_endpoint"} = target, request, opts) when is_map(request) do
    with {:ok, uri} <- validated_url(target["url"]),
         {:ok, address} <-
           EgressPolicy.authorize_uri(
             uri,
             Keyword.get(opts, :egress_policy, EgressPolicy.default())
           ),
         {:ok, body} <- Jason.encode(request),
         {:ok, headers} <- headers(target) do
      timeout = timeout(target, opts)
      max_bytes = Keyword.get(opts, :max_response_bytes, 1_048_576)

      uri
      |> request(
        address,
        [{"content-type", "application/json"} | headers],
        body,
        timeout,
        max_bytes
      )
      |> normalize_response(uri)
    end
  end

  def invoke(_target, _request, _opts), do: {:error, :unsupported_target}

  defp validated_url(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host} = uri
      when scheme in ["http", "https"] and is_binary(host) ->
        {:ok, uri}

      _uri ->
        {:error, :invalid_http_endpoint_url}
    end
  end

  defp validated_url(_url), do: {:error, :invalid_http_endpoint_url}

  defp headers(target) do
    with {:ok, static_headers} <- static_headers(Map.get(target, "headers", %{})),
         {:ok, auth_headers} <- auth_headers(target["auth"]) do
      headers =
        static_headers
        |> Enum.reject(fn {key, _value} ->
          blocked_header?(key) or not valid_header_name?(key)
        end)
        |> Enum.map(fn {key, value} -> {to_string(key), to_string(value)} end)
        |> merge_auth_headers(auth_headers)

      {:ok, headers}
    end
  end

  defp static_headers(%{} = headers) do
    if Enum.all?(headers, fn {key, value} -> is_binary(key) and is_binary(value) end),
      do: {:ok, headers},
      else: {:error, %{"code" => "invalid_target_headers"}}
  end

  defp static_headers(_headers), do: {:error, %{"code" => "invalid_target_headers"}}

  defp auth_headers(nil), do: {:ok, []}

  defp auth_headers(%{"type" => "bearer_env", "env" => env}) do
    with {:ok, secret} <- env_secret(env), do: {:ok, [{"authorization", "Bearer " <> secret}]}
  end

  defp auth_headers(%{"type" => "header_env", "env" => env, "header" => header}) do
    with :ok <- validate_auth_header(header),
         {:ok, secret} <- env_secret(env),
         do: {:ok, [{to_string(header), secret}]}
  end

  defp auth_headers(_auth), do: {:error, %{"code" => "invalid_target_auth"}}

  defp env_secret(env) when is_binary(env) and env != "" do
    case System.get_env(env) do
      value when is_binary(value) and value != "" ->
        {:ok, value}

      _missing ->
        {:error, %{"code" => "target_auth_secret_missing"}}
    end
  end

  defp env_secret(_env), do: {:error, %{"code" => "invalid_target_auth"}}

  defp validate_auth_header(header) when is_binary(header) do
    if blocked_header?(header) or not valid_header_name?(header),
      do: {:error, %{"code" => "invalid_target_auth_header"}},
      else: :ok
  end

  defp validate_auth_header(_header),
    do: {:error, %{"code" => "invalid_target_auth_header"}}

  defp merge_auth_headers(static_headers, []), do: static_headers

  defp merge_auth_headers(static_headers, auth_headers) do
    names = MapSet.new(auth_headers, fn {key, _value} -> String.downcase(key) end)

    static_headers
    |> Enum.reject(fn {key, _value} -> MapSet.member?(names, String.downcase(key)) end)
    |> Kernel.++(auth_headers)
  end

  defp blocked_header?(key), do: String.downcase(to_string(key)) in @blocked_headers

  defp valid_header_name?(key),
    do: String.match?(to_string(key), ~r/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/)

  defp timeout(target, opts) do
    case target["timeout_ms"] do
      value when is_integer(value) and value > 0 -> value
      _invalid -> Keyword.get(opts, :timeout, 30_000)
    end
  end

  defp request(uri, address, headers, body, timeout, max_bytes) do
    task =
      Task.async(fn ->
        do_request(uri, address, headers, body, timeout, max_bytes)
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      _timeout_or_exit -> {:error, :transport}
    end
  end

  defp do_request(uri, address, headers, body, timeout, max_bytes) do
    deadline = System.monotonic_time(:millisecond) + timeout

    with {:ok, connection} <- connect(uri, address, timeout),
         {:ok, connection, request_ref} <-
           Mint.HTTP.request(connection, "POST", request_path(uri), headers, body) do
      receive_response(connection, request_ref, deadline, max_bytes, %{
        status: nil,
        chunks: [],
        size: 0
      })
    else
      {:error, connection, _reason} ->
        close(connection)
        {:error, :transport}

      {:error, _reason} ->
        {:error, :transport}
    end
  end

  defp connect(uri, address, timeout) do
    pinned_address = address |> :inet.ntoa() |> List.to_string()

    Mint.HTTP.connect(
      String.to_existing_atom(uri.scheme),
      pinned_address,
      uri.port || default_port(uri),
      hostname: uri.host,
      mode: :passive,
      protocols: [:http1],
      max_header_list_size: @max_response_header_bytes,
      transport_opts: transport_opts(uri, address, timeout)
    )
  end

  defp transport_opts(%URI{scheme: "https"}, address, timeout),
    do:
      [
        timeout: timeout,
        send_timeout: timeout,
        send_timeout_close: true,
        cacerts: :public_key.cacerts_get()
      ] ++ address_family(address)

  defp transport_opts(_uri, address, timeout),
    do:
      [timeout: timeout, send_timeout: timeout, send_timeout_close: true] ++
        address_family(address)

  defp address_family(address) when tuple_size(address) == 8, do: [inet4: false, inet6: true]
  defp address_family(_address), do: [inet4: true, inet6: false]

  defp default_port(%URI{scheme: "https"}), do: 443
  defp default_port(_uri), do: 80

  defp request_path(uri) do
    path = if uri.path in [nil, ""], do: "/", else: uri.path
    if is_binary(uri.query), do: path <> "?" <> uri.query, else: path
  end

  defp receive_response(connection, request_ref, deadline, max_bytes, state) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    case Mint.HTTP.recv(connection, 0, timeout) do
      {:ok, connection, responses} ->
        handle_responses(connection, request_ref, deadline, max_bytes, state, responses)

      {:error, connection, _reason, responses} ->
        case consume_responses(responses, request_ref, max_bytes, state) do
          {:done, state} -> finish(connection, state)
          {:error, :too_large} -> too_large(connection)
          _incomplete -> transport_error(connection)
        end
    end
  end

  defp handle_responses(connection, request_ref, deadline, max_bytes, state, responses) do
    case consume_responses(responses, request_ref, max_bytes, state) do
      {:continue, state} ->
        receive_response(connection, request_ref, deadline, max_bytes, state)

      {:done, state} ->
        finish(connection, state)

      {:error, :too_large} ->
        too_large(connection)

      {:error, :transport} ->
        transport_error(connection)
    end
  end

  defp consume_responses(responses, request_ref, max_bytes, state) do
    Enum.reduce_while(responses, {:continue, state}, fn
      {:status, ^request_ref, status}, {:continue, state} ->
        {:cont, {:continue, %{state | status: status}}}

      {:headers, ^request_ref, headers}, {:continue, state} ->
        if response_too_large?(headers, max_bytes),
          do: {:halt, {:error, :too_large}},
          else: {:cont, {:continue, state}}

      {:data, ^request_ref, chunk}, {:continue, state} ->
        size = state.size + byte_size(chunk)

        if size > max_bytes do
          {:halt, {:error, :too_large}}
        else
          {:cont, {:continue, %{state | chunks: [chunk | state.chunks], size: size}}}
        end

      {:done, ^request_ref}, {:continue, state} ->
        {:halt, {:done, state}}

      {:error, ^request_ref, _reason}, _state ->
        {:halt, {:error, :transport}}

      _other, accumulator ->
        {:cont, accumulator}
    end)
  end

  defp response_too_large?(headers, max_bytes) do
    Enum.any?(headers, fn
      {"content-length", value} ->
        case Integer.parse(value) do
          {length, ""} -> length > max_bytes
          _invalid -> false
        end

      _header ->
        false
    end)
  end

  defp finish(connection, %{status: status, chunks: chunks}) when is_integer(status) do
    close(connection)
    {:ok, {status, chunks |> Enum.reverse() |> IO.iodata_to_binary()}}
  end

  defp finish(connection, _state), do: transport_error(connection)

  defp too_large(connection) do
    close(connection)
    {:error, :too_large}
  end

  defp transport_error(connection) do
    close(connection)
    {:error, :transport}
  end

  defp close(connection) do
    _closed = Mint.HTTP.close(connection)
    :ok
  end

  defp normalize_response({:error, :too_large}, uri) do
    Logger.warning("HTTP invocation target response exceeded limit host=#{uri.host}")
    {:error, %{"code" => "target_response_too_large"}}
  end

  defp normalize_response({:ok, {status, body}}, uri)
       when status in 200..299 do
    Logger.debug("HTTP invocation target completed host=#{uri.host} status=#{status}")
    {:ok, decode_body(body, status)}
  end

  defp normalize_response(
         {:ok, {status, _body}},
         uri
       )
       when status == 429 or status in 500..599 do
    Logger.warning("HTTP invocation target transient failure host=#{uri.host} status=#{status}")
    {:retry, %{"code" => "http_endpoint_transient_failure", "status" => status}}
  end

  defp normalize_response(
         {:ok, {status, _body}},
         uri
       ) do
    Logger.warning("HTTP invocation target terminal failure host=#{uri.host} status=#{status}")
    {:error, %{"code" => "http_endpoint_failed", "status" => status}}
  end

  defp normalize_response({:error, _reason}, uri) do
    Logger.warning("HTTP invocation target transport error host=#{uri.host}")
    {:retry, %{"code" => "http_endpoint_transport_error"}}
  end

  defp decode_body("", status), do: %{"status" => status}

  defp decode_body(body, _status) do
    case Jason.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> body
    end
  end
end
