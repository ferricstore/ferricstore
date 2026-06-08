defmodule FerricstoreServer.Test.DashboardCase do
  @moduledoc """
  Shared helpers for dashboard HTTP/rendering tests.
  """

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: false

      @moduletag :dashboard
      @moduletag timeout: 60_000

      import FerricstoreServer.Test.DashboardCase
    end
  end

  @spec restore_env(atom(), term()) :: :ok
  def restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  def restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  @spec http_get(:inet.port_number(), binary(), [{binary(), binary()}]) :: binary()
  def http_get(port, path, headers \\ []) do
    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    header_lines =
      headers
      |> Enum.map(fn {name, value} -> "#{name}: #{value}\r\n" end)
      |> Enum.join()

    :ok =
      :gen_tcp.send(
        conn,
        "GET #{path} HTTP/1.1\r\nHost: localhost\r\n" <>
          header_lines <>
          "Connection: close\r\n\r\n"
      )

    response = recv_all(conn, "")
    :gen_tcp.close(conn)
    response
  end

  @spec http_post_form(:inet.port_number(), binary(), map(), [{binary(), binary()}]) :: binary()
  def http_post_form(port, path, params, headers \\ []) do
    body = URI.encode_query(params)

    {:ok, conn} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    header_lines =
      headers
      |> Enum.map(fn {name, value} -> "#{name}: #{value}\r\n" end)
      |> Enum.join()

    request =
      "POST #{path} HTTP/1.1\r\n" <>
        "Host: localhost\r\n" <>
        header_lines <>
        "Content-Type: application/x-www-form-urlencoded\r\n" <>
        "Content-Length: #{byte_size(body)}\r\n" <>
        "Connection: close\r\n" <>
        "\r\n" <>
        body

    :ok = :gen_tcp.send(conn, request)

    response = recv_all(conn, "")
    :gen_tcp.close(conn)
    response
  end

  @spec extract_body(binary()) :: binary()
  def extract_body(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _ -> response
    end
  end

  @spec extract_headers(binary()) :: binary()
  def extract_headers(response) do
    case String.split(response, "\r\n\r\n", parts: 2) do
      [headers, _body] -> headers
      _ -> response
    end
  end

  @spec extract_status_code(binary()) :: non_neg_integer() | nil
  def extract_status_code(response) do
    case String.split(response, "\r\n", parts: 2) do
      [status_line, _rest] ->
        case Regex.run(~r/HTTP\/1\.\d\s+(\d+)/, status_line) do
          [_, code] -> String.to_integer(code)
          _ -> nil
        end

      _ ->
        nil
    end
  end

  @spec extract_header(binary(), binary()) :: binary() | nil
  def extract_header(response, name) do
    downcased = String.downcase(name)

    response
    |> extract_headers()
    |> String.split("\r\n")
    |> Enum.find_value(fn line ->
      case String.split(line, ":", parts: 2) do
        [header, value] ->
          if String.downcase(header) == downcased, do: String.trim(value), else: nil

        _ ->
          nil
      end
    end)
  end

  @spec dashboard_session_cookie(binary()) :: binary()
  def dashboard_session_cookie(response) do
    response
    |> extract_header("set-cookie")
    |> String.split(";", parts: 2)
    |> hd()
  end

  defp recv_all(conn, acc) do
    case :gen_tcp.recv(conn, 0, 5_000) do
      {:ok, data} -> recv_all(conn, acc <> data)
      {:error, :closed} -> acc
    end
  end
end
