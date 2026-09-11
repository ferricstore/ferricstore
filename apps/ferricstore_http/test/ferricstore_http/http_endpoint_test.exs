defmodule FerricstoreHttp.Targets.HttpEndpointTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias FerricstoreHttp.Targets.HttpEndpoint

  setup do
    ref = :"ferricstore_http_target_#{System.unique_integer([:positive])}"
    dispatch = :cowboy_router.compile([{:_, [{:_, __MODULE__.Handler, test_pid: self()}]}])
    {:ok, _pid} = :cowboy.start_clear(ref, [{:port, 0}], %{env: %{dispatch: dispatch}})
    port = :ranch.get_port(ref)
    on_exit(fn -> :cowboy.stop_listener(ref) end)
    {:ok, base_url: "http://127.0.0.1:#{port}"}
  end

  test "posts invocation JSON and decodes successful responses", %{base_url: base_url} do
    target = %{"kind" => "http_endpoint", "url" => "#{base_url}/ok"}

    assert {:ok, %{"ok" => true, "id" => "inv-1"}} =
             HttpEndpoint.invoke(target, %{"invocation_id" => "inv-1"})
  end

  test "loads target authorization from an environment variable", %{base_url: base_url} do
    with_env("FERRICSTORE_HTTP_TEST_TARGET_TOKEN", "target-secret", fn ->
      target = %{
        "kind" => "http_endpoint",
        "url" => "#{base_url}/auth",
        "auth" => %{"type" => "bearer_env", "env" => "FERRICSTORE_HTTP_TEST_TARGET_TOKEN"}
      }

      assert {:ok, %{"ok" => true}} = HttpEndpoint.invoke(target, %{"invocation_id" => "inv-1"})
    end)
  end

  test "maps transient, terminal, and oversized responses", %{base_url: base_url} do
    assert {:retry, %{"code" => "http_endpoint_transient_failure", "status" => 503}} =
             HttpEndpoint.invoke(
               %{"kind" => "http_endpoint", "url" => "#{base_url}/unavailable"},
               %{}
             )

    assert {:error, %{"code" => "http_endpoint_failed", "status" => 400}} =
             HttpEndpoint.invoke(
               %{"kind" => "http_endpoint", "url" => "#{base_url}/bad-request"},
               %{}
             )

    assert {:error, %{"code" => "target_response_too_large"}} =
             HttpEndpoint.invoke(
               %{"kind" => "http_endpoint", "url" => "#{base_url}/large"},
               %{},
               max_response_bytes: 8
             )
  end

  test "does not follow redirects or retry Retry-After responses", %{base_url: base_url} do
    assert {:error, %{"code" => "http_endpoint_failed", "status" => 302}} =
             HttpEndpoint.invoke(
               %{"kind" => "http_endpoint", "url" => "#{base_url}/redirect"},
               %{}
             )

    refute_receive :redirect_followed, 50

    assert {:retry, %{"code" => "http_endpoint_transient_failure", "status" => 503}} =
             HttpEndpoint.invoke(
               %{"kind" => "http_endpoint", "url" => "#{base_url}/retry-after"},
               %{}
             )

    assert_receive :retry_after_requested
    refute_receive :retry_after_requested, 100
  end

  test "stops reading a chunked response when it crosses the configured limit", %{
    base_url: base_url
  } do
    assert {:error, %{"code" => "target_response_too_large"}} =
             HttpEndpoint.invoke(
               %{"kind" => "http_endpoint", "url" => "#{base_url}/chunked-large"},
               %{},
               max_response_bytes: 1_024
             )
  end

  test "enforces the egress policy before connecting" do
    assert {:error, :target_host_not_allowed} =
             HttpEndpoint.invoke(
               %{"kind" => "http_endpoint", "url" => "https://blocked.example.com/invoke"},
               %{},
               egress_policy: %{allowed_hosts: ["allowed.example.com"]}
             )
  end

  test "falls back from an invalid target timeout and rejects malformed headers", %{
    base_url: base_url
  } do
    assert {:ok, %{"ok" => true, "id" => "inv-1"}} =
             HttpEndpoint.invoke(
               %{
                 "kind" => "http_endpoint",
                 "url" => "#{base_url}/ok",
                 "timeout_ms" => "1000"
               },
               %{"invocation_id" => "inv-1"},
               timeout: 1_000
             )

    assert {:error, %{"code" => "invalid_target_headers"}} =
             HttpEndpoint.invoke(
               %{
                 "kind" => "http_endpoint",
                 "url" => "#{base_url}/ok",
                 "headers" => ["not-a-map"]
               },
               %{}
             )
  end

  test "bounds request writes when a target accepts but does not read" do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, {_address, port}} = :inet.sockname(listener)

    acceptor =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)

        receive do
          :close -> :gen_tcp.close(socket)
        end
      end)

    request =
      Task.async(fn ->
        HttpEndpoint.invoke(
          %{
            "kind" => "http_endpoint",
            "url" => "http://127.0.0.1:#{port}/stalled",
            "timeout_ms" => 50
          },
          %{"payload" => String.duplicate("x", 16 * 1_024 * 1_024)}
        )
      end)

    result = Task.yield(request, 1_000) || Task.shutdown(request, :brutal_kill)
    send(acceptor.pid, :close)
    Task.await(acceptor, 1_000)
    :gen_tcp.close(listener)

    assert {:ok, {:retry, %{"code" => "http_endpoint_transport_error"}}} = result
  end

  defp with_env(key, value, operation) do
    previous = System.get_env(key)
    System.put_env(key, value)

    try do
      operation.()
    after
      if previous, do: System.put_env(key, previous), else: System.delete_env(key)
    end
  end
end

defmodule FerricstoreHttp.Targets.HttpEndpointTest.Handler do
  @moduledoc false

  @behaviour :cowboy_handler

  @impl :cowboy_handler
  def init(req, state) do
    {:ok, body, req} = :cowboy_req.read_body(req)

    case :cowboy_req.path(req) do
      "/chunked-large" -> stream_large_response(req, state)
      _other -> reply(req, state, body)
    end
  end

  defp stream_large_response(req, state) do
    req = :cowboy_req.stream_reply(200, %{"content-type" => "application/octet-stream"}, req)

    req =
      Enum.reduce_while(1..64, req, fn _index, req ->
        case :cowboy_req.stream_body(String.duplicate("x", 1_024), :nofin, req) do
          :ok -> {:cont, req}
          {:error, _reason} -> {:halt, req}
        end
      end)

    {:ok, req, state}
  end

  defp reply(req, state, body) do
    test_pid = Keyword.fetch!(state, :test_pid)

    {status, headers, response} =
      case :cowboy_req.path(req) do
        "/ok" ->
          {200, %{}, Jason.encode!(%{"ok" => true, "id" => Jason.decode!(body)["invocation_id"]})}

        "/auth" ->
          if :cowboy_req.header("authorization", req) == "Bearer target-secret",
            do: {200, %{}, Jason.encode!(%{"ok" => true})},
            else: {401, %{}, Jason.encode!(%{"error" => "unauthorized"})}

        "/unavailable" ->
          {503, %{}, Jason.encode!(%{"error" => "temporary"})}

        "/bad-request" ->
          {400, %{}, Jason.encode!(%{"error" => "bad request"})}

        "/large" ->
          {200, %{}, String.duplicate("x", 64)}

        "/redirect" ->
          {302, %{"location" => "/redirect-target"}, ""}

        "/redirect-target" ->
          send(test_pid, :redirect_followed)
          {200, %{}, Jason.encode!(%{"followed" => true})}

        "/retry-after" ->
          send(test_pid, :retry_after_requested)
          {503, %{"retry-after" => "0"}, Jason.encode!(%{"error" => "temporary"})}
      end

    headers = Map.put(headers, "content-type", "application/json")
    req = :cowboy_req.reply(status, headers, response, req)
    {:ok, req, state}
  end
end
