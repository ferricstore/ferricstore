defmodule FerricstoreServer.Native.RequestContextTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.RequestContext

  setup do
    previous = Application.get_env(:ferricstore, :native_trusted_request_context_users)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :native_trusted_request_context_users)
        value -> Application.put_env(:ferricstore, :native_trusted_request_context_users, value)
      end
    end)

    :ok
  end

  test "normalizes configured trusted native users once at connection setup" do
    for {configured, expected} <- [
          {["proxy", :internal, 42], MapSet.new(["proxy", "internal"])},
          {"proxy, internal", MapSet.new(["proxy", "internal"])},
          {:all, MapSet.new(["*"])},
          {%{"invalid" => true}, MapSet.new()}
        ] do
      Application.put_env(:ferricstore, :native_trusted_request_context_users, configured)
      assert RequestContext.configured_trusted_users() == expected
    end
  end

  test "normalizes only bounded authority for a trusted connection" do
    state = %{
      username: "proxy",
      trusted_request_context_users: MapSet.new(["proxy"])
    }

    assert {:ok, context} =
             RequestContext.from_payload(
               %{
                 "request_context" => %{
                   "subject" => "client-1",
                   :tenant => "tenant-a",
                   "scopes" => "tenant:a:read tenant:a:read invocation:create:*",
                   "ignored" => "untrusted-field"
                 }
               },
               state
             )

    assert context == %{
             "subject" => "client-1",
             "tenant" => "tenant-a",
             "scopes" => ["tenant:a:read", "invocation:create:*"]
           }

    assert {:ok, %{}} =
             RequestContext.from_payload(
               %{"request_context" => "not-an-object"},
               %{state | trusted_request_context_users: MapSet.new()}
             )
  end

  test "accepts exact authority limits and rejects the next byte or scope" do
    state = %{username: "proxy", trusted_request_context_users: ["proxy"]}
    max_identity = String.duplicate("i", 4_096)
    max_scope = String.duplicate("s", 1_024)

    max_scopes =
      Enum.map(1..64, fn index ->
        prefix = Integer.to_string(index) <> ":"
        prefix <> String.duplicate("s", 1_024 - byte_size(prefix))
      end)

    assert {:ok, %{"subject" => ^max_identity, "scopes" => ^max_scopes}} =
             RequestContext.from_payload(
               %{"request_context" => %{"subject" => max_identity, "scopes" => max_scopes}},
               state
             )

    for context <- [
          %{"subject" => max_identity <> "x"},
          %{"scopes" => [max_scope <> "x"]},
          %{"scopes" => List.duplicate("scope", 65)}
        ] do
      assert {:error, "ERR native request_context exceeds limits"} =
               RequestContext.from_payload(%{"request_context" => context}, state)
    end
  end

  test "rejects malformed or conflicting trusted authority instead of dropping it" do
    state = %{username: "proxy", trusted_request_context_users: ["proxy"]}

    for context <- [
          %{"tenant" => 42},
          %{"subject" => []},
          %{"scopes" => %{}},
          %{"scopes" => ["tenant:a:read", 42]},
          %{"tenant" => "tenant-a", tenant: "tenant-b"}
        ] do
      assert {:error, "ERR native request_context contains invalid authority"} =
               RequestContext.from_payload(%{"request_context" => context}, state)
    end
  end
end
