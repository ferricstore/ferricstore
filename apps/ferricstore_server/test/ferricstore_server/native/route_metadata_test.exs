defmodule FerricstoreServer.Native.RouteMetadataTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Native.RouteMetadata

  test "local endpoint uses explicit advertised host" do
    previous = Application.get_env(:ferricstore, :native_advertise_host)
    Application.put_env(:ferricstore, :native_advertise_host, "local.example")

    on_exit(fn ->
      restore_env(:native_advertise_host, previous)
    end)

    assert RouteMetadata.endpoint().host == "local.example"
  end

  test "remote fallback endpoint does not reuse local advertised host" do
    previous = Application.get_env(:ferricstore, :native_advertise_host)
    Application.put_env(:ferricstore, :native_advertise_host, "local.example")

    on_exit(fn ->
      restore_env(:native_advertise_host, previous)
    end)

    endpoint = RouteMetadata.endpoint_for_node(:"ferricstore@remote.example")

    assert endpoint.node == "ferricstore@remote.example"
    assert endpoint.host == "remote.example"
  end

  test "unknown leader metadata is explicit and does not advertise local endpoint as leader" do
    target = RouteMetadata.target_for_shard(999_999)

    assert target.hint == "leader_unknown"
    assert target.leader_node == nil
    refute Map.has_key?(target, :native_host)
    refute Map.has_key?(target, :endpoint)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
