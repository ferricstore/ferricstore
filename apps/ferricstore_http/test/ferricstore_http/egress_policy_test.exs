defmodule FerricstoreHttp.Targets.EgressPolicyTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.Targets.EgressPolicy

  test "allows local callbacks by default and rejects other hosts" do
    assert :ok =
             EgressPolicy.validate_uri(
               URI.parse("http://127.0.0.1:4000/invoke"),
               EgressPolicy.default()
             )

    assert {:error, :target_host_not_allowed} =
             EgressPolicy.validate_uri(
               URI.parse("https://blocked.example.com/invoke"),
               EgressPolicy.default()
             )
  end

  test "supports wildcard hosts, HTTPS requirements, and private network denial" do
    wildcard = %{allowed_hosts: ["*.hooks.example.com"], deny_private_networks?: false}

    assert :ok =
             EgressPolicy.validate_uri(
               URI.parse("https://a.hooks.example.com/invoke"),
               wildcard
             )

    assert {:error, :target_host_not_allowed} =
             EgressPolicy.validate_uri(URI.parse("https://hooks.example.com/invoke"), wildcard)

    assert {:error, :target_https_required} =
             EgressPolicy.validate_uri(
               URI.parse("http://example.com/invoke"),
               %{allowed_hosts: ["example.com"], require_https?: true}
             )

    assert {:error, :target_private_network_denied} =
             EgressPolicy.validate_uri(
               URI.parse("http://127.0.0.1:4000/invoke"),
               %{allowed_hosts: ["*"], private_network_allowlist: []}
             )

    assert {:error, :target_private_network_denied} =
             EgressPolicy.validate_uri(
               URI.parse("http://[::ffff:127.0.0.1]:4000/invoke"),
               %{allowed_hosts: ["*"], private_network_allowlist: []}
             )
  end

  test "rejects private IPv4 destinations embedded in IPv6 transition ranges" do
    policy = %{allowed_hosts: ["*"], private_network_allowlist: []}

    for url <- [
          "http://[64:ff9b::7f00:1]/invoke",
          "http://[64:ff9b::a00:1]/invoke",
          "http://[64:ff9b:1::a00:1]/invoke",
          "http://[::7f00:1]/invoke"
        ] do
      assert {:error, :target_private_network_denied} =
               EgressPolicy.validate_uri(URI.parse(url), policy)
    end
  end

  test "rejects site-local IPv6 and 6to4 addresses embedding private IPv4" do
    policy = %{allowed_hosts: ["*"], private_network_allowlist: []}

    for url <- [
          "http://[fec0::1]/invoke",
          "http://[feff::1]/invoke",
          "http://[2002:0a00:0001::]/invoke",
          "http://[2002:ac10:0001::]/invoke",
          "http://[2002:c0a8:0001::]/invoke"
        ] do
      assert {:error, :target_private_network_denied} =
               EgressPolicy.validate_uri(URI.parse(url), policy)
    end
  end
end
