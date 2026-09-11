defmodule Ferricstore.DependencySecurityGuardTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)

  test "timezone conversion has no runtime HTTP client dependency" do
    app_mix = File.read!(Path.join(@repo_root, "apps/ferricstore/mix.exs"))
    config = File.read!(Path.join(@repo_root, "config/config.exs"))

    schedule =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/flow/schedule.ex"))

    cron =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/flow/schedule/cron.ex"))

    assert app_mix =~ ~s({:tz, "~> 0.28"})
    refute app_mix =~ "{:tzdata,"
    refute app_mix =~ "{:hackney,"
    refute config =~ "config :tzdata"
    assert schedule =~ "Cron.next_run_at_ms"
    assert cron =~ "Tz.TimeZoneDatabase"
  end

  test "Elixir and native Rustler dependencies stay aligned" do
    core_mix = File.read!(Path.join(@repo_root, "apps/ferricstore/mix.exs"))
    server_mix = File.read!(Path.join(@repo_root, "apps/ferricstore_server/mix.exs"))

    for mix_source <- [core_mix, server_mix] do
      assert mix_source =~ ~s({:rustler_precompiled, "~> 0.9"})
      assert mix_source =~ ~s({:rustler, "~> 0.38", optional: true})
    end

    cargo_manifests = [
      "apps/ferricstore/native/ferricstore_bitcask/Cargo.toml",
      "apps/ferricstore/native/ferricstore_wal_nif/Cargo.toml",
      "apps/ferricstore_server/native/native_protocol_nif/Cargo.toml"
    ]

    for relative_path <- cargo_manifests do
      manifest = File.read!(Path.join(@repo_root, relative_path))
      assert manifest =~ ~s(rustler = { version = "0.38")
    end
  end
end
