defmodule Ferricstore.DependencySecurityGuardTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)

  test "timezone conversion has no runtime HTTP client dependency" do
    app_mix = File.read!(Path.join(@repo_root, "apps/ferricstore/mix.exs"))
    config = File.read!(Path.join(@repo_root, "config/config.exs"))

    schedule =
      File.read!(Path.join(@repo_root, "apps/ferricstore/lib/ferricstore/flow/schedule.ex"))

    assert app_mix =~ ~s({:tz, "~> 0.28"})
    refute app_mix =~ "{:tzdata,"
    refute app_mix =~ "{:hackney,"
    refute config =~ "config :tzdata"
    assert schedule =~ "Tz.TimeZoneDatabase"
  end
end
