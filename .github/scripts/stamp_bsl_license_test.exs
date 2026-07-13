ExUnit.start()

defmodule StampBslLicenseTest do
  use ExUnit.Case, async: true

  @script Path.expand("stamp_bsl_license.exs", __DIR__)

  setup do
    directory = Path.join(System.tmp_dir!(), "stamp-bsl-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    license_path = Path.join(directory, "LICENSE")

    File.write!(license_path, """
    Business Source License 1.1

    Licensor: FerricStore contributors

    Licensed Work: FerricStore versions first publicly distributed under this
    License on or after July 13, 2026. The Licensed Work is Copyright 2026
    FerricStore contributors.

    Additional Use Grant: allowed

    Change Date: Four years from the date a specific version of the Licensed Work
    is first publicly distributed under this License.

    Change License: Apache License, Version 2.0
    """)

    on_exit(fn -> File.rm_rf!(directory) end)
    %{license_path: license_path}
  end

  test "stamps the released version and an explicit date four years later", %{license_path: path} do
    assert {output, 0} =
             System.cmd("elixir", [@script, "0.8.0", "2027-05-09", path], stderr_to_stdout: true)

    assert output =~ "FerricStore 0.8.0: May 9, 2031"
    license = File.read!(path)
    assert license =~ "Licensed Work: FerricStore version 0.8.0."
    assert license =~ "Change Date: May 9, 2031"
  end

  test "check mode accepts a correctly stamped license", %{license_path: path} do
    assert {_, 0} = System.cmd("elixir", [@script, "0.8.0", "2027-05-09", path])

    assert {output, 0} =
             System.cmd("elixir", [@script, "--check", "0.8.0", "2027-05-09", path],
               stderr_to_stdout: true
             )

    assert output =~ "LICENSE is correctly stamped"
  end

  test "check mode rejects a different version", %{license_path: path} do
    assert {_, 0} = System.cmd("elixir", [@script, "0.8.0", "2027-05-09", path])

    assert {output, status} =
             System.cmd("elixir", [@script, "--check", "0.8.1", "2027-05-09", path],
               stderr_to_stdout: true
             )

    assert status != 0
    assert output =~ "LICENSE is not stamped"
  end

  test "check-timestamp mode derives the UTC release date", %{license_path: path} do
    timestamp = DateTime.to_unix(~U[2027-05-09 23:30:00Z]) |> Integer.to_string()
    assert {_, 0} = System.cmd("elixir", [@script, "0.8.0", "2027-05-09", path])

    assert {output, 0} =
             System.cmd("elixir", [@script, "--check-timestamp", "0.8.0", timestamp, path],
               stderr_to_stdout: true
             )

    assert output =~ "LICENSE is correctly stamped"
  end

  test "rejects an invalid semantic version", %{license_path: path} do
    assert {output, status} =
             System.cmd("elixir", [@script, "next", "2027-05-09", path], stderr_to_stdout: true)

    assert status != 0
    assert output =~ "invalid semantic version"
  end
end
