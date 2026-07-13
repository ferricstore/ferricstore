defmodule FerricStore.BslLicenseStamp do
  @version_pattern ~r/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/
  @months ~w(January February March April May June July August September October November December)

  def run(["--check-timestamp", version, timestamp | remaining]) do
    release_date =
      timestamp
      |> Integer.parse()
      |> case do
        {unix_timestamp, ""} -> unix_timestamp
        _other -> raise "invalid Unix release timestamp #{inspect(timestamp)}"
      end
      |> DateTime.from_unix!()
      |> DateTime.to_date()
      |> Date.to_iso8601()

    run(["--check", version, release_date | remaining])
  end

  def run(args) do
    {check?, positional} = extract_mode(args)
    {version, release_date, license_path} = parse_arguments(positional)
    validate_version!(version)

    source = File.read!(license_path)
    expected = stamp(source, version, release_date)
    change_date = change_date(release_date)

    if check? do
      if source == expected do
        IO.puts(
          "LICENSE is correctly stamped for FerricStore #{version}: #{display_date(change_date)}"
        )
      else
        raise "LICENSE is not stamped for FerricStore #{version} released on #{Date.to_iso8601(release_date)}"
      end
    else
      File.write!(license_path, expected)
      IO.puts("Stamped LICENSE for FerricStore #{version}: #{display_date(change_date)}")
    end
  end

  defp extract_mode(["--check" | rest]), do: {true, rest}
  defp extract_mode(rest), do: {false, rest}

  defp parse_arguments([version]) do
    {version, Date.utc_today(), "LICENSE"}
  end

  defp parse_arguments([version, release_date]) do
    {version, parse_date!(release_date), "LICENSE"}
  end

  defp parse_arguments([version, release_date, license_path]) do
    {version, parse_date!(release_date), license_path}
  end

  defp parse_arguments(_args) do
    raise "usage: elixir .github/scripts/stamp_bsl_license.exs [--check] VERSION [YYYY-MM-DD] [LICENSE_PATH]"
  end

  defp parse_date!(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      {:error, _reason} -> raise "invalid release date #{inspect(value)}; expected YYYY-MM-DD"
    end
  end

  defp validate_version!(version) do
    unless Regex.match?(@version_pattern, version) do
      raise "invalid semantic version #{inspect(version)}"
    end
  end

  defp stamp(source, version, release_date) do
    licensed_work = ~r/Licensed Work:.*?\n\nAdditional Use Grant:/s
    change_date = ~r/Change Date:.*?\n\nChange License:/s

    ensure_header!(source, licensed_work, "Licensed Work")
    ensure_header!(source, change_date, "Change Date")

    copyright_years =
      if release_date.year == 2026, do: "2026", else: "2026-#{release_date.year}"

    stamped_source =
      Regex.replace(
        licensed_work,
        source,
        "Licensed Work: FerricStore version #{version}. The Licensed Work is Copyright " <>
          "#{copyright_years} FerricStore contributors.\n\nAdditional Use Grant:",
        global: false
      )

    Regex.replace(
      change_date,
      stamped_source,
      "Change Date: #{display_date(change_date(release_date))}\n\nChange License:",
      global: false
    )
  end

  defp ensure_header!(source, pattern, name) do
    unless Regex.match?(pattern, source) do
      raise "could not find the #{name} header in LICENSE"
    end
  end

  defp change_date(release_date) do
    Date.new!(release_date.year + 4, release_date.month, release_date.day)
  end

  defp display_date(date) do
    "#{Enum.at(@months, date.month - 1)} #{date.day}, #{date.year}"
  end
end

try do
  FerricStore.BslLicenseStamp.run(System.argv())
rescue
  error ->
    IO.puts(:stderr, Exception.message(error))
    System.halt(1)
end
