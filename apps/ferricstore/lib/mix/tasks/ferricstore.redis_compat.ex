defmodule Mix.Tasks.Ferricstore.RedisCompat do
  @moduledoc """
  Generates Redis migration compatibility reports.

  ## Usage

      mix ferricstore.redis_compat matrix [--format markdown|json] [--output PATH]
      mix ferricstore.redis_compat assess PATH [--format markdown|json] [--output PATH]

  `assess` accepts simple command-per-line traces, Redis MONITOR-style lines,
  and `INFO commandstats` lines such as `cmdstat_get:calls=10,...`.
  """

  use Mix.Task

  alias Ferricstore.Migration.RedisCompatibility

  @shortdoc "Generate Redis compatibility matrix or workload assessment"

  @impl Mix.Task
  def run(args) do
    {opts, argv, invalid} =
      OptionParser.parse(args,
        strict: [format: :string, output: :string],
        aliases: [f: :format, o: :output]
      )

    with :ok <- validate_options(invalid),
         {:ok, format} <- parse_format(Keyword.get(opts, :format, "markdown")),
         {:ok, output} <- build_output(argv, format) do
      emit(output, Keyword.get(opts, :output))
    else
      {:error, reason} ->
        Mix.shell().error(reason)
        Mix.shell().info(usage())
    end

    :ok
  end

  defp build_output(["matrix"], format) do
    {:ok, RedisCompatibility.render_matrix(format)}
  end

  defp build_output(["assess", path], format) do
    report =
      path
      |> File.stream!()
      |> RedisCompatibility.assess_lines()

    {:ok, RedisCompatibility.render_assessment(report, format)}
  rescue
    File.Error -> {:error, "could not read assessment input: #{path}"}
  end

  defp build_output(_argv, _format), do: {:error, "invalid ferricstore.redis_compat arguments"}

  defp validate_options([]), do: :ok
  defp validate_options(invalid), do: {:error, "invalid options: #{inspect(invalid)}"}

  defp parse_format("json"), do: {:ok, :json}
  defp parse_format("markdown"), do: {:ok, :markdown}
  defp parse_format(format), do: {:error, "unsupported format: #{format}"}

  defp emit(output, nil), do: Mix.shell().info(output)

  defp emit(output, path) do
    File.write!(path, output <> "\n")
    Mix.shell().info("wrote #{path}")
  end

  defp usage do
    """
    Usage:
      mix ferricstore.redis_compat matrix [--format markdown|json] [--output PATH]
      mix ferricstore.redis_compat assess PATH [--format markdown|json] [--output PATH]
    """
  end
end
