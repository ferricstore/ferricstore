defmodule Ferricstore.CI.TestPartition do
  @moduledoc false

  @minimum_fallback_weight_ms 1_000
  @fallback_weight_per_line_ms 5

  def selected_files(root, timings_path, partition_count, partition_index, excluded_files \\ []) do
    validate_partition!(partition_count, partition_index)
    excluded = MapSet.new(excluded_files, &normalize_path/1)

    files =
      root
      |> Path.join("**/*_test.exs")
      |> Path.wildcard()
      |> Enum.uniq()
      |> Enum.reject(&(normalize_path(&1) in excluded))

    files
    |> plan(load_timings!(timings_path), partition_count)
    |> Enum.at(partition_index - 1)
    |> Map.fetch!(:files)
  end

  def plan(files, timings, partition_count)
      when is_list(files) and is_map(timings) and is_integer(partition_count) and
             partition_count > 0 do
    timings = Map.new(timings, fn {path, weight} -> {normalize_path(path), weight} end)

    weighted_files =
      files
      |> Enum.uniq()
      |> Enum.map(fn path ->
        weight = Map.get(timings, normalize_path(path), fallback_weight_ms(path))

        if not (is_integer(weight) and weight > 0) do
          raise ArgumentError, "test timing weight must be a positive integer for #{path}"
        end

        {path, weight}
      end)
      |> Enum.sort_by(fn {path, weight} -> {-weight, path} end)

    bins = for index <- 1..partition_count, do: %{index: index, weight_ms: 0, files: []}

    weighted_files
    |> Enum.reduce(bins, &assign_file/2)
    |> Enum.sort_by(& &1.index)
    |> Enum.map(fn bin -> %{bin | files: Enum.reverse(bin.files)} end)
  end

  def load_timings!(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == "" or String.starts_with?(&1, "#")))
    |> Enum.reduce(%{}, fn line, timings ->
      case String.split(line, ~r/\s+/, parts: 2) do
        [weight_text, test_path] ->
          with {weight, ""} when weight > 0 <- Integer.parse(weight_text),
               false <- Map.has_key?(timings, normalize_path(test_path)) do
            Map.put(timings, normalize_path(test_path), weight)
          else
            true -> raise ArgumentError, "duplicate test timing path: #{test_path}"
            _other -> raise ArgumentError, "invalid test timing row: #{line}"
          end

        _other ->
          raise ArgumentError, "invalid test timing row: #{line}"
      end
    end)
  end

  defp assign_file({path, weight}, bins) do
    selected = Enum.min_by(bins, &{&1.weight_ms, &1.index})

    Enum.map(bins, fn bin ->
      if bin.index == selected.index do
        %{bin | weight_ms: bin.weight_ms + weight, files: [path | bin.files]}
      else
        bin
      end
    end)
  end

  defp fallback_weight_ms(path) do
    line_count = path |> File.stream!() |> Enum.count()
    max(@minimum_fallback_weight_ms, line_count * @fallback_weight_per_line_ms)
  end

  defp validate_partition!(partition_count, partition_index)
       when is_integer(partition_count) and partition_count > 0 and
              is_integer(partition_index) and partition_index >= 1 and
              partition_index <= partition_count,
       do: :ok

  defp validate_partition!(_partition_count, _partition_index) do
    raise ArgumentError, "partition index must be within the positive partition count"
  end

  defp normalize_path(path), do: Path.expand(path)
end
