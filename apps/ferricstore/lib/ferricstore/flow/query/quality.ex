defmodule Ferricstore.Flow.Query.Quality do
  @moduledoc false

  @fields [:exactness, :freshness, :coverage, :pagination]
  @codes %{
    exactness: %{
      "authoritative" => 0,
      "projected_exact" => 1,
      "exact" => 2,
      "not_applicable" => 3
    },
    freshness: %{
      "current" => 0,
      "projection_watermark" => 1,
      "not_applicable" => 2
    },
    coverage: %{
      "complete" => 0,
      "unavailable" => 1
    },
    pagination: %{
      "none" => 0,
      "complete" => 1,
      "authenticated_seek" => 2,
      "live_seek" => 3
    }
  }

  @spec fields() :: [atom()]
  def fields, do: @fields

  @spec valid?(term()) :: boolean()
  def valid?(quality) when is_map(quality) and map_size(quality) == length(@fields) do
    Enum.all?(@fields, fn field ->
      with {:ok, value} <- fetch_exact(quality, field) do
        is_integer(get_in(@codes, [field, value]))
      else
        _missing_or_ambiguous -> false
      end
    end)
  end

  def valid?(_quality), do: false

  @spec encode(term()) :: {:ok, binary()} | :error
  def encode(quality) when is_map(quality) and map_size(quality) == length(@fields) do
    @fields
    |> Enum.reduce_while({:ok, []}, fn field, {:ok, acc} ->
      with {:ok, value} <- fetch_exact(quality, field),
           code when is_integer(code) <- get_in(@codes, [field, value]) do
        {:cont, {:ok, [code | acc]}}
      else
        _invalid -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, reversed |> Enum.reverse() |> :binary.list_to_bin()}
      :error -> :error
    end
  end

  def encode(_quality), do: :error

  defp fetch_exact(map, field) do
    string_field = Atom.to_string(field)

    case {Map.fetch(map, field), Map.fetch(map, string_field)} do
      {{:ok, _atom_value}, {:ok, _string_value}} -> :error
      {{:ok, value}, :error} -> {:ok, value}
      {:error, {:ok, value}} -> {:ok, value}
      {:error, :error} -> :missing
    end
  end
end
