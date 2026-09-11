defmodule FerricstoreServer.Health.Dashboard.Flow.DurationFields do
  @moduledoc false

  @units %{
    "milliseconds" => 1,
    "seconds" => 1000,
    "minutes" => 60_000,
    "hours" => 3600_000,
    "days" => 86_400_000
  }

  def normalize(params, fields) do
    Enum.reduce_while(fields, {:ok, params}, fn field, {:ok, normalized} ->
      case Map.fetch(params, field <> "_unit") do
        :error ->
          {:cont, {:ok, normalized}}

        {:ok, unit} ->
          case milliseconds(Map.get(params, field, ""), unit) do
            {:ok, value} ->
              {:cont, {:ok, normalized |> Map.put(field, value) |> Map.delete(field <> "_unit")}}

            {:error, reason} ->
              {:halt, {:error, {field, reason}}}
          end
      end
    end)
  end

  defp milliseconds(value, unit) when is_binary(value) do
    value = String.trim(value)

    with {:ok, multiplier} <- Map.fetch(@units, unit) do
      if value == "" do
        {:ok, ""}
      else
        # Decimal arithmetic avoids rounding a sub-millisecond duration into a valid value.
        case Regex.run(~r/^\+?(\d{1,30})(?:\.(\d{1,12}))?$/, value) do
          [_, whole] ->
            {:ok, to_string(String.to_integer(whole) * multiplier)}

          [_, whole, fraction] ->
            scale = Integer.pow(10, byte_size(fraction))

            numerator =
              (String.to_integer(whole) * scale + String.to_integer(fraction)) * multiplier

            if rem(numerator, scale) == 0,
              do: {:ok, to_string(div(numerator, scale))},
              else: {:error, "Duration must resolve to a whole number of milliseconds."}

          _ ->
            {:error, "Enter a non-negative decimal duration without exponents."}
        end
      end
    else
      :error -> {:error, "Select milliseconds, seconds, minutes, hours, or days."}
    end
  end

  defp milliseconds(_value, _unit), do: {:error, "Enter a decimal duration."}
end
