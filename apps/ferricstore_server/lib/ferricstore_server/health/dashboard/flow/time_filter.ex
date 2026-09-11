defmodule FerricstoreServer.Health.Dashboard.Flow.TimeFilter do
  @moduledoc false

  def validate(opts, from, to) do
    from = valid_timestamp(from)
    to = valid_timestamp(to)

    errors =
      Enum.reduce([{:from, :from_ms, from, "From"}, {:to, :to_ms, to, "To"}], %{}, fn {field, key,
                                                                                       parsed,
                                                                                       label},
                                                                                      errors ->
        raw = Keyword.get(opts, key)
        empty? = is_nil(raw) or (is_binary(raw) and String.trim(raw) == "")

        if not empty? and is_nil(parsed),
          do: Map.put(errors, field, "Enter a valid #{label} UTC date and time"),
          else: errors
      end)

    errors =
      if map_size(errors) == 0 and is_integer(from) and is_integer(to) and from > to,
        do: Map.put(errors, :to, "From UTC must not be later than To UTC"),
        else: errors

    %{
      from_ms: from,
      to_ms: to,
      errors: errors,
      draft:
        if(map_size(errors) == 0,
          do: %{},
          else: %{
            from: raw_text(Keyword.get(opts, :from_ms)),
            to: raw_text(Keyword.get(opts, :to_ms))
          }
        )
    }
  end

  def input_value(nil), do: ""

  def input_value(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} ->
        text = DateTime.to_iso8601(datetime)

        if rem(value, 60_000) == 0,
          do: binary_part(text, 0, 16),
          else: String.trim_trailing(text, "Z")

      _ ->
        to_string(value)
    end
  end

  defp valid_timestamp(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, %{year: year}} when year > 0 -> value
      _ -> nil
    end
  end

  defp valid_timestamp(_), do: nil
  defp raw_text(value) when is_binary(value) or is_integer(value), do: to_string(value)
  defp raw_text(_), do: ""
end
