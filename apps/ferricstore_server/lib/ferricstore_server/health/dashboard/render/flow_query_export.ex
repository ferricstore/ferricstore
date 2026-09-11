defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryExport do
  @moduledoc false
  alias FerricstoreServer.Health.Dashboard.Flow.QueryProjection

  @max_bytes 1_048_576

  def render(%{status: :ok} = result) do
    case encode(result) do
      {:ok, json} ->
        """
        <div class="flow-policy-actions">
          <button class="flow-search-button secondary" type="button" data-dashboard-download-json="flow-query-export">Download current page (JSON)</button>
          <span data-dashboard-download-status role="status"></span>
        </div>
        <script type="application/json" id="flow-query-export">#{json}</script>
        """

      {:error, :too_large} ->
        ~s(<p class="flow-section-note">Current-page export exceeds the 1 MiB download limit. Reduce the query limit or projection.</p>)

      {:error, _} ->
        ""
    end
  end

  def render(_), do: ""

  def encode(result, opts \\ []) do
    max_bytes = Keyword.get(opts, :max_bytes, @max_bytes)

    with {:ok, columns, rows} <- projected_rows(result),
         {:ok, encoded_columns} <- encode_safe(columns, max_bytes),
         prefix = ~s({"scope":"current_page","columns":) <> encoded_columns <> ~s(,"rows":[),
         {:ok, encoded_rows} <- encode_rows(rows, max_bytes - byte_size(prefix) - 2) do
      json =
        IO.iodata_to_binary([
          prefix,
          Enum.intersperse(encoded_rows, ","),
          "]}"
        ])

      if byte_size(json) <= max_bytes, do: {:ok, json}, else: {:error, :too_large}
    end
  end

  defp projected_rows(%{
         status: :ok,
         columns: columns,
         column_selectors: selectors,
         source: source,
         rows: rows
       })
       when is_list(columns) and is_list(selectors) and is_list(rows) and
              length(columns) == length(selectors) do
    {:ok, columns,
     Stream.map(rows, fn row ->
       Enum.map(selectors, &QueryProjection.value(row, source, &1))
     end)}
  end

  defp projected_rows(%{status: :ok, scalar: %{kind: kind, value: value}}),
    do: {:ok, [to_string(kind)], [[value]]}

  defp projected_rows(%{status: :ok, command: "FLOW.QUERY", rows: rows} = result)
       when is_list(rows),
       do:
         result
         |> Map.merge(%{
           columns: ["run_id", "type", "state", "updated_at_ms"],
           column_selectors: [:run_id, :type, :state, :updated_at_ms],
           source: :runs
         })
         |> projected_rows()

  defp projected_rows(_), do: {:error, :unsupported_result}

  # Stop before encoding an oversized value; export never reads another page or payload.
  defp encode_rows(rows, max_bytes) do
    Enum.reduce_while(rows, {:ok, [], 0}, fn row, {:ok, encoded, bytes} ->
      case encode_safe(row, max_bytes - bytes) do
        {:ok, json} -> {:cont, {:ok, [json | encoded], bytes + byte_size(json) + 1}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, rows, _} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  @doc false
  def encode_value(value, max_bytes) when is_integer(max_bytes) and max_bytes > 0,
    do: encode_safe(value, max_bytes)

  defp encode_safe(value, remaining) do
    if :erlang.external_size(value) > remaining do
      {:error, :too_large}
    else
      case Jason.encode_to_iodata(json_value(value)) do
        {:ok, encoded} ->
          if IO.iodata_length(encoded) > remaining do
            {:error, :too_large}
          else
            json = IO.iodata_to_binary(encoded)

            escaped_size =
              for <<byte <- json>>, reduce: byte_size(json) do
                bytes -> if byte in [?<, ?&], do: bytes + 5, else: bytes
              end

            if escaped_size > remaining do
              {:error, :too_large}
            else
              {:ok, json |> String.replace("<", "\\u003c") |> String.replace("&", "\\u0026")}
            end
          end

        {:error, _} ->
          {:error, :unsupported_result}
      end
    end
  end

  defp json_value(value) when is_binary(value),
    do:
      if(String.valid?(value), do: value, else: %{encoding: "base64", data: Base.encode64(value)})

  defp json_value(values) when is_list(values), do: Enum.map(values, &json_value/1)

  defp json_value(values) when is_map(values),
    do: Map.new(values, fn {key, value} -> {key, json_value(value)} end)

  defp json_value(value), do: value
end
