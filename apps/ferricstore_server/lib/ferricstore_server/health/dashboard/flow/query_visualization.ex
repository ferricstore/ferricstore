defmodule FerricstoreServer.Health.Dashboard.Flow.QueryVisualization do
  @moduledoc false

  alias Ferricstore.Flow.Query.Field

  @max_charts 4
  @max_category_values 12
  @max_named_values @max_category_values - 1
  @category_fields ~w(type state run_state)
  @time_fields ~w(updated_at_ms created_at_ms next_run_at_ms lease_deadline_ms)
  @identifier_fields ~w(run_id event_id partition_key parent_flow_id root_flow_id correlation_id)

  @spec attach(map()) :: map()
  def attach(result) when is_map(result) do
    case build(result) do
      %{charts: [_first | _rest]} = visualization ->
        Map.put(result, :visualization, visualization)

      _empty ->
        result
    end
  end

  @spec build(map()) :: map()
  def build(%{
        presentation: :workbench,
        rows: rows,
        columns: columns,
        column_selectors: selectors,
        source: source
      })
      when is_list(rows) and is_list(columns) and is_list(selectors) and
             source in [:runs, :events] do
    build_from_fields(rows, source, Enum.zip(columns, selectors))
  end

  def build(%{rows: rows, command: command})
      when is_list(rows) and command != "FLOW.HISTORY" do
    fields =
      [
        {"type", :type},
        {"state", :state},
        {"run_state", :run_state},
        {"updated_at_ms", :updated_at_ms},
        {"created_at_ms", :created_at_ms},
        {"next_run_at_ms", :next_run_at_ms},
        {"lease_deadline_ms", :lease_deadline_ms}
      ]
      |> Enum.filter(fn {_name, selector} ->
        Enum.any?(rows, &(not is_nil(selected_value(&1, :runs, selector))))
      end)

    build_from_fields(rows, :runs, fields)
  end

  def build(_result), do: %{scope: :current_page, row_count: 0, charts: []}

  defp build_from_fields(rows, source, fields) do
    category_charts =
      fields
      |> Enum.filter(fn {name, selector} -> category_field?(name, selector) end)
      |> Enum.flat_map(&category_chart(rows, source, &1))

    time_charts =
      fields
      |> Enum.filter(fn {name, _selector} -> name in @time_fields end)
      |> Enum.take(1)
      |> Enum.flat_map(&time_chart(rows, source, &1))

    category_limit = @max_charts - min(length(time_charts), 1)

    %{
      scope: :current_page,
      row_count: length(rows),
      charts: Enum.take(category_charts, category_limit) ++ time_charts
    }
  end

  defp category_field?(name, selector) do
    name in @category_fields or
      (name not in @identifier_fields and name not in @time_fields and dynamic_selector?(selector))
  end

  defp dynamic_selector?({:attribute, _name}), do: true
  defp dynamic_selector?({:state_meta, _state, _name}), do: true
  defp dynamic_selector?({:event_field, _name}), do: true
  defp dynamic_selector?(_selector), do: false

  defp category_chart(rows, source, {field, selector}) do
    counts =
      Enum.reduce(rows, %{}, fn row, acc ->
        case selected_value(row, source, selector) do
          value when is_binary(value) and value != "" ->
            Map.update(acc, value, 1, &(&1 + 1))

          value when is_integer(value) or is_float(value) or is_boolean(value) ->
            Map.update(acc, to_string(value), 1, &(&1 + 1))

          _missing_or_complex ->
            acc
        end
      end)

    case bounded_category_values(counts) do
      [] -> []
      values -> [%{kind: :category, field: field, values: values}]
    end
  end

  defp bounded_category_values(counts) do
    sorted = Enum.sort_by(counts, fn {label, count} -> {-count, label} end)

    if length(sorted) <= @max_category_values do
      Enum.map(sorted, fn {label, count} -> %{label: label, count: count} end)
    else
      {visible, remaining} = Enum.split(sorted, @max_named_values)
      other_count = Enum.reduce(remaining, 0, fn {_label, count}, total -> total + count end)

      Enum.map(visible, fn {label, count} -> %{label: label, count: count} end) ++
        [%{label: "Other", count: other_count}]
    end
  end

  defp time_chart(rows, source, {field, selector}) do
    values =
      rows
      |> Enum.flat_map(fn row ->
        case selected_value(row, source, selector) do
          value when is_integer(value) and value >= 0 -> [value]
          _invalid -> []
        end
      end)

    case time_buckets(values) do
      [] -> []
      buckets -> [%{kind: :time, field: field, values: buckets}]
    end
  end

  defp time_buckets([]), do: []

  defp time_buckets(values) do
    minimum = Enum.min(values)
    maximum = Enum.max(values)

    if minimum == maximum do
      [%{from_ms: minimum, to_ms: maximum, count: length(values)}]
    else
      bucket_count = min(8, max(2, trunc(:math.ceil(:math.sqrt(length(values))))))
      width = ceil_div(maximum - minimum + 1, bucket_count)

      counts =
        Enum.reduce(values, %{}, fn value, acc ->
          bucket = min(div(value - minimum, width), bucket_count - 1)
          Map.update(acc, bucket, 1, &(&1 + 1))
        end)

      0..(bucket_count - 1)
      |> Enum.map(fn bucket ->
        from_ms = minimum + bucket * width
        to_ms = min(maximum, from_ms + width - 1)
        %{from_ms: from_ms, to_ms: to_ms, count: Map.get(counts, bucket, 0)}
      end)
    end
  end

  defp ceil_div(value, divisor), do: div(value + divisor - 1, divisor)

  defp selected_value(record, :runs, selector) do
    case Field.fetch(record, selector) do
      {:ok, value} -> value
      :missing -> nil
    end
  end

  defp selected_value(record, :events, :event_id), do: fetch(record, :event_id)
  defp selected_value(record, :events, :fields), do: fetch(record, :fields)

  defp selected_value(record, :events, {:event_field, name}) do
    case fetch(record, :fields) do
      fields when is_map(fields) -> Map.get(fields, name)
      _missing -> nil
    end
  end

  defp selected_value(_record, _source, _selector), do: nil

  defp fetch(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp fetch(_map, _key), do: nil
end
