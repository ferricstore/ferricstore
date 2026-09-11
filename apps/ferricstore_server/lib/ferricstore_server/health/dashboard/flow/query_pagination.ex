defmodule FerricstoreServer.Health.Dashboard.Flow.QueryPagination do
  @moduledoc false

  alias Ferricstore.Flow.Query.Limits

  @max_history 16
  @max_history_bytes 32_768
  @max_page 1_000_000_000

  def prepare(params, form) do
    with {:ok, history} <- decode_history(Map.get(params, "cursor_history")),
         {:ok, page} <- page_number(Map.get(params, "page_number"), form.cursor),
         {:ok, action} <- page_action(Map.get(params, "page_action")) do
      {:ok, Map.merge(form, %{cursor_history: history, page_number: page, page_action: action})}
    end
  end

  def attach(%{status: :ok, page: page} = result, form) when is_map(page) do
    current = Map.get(form, :cursor)
    history = Map.get(form, :cursor_history, [])
    number = Map.get(form, :page_number, if(is_nil(current), do: 1))
    first = if current, do: target(form, nil, [], 1)

    previous =
      case List.pop_at(history, -1) do
        {cursor, rest} when history != [] ->
          target(form, cursor, rest, if(is_integer(number), do: max(number - 1, 1)))

        _ ->
          nil
      end

    next =
      case page do
        %{has_more: true, cursor: cursor} when is_binary(cursor) and cursor != "" ->
          target(
            form,
            cursor,
            bounded_history(history ++ [current]),
            if(is_integer(number), do: min(number + 1, @max_page))
          )

        _ ->
          nil
      end

    result =
      Map.put(result, :navigation, %{
        page_number: number,
        first: first,
        previous: previous,
        next: next
      })

    if next, do: Map.put(result, :continuation, next), else: Map.delete(result, :continuation)
  end

  def attach(result, _form), do: result

  defp target(form, cursor, history, number) do
    form
    |> Map.take([:mode, :fql, :params_json, :guided_query])
    |> Map.merge(%{
      action: :run,
      cursor: cursor,
      cursor_history: history,
      page_number: number,
      page_action: if(is_nil(cursor), do: :first, else: :page)
    })
  end

  defp decode_history(nil), do: {:ok, []}
  defp decode_history(""), do: {:ok, []}

  defp decode_history(json) when is_binary(json) and byte_size(json) <= @max_history_bytes do
    case Jason.decode(json) do
      {:ok, list} when is_list(list) and length(list) <= @max_history ->
        if Enum.all?(list, &valid_cursor?/1), do: {:ok, list}, else: invalid_history()

      _ ->
        invalid_history()
    end
  end

  defp decode_history(_), do: invalid_history()

  defp invalid_history,
    do: {:error, "ERR query page history must contain at most 16 cursors within 32 KiB"}

  defp valid_cursor?(nil), do: true

  defp valid_cursor?(value) when is_binary(value),
    do:
      byte_size(value) >= Limits.min_cursor_bytes() and
        byte_size(value) <= Limits.max_cursor_bytes()

  defp valid_cursor?(_), do: false

  defp bounded_history(history) do
    history = Enum.take(history, -@max_history)

    if byte_size(Jason.encode!(history)) <= @max_history_bytes,
      do: history,
      else: bounded_history(tl(history))
  end

  defp page_number(value, cursor) when value in [nil, ""],
    do: {:ok, if(is_nil(cursor), do: 1)}

  defp page_number(value, _cursor) when is_binary(value) and byte_size(value) <= 10 do
    case Integer.parse(value) do
      {number, ""} when number >= 1 and number <= @max_page -> {:ok, number}
      _ -> {:error, "ERR query page number is invalid"}
    end
  end

  defp page_number(_, _), do: {:error, "ERR query page number is invalid"}
  defp page_action(value) when value in [nil, "", "page"], do: {:ok, :page}
  defp page_action("first"), do: {:ok, :first}
  defp page_action(_), do: {:error, "ERR query page action is invalid"}
end
