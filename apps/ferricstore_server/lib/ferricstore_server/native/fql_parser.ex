defmodule FerricstoreServer.Native.FQLParser do
  @moduledoc """
  Production FQL parser backed by the bounded Rust native parser.

  The NIF returns only fixed atoms and the parsed value bytes. This wrapper
  constructs the shared canonical request used by the binder and executor.
  """

  alias Ferricstore.Flow.Query.{Field, Request}
  alias FerricstoreServer.Native.NIF

  @spec parse(binary()) :: {:ok, Request.t()} | {:error, atom()}
  def parse(query) when is_binary(query) do
    case NIF.parse_fql(query) do
      {:ok, mode, source, shape, predicates, order_by, limit, cursor}
      when mode in [:execute, :explain] and source in [:runs, :events] and
             shape in [:point, :collection, :history, :count] and
             is_list(predicates) and is_list(order_by) and
             (is_integer(limit) or is_nil(limit)) ->
        with {:ok, predicates} <- decode_predicates(predicates, []),
             {:ok, order_by} <- decode_order(order_by, []),
             {:ok, cursor} <- decode_cursor(cursor),
             {:ok, request} <-
               build_request(mode, source, shape, predicates, order_by, limit, cursor),
             :ok <- Request.validate_unbound(request) do
          {:ok, request}
        end

      {:error, reason} when is_atom(reason) ->
        {:error, reason}

      _invalid_native_result ->
        {:error, :invalid_syntax}
    end
  end

  def parse(_query), do: {:error, :invalid_syntax}

  defp decode_predicates([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_predicates([predicate | rest], acc) do
    with {:ok, predicate} <- decode_predicate(predicate) do
      decode_predicates(rest, [predicate | acc])
    end
  end

  defp decode_predicates(_predicates, _acc), do: {:error, :invalid_syntax}

  defp decode_predicate({:eq, field, value}) do
    with {:ok, field} <- Field.parse(field),
         {:ok, value} <- decode_value(value) do
      {:ok, {:eq, field, value}}
    end
  end

  defp decode_predicate({:in, field, values}) when is_list(values) do
    with {:ok, field} <- Field.parse(field),
         {:ok, values} <- decode_values(values, []) do
      {:ok, {:in, field, values}}
    end
  end

  defp decode_predicate({kind, field, lower, upper}) when kind in [:range, :time_window] do
    with {:ok, field} <- Field.parse(field),
         {:ok, lower} <- decode_value(lower),
         {:ok, upper} <- decode_value(upper) do
      {:ok, {kind, field, lower, upper}}
    end
  end

  defp decode_predicate({:is, field, kind}) when kind in [:null, :missing] do
    with {:ok, field} <- Field.parse(field), do: {:ok, {:is, field, kind}}
  end

  defp decode_predicate(_predicate), do: {:error, :invalid_syntax}

  defp decode_values([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_values([value | rest], acc) do
    with {:ok, value} <- decode_value(value), do: decode_values(rest, [value | acc])
  end

  defp decode_values(_values, _acc), do: {:error, :invalid_syntax}

  defp decode_value({kind, type, value})
       when kind in [:literal, :parameter] and type in [:keyword, :integer, :dynamic] do
    {:ok, {kind, type, value}}
  end

  defp decode_value(_value), do: {:error, :invalid_syntax}

  defp decode_order([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_order([{field, direction} | rest], acc) when direction in [:asc, :desc] do
    with {:ok, field} <- Field.parse(field) do
      decode_order(rest, [{field, direction} | acc])
    end
  end

  defp decode_order(_order_by, _acc), do: {:error, :invalid_syntax}

  defp decode_cursor(nil), do: {:ok, nil}

  defp decode_cursor({:parameter, :keyword, name})
       when is_binary(name) and name != "" and byte_size(name) <= 128,
       do: {:ok, {:parameter, :keyword, name}}

  defp decode_cursor(_cursor), do: {:error, :invalid_syntax}

  defp build_request(
         mode,
         :runs,
         :point,
         [{:eq, :run_id, run_id}],
         [],
         1,
         nil
       ) do
    {:ok, Request.point_read(mode, run_id)}
  end

  defp build_request(
         mode,
         :runs,
         :point,
         [
           {:eq, :partition_key, partition_key},
           {:eq, :run_id, run_id}
         ],
         [],
         1,
         nil
       ) do
    {:ok, Request.point_read(mode, partition_key, run_id)}
  end

  defp build_request(mode, :runs, :collection, predicates, order_by, limit, cursor) do
    {:ok, Request.collection(mode, predicates, order_by, limit, :record, cursor)}
  end

  defp build_request(mode, :runs, :count, predicates, [], nil, nil) do
    {:ok, Request.count(mode, predicates)}
  end

  defp build_request(
         mode,
         :events,
         :history,
         predicates,
         [{:event_id, direction}],
         limit,
         cursor
       ) do
    {:ok, Request.history(mode, predicates, direction, limit, cursor)}
  end

  defp build_request(_mode, _source, _shape, _predicates, _order_by, _limit, _cursor),
    do: {:error, :unsupported_query_shape}
end
