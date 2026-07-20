defmodule Ferricstore.Flow.Query.Binder do
  @moduledoc """
  Binds typed named parameters into a canonical Flow query request.

  The binder rejects unused parameters. This catches caller mistakes and keeps
  unused sensitive values out of downstream planner and telemetry state.
  """

  alias Ferricstore.Flow.Query.{Limits, Request}

  @spec bind(Request.t(), map()) :: {:ok, Request.t()} | {:error, atom()}
  def bind(%Request{} = request, params), do: do_bind(request, params, :typed)

  @doc false
  @spec bind_text(Request.t(), map()) :: {:ok, Request.t()} | {:error, atom()}
  def bind_text(%Request{} = request, params), do: do_bind(request, params, :text)

  defp do_bind(%Request{} = request, params, transport) when is_map(params) do
    with :ok <- Request.validate_unbound(request),
         {:ok, predicates} <- predicates(request),
         {:ok, parameter_names} <- parameter_names(predicates, request.cursor),
         :ok <- validate_parameter_keys(params, parameter_names),
         {:ok, bound_predicates} <- bind_predicates(predicates, params, transport),
         {:ok, bound_cursor} <- bind_cursor(request.cursor, params, transport),
         bound = %{request | predicate: {:and, bound_predicates}, cursor: bound_cursor},
         :ok <- Request.validate_bound(bound) do
      {:ok, bound}
    end
  end

  defp do_bind(%Request{}, _params, _transport), do: {:error, :invalid_parameters}

  defp predicates(%Request{predicate: {:and, predicates}}) when is_list(predicates),
    do: {:ok, predicates}

  defp predicates(%Request{}), do: {:error, :unsupported_query_shape}

  defp parameter_names(predicates, cursor) do
    values = Enum.flat_map(predicates, &predicate_values/1) ++ cursor_values(cursor)

    values
    |> Enum.reduce_while({:ok, []}, fn
      {:parameter, _type, name}, {:ok, names}
      when is_binary(name) and name != "" and byte_size(name) <= 128 ->
        {:cont, {:ok, [name | names]}}

      {:literal, _type, _value}, {:ok, names} ->
        {:cont, {:ok, names}}

      _invalid, _acc ->
        {:halt, {:error, :invalid_parameter_type}}
    end)
    |> case do
      {:ok, names} ->
        names = names |> Enum.reverse() |> Enum.uniq()

        if length(names) <= Limits.max_parameters(),
          do: {:ok, names},
          else: {:error, :unsupported_query_shape}

      {:error, _reason} = error ->
        error
    end
  end

  defp predicate_values({:eq, _field, value}), do: [value]
  defp predicate_values({:in, _field, values}) when is_list(values), do: values
  defp predicate_values({:range, _field, lower, upper}), do: [lower, upper]
  defp predicate_values({:time_window, _field, lower, upper}), do: [lower, upper]
  defp predicate_values({:is, _field, kind}) when kind in [:null, :missing], do: []
  defp predicate_values(_invalid), do: [:invalid]

  defp cursor_values(nil), do: []

  defp cursor_values({kind, :keyword, _value} = cursor) when kind in [:literal, :parameter],
    do: [cursor]

  defp cursor_values(_invalid), do: [:invalid]

  defp validate_parameter_keys(params, expected_names)
       when map_size(params) > length(expected_names),
       do: {:error, :unexpected_parameter}

  defp validate_parameter_keys(params, expected_names) do
    provided_names = Map.keys(params)

    cond do
      not Enum.all?(provided_names, &is_binary/1) -> {:error, :invalid_parameters}
      Enum.any?(expected_names, &(&1 not in provided_names)) -> {:error, :missing_parameter}
      Enum.any?(provided_names, &(&1 not in expected_names)) -> {:error, :unexpected_parameter}
      true -> :ok
    end
  end

  defp bind_predicates(predicates, params, transport) do
    Enum.reduce_while(predicates, {:ok, []}, fn predicate, {:ok, acc} ->
      case bind_predicate(predicate, params, transport) do
        {:ok, bound} -> {:cont, {:ok, [bound | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp bind_predicate({:eq, field, value}, params, transport) do
    with {:ok, bound} <- bind_value(value, params, transport), do: {:ok, {:eq, field, bound}}
  end

  defp bind_predicate({:in, field, values}, params, transport) when is_list(values) do
    with {:ok, bound} <- bind_values(values, params, transport), do: {:ok, {:in, field, bound}}
  end

  defp bind_predicate({:range, field, lower, upper}, params, transport) do
    with {:ok, lower} <- bind_value(lower, params, transport),
         {:ok, upper} <- bind_value(upper, params, transport) do
      {:ok, {:range, field, lower, upper}}
    end
  end

  defp bind_predicate({:time_window, field, lower, upper}, params, transport) do
    with {:ok, lower} <- bind_value(lower, params, transport),
         {:ok, upper} <- bind_value(upper, params, transport) do
      {:ok, {:time_window, field, lower, upper}}
    end
  end

  defp bind_predicate({:is, field, kind}, _params, _transport) when kind in [:null, :missing],
    do: {:ok, {:is, field, kind}}

  defp bind_predicate(_predicate, _params, _transport), do: {:error, :unsupported_query_shape}

  defp bind_cursor(nil, _params, _transport), do: {:ok, nil}

  defp bind_cursor({kind, :keyword, _value} = cursor, params, transport)
       when kind in [:literal, :parameter],
       do: bind_value(cursor, params, transport)

  defp bind_cursor(_cursor, _params, _transport), do: {:error, :query_cursor_invalid}

  defp bind_values(values, params, transport) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case bind_value(value, params, transport) do
        {:ok, bound} -> {:cont, {:ok, [bound | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp bind_value({:literal, _type, _value} = literal, _params, _transport), do: {:ok, literal}

  defp bind_value({:parameter, declared_type, name}, params, transport)
       when is_binary(name) and declared_type in [:keyword, :integer, :float, :boolean, :dynamic] do
    with {:ok, value} <- fetch_parameter(params, name),
         {:ok, value} <- decode_parameter(declared_type, value, transport),
         {:ok, actual_type} <- value_type(value),
         true <- declared_type in [:dynamic, actual_type] do
      {:ok, {:literal, actual_type, value}}
    else
      false -> {:error, :invalid_parameter_type}
      {:error, _reason} = error -> error
    end
  end

  defp bind_value(_value, _params, _transport), do: {:error, :invalid_parameter_type}

  defp decode_parameter(:integer, value, :text) when is_binary(value), do: parse_i64(value)
  defp decode_parameter(_declared_type, value, _transport), do: {:ok, value}

  defp fetch_parameter(params, name) do
    case Map.fetch(params, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :missing_parameter}
    end
  end

  defp value_type(value) when is_binary(value), do: {:ok, :keyword}
  defp value_type(value) when is_integer(value), do: {:ok, :integer}
  defp value_type(value) when is_float(value), do: {:ok, :float}
  defp value_type(value) when is_boolean(value), do: {:ok, :boolean}
  defp value_type(_value), do: {:error, :invalid_parameter_type}

  defp parse_i64(<<?-, digits::binary>>) when digits != <<>>,
    do: parse_i64_digits(digits, 0, 0x8000_0000_0000_0000, -1)

  defp parse_i64(digits) when is_binary(digits) and digits != <<>>,
    do: parse_i64_digits(digits, 0, 0x7FFF_FFFF_FFFF_FFFF, 1)

  defp parse_i64(_value), do: {:error, :invalid_parameter_type}

  defp parse_i64_digits(<<>>, value, _limit, sign), do: {:ok, sign * value}

  defp parse_i64_digits(<<byte, rest::binary>>, value, limit, sign) when byte in ?0..?9 do
    digit = byte - ?0

    if value > div(limit - digit, 10),
      do: {:error, :invalid_parameter_type},
      else: parse_i64_digits(rest, value * 10 + digit, limit, sign)
  end

  defp parse_i64_digits(_digits, _value, _limit, _sign),
    do: {:error, :invalid_parameter_type}
end
