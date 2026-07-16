defmodule Ferricstore.Commands.Stream.Args do
  @moduledoc false

  @max_timeout_ms 0xFFFFFFFF

  @spec parse_xadd_args([binary()]) ::
          {:ok, binary(), :auto | {:explicit, integer(), integer()} | {:partial, integer()},
           [
             binary()
           ], term(), boolean()}
          | {:error, binary()}
  def parse_xadd_args(args) do
    {key, rest} = {hd(args), tl(args)}

    {nomkstream, rest} =
      case rest do
        [option | r] when is_binary(option) ->
          if String.upcase(option) == "NOMKSTREAM", do: {true, r}, else: {false, rest}

        _ -> {false, rest}
      end

    {trim_opts, rest} =
      case rest do
        [option | r] when is_binary(option) ->
          case String.upcase(option) do
            "MAXLEN" -> parse_trim_maxlen(r)
            "MINID" -> parse_trim_minid(r)
            _ -> {nil, rest}
          end

        _ -> {nil, rest}
      end

    case trim_opts do
      {:error, _} = err ->
        err

      _ ->
        case rest do
          [id_spec_str | field_values] when field_values != [] ->
            if rem(length(field_values), 2) != 0 do
              {:error, "ERR wrong number of arguments for 'xadd' command"}
            else
              case parse_id_spec(id_spec_str) do
                {:error, _} = err -> err
                id_spec -> {:ok, key, id_spec, field_values, trim_opts, nomkstream}
              end
            end

          _ ->
            {:error, "ERR wrong number of arguments for 'xadd' command"}
        end
    end
  end

  @spec parse_trim_opts([binary()]) :: {:ok, term()} | {:error, binary()}
  def parse_trim_opts(rest) do
    case rest do
      [option | r] when is_binary(option) ->
        case String.upcase(option) do
          "MAXLEN" -> normalize_trim_parse_result(parse_trim_maxlen(r))
          "MINID" -> normalize_trim_parse_result(parse_trim_minid(r))
          _ -> {:error, "ERR syntax error"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  @spec parse_count_opt([binary()]) :: {:ok, non_neg_integer() | :infinity} | {:error, binary()}
  def parse_count_opt([]), do: {:ok, :infinity}

  def parse_count_opt([option, n_str]) when is_binary(option) do
    if String.upcase(option) == "COUNT" do
      case Integer.parse(n_str) do
        {n, ""} when n >= 0 -> {:ok, n}
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    else
      {:error, "ERR syntax error"}
    end
  end

  def parse_count_opt(_), do: {:error, "ERR syntax error"}

  @spec parse_xread_args([binary()]) ::
          {:ok, non_neg_integer() | :infinity, {:block, non_neg_integer()} | :no_block,
           [
             {binary(), binary()}
           ]}
          | {:error, binary()}
  def parse_xread_args(args) do
    with {:ok, count, block, operands} <- parse_xread_options(args, :infinity, :no_block),
         {:ok, stream_ids} <- parse_stream_operands(operands, "XREAD") do
      {:ok, count, block, stream_ids}
    end
  end

  @spec parse_xreadgroup_args([binary()]) ::
          {:ok, binary(), binary(), non_neg_integer() | :infinity,
           {:block, non_neg_integer()} | :no_block, [{binary(), binary()}]}
          | {:error, binary()}
  def parse_xreadgroup_args(args) do
    case args do
      [group_option, group, consumer | rest] when is_binary(group_option) ->
        if String.upcase(group_option) == "GROUP" do
          with {:ok, count, block, operands} <-
                 parse_xread_options(rest, :infinity, :no_block),
               {:ok, stream_ids} <- parse_stream_operands(operands, "XREADGROUP") do
            {:ok, group, consumer, count, block, stream_ids}
          end
        else
          {:error, "ERR syntax error"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_id_spec("*"), do: :auto

  defp parse_id_spec(id_str) do
    case String.split(id_str, "-", parts: 2) do
      [ms_str, seq_str] ->
        with {:ok, ms} <- parse_id_component(ms_str),
             {:ok, seq} <- parse_id_component(seq_str) do
          {:explicit, ms, seq}
        end

      [ms_str] ->
        with {:ok, ms} <- parse_id_component(ms_str) do
          {:partial, ms}
        end
    end
  end

  defp parse_id_component(value) do
    case Integer.parse(value) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> {:error, "ERR Invalid stream ID specified as stream command argument"}
    end
  end

  defp parse_trim_maxlen(rest) do
    {approx, rest} = consume_approx(rest)

    case rest do
      [threshold_str | remaining] ->
        case Integer.parse(threshold_str) do
          {n, ""} when n >= 0 -> {{:maxlen, approx, n}, remaining}
          _ -> {{:error, "ERR value is not an integer or out of range"}, rest}
        end

      [] ->
        {{:error, "ERR syntax error"}, rest}
    end
  end

  defp parse_trim_minid(rest) do
    {approx, rest} = consume_approx(rest)

    case rest do
      [id_str | remaining] -> {{:minid, approx, id_str}, remaining}
      [] -> {{:error, "ERR syntax error"}, rest}
    end
  end

  defp consume_approx(["~" | rest]), do: {true, rest}
  defp consume_approx(["=" | rest]), do: {false, rest}
  defp consume_approx(rest), do: {false, rest}

  defp parse_xread_options([], _count, _block), do: {:error, "ERR syntax error"}

  defp parse_xread_options([token | rest], count, block) when is_binary(token) do
    case String.upcase(token) do
      "STREAMS" ->
        {:ok, count, block, rest}

      "COUNT" when count == :infinity ->
        parse_xread_integer_option(rest, fn value, remaining ->
          parse_xread_options(remaining, value, block)
        end)

      "BLOCK" when block == :no_block ->
        parse_xread_integer_option(
          rest,
          fn value, remaining ->
            parse_xread_options(remaining, count, {:block, value})
          end,
          @max_timeout_ms
        )

      _unknown_or_duplicate ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_xread_options(_args, _count, _block), do: {:error, "ERR syntax error"}

  defp parse_xread_integer_option(args, continuation),
    do: parse_xread_integer_option(args, continuation, :infinity)

  defp parse_xread_integer_option([value | rest], continuation, max_value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 and (max_value == :infinity or parsed <= max_value) ->
        continuation.(parsed, rest)

      _invalid -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_xread_integer_option([], _continuation, _max_value),
    do: {:error, "ERR syntax error"}

  defp normalize_trim_parse_result({{:error, _} = error, _remaining}), do: error
  defp normalize_trim_parse_result({opts, []}), do: {:ok, opts}
  defp normalize_trim_parse_result({_opts, _remaining}), do: {:error, "ERR syntax error"}

  defp parse_stream_operands(operands, command) do
    operand_count = length(operands)

    if operand_count > 0 and rem(operand_count, 2) == 0 do
      {keys, ids} = Enum.split(operands, div(operand_count, 2))
      {:ok, Enum.zip(keys, ids)}
    else
      {:error,
       "ERR Unbalanced #{command} list of streams: for each stream key an ID must be specified"}
    end
  end
end
