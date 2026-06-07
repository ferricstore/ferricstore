defmodule Ferricstore.Commands.Stream.Args do
  @moduledoc false

  @spec parse_xadd_args([binary()]) ::
          {:ok, binary(), :auto | {:explicit, integer(), integer()} | {:partial, integer()}, [
             binary()
           ], term(), boolean()}
          | {:error, binary()}
  def parse_xadd_args(args) do
    {key, rest} = {hd(args), tl(args)}

    {nomkstream, rest} =
      case rest do
        ["NOMKSTREAM" | r] -> {true, r}
        _ -> {false, rest}
      end

    {trim_opts, rest} =
      case rest do
        ["MAXLEN" | r] -> parse_trim_maxlen(r)
        ["MINID" | r] -> parse_trim_minid(r)
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
      ["MAXLEN" | r] ->
        case parse_trim_maxlen(r) do
          {{:error, _} = err, _} -> err
          {opts, _remaining} -> {:ok, opts}
        end

      ["MINID" | r] ->
        case parse_trim_minid(r) do
          {{:error, _} = err, _} -> err
          {opts, _remaining} -> {:ok, opts}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  @spec parse_count_opt([binary()]) :: {:ok, non_neg_integer() | :infinity} | {:error, binary()}
  def parse_count_opt([]), do: {:ok, :infinity}

  def parse_count_opt(["COUNT", n_str | _rest]) do
    case Integer.parse(n_str) do
      {n, ""} when n >= 0 -> {:ok, n}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def parse_count_opt(_), do: {:error, "ERR syntax error"}

  @spec parse_xread_args([binary()]) ::
          {:ok, non_neg_integer() | :infinity, {:block, non_neg_integer()} | :no_block, [
             {binary(), binary()}
           ]}
          | {:error, binary()}
  def parse_xread_args(args) do
    # COUNT and BLOCK can appear in either order before STREAMS.
    {count, rest} = parse_xread_count(args)
    {block, rest} = parse_xread_block(rest)

    # Handle BLOCK before COUNT: XREAD BLOCK 100 COUNT 2 STREAMS ...
    {count, rest} =
      if count == :infinity do
        case parse_xread_count(rest) do
          {:infinity, _} -> {count, rest}
          {n, rest2} -> {n, rest2}
        end
      else
        {count, rest}
      end

    case split_at_streams(rest) do
      {:ok, keys, ids} when length(keys) == length(ids) and keys != [] ->
        stream_ids = Enum.zip(keys, ids)
        {:ok, count, block, stream_ids}

      {:ok, _, _} ->
        {:error,
         "ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified"}

      :not_found ->
        {:error, "ERR syntax error"}
    end
  end

  @spec parse_xreadgroup_args([binary()]) ::
          {:ok, binary(), binary(), non_neg_integer() | :infinity,
           {:block, non_neg_integer()} | :no_block, [{binary(), binary()}]}
          | {:error, binary()}
  def parse_xreadgroup_args(args) do
    case args do
      ["GROUP", group, consumer | rest] ->
        # COUNT and BLOCK can appear in either order before STREAMS.
        {count, rest2} = parse_xread_count(rest)
        {block, rest3} = parse_xread_block(rest2)

        # Handle BLOCK before COUNT.
        {count, rest3} =
          if count == :infinity do
            case parse_xread_count(rest3) do
              {:infinity, _} -> {count, rest3}
              {n, rest4} -> {n, rest4}
            end
          else
            {count, rest3}
          end

        case split_at_streams(rest3) do
          {:ok, keys, ids} when length(keys) == length(ids) and keys != [] ->
            stream_ids = Enum.zip(keys, ids)
            {:ok, group, consumer, count, block, stream_ids}

          {:ok, _, _} ->
            {:error,
             "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID must be specified"}

          :not_found ->
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

  defp parse_xread_count(["COUNT", n_str | rest]) do
    case Integer.parse(n_str) do
      {n, ""} when n >= 0 -> {n, rest}
      _ -> {:infinity, rest}
    end
  end

  defp parse_xread_count(rest), do: {:infinity, rest}

  defp parse_xread_block(["BLOCK", timeout_str | rest]) do
    case Integer.parse(timeout_str) do
      {n, ""} when n >= 0 -> {{:block, n}, rest}
      _ -> {:no_block, ["BLOCK", timeout_str | rest]}
    end
  end

  defp parse_xread_block(rest), do: {:no_block, rest}

  defp split_at_streams(args) do
    case Enum.find_index(args, &(String.upcase(&1) == "STREAMS")) do
      nil ->
        :not_found

      idx ->
        _streams_token = Enum.at(args, idx)
        after_streams = Enum.drop(args, idx + 1)
        half = div(length(after_streams), 2)
        {keys, ids} = Enum.split(after_streams, half)
        {:ok, keys, ids}
    end
  end
end
