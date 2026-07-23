defmodule Ferricstore.Flow.Query.ReferenceParser do
  @moduledoc """
  Pure Elixir oracle for the bounded FQL1 parser.

  Production native-protocol traffic uses the Rust parser. This implementation
  intentionally stays independent so differential tests can detect parser or
  canonicalization drift.
  """

  alias Ferricstore.Flow.Query.{Error, Field, Limits, Request}

  @max_query_bytes Limits.max_query_bytes()
  @max_tokens Limits.max_query_tokens()

  @type parse_error ::
          :invalid_syntax
          | :query_too_large
          | :unsupported_field
          | :unsupported_query_shape
          | :unsupported_source

  @spec parse(binary()) :: {:ok, Request.t()} | {:error, parse_error()}
  def parse(query) when is_binary(query) do
    if byte_size(query) > @max_query_bytes do
      {:error, :query_too_large}
    else
      with {:ok, tokens} <- tokenize(query),
           {:ok, mode, tokens} <- parse_mode(tokens) do
        parse_query(mode, tokens)
      end
    end
  end

  def parse(_query), do: {:error, :invalid_syntax}

  @doc false
  @spec parse_diagnostic(binary()) :: {:ok, Request.t()} | {:error, Error.t()}
  def parse_diagnostic(query) when is_binary(query) do
    case parse(query) do
      {:error, :query_too_large} ->
        {:error, Error.new(:query_too_large)}

      {:error, reason} ->
        case diagnostic_byte(query, reason) do
          byte when is_integer(byte) -> {:error, Error.diagnose(reason, query, byte)}
          nil -> {:error, Error.new(reason)}
        end

      result ->
        result
    end
  end

  def parse_diagnostic(_query), do: {:error, Error.new(:invalid_syntax)}

  defp parse_mode([{:word, explain}, {:word, analyze} | rest]) do
    cond do
      keyword?(explain, "EXPLAIN") and keyword?(analyze, "ANALYZE") ->
        {:ok, :analyze, rest}

      keyword?(explain, "EXPLAIN") ->
        {:ok, :explain, [{:word, analyze} | rest]}

      true ->
        {:ok, :execute, [{:word, explain}, {:word, analyze} | rest]}
    end
  end

  defp parse_mode([{:word, word} | rest]) do
    if keyword?(word, "EXPLAIN"),
      do: {:ok, :explain, rest},
      else: {:ok, :execute, [{:word, word} | rest]}
  end

  defp parse_mode(tokens), do: {:ok, :execute, tokens}

  defp parse_query(mode, [
         {:word, from},
         {:word, source},
         {:word, where_keyword}
         | rest
       ]) do
    cond do
      not keyword?(from, "FROM") ->
        {:error, :invalid_syntax}

      not keyword?(where_keyword, "WHERE") ->
        {:error, :unsupported_query_shape}

      true ->
        with {:ok, source} <- parse_source(source) do
          parse_predicates(mode, source, rest, [])
        end
    end
  end

  defp parse_query(_mode, [{:word, from}, {:word, source} | _rest]) do
    cond do
      not keyword?(from, "FROM") ->
        {:error, :invalid_syntax}

      true ->
        case parse_source(source) do
          {:ok, _source} -> {:error, :unsupported_query_shape}
          {:error, _reason} = error -> error
        end
    end
  end

  defp parse_query(_mode, _tokens), do: {:error, :invalid_syntax}

  defp parse_source(source) do
    cond do
      keyword?(source, "RUNS") -> {:ok, :runs}
      keyword?(source, "EVENTS") -> {:ok, :events}
      true -> {:error, :unsupported_source}
    end
  end

  defp parse_predicates(mode, source, tokens, acc) do
    with {:ok, predicate, rest} <- parse_predicate(tokens) do
      case rest do
        [{:word, and_keyword} | tail] ->
          if keyword?(and_keyword, "AND") do
            parse_predicates(mode, source, tail, [predicate | acc])
          else
            parse_tail(mode, source, Enum.reverse([predicate | acc]), rest)
          end

        _tail ->
          parse_tail(mode, source, Enum.reverse([predicate | acc]), rest)
      end
    end
  end

  defp parse_predicate(tokens) do
    with {:ok, field, rest} <- parse_field_tokens(tokens) do
      parse_predicate_operator(field, rest)
    end
  end

  defp parse_predicate_operator(field, [:equals, value | rest]) do
    with {:ok, value} <- parse_value(value, field), do: {:ok, {:eq, field, value}, rest}
  end

  defp parse_predicate_operator(field, [{:word, operator} | rest]),
    do: parse_named_predicate(field, operator, rest)

  defp parse_predicate_operator(_field, _tokens), do: {:error, :unsupported_query_shape}

  defp parse_field_tokens([
         {:word, prefix},
         :left_bracket,
         {:string, state},
         :right_bracket,
         :left_bracket,
         {:string, name},
         :right_bracket
         | rest
       ]) do
    field = {:state_meta, state, name}

    if keyword?(prefix, "STATE_META") and Field.valid?(field),
      do: {:ok, field, rest},
      else: {:error, :unsupported_field}
  end

  defp parse_field_tokens([
         {:word, prefix},
         :left_bracket,
         {:string, name},
         :right_bracket
         | rest
       ]) do
    field = {:attribute, name}

    if keyword?(prefix, "ATTRIBUTE") and Field.valid?(field),
      do: {:ok, field, rest},
      else: {:error, :unsupported_field}
  end

  defp parse_field_tokens([{:word, field_name} | rest]) do
    with {:ok, field} <- Field.parse(field_name), do: {:ok, field, rest}
  end

  defp parse_field_tokens(_tokens), do: {:error, :unsupported_query_shape}

  defp parse_named_predicate(field, operator, [:left_paren | rest]) do
    if keyword?(operator, "IN") do
      with {:ok, values, rest} <- parse_in_values(rest, field, []) do
        {:ok, {:in, field, values}, rest}
      end
    else
      {:error, :unsupported_query_shape}
    end
  end

  defp parse_named_predicate(field, operator, [{:word, kind} | rest]) do
    if keyword?(operator, "IS") do
      cond do
        keyword?(kind, "NULL") -> {:ok, {:is, field, :null}, rest}
        keyword?(kind, "MISSING") -> {:ok, {:is, field, :missing}, rest}
        true -> {:error, :unsupported_query_shape}
      end
    else
      {:error, :unsupported_query_shape}
    end
  end

  defp parse_named_predicate(field, operator, [lower, {:word, separator}, upper | rest]) do
    cond do
      keyword?(operator, "BETWEEN") and keyword?(separator, "AND") ->
        with {:ok, lower} <- parse_value(lower, field),
             {:ok, upper} <- parse_value(upper, field) do
          {:ok, {:range, field, lower, upper}, rest}
        end

      keyword?(operator, "FROM") and keyword?(separator, "TO") ->
        with {:ok, lower} <- parse_value(lower, field),
             {:ok, upper} <- parse_value(upper, field) do
          {:ok, {:time_window, field, lower, upper}, rest}
        end

      true ->
        {:error, :unsupported_query_shape}
    end
  end

  defp parse_named_predicate(_field, _operator, _rest),
    do: {:error, :unsupported_query_shape}

  defp parse_in_values([:right_paren | _rest], _field, []),
    do: {:error, :unsupported_query_shape}

  defp parse_in_values([value | rest], field, acc) do
    with {:ok, value} <- parse_value(value, field) do
      case rest do
        [:comma, :right_paren | _tail] ->
          {:error, :unsupported_query_shape}

        [:comma | tail] ->
          parse_in_values(tail, field, [value | acc])

        [:right_paren | tail] ->
          {:ok, Enum.reverse([value | acc]), tail}

        _invalid ->
          {:error, :unsupported_query_shape}
      end
    end
  end

  defp parse_in_values(_tokens, _field, _acc), do: {:error, :unsupported_query_shape}

  defp parse_tail(
         mode,
         source,
         predicates,
         [{:word, return_keyword}, {:word, shape} | tail] = tokens
       ) do
    if keyword?(return_keyword, "RETURN") do
      cond do
        source == :runs and keyword?(shape, "COUNT") and valid_terminator?(tail) ->
          mode
          |> Request.count(predicates)
          |> validate_request()

        source == :runs and keyword?(shape, "RECORD") and valid_terminator?(tail) ->
          with {:ok, point} <- canonical_point_predicates(predicates) do
            point
            |> point_request(mode)
            |> validate_request()
          end

        true ->
          {:error, :unsupported_query_shape}
      end
    else
      parse_collection_tail(mode, source, predicates, tokens)
    end
  end

  defp parse_tail(mode, source, predicates, tokens),
    do: parse_collection_tail(mode, source, predicates, tokens)

  defp point_request({:auto, run_id}, mode), do: Request.point_read(mode, run_id)

  defp point_request({:partitioned, partition_key, run_id}, mode),
    do: Request.point_read(mode, partition_key, run_id)

  defp parse_collection_tail(mode, source, predicates, tokens) do
    with {:ok, order_by, tokens} <- parse_order(tokens),
         {:ok, limit, tokens} <- parse_limit(tokens),
         {:ok, cursor, tokens} <- parse_cursor(tokens),
         :ok <- parse_records_return(tokens) do
      source
      |> collection_request(mode, predicates, order_by, limit, cursor)
      |> validate_request()
    end
  end

  defp collection_request(:runs, mode, predicates, order_by, limit, cursor),
    do: Request.collection(mode, predicates, order_by, limit, :record, cursor)

  defp collection_request(:events, mode, predicates, [{:event_id, direction}], limit, cursor),
    do: Request.history(mode, predicates, direction, limit, cursor)

  defp collection_request(_source, mode, predicates, order_by, limit, cursor),
    do: %Request{
      mode: mode,
      source: :events,
      predicate: {:and, predicates},
      order_by: order_by,
      limit: limit,
      cursor: cursor,
      return: :record
    }

  defp validate_request(request) do
    case Request.validate_unbound(request) do
      :ok -> {:ok, request}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_order([{:word, order}, {:word, by} | rest]) do
    if keyword?(order, "ORDER") and keyword?(by, "BY") do
      parse_order_fields(rest, [])
    else
      {:error, :unsupported_query_shape}
    end
  end

  defp parse_order(_tokens), do: {:error, :unsupported_query_shape}

  defp parse_order_fields([{:word, field_name}, {:word, direction} | rest], acc)
       when length(acc) < 2 do
    with {:ok, field} <- Field.parse(field_name),
         {:ok, direction} <- parse_direction(direction) do
      fields = [{field, direction} | acc]

      case rest do
        [:comma | tail] when length(fields) < 2 -> parse_order_fields(tail, fields)
        [:comma | _tail] -> {:error, :unsupported_query_shape}
        _tail -> {:ok, Enum.reverse(fields), rest}
      end
    end
  end

  defp parse_order_fields(_tokens, _acc), do: {:error, :unsupported_query_shape}

  defp parse_direction(direction) do
    cond do
      keyword?(direction, "ASC") -> {:ok, :asc}
      keyword?(direction, "DESC") -> {:ok, :desc}
      true -> {:error, :unsupported_query_shape}
    end
  end

  defp parse_limit([{:word, limit_keyword}, {:integer, limit} | rest])
       when is_integer(limit) and limit > 0 do
    if keyword?(limit_keyword, "LIMIT"),
      do: {:ok, limit, rest},
      else: {:error, :unsupported_query_shape}
  end

  defp parse_limit(_tokens), do: {:error, :unsupported_query_shape}

  defp parse_cursor([{:word, cursor_keyword}, {:parameter, name} | rest]) do
    if keyword?(cursor_keyword, "CURSOR"),
      do: {:ok, {:parameter, :keyword, name}, rest},
      else: {:ok, nil, [{:word, cursor_keyword}, {:parameter, name} | rest]}
  end

  defp parse_cursor([{:word, cursor_keyword} | rest]) do
    if keyword?(cursor_keyword, "CURSOR"),
      do: {:error, :unsupported_query_shape},
      else: {:ok, nil, [{:word, cursor_keyword} | rest]}
  end

  defp parse_cursor(tokens), do: {:ok, nil, tokens}

  defp parse_records_return([{:word, return_keyword}, {:word, shape} | tail]) do
    if keyword?(return_keyword, "RETURN") and keyword?(shape, "RECORDS") and
         valid_terminator?(tail),
       do: :ok,
       else: {:error, :unsupported_query_shape}
  end

  defp parse_records_return(_tokens), do: {:error, :unsupported_query_shape}

  defp canonical_point_predicates([
         {:eq, :partition_key, partition_key},
         {:eq, :run_id, run_id}
       ]),
       do: {:ok, {:partitioned, partition_key, run_id}}

  defp canonical_point_predicates([
         {:eq, :run_id, run_id},
         {:eq, :partition_key, partition_key}
       ]),
       do: {:ok, {:partitioned, partition_key, run_id}}

  defp canonical_point_predicates([{:eq, :run_id, run_id}]),
    do: {:ok, {:auto, run_id}}

  defp canonical_point_predicates(_predicates), do: {:error, :unsupported_query_shape}

  defp parse_value({:string, value}, _field), do: {:ok, {:literal, :keyword, value}}
  defp parse_value({:integer, value}, _field), do: {:ok, {:literal, :integer, value}}

  defp parse_value({:parameter, name}, field),
    do: {:ok, {:parameter, Field.value_type(field), name}}

  defp parse_value(_value, _field), do: {:error, :unsupported_query_shape}

  defp valid_terminator?([]), do: true
  defp valid_terminator?([:semicolon]), do: true
  defp valid_terminator?(_tail), do: false

  defp diagnostic_byte(query, reason) do
    case diagnostic_tokens(query) do
      {:error, byte} -> byte
      {:ok, tokens} -> diagnostic_token_byte(tokens, query, reason)
    end
  end

  defp diagnostic_token_byte(tokens, query, :unsupported_source),
    do: token_after_keyword_byte(tokens, "FROM") || query_end_byte(query)

  defp diagnostic_token_byte(tokens, query, :unsupported_field),
    do: unsupported_field_byte(tokens) || query_end_byte(query)

  defp diagnostic_token_byte(tokens, query, :query_cursor_invalid),
    do: keyword_token_byte(tokens, "CURSOR") || query_end_byte(query)

  defp diagnostic_token_byte(tokens, query, :unsupported_query_shape) do
    keyword_token_byte(tokens, "CURSOR") ||
      missing_where_byte(tokens) ||
      malformed_in_byte(tokens) ||
      extra_terminator_byte(tokens) ||
      oversized_limit_byte(tokens) ||
      unexpected_predicate_tail_byte(tokens) ||
      query_end_byte(query)
  end

  defp diagnostic_token_byte(_tokens, query, :invalid_syntax), do: query_end_byte(query)
  defp diagnostic_token_byte(_tokens, _query, _reason), do: nil

  defp token_after_keyword_byte(
         [{{:word, word}, _byte}, {_token, byte} = next | rest],
         keyword
       ) do
    if keyword?(word, keyword),
      do: byte,
      else: token_after_keyword_byte([next | rest], keyword)
  end

  defp token_after_keyword_byte([_token | rest], keyword),
    do: token_after_keyword_byte(rest, keyword)

  defp token_after_keyword_byte([], _keyword), do: nil

  defp unsupported_field_byte(tokens) do
    case tokens_after_keyword(tokens, "WHERE") do
      {:ok, predicate_tokens} -> unsupported_predicate_field_byte(predicate_tokens)
      :error -> nil
    end
  end

  defp unsupported_predicate_field_byte([]), do: nil

  defp unsupported_predicate_field_byte([{_token, field_byte} | _rest] = tokens) do
    raw_tokens = Enum.map(tokens, &elem(&1, 0))

    case parse_predicate(raw_tokens) do
      {:error, :unsupported_field} ->
        field_byte

      {:ok, _predicate, remaining_raw_tokens} ->
        consumed = length(raw_tokens) - length(remaining_raw_tokens)

        case Enum.drop(tokens, consumed) do
          [{{:word, and_keyword}, _byte} | rest] ->
            if keyword?(and_keyword, "AND"),
              do: unsupported_predicate_field_byte(rest),
              else: nil

          _tail ->
            nil
        end

      {:error, _reason} ->
        nil
    end
  end

  defp tokens_after_keyword([{{:word, word}, _byte} | rest], keyword) do
    if keyword?(word, keyword), do: {:ok, rest}, else: tokens_after_keyword(rest, keyword)
  end

  defp tokens_after_keyword([_token | rest], keyword), do: tokens_after_keyword(rest, keyword)
  defp tokens_after_keyword([], _keyword), do: :error

  defp missing_where_byte(tokens) do
    case query_tokens(tokens) do
      [
        {{:word, from}, _from_byte},
        {{:word, _source}, _source_byte},
        {{:word, where}, _where_byte} | _rest
      ] ->
        if keyword?(from, "FROM") and not keyword?(where, "WHERE"),
          do: token_byte(query_tokens(tokens), 2),
          else: nil

      _tokens ->
        nil
    end
  end

  defp malformed_in_byte([
         {{:word, in_keyword}, _in_byte},
         {:left_paren, _left_byte},
         {:right_paren, right_byte}
         | _rest
       ]) do
    if keyword?(in_keyword, "IN"), do: right_byte, else: nil
  end

  defp malformed_in_byte([{:comma, _comma_byte}, {:right_paren, right_byte} | _rest]),
    do: right_byte

  defp malformed_in_byte([_token | rest]), do: malformed_in_byte(rest)
  defp malformed_in_byte([]), do: nil

  defp extra_terminator_byte([
         {:semicolon, _terminator_byte},
         {_extra_token, extra_byte} | _rest
       ]),
       do: extra_byte

  defp extra_terminator_byte([_token | rest]), do: extra_terminator_byte(rest)
  defp extra_terminator_byte([]), do: nil

  defp oversized_limit_byte([
         {{:word, limit_keyword}, _limit_byte},
         {{:integer, limit}, value_byte}
         | _rest
       ]) do
    if keyword?(limit_keyword, "LIMIT") and limit > Limits.max_results(),
      do: value_byte,
      else: nil
  end

  defp oversized_limit_byte([_token | rest]), do: oversized_limit_byte(rest)
  defp oversized_limit_byte([]), do: nil

  defp unexpected_predicate_tail_byte(tokens) do
    case tokens_after_keyword(tokens, "WHERE") do
      {:ok, predicate_tokens} -> do_unexpected_predicate_tail_byte(predicate_tokens)
      :error -> nil
    end
  end

  defp do_unexpected_predicate_tail_byte([]), do: nil

  defp do_unexpected_predicate_tail_byte(tokens) do
    raw_tokens = Enum.map(tokens, &elem(&1, 0))

    case parse_predicate(raw_tokens) do
      {:ok, _predicate, remaining_raw_tokens} ->
        consumed = length(raw_tokens) - length(remaining_raw_tokens)

        case Enum.drop(tokens, consumed) do
          [{{:word, and_keyword}, _byte} = token | rest] ->
            if keyword?(and_keyword, "AND") do
              do_unexpected_predicate_tail_byte(rest)
            else
              unexpected_tail_start_byte([token | rest])
            end

          remaining ->
            unexpected_tail_start_byte(remaining)
        end

      {:error, _reason} ->
        nil
    end
  end

  defp unexpected_tail_start_byte([{{:word, word}, word_byte}, {_next_token, next_byte} | _rest]) do
    cond do
      keyword?(word, "RETURN") -> next_byte
      keyword?(word, "ORDER") -> nil
      true -> word_byte
    end
  end

  defp unexpected_tail_start_byte([{{:word, word}, byte} | _rest]) do
    if keyword?(word, "ORDER"), do: nil, else: byte
  end

  defp unexpected_tail_start_byte([{_token, byte} | _rest]), do: byte
  defp unexpected_tail_start_byte([]), do: nil

  defp query_tokens([{{:word, explain}, _byte}, {{:word, second}, _second_byte} | rest] = tokens) do
    cond do
      keyword?(explain, "EXPLAIN") and keyword?(second, "ANALYZE") -> rest
      keyword?(explain, "EXPLAIN") -> tl(tokens)
      true -> tokens
    end
  end

  defp query_tokens([{{:word, explain}, _byte} | rest] = tokens) do
    if keyword?(explain, "EXPLAIN"), do: rest, else: tokens
  end

  defp query_tokens(tokens), do: tokens

  defp keyword_token_byte(tokens, keyword) do
    Enum.find_value(tokens, fn
      {{:word, word}, byte} -> if keyword?(word, keyword), do: byte
      _token -> nil
    end)
  end

  defp token_byte(tokens, index) do
    case Enum.at(tokens, index) do
      {_token, byte} -> byte
      nil -> nil
    end
  end

  defp query_end_byte(query), do: byte_size(query) + 1

  defp diagnostic_tokens(query), do: diagnostic_tokens(query, 1, [])

  defp diagnostic_tokens(<<>>, _byte, tokens),
    do: {:ok, Enum.reverse(tokens)}

  defp diagnostic_tokens(<<char, rest::binary>>, byte, tokens)
       when char in [9, 10, 13, 32],
       do: diagnostic_tokens(rest, byte + 1, tokens)

  defp diagnostic_tokens(<<char, rest::binary>>, byte, tokens)
       when char in [?=, ?(, ?), ?[, ?], ?,, ?;] do
    diagnostic_tokens(rest, byte + 1, [{diagnostic_punctuation(char), byte} | tokens])
  end

  defp diagnostic_tokens(<<?', rest::binary>>, byte, tokens) do
    case diagnostic_string(rest, byte + 1) do
      {:ok, value, rest, next_byte} ->
        diagnostic_tokens(rest, next_byte, [{{:string, value}, byte} | tokens])

      :error ->
        {:error, byte}
    end
  end

  defp diagnostic_tokens(<<?@, rest::binary>>, byte, tokens) do
    case take_identifier(rest) do
      {"", _rest} ->
        {:error, byte}

      {name, remaining} ->
        consumed = byte_size(rest) - byte_size(remaining)

        diagnostic_tokens(
          remaining,
          byte + consumed + 1,
          [{{:parameter, name}, byte} | tokens]
        )
    end
  end

  defp diagnostic_tokens(<<char, rest::binary>> = query, byte, tokens)
       when char in ?0..?9 or (char == ?- and rest != <<>>) do
    {number, remaining} = take_number(query, query, 0)

    case parse_i64(number) do
      result when result in [:error, :overflow] ->
        {:error, byte}

      {:ok, value} ->
        diagnostic_tokens(
          remaining,
          byte + byte_size(number),
          [{{:integer, value}, byte} | tokens]
        )
    end
  end

  defp diagnostic_tokens(<<char, _rest::binary>> = query, byte, tokens)
       when char in ?A..?Z or char in ?a..?z or char == ?_ do
    {word, remaining} = take_identifier(query)

    diagnostic_tokens(
      remaining,
      byte + byte_size(word),
      [{{:word, word}, byte} | tokens]
    )
  end

  defp diagnostic_tokens(_query, byte, _tokens), do: {:error, byte}

  defp diagnostic_punctuation(?=), do: :equals
  defp diagnostic_punctuation(?(), do: :left_paren
  defp diagnostic_punctuation(?)), do: :right_paren
  defp diagnostic_punctuation(?[), do: :left_bracket
  defp diagnostic_punctuation(?]), do: :right_bracket
  defp diagnostic_punctuation(?,), do: :comma
  defp diagnostic_punctuation(?;), do: :semicolon

  defp diagnostic_string(query, byte), do: diagnostic_string(query, byte, [])

  defp diagnostic_string(<<?', ?', rest::binary>>, byte, bytes),
    do: diagnostic_string(rest, byte + 2, [?' | bytes])

  defp diagnostic_string(<<?', rest::binary>>, byte, bytes),
    do: {:ok, bytes |> Enum.reverse() |> :erlang.list_to_binary(), rest, byte + 1}

  defp diagnostic_string(<<char, rest::binary>>, byte, bytes),
    do: diagnostic_string(rest, byte + 1, [char | bytes])

  defp diagnostic_string(<<>>, _byte, _bytes), do: :error

  defp tokenize(query), do: tokenize(query, [], 0)

  defp tokenize(<<>>, tokens, _count), do: {:ok, Enum.reverse(tokens)}

  defp tokenize(<<byte, rest::binary>>, tokens, count) when byte in [9, 10, 13, 32],
    do: tokenize(rest, tokens, count)

  defp tokenize(_query, _tokens, count) when count >= @max_tokens,
    do: {:error, :unsupported_query_shape}

  defp tokenize(<<?=, rest::binary>>, tokens, count),
    do: tokenize(rest, [:equals | tokens], count + 1)

  defp tokenize(<<?(, rest::binary>>, tokens, count),
    do: tokenize(rest, [:left_paren | tokens], count + 1)

  defp tokenize(<<?), rest::binary>>, tokens, count),
    do: tokenize(rest, [:right_paren | tokens], count + 1)

  defp tokenize(<<?[, rest::binary>>, tokens, count),
    do: tokenize(rest, [:left_bracket | tokens], count + 1)

  defp tokenize(<<?], rest::binary>>, tokens, count),
    do: tokenize(rest, [:right_bracket | tokens], count + 1)

  defp tokenize(<<?,, rest::binary>>, tokens, count),
    do: tokenize(rest, [:comma | tokens], count + 1)

  defp tokenize(<<?;, rest::binary>>, tokens, count),
    do: tokenize(rest, [:semicolon | tokens], count + 1)

  defp tokenize(<<?', rest::binary>>, tokens, count) do
    with {:ok, value, rest} <- take_string(rest, []) do
      tokenize(rest, [{:string, value} | tokens], count + 1)
    end
  end

  defp tokenize(<<?@, rest::binary>>, tokens, count) do
    case take_identifier(rest) do
      {"", _rest} -> {:error, :invalid_syntax}
      {name, rest} -> tokenize(rest, [{:parameter, name} | tokens], count + 1)
    end
  end

  defp tokenize(<<byte, rest::binary>> = query, tokens, count)
       when byte in ?0..?9 or (byte == ?- and rest != <<>>) do
    case take_integer(query) do
      {:ok, value, rest} -> tokenize(rest, [{:integer, value} | tokens], count + 1)
      :error -> {:error, :invalid_syntax}
    end
  end

  defp tokenize(<<byte, _rest::binary>> = query, tokens, count)
       when byte in ?A..?Z or byte in ?a..?z or byte == ?_ do
    {word, rest} = take_identifier(query)
    tokenize(rest, [{:word, word} | tokens], count + 1)
  end

  defp tokenize(_query, _tokens, _count), do: {:error, :invalid_syntax}

  defp take_string(<<?', ?', rest::binary>>, acc), do: take_string(rest, [?' | acc])

  defp take_string(<<?', rest::binary>>, acc) do
    {:ok, acc |> Enum.reverse() |> :erlang.list_to_binary(), rest}
  end

  defp take_string(<<byte, rest::binary>>, acc), do: take_string(rest, [byte | acc])
  defp take_string(<<>>, _acc), do: {:error, :invalid_syntax}

  defp take_integer(binary) do
    {number, rest} = take_number(binary, binary, 0)

    case parse_i64(number) do
      {:ok, value} -> {:ok, value, rest}
      :overflow -> {:ok, :overflow, rest}
      :error -> :error
    end
  end

  defp parse_i64(<<?-, digits::binary>>) when digits != <<>>,
    do: parse_i64_digits(digits, 0, 0x8000_0000_0000_0000, -1)

  defp parse_i64(digits) when digits != <<>>,
    do: parse_i64_digits(digits, 0, 0x7FFF_FFFF_FFFF_FFFF, 1)

  defp parse_i64(_number), do: :error

  defp parse_i64_digits(<<>>, value, _limit, sign), do: {:ok, sign * value}

  defp parse_i64_digits(<<byte, rest::binary>>, value, limit, sign) when byte in ?0..?9 do
    digit = byte - ?0

    if value > div(limit - digit, 10),
      do: :overflow,
      else: parse_i64_digits(rest, value * 10 + digit, limit, sign)
  end

  defp parse_i64_digits(_digits, _value, _limit, _sign), do: :error

  defp take_number(original, <<byte, rest::binary>>, size)
       when byte in ?0..?9 or (byte == ?- and size == 0),
       do: take_number(original, rest, size + 1)

  defp take_number(original, rest, size) do
    <<number::binary-size(size), _suffix::binary>> = original
    {number, rest}
  end

  defp take_identifier(binary), do: take_identifier(binary, binary, 0)

  defp take_identifier(original, <<byte, rest::binary>>, size)
       when byte in ?A..?Z or byte in ?a..?z or byte in ?0..?9 or byte in [?_, ?., ?-] do
    take_identifier(original, rest, size + 1)
  end

  defp take_identifier(original, rest, size) do
    <<identifier::binary-size(size), _suffix::binary>> = original
    {identifier, rest}
  end

  defp keyword?(word, expected) when byte_size(word) == byte_size(expected) do
    keyword_bytes?(word, expected)
  end

  defp keyword?(_word, _expected), do: false

  defp keyword_bytes?(<<>>, <<>>), do: true

  defp keyword_bytes?(<<actual, word::binary>>, <<expected, rest::binary>>) do
    ascii_upper(actual) == expected and keyword_bytes?(word, rest)
  end

  defp ascii_upper(byte) when byte in ?a..?z, do: byte - 32
  defp ascii_upper(byte), do: byte
end
