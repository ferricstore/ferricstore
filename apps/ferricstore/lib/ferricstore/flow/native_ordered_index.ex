defmodule Ferricstore.Flow.NativeOrderedIndex do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF

  @registry_key {__MODULE__, :resource}
  @busy_retry_initial_ms 1
  @busy_retry_max_ms 8
  @native_page_size 4_096
  @max_request_items 100_000
  @max_request_bytes 64 * 1_024 * 1_024
  @request_too_large "flow index native request exceeds safety budget"

  @type resource :: reference()
  @type score_input :: binary() | integer() | float()
  @type index_name ::
          {__MODULE__, :index | :lookup, atom(), non_neg_integer()} | reference()

  @spec table_names(atom(), non_neg_integer()) :: {index_name(), index_name()}
  def table_names(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    {
      {__MODULE__, :index, instance_name, shard_index},
      {__MODULE__, :lookup, instance_name, shard_index}
    }
  end

  @spec new() :: resource()
  def new, do: NIF.flow_index_new()

  @spec register(index_name(), index_name(), resource()) :: :ok
  def register(index_table, lookup_table, resource) do
    :persistent_term.put(registry_key(index_table, lookup_table), resource)
    :ok
  end

  @spec reset(index_name(), index_name()) :: resource()
  def reset(index_table, lookup_table) do
    resource = new()
    register(index_table, lookup_table, resource)
    resource
  end

  @spec reset_shard(atom(), non_neg_integer()) :: resource()
  def reset_shard(instance_name, shard_index) do
    {index_table, lookup_table} = table_names(instance_name, shard_index)
    reset(index_table, lookup_table)
  end

  @spec reset_all(atom(), non_neg_integer()) :: :ok
  def reset_all(instance_name, shard_count) when is_integer(shard_count) and shard_count >= 0 do
    if shard_count > 0 do
      Enum.each(0..(shard_count - 1), &reset_shard(instance_name, &1))
    end

    :ok
  end

  @spec get(index_name(), index_name()) :: resource() | nil
  def get(index_table, lookup_table) do
    :persistent_term.get(registry_key(index_table, lookup_table), nil)
  end

  @spec put_member(resource(), binary(), binary(), score_input()) :: :ok
  def put_member(resource, key, member, score_input) do
    put_members(resource, key, [{member, score_input}])
  end

  @spec put_new_member(resource(), binary(), binary(), score_input()) :: :ok
  def put_new_member(resource, key, member, score_input) do
    put_new_members(resource, key, [{member, score_input}])
  end

  @spec put_members(resource(), binary(), [{binary(), score_input()}]) :: :ok
  def put_members(resource, key, member_score_pairs) do
    entries = parse_member_score_pairs(key, member_score_pairs)
    put_entries(resource, entries)
  end

  @spec put_new_members(resource(), binary(), [{binary(), score_input()}]) :: :ok
  def put_new_members(resource, key, member_score_pairs) do
    entries = parse_member_score_pairs(key, member_score_pairs)
    put_new_entries(resource, entries)
  end

  @spec put_entries(resource(), [{binary(), binary(), score_input()}]) :: :ok
  def put_entries(_resource, []), do: :ok

  def put_entries(resource, entries) do
    native_entries = parse_entries(entries)
    retry_busy(fn -> NIF.flow_index_put_entries(resource, native_entries) end)
  end

  @spec put_new_entries(resource(), [{binary(), binary(), score_input()}]) :: :ok
  def put_new_entries(_resource, []), do: :ok

  def put_new_entries(resource, entries) do
    native_entries = parse_entries(entries)
    retry_busy(fn -> NIF.flow_index_put_new_entries(resource, native_entries) end)
  end

  @spec move_entries(resource(), [{binary(), binary(), binary(), score_input()}]) :: :ok
  def move_entries(_resource, []), do: :ok

  def move_entries(resource, entries) do
    native_entries = parse_move_entries(entries)
    retry_busy(fn -> NIF.flow_index_move_entries(resource, native_entries) end)
  end

  @spec delete_member(resource(), binary(), binary()) :: :ok
  def delete_member(resource, key, member), do: delete_members(resource, key, [member])

  @spec delete_members(resource(), binary(), [binary()]) :: :ok
  def delete_members(_resource, _key, []), do: :ok

  def delete_members(resource, key, members) do
    retry_busy(fn -> NIF.flow_index_delete_members(resource, key, members) end)
  end

  @spec delete_entries(resource(), [{binary(), binary()}]) :: :ok
  def delete_entries(_resource, []), do: :ok

  def delete_entries(resource, entries) do
    retry_busy(fn -> NIF.flow_index_delete_entries(resource, entries) end)
  end

  @spec score_of(resource(), binary(), binary()) :: {:ok, float()} | :miss
  def score_of(resource, key, member),
    do: retry_busy(fn -> NIF.flow_index_score_of(resource, key, member) end)

  @spec range_slice(
          resource(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) :: [{binary(), float()}]
  def range_slice(_resource, _key, _min_bound, _max_bound, _reverse?, _offset, 0), do: []

  def range_slice(
        resource,
        key,
        min_bound,
        {:cursor_before, cursor_score, cursor_member},
        true,
        offset,
        count
      )
      when is_binary(cursor_member) and
             (count == :all or (is_integer(count) and count >= 0)) do
    case parse_score(cursor_score) do
      {:ok, parsed_cursor_score} ->
        {min_kind, min_score} = encode_min_bound(min_bound)
        {max_kind, max_score} = encode_max_bound({:inclusive, parsed_cursor_score})

        collect_range_pages(
          resource,
          key,
          min_kind,
          min_score,
          max_kind,
          max_score,
          true,
          offset,
          count,
          {parsed_cursor_score, cursor_member},
          []
        )

      :error ->
        []
    end
  end

  def range_slice(resource, key, min_bound, max_bound, reverse?, offset, count)
      when count == :all or (is_integer(count) and count >= 0) do
    {min_kind, min_score} = encode_min_bound(min_bound)
    {max_kind, max_score} = encode_max_bound(max_bound)

    collect_range_pages(
      resource,
      key,
      min_kind,
      min_score,
      max_kind,
      max_score,
      reverse?,
      offset,
      count,
      nil,
      []
    )
  end

  @spec take_due(resource(), binary(), score_input(), non_neg_integer()) :: [{binary(), float()}]
  def take_due(_resource, _key, _now_score, 0), do: []

  def take_due(resource, key, now_score, count) do
    case parse_score(now_score) do
      {:ok, score} -> NIF.flow_index_take_due(resource, key, score, count)
      :error -> []
    end
  end

  @spec rank_range(resource(), binary(), non_neg_integer(), non_neg_integer(), boolean()) :: [
          {binary(), float()}
        ]
  def rank_range(_resource, _key, start_idx, stop_idx, _reverse?) when start_idx > stop_idx,
    do: []

  def rank_range(resource, key, start_idx, stop_idx, reverse?) do
    range_slice(resource, key, :neg_inf, :inf, reverse?, start_idx, stop_idx - start_idx + 1)
  end

  @spec count_all(resource(), binary()) :: non_neg_integer()
  def count_all(resource, key) do
    resource
    |> then(fn native -> retry_busy(fn -> NIF.flow_index_count_all(native, key) end) end)
    |> max(0)
  end

  @spec count_many(resource(), [binary()]) :: [non_neg_integer()]
  def count_many(_resource, []), do: []

  def count_many(resource, keys) when is_list(keys) do
    resource
    |> then(fn native -> retry_busy(fn -> NIF.flow_index_count_many(native, keys) end) end)
    |> Enum.map(&max(&1, 0))
  end

  @spec reduce_due_count_key_pages(
          resource(),
          term(),
          ([binary()], term() -> {:cont, term()} | {:halt, term()})
        ) :: term()
  def reduce_due_count_key_pages(resource, acc, reducer) when is_function(reducer, 2) do
    reduce_count_key_pages(resource, true, nil, acc, reducer)
  end

  @spec count_keys_page(resource(), binary() | nil, pos_integer()) :: [binary()]
  def count_keys_page(resource, cursor, limit)
      when (is_nil(cursor) or is_binary(cursor)) and is_integer(limit) and limit > 0 and
             limit <= @native_page_size do
    retry_busy(fn -> NIF.flow_index_count_keys_page(resource, cursor, limit) end)
  end

  @spec due_count_keys_page(resource(), binary() | nil, pos_integer()) :: [binary()]
  def due_count_keys_page(resource, cursor, limit)
      when (is_nil(cursor) or is_binary(cursor)) and is_integer(limit) and limit > 0 and
             limit <= @native_page_size do
    retry_busy(fn -> NIF.flow_index_due_count_keys_page(resource, cursor, limit) end)
  end

  @spec earliest_due_score(resource(), [binary()], [binary()], [binary()]) ::
          float() | nil | {:error, term()}
  def earliest_due_score(resource, prefixes, needles, suffixes)
      when is_list(prefixes) and is_list(needles) and is_list(suffixes) do
    NIF.flow_index_earliest_due_score(resource, prefixes, needles, suffixes)
  end

  @spec restore_count(resource(), binary(), integer()) :: :ok
  def restore_count(resource, key, count),
    do: retry_busy(fn -> NIF.flow_index_restore_count(resource, key, count) end)

  @spec delete_count(resource(), binary()) :: :ok
  def delete_count(resource, key),
    do: retry_busy(fn -> NIF.flow_index_delete_count(resource, key) end)

  @spec apply_batch(resource(), [tuple()]) :: :ok | {:error, term()}
  def apply_batch(_resource, []), do: :ok

  def apply_batch(resource, ops) do
    {put_entries, put_new_entries, move_entries, delete_entries, claim_entries} =
      Enum.reduce(ops, {[], [], [], [], []}, fn
        {:put_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {[entries | puts], put_news, moves, deletes, claims}

        {:put_new_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {puts, [entries | put_news], moves, deletes, claims}

        {:move_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {puts, put_news, [entries | moves], deletes, claims}

        {:delete_members, key, members}, {puts, put_news, moves, deletes, claims} ->
          {puts, put_news, moves, [{key, members} | deletes], claims}

        {:apply_claim_entries, entries}, {puts, put_news, moves, deletes, claims} ->
          {puts, put_news, moves, deletes, [entries | claims]}

        invalid, _acc ->
          raise ArgumentError, "invalid native Flow index operation: #{inspect(invalid)}"
      end)

    NIF.flow_index_apply_batch(
      resource,
      parse_reversed_entry_groups(put_entries),
      parse_reversed_entry_groups(put_new_entries),
      parse_reversed_move_groups(move_entries),
      parse_reversed_delete_groups(delete_entries),
      prepend_reversed_groups(claim_entries)
    )
  end

  @doc false
  @spec batch_budget(tuple()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, binary()}
  def batch_budget({:put_entries, entries}), do: entry_batch_budget(entries, {0, 0})
  def batch_budget({:put_new_entries, entries}), do: entry_batch_budget(entries, {0, 0})
  def batch_budget({:move_entries, entries}), do: move_batch_budget(entries, {0, 0})

  def batch_budget({:delete_members, key, members}) when is_binary(key),
    do: delete_batch_budget(key, members, {0, 0})

  def batch_budget({:apply_claim_entries, entries}), do: claim_batch_budget(entries, {0, 0})
  def batch_budget(_invalid), do: {:error, "invalid native Flow index operation"}

  @doc false
  @spec validate_request_budget(non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, binary()}
  def validate_request_budget(items, bytes)
      when is_integer(items) and items >= 0 and is_integer(bytes) and bytes >= 0 do
    if items <= @max_request_items and bytes <= @max_request_bytes,
      do: :ok,
      else: {:error, @request_too_large}
  end

  @doc false
  @spec chunk_batch_ops([tuple()]) :: {:ok, [[tuple()]]} | {:error, binary()}
  def chunk_batch_ops(ops) when is_list(ops) do
    with {:ok, chunks} <- split_batch_ops(ops, []) do
      {:ok, pack_batch_chunks(chunks, [], {0, 0}, [])}
    end
  end

  def chunk_batch_ops(_invalid), do: {:error, "invalid native Flow index batch"}

  @spec claim_due_candidates(
          resource(),
          [binary()],
          score_input(),
          non_neg_integer(),
          non_neg_integer()
        ) :: [{binary(), [{binary(), float()}]}]
  def claim_due_candidates(_resource, _keys, _now_score, 0, _max_scan), do: []
  def claim_due_candidates(_resource, _keys, _now_score, _limit, 0), do: []

  def claim_due_candidates(resource, keys, now_score, limit, max_scan) when is_list(keys) do
    case parse_score(now_score) do
      {:ok, score} -> NIF.flow_index_claim_due_candidates(resource, keys, score, limit, max_scan)
      :error -> []
    end
  end

  @spec due_keys_present(resource(), [binary()], score_input()) :: [binary()]
  def due_keys_present(_resource, [], _now_score), do: []

  def due_keys_present(resource, keys, now_score) when is_list(keys) do
    case parse_score(now_score) do
      {:ok, score} -> retry_busy(fn -> NIF.flow_index_due_keys_present(resource, keys, score) end)
      :error -> []
    end
  end

  @spec apply_claim_entries(resource(), [tuple()]) :: :ok
  def apply_claim_entries(_resource, []), do: :ok

  def apply_claim_entries(resource, entries) do
    NIF.flow_index_apply_claim_entries(resource, entries)
  end

  @spec rollback_claim_entries(resource(), [tuple()]) :: :ok
  def rollback_claim_entries(_resource, []), do: :ok

  def rollback_claim_entries(resource, entries) do
    NIF.flow_index_rollback_claim_entries(resource, entries)
  end

  @spec plan_claims(
          [{binary(), float()}],
          [binary() | nil],
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary()
        ) :: {:ok, [tuple()], [binary()], non_neg_integer()} | :fallback
  def plan_claims(
        candidates,
        values,
        type,
        expected_state,
        worker,
        lease_ms,
        now_ms,
        remaining,
        from_due_key,
        to_due_key,
        from_state_key,
        to_state_key,
        inflight_key,
        worker_key,
        state_key_prefix
      ) do
    NIF.flow_record_plan_claims(
      candidates,
      values,
      type,
      expected_state,
      worker,
      lease_ms,
      now_ms,
      remaining,
      from_due_key,
      to_due_key,
      from_state_key,
      to_state_key,
      inflight_key,
      worker_key,
      state_key_prefix
    )
  end

  @spec plan_claims_with_history(
          [{binary(), float()}],
          [binary() | nil],
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary(),
          binary()
        ) :: {:ok, [tuple()], [binary()], non_neg_integer()} | :fallback
  def plan_claims_with_history(
        candidates,
        values,
        type,
        expected_state,
        worker,
        lease_ms,
        now_ms,
        remaining,
        from_due_key,
        to_due_key,
        from_state_key,
        to_state_key,
        inflight_key,
        worker_key,
        state_key_prefix,
        history_key_prefix
      ) do
    NIF.flow_record_plan_claims_with_history(
      candidates,
      values,
      type,
      expected_state,
      worker,
      lease_ms,
      now_ms,
      remaining,
      from_due_key,
      to_due_key,
      from_state_key,
      to_state_key,
      inflight_key,
      worker_key,
      state_key_prefix,
      history_key_prefix
    )
  end

  defp registry_key(index_table, lookup_table), do: {@registry_key, index_table, lookup_table}

  defp split_batch_ops([], chunks), do: {:ok, Enum.reverse(chunks)}

  defp split_batch_ops([op | ops], chunks) do
    with {:ok, op_chunks} <- split_batch_op(op) do
      split_batch_ops(ops, Enum.reverse(op_chunks, chunks))
    end
  end

  defp split_batch_op(op) do
    case batch_budget(op) do
      {:ok, budget} ->
        {:ok, [{op, budget}]}

      {:error, @request_too_large} ->
        split_oversized_batch_op(op)

      {:error, _reason} = error ->
        error
    end
  end

  defp split_oversized_batch_op({:put_entries, entries}) when is_list(entries),
    do: split_batch_entries(entries, :put_entries, nil, [], {0, 0}, [])

  defp split_oversized_batch_op({:put_new_entries, entries}) when is_list(entries),
    do: split_batch_entries(entries, :put_new_entries, nil, [], {0, 0}, [])

  defp split_oversized_batch_op({:move_entries, entries}) when is_list(entries),
    do: split_batch_entries(entries, :move_entries, nil, [], {0, 0}, [])

  defp split_oversized_batch_op({:delete_members, key, members})
       when is_binary(key) and is_list(members),
       do: split_batch_entries(members, :delete_members, key, [], {0, 0}, [])

  defp split_oversized_batch_op({:apply_claim_entries, entries}) when is_list(entries),
    do: split_batch_entries(entries, :apply_claim_entries, nil, [], {0, 0}, [])

  defp split_oversized_batch_op(_invalid),
    do: {:error, "invalid native Flow index operation"}

  defp split_batch_entries([], _tag, _context, [], _budget, chunks),
    do: {:ok, Enum.reverse(chunks)}

  defp split_batch_entries([], tag, context, entries, budget, chunks) do
    chunk = {build_batch_op(tag, context, Enum.reverse(entries)), budget}
    {:ok, Enum.reverse([chunk | chunks])}
  end

  defp split_batch_entries(
         [entry | entries],
         tag,
         context,
         current_entries,
         {items, bytes} = budget,
         chunks
       ) do
    with {:ok, {entry_items, entry_bytes}} <- batch_entry_budget(tag, context, entry) do
      next_budget = {items + entry_items, bytes + entry_bytes}

      case validate_request_budget(elem(next_budget, 0), elem(next_budget, 1)) do
        :ok ->
          split_batch_entries(
            entries,
            tag,
            context,
            [entry | current_entries],
            next_budget,
            chunks
          )

        {:error, _reason} = error when current_entries == [] ->
          error

        {:error, _reason} ->
          chunk = {build_batch_op(tag, context, Enum.reverse(current_entries)), budget}

          split_batch_entries(
            [entry | entries],
            tag,
            context,
            [],
            {0, 0},
            [chunk | chunks]
          )
      end
    end
  end

  defp batch_entry_budget(tag, context, entry) do
    tag
    |> build_batch_op(context, [entry])
    |> batch_budget()
  end

  defp build_batch_op(:put_entries, _context, entries), do: {:put_entries, entries}
  defp build_batch_op(:put_new_entries, _context, entries), do: {:put_new_entries, entries}
  defp build_batch_op(:move_entries, _context, entries), do: {:move_entries, entries}

  defp build_batch_op(:delete_members, key, members),
    do: {:delete_members, key, members}

  defp build_batch_op(:apply_claim_entries, _context, entries),
    do: {:apply_claim_entries, entries}

  defp pack_batch_chunks([], [], _budget, batches), do: Enum.reverse(batches)

  defp pack_batch_chunks([], current, _budget, batches),
    do: Enum.reverse([Enum.reverse(current) | batches])

  defp pack_batch_chunks(
         [{op, {op_items, op_bytes}} | chunks],
         current,
         {items, bytes},
         batches
       ) do
    next_budget = {items + op_items, bytes + op_bytes}

    case validate_request_budget(elem(next_budget, 0), elem(next_budget, 1)) do
      :ok ->
        pack_batch_chunks(chunks, [op | current], next_budget, batches)

      {:error, _reason} when current != [] ->
        pack_batch_chunks(
          [{op, {op_items, op_bytes}} | chunks],
          [],
          {0, 0},
          [Enum.reverse(current) | batches]
        )
    end
  end

  defp entry_batch_budget([], budget), do: {:ok, budget}

  defp entry_batch_budget([{key, member, _score} | entries], budget)
       when is_binary(key) and is_binary(member) do
    with {:ok, next_budget} <- add_batch_budget(budget, 1, byte_size(key) + byte_size(member)) do
      entry_batch_budget(entries, next_budget)
    end
  end

  defp entry_batch_budget(_invalid, _budget),
    do: {:error, "invalid native Flow index entry"}

  defp move_batch_budget([], budget), do: {:ok, budget}

  defp move_batch_budget([{from_key, to_key, member, _score} | entries], budget)
       when is_binary(from_key) and is_binary(to_key) and is_binary(member) do
    bytes = byte_size(from_key) + byte_size(to_key) + byte_size(member)

    with {:ok, next_budget} <- add_batch_budget(budget, 1, bytes) do
      move_batch_budget(entries, next_budget)
    end
  end

  defp move_batch_budget(_invalid, _budget),
    do: {:error, "invalid native Flow index move"}

  defp delete_batch_budget(_key, [], budget), do: {:ok, budget}

  defp delete_batch_budget(key, [member | members], budget) when is_binary(member) do
    with {:ok, next_budget} <-
           add_batch_budget(budget, 1, byte_size(key) + byte_size(member)) do
      delete_batch_budget(key, members, next_budget)
    end
  end

  defp delete_batch_budget(_key, _invalid, _budget),
    do: {:error, "invalid native Flow index delete"}

  defp claim_batch_budget([], budget), do: {:ok, budget}

  defp claim_batch_budget(
         [
           {id, from_due_key, _from_due_score, to_due_key, _to_due_score, from_state_key,
            _from_state_score, to_state_key, _to_state_score, inflight_key, worker_key,
            _lease_score}
           | entries
         ],
         budget
       )
       when is_binary(id) and is_binary(from_due_key) and is_binary(to_due_key) and
              is_binary(from_state_key) and is_binary(to_state_key) and is_binary(inflight_key) and
              is_binary(worker_key) do
    bytes =
      byte_size(id) * 6 + byte_size(from_due_key) + byte_size(to_due_key) +
        byte_size(from_state_key) + byte_size(to_state_key) + byte_size(inflight_key) +
        byte_size(worker_key)

    with {:ok, next_budget} <- add_batch_budget(budget, 6, bytes) do
      claim_batch_budget(entries, next_budget)
    end
  end

  defp claim_batch_budget(_invalid, _budget),
    do: {:error, "invalid native Flow index claim"}

  defp add_batch_budget({items, bytes}, item_increment, byte_increment) do
    next = {items + item_increment, bytes + byte_increment}

    case validate_request_budget(elem(next, 0), elem(next, 1)) do
      :ok -> {:ok, next}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  def __retry_busy_for_test__(fun, sleep_fun)
      when is_function(fun, 0) and is_function(sleep_fun, 1) do
    retry_busy(fun, sleep_fun, @busy_retry_initial_ms)
  end

  defp retry_busy(fun) do
    retry_busy(fun, &Process.sleep/1, @busy_retry_initial_ms)
  end

  defp retry_busy(fun, sleep_fun, delay_ms) do
    case fun.() do
      :busy ->
        sleep_fun.(delay_ms)
        retry_busy(fun, sleep_fun, min(delay_ms * 2, @busy_retry_max_ms))

      result ->
        result
    end
  end

  defp reduce_count_key_pages(resource, due_only, cursor, acc, reducer) do
    page =
      if due_only do
        due_count_keys_page(resource, cursor, @native_page_size)
      else
        count_keys_page(resource, cursor, @native_page_size)
      end

    case page do
      [] ->
        acc

      keys when is_list(keys) ->
        case reducer.(keys, acc) do
          {:cont, next_acc} ->
            reduce_count_key_pages(resource, due_only, List.last(keys), next_acc, reducer)

          {:halt, result} ->
            result

          invalid ->
            raise ArgumentError,
                  "invalid native Flow count-key page reducer result: #{inspect(invalid)}"
        end
    end
  end

  defp collect_range_pages(
         resource,
         key,
         min_kind,
         min_score,
         max_kind,
         max_score,
         reverse?,
         offset,
         remaining,
         cursor,
         pages
       ) do
    page_limit =
      if remaining == :all, do: @native_page_size, else: min(remaining, @native_page_size)

    page =
      case {reverse?, cursor} do
        {_reverse?, nil} ->
          retry_busy(fn ->
            NIF.flow_index_range_slice(
              resource,
              key,
              min_kind,
              min_score,
              max_kind,
              max_score,
              reverse?,
              offset,
              page_limit
            )
          end)

        {true, {cursor_score, cursor_member}} ->
          retry_busy(fn ->
            NIF.flow_index_range_cursor_slice(
              resource,
              key,
              min_kind,
              min_score,
              max_kind,
              max_score,
              cursor_score,
              cursor_member,
              offset,
              page_limit
            )
          end)

        {false, {cursor_score, cursor_member}} ->
          retry_busy(fn ->
            NIF.flow_index_range_after_slice(
              resource,
              key,
              min_kind,
              min_score,
              max_kind,
              max_score,
              cursor_score,
              cursor_member,
              offset,
              page_limit
            )
          end)
      end

    append_range_page_or_continue(
      page,
      page_limit,
      resource,
      key,
      min_kind,
      min_score,
      max_kind,
      max_score,
      reverse?,
      remaining,
      pages
    )
  end

  defp append_range_page_or_continue(
         page,
         page_limit,
         resource,
         key,
         min_kind,
         min_score,
         max_kind,
         max_score,
         reverse?,
         remaining,
         pages
       )
       when is_list(page) do
    page_count = length(page)
    pages = if page == [], do: pages, else: [page | pages]
    next_remaining = if remaining == :all, do: :all, else: remaining - page_count

    if page_count < page_limit or next_remaining == 0 do
      pages
      |> Enum.reverse()
      |> :lists.append()
    else
      {cursor_member, cursor_score} = List.last(page)

      collect_range_pages(
        resource,
        key,
        min_kind,
        min_score,
        max_kind,
        max_score,
        reverse?,
        0,
        next_remaining,
        {cursor_score, cursor_member},
        pages
      )
    end
  end

  defp parse_member_score_pairs(key, member_score_pairs)
       when is_binary(key) and is_list(member_score_pairs) do
    Enum.map(member_score_pairs, fn
      {member, score_input} when is_binary(member) ->
        {key, member, parse_score!(score_input)}

      invalid ->
        raise ArgumentError, "invalid native Flow index member: #{inspect(invalid)}"
    end)
  end

  defp parse_member_score_pairs(_key, member_score_pairs),
    do:
      raise(
        ArgumentError,
        "invalid native Flow index members: #{inspect(member_score_pairs)}"
      )

  defp parse_entries(entries) do
    Enum.map(entries, fn
      {key, member, score_input} when is_binary(key) and is_binary(member) ->
        {key, member, parse_score!(score_input)}

      invalid ->
        raise ArgumentError, "invalid native Flow index entry: #{inspect(invalid)}"
    end)
  end

  defp parse_move_entries(entries) when is_list(entries) do
    Enum.map(entries, fn
      {from_key, to_key, member, score_input}
      when is_binary(from_key) and is_binary(to_key) and is_binary(member) ->
        {from_key, to_key, member, parse_score!(score_input)}

      invalid ->
        raise ArgumentError, "invalid native Flow index move: #{inspect(invalid)}"
    end)
  end

  defp parse_reversed_entry_groups(groups) do
    Enum.reduce(groups, [], &prepend_parsed_entries/2)
  end

  defp prepend_parsed_entries([], acc), do: acc

  defp prepend_parsed_entries(
         [{key, member, score_input} | rest],
         acc
       )
       when is_binary(key) and is_binary(member),
       do: [{key, member, parse_score!(score_input)} | prepend_parsed_entries(rest, acc)]

  defp prepend_parsed_entries([invalid | _rest], _acc),
    do: raise(ArgumentError, "invalid native Flow index entry: #{inspect(invalid)}")

  defp parse_reversed_move_groups(groups) do
    Enum.reduce(groups, [], &prepend_parsed_move_entries/2)
  end

  defp prepend_parsed_move_entries([], acc), do: acc

  defp prepend_parsed_move_entries([{from_key, to_key, member, score_input} | rest], acc) do
    [
      {from_key, to_key, member, parse_score!(score_input)}
      | prepend_parsed_move_entries(rest, acc)
    ]
  end

  defp parse_reversed_delete_groups(groups) do
    Enum.reduce(groups, [], fn
      {key, members}, acc when is_binary(key) and is_list(members) ->
        prepend_delete_members(key, members, acc)

      invalid, _acc ->
        raise ArgumentError, "invalid native Flow index delete: #{inspect(invalid)}"
    end)
  end

  defp prepend_delete_members(_key, [], acc), do: acc

  defp prepend_delete_members(key, [member | rest], acc)
       when is_binary(key) and is_binary(member),
       do: [{key, member} | prepend_delete_members(key, rest, acc)]

  defp prepend_delete_members(_key, [invalid | _rest], _acc),
    do: raise(ArgumentError, "invalid native Flow index member: #{inspect(invalid)}")

  defp prepend_reversed_groups(groups) do
    Enum.reduce(groups, [], &prepend_group_entries/2)
  end

  defp prepend_group_entries([], acc), do: acc
  defp prepend_group_entries([entry | rest], acc), do: [entry | prepend_group_entries(rest, acc)]

  defp encode_min_bound(:neg_inf), do: {0, 0.0}
  defp encode_min_bound({:inclusive, score}), do: {1, score * 1.0}
  defp encode_min_bound({:exclusive, score}), do: {2, score * 1.0}
  defp encode_min_bound(:inf), do: {3, 0.0}
  defp encode_min_bound(:pos_inf), do: {3, 0.0}
  defp encode_min_bound(_other), do: {3, 0.0}

  defp encode_max_bound(:inf), do: {0, 0.0}
  defp encode_max_bound(:pos_inf), do: {0, 0.0}
  defp encode_max_bound({:inclusive, score}), do: {1, score * 1.0}
  defp encode_max_bound({:exclusive, score}), do: {2, score * 1.0}
  defp encode_max_bound(:neg_inf), do: {3, 0.0}
  defp encode_max_bound(_other), do: {3, 0.0}

  defp parse_score!(score) do
    case parse_score(score) do
      {:ok, parsed} -> parsed
      :error -> raise ArgumentError, "invalid native Flow index score: #{inspect(score)}"
    end
  end

  defp parse_score(score) when is_float(score), do: {:ok, score}

  defp parse_score(score) when is_integer(score) do
    {:ok, score * 1.0}
  rescue
    ArithmeticError -> :error
  end

  defp parse_score(score) when is_binary(score) do
    case Float.parse(score) do
      {score, ""} -> {:ok, score}
      _ -> :error
    end
  end

  defp parse_score(_score), do: :error
end
