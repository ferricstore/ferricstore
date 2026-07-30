defmodule Ferricstore.Commands.Stream.Groups do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{CacheKey, ID}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult}
  alias Ferricstore.TermCodec

  @groups_table Ferricstore.Stream.Groups
  @group_locks_table Ferricstore.Stream.GroupLocks
  @max_stream_id "18446744073709551615-18446744073709551615"
  @max_u64 18_446_744_073_709_551_615

  @spec lookup(map(), binary(), binary()) ::
          :missing | {:ok, binary(), map(), map()} | {:error, binary()}
  def lookup(store, key, group) do
    ensure_table()
    cache_key = CacheKey.build(store, key)

    case :ets.lookup(@groups_table, {cache_key, group}) do
      [{{^cache_key, ^group}, last_delivered, consumers, pending}] ->
        {:ok, last_delivered, consumers, pending}

      [] ->
        load_persisted(store, key, group)
    end
  end

  @spec persist(map(), binary(), binary(), binary(), map(), map()) :: :ok | {:error, term()}
  def persist(store, key, group, last_delivered, consumers, pending) do
    ensure_table()

    if Ops.has_compound?(store) do
      encoded = encode_state(last_delivered, consumers, pending)

      case Ops.compound_put(store, key, group_key(key, group), encoded, 0) do
        :ok ->
          cache(store, key, group, last_delivered, consumers, pending)
          :ok

        {:error, _reason} = error ->
          error
      end
    else
      cache(store, key, group, last_delivered, consumers, pending)
      :ok
    end
  end

  @doc false
  @spec serialized_entry(map(), binary(), binary()) ::
          :missing | {:ok, binary(), map(), map()} | {:error, term()}
  def serialized_entry(store, key, group) do
    case serialized_state(store, key, group) do
      {:v1, last_delivered, consumers, pending} ->
        {:ok, last_delivered, consumers, pending}

      {:v2, last_delivered} ->
        load_split_state(store, key, group, last_delivered)

      other ->
        other
    end
  end

  @doc false
  @spec serialized_state(map(), binary(), binary()) ::
          :missing
          | {:v1, binary(), map(), map()}
          | {:v2, binary()}
          | {:error, term()}
  def serialized_state(store, key, group) do
    case Ops.compound_get(store, key, group_key(key, group)) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil ->
        :missing

      raw ->
        case decode_state(raw) do
          {:ok, last_delivered, consumers, pending} ->
            {:v1, last_delivered, consumers, pending}

          {:ok, last_delivered} ->
            {:v2, last_delivered}

          :error ->
            ReadResult.failure(:invalid_stream_group_state)
        end
    end
  end

  @doc false
  @spec serialized_put_entry(binary(), binary(), binary(), map(), map()) ::
          {binary(), binary(), 0}
  def serialized_put_entry(key, group, last_delivered, consumers, pending) do
    {group_key(key, group), encode_state(last_delivered, consumers, pending), 0}
  end

  @doc false
  @spec serialized_v2_put_entry(binary(), binary(), binary()) :: {binary(), binary(), 0}
  def serialized_v2_put_entry(key, group, last_delivered) do
    {group_key(key, group), encode_v2_state(last_delivered), 0}
  end

  @doc false
  @spec serialized_delivery_entries(binary(), binary(), binary(), [[binary()]], non_neg_integer()) ::
          [{binary(), binary(), 0}]
  def serialized_delivery_entries(key, group, consumer, entries, delivered_at_ms) do
    pending_entries =
      Enum.map(entries, fn [id | _fields] ->
        {CompoundKey.stream_pending(key, group, id), encode_pending(consumer, delivered_at_ms), 0}
      end)

    [
      {CompoundKey.stream_consumer(key, group, consumer), encode_consumer(delivered_at_ms), 0}
      | pending_entries
    ]
  end

  @doc false
  @spec pending_keys(binary(), binary(), [binary()]) :: [binary()]
  def pending_keys(key, group, ids) do
    Enum.map(ids, &CompoundKey.stream_pending(key, group, &1))
  end

  @doc false
  @spec existing_pending_ids(map(), binary(), binary(), [binary()]) ::
          {:ok, [binary()]} | {:error, term()}
  def existing_pending_ids(store, key, group, ids) do
    unique_ids = Enum.uniq(ids)
    values = Ops.compound_batch_get(store, key, pending_keys(key, group, unique_ids))

    case ReadResult.first_failure(values) do
      nil -> validate_pending_values(unique_ids, values, [])
      failure -> failure
    end
  end

  @doc false
  @spec put_local(term(), binary(), binary(), binary(), map(), map()) :: true
  def put_local(store, key, group, last_delivered, consumers, pending) do
    ensure_table()
    cache(store, key, group, last_delivered, consumers, pending)
  end

  @doc false
  @spec pending_growth_bound(binary(), non_neg_integer()) :: non_neg_integer()
  def pending_growth_bound(consumer, count)
      when is_binary(consumer) and is_integer(count) and count >= 0 do
    empty_map_size = :erlang.external_size(%{})

    consumer_entry_size =
      :erlang.external_size(%{consumer => @max_u64}) - empty_map_size

    pending_entry_size =
      :erlang.external_size(%{@max_stream_id => {consumer, @max_u64}}) - empty_map_size

    byte_size(@max_stream_id) + consumer_entry_size + count * pending_entry_size
  end

  @doc false
  @spec initial_entry_bytes(binary(), binary(), binary()) :: pos_integer()
  def initial_entry_bytes(stream_key, group, last_delivered)
      when is_binary(stream_key) and is_binary(group) and group != "" and
             is_binary(last_delivered) and last_delivered != "" do
    key = group_key(stream_key, group)
    value = encode_state(last_delivered, %{}, %{})
    byte_size(key) + byte_size(value)
  end

  @spec count(binary(), map()) :: non_neg_integer() | ReadResult.failure()
  def count(key, store) do
    if Ops.has_compound?(store) do
      Ops.compound_count(store, key, CompoundKey.stream_group_prefix(key))
    else
      ensure_table()
      cache_key = CacheKey.build(store, key)

      :ets.foldl(
        fn
          {{^cache_key, _group}, _last, _consumers, _pending}, acc -> acc + 1
          _, acc -> acc
        end,
        0,
        @groups_table
      )
    end
  end

  @spec delete_local(binary()) :: true
  def delete_local(stream_key), do: delete_local(stream_key, nil)

  @spec delete_local(binary(), term()) :: true
  def delete_local(stream_key, store) do
    ensure_table()

    :ets.match_delete(
      @groups_table,
      {{CacheKey.build(store, stream_key), :_}, :_, :_, :_}
    )
  end

  @doc false
  @spec delete_group_local(term(), binary(), binary()) :: true
  def delete_group_local(store, stream_key, group) do
    ensure_table()
    :ets.delete(@groups_table, {CacheKey.build(store, stream_key), group})
  end

  @spec delete(map(), binary(), binary()) :: :ok | {:error, term()}
  def delete(store, key, group) do
    ensure_table()

    result =
      if Ops.has_compound?(store) do
        with :ok <- delete_split_group_entries(store, key, group),
             :ok <- Ops.compound_delete(store, key, group_key(key, group)) do
          :ok
        end
      else
        :ok
      end

    case result do
      :ok ->
        :ets.delete(@groups_table, {CacheKey.build(store, key), group})
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  @spec snapshot(non_neg_integer()) :: [map()]
  def snapshot(limit \\ 100) do
    ensure_table()
    limit = max(limit, 0)

    if limit == 0 do
      []
    else
      :ets.foldl(
        fn
          {{cache_key, group}, last_delivered, consumers, pending}, acc
          when is_binary(group) and is_map(consumers) and is_map(pending) ->
            case CacheKey.raw(cache_key) do
              key when is_binary(key) ->
                row = %{
                  key: key,
                  group: group,
                  last_delivered: last_delivered,
                  consumers: map_size(consumers),
                  pending: map_size(pending)
                }

                insert_group_snapshot(row, cache_key, acc, limit)

              nil ->
                acc
            end

          _invalid, acc ->
            acc
        end,
        :gb_sets.empty(),
        @groups_table
      )
      |> :gb_sets.to_list()
      |> Enum.map(&elem(&1, 1))
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp insert_group_snapshot(row, cache_key, acc, limit) do
    rank = {-row.pending, -row.consumers, row.key, row.group, cache_key}
    ranked = :gb_sets.add({rank, row}, acc)

    if :gb_sets.size(ranked) > limit do
      :gb_sets.del_element(:gb_sets.largest(ranked), ranked)
    else
      ranked
    end
  end

  @spec with_lock(binary(), binary(), (-> result)) :: result when result: term()
  def with_lock(key, group, fun) when is_function(fun, 0),
    do: with_lock(nil, key, group, fun)

  @spec with_lock(term(), binary(), binary(), (-> result)) :: result when result: term()
  def with_lock(store, key, group, fun) when is_function(fun, 0) do
    lock = {CacheKey.build(store, key), group}
    acquire_lock(lock)

    try do
      fun.()
    after
      release_lock(lock)
    end
  end

  defp load_persisted(store, key, group) do
    if Ops.has_compound?(store) do
      case serialized_state(store, key, group) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        :missing ->
          :missing

        {:v1, last_delivered, consumers, pending} ->
          cache(store, key, group, last_delivered, consumers, pending)
          {:ok, last_delivered, consumers, pending}

        {:v2, last_delivered} ->
          case load_split_state(store, key, group, last_delivered) do
            {:ok, ^last_delivered, consumers, pending} = loaded ->
              cache(store, key, group, last_delivered, consumers, pending)
              loaded

            {:error, {:storage_read_failed, _reason}} = failure ->
              ReadResult.command_error(failure)

            {:error, _reason} = failure ->
              ReadResult.command_error(failure)
          end
      end
    else
      :missing
    end
  end

  defp cache(store, key, group, last_delivered, consumers, pending) do
    :ets.insert(
      @groups_table,
      {{CacheKey.build(store, key), group}, last_delivered, consumers, pending}
    )
  end

  defp group_key(stream_key, group) do
    CompoundKey.stream_group(stream_key, group)
  end

  defp encode_state(last_delivered, consumers, pending) do
    TermCodec.encode({:stream_group, 1, last_delivered, consumers, pending})
  end

  defp encode_v2_state(last_delivered) do
    TermCodec.encode({:stream_group, 2, last_delivered})
  end

  defp encode_pending(consumer, delivered_at_ms) do
    TermCodec.encode({:stream_pending, 1, consumer, delivered_at_ms})
  end

  defp encode_consumer(seen_at_ms) do
    TermCodec.encode({:stream_consumer, 1, seen_at_ms})
  end

  defp decode_state(raw) when is_binary(raw) do
    case TermCodec.decode(raw) do
      {:ok, {:stream_group, 1, last_delivered, consumers, pending}}
      when is_binary(last_delivered) and is_map(consumers) and is_map(pending) ->
        if valid_state?(last_delivered, consumers, pending) do
          {:ok, last_delivered, consumers, pending}
        else
          :error
        end

      {:ok, {:stream_group, 2, last_delivered}} when is_binary(last_delivered) ->
        if valid_stream_id?(last_delivered), do: {:ok, last_delivered}, else: :error

      _other ->
        :error
    end
  end

  defp decode_state(_raw), do: :error

  defp load_split_state(store, key, group, last_delivered) do
    with {:ok, pending} <- load_split_pending(store, key, group),
         {:ok, consumers} <- load_split_consumers(store, key, group) do
      {:ok, last_delivered, consumers, pending}
    end
  end

  defp load_split_pending(store, key, group) do
    root = CompoundKey.stream_pending_prefix(key)

    group_prefix =
      split_group_member_prefix(root, CompoundKey.stream_pending_group_prefix(key, group))

    with {:ok, pairs} <- scan_split_entries(store, key, root, group_prefix) do
      Enum.reduce_while(pairs, {:ok, %{}}, fn {member, raw}, {:ok, pending} ->
        id = suffix_after_prefix(member, group_prefix)

        case {ID.parse_full_id(id), decode_pending(raw)} do
          {{:ok, _parsed}, {:ok, consumer, delivered_at_ms}} ->
            {:cont, {:ok, Map.put(pending, id, {consumer, delivered_at_ms})}}

          _invalid ->
            {:halt, ReadResult.failure(:invalid_stream_group_state)}
        end
      end)
    end
  end

  defp load_split_consumers(store, key, group) do
    root = CompoundKey.stream_consumer_prefix(key)

    group_prefix =
      split_group_member_prefix(root, CompoundKey.stream_consumer_group_prefix(key, group))

    with {:ok, pairs} <- scan_split_entries(store, key, root, group_prefix) do
      Enum.reduce_while(pairs, {:ok, %{}}, fn {member, raw}, {:ok, consumers} ->
        consumer = suffix_after_prefix(member, group_prefix)

        case decode_consumer(raw) do
          {:ok, seen_at_ms} ->
            {:cont, {:ok, Map.put(consumers, consumer, seen_at_ms)}}

          _invalid ->
            {:halt, ReadResult.failure(:invalid_stream_group_state)}
        end
      end)
    end
  end

  defp scan_split_entries(store, key, root, group_prefix) do
    case Ops.compound_fields(store, key, root) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      fields when is_list(fields) ->
        matching = Enum.filter(fields, &starts_with?(&1, group_prefix))
        values = Ops.compound_batch_get(store, key, Enum.map(matching, &(root <> &1)))

        case ReadResult.first_failure(values) do
          nil when length(values) == length(matching) -> {:ok, Enum.zip(matching, values)}
          nil -> ReadResult.failure(:invalid_stream_group_state)
          failure -> failure
        end

      _invalid ->
        ReadResult.failure(:invalid_stream_group_state)
    end
  end

  defp delete_split_group_entries(store, key, group) do
    with {:ok, pending_keys} <- split_group_keys(store, key, group, :pending),
         {:ok, consumer_keys} <- split_group_keys(store, key, group, :consumer) do
      Ops.compound_batch_delete(store, key, pending_keys ++ consumer_keys)
    end
  end

  defp split_group_keys(store, key, group, :pending) do
    split_group_keys(
      store,
      key,
      CompoundKey.stream_pending_prefix(key),
      CompoundKey.stream_pending_group_prefix(key, group)
    )
  end

  defp split_group_keys(store, key, group, :consumer) do
    split_group_keys(
      store,
      key,
      CompoundKey.stream_consumer_prefix(key),
      CompoundKey.stream_consumer_group_prefix(key, group)
    )
  end

  defp split_group_keys(store, key, root, full_group_prefix) do
    group_prefix = split_group_member_prefix(root, full_group_prefix)

    case Ops.compound_fields(store, key, root) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      fields when is_list(fields) ->
        keys =
          fields
          |> Enum.filter(&starts_with?(&1, group_prefix))
          |> Enum.map(&(root <> &1))

        {:ok, keys}

      _invalid ->
        ReadResult.failure(:invalid_stream_group_state)
    end
  end

  defp validate_pending_values([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp validate_pending_values([_id | ids], [nil | values], acc),
    do: validate_pending_values(ids, values, acc)

  defp validate_pending_values([id | ids], [raw | values], acc) do
    case decode_pending(raw) do
      {:ok, _consumer, _delivered_at_ms} ->
        validate_pending_values(ids, values, [id | acc])

      :error ->
        ReadResult.failure(:invalid_stream_group_state)
    end
  end

  defp validate_pending_values(_ids, _values, _acc),
    do: ReadResult.failure(:invalid_stream_group_state)

  defp decode_pending(raw) when is_binary(raw) do
    case TermCodec.decode(raw) do
      {:ok, {:stream_pending, 1, consumer, delivered_at_ms}}
      when is_binary(consumer) and is_integer(delivered_at_ms) and delivered_at_ms >= 0 ->
        {:ok, consumer, delivered_at_ms}

      _invalid ->
        :error
    end
  end

  defp decode_pending(_raw), do: :error

  defp decode_consumer(raw) when is_binary(raw) do
    case TermCodec.decode(raw) do
      {:ok, {:stream_consumer, 1, seen_at_ms}}
      when is_integer(seen_at_ms) and seen_at_ms >= 0 ->
        {:ok, seen_at_ms}

      _invalid ->
        :error
    end
  end

  defp decode_consumer(_raw), do: :error

  defp split_group_member_prefix(root, full_group_prefix) do
    CompoundKey.extract_subkey(full_group_prefix, root)
  end

  defp starts_with?(value, prefix) do
    value_size = byte_size(value)
    prefix_size = byte_size(prefix)
    value_size >= prefix_size and binary_part(value, 0, prefix_size) == prefix
  end

  defp suffix_after_prefix(value, prefix) do
    binary_part(value, byte_size(prefix), byte_size(value) - byte_size(prefix))
  end

  defp valid_state?(last_delivered, consumers, pending) do
    valid_stream_id?(last_delivered) and valid_consumers?(consumers) and
      valid_pending?(pending)
  end

  defp valid_consumers?(consumers) do
    Enum.all?(consumers, fn
      {consumer, seen_at_ms}
      when is_binary(consumer) and is_integer(seen_at_ms) and seen_at_ms >= 0 ->
        true

      _invalid ->
        false
    end)
  end

  defp valid_pending?(pending) do
    Enum.all?(pending, fn
      {id, {consumer, delivered_at_ms}}
      when is_binary(id) and is_binary(consumer) and is_integer(delivered_at_ms) and
             delivered_at_ms >= 0 ->
        valid_stream_id?(id)

      _invalid ->
        false
    end)
  end

  defp valid_stream_id?(id), do: match?({:ok, {_ms, _seq}}, ID.parse_full_id(id))

  defp acquire_lock(lock) do
    ensure_lock_table()

    case :ets.insert_new(@group_locks_table, {lock, self()}) do
      true ->
        :ok

      false ->
        wait_for_lock(lock)
    end
  end

  defp wait_for_lock(lock) do
    case :ets.lookup(@group_locks_table, lock) do
      [{^lock, holder}] when is_pid(holder) ->
        if Process.alive?(holder) do
          receive do
          after
            1 -> :ok
          end
        else
          :ets.select_delete(@group_locks_table, [{{lock, holder}, [], [true]}])
        end

      _other ->
        :ok
    end

    acquire_lock(lock)
  end

  defp release_lock(lock) do
    ensure_lock_table()
    :ets.select_delete(@group_locks_table, [{{lock, self()}, [], [true]}])
    :ok
  end

  defp ensure_table do
    case :ets.whereis(@groups_table) do
      :undefined ->
        try do
          :ets.new(@groups_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end

  defp ensure_lock_table do
    case :ets.whereis(@group_locks_table) do
      :undefined ->
        try do
          :ets.new(@group_locks_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end
end
