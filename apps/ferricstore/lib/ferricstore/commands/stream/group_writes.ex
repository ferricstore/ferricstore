defmodule Ferricstore.Commands.Stream.GroupWrites do
  @moduledoc false

  # XCLAIM, XAUTOCLAIM and XGROUP DESTROY, CREATECONSUMER, DELCONSUMER and
  # SETID as replicated stream operations (`{:stream_mutate, key, op}`), so
  # every replica decides from the same group state and the entry's apply
  # time. Older releases do not know these operations; the commands run from
  # feature level 11.
  #
  # As Redis 7: a claim checks the minimum idle time first, then drops an
  # entry the stream no longer holds from the pending list rather than
  # answering it; FORCE claims an entry no consumer holds (ignoring the idle
  # time) when the stream has it; JUSTID answers IDs and counts no delivery.

  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Stream.{CacheKey, Entries, Groups, ID, Meta}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @no_key "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE " <>
            "you may want to use the MKSTREAM option to create an empty stream automatically."

  # XAUTOCLAIM looks at up to this many entries per one it claims, as Redis.
  @autoclaim_attempts_factor 10

  @spec run(binary(), tuple(), map()) :: term()
  def run(key, {:claim, group, consumer, min_idle, ids, opts}, store) do
    with {:ok, state} <- claim_state(store, key, group) do
      claim(key, group, consumer, min_idle, ids, opts, state, store)
    end
  end

  def run(key, {:autoclaim, group, consumer, min_idle, start, count, justid}, store) do
    with {:ok, state} <- claim_state(store, key, group) do
      autoclaim(key, group, consumer, min_idle, start, count, justid, state, store)
    end
  end

  def run(key, {:destroy, group}, store) do
    with :ok <- stream_present(store, key) do
      case Groups.serialized_state(store, key, group) do
        :missing ->
          0

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        _present ->
          with :ok <- Groups.delete(store, key, group),
               :ok <- invalidate(key, group, store),
               do: 1
      end
    end
  end

  def run(key, {:create_consumer, group, consumer}, store) do
    with :ok <- stream_present(store, key),
         {:ok, state} <- group_state(store, key, group) do
      if Map.has_key?(state.consumers, consumer) do
        0
      else
        consumers = Map.put(state.consumers, consumer, CommandTime.now_ms())

        with :ok <-
               write(key, group, %{state | consumers: consumers}, [consumer], [], [], store),
             do: 1
      end
    end
  end

  def run(key, {:delete_consumer, group, consumer}, store) do
    with :ok <- stream_present(store, key),
         {:ok, state} <- group_state(store, key, group) do
      if Map.has_key?(state.consumers, consumer) do
        owned = for {id, entry} <- state.pending, elem(entry, 0) == consumer, do: id

        state = %{
          state
          | consumers: Map.delete(state.consumers, consumer),
            pending: Map.drop(state.pending, owned)
        }

        with :ok <- write(key, group, state, [], owned, [consumer], store), do: length(owned)
      else
        0
      end
    end
  end

  def run(key, {:set_group_id, group, id_str}, store) do
    with :ok <- stream_present(store, key),
         {:ok, state} <- group_state(store, key, group),
         {:ok, last_delivered} <- group_id(id_str, key, store) do
      state = %{state | last_delivered: last_delivered, last_changed?: true}
      write(key, group, state, [], [], [], store)
    end
  end

  # -- claims ---------------------------------------------------------------

  defp claim(key, group, consumer, min_idle, ids, opts, state, store) do
    now_ms = CommandTime.now_ms()
    delivered_at = delivery_time(opts, now_ms)
    ids = Enum.uniq(ids)

    with {:ok, live} <- live_ids(key, ids, store) do
      {claimed, dropped, pending} =
        Enum.reduce(ids, {[], [], state.pending}, fn id, {claimed, dropped, pending} ->
          case claim_one(id, Map.get(pending, id), live, min_idle, opts, now_ms) do
            :skip ->
              {claimed, dropped, pending}

            :drop ->
              {claimed, [id | dropped], Map.delete(pending, id)}

            {:claim, count} ->
              {[id | claimed], dropped, Map.put(pending, id, {consumer, delivered_at, count})}
          end
        end)

      claimed = Enum.reverse(claimed)
      last_delivered = later_id(state.last_delivered, opts.lastid)

      state = %{
        state
        | pending: pending,
          consumers: Map.put(state.consumers, consumer, now_ms),
          last_delivered: last_delivered,
          last_changed?: last_delivered != state.last_delivered
      }

      with :ok <- write(key, group, state, [consumer], dropped, [], store, claimed),
           do: claim_reply(key, claimed, opts.justid, store)
    end
  end

  defp claim_one(id, nil, live, _min_idle, %{force: true} = opts, _now_ms) do
    if MapSet.member?(live, id), do: {:claim, next_count(1, opts)}, else: :skip
  end

  defp claim_one(_id, nil, _live, _min_idle, _opts, _now_ms), do: :skip

  defp claim_one(id, entry, live, min_idle, opts, now_ms) do
    cond do
      now_ms - elem(entry, 1) < min_idle -> :skip
      not MapSet.member?(live, id) -> :drop
      true -> {:claim, next_count(delivery_count(entry), opts)}
    end
  end

  defp next_count(_count, %{retrycount: retrycount}) when is_integer(retrycount), do: retrycount
  defp next_count(count, %{justid: true}), do: count
  defp next_count(count, _opts), do: count + 1

  # TIME, or now less IDLE; one before 0 or after now is now, as Redis.
  defp delivery_time(%{time: time}, now_ms) when is_integer(time), do: clamp(time, now_ms)

  defp delivery_time(%{idle: idle}, now_ms) when is_integer(idle),
    do: clamp(now_ms - idle, now_ms)

  defp delivery_time(_opts, now_ms), do: now_ms

  defp clamp(time, now_ms) when time < 0 or time > now_ms, do: now_ms
  defp clamp(time, _now_ms), do: time

  defp autoclaim(key, group, consumer, min_idle, start, count, justid, state, store) do
    now_ms = CommandTime.now_ms()

    candidates =
      state.pending
      |> Enum.map(fn {id, entry} -> {ID.parse_id!(id), id, entry} end)
      |> Enum.filter(fn {parsed, _id, _entry} -> ID.in_range?(parsed, start, :max) end)
      |> Enum.sort_by(&elem(&1, 0))

    {scanned, rest} = Enum.split(candidates, count * @autoclaim_attempts_factor)

    with {:ok, live} <- live_ids(key, Enum.map(scanned, &elem(&1, 1)), store) do
      {claimed, dropped, pending, unscanned} =
        autoclaim_scan(scanned, live, min_idle, count, justid, consumer, now_ms, state.pending)

      next_cursor =
        case unscanned ++ rest do
          [{_parsed, id, _entry} | _] -> id
          [] -> "0-0"
        end

      state = %{state | pending: pending, consumers: Map.put(state.consumers, consumer, now_ms)}

      with :ok <- write(key, group, state, [consumer], dropped, [], store, claimed),
           reply when is_list(reply) <- claim_reply(key, claimed, justid, store) do
        [next_cursor, reply, dropped]
      end
    end
  end

  defp autoclaim_scan(entries, live, min_idle, count, justid, consumer, now_ms, pending) do
    autoclaim_scan(entries, live, min_idle, count, justid, consumer, now_ms, pending, [], [])
  end

  defp autoclaim_scan(rest, _live, _min, 0, _justid, _consumer, _now, pending, claimed, dropped),
    do: {Enum.reverse(claimed), Enum.reverse(dropped), pending, rest}

  defp autoclaim_scan(
         [],
         _live,
         _min,
         _count,
         _justid,
         _consumer,
         _now,
         pending,
         claimed,
         dropped
       ),
       do: {Enum.reverse(claimed), Enum.reverse(dropped), pending, []}

  defp autoclaim_scan(
         [{_parsed, id, entry} | rest],
         live,
         min_idle,
         count,
         justid,
         consumer,
         now_ms,
         pending,
         claimed,
         dropped
       ) do
    cond do
      now_ms - elem(entry, 1) < min_idle ->
        autoclaim_scan(
          rest,
          live,
          min_idle,
          count,
          justid,
          consumer,
          now_ms,
          pending,
          claimed,
          dropped
        )

      not MapSet.member?(live, id) ->
        pending = Map.delete(pending, id)

        autoclaim_scan(rest, live, min_idle, count, justid, consumer, now_ms, pending, claimed, [
          id | dropped
        ])

      true ->
        deliveries = next_count(delivery_count(entry), %{justid: justid})
        pending = Map.put(pending, id, {consumer, now_ms, deliveries})

        autoclaim_scan(
          rest,
          live,
          min_idle,
          count - 1,
          justid,
          consumer,
          now_ms,
          pending,
          [
            id | claimed
          ],
          dropped
        )
    end
  end

  defp claim_reply(_key, ids, true, _store), do: ids

  defp claim_reply(_key, [], false, _store), do: []

  defp claim_reply(key, ids, false, store) do
    raw_values = Entries.batch_get(store, key, Enum.map(ids, &Entries.entry_key(key, &1)))

    case ReadResult.first_failure(raw_values) do
      nil ->
        ids |> Enum.zip(raw_values) |> Enum.map(fn {id, raw} -> Entries.decode_entry(id, raw) end)

      failure ->
        ReadResult.command_error(failure)
    end
  end

  # The IDs among `ids` the stream holds.
  defp live_ids(_key, [], _store), do: {:ok, MapSet.new()}

  defp live_ids(key, ids, store) do
    raw_values = Entries.batch_get(store, key, Enum.map(ids, &Entries.entry_key(key, &1)))

    case ReadResult.first_failure(raw_values) do
      nil ->
        {:ok,
         ids
         |> Enum.zip(raw_values)
         |> Enum.reject(fn {_id, raw} -> is_nil(raw) end)
         |> MapSet.new(&elem(&1, 0))}

      failure ->
        ReadResult.command_error(failure)
    end
  end

  defp delivery_count({_consumer, _delivered_at_ms, count}), do: count
  defp delivery_count({_consumer, _delivered_at_ms}), do: 1

  defp later_id(current, nil), do: current

  defp later_id(current, candidate) do
    if ID.compare(ID.parse_id!(candidate), ID.parse_id!(current)) == :gt,
      do: candidate,
      else: current
  end

  # -- group state ----------------------------------------------------------

  defp claim_state(store, key, group) do
    case group_state(store, key, group) do
      {:error, "NOGROUP" <> _} ->
        {:error, "NOGROUP No such key '#{key}' or consumer group '#{group}'"}

      other ->
        other
    end
  end

  defp group_state(store, key, group) do
    with :ok <- TypeRegistry.command_check_type(key, :stream, store) do
      case Groups.serialized_state(store, key, group) do
        :missing ->
          {:error, "NOGROUP No such consumer group '#{group}' for key name '#{key}'"}

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        {:error, _reason} = error ->
          error

        {:v1, last_delivered, consumers, pending} ->
          {:ok, new_state(:v1, last_delivered, consumers, pending)}

        {:v2, _last_delivered} ->
          case Groups.serialized_entry(store, key, group) do
            {:ok, last_delivered, consumers, pending} ->
              {:ok, new_state(:v2, last_delivered, consumers, pending)}

            {:error, {:storage_read_failed, _reason}} = failure ->
              ReadResult.command_error(failure)

            {:error, _reason} = error ->
              error
          end
      end
    end
  end

  defp new_state(format, last_delivered, consumers, pending) do
    %{
      format: format,
      last_delivered: last_delivered,
      consumers: consumers,
      pending: pending,
      last_changed?: false
    }
  end

  defp stream_present(store, key) do
    case TypeRegistry.check_type_status(key, :stream, CompoundKey.type_key(key), store) do
      {:ok, true} -> :ok
      {:ok, false} -> {:error, @no_key}
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
    end
  end

  defp group_id("$", key, store) do
    case Meta.serialized_entry(key, store) do
      {_len, _first, last, _ms, _seq} -> {:ok, last}
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
    end
  end

  defp group_id(id_str, _key, _store) do
    case ID.parse_full_id(id_str) do
      {:ok, {ms, seq}} -> {:ok, "#{ms}-#{seq}"}
      {:error, _message} = error -> error
    end
  end

  # A v2 group keeps one row per pending entry and consumer: write the
  # `consumers` and `claimed` rows, delete the `dropped` pending rows and the
  # `deleted_consumers`, and the group row when its last-delivered ID moved. A
  # v1 group keeps everything in its one row; its entries keep no delivery
  # count past one.
  defp write(key, group, state, consumers, dropped, deleted_consumers, store, claimed \\ [])

  defp write(
         key,
         group,
         %{format: :v2} = state,
         consumers,
         dropped,
         deleted_consumers,
         store,
         claimed
       ) do
    puts =
      Enum.map(Enum.uniq(consumers), fn consumer ->
        Groups.serialized_consumer_entry(
          key,
          group,
          consumer,
          Map.fetch!(state.consumers, consumer)
        )
      end) ++
        Enum.map(claimed, fn id ->
          {consumer, delivered_at, count} = Map.fetch!(state.pending, id)
          Groups.serialized_pending_entry(key, group, id, consumer, delivered_at, count)
        end) ++
        if(state.last_changed?,
          do: [Groups.serialized_v2_put_entry(key, group, state.last_delivered)],
          else: []
        )

    deletes =
      Groups.pending_keys(key, group, dropped) ++
        Enum.map(deleted_consumers, &CompoundKey.stream_consumer(key, group, &1))

    with :ok <- put_rows(store, key, puts),
         :ok <- delete_rows(store, key, deletes),
         do: invalidate(key, group, store)
  end

  defp write(key, group, %{format: :v1} = state, _consumers, _dropped, _deleted, store, _claimed) do
    pending = Map.new(state.pending, fn {id, entry} -> {id, {elem(entry, 0), elem(entry, 1)}} end)

    entry =
      Groups.serialized_put_entry(key, group, state.last_delivered, state.consumers, pending)

    with :ok <- Ops.compound_batch_put(store, key, [entry]), do: invalidate(key, group, store)
  end

  defp put_rows(_store, _key, []), do: :ok
  defp put_rows(store, key, rows), do: Ops.compound_batch_put(store, key, rows)

  defp delete_rows(_store, _key, []), do: :ok
  defp delete_rows(store, key, keys), do: Ops.compound_batch_delete(store, key, keys)

  defp invalidate(key, group, %{defer_stream_group: defer} = store) when is_function(defer, 2),
    do: defer.(CacheKey.build(store, key), {:invalidate, group})

  defp invalidate(key, group, store) do
    Groups.delete_group_local(store, key, group)

    case store do
      %{defer_stream_cleanup: defer} when is_function(defer, 1) ->
        defer.(CacheKey.build(store, key))

      _other ->
        :ok
    end
  end
end
