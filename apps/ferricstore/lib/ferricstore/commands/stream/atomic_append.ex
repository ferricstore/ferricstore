defmodule Ferricstore.Commands.Stream.AtomicAppend do
  @moduledoc false

  defmodule Plan do
    @moduledoc """
    Deterministic, read-only result of planning one compact Stream append.

    `entries` is the complete value-validated durable projection (type marker
    when needed, entry rows, and one final metadata row). `publication` data
    is kept here so the Raft state machine can explicitly publish the member
    catalog only after that projection has been appended successfully.
    """

    @enforce_keys [
      :key,
      :results,
      :entries,
      :member_entries,
      :latest_update,
      :member_prefix
    ]
    defstruct @enforce_keys

    @type member_entry ::
            {{binary(), {non_neg_integer(), non_neg_integer()}}, binary()}

    @type t :: %__MODULE__{
            key: binary(),
            results: [term()],
            entries: [{binary(), binary(), non_neg_integer()}],
            member_entries: [member_entry()],
            latest_update: tuple() | nil,
            member_prefix: binary()
          }
  end

  defmodule Publication do
    @moduledoc """
    Derived cache and member-catalog rows published after an append plan commits.
    """

    @enforce_keys [:cache_entry, :member_prefix, :member_count, :member_entries]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            cache_entry: {
              Ferricstore.Commands.Stream.CacheKey.t(),
              non_neg_integer(),
              binary(),
              binary(),
              non_neg_integer(),
              non_neg_integer()
            },
            member_prefix: binary(),
            member_count: non_neg_integer(),
            member_entries: [Plan.member_entry()]
          }
  end

  alias Ferricstore.Commands.Stream.{CacheKey, Entries, ID, Index, Meta}
  alias Ferricstore.CommandTime
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @approximate_trim_slack 64
  @max_u64 18_446_744_073_709_551_615

  @spec run(binary(), term(), [binary()], term() | nil, boolean(), map()) :: term()
  def run(key, id_spec, fields, trim_opts, nomkstream, store) do
    encoded_fields = Entries.encode_fields(fields)
    run_payload(key, id_spec, {:value, encoded_fields}, trim_opts, nomkstream, store)
  end

  @doc false
  @spec run_blob_ref(binary(), term(), binary(), term() | nil, boolean(), map()) :: term()
  def run_blob_ref(key, id_spec, encoded_ref, trim_opts, nomkstream, store)
      when is_binary(encoded_ref) do
    run_payload(key, id_spec, {:blob_ref, encoded_ref}, trim_opts, nomkstream, store)
  end

  @doc false
  @spec run_many_auto(binary(), [[binary()]], map()) :: [term()] | {:error, term()}
  def run_many_auto(key, fields_lists, store) when is_binary(key) and is_list(fields_lists) do
    with {:ok, plan} <- plan_many_auto(key, fields_lists, store) do
      commit_many_auto(plan, store)
    end
  end

  @doc """
  Builds the complete deterministic projection for a compact auto-ID append
  without mutating storage, caches, or indexes.

  Per-entry validation failures are retained in `Plan.results`; a storage read
  failure aborts planning because no deterministic projection can be built.
  """
  @spec plan_many_auto(binary(), [[binary()]], map()) ::
          {:ok, Plan.t()} | {:error, term()}
  def plan_many_auto(key, fields_lists, store)
      when is_binary(key) and is_list(fields_lists) do
    {type_key, entry_prefix, meta_key} = CompoundKey.stream_write_keys(key)

    with {:ok, marker_present?} <- stream_type_status(key, type_key, store),
         {:ok, current} <- current_meta(key, marker_present?, store) do
      build_many_auto_plan(
        key,
        fields_lists,
        current,
        marker_present?,
        type_key,
        entry_prefix,
        meta_key,
        store
      )
    else
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      {:error, _reason} = error ->
        {:ok,
         empty_many_auto_plan(
           key,
           entry_prefix,
           List.duplicate(error, length(fields_lists))
         )}
    end
  end

  @doc false
  @spec commit_many_auto(Plan.t(), map()) :: [term()] | {:error, term()}
  def commit_many_auto(%Plan{entries: []} = plan, _store), do: plan.results

  def commit_many_auto(%Plan{} = plan, store) do
    commit_many_auto(plan, store, :generic)
  end

  @doc false
  @spec commit_terminal_many_auto(Plan.t(), map()) :: [term()] | {:error, term()}
  def commit_terminal_many_auto(%Plan{entries: []} = plan, _store), do: plan.results

  def commit_terminal_many_auto(%Plan{} = plan, store) do
    commit_many_auto(plan, store, :terminal)
  end

  @doc false
  @spec commit_terminal_many_auto_group([Plan.t()], map()) :: :ok | {:error, term()}
  def commit_terminal_many_auto_group(plans, store) when is_list(plans) do
    batches =
      Enum.reduce(plans, [], fn
        %Plan{entries: []}, acc ->
          acc

        %Plan{key: key, member_prefix: member_prefix, entries: entries}, acc ->
          [{key, member_prefix, entries} | acc]
      end)
      |> Enum.reverse()

    persist_terminal_auto_group(batches, store)
  end

  @doc false
  @spec commit_terminal_many_auto_group_reversed([Plan.t()], map()) :: :ok | {:error, term()}
  def commit_terminal_many_auto_group_reversed(reversed_plans, store)
      when is_list(reversed_plans) do
    # The grouped state-machine planner naturally accumulates plans in reverse.
    # Prepending their physical batches restores apply order without first
    # copying the plan list and then reversing the batch list again.
    batches =
      Enum.reduce(reversed_plans, [], fn
        %Plan{entries: []}, acc ->
          acc

        %Plan{key: key, member_prefix: member_prefix, entries: entries}, acc ->
          [{key, member_prefix, entries} | acc]
      end)

    persist_terminal_auto_group(batches, store)
  end

  defp commit_many_auto(plan, store, mode) do
    case persist_auto_batch(plan.key, plan.member_prefix, plan.entries, store, mode) do
      :ok ->
        finish_many_auto_commit(plan, store, mode)

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec publication(Plan.t(), term()) :: Publication.t()
  def publication(
        %Plan{
          latest_update: {:append, len, first, last, ms, seq, _id, _retained?, _deletes}
        } = plan,
        store
      ) do
    cache_key = CacheKey.build(store, plan.key)

    %Publication{
      cache_entry: {cache_key, len, first, last, ms, seq},
      member_prefix: plan.member_prefix,
      member_count: len,
      member_entries: plan.member_entries
    }
  end

  # Terminal plans return their complete derived publication explicitly to the
  # state machine. Generic plans retain the deferred cache path because they can
  # share a pending-write scope with adjacent non-Stream commands.
  defp finish_many_auto_commit(plan, _store, :terminal), do: plan.results

  defp finish_many_auto_commit(plan, store, _mode) do
    with :ok <-
           defer_cache_updates(
             plan.key,
             plan.latest_update,
             plan.member_entries,
             store
           ) do
      plan.results
    end
  end

  defp run_payload(key, id_spec, payload, trim_opts, nomkstream, store) do
    with {:ok, marker_present?} <- Meta.type_marker_status(key, store) do
      if nomkstream and not marker_present? do
        nil
      else
        append(key, id_spec, payload, trim_opts, marker_present?, store)
      end
    else
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
    end
  end

  defp append(key, id_spec, payload, trim_opts, marker_present?, store) do
    with :ok <- command_check_type(key, marker_present?, store),
         {:ok, current} <- current_meta(key, marker_present?, store),
         {:ok, {ms, seq}} <- resolve_id(id_spec, current),
         {id_str, entry_key} = Entries.entry_key_and_id(key, ms, seq),
         {:ok, delete_ids, retained?, first_id} <-
           trim_plan(key, id_str, trim_opts, current, store),
         :ok <-
           persist(
             key,
             id_str,
             entry_key,
             payload,
             current,
             ms,
             seq,
             delete_ids,
             retained?,
             first_id,
             marker_present?,
             store
           ) do
      id_str
    else
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
    end
  end

  defp build_many_auto_plan(
         key,
         fields_lists,
         current,
         marker_present?,
         type_key,
         entry_prefix,
         meta_key,
         store
       ) do
    apply_now_ms = stream_apply_now_ms(store)

    {results, entries, member_entries, final_meta, _id_prefix, _id_cursor} =
      build_many_auto_entries(
        fields_lists,
        store,
        entry_prefix,
        apply_now_ms,
        current,
        nil,
        :resolve,
        [],
        [],
        []
      )

    results = Enum.reverse(results)

    case entries do
      [] ->
        {:ok, empty_many_auto_plan(key, entry_prefix, results)}

      _successful ->
        {len, first, last, ms, seq} = final_meta
        meta_entry = Meta.serialized_put_entry_with_key(meta_key, len, first, last, ms, seq)
        # Build the forward durable projection in one pass. `Enum.reverse/1 ++`
        # copies the entire entry spine a second time for large compact batches.
        data_entries = :lists.reverse(entries, [meta_entry])

        put_entries =
          if marker_present? do
            data_entries
          else
            [{type_key, "stream", 0} | data_entries]
          end

        with :ok <- validate_many_auto_internal_values(marker_present?, meta_entry, store) do
          {:ok,
           %Plan{
             key: key,
             results: results,
             entries: put_entries,
             # Keep publication rows aligned with the durable entry projection so
             # storage consumes the explicit plan instead of re-identifying Stream
             # members from compound-key prefixes.
             member_entries: Enum.reverse(member_entries),
             latest_update: {:append, len, first, last, ms, seq, last, true, []},
             member_prefix: entry_prefix
           }}
        end
    end
  end

  defp build_many_auto_entries(
         [],
         _store,
         _entry_prefix,
         _apply_now_ms,
         meta,
         id_prefix,
         id_cursor,
         results,
         entries,
         member_entries
       ),
       do: {results, entries, member_entries, meta, id_prefix, id_cursor}

  defp build_many_auto_entries(
         [fields | remaining_fields],
         store,
         entry_prefix,
         apply_now_ms,
         meta,
         id_prefix,
         id_cursor,
         results,
         entries,
         member_entries
       ) do
    encoded_fields = Entries.encode_fields(fields)

    case validate_value(encoded_fields, store) do
      :ok ->
        case resolve_many_auto_id(id_cursor, apply_now_ms, meta) do
          {:ok, {ms, seq}, next_id_cursor} ->
            {next_id_prefix, prefix_binary} = id_prefix_binary(id_prefix, ms)
            sequence_binary = Integer.to_string(seq)
            entry_key = entry_prefix <> prefix_binary <> sequence_binary
            id_size = byte_size(prefix_binary) + byte_size(sequence_binary)
            id = binary_part(entry_key, byte_size(entry_prefix), id_size)
            {current_len, current_first, _current_last, _last_ms, _last_seq} = meta
            new_len = current_len + 1
            first_id = if current_len == 0, do: id, else: current_first
            next_meta = {new_len, first_id, id, ms, seq}

            build_many_auto_entries(
              remaining_fields,
              store,
              entry_prefix,
              apply_now_ms,
              next_meta,
              next_id_prefix,
              next_id_cursor,
              [id | results],
              [{entry_key, encoded_fields, 0} | entries],
              [{{entry_prefix, {ms, seq}}, entry_key} | member_entries]
            )

          {:error, reason, next_id_cursor} ->
            build_many_auto_entries(
              remaining_fields,
              store,
              entry_prefix,
              apply_now_ms,
              meta,
              id_prefix,
              next_id_cursor,
              [{:error, reason} | results],
              entries,
              member_entries
            )
        end

      {:error, _reason} = error ->
        build_many_auto_entries(
          remaining_fields,
          store,
          entry_prefix,
          apply_now_ms,
          meta,
          id_prefix,
          id_cursor,
          [error | results],
          entries,
          member_entries
        )
    end
  end

  defp resolve_many_auto_id(:resolve, apply_now_ms, {_len, _first, _last, last_ms, last_seq}) do
    case ID.resolve_auto_at(apply_now_ms, last_ms, last_seq) do
      {:ok, {ms, seq} = id} -> {:ok, id, next_many_auto_cursor(ms, seq)}
      {:error, reason} -> {:error, reason, {:failed, reason}}
    end
  end

  defp resolve_many_auto_id({:next, ms, seq}, _apply_now_ms, _meta),
    do: {:ok, {ms, seq}, next_many_auto_cursor(ms, seq)}

  defp resolve_many_auto_id({:failed, reason} = cursor, _apply_now_ms, _meta),
    do: {:error, reason, cursor}

  defp resolve_many_auto_id({:exhausted, ms}, _apply_now_ms, _meta) do
    {:error, reason} = ID.resolve_auto_at(ms, ms, @max_u64)
    {:error, reason, {:exhausted, ms}}
  end

  defp next_many_auto_cursor(ms, seq) when seq < @max_u64, do: {:next, ms, seq + 1}
  defp next_many_auto_cursor(ms, @max_u64), do: {:exhausted, ms}

  defp id_prefix_binary({ms, prefix_binary} = id_prefix, ms),
    do: {id_prefix, prefix_binary}

  defp id_prefix_binary(_id_prefix, ms) do
    prefix = Integer.to_string(ms) <> "-"
    {{ms, prefix}, prefix}
  end

  defp empty_many_auto_plan(key, member_prefix, results) do
    %Plan{
      key: key,
      results: results,
      entries: [],
      member_entries: [],
      latest_update: nil,
      member_prefix: member_prefix
    }
  end

  defp stream_apply_now_ms(%{stream_apply_now_ms: now_ms})
       when is_integer(now_ms) and now_ms >= 0,
       do: now_ms

  defp stream_apply_now_ms(_store), do: CommandTime.now_ms()

  defp persist_auto_batch(
         key,
         member_prefix,
         entries,
         %{terminal_stream_validated_batch_put: put_batch},
         :terminal
       )
       when is_function(put_batch, 3),
       do: put_batch.(key, member_prefix, entries)

  defp persist_auto_batch(
         key,
         _member_prefix,
         entries,
         %{terminal_stream_validated_batch_put: put_batch},
         :terminal
       )
       when is_function(put_batch, 2),
       do: put_batch.(key, entries)

  defp persist_auto_batch(key, _member_prefix, entries, store, _mode),
    do: Ops.compound_batch_put(store, key, entries)

  defp persist_terminal_auto_group(
         batches,
         %{terminal_stream_validated_grouped_batch_put: put_batches}
       )
       when is_function(put_batches, 1),
       do: put_batches.(batches)

  defp persist_terminal_auto_group(batches, store) do
    Enum.reduce_while(batches, :ok, fn {key, member_prefix, entries}, :ok ->
      case persist_auto_batch(key, member_prefix, entries, store, :terminal) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp stream_type_status(
         key,
         type_key,
         %{terminal_stream_type_status: type_status} = store
       )
       when is_function(type_status, 2) do
    case type_status.(key, type_key) do
      {:ok, true} = present -> present
      :fallback -> TypeRegistry.check_type_status(key, :stream, type_key, store)
      _invalid -> TypeRegistry.check_type_status(key, :stream, type_key, store)
    end
  end

  defp stream_type_status(key, type_key, store),
    do: TypeRegistry.check_type_status(key, :stream, type_key, store)

  defp current_meta(_key, false, _store), do: {:ok, {0, "0-0", "0-0", 0, 0}}

  defp current_meta(key, true, %{stream_meta_cache_safe: true} = store) do
    case Meta.cached_entry(key, store) do
      {:ok, meta} -> {:ok, meta}
      :miss -> current_meta_durable(key, store)
    end
  end

  defp current_meta(key, true, store), do: current_meta_durable(key, store)

  defp current_meta_durable(key, store) do
    case Meta.serialized_entry(key, store) do
      {len, first, last, ms, seq} -> {:ok, {len, first, last, ms, seq}}
      {:error, _reason} = error -> error
    end
  end

  defp resolve_id(id_spec, {_len, _first, _last, last_ms, last_seq}) do
    ID.resolve(id_spec, last_ms, last_seq)
  end

  defp command_check_type(_key, true, _store), do: :ok

  defp command_check_type(key, false, store) do
    key
    |> TypeRegistry.check_type(:stream, store)
    |> ReadResult.command_result()
  end

  defp trim_plan(_key, new_id, nil, current, _store),
    do: {:ok, [], true, appended_first_id(current, new_id)}

  defp trim_plan(key, new_id, {:maxlen, approximate?, max_len}, current, store)
       when is_integer(max_len) and max_len >= 0 do
    {current_len, _first, _last, _ms, _seq} = current
    target_len = current_len + 1
    trim_at = if approximate?, do: max_len + @approximate_trim_slack, else: max_len

    if target_len <= trim_at do
      {:ok, [], true, appended_first_id(current, new_id)}
    else
      indexed_maxlen_trim(key, new_id, max_len, current, store)
    end
  end

  defp trim_plan(key, new_id, {:minid, _approximate?, min_id_str}, current, store) do
    with {:ok, min_id} <- ID.parse_full_id(min_id_str) do
      if trim_minid_noop?(current, new_id, min_id) do
        {:ok, [], true, appended_first_id(current, new_id)}
      else
        indexed_minid_trim(key, new_id, min_id, store)
      end
    end
  end

  defp indexed_maxlen_trim(key, new_id, max_len, {current_len, _, _, _, _}, store) do
    delete_count = max(current_len + 1 - max_len, 0)
    delete_current_count = min(delete_count, current_len)
    retained? = delete_count <= current_len
    lookahead = if delete_current_count < current_len, do: 1, else: 0

    with :ok <- Index.ensure(key, store) do
      indexed = Index.slice(key, :min, :max, delete_current_count + lookahead, false, store)
      current_delete_ids = indexed |> Enum.take(delete_current_count) |> indexed_ids()

      first_id =
        case Enum.at(indexed, delete_current_count) do
          {id, _compound_key} -> id
          nil when retained? -> new_id
          nil -> "0-0"
        end

      delete_ids = if retained?, do: current_delete_ids, else: current_delete_ids ++ [new_id]
      {:ok, delete_ids, retained?, first_id}
    end
  end

  defp indexed_minid_trim(key, new_id, min_id, store) do
    with :ok <- Index.ensure(key, store) do
      delete_ids = Index.ids_before(key, min_id, store)
      retained? = ID.compare(ID.parse_id!(new_id), min_id) != :lt

      first_id =
        case Index.slice(key, min_id, :max, 1, false, store) do
          [{id, _compound_key}] -> id
          [] when retained? -> new_id
          [] -> "0-0"
        end

      delete_ids = if retained?, do: delete_ids, else: delete_ids ++ [new_id]
      {:ok, delete_ids, retained?, first_id}
    end
  end

  defp indexed_ids(indexed), do: Enum.map(indexed, &elem(&1, 0))

  defp appended_first_id({0, _first, _last, _ms, _seq}, new_id), do: new_id
  defp appended_first_id({_len, first, _last, _ms, _seq}, _new_id), do: first

  defp trim_minid_noop?({0, _first, _last, _ms, _seq}, new_id, min_id),
    do: ID.compare(ID.parse_id!(new_id), min_id) != :lt

  defp trim_minid_noop?({_len, first, _last, _ms, _seq}, new_id, min_id) do
    ID.compare(ID.parse_id!(first), min_id) != :lt and
      ID.compare(ID.parse_id!(new_id), min_id) != :lt
  end

  defp persist(
         key,
         id_str,
         entry_key,
         payload,
         {current_len, _current_first, _current_last, _last_ms, _last_seq},
         ms,
         seq,
         delete_ids,
         retained?,
         first_id,
         marker_present?,
         store
       ) do
    deleted_existing = Enum.count(delete_ids, &(&1 != id_str))
    new_len = max(current_len - deleted_existing, 0) + if(retained?, do: 1, else: 0)
    last_id = if retained?, do: id_str, else: "0-0"

    with :ok <- validate_payload(payload, store),
         :ok <- delete_entries(key, delete_ids, store) do
      meta_entry = Meta.serialized_put_entry(key, new_len, first_id, last_id, ms, seq)

      data_entries =
        if retained? do
          entry = {entry_key, payload_value(payload), 0}
          [entry, meta_entry]
        else
          [meta_entry]
        end

      put_entries =
        if marker_present? do
          data_entries
        else
          [{Ferricstore.Store.CompoundKey.type_key(key), "stream", 0} | data_entries]
        end

      case persist_entries(key, put_entries, payload, store) do
        :ok ->
          cache_update =
            {:append, new_len, first_id, last_id, ms, seq, id_str, retained?, delete_ids}

          defer_cache_update(key, cache_update, store)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp delete_entries(_key, [], _store), do: :ok

  defp delete_entries(key, ids, store) do
    Ops.compound_batch_delete(store, key, Entries.delete_keys(key, ids))
  end

  defp defer_cache_update(key, update, %{defer_stream_append: defer} = store)
       when is_function(defer, 2) do
    defer.(CacheKey.build(store, key), update)
  end

  defp defer_cache_update(key, _update, %{defer_stream_cleanup: defer} = store)
       when is_function(defer, 1) do
    defer.(CacheKey.build(store, key))
  end

  defp defer_cache_update(_key, _update, _store), do: :ok

  defp defer_cache_updates(
         key,
         latest_update,
         member_entries,
         %{defer_stream_append_many: defer} = store
       )
       when is_function(defer, 3) do
    defer.(CacheKey.build(store, key), latest_update, member_entries)
  end

  defp defer_cache_updates(key, latest_update, member_entries, store) do
    Enum.reduce_while(member_entries, :ok, fn
      {{_prefix, {ms, seq}}, _compound_key}, :ok ->
        id = Integer.to_string(ms) <> "-" <> Integer.to_string(seq)
        update = put_elem(latest_update, 6, id)

        case defer_cache_update(key, update, store) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  defp persist_entries(key, entries, {:value, _encoded_fields}, store) do
    Ops.compound_batch_put(store, key, entries)
  end

  defp persist_entries(
         key,
         entries,
         {:blob_ref, _encoded_ref},
         %{compound_blob_batch_put: put_blob_batch}
       )
       when is_function(put_blob_batch, 2) do
    blob_entries =
      Enum.map(entries, fn
        {compound_key, value, expire_at_ms} ->
          kind = if String.starts_with?(compound_key, "X:"), do: :blob_ref, else: :value
          {compound_key, value, expire_at_ms, kind}
      end)

    put_blob_batch.(key, blob_entries)
  end

  defp persist_entries(_key, _entries, {:blob_ref, _encoded_ref}, _store),
    do: {:error, :stream_blob_side_channel_unavailable}

  defp payload_value({:value, encoded_fields}), do: encoded_fields
  defp payload_value({:blob_ref, encoded_ref}), do: encoded_ref

  defp validate_payload({:value, value}, store), do: validate_value(value, store)
  defp validate_payload({:blob_ref, _encoded_ref}, _store), do: :ok

  defp validate_value(value, %{max_value_size: max})
       when is_binary(value) and is_integer(max) and max > 0 do
    size = byte_size(value)

    if size <= max do
      :ok
    else
      {:error, "ERR value too large (#{size} bytes, max #{max} bytes)"}
    end
  end

  defp validate_value(value, %{validate_value: validate}) when is_function(validate, 1),
    do: validate.(value)

  defp validate_value(_value, _store), do: :ok

  defp validate_many_auto_internal_values(marker_present?, {_key, meta, _expiry}, store) do
    with :ok <- maybe_validate_stream_type_value(marker_present?, store) do
      validate_value(meta, store)
    end
  end

  defp maybe_validate_stream_type_value(true, _store), do: :ok
  defp maybe_validate_stream_type_value(false, store), do: validate_value("stream", store)
end
