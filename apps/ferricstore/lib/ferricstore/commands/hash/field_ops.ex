defmodule Ferricstore.Commands.Hash.FieldOps do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Store.{CompoundKey, Ops, TypeRegistry}

  @spec set_pairs([binary()], binary(), map(), term()) :: non_neg_integer() | {:error, term()}
  def set_pairs(field_value_pairs, key, store, type_status) do
    set_pairs(field_value_pairs, key, store, 0, type_status)
  end

  @spec set_pairs_with_ttl([binary()], binary(), map(), integer(), term()) ::
          non_neg_integer() | {:error, term()}
  def set_pairs_with_ttl(field_value_pairs, key, store, expire_at_ms, type_status) do
    set_pairs(field_value_pairs, key, store, expire_at_ms, type_status)
  end

  @spec rollback_new_type_marker(binary(), map(), term(), term()) :: term()
  def rollback_new_type_marker(key, store, type_status, write_error) do
    rollback_new_hash_type_marker(key, store, type_status, write_error)
  end

  @spec expire_fields(binary(), [binary()], integer(), map()) :: [integer()] | {:error, term()}
  def expire_fields(key, fields, expire_at_ms, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      {unique_fields, compound_keys, metas_by_field} = batch_field_metas(fields, key, store)
      entries = existing_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms)

      case Ops.compound_batch_put(store, key, entries) do
        :ok ->
          Enum.map(fields, fn field ->
            case Map.fetch!(metas_by_field, field) do
              nil -> -2
              {_value, _old_expire} -> 1
            end
          end)

        {:error, _} = err ->
          err
      end
    end
  end

  @spec ttl_fields(binary(), [binary()], :seconds | :milliseconds, map()) ::
          [integer()] | {:error, term()}
  def ttl_fields(key, fields, unit, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      now = CommandTime.now_ms()
      {_unique_fields, _compound_keys, metas_by_field} = batch_field_metas(fields, key, store)

      Enum.map(fields, fn field ->
        case Map.fetch!(metas_by_field, field) do
          nil ->
            -2

          {_value, 0} ->
            -1

          {_value, expire_at_ms} ->
            remaining_ms = expire_at_ms - now

            cond do
              remaining_ms <= 0 -> -2
              unit == :seconds -> div(remaining_ms, 1000)
              true -> remaining_ms
            end
        end
      end)
    end
  end

  @spec persist_fields(binary(), [binary()], map()) :: [integer()] | {:error, term()}
  def persist_fields(key, fields, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      {unique_fields, compound_keys, metas_by_field} = batch_field_metas(fields, key, store)
      entries = persistent_field_entries(unique_fields, compound_keys, metas_by_field, [])

      case Ops.compound_batch_put(store, key, entries) do
        :ok ->
          Enum.map(fields, fn field ->
            case Map.fetch!(metas_by_field, field) do
              nil -> -2
              {_value, 0} -> -1
              {_value, _expire_at_ms} -> 1
            end
          end)

        {:error, _} = err ->
          err
      end
    end
  end

  @spec expiretime_fields(binary(), [binary()], map()) :: [integer()] | {:error, term()}
  def expiretime_fields(key, fields, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      {_unique_fields, _compound_keys, metas_by_field} = batch_field_metas(fields, key, store)

      Enum.map(fields, fn field ->
        case Map.fetch!(metas_by_field, field) do
          nil -> -2
          {_value, 0} -> -1
          {_value, expire_at_ms} -> div(expire_at_ms, 1000)
        end
      end)
    end
  end

  @spec getdel_fields(binary(), [binary()], map()) :: [binary() | nil] | {:error, term()}
  def getdel_fields(key, fields, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_keys =
        fields
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.hash_field(key, &1))

      metas = Ops.compound_batch_get_meta(store, key, compound_keys)
      metas_by_key = metas_by_key(compound_keys, metas, %{})
      {results, deleted_entries} = getdel_results(fields, key, metas_by_key, [], %{}, [])
      deleted_count = length(deleted_entries)

      with :ok <- delete_and_cleanup(key, deleted_entries, deleted_count, store) do
        results
      end
    end
  end

  @spec getex_parsed(binary(), [binary()], integer(), map()) :: [binary() | nil] | {:error, term()}
  def getex_parsed(key, fields, expire_at_ms, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      getex_fields(fields, key, store, expire_at_ms)
    end
  end

  @spec getex_fields([binary()], binary(), map(), integer()) :: [binary() | nil] | {:error, term()}
  def getex_fields(fields, key, store, expire_at_ms) do
    {unique_fields, compound_keys, metas_by_field} = batch_field_metas(fields, key, store)
    entries = existing_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms)

    case Ops.compound_batch_put(store, key, entries) do
      :ok ->
        Enum.map(fields, fn field ->
          case Map.fetch!(metas_by_field, field) do
            nil -> nil
            {value, _old_expire} -> value
          end
        end)

      {:error, _} = err ->
        err
    end
  end

  @spec delete_and_cleanup(binary(), [{binary(), binary(), integer()}], non_neg_integer(), map()) ::
          :ok | {:error, term()}
  def delete_and_cleanup(key, deleted_entries, deleted_count, store) do
    deleted_keys =
      Enum.map(deleted_entries, fn {compound_key, _value, _expire_at_ms} -> compound_key end)

    case Ops.compound_batch_delete(store, key, deleted_keys) do
      :ok ->
        case maybe_cleanup_empty_hash(key, deleted_count, store) do
          :ok -> :ok
          {:error, _} = error -> rollback_deleted_fields(key, deleted_entries, store, error)
        end

      {:error, _} = err ->
        err
    end
  end

  @spec put_entries([binary()], [binary()], [term()], map(), integer()) ::
          {non_neg_integer(), [{binary(), binary(), integer()}]}
  def put_entries(fields, compound_keys, existing_values, values_by_field, expire_at_ms) do
    put_entries(fields, compound_keys, existing_values, values_by_field, expire_at_ms, 0, [])
  end

  defp set_pairs(field_value_pairs, key, store, expire_at_ms, type_status) do
    {fields, values_by_field} = collapse_field_values(field_value_pairs, [], %{})
    compound_keys = Enum.map(fields, &CompoundKey.hash_field(key, &1))
    existing_values = Ops.compound_batch_get(store, key, compound_keys)
    {added, entries} = put_entries(fields, compound_keys, existing_values, values_by_field, expire_at_ms)

    case Ops.compound_batch_put(store, key, entries) do
      :ok -> added
      {:error, _} = err -> rollback_new_hash_type_marker(key, store, type_status, err)
    end
  end

  defp batch_field_metas(fields, key, store) do
    unique_fields = Enum.uniq(fields)
    compound_keys = Enum.map(unique_fields, &CompoundKey.hash_field(key, &1))
    metas = Ops.compound_batch_get_meta(store, key, compound_keys)
    metas_by_field = metas_by_field(unique_fields, metas, %{})
    {unique_fields, compound_keys, metas_by_field}
  end

  defp metas_by_field([field | fields], [meta | metas], acc) do
    metas_by_field(fields, metas, Map.put(acc, field, meta))
  end

  defp metas_by_field(_fields, _metas, acc), do: acc

  defp metas_by_key([compound_key | compound_keys], [meta | metas], acc) do
    metas_by_key(compound_keys, metas, Map.put(acc, compound_key, meta))
  end

  defp metas_by_key(_compound_keys, _metas, acc), do: acc

  defp getdel_results([], _key, _metas_by_key, results, _deleted, deleted_entries) do
    {Enum.reverse(results), Enum.reverse(deleted_entries)}
  end

  defp getdel_results([field | fields], key, metas_by_key, results, deleted, deleted_entries) do
    compound_key = CompoundKey.hash_field(key, field)

    cond do
      Map.has_key?(deleted, compound_key) ->
        getdel_results(fields, key, metas_by_key, [nil | results], deleted, deleted_entries)

      is_nil(Map.get(metas_by_key, compound_key)) ->
        getdel_results(fields, key, metas_by_key, [nil | results], deleted, deleted_entries)

      true ->
        {value, expire_at_ms} = Map.fetch!(metas_by_key, compound_key)

        getdel_results(
          fields,
          key,
          metas_by_key,
          [value | results],
          Map.put(deleted, compound_key, true),
          [{compound_key, value, expire_at_ms} | deleted_entries]
        )
    end
  end

  defp maybe_cleanup_empty_hash(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_hash(key, _deleted, store) do
    prefix = CompoundKey.hash_prefix(key)

    if Ops.compound_count(store, key, prefix) == 0 do
      TypeRegistry.delete_type(key, store)
    else
      :ok
    end
  end

  defp rollback_deleted_fields(_key, [], _store, write_error), do: write_error

  defp rollback_deleted_fields(key, deleted_entries, store, write_error) do
    case Ops.compound_batch_put(store, key, deleted_entries) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:hash_delete_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_hash_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.delete_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:hash_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_hash_type_marker(_key, _store, :ok, write_error), do: write_error

  defp collapse_field_values([], fields_rev, values_by_field) do
    {Enum.reverse(fields_rev), values_by_field}
  end

  defp collapse_field_values([field, value | rest], fields_rev, values_by_field) do
    next_fields_rev =
      if Map.has_key?(values_by_field, field) do
        fields_rev
      else
        [field | fields_rev]
      end

    collapse_field_values(rest, next_fields_rev, Map.put(values_by_field, field, value))
  end

  defp persistent_field_entries([field | fields], [compound_key | compound_keys], metas_by_field, acc) do
    next_acc =
      case Map.fetch!(metas_by_field, field) do
        {value, expire_at_ms} when expire_at_ms != 0 -> [{compound_key, value, 0} | acc]
        _nil_or_persistent -> acc
      end

    persistent_field_entries(fields, compound_keys, metas_by_field, next_acc)
  end

  defp persistent_field_entries(_fields, _compound_keys, _metas_by_field, acc),
    do: Enum.reverse(acc)

  defp existing_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms) do
    existing_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms, [])
  end

  defp existing_field_entries([field | fields], [compound_key | compound_keys], metas_by_field, expire_at_ms, acc) do
    next_acc =
      case Map.fetch!(metas_by_field, field) do
        {value, _old_expire} -> [{compound_key, value, expire_at_ms} | acc]
        nil -> acc
      end

    existing_field_entries(fields, compound_keys, metas_by_field, expire_at_ms, next_acc)
  end

  defp existing_field_entries(_fields, _compound_keys, _metas_by_field, _expire_at_ms, acc),
    do: Enum.reverse(acc)

  defp put_entries([], [], _existing_values, _values_by_field, _expire_at_ms, added, entries) do
    {added, Enum.reverse(entries)}
  end

  defp put_entries([field | fields], [compound_key | compound_keys], [nil | existing_values], values_by_field, expire_at_ms, added, entries) do
    entry = {compound_key, Map.fetch!(values_by_field, field), expire_at_ms}

    put_entries(
      fields,
      compound_keys,
      existing_values,
      values_by_field,
      expire_at_ms,
      added + 1,
      [entry | entries]
    )
  end

  defp put_entries([field | fields], [compound_key | compound_keys], [_existing | existing_values], values_by_field, expire_at_ms, added, entries) do
    entry = {compound_key, Map.fetch!(values_by_field, field), expire_at_ms}

    put_entries(
      fields,
      compound_keys,
      existing_values,
      values_by_field,
      expire_at_ms,
      added,
      [entry | entries]
    )
  end

  defp put_entries([field | fields], [compound_key | compound_keys], [], values_by_field, expire_at_ms, added, entries) do
    entry = {compound_key, Map.fetch!(values_by_field, field), expire_at_ms}

    put_entries(fields, compound_keys, [], values_by_field, expire_at_ms, added, [
      entry | entries
    ])
  end
end
