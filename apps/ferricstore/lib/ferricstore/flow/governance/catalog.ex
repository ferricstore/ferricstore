defmodule Ferricstore.Flow.Governance.Catalog do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  @page_size 256
  @kinds [:approval, :budget, :circuit, :limit]

  def register(_ctx, nil, _record_key), do: :ok

  def register(ctx, kind, record_key)
      when kind in @kinds and is_binary(record_key) do
    register_key(ctx, Keys.governance_catalog_key(kind), record_key)
  end

  def register_key(ctx, catalog_key, record_key)
      when is_binary(catalog_key) and is_binary(record_key) do
    register_keys(ctx, catalog_key, [record_key])
  end

  def register_keys(ctx, catalog_key, record_keys)
      when is_binary(catalog_key) and is_list(record_keys) and length(record_keys) <= @page_size do
    if Enum.all?(record_keys, &(is_binary(&1) and &1 != "")) do
      do_register_keys(ctx, catalog_key, Enum.uniq(record_keys))
    else
      {:error, "ERR invalid flow governance catalog registration"}
    end
  end

  def register_keys(_ctx, _catalog_key, _record_keys),
    do: {:error, "ERR invalid flow governance catalog registration"}

  defp do_register_keys(_ctx, _catalog_key, []), do: :ok

  defp do_register_keys(ctx, catalog_key, record_keys) do
    entries = Enum.map(record_keys, &{0, &1})

    case FerricStore.Impl.zadd(ctx, catalog_key, entries) do
      {:ok, _added} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def unregister_key(ctx, catalog_key, record_key)
      when is_binary(catalog_key) and is_binary(record_key) do
    case FerricStore.Impl.zrem(ctx, catalog_key, [record_key]) do
      {:ok, _removed} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def member?(ctx, catalog_key, record_key)
      when is_binary(catalog_key) and is_binary(record_key) do
    case FerricStore.Impl.zscore(ctx, catalog_key, record_key) do
      {:ok, nil} -> {:ok, false}
      {:ok, _score} -> {:ok, true}
      {:error, _reason} = error -> error
    end
  end

  def collect(ctx, kind, limit, loader, sort_by, direction \\ :asc)

  def collect(ctx, kind, limit, loader, sort_by, direction)
      when kind in @kinds and is_integer(limit) and limit > 0 and is_function(loader, 1) and
             is_function(sort_by, 1) and direction in [:asc, :desc] do
    collect_key(ctx, Keys.governance_catalog_key(kind), limit, loader, sort_by, direction)
  end

  def collect_key(ctx, catalog_key, limit, loader, sort_by, direction \\ :asc)

  def collect_key(ctx, catalog_key, limit, loader, sort_by, direction)
      when is_binary(catalog_key) and is_integer(limit) and limit > 0 and is_function(loader, 1) and
             is_function(sort_by, 1) and direction in [:asc, :desc] do
    do_collect_key(ctx, catalog_key, nil, [], limit, loader, sort_by, direction)
  end

  def collect_keys(keys, limit, loader, sort_by, direction \\ :asc)

  def collect_keys(keys, limit, loader, sort_by, direction)
      when is_list(keys) and is_integer(limit) and limit > 0 and is_function(loader, 1) and
             is_function(sort_by, 1) and direction in [:asc, :desc] do
    collect_keys_page(keys, [], limit, loader, sort_by, direction)
  end

  def reduce_pages(ctx, kind, acc, reducer)
      when kind in @kinds and is_function(reducer, 2) do
    reduce_key_pages(ctx, Keys.governance_catalog_key(kind), acc, reducer)
  end

  def reduce_key_pages(ctx, catalog_key, acc, reducer)
      when is_binary(catalog_key) and is_function(reducer, 2) do
    do_reduce_pages(ctx, catalog_key, 0, acc, reducer)
  end

  def page(ctx, kind, cursor, limit)
      when kind in @kinds and (is_nil(cursor) or is_binary(cursor)) and is_integer(limit) and
             limit > 0 and limit <= @page_size do
    page_key(ctx, Keys.governance_catalog_key(kind), cursor, limit)
  end

  def page_key(ctx, catalog_key, cursor, limit)
      when is_binary(catalog_key) and (is_nil(cursor) or is_binary(cursor)) and is_integer(limit) and
             limit > 0 and limit <= @page_size do
    with {:ok, start} <- page_start(ctx, catalog_key, cursor),
         {:ok, members} <- FerricStore.Impl.zrange(ctx, catalog_key, start, start + limit - 1, []),
         true <- is_list(members) do
      keys = Enum.filter(members, &is_binary/1)
      next_cursor = if length(members) == limit, do: List.last(members), else: nil
      {:ok, %{keys: keys, next_cursor: next_cursor}}
    else
      false -> {:error, "ERR flow governance catalog is corrupt"}
      {:error, _reason} = error -> error
    end
  end

  defp do_reduce_pages(ctx, catalog_key, offset, acc, reducer) do
    stop = offset + @page_size - 1

    case FerricStore.Impl.zrange(ctx, catalog_key, offset, stop, []) do
      {:ok, []} ->
        {:ok, acc}

      {:ok, members} when is_list(members) ->
        member_count = length(members)
        keys = Enum.filter(members, &is_binary/1)
        next_acc = reducer.(keys, acc)

        if member_count < @page_size do
          {:ok, next_acc}
        else
          continue_after_member(ctx, catalog_key, List.last(members), next_acc, reducer)
        end

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, "ERR flow governance catalog is corrupt"}
    end
  end

  defp continue_after_member(ctx, catalog_key, member, acc, reducer) when is_binary(member) do
    case FerricStore.Impl.zrank(ctx, catalog_key, member) do
      {:ok, rank} when is_integer(rank) and rank >= 0 ->
        do_reduce_pages(ctx, catalog_key, rank + 1, acc, reducer)

      {:ok, nil} ->
        {:error, "ERR flow governance catalog changed during traversal"}

      {:error, _reason} = error ->
        error
    end
  end

  defp continue_after_member(_ctx, _catalog_key, _member, _acc, _reducer),
    do: {:error, "ERR flow governance catalog is corrupt"}

  defp page_start(_ctx, _catalog_key, nil), do: {:ok, 0}

  defp page_start(ctx, catalog_key, cursor) do
    case FerricStore.Impl.zrank(ctx, catalog_key, cursor) do
      {:ok, rank} when is_integer(rank) and rank >= 0 -> {:ok, rank + 1}
      {:ok, nil} -> {:error, "ERR flow governance catalog changed during traversal"}
      {:error, _reason} = error -> error
    end
  end

  defp do_collect_key(ctx, catalog_key, cursor, acc, limit, loader, sort_by, direction) do
    with {:ok, %{keys: keys, next_cursor: next_cursor}} <-
           page_key(ctx, catalog_key, cursor, @page_size),
         {:ok, acc} <- collect_keys_page(keys, acc, limit, loader, sort_by, direction) do
      if is_nil(next_cursor) do
        {:ok, acc}
      else
        do_collect_key(ctx, catalog_key, next_cursor, acc, limit, loader, sort_by, direction)
      end
    end
  end

  defp collect_keys_page(keys, acc, limit, loader, sort_by, direction) do
    keys
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, records} ->
      case loader.(key) do
        {:ok, record} -> {:cont, {:ok, [record | records]}}
        :skip -> {:cont, {:ok, records}}
        {:error, _reason} = error -> {:halt, error}
        _invalid -> {:halt, {:error, "ERR flow governance catalog loader is invalid"}}
      end
    end)
    |> case do
      {:ok, page} ->
        {:ok,
         page
         |> Kernel.++(acc)
         |> Enum.sort_by(sort_by, direction)
         |> Enum.take(limit)}

      {:error, _reason} = error ->
        error
    end
  end
end
