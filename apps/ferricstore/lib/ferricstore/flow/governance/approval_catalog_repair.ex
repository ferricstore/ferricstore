defmodule Ferricstore.Flow.Governance.ApprovalCatalogRepair do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  @page_size 64
  @progress_tag :flow_governance_approval_catalog_repair_v1
  @catalog_changed "ERR flow governance catalog changed during traversal"

  @doc false
  def page_size, do: @page_size

  @doc false
  def source_progress_key,
    do: Keys.governance_catalog_key(:approval) <> ":repair:source:v1"

  @doc false
  def target_progress_key(target_catalog_key) when is_binary(target_catalog_key),
    do: target_catalog_key <> ":repair:target:v1"

  def mark_dirty(ctx) do
    persist_progress(ctx, source_progress_key(), :source, nil)
  end

  def step(ctx, target_catalog_key, matcher, source_targets)
      when is_binary(target_catalog_key) and is_function(matcher, 1) and
             is_function(source_targets, 1) do
    with :ok <- step_source(ctx, source_targets),
         :ok <- step_target(ctx, target_catalog_key, matcher) do
      :ok
    end
  end

  defp step_source(ctx, source_targets) do
    progress_key = source_progress_key()

    with {:ok, {cursor, persisted?}} <- load_progress(ctx, progress_key, :source) do
      case Catalog.page(ctx, :approval, cursor, @page_size) do
        {:ok, %{keys: keys, next_cursor: next_cursor}} ->
          with :ok <- reconcile_source(ctx, keys, source_targets) do
            persist_source_progress(ctx, progress_key, next_cursor, persisted?)
          end

        {:error, @catalog_changed} ->
          if persisted?, do: Router.delete(ctx, progress_key), else: :ok

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp step_target(ctx, target_catalog_key, matcher) do
    progress_key = target_progress_key(target_catalog_key)

    with {:ok, {cursor, persisted?}} <-
           load_progress(ctx, progress_key, {:target, target_catalog_key}) do
      case Catalog.page_key(ctx, target_catalog_key, cursor, @page_size) do
        {:ok, %{keys: [], next_cursor: nil}} ->
          if persisted?, do: Router.delete(ctx, progress_key), else: :ok

        {:ok, %{keys: keys, next_cursor: next_cursor}} ->
          with {:ok, last_retained} <-
                 reconcile_target(ctx, target_catalog_key, keys, matcher, nil) do
            persist_target_progress(
              ctx,
              progress_key,
              target_catalog_key,
              cursor,
              next_cursor,
              last_retained
            )
          end

        {:error, @catalog_changed} ->
          persist_progress(ctx, progress_key, {:target, target_catalog_key}, nil)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp reconcile_source(ctx, keys, source_targets) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      operation =
        case source_targets.(key) do
          {:ok, catalog_keys} when is_list(catalog_keys) ->
            ensure_catalogs_present(ctx, key, catalog_keys)

          :skip ->
            :ok

          _invalid ->
            {:error, "ERR flow approval catalog repair source is invalid"}
        end

      case operation do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp ensure_catalogs_present(ctx, key, catalog_keys) do
    Enum.reduce_while(catalog_keys, :ok, fn
      catalog_key, :ok when is_binary(catalog_key) ->
        case ensure_present(ctx, catalog_key, key) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      _invalid, _acc ->
        {:halt, {:error, "ERR flow approval catalog repair source is invalid"}}
    end)
  end

  defp reconcile_target(_ctx, _target_catalog_key, [], _matcher, last_retained),
    do: {:ok, last_retained}

  defp reconcile_target(ctx, target_catalog_key, [key | rest], matcher, last_retained) do
    case matcher.(key) do
      true ->
        reconcile_target(ctx, target_catalog_key, rest, matcher, key)

      false ->
        case Catalog.unregister_key(ctx, target_catalog_key, key) do
          :ok -> reconcile_target(ctx, target_catalog_key, rest, matcher, last_retained)
          {:error, _reason} = error -> error
        end

      _invalid ->
        {:error, "ERR flow approval catalog repair matcher is invalid"}
    end
  end

  defp ensure_present(ctx, target_catalog_key, key) do
    case Catalog.member?(ctx, target_catalog_key, key) do
      {:ok, true} -> :ok
      {:ok, false} -> Catalog.register_key(ctx, target_catalog_key, key)
      {:error, _reason} = error -> error
    end
  end

  defp persist_target_progress(
         ctx,
         progress_key,
         target_catalog_key,
         cursor,
         next_cursor,
         last_retained
       ) do
    cond do
      is_nil(next_cursor) and is_nil(last_retained) ->
        Router.delete(ctx, progress_key)

      is_nil(next_cursor) ->
        persist_progress(ctx, progress_key, {:target, target_catalog_key}, nil)

      true ->
        persist_progress(
          ctx,
          progress_key,
          {:target, target_catalog_key},
          last_retained || cursor
        )
    end
  end

  defp persist_source_progress(ctx, progress_key, nil, true),
    do: Router.delete(ctx, progress_key)

  defp persist_source_progress(_ctx, _progress_key, nil, false), do: :ok

  defp persist_source_progress(ctx, progress_key, next_cursor, _persisted?)
       when is_binary(next_cursor),
       do: persist_progress(ctx, progress_key, :source, next_cursor)

  defp load_progress(ctx, progress_key, identity) do
    case Router.get(ctx, progress_key) do
      nil ->
        {:ok, {nil, false}}

      value when is_binary(value) ->
        with {:ok, cursor} <- decode_progress(value, identity) do
          {:ok, {cursor, true}}
        end

      _invalid ->
        {:error, "ERR flow approval catalog repair progress is corrupt"}
    end
  end

  defp persist_progress(ctx, progress_key, identity, cursor) do
    encoded = :erlang.term_to_binary({@progress_tag, identity, cursor})
    Router.put(ctx, progress_key, encoded, 0)
  end

  defp decode_progress(value, identity) do
    case :erlang.binary_to_term(value, [:safe]) do
      {@progress_tag, ^identity, cursor} when is_nil(cursor) or is_binary(cursor) ->
        {:ok, cursor}

      _invalid ->
        {:error, "ERR flow approval catalog repair progress is corrupt"}
    end
  rescue
    _error -> {:error, "ERR flow approval catalog repair progress is corrupt"}
  end
end
