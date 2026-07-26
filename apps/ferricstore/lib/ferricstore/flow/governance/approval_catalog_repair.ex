defmodule Ferricstore.Flow.Governance.ApprovalCatalogRepair do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

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
          with {:ok, last_retained} <- reconcile_source(ctx, keys, source_targets) do
            persist_source_progress(
              ctx,
              progress_key,
              cursor,
              next_cursor,
              last_retained,
              persisted?
            )
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
    source_catalog_key = Keys.governance_catalog_key(:approval)

    keys
    |> Enum.reduce_while({:ok, nil, []}, fn key, {:ok, last_retained, missing_keys} ->
      case source_targets.(key) do
        {:ok, catalog_keys} when is_list(catalog_keys) ->
          case ensure_catalogs_present(ctx, key, catalog_keys) do
            :ok -> {:cont, {:ok, key, missing_keys}}
            {:error, _reason} = error -> {:halt, error}
          end

        :missing ->
          {:cont, {:ok, last_retained, [key | missing_keys]}}

        :skip ->
          {:cont, {:ok, key, missing_keys}}

        {:error, _reason} = error ->
          {:halt, error}

        _invalid ->
          {:halt, {:error, "ERR flow approval catalog repair source is invalid"}}
      end
    end)
    |> case do
      {:ok, last_retained, missing_keys} ->
        missing_keys = Enum.reverse(missing_keys)

        revalidate = &revalidate_source_membership(ctx, &1, source_targets)

        with :ok <-
               batch_remove_and_revalidate(ctx, source_catalog_key, missing_keys, revalidate) do
          {:ok, last_retained}
        end

      {:error, _reason} = error ->
        error
    end
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

  defp reconcile_target(ctx, target_catalog_key, keys, matcher, last_retained) do
    keys
    |> Enum.reduce_while({:ok, last_retained, []}, fn key, {:ok, current_last, stale_keys} ->
      case matcher.(key) do
        true ->
          {:cont, {:ok, key, stale_keys}}

        false ->
          {:cont, {:ok, current_last, [key | stale_keys]}}

        {:error, _reason} = error ->
          {:halt, error}

        _invalid ->
          {:halt, {:error, "ERR flow approval catalog repair matcher is invalid"}}
      end
    end)
    |> case do
      {:ok, next_last_retained, stale_keys} ->
        stale_keys = Enum.reverse(stale_keys)

        revalidate = &revalidate_target_membership(&1, matcher)

        with :ok <-
               batch_remove_and_revalidate(ctx, target_catalog_key, stale_keys, revalidate) do
          {:ok, next_last_retained}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp batch_remove_and_revalidate(ctx, catalog_key, keys, revalidate) do
    with :ok <- Catalog.unregister_keys(ctx, catalog_key, keys) do
      Enum.reduce_while(keys, :ok, fn key, :ok ->
        operation =
          case revalidate.(key) do
            :stale ->
              :ok

            :restore ->
              Catalog.register_key(ctx, catalog_key, key)

            {:error, _reason} = error ->
              restore_membership_before_error(ctx, catalog_key, key, error)
          end

        case operation do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp revalidate_source_membership(ctx, key, source_targets) do
    case source_targets.(key) do
      :missing ->
        :stale

      {:ok, catalog_keys} when is_list(catalog_keys) ->
        case ensure_catalogs_present(ctx, key, catalog_keys) do
          :ok -> :restore
          {:error, _reason} = error -> error
        end

      :skip ->
        :restore

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, "ERR flow approval catalog repair source is invalid"}
    end
  end

  defp revalidate_target_membership(key, matcher) do
    case matcher.(key) do
      true -> :restore
      false -> :stale
      {:error, _reason} = error -> error
      _invalid -> {:error, "ERR flow approval catalog repair matcher is invalid"}
    end
  end

  defp restore_membership_before_error(ctx, catalog_key, key, error) do
    case Catalog.register_key(ctx, catalog_key, key) do
      :ok -> error
      {:error, _reason} = restore_error -> restore_error
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

  defp persist_source_progress(ctx, progress_key, _cursor, nil, _last_retained, true),
    do: Router.delete(ctx, progress_key)

  defp persist_source_progress(_ctx, _progress_key, _cursor, nil, _last_retained, false),
    do: :ok

  defp persist_source_progress(
         ctx,
         progress_key,
         cursor,
         next_cursor,
         last_retained,
         _persisted?
       )
       when is_binary(next_cursor) do
    persist_progress(ctx, progress_key, :source, last_retained || cursor)
  end

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
    if valid_cursor?(cursor) do
      encoded = TermCodec.encode({@progress_tag, identity, cursor})
      Router.put(ctx, progress_key, encoded, 0)
    else
      {:error, "ERR flow approval catalog repair progress is invalid"}
    end
  end

  defp decode_progress(value, identity) do
    case TermCodec.decode(value) do
      {:ok, {@progress_tag, ^identity, cursor}} ->
        if valid_cursor?(cursor) do
          {:ok, cursor}
        else
          {:error, "ERR flow approval catalog repair progress is corrupt"}
        end

      _invalid ->
        {:error, "ERR flow approval catalog repair progress is corrupt"}
    end
  end

  defp valid_cursor?(nil), do: true

  defp valid_cursor?(cursor) when is_binary(cursor),
    do: byte_size(cursor) <= Router.max_key_size()

  defp valid_cursor?(_cursor), do: false
end
