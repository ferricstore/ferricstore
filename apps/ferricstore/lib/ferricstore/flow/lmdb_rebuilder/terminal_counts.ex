defmodule Ferricstore.Flow.LMDBRebuilder.TerminalCounts do
  @moduledoc false

  alias Ferricstore.Flow.LMDB

  @default_scan_page_size 4_096
  @max_scan_page_size 65_536

  def persist(stats, lmdb_path) when is_map(stats) and is_binary(lmdb_path) do
    if not Ferricstore.FS.dir?(lmdb_path) do
      stats
    else
      case rebuild_from_index(lmdb_path) do
        {:ok, count_key_count} ->
          Map.put(stats, :terminal_count_keys, count_key_count)

        {:error, _reason} ->
          %{stats | lmdb_errors: stats.lmdb_errors + 1}
      end
    end
  end

  defp rebuild_from_index(lmdb_path) do
    page_size = reconcile_page_size()

    with :ok <- delete_existing(lmdb_path, page_size),
         {:ok, state} <- scan_terminal_index(lmdb_path, page_size) do
      finish_count(lmdb_path, state)
    end
  end

  defp delete_existing(lmdb_path, page_size) do
    scan_existing_pages(lmdb_path, page_size, fn entries ->
      entries
      |> Enum.map(fn {key, _value} -> {:delete, key} end)
      |> then(&write_batch(&1, fn ops -> LMDB.write_batch(lmdb_path, ops) end))
    end)
  end

  defp scan_terminal_index(lmdb_path, page_size) do
    LMDB.reduce_prefix_entries(
      lmdb_path,
      LMDB.terminal_index_global_prefix(),
      page_size,
      {nil, 0, 0},
      fn entries, state ->
        with {:ok, next_state, ops} <- page_count_ops(entries, state),
             :ok <- LMDB.write_batch(lmdb_path, ops) do
          {:ok, next_state}
        end
      end
    )
  end

  @doc false
  def __page_count_ops_for_test__(entries, state), do: page_count_ops(entries, state)

  defp page_count_ops(entries, state) when is_list(entries) do
    entries
    |> Enum.reduce_while({:ok, state, []}, fn entry, {:ok, current, reversed_ops} ->
      case entry_count_key(entry) do
        {:ok, count_key} ->
          case advance_count(current, count_key) do
            {:ok, next, nil} ->
              {:cont, {:ok, next, reversed_ops}}

            {:ok, next, op} ->
              {:cont, {:ok, next, [op | reversed_ops]}}

            {:error, _reason} = error ->
              {:halt, error}
          end

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, next, reversed_ops} -> {:ok, next, Enum.reverse(reversed_ops)}
      {:error, _reason} = error -> error
    end
  end

  defp page_count_ops(_entries, _state), do: {:error, :invalid_terminal_index_count_page}

  defp advance_count({nil, 0, count_key_count}, count_key)
       when is_binary(count_key) and is_integer(count_key_count) and count_key_count >= 0,
       do: {:ok, {count_key, 1, count_key_count}, nil}

  defp advance_count({count_key, count, count_key_count}, count_key)
       when is_binary(count_key) and is_integer(count) and count > 0 and
              is_integer(count_key_count) and count_key_count >= 0,
       do: {:ok, {count_key, count + 1, count_key_count}, nil}

  defp advance_count({current_key, count, count_key_count}, next_key)
       when is_binary(current_key) and is_binary(next_key) and next_key > current_key and
              is_integer(count) and count > 0 and is_integer(count_key_count) and
              count_key_count >= 0 do
    op = {:put, current_key, LMDB.encode_count(count)}
    {:ok, {next_key, 1, count_key_count + 1}, op}
  end

  defp advance_count(_state, _count_key),
    do: {:error, :noncontiguous_terminal_index_count_group}

  defp entry_count_key({terminal_key, value})
       when is_binary(terminal_key) and is_binary(value) do
    with {:ok, {id, updated_at_ms, _expire_at_ms, _state_key, count_key}} <-
           LMDB.decode_terminal_index_entry(value),
         true <- LMDB.terminal_index_entry_key?(terminal_key, id, updated_at_ms),
         true <- owns_count_key?(terminal_key, count_key) do
      {:ok, count_key}
    else
      _invalid -> {:error, :invalid_terminal_index_count_entry}
    end
  end

  defp entry_count_key(_invalid), do: {:error, :invalid_terminal_index_count_entry}

  defp owns_count_key?(terminal_key, count_key) do
    count_prefix = LMDB.terminal_count_prefix()

    if String.starts_with?(count_key, count_prefix) and
         byte_size(count_key) > byte_size(count_prefix) do
      component =
        binary_part(
          count_key,
          byte_size(count_prefix),
          byte_size(count_key) - byte_size(count_prefix)
        )

      String.starts_with?(terminal_key, LMDB.terminal_index_global_prefix() <> component <> <<0>>)
    else
      false
    end
  end

  defp finish_count(_lmdb_path, {nil, 0, count_key_count}), do: {:ok, count_key_count}

  defp finish_count(lmdb_path, {count_key, count, count_key_count})
       when is_binary(count_key) and is_integer(count) and count > 0 do
    case LMDB.write_batch(lmdb_path, [{:put, count_key, LMDB.encode_count(count)}]) do
      :ok -> {:ok, count_key_count + 1}
      {:error, _reason} = error -> error
      invalid -> {:error, {:invalid_terminal_count_write, invalid}}
    end
  end

  defp finish_count(_lmdb_path, _invalid), do: {:error, :invalid_terminal_index_count_state}

  defp scan_existing_pages(lmdb_path, page_size, page_fun) do
    LMDB.reduce_prefix_entries(
      lmdb_path,
      LMDB.terminal_count_prefix(),
      page_size,
      :ok,
      fn entries, :ok ->
        case page_fun.(entries) do
          :ok -> {:ok, :ok}
          {:error, _reason} = error -> error
          invalid -> {:error, {:invalid_terminal_count_page_result, invalid}}
        end
      end
    )
    |> case do
      {:ok, :ok} -> :ok
      {:error, _reason} = error -> error
      invalid -> {:error, {:invalid_terminal_count_scan, invalid}}
    end
  end

  defp write_batch([], _write_fun), do: :ok

  defp write_batch(ops, write_fun) do
    case write_fun.(ops) do
      :ok -> :ok
      {:error, _reason} = error -> error
      invalid -> {:error, {:invalid_terminal_count_write, invalid}}
    end
  end

  defp reconcile_page_size do
    :ferricstore
    |> Application.get_env(
      :flow_lmdb_rebuild_count_key_page_size,
      @default_scan_page_size
    )
    |> normalize_page_size()
  end

  defp normalize_page_size(value) when is_integer(value) and value > 0,
    do: min(value, @max_scan_page_size)

  defp normalize_page_size(_invalid), do: @default_scan_page_size
end
