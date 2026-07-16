defmodule Mix.Tasks.Ferricstore.Keys do
  @moduledoc """
  Lists keys stored in FerricStore, with optional glob pattern filtering.

  ## Usage

      mix ferricstore.keys [pattern]

  ## Arguments

    * `pattern` (optional) -- a glob pattern to filter keys. Supports `*`
      (match any sequence of characters) and `?` (match exactly one character).
      When omitted, all keys are listed.

  ## Examples

      # List all keys
      mix ferricstore.keys

      # List keys matching a prefix
      mix ferricstore.keys "user:*"

      # List keys with a single-char wildcard
      mix ferricstore.keys "k?"

  ## Output

  Prints each matching key on its own line, followed by a summary count.
  """

  use Mix.Task

  @shortdoc "List keys with optional glob pattern filtering"
  @scan_page_size 1_000

  @doc """
  Runs the keys task, listing keys that match the given pattern.

  ## Parameters

    * `args` -- list of command-line arguments. The first element, if present,
      is treated as a glob pattern.

  """
  @spec run(list()) :: :ok
  @impl Mix.Task
  def run(args) do
    ensure_started()

    pattern = parse_pattern!(args)
    ctx = FerricStore.Instance.get(:default)
    scan_page = &Ferricstore.Store.Router.scan_keys_page(ctx, &1, &2, &3, nil)
    count = stream_keys(scan_page, fn key -> Mix.shell().info(key) end, pattern, @scan_page_size)

    if pattern do
      Mix.shell().info("\n#{count} key(s) matching \"#{pattern}\"")
    else
      Mix.shell().info("\n#{count} key(s) total")
    end

    :ok
  end

  @doc false
  @spec stream_keys(
          (binary(), pos_integer(), binary() | nil ->
             {:ok, {binary(), [binary()]}} | {:error, term()}),
          (binary() -> term()),
          binary() | nil,
          pos_integer()
        ) :: non_neg_integer()
  def stream_keys(scan_page, emit, pattern, page_size)
      when is_function(scan_page, 3) and is_function(emit, 1) and
             (is_binary(pattern) or is_nil(pattern)) and is_integer(page_size) and page_size > 0 do
    stream_keys_page("0", scan_page, emit, pattern, page_size, 0)
  end

  defp stream_keys_page(cursor, scan_page, emit, pattern, page_size, count) do
    case scan_page.(cursor, page_size, pattern) do
      {:ok, {next_cursor, keys}}
      when is_binary(next_cursor) and is_list(keys) ->
        unless Enum.all?(keys, &is_binary/1) do
          Mix.raise("key scan returned a non-binary key: #{inspect(keys)}")
        end

        Enum.each(keys, emit)
        count = count + length(keys)

        cond do
          next_cursor == "0" ->
            count

          next_cursor == cursor ->
            Mix.raise("scan cursor did not advance from #{inspect(cursor)}")

          true ->
            stream_keys_page(next_cursor, scan_page, emit, pattern, page_size, count)
        end

      {:error, reason} ->
        Mix.raise("failed to scan keys: #{inspect(reason)}")

      other ->
        Mix.raise("invalid key scan response: #{inspect(other)}")
    end
  end

  defp ensure_started do
    Mix.Task.run("app.start")
  end

  defp parse_pattern!([]), do: nil
  defp parse_pattern!([pattern]) when is_binary(pattern), do: pattern
  defp parse_pattern!(_args), do: Mix.raise("usage: mix ferricstore.keys [pattern]")
end
