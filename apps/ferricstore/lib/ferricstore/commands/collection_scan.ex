defmodule Ferricstore.Commands.CollectionScan do
  @moduledoc false

  alias Ferricstore.Store.{Ops, ReadResult}

  @cursor_prefix "~"
  @max_cursor_member_bytes 65_535
  @max_cursor_token_bytes 87_384
  @max_page_count 10_000

  @type cursor :: 0 | {:after, binary()}

  @spec parse_cursor(binary()) :: {:ok, cursor()} | {:error, binary()}
  def parse_cursor("0"), do: {:ok, 0}

  def parse_cursor(<<@cursor_prefix, encoded::binary>> = token)
      when byte_size(token) <= @max_cursor_token_bytes do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, member} when byte_size(member) <= @max_cursor_member_bytes ->
        {:ok, {:after, member}}

      {:ok, _oversized} ->
        {:error, "ERR invalid cursor"}

      :error ->
        {:error, "ERR invalid cursor"}
    end
  end

  def parse_cursor(_cursor), do: {:error, "ERR invalid cursor"}

  @spec valid_cursor?(term()) :: boolean()
  def valid_cursor?(0), do: true

  def valid_cursor?({:after, member})
      when is_binary(member) and byte_size(member) <= @max_cursor_member_bytes,
      do: true

  def valid_cursor?(_cursor), do: false

  @spec encode_cursor(cursor()) :: binary()
  def encode_cursor(0), do: "0"

  def encode_cursor({:after, member}) when is_binary(member),
    do: @cursor_prefix <> Base.url_encode64(member, padding: false)

  @spec page(
          term(),
          binary(),
          binary(),
          cursor(),
          pos_integer(),
          binary() | nil,
          boolean()
        ) :: {:ok, {binary(), [{binary(), binary() | nil}]}} | {:error, binary()}
  def page(_store, _key, _prefix, _cursor, count, _match_pattern, _fields_only)
      when not is_integer(count) or count <= 0 or count > @max_page_count,
      do: {:error, "ERR value is not an integer or out of range"}

  def page(store, key, prefix, cursor, count, match_pattern, fields_only) do
    case Ops.compound_scan_page(
           store,
           key,
           prefix,
           cursor,
           count,
           match_pattern,
           fields_only
         ) do
      {:ok, {next_cursor, pairs}} when is_list(pairs) ->
        if length(pairs) <= count do
          if valid_cursor?(next_cursor) do
            {:ok, {encode_cursor(next_cursor), pairs}}
          else
            {:error, "ERR storage read failed"}
          end
        else
          {:error, "ERR storage read failed"}
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      _invalid ->
        {:error, "ERR storage read failed"}
    end
  end
end
