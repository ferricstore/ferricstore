defmodule FerricstoreServer.Acl.Limits do
  @moduledoc false

  @default_max_users 10_000
  @max_users 100_000
  @file_line_slack 16
  @max_file_line_bytes 2 * 1024 * 1024

  @spec default_max_users() :: pos_integer()
  def default_max_users, do: @default_max_users

  @spec max_users() :: pos_integer()
  def max_users, do: @max_users

  @spec max_file_line_bytes() :: pos_integer()
  def max_file_line_bytes, do: @max_file_line_bytes

  @spec max_file_lines(non_neg_integer()) :: pos_integer()
  def max_file_lines(max_users) when is_integer(max_users) and max_users >= 0,
    do: max_users * 2 + @file_line_slack

  @spec configured_max_users() :: {:ok, pos_integer()} | {:error, binary()}
  def configured_max_users do
    case Application.get_env(:ferricstore, :max_acl_users, @default_max_users) do
      max_users when is_integer(max_users) and max_users >= 1 and max_users <= @max_users ->
        {:ok, max_users}

      _invalid ->
        {:error, "ERR invalid max ACL users configuration"}
    end
  end
end
