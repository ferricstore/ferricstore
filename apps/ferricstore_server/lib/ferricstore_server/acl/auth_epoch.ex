defmodule FerricstoreServer.Acl.AuthEpoch do
  @moduledoc false

  @global_prefix "# FerricStore ACL auth epoch "
  @user_prefix "# FerricStore ACL user auth epoch "
  @max_epoch_digits 20

  @spec global_metadata(non_neg_integer()) :: binary()
  def global_metadata(epoch) when is_integer(epoch) and epoch >= 0 do
    @global_prefix <> Integer.to_string(epoch)
  end

  @spec user_metadata(binary(), map()) :: binary()
  def user_metadata(username, user) do
    encoded_username = Base.url_encode64(username, padding: false)
    epoch = Map.get(user, :auth_epoch, 0)
    @user_prefix <> encoded_username <> " " <> Integer.to_string(epoch)
  end

  @spec restore(binary(), [{binary(), map()}], non_neg_integer()) ::
          {:ok, [{binary(), map()}], non_neg_integer()} | {:error, binary()}
  def restore(contents, users, live_epoch) do
    with {:ok, persisted_epoch, user_epochs} <- parse_metadata(contents) do
      starting_epoch = max(live_epoch, max(persisted_epoch, max_user_epoch(user_epochs)))

      {users, final_epoch} =
        users
        |> Enum.sort_by(fn {username, _user} -> username end)
        |> Enum.map_reduce(starting_epoch, fn {username, user}, epoch ->
          case Map.fetch(user_epochs, username) do
            {:ok, user_epoch} ->
              {{username, Map.put(user, :auth_epoch, user_epoch)}, max(epoch, user_epoch)}

            :error ->
              next_epoch = epoch + 1
              {{username, Map.put(user, :auth_epoch, next_epoch)}, next_epoch}
          end
        end)

      {:ok, users, final_epoch}
    end
  end

  @spec rebase([{binary(), map()}], non_neg_integer()) ::
          {[{binary(), map()}], non_neg_integer()}
  def rebase(users, live_epoch) do
    users
    |> Enum.sort_by(fn {username, _user} -> username end)
    |> Enum.map_reduce(live_epoch, fn {username, user}, epoch ->
      next_epoch = epoch + 1
      {{username, Map.put(user, :auth_epoch, next_epoch)}, next_epoch}
    end)
  end

  defp parse_metadata(contents) do
    contents
    |> String.split(~r/\r?\n/)
    |> Enum.reduce_while({:ok, 0, %{}}, fn line, {:ok, global_epoch, user_epochs} ->
      cond do
        String.starts_with?(line, @global_prefix) ->
          value = String.replace_prefix(line, @global_prefix, "")

          case parse_epoch(value) do
            {:ok, epoch} -> {:cont, {:ok, max(global_epoch, epoch), user_epochs}}
            :error -> {:halt, {:error, "ERR Invalid ACL auth epoch metadata"}}
          end

        String.starts_with?(line, @user_prefix) ->
          metadata = String.replace_prefix(line, @user_prefix, "")

          with [encoded_username, value] <- String.split(metadata, " ", parts: 2),
               {:ok, username} <- Base.url_decode64(encoded_username, padding: false),
               {:ok, epoch} <- parse_epoch(value) do
            {:cont, {:ok, global_epoch, Map.put(user_epochs, username, epoch)}}
          else
            _other -> {:halt, {:error, "ERR Invalid ACL user auth epoch metadata"}}
          end

        true ->
          {:cont, {:ok, global_epoch, user_epochs}}
      end
    end)
  end

  defp parse_epoch(value) when byte_size(value) <= @max_epoch_digits do
    case Integer.parse(value) do
      {epoch, ""} when epoch >= 0 -> {:ok, epoch}
      _other -> :error
    end
  end

  defp parse_epoch(_value), do: :error

  defp max_user_epoch(user_epochs) do
    user_epochs
    |> Map.values()
    |> Enum.max(fn -> 0 end)
  end
end
