defmodule Ferricstore.Flow.HistoryProjectedIndex do
  @moduledoc false

  @filename "flow_history_projected.index"

  @spec path(binary()) :: binary()
  def path(shard_data_path), do: Path.join(shard_data_path, @filename)

  @spec read(binary()) :: non_neg_integer()
  def read(shard_data_path) do
    shard_data_path
    |> path()
    |> File.read()
    |> case do
      {:ok, bin} ->
        bin
        |> String.trim()
        |> Integer.parse()
        |> case do
          {idx, ""} when idx >= 0 -> idx
          _ -> 0
        end

      _ ->
        0
    end
  end

  @spec persist(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def persist(shard_data_path, index) when is_integer(index) and index >= 0 do
    target = path(shard_data_path)
    tmp = target <> ".tmp"

    with :ok <- File.mkdir_p(shard_data_path),
         :ok <- File.write(tmp, Integer.to_string(index)),
         :ok <- File.rename(tmp, target) do
      :ok
    end
  end
end
