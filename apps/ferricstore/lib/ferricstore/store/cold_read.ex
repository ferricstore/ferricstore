defmodule Ferricstore.Store.ColdRead do
  @moduledoc """
  Helpers for synchronous callers that need to wait on Tokio cold-read NIFs.

  The NIF sends `{:tokio_complete, corr_id, ...}` to the pid passed at submit
  time. If a caller waits directly and times out, a late completion can remain
  in that caller's mailbox. These helpers submit through a short-lived proxy
  process, so late completions are consumed or dropped away from the caller.
  """

  alias Ferricstore.Bitcask.{Async, NIF}

  @type submit_fun :: (pid(), pos_integer() -> :ok | {:error, term()})
  @type result :: {:ok, term()} | {:error, term()}

  @doc false
  @spec await_tokio(submit_fun(), timeout()) :: result()
  def await_tokio(submit_fun, timeout_ms) do
    Async.await(submit_fun, timeout_ms)
  end

  @spec pread_at(binary(), non_neg_integer(), timeout()) :: result()
  def pread_at(path, offset, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        NIF.v2_pread_at_async(proxy, corr_id, path, offset)
      end,
      timeout_ms
    )
  end

  @spec pread_batch([{binary(), non_neg_integer()}], timeout()) :: result()
  def pread_batch([], _timeout_ms), do: {:ok, []}

  def pread_batch(locations, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        case pread_batch_submit_shape(locations) do
          {:single_path, path, offsets} ->
            NIF.v2_pread_batch_path_async(proxy, corr_id, path, offsets)

          {:grouped_paths, groups} ->
            NIF.v2_pread_batch_grouped_async(proxy, corr_id, groups)
        end
      end,
      timeout_ms
    )
  end

  @doc false
  @spec pread_batch_submit_shape([{binary(), non_neg_integer()}]) ::
          {:single_path, binary(), [non_neg_integer()]}
          | {:grouped_paths, [{binary(), [{non_neg_integer(), non_neg_integer()}]}]}
  def pread_batch_submit_shape([{path, offset} | rest] = locations) do
    same_path_offsets(rest, path, [offset], locations)
  end

  def pread_batch_submit_shape([]), do: {:grouped_paths, []}

  defp same_path_offsets([{path, offset} | rest], path, offsets, locations) do
    same_path_offsets(rest, path, [offset | offsets], locations)
  end

  defp same_path_offsets([], path, offsets, _locations) do
    {:single_path, path, Enum.reverse(offsets)}
  end

  defp same_path_offsets(_rest, _path, _offsets, locations),
    do: {:grouped_paths, group_paths(locations)}

  defp group_paths(locations) do
    {groups, order} =
      locations
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {{path, offset}, index}, {groups, order} ->
        order = if Map.has_key?(groups, path), do: order, else: [path | order]
        groups = Map.update(groups, path, [{index, offset}], &[{index, offset} | &1])
        {groups, order}
      end)

    order
    |> Enum.reverse()
    |> Enum.map(fn path -> {path, groups |> Map.fetch!(path) |> Enum.reverse()} end)
  end
end
