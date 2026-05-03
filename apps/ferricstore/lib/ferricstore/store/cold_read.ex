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
    |> emit_pread_error(path)
  end

  @spec pread_at(binary(), non_neg_integer(), binary(), timeout()) :: result()
  def pread_at(path, offset, expected_key, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        NIF.v2_pread_at_key_async(proxy, corr_id, path, offset, expected_key)
      end,
      timeout_ms
    )
    |> emit_pread_error(path)
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

  @spec pread_batch_keyed([{binary(), non_neg_integer(), binary()}], timeout()) :: result()
  def pread_batch_keyed([], _timeout_ms), do: {:ok, []}

  def pread_batch_keyed(locations, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        case pread_batch_keyed_submit_shape(locations) do
          {:single_path, path, reads} ->
            NIF.v2_pread_batch_path_key_async(proxy, corr_id, path, reads)

          {:grouped_paths, groups} ->
            NIF.v2_pread_batch_grouped_key_async(proxy, corr_id, groups)
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

  @doc false
  @spec pread_batch_keyed_submit_shape([{binary(), non_neg_integer(), binary()}]) ::
          {:single_path, binary(), [{non_neg_integer(), binary()}]}
          | {:grouped_paths,
             [
               {binary(), [{non_neg_integer(), non_neg_integer(), binary()}]}
             ]}
  def pread_batch_keyed_submit_shape([{path, offset, key} | rest] = locations) do
    same_path_keyed_offsets(rest, path, [{offset, key}], locations)
  end

  def pread_batch_keyed_submit_shape([]), do: {:grouped_paths, []}

  defp same_path_keyed_offsets([{path, offset, key} | rest], path, reads, locations) do
    same_path_keyed_offsets(rest, path, [{offset, key} | reads], locations)
  end

  defp same_path_keyed_offsets([], path, reads, _locations) do
    {:single_path, path, Enum.reverse(reads)}
  end

  defp same_path_keyed_offsets(_rest, _path, _reads, locations),
    do: {:grouped_paths, group_keyed_paths(locations)}

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

  defp group_keyed_paths(locations) do
    {groups, order} =
      locations
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {{path, offset, key}, index}, {groups, order} ->
        order = if Map.has_key?(groups, path), do: order, else: [path | order]
        groups = Map.update(groups, path, [{index, offset, key}], &[{index, offset, key} | &1])
        {groups, order}
      end)

    order
    |> Enum.reverse()
    |> Enum.map(fn path -> {path, groups |> Map.fetch!(path) |> Enum.reverse()} end)
  end

  defp emit_pread_error({:error, reason} = result, path) do
    :telemetry.execute(
      [:ferricstore, :bitcask, :pread_corrupt],
      %{count: 1},
      %{path: path, reason: classify_pread_error(reason), raw_reason: reason}
    )

    result
  end

  defp emit_pread_error(result, _path), do: result

  defp classify_pread_error(:timeout), do: :timeout

  defp classify_pread_error(reason) when is_binary(reason) do
    downcased = String.downcase(reason)

    if String.contains?(downcased, "missing_file") or
         String.contains?(downcased, "no such file") do
      :missing_file
    else
      :corrupt_record
    end
  end

  defp classify_pread_error(_reason), do: :corrupt_record
end
