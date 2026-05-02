defmodule Ferricstore.Store.ColdRead do
  @moduledoc """
  Helpers for synchronous callers that need to wait on Tokio cold-read NIFs.

  The NIF sends `{:tokio_complete, corr_id, ...}` to the pid passed at submit
  time. If a caller waits directly and times out, a late completion can remain
  in that caller's mailbox. These helpers submit through a short-lived proxy
  process, so late completions are consumed or dropped away from the caller.
  """

  alias Ferricstore.Bitcask.NIF

  @type submit_fun :: (pid(), pos_integer() -> :ok | {:error, term()})
  @type result :: {:ok, term()} | {:error, term()}

  @doc false
  @spec await_tokio(submit_fun(), timeout()) :: result()
  def await_tokio(submit_fun, timeout_ms) do
    parent = :erlang.alias()
    ref = make_ref()
    corr_id = System.unique_integer([:positive, :monotonic])

    proxy =
      spawn(fn ->
        case submit_fun.(self(), corr_id) do
          :ok -> proxy_receive(parent, ref, corr_id, timeout_ms)
          {:error, _reason} = error -> send(parent, {ref, error})
        end
      end)

    receive do
      {^ref, result} ->
        :erlang.unalias(parent)
        result
    after
      timeout_ms ->
        receive do
          {^ref, result} ->
            :erlang.unalias(parent)
            result
        after
          0 ->
            :erlang.unalias(parent)
            send(proxy, {ref, :cancel})
            {:error, :timeout}
        end
    end
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
  def pread_batch(locations, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        NIF.v2_pread_batch_async(proxy, corr_id, locations)
      end,
      timeout_ms
    )
  end

  defp proxy_receive(parent, ref, corr_id, timeout_ms) do
    receive do
      {:tokio_complete, ^corr_id, :ok, value} ->
        maybe_send_result(parent, ref, {:ok, value})

      {:tokio_complete, ^corr_id, :error, reason} ->
        maybe_send_result(parent, ref, {:error, reason})

      {^ref, :cancel} ->
        proxy_drain(corr_id, timeout_ms)
    after
      timeout_ms ->
        :ok
    end
  end

  defp proxy_drain(corr_id, timeout_ms) do
    receive do
      {:tokio_complete, ^corr_id, _status, _payload} -> :ok
    after
      timeout_ms -> :ok
    end
  end

  defp maybe_send_result(parent, ref, result) do
    receive do
      {^ref, :cancel} -> :ok
    after
      0 -> send(parent, {ref, result})
    end
  end
end
