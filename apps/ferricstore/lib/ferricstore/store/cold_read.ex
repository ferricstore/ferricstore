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
  def pread_batch(locations, timeout_ms) do
    await_tokio(
      fn proxy, corr_id ->
        NIF.v2_pread_batch_async(proxy, corr_id, locations)
      end,
      timeout_ms
    )
  end
end
