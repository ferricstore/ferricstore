defmodule Ferricstore.Bitcask.Async do
  @moduledoc """
  Safe waiter for Tokio-backed async NIF calls.

  Async NIFs send `:tokio_complete` to the pid they receive at submit time.
  Directly passing the caller pid is unsafe for synchronous command handlers:
  when the caller times out, a late completion remains in that mailbox and can
  confuse later receives. This helper submits through a short-lived proxy
  process and replies through a process alias, so late replies are dropped after
  timeout instead of leaking into the caller.

  This helper creates one short-lived BEAM process per wait. That cost is
  acceptable for synchronous cold reads, probabilistic reads, and cleanup
  operations, but it is not the final shape for a proven read-hot bottleneck.
  If benchmarking shows regression on hot async-NIF read workloads, replace
  per-read proxies with a tracked waiter/collector owned by the connection or
  command process, so correlation IDs are still isolated without spawning a
  process for every read.
  """

  @type submit_fun :: (pid(), pos_integer() -> :ok | {:error, term()})
  @type result :: {:ok, term()} | {:error, term()}

  @spec await(submit_fun(), timeout()) :: result()
  def await(submit_fun, timeout_ms) do
    parent = :erlang.alias()
    ref = make_ref()
    corr_id = System.unique_integer([:positive, :monotonic])

    proxy =
      spawn(fn ->
        case submit_fun.(self(), corr_id) do
          :ok -> proxy_receive(parent, ref, corr_id, timeout_ms)
          {:error, _reason} = error -> maybe_send_result(parent, ref, error)
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

  defp proxy_receive(parent, ref, corr_id, timeout_ms) do
    receive do
      {:tokio_complete, ^corr_id, :ok} ->
        maybe_send_result(parent, ref, {:ok, :ok})

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
      {:tokio_complete, ^corr_id, :ok} -> :ok
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
