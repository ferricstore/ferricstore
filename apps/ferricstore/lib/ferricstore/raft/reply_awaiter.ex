defmodule Ferricstore.Raft.ReplyAwaiter do
  @moduledoc """
  Alias-backed waiters for replies sent with `GenServer.reply/2`.

  Router sometimes passes explicit `{pid, tag}` reply targets into the Raft
  batcher and then waits in the caller process. A plain `{self(), ref}` target
  leaves late replies in the caller mailbox after timeout. Process aliases let
  us turn the reply target off on timeout, so late Raft replies are dropped
  instead of confusing later receives on the same connection process.
  """

  @type token :: {reference(), {__MODULE__, reference()}}
  @type from :: {reference(), {__MODULE__, reference()}}

  @spec new() :: {from(), token()}
  def new do
    alias_ref = :erlang.alias()
    tag = {__MODULE__, make_ref()}
    {{alias_ref, tag}, {alias_ref, tag}}
  end

  @spec await(token(), timeout(), term()) :: term()
  def await({alias_ref, tag}, timeout_ms, timeout_result) do
    receive do
      {^tag, reply} ->
        cleanup(alias_ref, tag)
        reply
    after
      timeout_ms ->
        cleanup(alias_ref, tag)
        timeout_result
    end
  end

  @spec collect([token()], timeout()) :: {:ok | :timeout, [{token(), term()}], [token()]}
  def collect(tokens, timeout_ms) do
    pending =
      Map.new(tokens, fn {_alias_ref, tag} = token ->
        {tag, token}
      end)

    collect_pending(pending, [], System.monotonic_time(:millisecond), timeout_ms)
  end

  @spec collect_tagged([{token(), term()}], timeout()) ::
          {:ok | :timeout, [{term(), term()}], [{token(), term()}]}
  def collect_tagged(token_meta_pairs, timeout_ms) do
    pending =
      Map.new(token_meta_pairs, fn {{_alias_ref, tag} = token, meta} ->
        {tag, {token, meta}}
      end)

    collect_tagged_pending(pending, [], System.monotonic_time(:millisecond), timeout_ms)
  end

  defp collect_pending(pending, replies, _started_at, _timeout_ms) when map_size(pending) == 0 do
    {:ok, Enum.reverse(replies), []}
  end

  defp collect_pending(pending, replies, started_at, timeout_ms) do
    elapsed = System.monotonic_time(:millisecond) - started_at
    remaining_timeout = max(timeout_ms - elapsed, 0)

    receive do
      {{__MODULE__, _ref} = tag, reply} ->
        case Map.pop(pending, tag) do
          {nil, next_pending} ->
            collect_pending(next_pending, replies, started_at, timeout_ms)

          {{alias_ref, ^tag} = token, next_pending} ->
            cleanup(alias_ref, tag)
            collect_pending(next_pending, [{token, reply} | replies], started_at, timeout_ms)
        end
    after
      remaining_timeout ->
        unresolved = Map.values(pending)
        Enum.each(unresolved, fn {alias_ref, tag} -> cleanup(alias_ref, tag) end)
        {:timeout, Enum.reverse(replies), unresolved}
    end
  end

  defp collect_tagged_pending(pending, replies, _started_at, _timeout_ms)
       when map_size(pending) == 0 do
    {:ok, Enum.reverse(replies), []}
  end

  defp collect_tagged_pending(pending, replies, started_at, timeout_ms) do
    elapsed = System.monotonic_time(:millisecond) - started_at
    remaining_timeout = max(timeout_ms - elapsed, 0)

    receive do
      {{__MODULE__, _ref} = tag, reply} ->
        case Map.pop(pending, tag) do
          {nil, next_pending} ->
            collect_tagged_pending(next_pending, replies, started_at, timeout_ms)

          {{{alias_ref, ^tag}, meta}, next_pending} ->
            cleanup(alias_ref, tag)
            collect_tagged_pending(next_pending, [{meta, reply} | replies], started_at, timeout_ms)
        end
    after
      remaining_timeout ->
        unresolved = Map.values(pending)

        Enum.each(unresolved, fn
          {{alias_ref, tag}, _meta} -> cleanup(alias_ref, tag)
        end)

        {:timeout, Enum.reverse(replies), unresolved}
    end
  end

  defp cleanup(alias_ref, tag) do
    :erlang.unalias(alias_ref)
    flush_tag(tag)
  end

  defp flush_tag(tag) do
    receive do
      {^tag, _reply} -> flush_tag(tag)
    after
      0 -> :ok
    end
  end
end
