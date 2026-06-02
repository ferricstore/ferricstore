defmodule Ferricstore.Raft.Batcher do
  @moduledoc """
  WARaft write facade for default-instance call sites.

  Default-instance writes are committed by `Ferricstore.Raft.WARaftBackend`;
  this module keeps the small batching API used by Router, Shard forwarding,
  and cross-shard helpers.
  """

  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher

  @type command :: tuple()

  @spec start_link(keyword()) :: {:error, :removed}
  def start_link(_opts), do: {:error, :removed}

  @spec write(non_neg_integer(), command()) :: term()
  def write(shard_index, command), do: WARaftBackend.write(shard_index, command)

  @spec write_async(non_neg_integer(), command(), GenServer.from()) :: :ok
  def write_async(shard_index, command, reply_to) do
    reply_later(reply_to, fn -> WARaftBackend.write(shard_index, command) end)
  end

  @spec write_async_quorum(non_neg_integer(), command(), GenServer.from()) :: :ok
  def write_async_quorum(shard_index, command, reply_to),
    do: write_async(shard_index, command, reply_to)

  @spec write_async_quorum_forwarded(non_neg_integer(), command(), GenServer.from(), node()) ::
          :ok
  def write_async_quorum_forwarded(shard_index, command, reply_to, _origin_node),
    do: write_async(shard_index, command, reply_to)

  @spec write_batch(non_neg_integer(), [command()], GenServer.from()) :: :ok
  def write_batch(shard_index, commands, reply_to) do
    reply_later(reply_to, fn -> WARaftBackend.write_batch(shard_index, commands) end)
  end

  @spec write_batch_forwarded(non_neg_integer(), [command()], GenServer.from(), node()) :: :ok
  def write_batch_forwarded(shard_index, commands, reply_to, _origin_node),
    do: write_batch(shard_index, commands, reply_to)

  @spec write_put_batch(
          non_neg_integer(),
          [{binary(), binary(), non_neg_integer()}],
          GenServer.from()
        ) :: :ok
  def write_put_batch(shard_index, entries, reply_to) do
    case WARaftBackend.write_put_batch_async(shard_index, entries, reply_to) do
      :ok -> :ok
      {:direct, result} -> GenServer.reply(reply_to, result)
    end
  end

  @spec write_put_batch_forwarded(
          non_neg_integer(),
          [{binary(), binary(), non_neg_integer()}],
          GenServer.from(),
          node()
        ) :: :ok
  def write_put_batch_forwarded(shard_index, entries, reply_to, _origin_node),
    do: write_put_batch(shard_index, entries, reply_to)

  @spec write_delete_batch(non_neg_integer(), [binary()], GenServer.from()) :: :ok
  def write_delete_batch(shard_index, keys, reply_to) do
    case WARaftBackend.write_delete_batch_async(shard_index, keys, reply_to) do
      :ok -> :ok
      {:direct, result} -> GenServer.reply(reply_to, result)
    end
  end

  @spec write_delete_batch_forwarded(non_neg_integer(), [binary()], GenServer.from(), node()) ::
          :ok
  def write_delete_batch_forwarded(shard_index, keys, reply_to, _origin_node),
    do: write_delete_batch(shard_index, keys, reply_to)

  @spec origin_submit(non_neg_integer(), command()) :: :ok
  def origin_submit(shard_index, command) do
    _ = Task.start(fn -> WARaftBackend.write(shard_index, {:async, node(), command}) end)
    :ok
  end

  @spec origin_submit_ordered(non_neg_integer(), command()) :: :ok | {:error, term()}
  def origin_submit_ordered(shard_index, command) do
    case WARaftBackend.write(shard_index, {:async, node(), command}) do
      {:error, _reason} = error -> error
      _result -> :ok
    end
  end

  @spec origin_enqueue_ordered(non_neg_integer(), command()) :: :ok | {:error, term()}
  def origin_enqueue_ordered(shard_index, command),
    do: origin_submit_ordered(shard_index, command)

  @spec origin_submit_batch_ordered(non_neg_integer(), [command()]) :: :ok | {:error, term()}
  def origin_submit_batch_ordered(_shard_index, []), do: :ok

  def origin_submit_batch_ordered(shard_index, commands) do
    wrapped = Enum.map(commands, &{:async, node(), &1})

    case WARaftBackend.write_batch(shard_index, wrapped) do
      {:error, _reason} = error -> error
      _result -> :ok
    end
  end

  @spec origin_submit_batch(non_neg_integer(), [command()]) :: :ok
  def origin_submit_batch(_shard_index, []), do: :ok

  def origin_submit_batch(shard_index, commands) do
    _ = Task.start(fn -> origin_submit_batch_ordered(shard_index, commands) end)
    :ok
  end

  @spec origin_accepting?(non_neg_integer()) :: boolean()
  def origin_accepting?(_shard_index), do: true

  @spec pause_writes_for_sync(non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def pause_writes_for_sync(shard_index, timeout \\ 30_000),
    do: WARaftBackend.pause_writes_for_sync(shard_index, timeout)

  @spec resume_writes_for_sync(non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def resume_writes_for_sync(shard_index, timeout \\ 5_000),
    do: WARaftBackend.resume_writes_for_sync(shard_index, timeout)

  @spec await_local_applied(non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, :timeout}
  def await_local_applied(shard_index, raft_index, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_local_applied(shard_index, raft_index, deadline)
  end

  @spec flush(non_neg_integer(), timeout()) :: :ok
  def flush(shard_index, timeout \\ 10_000), do: NamespaceBatcher.flush(shard_index, timeout)

  @spec flush_all(non_neg_integer(), timeout()) :: :ok | {:error, [{non_neg_integer(), term()}]}
  def flush_all(shard_count \\ 4, timeout \\ 10_000) do
    failures =
      if shard_count > 0 do
        0..(shard_count - 1)
        |> Enum.reduce([], fn shard_index, acc ->
          case flush(shard_index, timeout) do
            :ok -> acc
            other -> [{shard_index, other} | acc]
          end
        end)
      else
        []
      end

    case Enum.reverse(failures) do
      [] -> :ok
      errors -> {:error, errors}
    end
  end

  @spec batcher_name(non_neg_integer()) :: atom()
  def batcher_name(shard_index), do: NamespaceBatcher.name(shard_index)

  @spec remote_origin_from(node(), GenServer.from()) :: GenServer.from()
  def remote_origin_from(_origin_node, from), do: from

  @spec extract_prefix(command()) :: binary()
  def extract_prefix({:put, key, _value, _expire_at_ms}) when is_binary(key),
    do: extract_prefix_from_key(key)

  def extract_prefix({:delete, key}) when is_binary(key), do: extract_prefix_from_key(key)
  def extract_prefix(_command), do: "__default__"

  defp extract_prefix_from_key(key) do
    case :binary.split(key, ":") do
      [prefix, _rest] -> prefix
      [_] -> "__default__"
    end
  end

  defp reply_later(reply_to, fun) when is_function(fun, 0) do
    _ =
      Task.start(fn ->
        GenServer.reply(reply_to, fun.())
      end)

    :ok
  end

  defp do_await_local_applied(_shard_index, raft_index, _deadline)
       when not is_integer(raft_index) or raft_index <= 0,
       do: :ok

  defp do_await_local_applied(shard_index, raft_index, deadline) do
    case WARaftBackend.storage_position(shard_index) do
      {:ok, {:raft_log_pos, index, _term}} when is_integer(index) and index >= raft_index ->
        :ok

      _other ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(1)
          do_await_local_applied(shard_index, raft_index, deadline)
        end
    end
  end
end
