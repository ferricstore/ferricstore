defmodule Ferricstore.Store.Ops.LocalNative do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Store.{LocalTxStore, RateLimit, ValueCodec}
  alias Ferricstore.Store.Ops.LocalRead
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @spec cas(LocalTxStore.t(), binary(), binary(), binary(), non_neg_integer() | nil) ::
          1 | 0 | nil | {:error, term()}
  def cas(tx, key, expected, new_value, ttl_ms) do
    case LocalRead.local_read_meta(tx, key) do
      {:error, _reason} = error ->
        error

      nil ->
        nil

      {current, old_expire_at_ms} ->
        if normalize_value(current) == expected do
          expire_at_ms =
            if is_integer(ttl_ms), do: CommandTime.now_ms() + ttl_ms, else: old_expire_at_ms

          :ok = write(tx, key, new_value, expire_at_ms)
          1
        else
          0
        end
    end
  end

  @spec lock(LocalTxStore.t(), binary(), binary(), pos_integer()) :: :ok | {:error, term()}
  def lock(tx, key, owner, ttl_ms) do
    expire_at_ms = CommandTime.now_ms() + ttl_ms

    case LocalRead.local_read_meta(tx, key) do
      {:error, _reason} = error ->
        error

      nil ->
        write(tx, key, owner, expire_at_ms)

      {^owner, _old_expire_at_ms} ->
        write(tx, key, owner, expire_at_ms)

      {_other, _old_expire_at_ms} ->
        {:error, "DISTLOCK lock is held by another owner"}
    end
  end

  @spec unlock(LocalTxStore.t(), binary(), binary()) :: 1 | {:error, term()}
  def unlock(tx, key, owner) do
    case LocalRead.local_read_meta(tx, key) do
      {:error, _reason} = error ->
        error

      nil ->
        1

      {^owner, _expire_at_ms} ->
        :ok = delete(tx, key)
        1

      {_other, _expire_at_ms} ->
        {:error, "DISTLOCK caller is not the lock owner"}
    end
  end

  @spec extend(LocalTxStore.t(), binary(), binary(), pos_integer()) :: 1 | {:error, term()}
  def extend(tx, key, owner, ttl_ms) do
    case LocalRead.local_read_meta(tx, key) do
      {:error, _reason} = error ->
        error

      nil ->
        {:error, "DISTLOCK lock does not exist or has expired"}

      {^owner, _old_expire_at_ms} ->
        :ok = write(tx, key, owner, CommandTime.now_ms() + ttl_ms)
        1

      {_other, _old_expire_at_ms} ->
        {:error, "DISTLOCK caller is not the lock owner"}
    end
  end

  @spec ratelimit_add(
          LocalTxStore.t(),
          binary(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: [term()] | {:error, term()}
  def ratelimit_add(tx, key, window_ms, limit, count) do
    now_ms = CommandTime.now_ms()

    case LocalRead.local_read_meta(tx, key) do
      {:error, _reason} = error ->
        error

      meta ->
        {current_count, current_start_ms, previous_count} = decode_state(meta, now_ms)

        {current_count, current_start_ms, previous_count} =
          rotate_window(current_count, current_start_ms, previous_count, now_ms, window_ms)

        elapsed_ms = now_ms - current_start_ms

        effective_count =
          RateLimit.effective_count(current_count, previous_count, elapsed_ms, window_ms)

        {status, final_count, remaining, stored_count} =
          if effective_count + count > limit do
            {"denied", effective_count, max(0, limit - effective_count), current_count}
          else
            new_count = current_count + count

            {"allowed", effective_count + count, max(0, limit - effective_count - count),
             new_count}
          end

        encoded = ValueCodec.encode_ratelimit(stored_count, current_start_ms, previous_count)
        :ok = write(tx, key, encoded, current_start_ms + window_ms * 2)

        [status, final_count, remaining, max(0, current_start_ms + window_ms - now_ms)]
    end
  end

  defp decode_state(nil, now_ms), do: {0, now_ms, 0}

  defp decode_state({value, _expire_at_ms}, now_ms),
    do: ValueCodec.decode_ratelimit(value, now_ms)

  defp rotate_window(_current_count, current_start_ms, _previous_count, now_ms, window_ms)
       when now_ms - current_start_ms >= window_ms * 2,
       do: {0, now_ms, 0}

  defp rotate_window(current_count, current_start_ms, _previous_count, now_ms, window_ms)
       when now_ms - current_start_ms >= window_ms,
       do: {0, now_ms, current_count}

  defp rotate_window(current_count, current_start_ms, previous_count, _now_ms, _window_ms),
    do: {current_count, current_start_ms, previous_count}

  defp write(tx, key, value, expire_at_ms) do
    ShardETS.ets_insert(tx.shard_state, key, value, expire_at_ms)
    LocalRead.tx_put_pending(key, value, expire_at_ms)
    send(self(), {:tx_pending_write, key, value, expire_at_ms})
    :ok
  end

  defp delete(tx, key) do
    ShardETS.ets_delete_key(tx.shard_state, key)
    LocalRead.tx_drop_pending(key)
    LocalRead.tx_mark_deleted(key)
    send(self(), {:tx_pending_delete, key})
    :ok
  end

  defp normalize_value(value) when is_integer(value), do: Integer.to_string(value)
  defp normalize_value(value) when is_float(value), do: ValueCodec.format_float(value)
  defp normalize_value(value), do: value
end
