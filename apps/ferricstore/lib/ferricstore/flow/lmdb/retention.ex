defmodule Ferricstore.Flow.LMDB.Retention do
  @moduledoc false

  alias Ferricstore.Flow.LMDB.Access
  alias Ferricstore.Flow.LMDB.IndexCodec

  def sweep_expired_terminal(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.terminal_expire_prefix(),
             limit
           ) do
      {ops, counts, swept} = expired_terminal_sweep_ops(path, entries, now_ms)

      count_ops =
        Enum.map(counts, fn {count_key, count} ->
          {:put, count_key, IndexCodec.encode_count(count)}
        end)

      case Access.write_batch(path, count_ops ++ ops) do
        :ok ->
          Enum.each(counts, fn {count_key, count} ->
            Ferricstore.Flow.LMDB.TerminalCounts.put_cached_count_key(path, count_key, count)
          end)

          {:ok, swept}

        {:error, _reason} = error ->
          error
      end
    end
  end

  def sweep_expired_terminal(_path, _now_ms, _limit), do: {:ok, 0}

  def expired_terminal_state_keys(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.terminal_expire_prefix(),
             limit
           ) do
      keys =
        Enum.reduce_while(entries, [], fn {expire_key, expire_value}, acc ->
          case terminal_expire_key_time(expire_key) do
            {:ok, expire_at_ms} when expire_at_ms > now_ms ->
              {:halt, acc}

            {:ok, _expire_at_ms} ->
              case expired_terminal_state_key(path, expire_value, now_ms) do
                state_key when is_binary(state_key) -> {:cont, [state_key | acc]}
                _ -> {:cont, acc}
              end

            :error ->
              {:cont, acc}
          end
        end)

      {:ok, keys |> Enum.reverse() |> Enum.uniq()}
    end
  end

  def expired_terminal_state_keys(_path, _now_ms, _limit), do: {:ok, []}

  def expired_active_timeout_state_keys(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    index_key = Ferricstore.Flow.Keys.active_timeout_index_key()

    with {:ok, entries} <-
           Access.prefix_entries(path, IndexCodec.active_index_prefix(index_key), limit) do
      state_keys =
        Enum.reduce_while(entries, [], fn {_key, value}, acc ->
          case IndexCodec.decode_active_index_value(value) do
            {:ok, {^index_key, _member, deadline_ms, _expire_at_ms, state_key}}
            when deadline_ms <= now_ms ->
              {:cont, [state_key | acc]}

            {:ok, {^index_key, _member, deadline_ms, _expire_at_ms, _state_key}}
            when deadline_ms > now_ms ->
              {:halt, acc}

            _invalid ->
              {:cont, acc}
          end
        end)

      {:ok, Enum.reverse(state_keys)}
    end
  end

  def expired_active_timeout_state_keys(_path, _now_ms, _limit), do: {:ok, []}

  def sweep_expired_history(path, now_ms, limit)
      when is_binary(path) and is_integer(now_ms) and is_integer(limit) and limit > 0 do
    with {:ok, entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.history_expire_prefix(),
             limit
           ),
         {:ok, flow_entries} <-
           Access.prefix_entries(
             path,
             IndexCodec.history_flow_expire_prefix(),
             limit
           ) do
      {ops, swept} = expired_history_sweep_ops(path, entries, now_ms)
      {flow_ops, flow_swept} = expired_history_flow_sweep_ops(path, flow_entries, now_ms, limit)

      case Access.write_batch(path, flow_ops ++ ops) do
        :ok -> {:ok, swept + flow_swept}
        {:error, _reason} = error -> error
      end
    end
  end

  def sweep_expired_history(_path, _now_ms, _limit), do: {:ok, 0}

  defp expired_terminal_sweep_ops(path, entries, now_ms) do
    Enum.reduce_while(entries, {[], %{}, 0}, fn {expire_key, expire_value},
                                                {ops, counts, swept} ->
      case terminal_expire_key_time(expire_key) do
        {:ok, expire_at_ms} when expire_at_ms > now_ms ->
          {:halt, {ops, counts, swept}}

        {:ok, _expire_at_ms} ->
          {entry_ops, counts, entry_swept} =
            expired_terminal_entry_ops(path, expire_key, expire_value, counts, now_ms)

          {:cont, {entry_ops ++ ops, counts, swept + entry_swept}}

        :error ->
          {:cont, {[{:delete, expire_key} | ops], counts, swept}}
      end
    end)
  end

  defp expired_history_sweep_ops(path, entries, now_ms) do
    Enum.reduce_while(entries, {[], 0}, fn {expire_key, expire_value}, {ops, swept} ->
      case expire_key_time(expire_key, IndexCodec.history_expire_prefix()) do
        {:ok, expire_at_ms} when expire_at_ms > now_ms ->
          {:halt, {ops, swept}}

        {:ok, _expire_at_ms} ->
          {entry_ops, entry_swept} =
            expired_history_entry_ops(path, expire_key, expire_value, now_ms)

          {:cont, {entry_ops ++ ops, swept + entry_swept}}

        :error ->
          {:cont, {[{:delete, expire_key} | ops], swept}}
      end
    end)
  end

  defp expired_history_flow_sweep_ops(path, entries, now_ms, limit) do
    Enum.reduce_while(entries, {[], 0}, fn {expire_key, expire_value}, {ops, swept} ->
      case expire_key_time(expire_key, IndexCodec.history_flow_expire_prefix()) do
        {:ok, expire_at_ms} when expire_at_ms > now_ms ->
          {:halt, {ops, swept}}

        {:ok, _expire_at_ms} ->
          {entry_ops, entry_swept} =
            expired_history_flow_entry_ops(path, expire_key, expire_value, now_ms, limit)

          {:cont, {entry_ops ++ ops, swept + entry_swept}}

        :error ->
          {:cont, {[{:delete, expire_key} | ops], swept}}
      end
    end)
  end

  defp expired_history_flow_entry_ops(path, expire_key, expire_value, now_ms, limit) do
    case IndexCodec.decode_history_flow_expire_value(expire_value) do
      {:ok, {history_key, history_cutoff_ms}} ->
        prefix = IndexCodec.history_index_prefix(history_key)
        read_limit = max(limit, 1) + 1

        case Access.prefix_entries(path, prefix, read_limit) do
          {:ok, entries} ->
            {entries, keep_marker?} =
              if length(entries) > limit do
                {Enum.take(entries, limit), true}
              else
                {entries, false}
              end

            base_ops = if keep_marker?, do: [], else: [{:delete, expire_key}]

            {ops, swept} =
              Enum.reduce(entries, {base_ops, 0}, fn {history_index_key, history_value},
                                                     {ops_acc, swept_acc} ->
                case IndexCodec.decode_history_index_value(history_value) do
                  {:ok, {_event_id, event_ms, expire_at_ms, _compound_key}}
                  when expire_at_ms <= 0 or expire_at_ms <= now_ms ->
                    if history_event_before_cutoff?(event_ms, history_cutoff_ms) do
                      {history_index_delete_ops_from_value(
                         [{:delete, history_index_key} | ops_acc],
                         history_index_key,
                         history_value
                       ), swept_acc + 1}
                    else
                      {ops_acc, swept_acc}
                    end

                  {:ok, {_event_id, event_ms, _expire_at_ms, _compound_key}} ->
                    if history_event_before_cutoff?(event_ms, history_cutoff_ms) do
                      {history_index_delete_ops_from_value(
                         [{:delete, history_index_key} | ops_acc],
                         history_index_key,
                         history_value
                       ), swept_acc + 1}
                    else
                      {ops_acc, swept_acc}
                    end

                  _ ->
                    {ops_acc, swept_acc}
                end
              end)

            {ops, swept}

          {:error, _reason} ->
            {[], 0}
        end

      :error ->
        {[{:delete, expire_key}], 0}
    end
  end

  defp history_event_before_cutoff?(event_ms, :infinity) when is_integer(event_ms), do: true

  defp history_event_before_cutoff?(event_ms, cutoff_ms)
       when is_integer(event_ms) and is_integer(cutoff_ms),
       do: event_ms <= cutoff_ms

  defp history_event_before_cutoff?(_event_ms, _cutoff_ms), do: false

  defp expired_history_entry_ops(path, expire_key, expire_value, now_ms) do
    case IndexCodec.decode_history_expire_value(expire_value) do
      {:ok, history_index_key} ->
        case Access.get(path, history_index_key) do
          {:ok, history_value} ->
            expired_live_history_ops(expire_key, history_index_key, history_value, now_ms)

          :not_found ->
            {[{:delete, expire_key}], 0}

          {:error, _reason} ->
            {[], 0}
        end

      :error ->
        {[{:delete, expire_key}], 0}
    end
  end

  defp expired_live_history_ops(expire_key, history_index_key, history_value, now_ms) do
    case IndexCodec.decode_history_index_value(history_value) do
      {:ok, {_event_id, _event_ms, expire_at_ms, _compound_key}}
      when expire_at_ms > 0 and expire_at_ms <= now_ms ->
        {[{:delete, expire_key}, {:delete, history_index_key}], 1}

      _ ->
        {[{:delete, expire_key}], 0}
    end
  end

  defp expired_terminal_entry_ops(path, expire_key, expire_value, counts, now_ms) do
    case IndexCodec.decode_terminal_expire_value(expire_value) do
      {:ok, {terminal_key, state_key, count_key}} ->
        case Access.get(path, terminal_key) do
          {:ok, terminal_value} ->
            expired_live_terminal_ops(
              path,
              expire_key,
              terminal_key,
              terminal_value,
              state_key,
              count_key,
              counts,
              now_ms
            )

          :not_found ->
            expired_missing_terminal_ops(path, expire_key, state_key, counts)

          {:error, _reason} ->
            {[], counts, 0}
        end

      :error ->
        {[{:delete, expire_key}], counts, 0}
    end
  end

  defp expired_terminal_state_key(path, expire_value, now_ms) do
    with {:ok, {terminal_key, state_key, _count_key}} <-
           IndexCodec.decode_terminal_expire_value(expire_value),
         {:ok, terminal_value} <- Access.get(path, terminal_key),
         {:ok, {_id, _updated_at_ms, expire_at_ms, decoded_state_key}} <-
           IndexCodec.decode_terminal_index_value(terminal_value),
         true <- expire_at_ms > 0 and expire_at_ms <= now_ms do
      decoded_state_key || state_key
    else
      :not_found ->
        case IndexCodec.decode_terminal_expire_value(expire_value) do
          {:ok, {_terminal_key, state_key, _count_key}} -> state_key
          :error -> nil
        end

      _ ->
        nil
    end
  end

  defp expired_live_terminal_ops(
         path,
         expire_key,
         terminal_key,
         terminal_value,
         state_key,
         count_key,
         counts,
         now_ms
       ) do
    case IndexCodec.decode_terminal_index_value(terminal_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, decoded_state_key}}
      when expire_at_ms > 0 and expire_at_ms <= now_ms ->
        state_key = decoded_state_key || state_key

        reverse_key =
          if is_binary(state_key),
            do: IndexCodec.terminal_by_state_key_key(state_key),
            else: nil

        current_count =
          Map.get_lazy(counts, count_key, fn ->
            Ferricstore.Flow.LMDB.TerminalCounts.read_count_key(path, count_key)
          end)

        count = max(current_count - 1, 0)

        ops =
          [{:delete, terminal_key}]
          |> maybe_delete_key(reverse_key)
          |> maybe_delete_expire_key(path, expire_key, state_key)

        {ops, Map.put(counts, count_key, count), 1}

      _ ->
        {[{:delete, expire_key}], counts, 0}
    end
  end

  defp expired_missing_terminal_ops(_path, expire_key, state_key, counts)
       when not is_binary(state_key) do
    {[{:delete, expire_key}], counts, 0}
  end

  defp expired_missing_terminal_ops(path, expire_key, state_key, counts) do
    case Access.get(path, state_key) do
      {:ok, _state_value} ->
        {[], counts, 0}

      :not_found ->
        {[{:delete, expire_key}], counts, 0}

      {:error, _reason} ->
        {[], counts, 0}
    end
  end

  defp terminal_expire_key_time(key) do
    expire_key_time(key, IndexCodec.terminal_expire_prefix())
  end

  defp maybe_delete_expire_key(ops, path, expire_key, state_key)
       when is_binary(expire_key) and is_binary(state_key) do
    case Access.get(path, state_key) do
      :not_found -> [{:delete, expire_key} | ops]
      {:ok, _state_value} -> ops
      {:error, _reason} -> ops
    end
  end

  defp maybe_delete_expire_key(ops, _path, expire_key, _state_key) when is_binary(expire_key),
    do: [{:delete, expire_key} | ops]

  defp maybe_delete_expire_key(ops, _path, _expire_key, _state_key), do: ops

  defp expire_key_time(key, prefix) do
    size = byte_size(prefix)

    if byte_size(key) > size + 21 and binary_part(key, 0, size) == prefix do
      digits = binary_part(key, size, 20)

      case Integer.parse(digits) do
        {value, ""} -> {:ok, value}
        _ -> :error
      end
    else
      :error
    end
  end

  defp maybe_delete_key(ops, key) when is_binary(key), do: [{:delete, key} | ops]
  defp maybe_delete_key(ops, _key), do: ops

  defp history_index_delete_ops_from_value(ops, history_index_key, history_value) do
    maybe_delete_history_expire_key(ops, history_index_key, history_value)
  end

  defp maybe_delete_history_expire_key(ops, history_index_key, history_value) do
    case IndexCodec.decode_history_index_value(history_value) do
      {:ok, {_event_id, _event_ms, expire_at_ms, _compound_key}} when expire_at_ms > 0 ->
        [{:delete, IndexCodec.history_expire_key(expire_at_ms, history_index_key)} | ops]

      _ ->
        ops
    end
  end
end
