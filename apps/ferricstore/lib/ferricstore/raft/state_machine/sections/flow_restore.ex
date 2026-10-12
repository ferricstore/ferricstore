defmodule Ferricstore.Raft.StateMachine.Sections.FlowRestore do
  @moduledoc false

  # Applies one run of a Flow export (Ferricstore.Flow.Transfer.restore/3) into
  # this shard: its values and auxiliary rows as stored, its record through the
  # same writes a create makes (type catalog, retention, shared refs, lanes,
  # due and secondary indexes), and its history events with their stored ids
  # and bytes. Keys arrive already in this node's data format.

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]

      alias Ferricstore.Flow.Keys, as: FlowKeys

      defp do_flow_restore(state, %{restore: :flow, record: %{id: id} = record} = attrs)
           when is_binary(id) do
        partition_key = Map.get(record, :partition_key)
        state_key = FlowKeys.state_key(id, partition_key)

        case flow_read_record(state, id, partition_key) do
          nil ->
            record = flow_restore_restamp(state, record)

            with :ok <- flow_restore_values(state, record, Map.get(attrs, :values, [])),
                 :ok <- flow_put_new_state_record(state, state_key, record),
                 :ok <- flow_create_put_registry_marker(state, record),
                 :ok <- flow_due_put(state, record),
                 :ok <- flow_index_put(state, record),
                 :ok <- flow_restore_history(state, record, Map.get(attrs, :history, [])) do
              flow_restore_aux(state, record, Map.get(attrs, :aux, []))
            end

          _existing ->
            {:error, "ERR flow already exists"}
        end
      end

      # Further values and history of a run restored by an earlier command. The
      # run's record travels with each chunk: it may hibernate in between.
      defp do_flow_restore(state, %{restore: :values, record: record, values: values})
           when is_map(record) and is_list(values),
           do: flow_restore_values(state, record, values)

      defp do_flow_restore(state, %{restore: :history, record: record, history: entries})
           when is_map(record) and is_list(entries),
           do: flow_restore_history(state, record, entries)

      defp do_flow_restore(state, %{restore: :shared_value, key: key, value: value} = attrs)
           when is_binary(key) and is_binary(value) do
        with :ok <- flow_validate_key_size(key) do
          raw_put_cold(state, key, value, Map.get(attrs, :expire_at_ms, 0))
        end
      end

      defp do_flow_restore(_state, _attrs), do: {:error, "ERR invalid flow restore"}

      # incarnation/state_enter_seq come from this shard's Raft position and
      # identify the run (retention guards compare them), so the run takes
      # fresh ones here rather than numbers from the source shard.
      defp flow_restore_restamp(state, record) do
        seq = flow_next_state_enter_seq(state)

        record
        |> flow_restore_put_present(:incarnation, seq)
        |> flow_restore_put_present(:state_enter_seq, seq)
      end

      defp flow_restore_put_present(record, field, value) do
        if Map.has_key?(record, field), do: Map.put(record, field, value), else: record
      end

      defp flow_restore_values(state, record, values) do
        expire_at_ms = flow_record_expire_at(record)

        Enum.reduce_while(values, :ok, fn {key, value}, :ok ->
          result =
            with :ok <- flow_validate_key_size(key),
                 :ok <- raw_put_cold(state, key, value, expire_at_ms) do
              if flow_retention_owned_value_ref?(key, record),
                do: flow_track_retention_cleanup_key(state, record, key),
                else: :ok
            end

          case result do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_restore_history(state, %{id: id} = record, entries) do
        history_key = FlowKeys.history_key(id, Map.get(record, :partition_key))
        terminal? = Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state))

        Enum.reduce_while(entries, :ok, fn {event_id, event_ms, version, value}, :ok ->
          entry = %{
            key: FlowKeys.stream_entry_key_from_history_key(history_key, event_id),
            expire_at_ms: 0,
            history_key: history_key,
            event_id: event_id,
            event_ms: event_ms,
            version: version,
            shard_index: state.shard_index,
            history_hot_max_events: Map.get(record, :history_hot_max_events),
            history_max_events: Map.get(record, :history_max_events),
            terminal?: terminal?,
            value: value
          }

          case flow_history_put_or_queue_entry(state, entry) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      # Signal idempotency and shared-value link rows, as stored. Links are
      # owned keys that retention cleanup deletes with the run.
      defp flow_restore_aux(state, record, rows) do
        expire_at_ms = flow_record_expire_at(record)

        Enum.reduce_while(rows, :ok, fn {kind, key, value}, :ok ->
          result =
            with :ok <- flow_validate_key_size(key),
                 :ok <- raw_put_cold(state, key, value, expire_at_ms) do
              if kind == :link,
                do: flow_track_retention_cleanup_key(state, record, key),
                else: :ok
            end

          case result do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end
    end
  end
end
