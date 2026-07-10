defmodule Ferricstore.Raft.StateMachine.Sections.FlowGovernanceReleaseOutbox do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]

      alias Ferricstore.Flow.Keys, as: FlowKeys

      defp flow_enqueue_governance_release_intents(state, key_records)
           when is_list(key_records) do
        intents =
          key_records
          |> Enum.reduce([], fn {_key, record}, acc ->
            case flow_governance_release_intent(record) do
              nil -> acc
              intent -> [intent | acc]
            end
          end)
          |> Enum.reverse()
          |> Enum.uniq_by(&Map.fetch!(&1, :reservation_id))

        flow_append_governance_release_intents(state, intents)
      end

      defp flow_governance_release_intent(
             %{
               id: flow_id,
               state: flow_state,
               governance_limit:
                 %{
                   scope: scope,
                   shard_id: shard_id,
                   reservation_id: reservation_id
                 } = reservation
             } = record
           )
           when flow_state != "running" and is_binary(flow_id) and flow_id != "" and
                  is_binary(scope) and scope != "" and is_integer(shard_id) and shard_id >= 0 and
                  is_binary(reservation_id) and reservation_id != "" do
        %{
          flow_id: flow_id,
          partition_key: Map.get(record, :partition_key),
          scope: scope,
          shard_id: shard_id,
          reservation_id: reservation_id,
          enforcement: Map.get(reservation, :enforcement, :approximate_global),
          created_at_ms: Map.get(record, :updated_at_ms, 0)
        }
      end

      defp flow_governance_release_intent(_record), do: nil

      defp flow_append_governance_release_intents(_state, []), do: :ok

      defp flow_append_governance_release_intents(state, intents) do
        shard_index = state.shard_index
        meta_key = FlowKeys.governance_release_outbox_meta_key(shard_index)

        with {:ok, meta} <-
               Ferricstore.Flow.Governance.ReleaseOutbox.decode_meta(do_get(state, meta_key)),
             {:ok, next_meta, sequences} <-
               Ferricstore.Flow.Governance.ReleaseOutbox.append(meta, length(intents)),
             :ok <- flow_put_governance_release_intents(state, shard_index, sequences, intents) do
          do_put(
            state,
            meta_key,
            Ferricstore.Flow.Governance.ReleaseOutbox.encode_meta(next_meta),
            0
          )
        end
      end

      defp flow_put_governance_release_intents(state, shard_index, sequences, intents) do
        sequences
        |> Enum.zip(intents)
        |> Enum.reduce_while(:ok, fn {sequence, intent}, :ok ->
          key = FlowKeys.governance_release_outbox_intent_key(shard_index, sequence)
          value = Ferricstore.Flow.Governance.ReleaseOutbox.encode_intent(intent)

          case do_put(state, key, value, 0) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp do_flow_governance_release_outbox_ack(
             state,
             shard_index,
             expected_head,
             up_to
           )
           when is_integer(shard_index) and shard_index >= 0 and is_integer(expected_head) and
                  expected_head > 0 and is_integer(up_to) and up_to >= expected_head and
                  up_to - expected_head < 256 do
        if shard_index == state.shard_index do
          meta_key = FlowKeys.governance_release_outbox_meta_key(shard_index)

          with {:ok, meta} <-
                 Ferricstore.Flow.Governance.ReleaseOutbox.decode_meta(do_get(state, meta_key)),
               {:ok, next_meta, acknowledged} <-
                 Ferricstore.Flow.Governance.ReleaseOutbox.acknowledge(
                   meta,
                   expected_head,
                   up_to
                 ),
               :ok <- flow_delete_governance_release_intents(state, shard_index, acknowledged) do
            do_put(
              state,
              meta_key,
              Ferricstore.Flow.Governance.ReleaseOutbox.encode_meta(next_meta),
              0
            )
          end
        else
          {:error, "ERR flow governance release outbox shard mismatch"}
        end
      end

      defp do_flow_governance_release_outbox_ack(
             _state,
             _shard_index,
             _expected_head,
             _up_to
           ),
           do: {:error, "ERR invalid flow governance release outbox acknowledgement"}

      defp do_flow_governance_release_outbox_mark_completed(state, shard_index, sequences)
           when is_integer(shard_index) and shard_index >= 0 and is_list(sequences) and
                  sequences != [] and length(sequences) <= 256 do
        if shard_index == state.shard_index do
          meta_key = FlowKeys.governance_release_outbox_meta_key(shard_index)

          with true <-
                 length(Enum.uniq(sequences)) == length(sequences) and
                   Enum.all?(sequences, &(is_integer(&1) and &1 > 0)),
               {:ok, %{head: head, tail: tail}} <-
                 Ferricstore.Flow.Governance.ReleaseOutbox.decode_meta(do_get(state, meta_key)),
               true <- Enum.all?(sequences, &(&1 <= tail)) do
            sequences
            |> Enum.filter(&(&1 >= head))
            |> flow_put_governance_release_completed(state, shard_index)
          else
            _invalid -> {:error, "ERR invalid flow governance release outbox completion"}
          end
        else
          {:error, "ERR flow governance release outbox shard mismatch"}
        end
      end

      defp do_flow_governance_release_outbox_mark_completed(
             _state,
             _shard_index,
             _sequences
           ),
           do: {:error, "ERR invalid flow governance release outbox completion"}

      defp flow_put_governance_release_completed(sequences, state, shard_index) do
        Enum.reduce_while(sequences, :ok, fn sequence, :ok ->
          key = FlowKeys.governance_release_outbox_completed_key(shard_index, sequence)
          value = Ferricstore.Flow.Governance.ReleaseOutbox.completed_marker()

          case do_put(state, key, value, 0) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_delete_governance_release_intents(state, shard_index, acknowledged) do
        Enum.reduce_while(acknowledged, :ok, fn sequence, :ok ->
          intent_key = FlowKeys.governance_release_outbox_intent_key(shard_index, sequence)
          completed_key = FlowKeys.governance_release_outbox_completed_key(shard_index, sequence)

          with :ok <- do_delete(state, intent_key),
               :ok <- do_delete(state, completed_key) do
            {:cont, :ok}
          else
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end
    end
  end
end
