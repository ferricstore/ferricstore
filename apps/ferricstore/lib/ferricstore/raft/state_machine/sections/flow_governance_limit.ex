defmodule Ferricstore.Raft.StateMachine.Sections.FlowGovernanceLimit do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Flow.Governance.CreditLease
      alias Ferricstore.Flow.Governance.LimitRecord
      alias Ferricstore.Flow.Governance.View
      alias Ferricstore.Flow.Keys, as: FlowKeys

      @flow_limit_max_amount 1_000
      @flow_limit_max_exact_version 9_007_199_254_740_991
      @flow_limit_max_cleanup_tasks 256

      defp do_flow_governance_limit_mutate(state, key, attrs) do
        with :ok <- flow_limit_validate_request(state, key, attrs),
             {:ok, owner, new_owner?} <- flow_limit_load_owner(state, key, attrs),
             {:ok, owner} <-
               flow_limit_apply_configuration(owner, Map.get(attrs, :configuration)),
             {:ok, owner} <-
               flow_limit_prepare_owner(state, owner, attrs),
             :ok <- flow_limit_maybe_enqueue_catalog_publication(state, key, new_owner?) do
          flow_limit_dispatch(state, key, owner, attrs)
        end
      end

      defp flow_limit_prepare_owner(_state, owner, %{op: :release}), do: {:ok, owner}

      defp flow_limit_prepare_owner(state, owner, %{op: :spend, shard_id: shard_id} = attrs) do
        with {:ok, owner} <-
               flow_limit_reclaim_expired(state, owner, Map.get(attrs, :now_ms, 0)) do
          case Map.fetch(owner.leases, shard_id) do
            {:ok, _lease} -> flow_limit_retire_released_epoch(state, owner, shard_id)
            :error -> {:ok, owner}
          end
        end
      end

      defp flow_limit_prepare_owner(state, owner, attrs) do
        flow_limit_reclaim_expired(state, owner, Map.get(attrs, :now_ms, 0))
      end

      defp flow_limit_dispatch(state, key, owner, %{op: :lease} = attrs) do
        case CreditLease.grant(owner, attrs.shard_id, attrs.amount,
               now_ms: attrs.now_ms,
               ttl_ms: attrs.ttl_ms
             ) do
          {:ok, owner, lease} ->
            flow_limit_write_owner(
              state,
              key,
              owner,
              {:ok, %{owner: View.public(owner), lease: View.public(lease)}}
            )

          {:error, reason, owner} ->
            flow_limit_write_owner(state, key, owner, {:error, reason})
        end
      end

      defp flow_limit_dispatch(state, key, owner, %{op: :spend} = attrs) do
        case Map.fetch(owner.leases, attrs.shard_id) do
          {:ok, lease} when lease.epoch == attrs.lease_epoch ->
            with {:ok, reservations} <-
                   flow_limit_reservation_states(
                     state,
                     owner.scope,
                     attrs.shard_id,
                     lease.epoch,
                     attrs.reservation_ids
                   ) do
              flow_limit_spend_with_reservations(state, key, owner, lease, attrs, reservations)
            end

          {:ok, _newer_or_older_lease} ->
            flow_limit_write_owner(
              state,
              key,
              owner,
              {:error, "ERR flow limit lease generation changed"}
            )

          :error ->
            case CreditLease.spend(owner, attrs.shard_id, attrs.amount,
                   now_ms: attrs.now_ms,
                   ttl_ms: attrs.ttl_ms,
                   reservation_ids: attrs.reservation_ids
                 ) do
              {:error, reason, owner} ->
                flow_limit_write_owner(state, key, owner, {:error, reason})

              {:ok, _owner, _lease} ->
                {:error, "ERR flow limit record is corrupt"}
            end
        end
      end

      defp flow_limit_dispatch(state, key, owner, %{op: :release} = attrs) do
        case Map.fetch(owner.leases, attrs.shard_id) do
          {:ok, lease} ->
            with {:ok, reservations} <-
                   flow_limit_reservation_states(
                     state,
                     owner.scope,
                     attrs.shard_id,
                     lease.epoch,
                     attrs.reservation_ids
                   ),
                 active = Enum.filter(reservations, &match?({_id, _key, :active}, &1)),
                 owner <-
                   CreditLease.release_identified_amount(owner, attrs.shard_id, length(active)),
                 :ok <- flow_limit_mark_released_reservations(state, active),
                 {:ok, owner} <-
                   flow_limit_retire_released_epoch(state, owner, attrs.shard_id) do
              flow_limit_write_owner(state, key, owner, {:ok, View.public(owner)})
            end

          :error ->
            flow_limit_write_owner(state, key, owner, {:ok, View.public(owner)})
        end
      end

      defp flow_limit_dispatch(state, key, owner, %{op: :renew} = attrs) do
        case CreditLease.renew(owner, attrs.shard_id,
               now_ms: attrs.now_ms,
               ttl_ms: attrs.ttl_ms
             ) do
          {:ok, owner, lease} ->
            flow_limit_write_owner(
              state,
              key,
              owner,
              {:ok, %{owner: View.public(owner), lease: View.public(lease)}}
            )

          {:error, reason, owner} ->
            flow_limit_write_owner(state, key, owner, {:error, reason})
        end
      end

      defp flow_limit_dispatch(state, key, owner, %{op: :get}) do
        flow_limit_write_owner(state, key, owner, {:ok, View.public(owner)})
      end

      defp flow_limit_dispatch(state, key, owner, %{op: :cleanup}) do
        flow_limit_cleanup_page(state, key, owner)
      end

      defp flow_limit_spend_with_reservations(
             state,
             key,
             owner,
             lease,
             attrs,
             reservations
           ) do
        known_count =
          Enum.count(
            reservations,
            &match?({_id, _key, status} when status in [:active, :released], &1)
          )

        cond do
          known_count == attrs.amount ->
            flow_limit_write_owner(
              state,
              key,
              owner,
              {:ok,
               %{
                 owner: View.public(owner),
                 lease: View.public(lease),
                 reservation_ids: attrs.reservation_ids
               }}
            )

          known_count > 0 ->
            flow_limit_write_owner(
              state,
              key,
              owner,
              {:error, "ERR flow limit reservation_id conflict"}
            )

          true ->
            case CreditLease.spend(owner, attrs.shard_id, attrs.amount,
                   now_ms: attrs.now_ms,
                   ttl_ms: attrs.ttl_ms,
                   reservation_ids: attrs.reservation_ids
                 ) do
              {:ok, owner, _lease} ->
                with {:ok, owner} <-
                       flow_limit_append_reservation_pages(
                         state,
                         owner,
                         attrs.shard_id,
                         attrs.reservation_ids
                       ),
                     :ok <-
                       flow_limit_put_reservations(
                         state,
                         owner.scope,
                         attrs.shard_id,
                         owner.leases[attrs.shard_id].epoch,
                         attrs.reservation_ids
                       ) do
                  lease = owner.leases[attrs.shard_id]

                  flow_limit_write_owner(
                    state,
                    key,
                    owner,
                    {:ok,
                     %{
                       owner: View.public(owner),
                       lease: View.public(lease),
                       reservation_ids: attrs.reservation_ids
                     }}
                  )
                end

              {:error, reason, owner} ->
                flow_limit_write_owner(state, key, owner, {:error, reason})
            end
        end
      end

      defp flow_limit_append_reservation_pages(state, owner, shard_id, reservation_ids) do
        lease = owner.leases[shard_id]

        current_entries =
          if lease.reservation_page == 0 do
            0
          else
            (lease.reservation_page - 1) * LimitRecord.page_size() +
              lease.reservation_page_fill
          end

        with true <-
               current_entries + length(reservation_ids) <=
                 LimitRecord.max_reservation_pages() * LimitRecord.page_size(),
             {:ok, page, fill} <-
               flow_limit_append_pages(
                 state,
                 owner.scope,
                 shard_id,
                 lease.epoch,
                 lease.reservation_page,
                 lease.reservation_page_fill,
                 reservation_ids
               ) do
          lease = %{
            lease
            | reservation_page: page,
              reservation_page_fill: fill,
              reservations: %{}
          }

          {:ok, %{owner | leases: Map.put(owner.leases, shard_id, lease)}}
        else
          false -> {:error, "ERR flow limit reservation history is full"}
          {:error, _reason} = error -> error
        end
      end

      defp flow_limit_append_pages(
             _state,
             _scope,
             _shard_id,
             _epoch,
             page,
             fill,
             []
           ),
           do: {:ok, page, fill}

      defp flow_limit_append_pages(state, scope, shard_id, epoch, 0, 0, reservation_ids) do
        flow_limit_write_new_pages(state, scope, shard_id, epoch, 0, reservation_ids)
      end

      defp flow_limit_append_pages(
             state,
             scope,
             shard_id,
             epoch,
             page,
             fill,
             reservation_ids
           )
           when fill < 256 do
        page_key = FlowKeys.governance_limit_reservation_page_key(scope, shard_id, epoch, page)

        with :ok <- flow_limit_validate_storage_key(page_key),
             value when is_binary(value) <- do_get(state, page_key),
             {:ok, existing_ids} <- LimitRecord.decode_page(value),
             true <- length(existing_ids) == fill,
             capacity = LimitRecord.page_size() - fill,
             {added, remaining} <- Enum.split(reservation_ids, capacity),
             {:ok, encoded} <- LimitRecord.encode_page(existing_ids ++ added),
             :ok <- flow_limit_validate_storage_value(encoded),
             :ok <- do_put(state, page_key, encoded, 0) do
          next_fill = fill + length(added)

          if remaining == [] do
            {:ok, page, next_fill}
          else
            flow_limit_write_new_pages(state, scope, shard_id, epoch, page, remaining)
          end
        else
          nil -> {:error, "ERR flow limit reservation page is missing"}
          false -> {:error, "ERR flow limit reservation page is corrupt"}
          {:error, _reason} = error -> error
        end
      end

      defp flow_limit_append_pages(
             state,
             scope,
             shard_id,
             epoch,
             page,
             256,
             reservation_ids
           ) do
        flow_limit_write_new_pages(state, scope, shard_id, epoch, page, reservation_ids)
      end

      defp flow_limit_write_new_pages(
             state,
             scope,
             shard_id,
             epoch,
             previous_page,
             reservation_ids
           ) do
        {page_ids, remaining} = Enum.split(reservation_ids, LimitRecord.page_size())
        page = previous_page + 1
        page_key = FlowKeys.governance_limit_reservation_page_key(scope, shard_id, epoch, page)

        with :ok <- flow_limit_validate_storage_key(page_key),
             {:ok, encoded} <- LimitRecord.encode_page(page_ids),
             :ok <- flow_limit_validate_storage_value(encoded),
             nil <- do_get(state, page_key),
             :ok <- do_put(state, page_key, encoded, 0) do
          if remaining == [] do
            {:ok, page, length(page_ids)}
          else
            flow_limit_write_new_pages(state, scope, shard_id, epoch, page, remaining)
          end
        else
          value when is_binary(value) ->
            {:error, "ERR flow limit reservation page already exists"}

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_limit_put_reservations(state, scope, shard_id, epoch, reservation_ids) do
        Enum.reduce_while(reservation_ids, :ok, fn reservation_id, :ok ->
          reservation_key =
            FlowKeys.governance_limit_reservation_key(
              scope,
              shard_id,
              epoch,
              reservation_id
            )

          value = LimitRecord.encode_reservation(reservation_id)

          with :ok <- flow_limit_validate_storage_key(reservation_key),
               :ok <- flow_limit_validate_storage_value(value),
               nil <- do_get(state, reservation_key),
               :ok <- do_put(state, reservation_key, value, 0) do
            {:cont, :ok}
          else
            existing when is_binary(existing) ->
              {:halt, {:error, "ERR flow limit reservation already exists"}}

            {:error, _reason} = error ->
              {:halt, error}
          end
        end)
      end

      defp flow_limit_reservation_states(
             state,
             scope,
             shard_id,
             epoch,
             reservation_ids
           ) do
        Enum.reduce_while(reservation_ids, {:ok, []}, fn reservation_id, {:ok, acc} ->
          reservation_key =
            FlowKeys.governance_limit_reservation_key(
              scope,
              shard_id,
              epoch,
              reservation_id
            )

          with :ok <- flow_limit_validate_storage_key(reservation_key) do
            case do_get(state, reservation_key) do
              nil ->
                {:cont, {:ok, [{reservation_id, reservation_key, :missing} | acc]}}

              value when is_binary(value) ->
                case LimitRecord.decode_reservation(value, reservation_id) do
                  {:ok, status} when status in [:active, :released] ->
                    {:cont, {:ok, [{reservation_id, reservation_key, status} | acc]}}

                  {:error, _reason} = error ->
                    {:halt, error}
                end

              _invalid ->
                {:halt, {:error, "ERR flow limit reservation record is corrupt"}}
            end
          else
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, reservations} -> {:ok, Enum.reverse(reservations)}
          {:error, _reason} = error -> error
        end
      end

      defp flow_limit_mark_released_reservations(state, reservations) do
        Enum.reduce_while(reservations, :ok, fn
          {reservation_id, reservation_key, :active}, :ok ->
            value = LimitRecord.encode_reservation(reservation_id, :released)

            with :ok <- flow_limit_validate_storage_value(value),
                 :ok <- do_put(state, reservation_key, value, 0) do
              {:cont, :ok}
            else
              {:error, _reason} = error -> {:halt, error}
            end

          _missing, :ok ->
            {:cont, :ok}
        end)
      end

      defp flow_limit_retire_released_epoch(state, owner, shard_id) do
        lease = owner.leases[shard_id]
        pending_tasks = owner.cleanup_tail - owner.cleanup_head + 1

        cond do
          lease.in_use != 0 or lease.reservation_page == 0 ->
            {:ok, owner}

          pending_tasks >= @flow_limit_max_cleanup_tasks ->
            {:ok, owner}

          owner.epoch >= @flow_limit_max_exact_version ->
            {:error, "ERR flow limit lease generation is exhausted"}

          true ->
            old_ref = {shard_id, lease.epoch, lease.reservation_page}

            with {:ok, owner} <- flow_limit_enqueue_cleanup(state, owner, [old_ref]) do
              epoch = owner.epoch + 1

              lease = %{
                lease
                | epoch: epoch,
                  reservation_page: 0,
                  reservation_page_fill: 0,
                  reservations: %{}
              }

              {:ok,
               %{
                 owner
                 | epoch: epoch,
                   leases: Map.put(owner.leases, shard_id, lease)
               }}
            end
        end
      end

      defp flow_limit_reclaim_expired(state, owner, now_ms) do
        expired = CreditLease.expired_lease_refs(owner, now_ms)
        owner = CreditLease.reclaim_expired(owner, now_ms)

        flow_limit_enqueue_cleanup(state, owner, expired)
      end

      defp flow_limit_enqueue_cleanup(state, owner, refs) do
        if owner.cleanup_tail - owner.cleanup_head + 1 + length(refs) >
             @flow_limit_max_cleanup_tasks do
          {:error, "ERR flow limit cleanup backlog is full"}
        else
          flow_limit_do_enqueue_cleanup(state, owner, refs)
        end
      end

      defp flow_limit_do_enqueue_cleanup(state, owner, refs) do
        Enum.reduce_while(refs, {:ok, owner}, fn
          {shard_id, epoch, last_page}, {:ok, owner} ->
            sequence = owner.cleanup_tail + 1
            cleanup_key = FlowKeys.governance_limit_cleanup_key(owner.scope, sequence)
            cleanup_value = LimitRecord.encode_cleanup(shard_id, epoch, 1, last_page)

            with :ok <- flow_limit_validate_storage_key(cleanup_key),
                 :ok <- flow_limit_validate_storage_value(cleanup_value),
                 nil <- do_get(state, cleanup_key),
                 :ok <- do_put(state, cleanup_key, cleanup_value, 0) do
              {:cont, {:ok, %{owner | cleanup_tail: sequence}}}
            else
              existing when is_binary(existing) ->
                {:halt, {:error, "ERR flow limit cleanup record already exists"}}

              {:error, _reason} = error ->
                {:halt, error}
            end
        end)
      end

      defp flow_limit_cleanup_page(state, key, owner)
           when owner.cleanup_head > owner.cleanup_tail do
        flow_limit_write_owner(
          state,
          key,
          owner,
          {:ok, %{deleted: 0, pending?: false}}
        )
      end

      defp flow_limit_cleanup_page(state, key, owner) do
        cleanup_key = FlowKeys.governance_limit_cleanup_key(owner.scope, owner.cleanup_head)

        with :ok <- flow_limit_validate_storage_key(cleanup_key),
             cleanup_value when is_binary(cleanup_value) <- do_get(state, cleanup_key),
             {:ok, cleanup} <- LimitRecord.decode_cleanup(cleanup_value),
             true <- flow_limit_shard_valid?(state, cleanup.shard_id),
             page_key <-
               FlowKeys.governance_limit_reservation_page_key(
                 owner.scope,
                 cleanup.shard_id,
                 cleanup.epoch,
                 cleanup.next_page
               ),
             :ok <- flow_limit_validate_storage_key(page_key),
             page_value when is_binary(page_value) <- do_get(state, page_key),
             {:ok, reservation_ids} <- LimitRecord.decode_page(page_value),
             :ok <-
               flow_limit_delete_cleanup_reservations(
                 state,
                 owner.scope,
                 cleanup.shard_id,
                 cleanup.epoch,
                 reservation_ids
               ),
             :ok <- do_delete(state, page_key),
             {:ok, owner} <-
               flow_limit_advance_cleanup(state, owner, cleanup_key, cleanup) do
          flow_limit_write_owner(
            state,
            key,
            owner,
            {:ok,
             %{
               deleted: length(reservation_ids),
               pending?: owner.cleanup_head <= owner.cleanup_tail
             }}
          )
        else
          nil -> {:error, "ERR flow limit cleanup record is missing"}
          false -> {:error, "ERR flow limit cleanup record has an invalid shard"}
          {:error, _reason} = error -> error
        end
      end

      defp flow_limit_delete_cleanup_reservations(
             state,
             scope,
             shard_id,
             epoch,
             reservation_ids
           ) do
        Enum.reduce_while(reservation_ids, :ok, fn reservation_id, :ok ->
          reservation_key =
            FlowKeys.governance_limit_reservation_key(
              scope,
              shard_id,
              epoch,
              reservation_id
            )

          with :ok <- flow_limit_validate_storage_key(reservation_key),
               :ok <- do_delete(state, reservation_key) do
            {:cont, :ok}
          else
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_limit_advance_cleanup(
             state,
             owner,
             cleanup_key,
             %{next_page: page, last_page: page}
           ) do
        with :ok <- do_delete(state, cleanup_key) do
          {:ok, %{owner | cleanup_head: owner.cleanup_head + 1}}
        end
      end

      defp flow_limit_advance_cleanup(state, owner, cleanup_key, cleanup) do
        value =
          LimitRecord.encode_cleanup(
            cleanup.shard_id,
            cleanup.epoch,
            cleanup.next_page + 1,
            cleanup.last_page
          )

        with :ok <- flow_limit_validate_storage_value(value),
             :ok <- do_put(state, cleanup_key, value, 0) do
          {:ok, owner}
        end
      end

      defp flow_limit_load_owner(state, key, %{op: :lease} = attrs) do
        case do_get(state, key) do
          nil ->
            case attrs.configuration do
              %{limit: limit, config_version: config_version, policy_version: policy_version}
              when is_integer(limit) and limit >= 0 ->
                {:ok,
                 CreditLease.owner(attrs.scope, limit,
                   config_version: config_version || 0,
                   policy_version: policy_version
                 ), true}

              _invalid ->
                {:error, "ERR flow limit limit must be a non-negative integer"}
            end

          value when is_binary(value) ->
            with {:ok, owner} <- flow_limit_decode_owner(state, value, attrs.scope) do
              {:ok, owner, false}
            end

          _invalid ->
            {:error, "ERR flow limit record is corrupt"}
        end
      end

      defp flow_limit_load_owner(state, key, attrs) do
        case do_get(state, key) do
          nil ->
            {:error, "ERR flow limit not found"}

          value when is_binary(value) ->
            with {:ok, owner} <- flow_limit_decode_owner(state, value, attrs.scope) do
              {:ok, owner, false}
            end

          _invalid ->
            {:error, "ERR flow limit record is corrupt"}
        end
      end

      defp flow_limit_maybe_enqueue_catalog_publication(_state, _key, false), do: :ok

      defp flow_limit_maybe_enqueue_catalog_publication(state, owner_key, true) do
        shard_index = state.shard_index
        meta_key = FlowKeys.governance_limit_catalog_outbox_meta_key(shard_index)

        with {:ok, meta} <-
               Ferricstore.Flow.Governance.LimitCatalogOutbox.decode_meta(do_get(state, meta_key)),
             {:ok, pending?} <-
               flow_limit_catalog_publication_pending?(state, shard_index, meta, owner_key) do
          if pending? do
            :ok
          else
            flow_limit_append_catalog_publication(
              state,
              shard_index,
              meta_key,
              meta,
              owner_key
            )
          end
        end
      end

      defp flow_limit_catalog_publication_pending?(
             _state,
             _shard_index,
             %{head: head, tail: tail},
             _owner_key
           )
           when head > tail,
           do: {:ok, false}

      defp flow_limit_catalog_publication_pending?(
             state,
             shard_index,
             %{tail: tail},
             owner_key
           ) do
        intent_key = FlowKeys.governance_limit_catalog_outbox_intent_key(shard_index, tail)

        case do_get(state, intent_key) do
          value when is_binary(value) ->
            with {:ok, pending_owner_key} <-
                   Ferricstore.Flow.Governance.LimitCatalogOutbox.decode_intent(value) do
              {:ok, pending_owner_key == owner_key}
            end

          _missing_or_invalid ->
            {:error, "ERR flow limit catalog publication entry is missing or corrupt"}
        end
      end

      defp flow_limit_append_catalog_publication(
             state,
             shard_index,
             meta_key,
             meta,
             owner_key
           ) do
        with {:ok, next_meta, sequence} <-
               Ferricstore.Flow.Governance.LimitCatalogOutbox.append(meta, owner_key),
             intent_key <-
               FlowKeys.governance_limit_catalog_outbox_intent_key(shard_index, sequence) do
          case do_get(state, intent_key) do
            nil ->
              with :ok <- flow_limit_put_catalog_publication_intent(state, intent_key, owner_key) do
                flow_limit_put_catalog_publication_meta(state, meta_key, next_meta)
              end

            value when is_binary(value) ->
              with {:ok, pending_owner_key} <-
                     Ferricstore.Flow.Governance.LimitCatalogOutbox.decode_intent(value) do
                cond do
                  pending_owner_key == owner_key ->
                    flow_limit_put_catalog_publication_meta(state, meta_key, next_meta)

                  is_nil(do_get(state, pending_owner_key)) ->
                    with :ok <-
                           flow_limit_put_catalog_publication_intent(
                             state,
                             intent_key,
                             owner_key
                           ) do
                      flow_limit_put_catalog_publication_meta(state, meta_key, next_meta)
                    end

                  true ->
                    {:error, "ERR flow limit catalog publication entry already exists"}
                end
              else
                {:error, _reason} = error -> error
              end

            _invalid ->
              {:error, "ERR flow limit catalog publication entry is corrupt"}
          end
        end
      end

      defp flow_limit_put_catalog_publication_intent(state, intent_key, owner_key) do
        do_put(
          state,
          intent_key,
          Ferricstore.Flow.Governance.LimitCatalogOutbox.encode_intent(owner_key),
          0
        )
      end

      defp flow_limit_put_catalog_publication_meta(state, meta_key, meta) do
        do_put(
          state,
          meta_key,
          Ferricstore.Flow.Governance.LimitCatalogOutbox.encode_meta(meta),
          0
        )
      end

      defp do_flow_governance_limit_catalog_outbox_ack(
             state,
             shard_index,
             expected_head,
             up_to
           )
           when is_integer(shard_index) and shard_index >= 0 and is_integer(expected_head) and
                  expected_head > 0 and is_integer(up_to) and up_to >= expected_head and
                  up_to - expected_head < 256 do
        if shard_index == state.shard_index do
          meta_key = FlowKeys.governance_limit_catalog_outbox_meta_key(shard_index)

          with {:ok, meta} <-
                 Ferricstore.Flow.Governance.LimitCatalogOutbox.decode_meta(
                   do_get(state, meta_key)
                 ),
               {:ok, next_meta, acknowledged} <-
                 Ferricstore.Flow.Governance.LimitCatalogOutbox.acknowledge(
                   meta,
                   expected_head,
                   up_to
                 ),
               :ok <-
                 flow_limit_delete_catalog_publications(state, shard_index, acknowledged) do
            do_put(
              state,
              meta_key,
              Ferricstore.Flow.Governance.LimitCatalogOutbox.encode_meta(next_meta),
              0
            )
          end
        else
          {:error, "ERR flow limit catalog publication shard mismatch"}
        end
      end

      defp do_flow_governance_limit_catalog_outbox_ack(
             _state,
             _shard_index,
             _expected_head,
             _up_to
           ),
           do: {:error, "ERR invalid flow limit catalog publication acknowledgement"}

      defp flow_limit_delete_catalog_publications(state, shard_index, sequences) do
        Enum.reduce_while(sequences, :ok, fn sequence, :ok ->
          key = FlowKeys.governance_limit_catalog_outbox_intent_key(shard_index, sequence)

          case do_delete(state, key) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_limit_decode_owner(state, value, scope) do
        with {:ok, owner} <- LimitRecord.decode_owner(value),
             true <- owner.scope == scope,
             true <- flow_limit_owner_shards_valid?(state, owner) do
          {:ok, owner}
        else
          false -> {:error, "ERR flow limit record scope mismatch"}
          {:error, _reason} = error -> error
        end
      end

      defp flow_limit_apply_configuration(owner, nil), do: {:ok, owner}
      defp flow_limit_apply_configuration(owner, %{limit: nil}), do: {:ok, owner}

      defp flow_limit_apply_configuration(
             owner,
             %{limit: limit, config_version: nil}
           ) do
        if limit == owner.limit do
          {:ok, owner}
        else
          {:error, "ERR flow limit config_version is required to change limit"}
        end
      end

      defp flow_limit_apply_configuration(owner, configuration) do
        case CreditLease.reconfigure(
               owner,
               configuration.limit,
               configuration.config_version,
               configuration.policy_version
             ) do
          {:ok, owner} -> {:ok, owner}
          {:error, reason, _owner} -> {:error, reason}
        end
      end

      defp flow_limit_write_owner(state, key, owner, reply) do
        owner = LimitRecord.detach_reservations(owner)

        with {:ok, value} <- LimitRecord.encode_owner(owner),
             :ok <- flow_limit_validate_storage_key(key),
             :ok <- flow_limit_validate_storage_value(value),
             :ok <- do_put(state, key, value, 0) do
          {:flow_limit_reply, reply}
        end
      end

      defp flow_limit_validate_request(state, key, %{scope: scope, op: op} = attrs)
           when is_binary(scope) and scope != "" and
                  op in [:lease, :spend, :release, :renew, :get, :cleanup] do
        with true <- key == FlowKeys.governance_limit_key(scope),
             :ok <- flow_limit_validate_storage_key(key),
             true <- flow_limit_request_shard_count_valid?(state, attrs),
             :ok <- flow_limit_validate_operation(attrs) do
          :ok
        else
          false -> {:error, "ERR invalid flow limit mutation key"}
          {:error, _reason} = error -> error
        end
      end

      defp flow_limit_validate_request(_state, _key, _attrs),
        do: {:error, "ERR invalid flow limit mutation"}

      defp flow_limit_validate_operation(%{
             op: :lease,
             shard_id: shard_id,
             shard_count: shard_count,
             amount: amount,
             ttl_ms: ttl_ms,
             now_ms: now_ms,
             configuration: configuration
           }) do
        with :ok <- flow_limit_validate_shard_amount(shard_id, shard_count, amount),
             true <- flow_limit_valid_deadline?(now_ms, ttl_ms),
             true <- flow_limit_valid_configuration?(configuration) do
          :ok
        else
          _invalid -> {:error, "ERR invalid flow limit lease mutation"}
        end
      end

      defp flow_limit_validate_operation(%{
             op: :spend,
             shard_id: shard_id,
             shard_count: shard_count,
             lease_epoch: lease_epoch,
             amount: amount,
             ttl_ms: ttl_ms,
             now_ms: now_ms,
             reservation_ids: reservation_ids,
             configuration: configuration
           }) do
        with :ok <- flow_limit_validate_shard_amount(shard_id, shard_count, amount),
             true <-
               is_integer(lease_epoch) and lease_epoch >= 0 and
                 lease_epoch <= @flow_limit_max_exact_version,
             true <- flow_limit_valid_deadline?(now_ms, ttl_ms),
             true <- flow_limit_valid_reservation_ids?(reservation_ids, amount),
             true <- flow_limit_valid_configuration?(configuration) do
          :ok
        else
          _invalid -> {:error, "ERR invalid flow limit spend mutation"}
        end
      end

      defp flow_limit_validate_operation(%{
             op: :release,
             shard_id: shard_id,
             shard_count: shard_count,
             amount: amount,
             now_ms: now_ms,
             reservation_ids: reservation_ids
           }) do
        with :ok <- flow_limit_validate_shard_amount(shard_id, shard_count, amount),
             true <- flow_limit_valid_timestamp?(now_ms),
             true <- flow_limit_valid_reservation_ids?(reservation_ids, amount) do
          :ok
        else
          _invalid -> {:error, "ERR invalid flow limit release mutation"}
        end
      end

      defp flow_limit_validate_operation(%{
             op: :renew,
             shard_id: shard_id,
             shard_count: shard_count,
             ttl_ms: ttl_ms,
             now_ms: now_ms
           }) do
        with true <- is_integer(shard_count) and shard_count > 0,
             true <- is_integer(shard_id) and shard_id >= 0 and shard_id < shard_count,
             true <- flow_limit_valid_deadline?(now_ms, ttl_ms) do
          :ok
        else
          _invalid -> {:error, "ERR invalid flow limit renew mutation"}
        end
      end

      defp flow_limit_validate_operation(%{op: op, now_ms: now_ms})
           when op in [:get, :cleanup] and is_integer(now_ms) and now_ms >= 0 and
                  now_ms <= @flow_limit_max_exact_version,
           do: :ok

      defp flow_limit_validate_operation(_attrs),
        do: {:error, "ERR invalid flow limit mutation"}

      defp flow_limit_validate_shard_amount(shard_id, shard_count, amount)
           when is_integer(shard_count) and shard_count > 0 and is_integer(shard_id) and
                  shard_id >= 0 and shard_id < shard_count and is_integer(amount) and amount > 0 and
                  amount <= @flow_limit_max_amount,
           do: :ok

      defp flow_limit_validate_shard_amount(_shard_id, _shard_count, _amount),
        do: {:error, "ERR invalid flow limit mutation amount"}

      defp flow_limit_valid_reservation_ids?(reservation_ids, amount)
           when is_list(reservation_ids) and length(reservation_ids) == amount do
        length(Enum.uniq(reservation_ids)) == amount and
          Enum.all?(reservation_ids, &LimitRecord.valid_reservation_id?/1)
      end

      defp flow_limit_valid_reservation_ids?(_reservation_ids, _amount), do: false

      defp flow_limit_valid_configuration?(%{
             limit: limit,
             config_version: config_version,
             policy_version: policy_version
           }) do
        (is_nil(limit) or
           (is_integer(limit) and limit >= 0 and limit <= @flow_limit_max_exact_version)) and
          (is_nil(config_version) or
             (is_integer(config_version) and config_version >= 0 and
                config_version <= @flow_limit_max_exact_version)) and
          (is_nil(policy_version) or
             match?(
               {:sha256, digest} when is_binary(digest) and byte_size(digest) == 32,
               policy_version
             )) and
          (not is_nil(limit) or (is_nil(config_version) and is_nil(policy_version)))
      end

      defp flow_limit_valid_configuration?(_configuration), do: false

      defp flow_limit_valid_timestamp?(value),
        do:
          is_integer(value) and value >= 0 and
            value <= @flow_limit_max_exact_version

      defp flow_limit_valid_deadline?(now_ms, nil), do: flow_limit_valid_timestamp?(now_ms)

      defp flow_limit_valid_deadline?(now_ms, ttl_ms) do
        flow_limit_valid_timestamp?(now_ms) and is_integer(ttl_ms) and ttl_ms > 0 and
          ttl_ms <= @flow_limit_max_exact_version and
          now_ms <= @flow_limit_max_exact_version - ttl_ms
      end

      defp flow_limit_request_shard_count_valid?(
             %{instance_ctx: %{shard_count: shard_count}},
             %{shard_count: shard_count}
           )
           when is_integer(shard_count) and shard_count > 0,
           do: true

      defp flow_limit_request_shard_count_valid?(_state, %{op: op})
           when op in [:get, :cleanup],
           do: true

      defp flow_limit_request_shard_count_valid?(_state, _attrs), do: false

      defp flow_limit_owner_shards_valid?(
             %{instance_ctx: %{shard_count: shard_count}},
             owner
           )
           when is_integer(shard_count) and shard_count > 0 do
        map_size(owner.leases) <= shard_count and
          Enum.all?(owner.leases, fn {shard_id, _lease} ->
            is_integer(shard_id) and shard_id >= 0 and shard_id < shard_count
          end)
      end

      defp flow_limit_owner_shards_valid?(_state, _owner), do: false

      defp flow_limit_shard_valid?(
             %{instance_ctx: %{shard_count: shard_count}},
             shard_id
           ),
           do:
             is_integer(shard_count) and shard_count > 0 and is_integer(shard_id) and
               shard_id >= 0 and shard_id < shard_count

      defp flow_limit_validate_storage_key(key)
           when is_binary(key) and key != "" and byte_size(key) <= @flow_max_key_size,
           do: :ok

      defp flow_limit_validate_storage_key(_key),
        do: {:error, "ERR invalid flow limit storage key"}

      defp flow_limit_validate_storage_value(value)
           when is_binary(value) and byte_size(value) <= 131_072,
           do: :ok

      defp flow_limit_validate_storage_value(_value),
        do: {:error, "ERR invalid flow limit storage value"}
    end
  end
end
