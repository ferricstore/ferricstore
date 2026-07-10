defmodule Ferricstore.Raft.StateMachine.Sections.FlowPolicyMigration do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]

      alias Ferricstore.Flow
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.PolicyMigration
      alias Ferricstore.Flow.RetryPolicy
      alias Ferricstore.Flow.StateMeta

      defp flow_put_type_catalog_member(
             state,
             %{id: id, type: type} = record
           )
           when is_binary(id) and is_binary(type) and type != "" do
        if Process.get(:sm_flow_catalog_maintenance_suspended, false) do
          :ok
        else
          state_key = FlowKeys.state_key(id, Map.get(record, :partition_key))
          flow_put_type_catalog_member(state, state_key, record)
        end
      end

      defp flow_put_type_catalog_member(
             state,
             state_key,
             %{id: id, type: type} = record
           )
           when is_binary(state_key) and is_binary(id) and is_binary(type) and type != "" do
        if Process.get(:sm_flow_catalog_maintenance_suspended, false) do
          :ok
        else
          expected_state_key = FlowKeys.state_key(id, Map.get(record, :partition_key))

          if state_key == expected_state_key do
            flow_do_put_type_catalog_member(state, state_key, type, record)
          else
            {:error, "ERR invalid flow type catalog state key"}
          end
        end
      end

      defp flow_do_put_type_catalog_member(state, state_key, type, record) do
        catalog_key = FlowKeys.type_catalog_member_key(type, state_key)
        captured_generation = flow_policy_catalog_generation(state, type)
        expire_at_ms = 0

        case PolicyMigration.decode_catalog(do_get(state, catalog_key)) do
          {:ok, catalog} ->
            if flow_catalog_owned?(catalog, type, state_key) do
              generation = max(captured_generation, catalog.migration_generation)
              value = PolicyMigration.encode_catalog(type, state_key, generation)

              with {:ok, _revision} <- flow_ensure_type_descriptor(state, type, false),
                   :ok <-
                     flow_put_type_catalog_value(
                       state,
                       catalog_key,
                       type,
                       value,
                       expire_at_ms,
                       catalog.migration_generation
                     ) do
                flow_reopen_stale_policy_migration(state, type, generation)
              end
            else
              {:error, "ERR flow type catalog ownership mismatch"}
            end

          :error ->
            if is_nil(do_get(state, catalog_key)) do
              generation = captured_generation
              value = PolicyMigration.encode_catalog(type, state_key, generation)

              with {:ok, revision} <- flow_ensure_type_descriptor(state, type, true),
                   :ok <-
                     flow_put_type_catalog_value(
                       state,
                       catalog_key,
                       type,
                       value,
                       expire_at_ms,
                       nil
                     ),
                   :ok <- flow_advance_active_job_barrier(state, type, revision) do
                flow_reopen_stale_policy_migration(state, type, generation)
              end
            else
              {:error, "ERR flow type catalog entry is corrupt"}
            end
        end
      end

      defp flow_put_type_catalog_member(_state, _record),
        do: {:error, "ERR invalid flow type catalog record"}

      defp flow_put_type_catalog_member(_state, _state_key, _record),
        do: {:error, "ERR invalid flow type catalog record"}

      defp flow_delete_type_catalog_member(state, %{id: id, type: type} = record)
           when is_binary(id) and is_binary(type) and type != "" do
        state_key = FlowKeys.state_key(id, Map.get(record, :partition_key))
        catalog_key = FlowKeys.type_catalog_member_key(type, state_key)

        case PolicyMigration.decode_catalog(do_get(state, catalog_key)) do
          {:ok, catalog} ->
            if flow_catalog_owned?(catalog, type, state_key) do
              with {:ok, revision} <- flow_ensure_type_descriptor(state, type, true),
                   :ok <-
                     flow_delete_type_catalog_value(
                       state,
                       catalog_key,
                       type,
                       catalog.migration_generation
                     ) do
                flow_advance_active_job_barrier(state, type, revision)
              end
            else
              {:error, "ERR flow type catalog ownership mismatch"}
            end

          :error ->
            if is_nil(do_get(state, catalog_key)),
              do: :ok,
              else: {:error, "ERR flow type catalog entry is corrupt"}
        end
      end

      defp flow_delete_type_catalog_member(_state, _record), do: :ok

      defp flow_catalog_projection_newer?(state, %{id: id, type: type} = record)
           when is_binary(id) and is_binary(type) and type != "" do
        state_key = FlowKeys.state_key(id, Map.get(record, :partition_key))
        catalog_key = FlowKeys.type_catalog_member_key(type, state_key)

        case PolicyMigration.decode_catalog(do_get(state, catalog_key)) do
          {:ok, catalog} ->
            flow_catalog_owned?(catalog, type, state_key) and
              catalog.migration_generation > flow_read_policy_generation(state, type)

          _missing_or_invalid ->
            false
        end
      end

      defp flow_catalog_projection_newer?(_state, _record), do: false

      defp flow_catalog_owned?(catalog, _type, state_key),
        do: catalog.state_key == state_key

      defp flow_enqueue_policy_migration(state, type, policy_generation, indexed_state_meta)
           when is_binary(type) and type != "" and is_integer(policy_generation) and
                  policy_generation >= 0 and
                  (is_nil(indexed_state_meta) or is_binary(indexed_state_meta)) do
        with {:ok, migration_generation} <-
               flow_policy_migration_target_generation(state, type, policy_generation) do
          case flow_existing_policy_migration(
                 state,
                 type,
                 migration_generation,
                 indexed_state_meta
               ) do
            :complete ->
              :ok

            {:active, job_key, job} ->
              flow_put_active_policy_migration(state, job_key, %{
                job
                | membership_revision: flow_type_membership_revision(state, type)
              })

            :new ->
              job_key = FlowKeys.policy_migration_job_key(type)

              flow_put_active_policy_migration(state, job_key, %{
                type: type,
                migration_generation: migration_generation,
                membership_revision: flow_type_membership_revision(state, type),
                indexed_state_meta: indexed_state_meta,
                status: :active
              })

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp flow_policy_migration_target_generation(_state, _type, policy_generation)
           when policy_generation > 0,
           do: {:ok, policy_generation}

      defp flow_policy_migration_target_generation(state, type, 0) do
        state
        |> flow_policy_migration_marker_generation(type)
        |> max(flow_read_policy_generation(state, type))
        |> PolicyMigration.next_generation()
      end

      defp flow_existing_policy_migration(state, type, generation, indexed_state_meta) do
        marker_key = FlowKeys.policy_migration_marker_key(type)

        case flow_decode_policy_migration_job(do_get(state, marker_key)) do
          {:ok, %{status: :done, migration_generation: marker_generation}}
          when marker_generation > generation ->
            :complete

          {:ok,
           %{
             status: :done,
             migration_generation: ^generation,
             indexed_state_meta: ^indexed_state_meta
           }} ->
            :complete

          {:ok, %{status: :done, migration_generation: ^generation}} ->
            {:error, "ERR conflicting flow policy migration generation"}

          _not_complete ->
            job_key = FlowKeys.policy_migration_job_key(type)

            case flow_decode_policy_migration_job(do_get(state, job_key)) do
              {:ok, %{status: :active, migration_generation: active_generation} = job}
              when active_generation > generation ->
                {:active, job_key, job}

              {:ok,
               %{
                 status: :active,
                 migration_generation: ^generation,
                 indexed_state_meta: ^indexed_state_meta
               } = job} ->
                {:active, job_key, job}

              {:ok, %{status: :active, migration_generation: ^generation}} ->
                {:error, "ERR conflicting flow policy migration generation"}

              _missing_or_different ->
                :new
            end
        end
      end

      defp flow_put_active_policy_migration(state, job_key, job) do
        value =
          PolicyMigration.encode_job(
            job.type,
            job.migration_generation,
            job.membership_revision,
            job.indexed_state_meta,
            :active
          )

        flow_put_policy_migration_job_value(state, job_key, value)
      end

      defp do_flow_policy_migration_step(
             state,
             %{
               job_key: job_key,
               type: type,
               migration_generation: expected_generation,
               membership_revision: expected_membership_revision,
               indexed_state_meta: indexed_state_meta,
               catalog_entries: catalog_entries,
               done?: done?,
               backfill_proof: %{
                 run_token: backfill_run_token,
                 source_token: backfill_source_token
               }
             }
           )
           when is_binary(job_key) and is_binary(type) and type != "" and
                  is_integer(expected_generation) and expected_generation >= 0 and
                  expected_generation <= 9_007_199_254_740_991 and
                  is_integer(expected_membership_revision) and
                  expected_membership_revision >= 0 and
                  expected_membership_revision <= 0xFFFFFFFFFFFFFFFF and
                  (is_nil(indexed_state_meta) or is_binary(indexed_state_meta)) and
                  is_list(catalog_entries) and is_boolean(done?) and
                  length(catalog_entries) <= 256 and is_binary(backfill_run_token) and
                  is_binary(backfill_source_token) do
        with true <- Enum.all?(catalog_entries, &flow_valid_policy_catalog_plan_entry?/1),
             true <- FlowKeys.policy_migration_job_key(type) == job_key,
             :ok <-
               flow_require_completed_policy_catalog_backfill(
                 state,
                 backfill_run_token,
                 backfill_source_token
               ) do
          flow_policy_migration_apply_plan(
            state,
            job_key,
            type,
            expected_generation,
            expected_membership_revision,
            indexed_state_meta,
            catalog_entries,
            done?
          )
        else
          false -> {:error, "ERR invalid flow policy migration plan"}
          {:error, _reason} = error -> error
        end
      end

      defp do_flow_policy_migration_step(_state, _plan),
        do: {:error, "ERR invalid flow policy migration plan"}

      defp flow_valid_policy_catalog_plan_entry?(%{
             catalog_key: catalog_key,
             migration_generation: generation,
             record_value: record_value
           })
           when is_binary(catalog_key) and is_integer(generation) and generation >= 0 and
                  generation <= 9_007_199_254_740_991 and
                  (is_binary(record_value) or is_nil(record_value)),
           do: true

      defp flow_valid_policy_catalog_plan_entry?(_entry), do: false

      defp flow_require_completed_policy_catalog_backfill(
             state,
             run_token,
             source_token
           ) do
        progress_key = FlowKeys.policy_catalog_backfill_key(state.shard_index)

        case PolicyMigration.decode_backfill_progress(do_get(state, progress_key)) do
          {:ok, %{run_token: ^run_token, source_token: ^source_token, status: :done}} ->
            :ok

          _incomplete ->
            {:error, "ERR flow policy catalog backfill incomplete"}
        end
      end

      defp flow_policy_migration_apply_plan(
             state,
             job_key,
             type,
             expected_generation,
             expected_membership_revision,
             indexed_state_meta,
             catalog_entries,
             done?
           ) do
        planned_job = %{
          type: type,
          migration_generation: expected_generation,
          membership_revision: expected_membership_revision,
          indexed_state_meta: indexed_state_meta,
          status: :active
        }

        case flow_policy_migration_plan_job_state(state, job_key, planned_job) do
          {:apply, job} ->
            flow_policy_migration_apply_active_plan(
              state,
              job_key,
              job,
              catalog_entries,
              done?
            )

          :already_complete ->
            {:ok, %{processed: 0, done?: true, idle?: false}}

          :superseded ->
            {:ok, %{processed: 0, done?: true, idle?: false}}

          :stale_revision ->
            {:ok, %{processed: 0, done?: false, idle?: false}}

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_policy_migration_plan_job_state(state, job_key, planned_job) do
        case flow_decode_policy_migration_job(do_get(state, job_key)) do
          {:ok, ^planned_job} ->
            {:apply, planned_job}

          {:ok, %{status: :active, migration_generation: generation}}
          when generation > planned_job.migration_generation ->
            :superseded

          {:ok,
           %{
             status: :active,
             migration_generation: generation,
             membership_revision: revision
           }}
          when generation == planned_job.migration_generation and
                 revision > planned_job.membership_revision ->
            :stale_revision

          {:ok,
           %{
             status: :active,
             migration_generation: generation,
             indexed_state_meta: indexed_state_meta
           }}
          when generation == planned_job.migration_generation and
                 indexed_state_meta != planned_job.indexed_state_meta ->
            {:error, "ERR conflicting flow policy migration generation"}

          {:ok, %{status: :active}} ->
            flow_install_planned_policy_migration(state, job_key, planned_job)

          {:ok, %{status: :done}} ->
            :already_complete

          :missing ->
            marker_key = FlowKeys.policy_migration_marker_key(planned_job.type)

            case flow_decode_policy_migration_job(do_get(state, marker_key)) do
              {:ok, %{status: :done, migration_generation: generation}}
              when generation > planned_job.migration_generation ->
                :already_complete

              {:ok,
               %{
                 status: :done,
                 migration_generation: generation,
                 indexed_state_meta: indexed_state_meta
               }}
              when generation == planned_job.migration_generation and
                     indexed_state_meta == planned_job.indexed_state_meta ->
                :already_complete

              {:ok, %{status: :done, migration_generation: generation}}
              when generation == planned_job.migration_generation ->
                {:error, "ERR conflicting flow policy migration generation"}

              _not_complete ->
                flow_install_planned_policy_migration(state, job_key, planned_job)
            end

          :error ->
            {:error, "ERR flow policy migration job is corrupt"}
        end
      end

      defp flow_install_planned_policy_migration(state, job_key, job) do
        value =
          PolicyMigration.encode_job(
            job.type,
            job.migration_generation,
            job.membership_revision,
            job.indexed_state_meta,
            :active
          )

        with :ok <- flow_put_policy_migration_job_value(state, job_key, value) do
          {:apply, job}
        end
      end

      defp do_flow_policy_catalog_backfill_step(
             state,
             %{
               run_token: run_token,
               source_token: source_token,
               expected_cursor: expected_cursor,
               cursor: cursor,
               candidates: candidates,
               done?: done?
             }
           )
           when is_binary(run_token) and run_token != "" and byte_size(run_token) <= 64 and
                  is_binary(source_token) and source_token != "" and
                  byte_size(source_token) <= 64 and is_binary(expected_cursor) and
                  is_binary(cursor) and cursor >= expected_cursor and is_list(candidates) and
                  length(candidates) <= 256 and is_boolean(done?) do
        progress_key = FlowKeys.policy_catalog_backfill_key(state.shard_index)

        with {:ok, action} <-
               flow_policy_catalog_backfill_action(
                 do_get(state, progress_key),
                 run_token,
                 source_token,
                 expected_cursor,
                 cursor,
                 done?
               ) do
          case action do
            :already_applied ->
              {:ok, %{processed: 0, cursor: cursor, done?: done?, already_applied?: true}}

            :apply ->
              with :ok <- flow_policy_catalog_backfill_candidates(state, candidates),
                   :ok <-
                     flow_put_policy_catalog_backfill_progress(
                       state,
                       progress_key,
                       run_token,
                       source_token,
                       cursor,
                       if(done?, do: :done, else: :active)
                     ) do
                {:ok,
                 %{
                   processed: length(candidates),
                   cursor: cursor,
                   done?: done?,
                   already_applied?: false
                 }}
              end
          end
        end
      end

      defp do_flow_policy_catalog_backfill_step(_state, _request),
        do: {:error, "ERR invalid flow policy catalog backfill step"}

      defp flow_policy_catalog_backfill_action(
             value,
             run_token,
             source_token,
             expected_cursor,
             cursor,
             done?
           ) do
        case PolicyMigration.decode_backfill_progress(value) do
          {:ok,
           %{
             run_token: ^run_token,
             source_token: ^source_token,
             cursor: ^cursor,
             status: status
           }}
          when (done? and status == :done) or (not done? and status == :active) ->
            {:ok, :already_applied}

          {:ok,
           %{
             run_token: ^run_token,
             source_token: ^source_token,
             cursor: ^expected_cursor,
             status: :active
           }} ->
            {:ok, :apply}

          {:ok, %{source_token: existing_source}}
          when expected_cursor == "" and existing_source != source_token ->
            {:ok, :apply}

          {:ok, %{status: :done}} when expected_cursor == "" ->
            {:ok, :apply}

          :error when is_nil(value) and expected_cursor == "" ->
            {:ok, :apply}

          :error when is_nil(value) ->
            {:error, "ERR stale flow policy catalog backfill cursor"}

          :error ->
            {:error, "ERR flow policy catalog backfill progress is corrupt"}

          {:ok, _other} ->
            {:error, "ERR stale flow policy catalog backfill cursor"}
        end
      end

      defp flow_policy_catalog_backfill_candidates(state, candidates) do
        Enum.reduce_while(candidates, :ok, fn candidate, :ok ->
          case flow_policy_catalog_backfill_candidate(state, candidate) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_policy_catalog_backfill_candidate(
             state,
             %{
               kind: :catalog,
               catalog_key: catalog_key,
               descriptor_key: descriptor_key
             }
           )
           when is_binary(catalog_key) and is_binary(descriptor_key) do
        case PolicyMigration.decode_catalog(do_get(state, catalog_key)) do
          {:ok, %{state_key: state_key, migration_generation: generation} = catalog} ->
            with {:ok, exact_type} <- flow_catalog_descriptor_type(state, descriptor_key),
                 true <- FlowKeys.type_catalog_member_owns_state_key?(catalog_key, state_key),
                 true <- flow_catalog_owned?(catalog, exact_type, state_key),
                 true <- FlowKeys.type_catalog_member_key(exact_type, state_key) == catalog_key,
                 compact = PolicyMigration.encode_catalog(exact_type, state_key, generation),
                 :ok <-
                   flow_put_type_catalog_value(
                     state,
                     catalog_key,
                     exact_type,
                     compact,
                     0,
                     generation
                   ),
                 :ok <- flow_queue_type_descriptor_barrier(state, exact_type) do
              flow_queue_policy_catalog_projection(exact_type, catalog_key, generation)
            else
              false -> {:error, "ERR flow type catalog ownership mismatch"}
              {:error, _reason} = error -> error
            end

          :error ->
            if is_nil(do_get(state, catalog_key)) do
              with_lmdb_mirror_shard(state, fn ->
                queue_pending_lmdb_mirror_delete(catalog_key)
              end)

              :ok
            else
              {:error, "ERR flow type catalog entry is corrupt"}
            end

          {:ok, _mismatch} ->
            {:error, "ERR flow type catalog ownership mismatch"}
        end
      end

      defp flow_policy_catalog_backfill_candidate(
             state,
             %{kind: :state, state_key: state_key, record_value: record_value}
           )
           when is_binary(state_key) and (is_binary(record_value) or is_nil(record_value)) do
        case flow_policy_planned_record(state, state_key, record_value) do
          {:ok, %{type: current_type} = record}
          when is_binary(current_type) and current_type != "" ->
            flow_policy_catalog_backfill_current_record(
              state,
              state_key,
              current_type,
              record
            )

          :missing ->
            :ok

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_policy_catalog_backfill_candidate(state, %{kind: :job, job_key: job_key})
           when is_binary(job_key) do
        case flow_decode_policy_migration_job(do_get(state, job_key)) do
          {:ok, %{status: :active}} ->
            flow_queue_policy_job_barrier(state, job_key)

          {:ok, %{status: :done}} ->
            with_lmdb_mirror_shard(state, fn -> queue_pending_lmdb_mirror_delete(job_key) end)
            :ok

          :missing ->
            with_lmdb_mirror_shard(state, fn -> queue_pending_lmdb_mirror_delete(job_key) end)
            :ok

          :error ->
            {:error, "ERR flow policy migration job is corrupt"}
        end
      end

      defp flow_policy_catalog_backfill_candidate(_state, _candidate),
        do: {:error, "ERR invalid flow policy catalog backfill candidate"}

      defp flow_policy_catalog_backfill_current_record(state, state_key, type, record) do
        catalog_key = FlowKeys.type_catalog_member_key(type, state_key)

        case PolicyMigration.decode_catalog(do_get(state, catalog_key)) do
          {:ok, %{state_key: ^state_key, migration_generation: generation} = catalog} ->
            if flow_catalog_owned?(catalog, type, state_key) do
              with {:ok, _revision} <- flow_ensure_type_descriptor(state, type, false),
                   :ok <- flow_queue_type_descriptor_barrier(state, type) do
                flow_queue_policy_catalog_projection(type, catalog_key, generation)
              end
            else
              {:error, "ERR flow type catalog ownership mismatch"}
            end

          :error ->
            if is_nil(do_get(state, catalog_key)) do
              generation = flow_policy_catalog_generation(state, type)
              value = PolicyMigration.encode_catalog(type, state_key, generation)

              with {:ok, revision} <- flow_ensure_type_descriptor(state, type, true),
                   :ok <-
                     flow_put_type_catalog_value(
                       state,
                       catalog_key,
                       type,
                       value,
                       0,
                       nil
                     ),
                   :ok <- flow_advance_active_job_barrier(state, type, revision) do
                flow_reopen_stale_policy_migration(state, type, generation)
              end
            else
              {:error, "ERR flow type catalog entry is corrupt"}
            end

          {:ok, _mismatch} ->
            {:error, "ERR flow type catalog ownership mismatch"}
        end
      end

      defp flow_policy_planned_record(_state, _state_key, nil), do: :missing

      defp flow_policy_planned_record(state, state_key, record_value)
           when is_binary(state_key) and is_binary(record_value) do
        record = Flow.decode_record(record_value)

        with %{id: id, type: type} when is_binary(id) and is_binary(type) and type != "" <- record,
             true <- FlowKeys.state_key(id, Map.get(record, :partition_key)) == state_key,
             {:ok, guard_key} <- FlowKeys.retention_guard_key_from_state_key(state_key) do
          expected_guard = Ferricstore.Flow.RetentionGuard.encode(record)

          case do_get(state, guard_key) do
            ^expected_guard -> {:ok, record}
            nil -> flow_policy_planned_legacy_record(state, state_key, record)
            _changed -> {:error, "ERR stale flow policy catalog record plan"}
          end
        else
          false -> {:error, "ERR invalid flow policy catalog state record"}
          :error -> {:error, "ERR invalid flow policy catalog state key"}
          _invalid -> {:error, "ERR invalid flow policy catalog state record"}
        end
      rescue
        _error -> {:error, "ERR invalid flow policy catalog state record"}
      end

      defp flow_policy_planned_legacy_record(state, state_key, record) do
        case FlowKeys.registry_key_from_state_key(state_key) do
          {:ok, registry_key} ->
            if is_nil(do_get(state, registry_key)), do: :missing, else: {:ok, record}

          :error ->
            {:error, "ERR invalid flow policy catalog state key"}
        end
      end

      defp flow_put_policy_catalog_backfill_progress(
             state,
             key,
             run_token,
             source_token,
             cursor,
             status
           ) do
        value =
          PolicyMigration.encode_backfill_progress(
            run_token,
            source_token,
            cursor,
            status
          )

        with :ok <- flow_put_hot_value(state, key, value, 0) do
          with_lmdb_mirror_shard(state, fn -> queue_pending_lmdb_mirror_put(key, value, 0) end)
          :ok
        end
      end

      defp flow_policy_migration_apply_active_plan(
             state,
             job_key,
             job,
             catalog_entries,
             done?
           ) do
        with :ok <- flow_policy_migration_process_batch(state, job, catalog_entries),
             {:ok, finished?} <-
               flow_policy_migration_maybe_finish_job(state, job_key, job, done?) do
          {:ok, %{processed: length(catalog_entries), done?: finished?, idle?: false}}
        end
      end

      defp flow_policy_migration_process_batch(state, job, catalog_entries) do
        Enum.reduce_while(catalog_entries, :ok, fn entry, :ok ->
          case flow_policy_migration_process_catalog_member(state, entry, job) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_policy_migration_process_catalog_member(
             state,
             %{
               catalog_key: catalog_key,
               migration_generation: projected_generation,
               record_value: record_value
             },
             job
           ) do
        case PolicyMigration.decode_catalog(do_get(state, catalog_key)) do
          {:ok, catalog} ->
            flow_policy_migration_validate_catalog_member(catalog_key, catalog, job)
            |> case do
              :ok ->
                flow_policy_migration_apply_catalog_member(
                  state,
                  catalog_key,
                  catalog,
                  projected_generation,
                  record_value,
                  job
                )

              {:error, _reason} = error ->
                error
            end

          :error ->
            if is_nil(do_get(state, catalog_key)) do
              flow_delete_policy_catalog_projection(
                job.type,
                catalog_key,
                projected_generation
              )
            else
              {:error, "ERR flow type catalog entry is corrupt"}
            end
        end
      end

      defp flow_policy_migration_validate_catalog_member(catalog_key, catalog, job) do
        expected_key = FlowKeys.type_catalog_member_key(job.type, catalog.state_key)

        cond do
          expected_key != catalog_key ->
            {:error, "ERR flow type catalog ownership mismatch"}

          true ->
            :ok
        end
      end

      defp flow_policy_migration_apply_catalog_member(
             state,
             catalog_key,
             catalog,
             projected_generation,
             record_value,
             job
           ) do
        if catalog.migration_generation >= job.migration_generation do
          with :ok <-
                 flow_maybe_delete_old_policy_catalog_projection(
                   job.type,
                   catalog_key,
                   projected_generation,
                   catalog.migration_generation
                 ) do
            flow_queue_policy_catalog_projection(
              job.type,
              catalog_key,
              catalog.migration_generation
            )
          end
        else
          case flow_policy_planned_record(state, catalog.state_key, record_value) do
            {:ok, %{type: type} = record} when type == job.type ->
              with :ok <-
                     flow_policy_migration_reindex_record(
                       state,
                       catalog.state_key,
                       record,
                       job.indexed_state_meta
                     ),
                   value =
                     PolicyMigration.encode_catalog(
                       job.type,
                       catalog.state_key,
                       job.migration_generation
                     ),
                   :ok <-
                     flow_put_type_catalog_value(
                       state,
                       catalog_key,
                       job.type,
                       value,
                       0,
                       catalog.migration_generation
                     ) do
                :ok
              end

            {:ok, _reused_type} ->
              {:error, "ERR flow type catalog ownership mismatch"}

            :missing ->
              with {:ok, revision} <- flow_ensure_type_descriptor(state, job.type, true),
                   :ok <-
                     flow_delete_type_catalog_value(
                       state,
                       catalog_key,
                       job.type,
                       catalog.migration_generation
                     ) do
                flow_advance_active_job_barrier(state, job.type, revision)
              end

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp flow_policy_migration_reindex_record(state, state_key, record, indexed_state_meta) do
        target_key =
          case StateMeta.record(record) do
            meta when is_map(meta) and map_size(meta) > 0 -> indexed_state_meta
            _other -> nil
          end

        if StateMeta.indexed_key(record) == target_key do
          :ok
        else
          next = StateMeta.put_indexed_key(record, target_key)

          with :ok <-
                 flow_with_catalog_maintenance_suspended(fn ->
                   flow_put_state_record(state, state_key, next)
                 end) do
            with_lmdb_mirror_shard(state, fn ->
              queue_pending_lmdb_flow_state_projection(
                state_key,
                flow_encode(next),
                flow_state_record_expire_at(next)
              )
            end)

            :ok
          end
        end
      end

      defp flow_with_catalog_maintenance_suspended(fun) when is_function(fun, 0) do
        previous = Process.get(:sm_flow_catalog_maintenance_suspended, :undefined)
        Process.put(:sm_flow_catalog_maintenance_suspended, true)

        try do
          fun.()
        after
          case previous do
            :undefined -> Process.delete(:sm_flow_catalog_maintenance_suspended)
            value -> Process.put(:sm_flow_catalog_maintenance_suspended, value)
          end
        end
      end

      defp flow_policy_migration_maybe_finish_job(_state, _job_key, _job, false),
        do: {:ok, false}

      defp flow_policy_migration_maybe_finish_job(state, job_key, job, true) do
        if flow_type_membership_revision(state, job.type) == job.membership_revision do
          value =
            PolicyMigration.encode_job(
              job.type,
              job.migration_generation,
              job.membership_revision,
              job.indexed_state_meta,
              :done
            )

          with :ok <-
                 flow_put_hot_value(
                   state,
                   FlowKeys.policy_migration_marker_key(job.type),
                   value,
                   0
                 ),
               :ok <- flow_delete_policy_migration_job_value(state, job_key) do
            {:ok, true}
          end
        else
          {:ok, false}
        end
      end

      defp flow_policy_catalog_generation(state, type) do
        case Process.get(:sm_flow_policy_snapshots, :legacy) do
          snapshots when is_map(snapshots) ->
            flow_read_policy_generation(state, type)

          :legacy ->
            max(
              flow_read_policy_generation(state, type),
              flow_policy_migration_marker_generation(state, type)
            )
        end
      end

      defp flow_reopen_stale_policy_migration(state, type, catalog_generation) do
        active_key = FlowKeys.policy_migration_job_key(type)

        case flow_decode_policy_migration_job(do_get(state, active_key)) do
          {:ok, %{status: :active, migration_generation: generation}}
          when generation > catalog_generation ->
            :ok

          _no_newer_active_job ->
            flow_reopen_completed_policy_migration(
              state,
              type,
              active_key,
              catalog_generation
            )
        end
      end

      defp flow_reopen_completed_policy_migration(
             state,
             type,
             active_key,
             catalog_generation
           ) do
        marker_key = FlowKeys.policy_migration_marker_key(type)

        case flow_decode_policy_migration_job(do_get(state, marker_key)) do
          {:ok, %{status: :done, migration_generation: generation} = marker}
          when generation > catalog_generation ->
            value =
              PolicyMigration.encode_job(
                type,
                generation,
                flow_type_membership_revision(state, type),
                marker.indexed_state_meta,
                :active
              )

            flow_put_policy_migration_job_value(state, active_key, value)

          _current ->
            :ok
        end
      end

      defp flow_read_policy_generation(_state, type) when not is_binary(type), do: 0

      defp flow_read_policy_generation(state, type) do
        case Process.get(:sm_flow_policy_snapshots, :legacy) do
          %{^type => %{generation: generation}}
          when is_integer(generation) and generation >= 0 ->
            generation

          snapshots when is_map(snapshots) ->
            0

          :legacy ->
            case ets_lookup(state, FlowKeys.policy_key(type)) do
              {:hit, value, _expire_at_ms} when is_binary(value) ->
                case RetryPolicy.decode_flow_policy_entry(value) do
                  {:ok, {generation, _policy}} -> generation
                  :error -> 0
                end

              _missing ->
                0
            end
        end
      end

      defp flow_policy_migration_marker_generation(state, type) do
        [
          FlowKeys.policy_migration_job_key(type),
          FlowKeys.policy_migration_marker_key(type)
        ]
        |> Enum.map(fn key ->
          state
          |> do_get(key)
          |> flow_decode_policy_migration_job()
          |> case do
            {:ok, %{migration_generation: generation}} -> generation
            _missing_or_invalid -> 0
          end
        end)
        |> Enum.max(fn -> 0 end)
      end

      defp flow_decode_policy_migration_job(nil), do: :missing

      defp flow_decode_policy_migration_job(value) do
        case PolicyMigration.decode_job(value) do
          {:ok, job} -> {:ok, job}
          :error -> :error
        end
      end

      defp flow_ensure_type_descriptor(state, type, membership_change?) do
        key = FlowKeys.type_catalog_descriptor_key(type)

        case PolicyMigration.decode_type_descriptor(do_get(state, key)) do
          {:ok, %{type: ^type, membership_revision: revision}} ->
            if membership_change? do
              with {:ok, next_revision} <- flow_next_membership_revision(revision),
                   :ok <- flow_put_type_descriptor_value(state, key, type, next_revision) do
                {:ok, next_revision}
              end
            else
              {:ok, revision}
            end

          {:ok, _collision} ->
            {:error, "ERR flow type catalog digest collision"}

          :error ->
            if is_nil(do_get(state, key)) do
              with {:ok, revision} <-
                     if(membership_change?,
                       do: flow_next_membership_revision(0),
                       else: {:ok, 0}
                     ),
                   :ok <- flow_put_type_descriptor_value(state, key, type, revision) do
                {:ok, revision}
              end
            else
              {:error, "ERR flow type catalog descriptor is corrupt"}
            end
        end
      end

      defp flow_next_membership_revision(revision) when revision < 0xFFFFFFFFFFFFFFFF do
        case current_ra_index() do
          index when is_integer(index) and index >= revision and index <= 0xFFFFFFFFFFFFFFFF ->
            {:ok, index}

          _missing_or_replayed_index ->
            {:ok, revision + 1}
        end
      end

      defp flow_next_membership_revision(_revision),
        do: {:error, "ERR flow type catalog revision exhausted"}

      defp flow_type_membership_revision(state, type) do
        key = FlowKeys.type_catalog_descriptor_key(type)

        case PolicyMigration.decode_type_descriptor(do_get(state, key)) do
          {:ok, %{type: ^type, membership_revision: revision}} -> revision
          _missing_or_invalid -> 0
        end
      end

      defp flow_put_type_descriptor_value(state, key, type, revision) do
        value = PolicyMigration.encode_type_descriptor(type, revision)

        if flow_value_and_expiry_match?(state, key, value, 0) do
          :ok
        else
          with :ok <- flow_put_hot_value(state, key, value, 0) do
            with_lmdb_mirror_shard(state, fn -> queue_pending_lmdb_mirror_put(key, value, 0) end)
            :ok
          end
        end
      end

      defp flow_queue_type_descriptor_barrier(state, type) do
        key = FlowKeys.type_catalog_descriptor_key(type)

        case PolicyMigration.decode_type_descriptor(do_get(state, key)) do
          {:ok, %{type: ^type}} ->
            value = do_get(state, key)

            with_lmdb_mirror_shard(state, fn ->
              queue_pending_lmdb_mirror_put(key, value, 0)
            end)

            :ok

          {:ok, _collision} ->
            {:error, "ERR flow type catalog digest collision"}

          :error ->
            {:error, "ERR flow type catalog descriptor is corrupt"}
        end
      end

      defp flow_catalog_descriptor_type(state, descriptor_key) do
        case PolicyMigration.decode_type_descriptor(do_get(state, descriptor_key)) do
          {:ok, %{type: exact_type}} ->
            if FlowKeys.type_catalog_descriptor_key(exact_type) == descriptor_key,
              do: {:ok, exact_type},
              else: {:error, "ERR flow type catalog digest collision"}

          _missing_or_invalid ->
            {:error, "ERR flow type catalog descriptor is corrupt"}
        end
      end

      defp flow_advance_active_job_barrier(state, type, revision) do
        job_key = FlowKeys.policy_migration_job_key(type)

        case flow_decode_policy_migration_job(do_get(state, job_key)) do
          {:ok, %{status: :active, membership_revision: ^revision}} ->
            flow_queue_policy_job_barrier(state, job_key)

          {:ok, %{status: :active} = job} ->
            flow_put_active_policy_migration(state, job_key, %{
              job
              | membership_revision: revision
            })

          :missing ->
            :ok

          :error ->
            {:error, "ERR flow policy migration job is corrupt"}

          {:ok, %{status: :done}} ->
            :ok
        end
      end

      defp flow_queue_policy_job_barrier(state, job_key) do
        case do_get(state, job_key) do
          value when is_binary(value) ->
            with_lmdb_mirror_shard(state, fn ->
              queue_pending_lmdb_mirror_put(job_key, value, 0)
            end)

            :ok

          _missing ->
            :ok
        end
      end

      defp flow_put_type_catalog_value(
             state,
             key,
             type,
             value,
             expire_at_ms,
             previous_generation
           ) do
        with {:ok, %{migration_generation: generation}} <-
               PolicyMigration.decode_catalog(value) do
          if flow_value_and_expiry_match?(state, key, value, expire_at_ms) do
            :ok
          else
            with :ok <- flow_put_hot_value(state, key, value, expire_at_ms) do
              with_lmdb_mirror_shard(state, fn ->
                queue_pending_lmdb_mirror_put(key, value, expire_at_ms)

                if is_integer(previous_generation) and previous_generation != generation do
                  flow_delete_policy_catalog_projection(type, key, previous_generation)
                end

                flow_queue_policy_catalog_projection(type, key, generation)
              end)

              :ok
            end
          end
        else
          :error -> {:error, "ERR flow type catalog entry is corrupt"}
        end
      end

      defp flow_delete_type_catalog_value(state, key, type, generation) do
        with :ok <- do_delete(state, key) do
          with_lmdb_mirror_shard(state, fn ->
            queue_pending_lmdb_mirror_delete(key)
            flow_delete_policy_catalog_projection(type, key, generation)
          end)

          :ok
        end
      end

      defp flow_queue_policy_catalog_projection(type, catalog_key, generation) do
        queue_pending_lmdb_mirror_op(
          {:put, FlowKeys.policy_catalog_projection_key(type, catalog_key, generation), <<1>>}
        )

        :ok
      end

      defp flow_delete_policy_catalog_projection(type, catalog_key, generation) do
        queue_pending_lmdb_mirror_op(
          {:delete, FlowKeys.policy_catalog_projection_key(type, catalog_key, generation)}
        )

        :ok
      end

      defp flow_maybe_delete_old_policy_catalog_projection(
             _type,
             _catalog_key,
             generation,
             generation
           ),
           do: :ok

      defp flow_maybe_delete_old_policy_catalog_projection(
             type,
             catalog_key,
             previous_generation,
             _generation
           ),
           do: flow_delete_policy_catalog_projection(type, catalog_key, previous_generation)

      defp flow_value_and_expiry_match?(state, key, value, expire_at_ms) do
        case ets_lookup(state, key) do
          {:hit, ^value, ^expire_at_ms} -> true
          _different -> false
        end
      end

      defp flow_put_policy_migration_job_value(state, key, value) do
        if flow_value_and_expiry_match?(state, key, value, 0) do
          :ok
        else
          with :ok <- flow_put_hot_value(state, key, value, 0) do
            with_lmdb_mirror_shard(state, fn -> queue_pending_lmdb_mirror_put(key, value, 0) end)
            :ok
          end
        end
      end

      defp flow_delete_policy_migration_job_value(state, key) do
        with :ok <- do_delete(state, key) do
          with_lmdb_mirror_shard(state, fn -> queue_pending_lmdb_mirror_delete(key) end)
          :ok
        end
      end
    end
  end
end
