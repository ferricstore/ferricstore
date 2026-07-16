defmodule Ferricstore.Flow.Governance.LimitCache do
  @moduledoc false

  use GenServer

  alias Ferricstore.Flow.Governance.CacheSessionStore
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Governance.Telemetry

  @table :ferricstore_flow_governance_limit_cache
  @entry_tag :flow_governance_limit_cache_entry
  @coord_tag :"$ferricstore_flow_governance_limit_cache_coord"
  @session_coord_tag :"$ferricstore_flow_governance_limit_cache_session"
  @flush_batch_size 128
  @default_multiplier 4
  @default_max_chunk 10_000
  @limit_store_max_mutation_amount 1_000
  @default_session_page_size 256
  @max_pending_activation_attempts 16
  @max_flush_waiters_per_instance 64

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    _table = create_table!()
    Process.send_after(self(), :recover_existing_default_session, 50)

    {:ok,
     %{
       sessions: %{},
       session_contexts: %{},
       pending_pages: %{},
       recovery_cursors: %{},
       flush_owners: %{},
       flush_waiters: %{},
       flush_monitors: %{}
     }}
  end

  @impl true
  def handle_call(:ensure_table, _from, state) do
    {:reply, create_table!(), state}
  end

  @impl true
  def handle_call({:cache_status, instance_name}, _from, state) do
    status = create_table!() |> ensure_coord(instance_name) |> coord_status()
    {:reply, status, state}
  end

  @impl true
  def handle_call(:cached_reservations_present, _from, state) do
    present? = cached_entries?(create_table!()) or pending_reservations?(state)
    {:reply, present?, state}
  end

  @impl true
  def handle_call({:ensure_session, ctx}, _from, state) do
    case ensure_state_session(ctx, state) do
      {:ok, session, updated_state} ->
        cache_session_coord(ctx, session)
        {:reply, {:ok, session}, updated_state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:recover_session, ctx, opts}, _from, state) do
    case ensure_state_session(ctx, state) do
      {:ok, session, state} ->
        instance_name = instance_name(ctx)
        cursor = Map.get(state.recovery_cursors, instance_name, nil)

        if cursor == :done do
          counts = %{released: 0, retained: 0, errors: 0, processed: 0, next_cursor: nil}
          {:reply, {:ok, counts}, state}
        else
          recover_opts = Keyword.put(opts, :cursor, cursor)

          case CacheSessionStore.recover(ctx, session, recover_opts) do
            {:ok, counts} = ok ->
              recovery_cursors =
                Map.put(
                  state.recovery_cursors,
                  instance_name,
                  recovery_cursor_state(counts.next_cursor)
                )

              {:reply, ok, %{state | recovery_cursors: recovery_cursors}}

            {:error, _reason} = error ->
              {:reply, error, state}
          end
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:reset_session, instance_name}, _from, state) do
    :ets.delete(create_table!(), session_coord_key(instance_name))
    {:reply, :ok, drop_state_session(state, instance_name)}
  end

  @impl true
  def handle_call({:begin_flush, instance_name, token}, from, state) do
    table = create_table!()

    case ensure_coord(table, instance_name) do
      {epoch, nil} ->
        {reply, state} = activate_flush(table, instance_name, epoch, token, from, state)
        {:reply, reply, state}

      {_epoch, _active_token} ->
        if flush_owner?(state, instance_name, elem(from, 0)) do
          {:reply, {:error, :flush_reentrant}, state}
        else
          case enqueue_flush_waiter(instance_name, token, from, state) do
            {:ok, state} -> {:noreply, state}
            {:error, reason, state} -> {:reply, {:error, reason}, state}
          end
        end
    end
  end

  @impl true
  def handle_call({:finish_flush, instance_name, token}, _from, state) do
    table = create_table!()
    {:reply, :ok, finish_active_flush(table, instance_name, token, state, true)}
  end

  @impl true
  def handle_call(
        {:detach_flush_batch, instance_name, token, ctx, now_ms, keys},
        from,
        state
      ) do
    table = create_table!()

    case detach_active_flush_batch(
           table,
           instance_name,
           token,
           elem(from, 0),
           ctx,
           now_ms,
           keys,
           state
         ) do
      {:ok, entries, state} -> {:reply, {:ok, entries}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(
        {:detach_pending_flush_batch, instance_name, token, ctx, now_ms},
        from,
        state
      ) do
    case detach_active_pending_flush_batch(
           instance_name,
           token,
           elem(from, 0),
           ctx,
           now_ms,
           state
         ) do
      {:ok, entries, state} -> {:reply, {:ok, entries}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:resolve_flush_batch, instance_name, token, resolutions}, from, state) do
    table = create_table!()

    case resolve_active_flush_batch(
           table,
           instance_name,
           token,
           elem(from, 0),
           resolutions,
           state
         ) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(
        {:plan_cached, ctx, instance_name, generation, key, reservation_ids, capacity,
         expires_at_ms, config_version, effective_limit},
        _from,
        state
      ) do
    table = create_table!()

    case {ensure_coord(table, instance_name), ensure_state_session(ctx, state)} do
      {{^generation, nil}, {:ok, session, state}} ->
        scope = elem(key, 1)
        shard_id = elem(key, 2)

        case persist_cache_plan(
               ctx,
               session,
               key,
               scope,
               shard_id,
               reservation_ids,
               capacity,
               generation,
               expires_at_ms,
               config_version,
               effective_limit
             ) do
          {:error, :stale_cache_session} ->
            state = drop_state_session(state, instance_name)

            case ensure_state_session(ctx, state) do
              {:ok, refreshed_session, state} ->
                :ok = cache_session_coord(ctx, refreshed_session)

                {:reply,
                 persist_cache_plan(
                   ctx,
                   refreshed_session,
                   key,
                   scope,
                   shard_id,
                   reservation_ids,
                   capacity,
                   generation,
                   expires_at_ms,
                   config_version,
                   effective_limit
                 ), state}

              {:error, _reason} = error ->
                {:reply, error, state}
            end

          result ->
            {:reply, result, state}
        end

      {{^generation, nil}, {:error, _reason} = error} ->
        {:reply, error, state}

      {_flush_started_or_generation_changed, _session_result} ->
        {:reply, {:error, :cache_generation_changed}, state}
    end
  end

  @impl true
  def handle_call(
        {:finalize_cached, ctx, plan, expires_at_ms, config_version, effective_limit},
        _from,
        state
      ) do
    table = create_table!()
    instance_name = elem(plan.key, 0)

    case ensure_coord(table, instance_name) do
      {generation, nil} when generation == plan.generation ->
        with {:ok, pages} <-
               CacheSessionStore.update_pages(ctx, plan.session, plan.pages,
                 expires_at_ms: expires_at_ms,
                 config_version: config_version,
                 effective_limit: effective_limit
               ),
             [first_page | remaining_pages] <- pages,
             {:ok, activated_page} <-
               CacheSessionStore.activate_page(ctx, plan.session, first_page),
             claimed_capacity = max(plan.capacity - length(plan.reservation_ids), 0),
             first_capacity = claimed_capacity + length(activated_page.reservation_ids),
             :ok <-
               do_add_cached(
                 table,
                 plan.key,
                 activated_page.reservation_ids,
                 first_capacity,
                 activated_page.expires_at_ms,
                 activated_page.config_version,
                 activated_page.effective_limit
               ) do
          _ = CacheSessionStore.acknowledge_page(ctx, plan.session, activated_page)

          pending_pages =
            Map.update(
              state.pending_pages,
              plan.key,
              remaining_pages,
              &(&1 ++ remaining_pages)
            )

          {:reply, :ok, %{state | pending_pages: pending_pages}}
        else
          {:error, _reason} = error -> {:reply, error, state}
          _invalid_pages -> {:reply, {:error, :cache_session_plan_invalid}, state}
        end

      _flush_started_or_generation_changed ->
        {:reply, {:error, :cache_generation_changed}, state}
    end
  end

  @impl true
  def handle_call(
        {:activate_pending, ctx, key, generation, now_ms, requested_configuration, release_fun},
        _from,
        state
      ) do
    table = create_table!()
    instance_name = elem(key, 0)

    case {ensure_coord(table, instance_name), Map.get(state.pending_pages, key, [])} do
      {{^generation, nil}, [%{retry_only: true} | _remaining_pages] = pages} ->
        session = Map.get(state.sessions, instance_name)

        {_counts, kept} =
          flush_pending_pages(
            ctx,
            session,
            instance_name,
            now_ms,
            release_fun,
            %{key => pages}
          )

        pending_pages =
          case Map.get(kept, key, []) do
            [] -> Map.delete(state.pending_pages, key)
            remaining -> Map.put(state.pending_pages, key, remaining)
          end

        {:reply, :stale_configuration, %{state | pending_pages: pending_pages}}

      {{^generation, nil}, [page | _remaining_pages] = pages}
      when page.config_version != elem(requested_configuration, 0) or
             (not is_nil(elem(requested_configuration, 1)) and
                page.effective_limit != elem(requested_configuration, 1)) ->
        session = Map.get(state.sessions, instance_name)

        {_counts, kept} =
          flush_pending_pages(
            ctx,
            session,
            instance_name,
            now_ms,
            release_fun,
            %{key => pages}
          )

        pending_pages =
          case Map.get(kept, key, []) do
            [] -> Map.delete(state.pending_pages, key)
            remaining -> Map.put(state.pending_pages, key, remaining)
          end

        {:reply, :stale_configuration, %{state | pending_pages: pending_pages}}

      {{^generation, nil}, [page | remaining_pages]} when page.expires_at_ms > now_ms ->
        case session_from_state(state, instance_name) do
          {:ok, session} ->
            case CacheSessionStore.activate_page(ctx, session, page) do
              {:ok, activated_page} ->
                :ok =
                  do_add_cached(
                    table,
                    key,
                    activated_page.reservation_ids,
                    length(activated_page.reservation_ids),
                    activated_page.expires_at_ms,
                    activated_page.config_version,
                    activated_page.effective_limit
                  )

                _ = CacheSessionStore.acknowledge_page(ctx, session, activated_page)

                pending_pages = put_pending_pages(state.pending_pages, key, remaining_pages)
                {:reply, :activated, %{state | pending_pages: pending_pages}}

              {:error, _reason} = error ->
                {:reply, error, state}
            end

          {:error, _reason} = error ->
            {:reply, error, state}
        end

      {{^generation, nil}, [expired_page | remaining_pages]} ->
        case session_from_state(state, instance_name) do
          {:ok, session} ->
            case release_pending_page(
                   ctx,
                   session,
                   expired_page,
                   now_ms,
                   release_fun,
                   &discard_pending_pages/4
                 ) do
              {:ok, _released} ->
                pending_pages = put_pending_pages(state.pending_pages, key, remaining_pages)
                {:reply, :expired, %{state | pending_pages: pending_pages}}

              {:error, reason, _released} = error ->
                retry_page = pending_page_after_failure(expired_page, error)

                pending_pages =
                  put_pending_pages(state.pending_pages, key, [retry_page | remaining_pages])

                {:reply, {:error, reason}, %{state | pending_pages: pending_pages}}

              {:error, _reason} = error ->
                retry_page = pending_page_after_failure(expired_page, error)

                pending_pages =
                  put_pending_pages(state.pending_pages, key, [retry_page | remaining_pages])

                {:reply, error, %{state | pending_pages: pending_pages}}
            end

          {:error, _reason} = error ->
            {:reply, error, state}
        end

      {{^generation, nil}, []} ->
        {:reply, :none, state}

      {_flush_started_or_generation_changed, _pages} ->
        {:reply, {:error, :cache_generation_changed}, state}
    end
  end

  @impl true
  def handle_call({:fence_pending_configuration, ctx, key, now_ms}, _from, state) do
    instance_name = elem(key, 0)
    session = Map.get(state.sessions, instance_name)
    pages = Map.get(state.pending_pages, key, [])

    {counts, kept} =
      flush_pending_pages(
        ctx,
        session,
        instance_name,
        now_ms,
        &LimitStore.release/3,
        %{key => pages}
      )

    pending_pages =
      case Map.get(kept, key, []) do
        [] -> Map.delete(state.pending_pages, key)
        remaining -> Map.put(state.pending_pages, key, remaining)
      end

    {:reply, counts, %{state | pending_pages: pending_pages}}
  end

  @impl true
  def handle_call({:fence_stale_entry, ctx, key, entry, now_ms, release_fun}, _from, state) do
    instance_name = elem(key, 0)
    scope = elem(key, 1)
    shard_id = elem(key, 2)
    session = Map.get(state.sessions, instance_name)
    existing_pending = Map.get(state.pending_pages, key, [])

    {entry_counts, retry_pages} =
      release_or_persist_fenced_entry(
        ctx,
        session,
        scope,
        shard_id,
        entry,
        now_ms,
        release_fun
      )

    {pending_counts, kept} =
      flush_pending_pages(
        ctx,
        session,
        instance_name,
        now_ms,
        release_fun,
        %{key => existing_pending}
      )

    remaining = Map.get(kept, key, []) ++ retry_pages
    pending_pages = put_pending_pages(state.pending_pages, key, remaining)
    counts = merge_flush_counts(entry_counts, pending_counts)
    {:reply, counts, %{state | pending_pages: pending_pages}}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.flush_monitors, monitor_ref) do
      {nil, _flush_monitors} ->
        {:noreply, state}

      {{:owner, instance_name, token}, flush_monitors} ->
        state = %{state | flush_monitors: flush_monitors}
        table = create_table!()
        {:noreply, finish_active_flush(table, instance_name, token, state, false)}

      {{:waiter, instance_name, token}, flush_monitors} ->
        state = %{state | flush_monitors: flush_monitors}
        {_waiter, state} = pop_flush_waiter(instance_name, token, state)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:recover_existing_default_session, state) do
    state = maybe_open_existing_default_session(state)

    if map_size(state.sessions) == 0 do
      Process.send_after(self(), :recover_existing_default_session, 250)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:continue_session_recovery, instance_name}, state) do
    {:noreply, continue_session_recovery(instance_name, state)}
  end

  @spec spend(FerricStore.Instance.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def spend(ctx, scope, opts) when is_binary(scope) and is_list(opts) do
    if enabled?() do
      cached_spend(ctx, scope, opts)
    else
      LimitStore.spend(ctx, scope, opts)
    end
  end

  def spend(ctx, scope, opts), do: LimitStore.spend(ctx, scope, opts)

  @spec release(FerricStore.Instance.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def release(ctx, scope, opts) when is_binary(scope) and is_list(opts) do
    LimitStore.release(ctx, scope, opts)
  end

  def release(ctx, scope, opts), do: LimitStore.release(ctx, scope, opts)

  @spec renew(FerricStore.Instance.t(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def renew(ctx, scope, opts) when is_binary(scope) and is_list(opts) do
    case LimitStore.renew(ctx, scope, opts) do
      {:ok, result} = ok ->
        maybe_renew_cached_expiry(ctx, scope, opts, result)
        ok

      {:error, _reason} = error ->
        error
    end
  end

  def renew(ctx, scope, opts), do: LimitStore.renew(ctx, scope, opts)

  @spec clear() :: :ok | {:error, :cached_reservations_present}
  def clear do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      _table ->
        if GenServer.call(__MODULE__, :cached_reservations_present) do
          {:error, :cached_reservations_present}
        else
          :ok
        end
    end
  end

  @spec clear(FerricStore.Instance.t(), keyword()) ::
          {:ok, %{released: non_neg_integer(), errors: 0}} | {:error, term()}
  def clear(ctx, opts \\ [])

  def clear(ctx, opts) when is_list(opts) do
    case flush(ctx, opts) do
      {:ok, %{errors: 0}} = ok ->
        :ok = GenServer.call(__MODULE__, {:reset_session, instance_name(ctx)})
        ok

      {:ok, counts} ->
        {:error, {:cached_reservation_release_failed, counts}}

      {:error, _reason} = error ->
        error
    end
  end

  def clear(_ctx, _opts), do: {:error, "ERR flow limit cache opts must be a keyword list"}

  @doc false
  def with_drained_cache(ctx, fun, opts \\ [])

  def with_drained_cache(ctx, fun, opts) when is_function(fun, 0) and is_list(opts) do
    if Keyword.keyword?(opts) do
      now_ms = Keyword.get(opts, :now_ms, Ferricstore.CommandTime.now_ms())
      before_detach_fun = Keyword.get(opts, :before_detach_fun, fn _entry -> :ok end)
      release_fun = Keyword.get(opts, :release_fun, &LimitStore.release/3)
      page_discard_fun = Keyword.get(opts, :page_discard_fun, &discard_pending_pages/4)

      if is_integer(now_ms) and now_ms >= 0 and is_function(before_detach_fun, 1) and
           is_function(release_fun, 3) and is_function(page_discard_fun, 4) do
        case :ets.whereis(@table) do
          :undefined ->
            fun.()

          table ->
            with_cache_flush(
              table,
              ctx,
              now_ms,
              before_detach_fun,
              release_fun,
              page_discard_fun,
              fn counts ->
                if counts.errors == 0 do
                  try do
                    fun.()
                  after
                    GenServer.call(__MODULE__, {:reset_session, instance_name(ctx)})
                  end
                else
                  {:error, {:cached_reservation_release_failed, counts}}
                end
              end
            )
        end
      else
        {:error, "ERR invalid flow limit cache drain opts"}
      end
    else
      {:error, "ERR invalid flow limit cache drain opts"}
    end
  end

  def with_drained_cache(_ctx, _fun, _opts),
    do: {:error, "ERR invalid flow limit cache drain opts"}

  @doc false
  def flush(ctx, opts \\ [])

  def flush(ctx, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      now_ms = Keyword.get(opts, :now_ms, Ferricstore.CommandTime.now_ms())
      before_detach_fun = Keyword.get(opts, :before_detach_fun, fn _entry -> :ok end)
      release_fun = Keyword.get(opts, :release_fun, &LimitStore.release/3)
      page_discard_fun = Keyword.get(opts, :page_discard_fun, &discard_pending_pages/4)

      if is_integer(now_ms) and now_ms >= 0 and is_function(before_detach_fun, 1) and
           is_function(release_fun, 3) and is_function(page_discard_fun, 4) do
        case :ets.whereis(@table) do
          :undefined ->
            {:ok, %{released: 0, errors: 0}}

          table ->
            flush_table(
              table,
              ctx,
              now_ms,
              before_detach_fun,
              release_fun,
              page_discard_fun
            )
        end
      else
        {:error, "ERR invalid flow limit cache flush opts"}
      end
    else
      {:error, "ERR invalid flow limit cache flush opts"}
    end
  end

  def flush(_ctx, _opts), do: {:error, "ERR invalid flow limit cache flush opts"}

  @doc false
  def recover(ctx, opts \\ [])

  def recover(ctx, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      GenServer.call(__MODULE__, {:recover_session, ctx, opts}, 30_000)
    else
      {:error, "ERR invalid flow limit cache recovery opts"}
    end
  end

  def recover(_ctx, _opts), do: {:error, "ERR invalid flow limit cache recovery opts"}

  defp cached_spend(ctx, scope, opts) do
    with {:ok, shard_id} <- fetch_non_negative_integer(opts, :shard_id),
         {:ok, amount} <- fetch_positive_integer(opts, :amount),
         {:ok, now_ms} <- fetch_non_negative_integer(opts, :now_ms),
         {:ok, _session} <- ensure_cache_session(ctx) do
      instance_name = instance_name(ctx)
      key = {instance_name, scope, shard_id}
      cache_status = cache_status(instance_name)
      requested_configuration = requested_cache_configuration(opts)
      cache_release_fun = Keyword.get(opts, :cache_release_fun, &LimitStore.release/3)

      with :ok <- ensure_cached_lease_deadline(ctx, scope, key, opts, now_ms) do
        case take_cached(key, amount, now_ms, requested_configuration) do
          {:ok, reservation_ids} ->
            Telemetry.emit(:limit_cache_hit, :ok, %{
              scope: scope,
              shard_id: shard_id,
              amount: amount
            })

            {:ok,
             %{
               cache: :hit,
               scope: scope,
               shard_id: shard_id,
               amount: amount,
               reservation_ids: reservation_ids
             }}

          {:stale_configuration, entry} ->
            fence_stale_configuration(ctx, key, entry, now_ms, cache_release_fun)
            LimitStore.spend(ctx, scope, opts)

          :miss ->
            case cache_status do
              {:ready, generation} ->
                case take_pending_cached(
                       ctx,
                       key,
                       amount,
                       now_ms,
                       generation,
                       requested_configuration,
                       cache_release_fun,
                       @max_pending_activation_attempts
                     ) do
                  {:ok, reservation_ids} ->
                    cache_hit(scope, shard_id, amount, reservation_ids)

                  :stale_configuration ->
                    LimitStore.spend(ctx, scope, opts)

                  :miss ->
                    Telemetry.emit(:limit_cache_miss, :ok, %{
                      scope: scope,
                      shard_id: shard_id,
                      amount: amount
                    })

                    spend_and_fill_cache(ctx, scope, opts, key, amount, generation)
                end

              {:flushing, _generation} ->
                LimitStore.spend(ctx, scope, opts)
            end
        end
      end
    else
      _invalid -> LimitStore.spend(ctx, scope, opts)
    end
  end

  defp take_pending_cached(
         ctx,
         key,
         amount,
         now_ms,
         generation,
         requested_configuration,
         release_fun,
         attempts_left
       ) do
    if attempts_left <= 0 do
      :miss
    else
      do_take_pending_cached(
        ctx,
        key,
        amount,
        now_ms,
        generation,
        requested_configuration,
        release_fun,
        attempts_left
      )
    end
  end

  defp do_take_pending_cached(
         ctx,
         key,
         amount,
         now_ms,
         generation,
         requested_configuration,
         release_fun,
         attempts_left
       ) do
    case GenServer.call(
           __MODULE__,
           {:activate_pending, ctx, key, generation, now_ms, requested_configuration,
            release_fun},
           30_000
         ) do
      :activated ->
        case take_cached(key, amount, now_ms, requested_configuration) do
          {:ok, _reservation_ids} = hit ->
            hit

          {:stale_configuration, entry} ->
            fence_stale_configuration(ctx, key, entry, now_ms, release_fun)
            :stale_configuration

          :miss ->
            take_pending_cached(
              ctx,
              key,
              amount,
              now_ms,
              generation,
              requested_configuration,
              release_fun,
              attempts_left - 1
            )
        end

      :expired ->
        take_pending_cached(
          ctx,
          key,
          amount,
          now_ms,
          generation,
          requested_configuration,
          release_fun,
          attempts_left - 1
        )

      :stale_configuration ->
        :stale_configuration

      :none ->
        :miss

      {:error, _reason} ->
        :miss
    end
  end

  defp cache_hit(scope, shard_id, amount, reservation_ids) do
    Telemetry.emit(:limit_cache_hit, :ok, %{
      scope: scope,
      shard_id: shard_id,
      amount: amount
    })

    {:ok,
     %{
       cache: :hit,
       scope: scope,
       shard_id: shard_id,
       amount: amount,
       reservation_ids: reservation_ids
     }}
  end

  defp spend_and_fill_cache(ctx, scope, opts, key, amount, generation) do
    chunk = cache_chunk(amount)

    if chunk == amount do
      LimitStore.spend(ctx, scope, opts)
    else
      with {:ok, reservation_ids} <-
             LimitStore.generate_reservation_ids(
               ctx,
               scope,
               Keyword.get(opts, :shard_id),
               chunk
             ),
           {claimed_ids, cached_ids} = Enum.split(reservation_ids, amount),
           {planned_version, planned_limit} = requested_cache_configuration(opts),
           {:ok, plan} <-
             plan_cached(
               ctx,
               key,
               cached_ids,
               chunk,
               planned_expiry(opts),
               generation,
               planned_version,
               planned_limit
             ) do
        spend_planned_chunk(
          ctx,
          scope,
          opts,
          chunk,
          reservation_ids,
          claimed_ids,
          cached_ids,
          plan
        )
      else
        {:error, _reason} = error ->
          Telemetry.emit(:limit_cache_fill, error, %{
            scope: scope,
            shard_id: Keyword.get(opts, :shard_id),
            amount: chunk - amount
          })

          LimitStore.spend(ctx, scope, opts)
      end
    end
  end

  defp spend_planned_chunk(
         ctx,
         scope,
         opts,
         chunk,
         reservation_ids,
         claimed_ids,
         cached_ids,
         plan
       ) do
    spend_opts = Keyword.put(opts, :amount, chunk)

    case LimitStore.spend_reserved(ctx, scope, spend_opts, reservation_ids) do
      {:ok, result} ->
        run_after_reserved_spend_hook(opts, result)
        {config_version, effective_limit} = result_cache_configuration(result, opts)

        finalize_result =
          finalize_cached(
            ctx,
            plan,
            lease_expires_at_ms(result),
            config_version,
            effective_limit
          )

        Telemetry.emit(:limit_cache_fill, finalize_result, %{
          scope: scope,
          shard_id: Keyword.get(opts, :shard_id),
          amount: length(cached_ids)
        })

        case finalize_result do
          :ok ->
            :ok

          {:error, _reason} ->
            release_planned_after_finalize_failure(ctx, scope, opts, cached_ids, plan)
        end

        {:ok, Map.put(result, :reservation_ids, claimed_ids)}

      {:error, {:timeout, :unknown_outcome}} = error ->
        normalize_reserved_spend_result(error, scope, opts, reservation_ids)

      {:error, _reason} ->
        CacheSessionStore.discard_pages(ctx, plan.session, plan.pages)
        LimitStore.spend(ctx, scope, opts)
    end
  end

  @doc false
  def normalize_reserved_spend_result(result, scope, opts, reservation_ids)
      when is_binary(scope) and is_list(opts) and is_list(reservation_ids) do
    LimitStore.normalize_spend_result(
      result,
      scope,
      Keyword.get(opts, :shard_id),
      reservation_ids
    )
  end

  defp take_cached(key, amount, now_ms, requested_configuration) do
    table = table()

    case :ets.lookup(table, key) do
      [
        current =
            {^key, _available, expires_at_ms, _capacity, _reservation_ids, _config_version,
             _effective_limit, @entry_tag}
      ]
      when is_integer(expires_at_ms) and expires_at_ms <= now_ms ->
        :ets.delete_object(table, current)
        :miss

      [
        current =
            {^key, available, expires_at_ms, capacity, reservation_ids, config_version,
             effective_limit, @entry_tag}
      ]
      when is_integer(available) and available >= amount and is_list(reservation_ids) ->
        if cache_configuration_matches?(
             config_version,
             effective_limit,
             requested_configuration
           ) do
          {taken, remaining} = Enum.split(reservation_ids, amount)

          if length(taken) == amount do
            updated =
              {key, available - amount, expires_at_ms, capacity, remaining, config_version,
               effective_limit, @entry_tag}

            if replace_exact(table, current, updated) do
              {:ok, taken}
            else
              take_cached(key, amount, now_ms, requested_configuration)
            end
          else
            :miss
          end
        else
          detach_stale_entry(table, current, key, amount, now_ms, requested_configuration)
        end

      [invalid_entry] ->
        :ets.delete_object(table, invalid_entry)
        :miss

      _missing_or_insufficient ->
        :miss
    end
  end

  defp detach_stale_entry(table, current, key, amount, now_ms, requested_configuration) do
    if delete_exact(table, current) do
      {:stale_configuration, current}
    else
      take_cached(key, amount, now_ms, requested_configuration)
    end
  end

  defp plan_cached(
         _ctx,
         _key,
         [],
         _capacity,
         _expires_at_ms,
         _generation,
         _config_version,
         _effective_limit
       ),
       do: {:error, :no_prefetch}

  defp plan_cached(
         ctx,
         {instance_name, _scope, _shard_id} = key,
         reservation_ids,
         capacity,
         expires_at_ms,
         generation,
         config_version,
         effective_limit
       ) do
    GenServer.call(
      __MODULE__,
      {:plan_cached, ctx, instance_name, generation, key, reservation_ids, capacity,
       expires_at_ms, config_version, effective_limit},
      30_000
    )
  end

  defp finalize_cached(ctx, plan, expires_at_ms, config_version, effective_limit) do
    GenServer.call(
      __MODULE__,
      {:finalize_cached, ctx, plan, expires_at_ms, config_version, effective_limit},
      30_000
    )
  end

  defp do_add_cached(
         table,
         key,
         reservation_ids,
         capacity,
         expires_at_ms,
         config_version,
         effective_limit
       ) do
    new_entry =
      {key, length(reservation_ids), expires_at_ms, capacity, reservation_ids, config_version,
       effective_limit, @entry_tag}

    case :ets.lookup(table, key) do
      [] ->
        if :ets.insert_new(table, new_entry) do
          :ok
        else
          do_add_cached(
            table,
            key,
            reservation_ids,
            capacity,
            expires_at_ms,
            config_version,
            effective_limit
          )
        end

      [
        current =
            {^key, available, old_expiry, old_capacity, old_ids, ^config_version,
             ^effective_limit, @entry_tag}
      ]
      when is_integer(available) and is_integer(old_capacity) and is_list(old_ids) ->
        updated =
          {key, available + length(reservation_ids), max(old_expiry, expires_at_ms),
           old_capacity + capacity, reservation_ids ++ old_ids, config_version, effective_limit,
           @entry_tag}

        if replace_exact(table, current, updated) do
          :ok
        else
          do_add_cached(
            table,
            key,
            reservation_ids,
            capacity,
            expires_at_ms,
            config_version,
            effective_limit
          )
        end

      [current]
      when is_tuple(current) and tuple_size(current) == 8 and
             elem(current, 7) == @entry_tag ->
        {:error, :cache_configuration_changed}

      [invalid_entry] ->
        if delete_exact(table, invalid_entry) do
          do_add_cached(
            table,
            key,
            reservation_ids,
            capacity,
            expires_at_ms,
            config_version,
            effective_limit
          )
        else
          do_add_cached(
            table,
            key,
            reservation_ids,
            capacity,
            expires_at_ms,
            config_version,
            effective_limit
          )
        end
    end
  end

  defp replace_exact(table, current, updated) do
    :ets.select_replace(table, [{current, [], [{:const, updated}]}]) == 1
  end

  defp delete_exact(table, current) do
    :ets.select_delete(table, [{current, [], [true]}]) == 1
  end

  defp fence_stale_configuration(ctx, key, entry, now_ms, release_fun) do
    _ =
      GenServer.call(
        __MODULE__,
        {:fence_stale_entry, ctx, key, entry, now_ms, release_fun},
        30_000
      )

    :ok
  end

  defp release_planned_after_finalize_failure(ctx, scope, opts, reservation_ids, plan) do
    release_opts =
      opts
      |> Keyword.put(:amount, length(reservation_ids))
      |> Keyword.put(:reservation_ids, reservation_ids)

    case safe_release(&LimitStore.release/3, ctx, scope, release_opts) do
      {:ok, _owner} ->
        CacheSessionStore.discard_pages(ctx, plan.session, plan.pages,
          allowed_states: [:unused, :uncertain]
        )

      {:error, _reason} ->
        :retained_fail_closed
    end
  end

  defp planned_expiry(opts) do
    case {Keyword.get(opts, :now_ms), Keyword.get(opts, :ttl_ms)} do
      {now_ms, ttl_ms}
      when is_integer(now_ms) and now_ms >= 0 and is_integer(ttl_ms) and ttl_ms > 0 ->
        now_ms + ttl_ms

      _missing_or_invalid ->
        0
    end
  end

  defp run_after_reserved_spend_hook(opts, result) do
    case Keyword.get(opts, :after_reserved_spend_fun) do
      fun when is_function(fun, 1) -> fun.(result)
      _missing -> :ok
    end
  end

  defp flush_table(
         table,
         ctx,
         now_ms,
         before_detach_fun,
         release_fun,
         page_discard_fun
       ) do
    with_cache_flush(
      table,
      ctx,
      now_ms,
      before_detach_fun,
      release_fun,
      page_discard_fun,
      fn counts -> {:ok, counts} end
    )
  end

  defp with_cache_flush(
         table,
         ctx,
         now_ms,
         before_detach_fun,
         release_fun,
         page_discard_fun,
         after_drain_fun
       ) do
    instance_name = instance_name(ctx)
    token = make_ref()

    case GenServer.call(__MODULE__, {:begin_flush, instance_name, token}, :infinity) do
      {:ok, _generation} ->
        try do
          detached_counts =
            drain_cached_batches(
              table,
              ctx,
              instance_name,
              token,
              now_ms,
              before_detach_fun,
              release_fun,
              %{released: 0, errors: 0}
            )

          pending_counts =
            drain_pending_flush_batches(
              ctx,
              instance_name,
              token,
              now_ms,
              release_fun,
              page_discard_fun,
              %{released: 0, errors: 0}
            )

          detached_counts
          |> merge_flush_counts(pending_counts)
          |> after_drain_fun.()
        after
          GenServer.call(__MODULE__, {:finish_flush, instance_name, token}, :infinity)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp drain_cached_batches(
         table,
         ctx,
         instance_name,
         token,
         now_ms,
         before_detach_fun,
         release_fun,
         counts
       ) do
    case detach_cached_batch(
           table,
           ctx,
           instance_name,
           token,
           now_ms,
           before_detach_fun
         ) do
      {:ok, []} ->
        counts

      {:ok, detached_entries} ->
        batch_counts =
          release_detached(
            ctx,
            instance_name,
            token,
            detached_entries,
            now_ms,
            release_fun
          )

        counts = merge_flush_counts(counts, batch_counts)

        if batch_counts.errors == 0 do
          drain_cached_batches(
            table,
            ctx,
            instance_name,
            token,
            now_ms,
            before_detach_fun,
            release_fun,
            counts
          )
        else
          counts
        end

      {:error, _reason} ->
        Map.update!(counts, :errors, &(&1 + 1))
    end
  end

  defp drain_pending_flush_batches(
         ctx,
         instance_name,
         token,
         now_ms,
         release_fun,
         page_discard_fun,
         counts
       ) do
    case GenServer.call(
           __MODULE__,
           {:detach_pending_flush_batch, instance_name, token, ctx, now_ms},
           :infinity
         ) do
      {:ok, []} ->
        counts

      {:ok, detached_entries} ->
        batch_counts =
          release_detached_pending_pages(
            instance_name,
            token,
            detached_entries,
            now_ms,
            release_fun,
            page_discard_fun
          )

        counts = merge_flush_counts(counts, batch_counts)

        if batch_counts.errors == 0 do
          drain_pending_flush_batches(
            ctx,
            instance_name,
            token,
            now_ms,
            release_fun,
            page_discard_fun,
            counts
          )
        else
          counts
        end

      {:error, _reason} ->
        Map.update!(counts, :errors, &(&1 + 1))
    end
  end

  defp detach_cached_batch(table, ctx, instance_name, token, now_ms, before_detach_fun) do
    match_spec = cache_entry_match_spec(instance_name)

    case :ets.select(table, match_spec, @flush_batch_size) do
      {entries, _continuation} ->
        Enum.each(entries, &invoke_before_detach(before_detach_fun, &1))
        keys = Enum.map(entries, &elem(&1, 0))

        GenServer.call(
          __MODULE__,
          {:detach_flush_batch, instance_name, token, ctx, now_ms, keys},
          :infinity
        )

      :"$end_of_table" ->
        {:ok, []}
    end
  end

  defp invoke_before_detach(before_detach_fun, entry) do
    before_detach_fun.(entry)
  catch
    _kind, _reason -> :ok
  end

  defp release_detached(ctx, instance_name, token, entries, now_ms, release_fun) do
    {counts, resolutions} =
      Enum.reduce(entries, {%{released: 0, errors: 0}, []}, fn
        {{_instance, scope, shard_id}, _available, expires_at_ms, capacity, reservation_ids,
         _config_version, _effective_limit, @entry_tag} = entry,
        {counts, resolutions}
        when is_binary(scope) and is_integer(shard_id) and is_integer(expires_at_ms) and
               is_integer(capacity) and capacity >= 0 and is_list(reservation_ids) ->
          case release_detached_entry(
                 ctx,
                 scope,
                 shard_id,
                 reservation_ids,
                 now_ms,
                 release_fun
               ) do
            {:ok, released} ->
              counts = Map.update!(counts, :released, &(&1 + released))
              {counts, [{:released, {:cache_entry, elem(entry, 0)}} | resolutions]}

            {:error, _reason} = error ->
              counts = Map.update!(counts, :errors, &(&1 + 1))

              resolution =
                cache_entry_failure_resolution(
                  {:cache_entry, elem(entry, 0)},
                  error
                )

              {counts, [resolution | resolutions]}
          end

        invalid_entry, {counts, resolutions} ->
          counts = Map.update!(counts, :errors, &(&1 + 1))
          {counts, [{:restore, {:cache_entry, elem(invalid_entry, 0)}} | resolutions]}
      end)

    case GenServer.call(
           __MODULE__,
           {:resolve_flush_batch, instance_name, token, Enum.reverse(resolutions)},
           :infinity
         ) do
      :ok -> counts
      {:error, _reason} -> Map.update!(counts, :errors, &(&1 + 1))
    end
  end

  defp release_detached_pending_pages(
         instance_name,
         token,
         entries,
         now_ms,
         release_fun,
         page_discard_fun
       ) do
    {counts, resolutions} =
      Enum.reduce(entries, {%{released: 0, errors: 0}, []}, fn
        %{inflight_key: inflight_key, ctx: ctx, session: session, page: page},
        {counts, resolutions} ->
          case release_pending_page(
                 ctx,
                 session,
                 page,
                 now_ms,
                 release_fun,
                 page_discard_fun
               ) do
            {:ok, released} ->
              counts = Map.update!(counts, :released, &(&1 + released))
              {counts, [{:released, inflight_key} | resolutions]}

            {:error, _reason, released} = error ->
              counts =
                counts
                |> Map.update!(:released, &(&1 + released))
                |> Map.update!(:errors, &(&1 + 1))

              {counts, [pending_page_failure_resolution(inflight_key, error) | resolutions]}

            {:error, _reason} = error ->
              counts = Map.update!(counts, :errors, &(&1 + 1))
              {counts, [pending_page_failure_resolution(inflight_key, error) | resolutions]}
          end
      end)

    case GenServer.call(
           __MODULE__,
           {:resolve_flush_batch, instance_name, token, Enum.reverse(resolutions)},
           :infinity
         ) do
      :ok -> counts
      {:error, _reason} -> Map.update!(counts, :errors, &(&1 + 1))
    end
  end

  defp release_detached_entry(
         _ctx,
         _scope,
         _shard_id,
         [],
         _now_ms,
         _release_fun
       ),
       do: {:ok, 0}

  defp release_detached_entry(
         ctx,
         scope,
         shard_id,
         reservation_ids,
         now_ms,
         release_fun
       ) do
    opts = [
      shard_id: shard_id,
      amount: length(reservation_ids),
      reservation_ids: reservation_ids,
      now_ms: now_ms
    ]

    case safe_release(release_fun, ctx, scope, opts) do
      {:ok, _owner} -> {:ok, length(reservation_ids)}
      {:error, _reason} = error -> error
    end
  end

  defp release_pending_page(ctx, session, page, now_ms, release_fun, page_discard_fun)
       when is_map(session) and is_map(page) and is_function(page_discard_fun, 4) do
    opts = [
      shard_id: page.shard_id,
      amount: length(page.reservation_ids),
      reservation_ids: page.reservation_ids,
      now_ms: now_ms
    ]

    case safe_release(release_fun, ctx, page.scope, opts) do
      {:ok, _owner} ->
        case safe_discard_pages(
               page_discard_fun,
               ctx,
               session,
               [page],
               allowed_states: [:unused, :uncertain]
             ) do
          :ok ->
            {:ok, length(page.reservation_ids)}

          {:error, reason} ->
            {:error, {:pending_page_discard_failed, reason}, length(page.reservation_ids)}
        end

      {:error, reason} ->
        {:error, {:pending_page_release_failed, reason}}
    end
  end

  defp release_pending_page(
         _ctx,
         _session,
         _page,
         _now_ms,
         _release_fun,
         _page_discard_fun
       ),
       do: {:error, :cache_session_unavailable}

  defp flush_pending_pages(
         ctx,
         session,
         instance_name,
         now_ms,
         release_fun,
         pending_pages
       ) do
    Enum.reduce(
      pending_pages,
      {%{released: 0, errors: 0}, %{}},
      fn {key, pages}, {counts, kept_pages} ->
        if elem(key, 0) == instance_name do
          {counts, remaining} =
            Enum.reduce(pages, {counts, []}, fn page, {page_counts, remaining} ->
              case release_pending_page(
                     ctx,
                     session,
                     page,
                     now_ms,
                     release_fun,
                     &discard_pending_pages/4
                   ) do
                {:ok, released} ->
                  updated_counts =
                    Map.update!(
                      page_counts,
                      :released,
                      &(&1 + released)
                    )

                  {updated_counts, remaining}

                {:error, _reason, released} = error ->
                  retry_page = pending_page_after_failure(page, error)

                  updated_counts =
                    page_counts
                    |> Map.update!(:released, &(&1 + released))
                    |> Map.update!(:errors, &(&1 + 1))

                  {updated_counts, [retry_page | remaining]}

                {:error, _reason} = error ->
                  retry_page = pending_page_after_failure(page, error)

                  {Map.update!(page_counts, :errors, &(&1 + 1)), [retry_page | remaining]}
              end
            end)

          kept_pages =
            case Enum.reverse(remaining) do
              [] -> kept_pages
              remaining -> Map.put(kept_pages, key, remaining)
            end

          {counts, kept_pages}
        else
          {counts, Map.put(kept_pages, key, pages)}
        end
      end
    )
  end

  defp release_or_persist_fenced_entry(
         _ctx,
         _session,
         _scope,
         _shard_id,
         entry,
         _now_ms,
         _release_fun
       )
       when elem(entry, 4) == [],
       do: {%{released: 0, errors: 0}, []}

  defp release_or_persist_fenced_entry(
         ctx,
         session,
         scope,
         shard_id,
         entry,
         now_ms,
         release_fun
       ) do
    reservation_ids = elem(entry, 4)

    opts = [
      shard_id: shard_id,
      amount: length(reservation_ids),
      reservation_ids: reservation_ids,
      now_ms: now_ms
    ]

    case safe_release(release_fun, ctx, scope, opts) do
      {:ok, _owner} ->
        {%{released: length(reservation_ids), errors: 0}, []}

      {:error, _reason} ->
        retry_pages = persist_fenced_retry_pages(ctx, session, scope, shard_id, entry)
        {%{released: 0, errors: 1}, retry_pages}
    end
  end

  defp persist_fenced_retry_pages(ctx, session, scope, shard_id, entry)
       when is_map(session) do
    {config_version, effective_limit} = entry_cache_configuration(entry)

    case CacheSessionStore.persist_prefetch(
           ctx,
           session,
           scope,
           shard_id,
           elem(entry, 4),
           page_size: session_page_size(),
           expires_at_ms: elem(entry, 2),
           config_version: config_version,
           effective_limit: effective_limit
         ) do
      {:ok, pages} ->
        Enum.map(pages, &Map.put(&1, :retry_only, true))

      {:error, _reason} ->
        restore_fenced_entry(entry)
        []
    end
  end

  defp persist_fenced_retry_pages(_ctx, _session, _scope, _shard_id, entry) do
    restore_fenced_entry(entry)
    []
  end

  defp restore_fenced_entry(entry) do
    restore_fenced_detached_entry(table(), entry)
    :ok
  end

  defp entry_cache_configuration(
         {_key, _available, _expires_at_ms, _capacity, _ids, config_version, effective_limit,
          @entry_tag}
       ),
       do: {unfenced_config_version(config_version), normalized_effective_limit(effective_limit)}

  defp merge_flush_counts(left, right) do
    %{
      released: left.released + right.released,
      errors: left.errors + right.errors
    }
  end

  defp safe_release(release_fun, ctx, scope, opts) do
    case release_fun.(ctx, scope, opts) do
      {:ok, _owner} = ok -> ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_release_result, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp safe_discard_pages(page_discard_fun, ctx, session, pages, opts) do
    case page_discard_fun.(ctx, session, pages, opts) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_page_discard_result, other}}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp discard_pending_pages(ctx, session, pages, opts) do
    CacheSessionStore.discard_pages(ctx, session, pages, opts)
  end

  defp cache_entry_failure_resolution(key, {:error, {:timeout, :unknown_outcome}}),
    do: {:retry, key}

  defp cache_entry_failure_resolution(key, _error), do: {:restore, key}

  defp pending_page_failure_resolution(key, error) do
    if pending_page_retry_required?(error), do: {:retry, key}, else: {:restore, key}
  end

  defp pending_page_after_failure(page, error) do
    if pending_page_retry_required?(error), do: Map.put(page, :retry_only, true), else: page
  end

  defp pending_page_retry_required?(
         {:error, {:pending_page_release_failed, {:timeout, :unknown_outcome}}}
       ),
       do: true

  defp pending_page_retry_required?({:error, {:pending_page_discard_failed, _reason}}),
    do: true

  defp pending_page_retry_required?({:error, {:pending_page_discard_failed, _reason}, _released}),
    do: true

  defp pending_page_retry_required?(_error), do: false

  defp restore_cached_entry(
         table,
         {key, _available, expires_at_ms, capacity, reservation_ids, config_version,
          effective_limit, @entry_tag}
       ) do
    do_add_cached(
      table,
      key,
      reservation_ids,
      capacity,
      expires_at_ms,
      config_version,
      effective_limit
    )
  end

  defp restore_detached_entry(
         table,
         {{_instance, scope, shard_id}, _available, expires_at_ms, capacity, reservation_ids,
          _config_version, _effective_limit, @entry_tag} = entry
       )
       when is_binary(scope) and is_integer(shard_id) and is_integer(expires_at_ms) and
              is_integer(capacity) and capacity >= 0 and is_list(reservation_ids) do
    restore_cached_entry(table, entry)
  end

  defp restore_detached_entry(table, entry), do: restore_invalid_entry(table, entry)

  defp restore_fenced_detached_entry(
         table,
         {key, _available, expires_at_ms, capacity, reservation_ids, config_version,
          effective_limit, @entry_tag}
       )
       when is_integer(expires_at_ms) and is_integer(capacity) and capacity >= 0 and
              is_list(reservation_ids) do
    fenced_entry =
      {key, length(reservation_ids), expires_at_ms, capacity, reservation_ids,
       {:fenced, unfenced_config_version(config_version)},
       normalized_effective_limit(effective_limit), @entry_tag}

    restore_fenced_cached_entry(table, fenced_entry)
  end

  defp restore_fenced_detached_entry(table, entry), do: restore_invalid_entry(table, entry)

  defp restore_fenced_cached_entry(
         table,
         {key, available, expires_at_ms, capacity, reservation_ids, fenced_version,
          effective_limit, @entry_tag} = fenced_entry
       ) do
    case :ets.lookup(table, key) do
      [] ->
        if :ets.insert_new(table, fenced_entry) do
          :ok
        else
          restore_fenced_cached_entry(table, fenced_entry)
        end

      [
        current =
            {^key, old_available, old_expiry, old_capacity, old_ids, _old_version,
             _old_effective_limit, @entry_tag}
      ]
      when is_integer(old_available) and old_available >= 0 and is_integer(old_expiry) and
             is_integer(old_capacity) and old_capacity >= 0 and is_list(old_ids) ->
        updated =
          {key, old_available + available, max(old_expiry, expires_at_ms),
           old_capacity + capacity, reservation_ids ++ old_ids, fenced_version, effective_limit,
           @entry_tag}

        if replace_exact(table, current, updated) do
          :ok
        else
          restore_fenced_cached_entry(table, fenced_entry)
        end

      [invalid_entry] ->
        if delete_exact(table, invalid_entry) do
          restore_fenced_cached_entry(table, fenced_entry)
        else
          restore_fenced_cached_entry(table, fenced_entry)
        end
    end
  end

  defp unfenced_config_version({:fenced, version}), do: unfenced_config_version(version)
  defp unfenced_config_version(version) when is_integer(version) and version >= 0, do: version
  defp unfenced_config_version(_invalid), do: 0

  defp normalized_effective_limit(nil), do: nil

  defp normalized_effective_limit(limit) when is_integer(limit) and limit >= 0,
    do: limit

  defp normalized_effective_limit(_invalid), do: nil

  defp restore_invalid_entry(table, entry) do
    case entry do
      {key, _field_2, _field_3, _field_4, _field_5, _field_6, _field_7, @entry_tag} ->
        if :ets.lookup(table, key) == [], do: :ets.insert_new(table, entry), else: true

      _other ->
        false
    end
  end

  defp cache_entry_match_spec(instance_name) do
    [
      {{{instance_name, :"$1", :"$2"}, :"$3", :"$4", :"$5", :"$6", :"$7", :"$8", @entry_tag}, [],
       [:"$_"]}
    ]
  end

  defp flush_cache_key?({instance_name, _scope, _shard_id}, instance_name), do: true
  defp flush_cache_key?(_key, _instance_name), do: false

  defp ensure_cached_lease_deadline(ctx, scope, key, opts, now_ms) do
    case {Keyword.get(opts, :ttl_ms), :ets.lookup(table(), key)} do
      {ttl_ms,
       [
         {^key, _available, expires_at_ms, _capacity, _reservation_ids, _config_version,
          _effective_limit, @entry_tag}
       ]}
      when is_integer(ttl_ms) and ttl_ms > 0 and expires_at_ms > now_ms and
             expires_at_ms < now_ms + ttl_ms ->
        renew_cached_lease_deadline(ctx, scope, opts)

      _current_or_missing ->
        :ok
    end
  end

  defp renew_cached_lease_deadline(ctx, scope, opts) do
    case LimitStore.renew(ctx, scope, opts) do
      {:ok, result} ->
        maybe_renew_cached_expiry(ctx, scope, opts, result)
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp maybe_renew_cached_expiry(ctx, scope, opts, result) do
    with shard_id when is_integer(shard_id) and shard_id >= 0 <- Keyword.get(opts, :shard_id),
         expires_at_ms when is_integer(expires_at_ms) <- lease_expires_at_ms(result) do
      key = {instance_name(ctx), scope, shard_id}

      case :ets.lookup(table(), key) do
        [
          {^key, _available, _old_expiry, _capacity, _reservation_ids, _config_version,
           _effective_limit, @entry_tag}
        ] ->
          :ets.update_element(table(), key, {3, expires_at_ms})

        _missing_or_invalid ->
          false
      end
    else
      _invalid -> false
    end
  end

  defp cached_entries?(table) do
    match_spec = [
      {{{:"$1", :"$2", :"$3"}, :"$4", :"$5", :"$6", :"$7", :"$8", :"$9", @entry_tag}, [], [true]}
    ]

    :ets.select_count(table, match_spec) > 0
  end

  defp pending_reservations?(state) do
    Enum.any?(state.pending_pages, fn {_key, pages} -> pages != [] end) or
      Enum.any?(state.recovery_cursors, fn {_instance_name, cursor} -> cursor != :done end) or
      Enum.any?(state.flush_owners, fn {_instance_name, owner} -> owner.inflight != %{} end)
  end

  defp ensure_cache_session(ctx) do
    table = table()
    key = session_coord_key(instance_name(ctx))

    case :ets.lookup(table, key) do
      [{^key, session}] when is_map(session) ->
        {:ok, session}

      [] ->
        GenServer.call(__MODULE__, {:ensure_session, ctx}, 30_000)
    end
  end

  defp ensure_state_session(ctx, state) do
    instance_name = instance_name(ctx)

    case Map.fetch(state.sessions, instance_name) do
      {:ok, session} ->
        {:ok, session, state}

      :error ->
        case CacheSessionStore.open(ctx,
               node_id: CacheSessionStore.node_id(),
               instance_name: CacheSessionStore.instance_name(ctx)
             ) do
          {:ok, session} ->
            sessions = Map.put(state.sessions, instance_name, session)
            session_contexts = Map.put(state.session_contexts, instance_name, ctx)
            recovery_cursors = Map.put(state.recovery_cursors, instance_name, nil)

            state = %{
              state
              | sessions: sessions,
                session_contexts: session_contexts,
                recovery_cursors: recovery_cursors
            }

            {:ok, session, continue_session_recovery(instance_name, state)}

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp drop_state_session(state, instance_name) do
    pending_pages =
      Map.reject(state.pending_pages, fn
        {{^instance_name, _scope, _shard_id}, _pages} -> true
        _other -> false
      end)

    %{
      state
      | sessions: Map.delete(state.sessions, instance_name),
        session_contexts: Map.delete(state.session_contexts, instance_name),
        pending_pages: pending_pages,
        recovery_cursors: Map.delete(state.recovery_cursors, instance_name)
    }
  end

  defp persist_cache_plan(
         ctx,
         session,
         key,
         scope,
         shard_id,
         reservation_ids,
         capacity,
         generation,
         expires_at_ms,
         config_version,
         effective_limit
       ) do
    case CacheSessionStore.persist_prefetch(
           ctx,
           session,
           scope,
           shard_id,
           reservation_ids,
           page_size: session_page_size(),
           expires_at_ms: expires_at_ms,
           config_version: config_version,
           effective_limit: effective_limit
         ) do
      {:ok, pages} ->
        {:ok,
         %{
           key: key,
           generation: generation,
           capacity: capacity,
           reservation_ids: reservation_ids,
           pages: pages,
           session: session
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp session_from_state(state, instance_name) do
    case Map.fetch(state.sessions, instance_name) do
      {:ok, session} -> {:ok, session}
      :error -> {:error, :cache_session_missing}
    end
  end

  defp continue_session_recovery(instance_name, state) do
    cursor = Map.get(state.recovery_cursors, instance_name, :done)

    with false <- cursor == :done,
         {:ok, session} <- Map.fetch(state.sessions, instance_name),
         {:ok, ctx} <- Map.fetch(state.session_contexts, instance_name) do
      case CacheSessionStore.recover(ctx, session,
             cursor: cursor,
             limit: session_recovery_limit()
           ) do
        {:ok, counts} ->
          cursor_state = recovery_cursor_state(counts.next_cursor)
          recovery_cursors = Map.put(state.recovery_cursors, instance_name, cursor_state)
          state = %{state | recovery_cursors: recovery_cursors}

          if cursor_state != :done do
            send(self(), {:continue_session_recovery, instance_name})
          end

          state

        {:error, _reason} ->
          Process.send_after(self(), {:continue_session_recovery, instance_name}, 250)
          state
      end
    else
      _done_or_missing -> state
    end
  end

  defp maybe_open_existing_default_session(state) do
    ctx = FerricStore.Instance.get(:default)
    instance_name = instance_name(ctx)

    cond do
      Map.has_key?(state.sessions, instance_name) ->
        state

      CacheSessionStore.head_present?(ctx,
        node_id: CacheSessionStore.node_id(),
        instance_name: CacheSessionStore.instance_name(ctx)
      ) ->
        case ensure_state_session(ctx, state) do
          {:ok, session, state} ->
            cache_session_coord(ctx, session)
            state

          {:error, _reason} ->
            state
        end

      true ->
        state
    end
  rescue
    _not_ready -> state
  catch
    _kind, _reason -> state
  end

  defp recovery_cursor_state(nil), do: :done
  defp recovery_cursor_state(cursor), do: cursor

  defp session_recovery_limit do
    :ferricstore
    |> Application.get_env(:flow_governance_cache_session_recovery_page_size, 256)
    |> normalize_page_size()
  end

  defp cache_session_coord(ctx, session) do
    true = :ets.insert(table(), {session_coord_key(instance_name(ctx)), session})
    :ok
  end

  defp put_pending_pages(pending_pages, key, []), do: Map.delete(pending_pages, key)
  defp put_pending_pages(pending_pages, key, pages), do: Map.put(pending_pages, key, pages)

  defp session_page_size do
    :ferricstore
    |> Application.get_env(
      :flow_governance_cache_session_page_size,
      @default_session_page_size
    )
    |> normalize_page_size()
  end

  defp normalize_page_size(value) when is_integer(value) and value > 0, do: min(value, 256)
  defp normalize_page_size(_value), do: @default_session_page_size

  defp cache_status(instance_name) do
    table = table()
    key = coord_key(instance_name)

    case :ets.lookup(table, key) do
      [{^key, epoch, token}] when is_integer(epoch) and epoch >= 0 ->
        coord_status({epoch, token})

      [] ->
        GenServer.call(__MODULE__, {:cache_status, instance_name})
    end
  end

  defp ensure_coord(table, instance_name) do
    key = coord_key(instance_name)

    case :ets.lookup(table, key) do
      [{^key, epoch, token}] when is_integer(epoch) and epoch >= 0 ->
        {epoch, token}

      [] ->
        if :ets.insert_new(table, {key, 0, nil}) do
          {0, nil}
        else
          ensure_coord(table, instance_name)
        end
    end
  end

  defp activate_flush(table, instance_name, epoch, token, from, state) do
    pid = elem(from, 0)
    monitor_ref = Process.monitor(pid)
    next_epoch = epoch + 1
    key = coord_key(instance_name)

    true = :ets.insert(table, {key, next_epoch, token})

    owner = %{pid: pid, token: token, monitor_ref: monitor_ref, inflight: %{}}

    state = %{
      state
      | flush_owners: Map.put(state.flush_owners, instance_name, owner),
        flush_monitors: Map.put(state.flush_monitors, monitor_ref, {:owner, instance_name, token})
    }

    {{:ok, next_epoch}, state}
  end

  defp enqueue_flush_waiter(instance_name, token, from, state) do
    queue = Map.get(state.flush_waiters, instance_name, :queue.new())

    if :queue.len(queue) >= @max_flush_waiters_per_instance do
      {:error, :flush_queue_full, state}
    else
      pid = elem(from, 0)
      monitor_ref = Process.monitor(pid)

      waiter = %{
        from: from,
        pid: pid,
        token: token,
        monitor_ref: monitor_ref
      }

      state = %{
        state
        | flush_waiters: Map.put(state.flush_waiters, instance_name, :queue.in(waiter, queue)),
          flush_monitors:
            Map.put(state.flush_monitors, monitor_ref, {:waiter, instance_name, token})
      }

      {:ok, state}
    end
  end

  defp detach_active_flush_batch(
         table,
         instance_name,
         token,
         pid,
         ctx,
         now_ms,
         keys,
         state
       )
       when is_list(keys) do
    case Map.get(state.flush_owners, instance_name) do
      %{token: ^token, pid: ^pid, inflight: inflight} = owner ->
        {entries, inflight} =
          Enum.reduce(keys, {[], inflight}, fn key, {entries, inflight} ->
            cond do
              not flush_cache_key?(key, instance_name) ->
                {entries, inflight}

              Map.has_key?(inflight, {:cache_entry, key}) ->
                {entries, inflight}

              true ->
                case :ets.take(table, key) do
                  [entry] ->
                    inflight_key = {:cache_entry, key}
                    recovery = %{kind: :cache_entry, entry: entry, ctx: ctx, now_ms: now_ms}
                    {[entry | entries], Map.put(inflight, inflight_key, recovery)}

                  [] ->
                    {entries, inflight}
                end
            end
          end)

        owner = %{owner | inflight: inflight}
        state = put_flush_owner(state, instance_name, owner)
        {:ok, Enum.reverse(entries), state}

      _missing_or_replaced ->
        {:error, :cache_generation_changed}
    end
  end

  defp detach_active_flush_batch(
         _table,
         _instance_name,
         _token,
         _pid,
         _ctx,
         _now_ms,
         _keys,
         _state
       ),
       do: {:error, :cache_generation_changed}

  defp detach_active_pending_flush_batch(
         instance_name,
         token,
         pid,
         ctx,
         now_ms,
         state
       ) do
    case Map.get(state.flush_owners, instance_name) do
      %{token: ^token, pid: ^pid, inflight: inflight} = owner ->
        if pending_pages_for_instance?(state.pending_pages, instance_name) do
          case Map.get(state.sessions, instance_name) do
            session when is_map(session) ->
              {entries, pending_pages} =
                detach_pending_pages(
                  state.pending_pages,
                  instance_name,
                  ctx,
                  session,
                  @flush_batch_size
                )

              inflight =
                Enum.reduce(entries, inflight, fn entry, inflight ->
                  recovery =
                    entry
                    |> Map.take([:ctx, :session, :page, :pending_key])
                    |> Map.put(:kind, :pending_page)
                    |> Map.put(:now_ms, now_ms)

                  Map.put(inflight, entry.inflight_key, recovery)
                end)

              owner = %{owner | inflight: inflight}

              state =
                state
                |> Map.put(:pending_pages, pending_pages)
                |> put_flush_owner(instance_name, owner)

              {:ok, entries, state}

            _missing_session ->
              {:error, :cache_session_unavailable}
          end
        else
          {:ok, [], state}
        end

      _missing_or_replaced ->
        {:error, :cache_generation_changed}
    end
  end

  defp detach_pending_pages(pending_pages, instance_name, ctx, session, limit) do
    {entries, pending_pages, _remaining} =
      Enum.reduce_while(
        pending_pages,
        {[], pending_pages, limit},
        fn
          {{^instance_name, _scope, _shard_id} = pending_key, pages},
          {entries, pending_pages, remaining}
          when remaining > 0 ->
            {detached, kept} = Enum.split(pages, remaining)

            pending_pages =
              if kept == [] do
                Map.delete(pending_pages, pending_key)
              else
                Map.put(pending_pages, pending_key, kept)
              end

            entries =
              Enum.reduce(detached, entries, fn page, entries ->
                [
                  %{
                    inflight_key: pending_page_inflight_key(page),
                    pending_key: pending_key,
                    ctx: ctx,
                    session: session,
                    page: page
                  }
                  | entries
                ]
              end)

            remaining = remaining - length(detached)

            if remaining == 0 do
              {:halt, {entries, pending_pages, remaining}}
            else
              {:cont, {entries, pending_pages, remaining}}
            end

          _other, acc ->
            {:cont, acc}
        end
      )

    {Enum.reverse(entries), pending_pages}
  end

  defp resolve_active_flush_batch(
         table,
         instance_name,
         token,
         pid,
         resolutions,
         state
       )
       when is_list(resolutions) do
    case Map.get(state.flush_owners, instance_name) do
      %{token: ^token, pid: ^pid, inflight: inflight} = owner ->
        {inflight, state} =
          Enum.reduce(resolutions, {inflight, state}, fn
            {:released, key}, {inflight, state} ->
              {Map.delete(inflight, key), state}

            {:restore, key}, {inflight, state} ->
              case Map.pop(inflight, key) do
                {nil, inflight} ->
                  {inflight, state}

                {recovery, inflight} ->
                  {inflight, restore_inflight(table, recovery, state, :restore)}
              end

            {:retry, key}, {inflight, state} ->
              case Map.pop(inflight, key) do
                {nil, inflight} ->
                  {inflight, state}

                {recovery, inflight} ->
                  {inflight, restore_inflight(table, recovery, state, :retry)}
              end

            _invalid_resolution, acc ->
              acc
          end)

        owner = %{owner | inflight: inflight}
        {:ok, put_flush_owner(state, instance_name, owner)}

      _missing_or_replaced ->
        {:error, :cache_generation_changed}
    end
  end

  defp resolve_active_flush_batch(
         _table,
         _instance_name,
         _token,
         _pid,
         _resolutions,
         _state
       ),
       do: {:error, :cache_generation_changed}

  defp recover_active_flush_entries(table, instance_name, token, state) do
    case Map.get(state.flush_owners, instance_name) do
      %{token: ^token, inflight: inflight} = owner ->
        state =
          Enum.reduce(inflight, state, fn {_key, recovery}, state ->
            recover_inflight(table, recovery, state)
          end)

        put_flush_owner(state, instance_name, %{owner | inflight: %{}})

      _missing_or_replaced ->
        state
    end
  end

  defp recover_inflight(
         table,
         %{
           kind: :cache_entry,
           entry:
             {{_instance, scope, shard_id}, _available, expires_at_ms, capacity, reservation_ids,
              _config_version, _effective_limit, @entry_tag} = entry,
           ctx: ctx,
           now_ms: now_ms
         },
         state
       )
       when is_binary(scope) and is_integer(shard_id) and is_integer(expires_at_ms) and
              is_integer(capacity) and capacity >= 0 and is_list(reservation_ids) do
    case release_detached_entry(
           ctx,
           scope,
           shard_id,
           reservation_ids,
           now_ms,
           &LimitStore.release/3
         ) do
      {:ok, _released} ->
        state

      {:error, _reason} = error ->
        mode =
          case cache_entry_failure_resolution(:recovery, error) do
            {:retry, :recovery} -> :retry
            {:restore, :recovery} -> :restore
          end

        restore_inflight(table, %{kind: :cache_entry, entry: entry}, state, mode)
    end
  end

  defp recover_inflight(
         _table,
         %{
           kind: :pending_page,
           ctx: ctx,
           session: session,
           page: page,
           now_ms: now_ms
         } = recovery,
         state
       ) do
    case release_pending_page(
           ctx,
           session,
           page,
           now_ms,
           &LimitStore.release/3,
           &discard_pending_pages/4
         ) do
      {:ok, _released} ->
        state

      {:error, _reason, _released} ->
        restore_inflight(nil, recovery, state, :retry)

      {:error, _reason} = error ->
        mode = if pending_page_retry_required?(error), do: :retry, else: :restore
        restore_inflight(nil, recovery, state, mode)
    end
  end

  defp recover_inflight(table, %{entry: entry}, state) do
    restore_detached_entry(table, entry)
    state
  end

  defp recover_inflight(_table, recovery, state) do
    restore_inflight(nil, recovery, state, :restore)
  end

  defp restore_inflight(table, %{kind: :cache_entry, entry: entry}, state, :retry) do
    restore_fenced_detached_entry(table, entry)
    state
  end

  defp restore_inflight(table, %{kind: :cache_entry, entry: entry}, state, _mode) do
    restore_detached_entry(table, entry)
    state
  end

  defp restore_inflight(
         _table,
         %{kind: :pending_page, pending_key: pending_key, page: page},
         state,
         mode
       ) do
    page = if mode == :retry, do: Map.put(page, :retry_only, true), else: page

    pending_pages =
      Map.update(state.pending_pages, pending_key, [page], fn pages ->
        [page | pages]
        |> Enum.uniq_by(&pending_page_inflight_key/1)
        |> Enum.sort_by(& &1.sequence)
      end)

    %{state | pending_pages: pending_pages}
  end

  defp restore_inflight(_table, _invalid_recovery, state, _mode), do: state

  defp pending_page_inflight_key(page) do
    {:pending_page, page.session_id, page.generation, page.sequence}
  end

  defp pending_pages_for_instance?(pending_pages, instance_name) do
    Enum.any?(pending_pages, fn
      {{^instance_name, _scope, _shard_id}, pages} -> pages != []
      _other -> false
    end)
  end

  defp finish_active_flush(table, instance_name, token, state, demonitor?) do
    key = coord_key(instance_name)

    case :ets.lookup(table, key) do
      [{^key, epoch, ^token}] ->
        state = recover_active_flush_entries(table, instance_name, token, state)
        state = drop_flush_owner(instance_name, token, state, demonitor?)
        promote_next_flush(table, instance_name, epoch, state)

      _stale_or_finished ->
        state
    end
  end

  defp drop_flush_owner(instance_name, token, state, demonitor?) do
    case Map.get(state.flush_owners, instance_name) do
      %{token: ^token, monitor_ref: monitor_ref} ->
        if demonitor?, do: Process.demonitor(monitor_ref, [:flush])

        %{
          state
          | flush_owners: Map.delete(state.flush_owners, instance_name),
            flush_monitors: Map.delete(state.flush_monitors, monitor_ref)
        }

      _missing_or_replaced ->
        state
    end
  end

  defp promote_next_flush(table, instance_name, epoch, state) do
    case take_next_live_flush_waiter(instance_name, state) do
      {nil, state} ->
        true = :ets.insert(table, {coord_key(instance_name), epoch, nil})
        state

      {waiter, state} ->
        next_epoch = epoch + 1

        true =
          :ets.insert(
            table,
            {coord_key(instance_name), next_epoch, waiter.token}
          )

        owner =
          waiter
          |> Map.take([:pid, :token, :monitor_ref])
          |> Map.put(:inflight, %{})

        state = %{
          state
          | flush_owners: Map.put(state.flush_owners, instance_name, owner),
            flush_monitors:
              Map.put(
                state.flush_monitors,
                waiter.monitor_ref,
                {:owner, instance_name, waiter.token}
              )
        }

        GenServer.reply(waiter.from, {:ok, next_epoch})
        state
    end
  end

  defp take_next_live_flush_waiter(instance_name, state) do
    queue = Map.get(state.flush_waiters, instance_name, :queue.new())

    case :queue.out(queue) do
      {:empty, _queue} ->
        {nil, %{state | flush_waiters: Map.delete(state.flush_waiters, instance_name)}}

      {{:value, waiter}, remaining} ->
        state = put_flush_waiter_queue(state, instance_name, remaining)

        if Process.alive?(waiter.pid) do
          {waiter, state}
        else
          Process.demonitor(waiter.monitor_ref, [:flush])

          state = %{
            state
            | flush_monitors: Map.delete(state.flush_monitors, waiter.monitor_ref)
          }

          take_next_live_flush_waiter(instance_name, state)
        end
    end
  end

  defp pop_flush_waiter(instance_name, token, state) do
    queue = Map.get(state.flush_waiters, instance_name, :queue.new())

    {waiter, remaining} =
      queue
      |> :queue.to_list()
      |> Enum.reduce({nil, []}, fn
        %{token: ^token} = entry, {nil, kept} -> {entry, kept}
        entry, {found, kept} -> {found, [entry | kept]}
      end)

    state =
      state
      |> put_flush_waiter_queue(instance_name, :queue.from_list(Enum.reverse(remaining)))

    {waiter, state}
  end

  defp flush_owner?(state, instance_name, pid) do
    match?(%{pid: ^pid}, Map.get(state.flush_owners, instance_name))
  end

  defp put_flush_owner(state, instance_name, owner) do
    %{state | flush_owners: Map.put(state.flush_owners, instance_name, owner)}
  end

  defp put_flush_waiter_queue(state, instance_name, queue) do
    flush_waiters =
      if :queue.is_empty(queue) do
        Map.delete(state.flush_waiters, instance_name)
      else
        Map.put(state.flush_waiters, instance_name, queue)
      end

    %{state | flush_waiters: flush_waiters}
  end

  defp coord_status({epoch, nil}), do: {:ready, epoch}
  defp coord_status({epoch, _token}), do: {:flushing, epoch}

  defp coord_key(instance_name), do: {@coord_tag, instance_name}
  defp session_coord_key(instance_name), do: {@session_coord_tag, instance_name}

  defp table do
    case :ets.whereis(@table) do
      :undefined ->
        GenServer.call(__MODULE__, :ensure_table)

      table ->
        table
    end
  end

  defp create_table! do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
          :named_table,
          :public,
          read_concurrency: true,
          write_concurrency: true
        ])

      table ->
        if :ets.info(table, :owner) == self() do
          table
        else
          raise "flow governance limit cache has an unexpected ETS owner"
        end
    end
  end

  defp cache_chunk(amount) do
    multiplier =
      :ferricstore
      |> Application.get_env(:flow_governance_limit_cache_multiplier, @default_multiplier)
      |> normalize_positive_integer(@default_multiplier)

    max_chunk =
      :ferricstore
      |> Application.get_env(:flow_governance_limit_cache_max_chunk, @default_max_chunk)
      |> normalize_positive_integer(@default_max_chunk)
      |> min(@limit_store_max_mutation_amount)

    amount
    |> Kernel.*(multiplier)
    |> min(max_chunk)
    |> max(amount)
  end

  defp enabled? do
    Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled, true) != false
  end

  defp lease_expires_at_ms(%{lease: %{expires_at_ms: expires_at_ms}})
       when is_integer(expires_at_ms),
       do: expires_at_ms

  defp lease_expires_at_ms(_result), do: 0

  defp requested_cache_configuration(opts) do
    {Keyword.get(opts, :config_version, 0), Keyword.get(opts, :limit)}
  end

  defp result_cache_configuration(result, opts) do
    owner = Map.get(result, :owner, %{})

    {
      Map.get(owner, :config_version, Keyword.get(opts, :config_version, 0)),
      Map.get(owner, :limit, Keyword.get(opts, :limit))
    }
  end

  defp cache_configuration_matches?(
         stored_version,
         stored_limit,
         {requested_version, requested_limit}
       ) do
    stored_version == requested_version and
      (is_nil(requested_limit) or stored_limit == requested_limit)
  end

  defp instance_name(%{name: name}), do: name
  defp instance_name(_ctx), do: :default

  defp fetch_positive_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, value}
      _other -> :error
    end
  end

  defp fetch_non_negative_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> :error
    end
  end

  defp normalize_positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp normalize_positive_integer(_value, default), do: default
end
