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

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    _table = create_table!()
    Process.send_after(self(), :recover_existing_default_session, 50)

    {:ok, %{sessions: %{}, session_contexts: %{}, pending_pages: %{}, recovery_cursors: %{}}}
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
  def handle_call({:begin_flush, instance_name, token}, _from, state) do
    table = create_table!()
    key = coord_key(instance_name)

    case ensure_coord(table, instance_name) do
      {epoch, nil} ->
        next_epoch = epoch + 1
        true = :ets.insert(table, {key, next_epoch, token})
        {:reply, {:ok, next_epoch}, state}

      {_epoch, _active_token} ->
        {:reply, {:error, :flush_in_progress}, state}
    end
  end

  @impl true
  def handle_call({:finish_flush, instance_name, token}, _from, state) do
    table = create_table!()
    key = coord_key(instance_name)

    case :ets.lookup(table, key) do
      [{^key, epoch, ^token}] ->
        true = :ets.insert(table, {key, epoch, nil})
        {:reply, :ok, state}

      _stale_or_finished ->
        {:reply, :ok, state}
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
        opts = [
          shard_id: expired_page.shard_id,
          amount: length(expired_page.reservation_ids),
          reservation_ids: expired_page.reservation_ids,
          now_ms: now_ms
        ]

        case safe_release(&LimitStore.release/3, ctx, expired_page.scope, opts) do
          {:ok, _owner} ->
            case session_from_state(state, instance_name) do
              {:ok, session} ->
                CacheSessionStore.discard_pages(ctx, session, [expired_page],
                  allowed_states: [:unused, :uncertain]
                )

              {:error, _reason} ->
                :ok
            end

            pending_pages = put_pending_pages(state.pending_pages, key, remaining_pages)
            {:reply, :expired, %{state | pending_pages: pending_pages}}

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
  def handle_call({:flush_pending, ctx, instance_name, now_ms, release_fun}, _from, state) do
    {counts, pending_pages} =
      flush_pending_pages(
        ctx,
        Map.get(state.sessions, instance_name),
        instance_name,
        now_ms,
        release_fun,
        state.pending_pages
      )

    {:reply, counts, %{state | pending_pages: pending_pages}}
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
  def flush(ctx, opts \\ []) when is_list(opts) do
    now_ms = Keyword.get(opts, :now_ms, Ferricstore.CommandTime.now_ms())
    before_detach_fun = Keyword.get(opts, :before_detach_fun, fn _entry -> :ok end)
    release_fun = Keyword.get(opts, :release_fun, &LimitStore.release/3)

    if is_integer(now_ms) and now_ms >= 0 and is_function(before_detach_fun, 1) and
         is_function(release_fun, 3) do
      case :ets.whereis(@table) do
        :undefined -> {:ok, %{released: 0, errors: 0}}
        table -> flush_table(table, ctx, now_ms, before_detach_fun, release_fun)
      end
    else
      {:error, "ERR invalid flow limit cache flush opts"}
    end
  end

  @doc false
  def recover(ctx, opts \\ []) when is_list(opts) do
    GenServer.call(__MODULE__, {:recover_session, ctx, opts}, 30_000)
  end

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
                       cache_release_fun
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
         release_fun
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
              release_fun
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
          release_fun
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

  defp flush_table(table, ctx, now_ms, before_detach_fun, release_fun) do
    instance_name = instance_name(ctx)
    token = make_ref()

    case GenServer.call(__MODULE__, {:begin_flush, instance_name, token}) do
      {:ok, _generation} ->
        try do
          detached_entries =
            detach_cached_entries(table, instance_name, before_detach_fun, [])

          detached_counts =
            release_detached(table, ctx, detached_entries, now_ms, release_fun)

          pending_counts =
            GenServer.call(
              __MODULE__,
              {:flush_pending, ctx, instance_name, now_ms, release_fun},
              30_000
            )

          {:ok, merge_flush_counts(detached_counts, pending_counts)}
        after
          GenServer.call(__MODULE__, {:finish_flush, instance_name, token})
        end

      {:error, :flush_in_progress} = error ->
        error
    end
  end

  defp detach_cached_entries(table, instance_name, before_detach_fun, detached) do
    match_spec = cache_entry_match_spec(instance_name)

    case :ets.select(table, match_spec, @flush_batch_size) do
      {entries, _continuation} ->
        newly_detached =
          Enum.reduce(entries, detached, fn snapshot, acc ->
            invoke_before_detach(before_detach_fun, snapshot)

            case :ets.take(table, elem(snapshot, 0)) do
              [entry] -> [entry | acc]
              [] -> acc
            end
          end)

        detach_cached_entries(table, instance_name, before_detach_fun, newly_detached)

      :"$end_of_table" ->
        Enum.reverse(detached)
    end
  end

  defp invoke_before_detach(before_detach_fun, entry) do
    before_detach_fun.(entry)
  catch
    _kind, _reason -> :ok
  end

  defp release_detached(table, ctx, entries, now_ms, release_fun) do
    Enum.reduce(entries, %{released: 0, errors: 0}, fn
      {{_instance, scope, shard_id}, _available, expires_at_ms, capacity, reservation_ids,
       _config_version, _effective_limit, @entry_tag} = entry,
      acc
      when is_binary(scope) and is_integer(shard_id) and is_integer(expires_at_ms) and
             is_integer(capacity) and capacity >= 0 and is_list(reservation_ids) ->
        release_detached_entry(
          table,
          ctx,
          entry,
          scope,
          shard_id,
          reservation_ids,
          now_ms,
          release_fun,
          acc
        )

      invalid_entry, acc ->
        restore_invalid_entry(table, invalid_entry)
        Map.update!(acc, :errors, &(&1 + 1))
    end)
  end

  defp release_detached_entry(
         _table,
         _ctx,
         _entry,
         _scope,
         _shard_id,
         [],
         _now_ms,
         _release_fun,
         counts
       ),
       do: counts

  defp release_detached_entry(
         table,
         ctx,
         entry,
         scope,
         shard_id,
         reservation_ids,
         now_ms,
         release_fun,
         counts
       ) do
    opts = [
      shard_id: shard_id,
      amount: length(reservation_ids),
      reservation_ids: reservation_ids,
      now_ms: now_ms
    ]

    case safe_release(release_fun, ctx, scope, opts) do
      {:ok, _owner} ->
        Map.update!(counts, :released, &(&1 + length(reservation_ids)))

      {:error, _reason} ->
        restore_cached_entry(table, entry)
        Map.update!(counts, :errors, &(&1 + 1))
    end
  end

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
              opts = [
                shard_id: page.shard_id,
                amount: length(page.reservation_ids),
                reservation_ids: page.reservation_ids,
                now_ms: now_ms
              ]

              case safe_release(release_fun, ctx, page.scope, opts) do
                {:ok, _owner} ->
                  if is_map(session) do
                    CacheSessionStore.discard_pages(ctx, session, [page],
                      allowed_states: [:unused, :uncertain]
                    )
                  end

                  updated_counts =
                    Map.update!(
                      page_counts,
                      :released,
                      &(&1 + length(page.reservation_ids))
                    )

                  {updated_counts, remaining}

                {:error, _reason} ->
                  {Map.update!(page_counts, :errors, &(&1 + 1)), [page | remaining]}
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

  defp restore_fenced_entry(
         {key, available, expires_at_ms, capacity, reservation_ids, config_version,
          effective_limit, @entry_tag}
       ) do
    fenced_entry =
      {key, available, expires_at_ms, capacity, reservation_ids, {:fenced, config_version},
       effective_limit, @entry_tag}

    :ets.insert_new(table(), fenced_entry)
    :ok
  end

  defp entry_cache_configuration(
         {_key, _available, _expires_at_ms, _capacity, _ids, config_version, effective_limit,
          @entry_tag}
       ),
       do: {config_version, effective_limit}

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
      Enum.any?(state.recovery_cursors, fn {_instance_name, cursor} -> cursor != :done end)
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
