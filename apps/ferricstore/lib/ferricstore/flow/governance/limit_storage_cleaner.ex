defmodule Ferricstore.Flow.Governance.LimitStorageCleaner do
  @moduledoc false

  use GenServer

  require Logger

  alias Ferricstore.Flow.Governance.Catalog
  alias Ferricstore.Flow.Governance.CreditLease
  alias Ferricstore.Flow.Governance.LimitRecord
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @default_interval_ms 1_000
  @default_pages_per_tick 16
  @max_pages_per_tick 64
  @max_consecutive_pages_per_scope 2
  @max_exact_version 9_007_199_254_740_991
  @progress_tag :flow_governance_limit_cleanup_progress
  @catalog_changed "ERR flow governance catalog changed during traversal"

  def start_link(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, ctx} <- Keyword.fetch(opts, :instance_ctx) do
      name = Keyword.get(opts, :name, process_name(ctx))
      GenServer.start_link(__MODULE__, ctx, name: name)
    else
      _invalid -> {:error, "ERR invalid flow limit storage cleaner options"}
    end
  end

  def start_link(_opts), do: {:error, "ERR invalid flow limit storage cleaner options"}

  @doc false
  def process_name(%{name: :default}), do: __MODULE__
  def process_name(%{name: name}) when is_atom(name), do: :"#{name}.FlowGovernanceLimitCleaner"

  @impl true
  def init(ctx), do: {:ok, schedule(%{ctx: ctx, timer: nil})}

  @impl true
  def handle_info(:cleanup, state) do
    state = %{state | timer: nil}

    result = run_tick(state.ctx)

    if result.errors > 0 do
      Logger.warning(
        "flow governance limit storage cleanup had #{result.errors} errors in one bounded tick"
      )
    end

    delay =
      if result.commands >= cleanup_pages_per_tick() and not result.caught_up?,
        do: 0,
        else: cleanup_interval_ms()

    {:noreply, schedule(state, delay)}
  end

  @doc false
  def run_tick(ctx, opts \\ [])

  def run_tick(ctx, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      page_budget = Keyword.get(opts, :page_budget, cleanup_pages_per_tick())
      now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))

      cond do
        not (is_integer(page_budget) and page_budget > 0 and
                 page_budget <= @max_pages_per_tick) ->
          {:error, "ERR invalid flow limit cleanup page budget"}

        not (is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_exact_version) ->
          {:error, "ERR invalid flow limit cleanup time"}

        true ->
          run_tick(ctx, now_ms, page_budget, page_budget * 4 + 4, %{
            commands: 0,
            caught_up?: false,
            cycle_commands: 0,
            cycle_deleted: 0,
            deleted: 0,
            errors: 0,
            streak_key: nil,
            streak_pages: 0,
            wrapped?: false
          })
      end
    else
      {:error, "ERR invalid flow limit cleanup options"}
    end
  end

  def run_tick(_ctx, _opts), do: {:error, "ERR invalid flow limit cleanup options"}

  @doc false
  def run_once(ctx, opts \\ [])

  def run_once(ctx, opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))

      if is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_exact_version do
        ctx
        |> run_once_accounted(now_ms, :infinity, nil, 0, 1)
        |> Map.fetch!(:reply)
      else
        {:error, "ERR invalid flow limit cleanup time"}
      end
    else
      {:error, "ERR invalid flow limit cleanup options"}
    end
  end

  def run_once(_ctx, _opts), do: {:error, "ERR invalid flow limit cleanup options"}

  defp run_once_accounted(ctx, now_ms, command_budget, streak_key, streak_pages, burst_limit) do
    case load_cursor(ctx) do
      {:ok, cursor, cursor_commands} ->
        remaining = remaining_budget(command_budget, cursor_commands)

        if remaining == 0 do
          step_result(
            {:ok, %{deleted: 0, pending?: false, next_cursor: cursor}},
            cursor_commands,
            0,
            0,
            streak_key,
            streak_pages,
            false
          )
        else
          ctx
          |> run_catalog_page(
            cursor,
            now_ms,
            remaining,
            streak_key,
            streak_pages,
            burst_limit
          )
          |> Map.update!(:commands, &(&1 + cursor_commands))
        end

      {:error, reason, cursor_commands} ->
        step_result({:error, reason}, cursor_commands, 0, 1, streak_key, streak_pages, false)
    end
  end

  defp run_catalog_page(
         ctx,
         cursor,
         now_ms,
         command_budget,
         streak_key,
         streak_pages,
         burst_limit
       ) do
    case Catalog.page(ctx, :limit, cursor, 1) do
      {:ok, %{keys: [], next_cursor: nil}} ->
        finish_catalog_step(
          ctx,
          cursor,
          nil,
          command_budget,
          {:ok, %{deleted: 0, pending?: false}},
          0,
          0,
          nil,
          0,
          true
        )

      {:ok, %{keys: [key], next_cursor: next_cursor}} ->
        {cleanup_result, cleanup_commands} = cleanup_key(ctx, key, now_ms)

        case cleanup_result do
          {:ok, cleanup} ->
            pending? = Map.get(cleanup, :pending?, false)
            next_streak = if streak_key == key, do: streak_pages + 1, else: 1
            advance? = not pending? or next_streak >= burst_limit

            target_cursor =
              if advance?, do: next_traversal_cursor(ctx, next_cursor), else: cursor

            finish_catalog_step(
              ctx,
              cursor,
              target_cursor,
              remaining_budget(command_budget, cleanup_commands),
              {:ok, cleanup},
              cleanup_commands,
              Map.get(cleanup, :deleted, 0),
              if(advance?, do: nil, else: key),
              if(advance?, do: 0, else: next_streak),
              advance? and is_nil(target_cursor)
            )

          {:error, reason} ->
            target_cursor = next_traversal_cursor(ctx, next_cursor)

            finish_catalog_step(
              ctx,
              cursor,
              target_cursor,
              remaining_budget(command_budget, cleanup_commands),
              {:error, reason},
              cleanup_commands,
              0,
              nil,
              0,
              is_nil(target_cursor)
            )
        end

      {:error, @catalog_changed} ->
        finish_catalog_step(
          ctx,
          cursor,
          nil,
          command_budget,
          {:ok, %{deleted: 0, pending?: false}},
          0,
          0,
          nil,
          0,
          true
        )

      {:error, reason} ->
        step_result({:error, reason}, 0, 0, 1, streak_key, streak_pages, false)
    end
  end

  defp cleanup_key(ctx, key, now_ms) do
    case Router.get(ctx, key) do
      value when is_binary(value) ->
        case LimitRecord.decode_owner(value) do
          {:ok, owner} ->
            if cleanup_due?(owner, now_ms) do
              {LimitStore.cleanup(ctx, owner.scope, now_ms: now_ms), 1}
            else
              {{:ok, %{deleted: 0, pending?: false}}, 0}
            end

          {:error, _reason} = error ->
            {error, 0}
        end

      nil ->
        catalog_key = Keys.governance_catalog_key(:limit)

        case Catalog.unregister_key(ctx, catalog_key, key) do
          :ok -> {{:ok, %{deleted: 0, pending?: false, pruned?: true}}, 1}
          {:error, _reason} = error -> {error, 1}
        end

      _invalid ->
        {{:error, "ERR flow limit record is corrupt"}, 0}
    end
  end

  defp cleanup_due?(owner, now_ms) do
    owner.cleanup_head <= owner.cleanup_tail or
      CreditLease.expired_lease_refs(owner, now_ms) != []
  end

  defp load_cursor(ctx) do
    case Router.get(ctx, Keys.governance_limit_cleanup_progress_key()) do
      nil ->
        {:ok, nil, 0}

      value when is_binary(value) ->
        case decode_cursor(value) do
          {:ok, cursor} -> {:ok, cursor, 0}
          _invalid -> reset_corrupt_cursor(ctx)
        end

      _invalid ->
        reset_corrupt_cursor(ctx)
    end
  end

  defp reset_corrupt_cursor(ctx) do
    case delete_cursor(ctx) do
      :ok -> {:ok, nil, 1}
      {:error, reason} -> {:error, reason, 1}
    end
  end

  defp delete_cursor(ctx) do
    case Router.delete(ctx, Keys.governance_limit_cleanup_progress_key()) do
      :ok -> :ok
      0 -> :ok
      1 -> :ok
      {:error, _reason} = error -> error
      _other -> :ok
    end
  end

  defp put_cursor(ctx, cursor) when is_binary(cursor) do
    if valid_cursor?(cursor) do
      value = TermCodec.encode({@progress_tag, cursor})
      Router.put(ctx, Keys.governance_limit_cleanup_progress_key(), value, 0)
    else
      {:error, "ERR invalid flow limit cleanup cursor"}
    end
  end

  defp decode_cursor(value) do
    if byte_size(value) <= Router.max_key_size() + 128 do
      case TermCodec.decode(value) do
        {:ok, {@progress_tag, cursor}} ->
          if valid_cursor?(cursor), do: {:ok, cursor}, else: :error

        _invalid ->
          :error
      end
    else
      :error
    end
  end

  defp valid_cursor?(nil), do: true

  defp valid_cursor?(cursor) when is_binary(cursor),
    do: byte_size(cursor) <= Router.max_key_size()

  defp valid_cursor?(_cursor), do: false

  defp finish_catalog_step(
         ctx,
         current_cursor,
         target_cursor,
         cursor_budget,
         reply,
         commands,
         deleted,
         streak_key,
         streak_pages,
         wrapped?
       ) do
    case persist_cursor(ctx, current_cursor, target_cursor, cursor_budget) do
      {:ok, effective_cursor, cursor_commands} ->
        reply = put_reply_cursor(reply, effective_cursor)

        step_result(
          reply,
          commands + cursor_commands,
          deleted,
          error_count(reply),
          streak_key,
          streak_pages,
          wrapped? and effective_cursor == target_cursor
        )

      {:error, reason, cursor_commands} ->
        step_result(
          {:error, reason},
          commands + cursor_commands,
          deleted,
          1,
          streak_key,
          streak_pages,
          false
        )
    end
  end

  defp persist_cursor(_ctx, cursor, cursor, _budget), do: {:ok, cursor, 0}

  defp persist_cursor(_ctx, current_cursor, _target_cursor, 0),
    do: {:ok, current_cursor, 0}

  defp persist_cursor(ctx, _current_cursor, nil, budget)
       when budget == :infinity or budget > 0 do
    case delete_cursor(ctx) do
      :ok -> {:ok, nil, 1}
      {:error, reason} -> {:error, reason, 1}
    end
  end

  defp persist_cursor(ctx, _current_cursor, cursor, budget)
       when is_binary(cursor) and (budget == :infinity or budget > 0) do
    case put_cursor(ctx, cursor) do
      :ok -> {:ok, cursor, 1}
      {:error, reason} -> {:error, reason, 1}
    end
  end

  defp next_traversal_cursor(_ctx, nil), do: nil

  defp next_traversal_cursor(ctx, cursor) when is_binary(cursor) do
    case Catalog.page(ctx, :limit, cursor, 1) do
      {:ok, %{keys: [], next_cursor: nil}} -> nil
      {:ok, %{keys: [_next], next_cursor: _next_cursor}} -> cursor
      {:error, @catalog_changed} -> nil
      {:error, _reason} -> cursor
    end
  end

  defp put_reply_cursor({:ok, reply}, cursor), do: {:ok, Map.put(reply, :next_cursor, cursor)}
  defp put_reply_cursor({:error, _reason} = error, _cursor), do: error

  defp error_count({:error, _reason}), do: 1
  defp error_count(_reply), do: 0

  defp step_result(
         reply,
         commands,
         deleted,
         errors,
         streak_key,
         streak_pages,
         wrapped?
       ) do
    %{
      reply: reply,
      commands: commands,
      deleted: deleted,
      errors: errors,
      streak_key: streak_key,
      streak_pages: streak_pages,
      wrapped?: wrapped?
    }
  end

  defp remaining_budget(:infinity, _used), do: :infinity
  defp remaining_budget(budget, used), do: max(budget - used, 0)

  defp run_tick(_ctx, _now_ms, page_budget, _attempts_left, result)
       when result.commands >= page_budget,
       do: result

  defp run_tick(_ctx, _now_ms, _page_budget, attempts_left, result)
       when attempts_left <= 0,
       do: result

  defp run_tick(ctx, now_ms, page_budget, attempts_left, result) do
    remaining = page_budget - result.commands

    step =
      run_once_accounted(
        ctx,
        now_ms,
        remaining,
        result.streak_key,
        result.streak_pages,
        @max_consecutive_pages_per_scope
      )

    cycle_deleted = result.cycle_deleted + step.deleted

    result = %{
      result
      | commands: result.commands + step.commands,
        cycle_commands: result.cycle_commands + step.commands,
        cycle_deleted: cycle_deleted,
        deleted: result.deleted + step.deleted,
        errors: result.errors + step.errors,
        streak_key: step.streak_key,
        streak_pages: step.streak_pages,
        wrapped?: result.wrapped? or step.wrapped?
    }

    {result, continue?} =
      cond do
        step.wrapped? and cycle_deleted == 0 ->
          {%{result | caught_up?: true}, false}

        step.wrapped? ->
          {%{result | cycle_commands: 0, cycle_deleted: 0}, true}

        true ->
          {result, true}
      end

    if continue? do
      run_tick(ctx, now_ms, page_budget, attempts_left - 1, result)
    else
      result
    end
  end

  defp schedule(%{timer: nil} = state), do: schedule(state, cleanup_interval_ms())
  defp schedule(state), do: state

  defp schedule(%{timer: nil} = state, delay_ms)
       when is_integer(delay_ms) and delay_ms >= 0 do
    %{state | timer: Process.send_after(self(), :cleanup, delay_ms)}
  end

  defp cleanup_interval_ms do
    case Application.get_env(
           :ferricstore,
           :flow_governance_limit_storage_cleanup_interval_ms,
           @default_interval_ms
         ) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> @default_interval_ms
    end
  end

  defp cleanup_pages_per_tick do
    case Application.get_env(
           :ferricstore,
           :flow_governance_limit_storage_cleanup_pages_per_tick,
           @default_pages_per_tick
         ) do
      value when is_integer(value) and value > 0 -> min(value, @max_pages_per_tick)
      _invalid -> @default_pages_per_tick
    end
  end
end
