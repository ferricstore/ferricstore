defmodule Ferricstore.Store.PromotedPublication do
  @moduledoc false
  alias Ferricstore.Store.PublicationEpoch

  @scope {__MODULE__, :scope}

  # The logical mutation owns the scope, but its epoch starts only when the
  # first durable result is published to the keydir. Subsequent publications
  # share that epoch until the operation (including rollback) has finished.
  def with_scope(fun) when is_function(fun, 0) do
    case Process.get(@scope) do
      %{depth: depth} = scope ->
        Process.put(@scope, %{scope | depth: depth + 1})

        try do
          finish_result(fun.())
        catch
          kind, reason ->
            fail_scope()
            :erlang.raise(kind, reason, __STACKTRACE__)
        after
          Process.put(@scope, Map.update!(Process.get(@scope), :depth, &(&1 - 1)))
        end

      _ ->
        Process.put(@scope, %{depth: 1, tokens: %{}, fences: %{}, failed: false})

        try do
          finish_result(fun.())
        catch
          kind, reason ->
            fail_scope()
            :erlang.raise(kind, reason, __STACKTRACE__)
        after
          finish()
          Process.delete(@scope)
        end
    end
  end

  def finish do
    case Process.get(@scope) do
      %{depth: 1, tokens: tokens, fences: fences, failed: failed} = scope ->
        unless failed do
          Enum.each(fences, fn {_key, {table, row, inserted?}} ->
            if inserted?, do: :ets.delete_object(table, row)
          end)
        end

        Enum.each(tokens, fn {_key, token} -> PublicationEpoch.end_write(token) end)
        Process.put(@scope, %{scope | tokens: %{}, fences: %{}})

      _ ->
        :ok
    end

    :ok
  end

  def publish_existing(owner, fun) when is_map(owner) and is_function(fun, 0) do
    {ctx, index} = context(owner)
    if owned_epoch?(ctx, index), do: fun.(), else: PublicationEpoch.with_write(ctx, index, fun)
  end

  # A failed or killed publisher may leave partial cache rows. Such a fence
  # persists until recovery resets it; epoch repair alone must not authorize a
  # cache shortcut. The caller retains its serialized fallback in this case.
  def read(ctx, index, fun) when is_map(ctx) and is_function(fun, 0) do
    if lifecycle_clear?(ctx, index) do
      PublicationEpoch.read(ctx, [index], fn ->
        generation = lifecycle_generation(ctx, index)

        if fence_clear?(ctx, index) and lifecycle_clear?(ctx, index) do
          result = fun.()

          if lifecycle_clear?(ctx, index) and generation == lifecycle_generation(ctx, index),
            do: result,
            else: :fallback
        else
          :fallback
        end
      end)
    else
      :fallback
    end
  end

  # Recovery and snapshot replacement may repopulate many rows without ordinary
  # mutation callbacks. Cache shortcuts must use the serialized reader until
  # the lifecycle operation has completed successfully.
  def with_lifecycle(owner, fun) when is_map(owner) and is_function(fun, 0) do
    {ctx, index} = context(owner)
    table = fence_table(ctx, index)

    if is_nil(table) do
      fun.()
    else
      key = {__MODULE__, :lifecycle, index}
      row = {key, self(), make_ref()}

      case acquire_lifecycle(table, key, row) do
        :borrowed ->
          fun.()

        :owned ->
          :ets.insert(table, {{__MODULE__, :lifecycle_generation, index}, make_ref()})

          try do
            result = fun.()

            if lifecycle_success?(result) do
              reset(ctx, index)
              :ets.delete_object(table, row)
            else
              :ets.insert(table, {key, :failed})
            end

            result
          catch
            kind, reason ->
              :ets.insert(table, {key, :failed})
              :erlang.raise(kind, reason, __STACKTRACE__)
          end
      end
    end
  end

  defp acquire_lifecycle(table, key, row) do
    if :ets.insert_new(table, row) do
      :owned
    else
      case :ets.lookup(table, key) do
        [{^key, owner, _tag}] when owner == self() ->
          :borrowed

        [{^key, owner, _tag} = previous] when is_pid(owner) ->
          if Process.alive?(owner) do
            Process.sleep(1)
            acquire_lifecycle(table, key, row)
          else
            replace_lifecycle(table, key, previous, row)
          end

        [previous] ->
          replace_lifecycle(table, key, previous, row)

        [] ->
          acquire_lifecycle(table, key, row)
      end
    end
  end

  defp replace_lifecycle(table, key, previous, row) do
    # Failed/dead replacement must never leave a momentarily clear barrier.
    if :ets.select_replace(table, [{previous, [], [{:const, row}]}]) == 1,
      do: :owned,
      else: acquire_lifecycle(table, key, row)
  end

  def reset(ctx, index) do
    case fence_table(ctx, index) do
      nil -> :ok
      table -> :ets.delete(table, {__MODULE__, :publisher, index})
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  def publish(owner, fun) when is_map(owner) and is_function(fun, 0) do
    {ctx, index} = context(owner)

    case Process.get(@scope) do
      %{tokens: tokens} = scope ->
        key = {Map.get(ctx, :publication_epoch), index}

        unless Map.has_key?(tokens, key) or owned_epoch?(ctx, index) do
          token = PublicationEpoch.begin_write(ctx, index)
          Process.put(@scope, %{scope | tokens: Map.put(tokens, key, token)})
        end

        register_fence(ctx, index)

        try do
          run_publish(fun)
        catch
          kind, reason ->
            fail_scope()
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      _ ->
        with_scope(fn -> publish(owner, fun) end)
    end
  end

  defp context(%{instance_ctx: ctx, shard_index: index}) when is_map(ctx), do: {ctx, index}
  defp context(%{instance_ctx: ctx, index: index}) when is_map(ctx), do: {ctx, index}
  defp context(_owner), do: {%{}, 0}

  defp owned_epoch?(%{publication_epoch: ref, latch_refs: refs}, index)
       when is_reference(ref) and is_tuple(refs) and index >= 0 and index < tuple_size(refs) do
    table = elem(refs, index)
    key = {PublicationEpoch, :writer, index}
    :ets.lookup(table, key) == [{key, self()}] and rem(:atomics.get(ref, index + 1), 2) == 1
  rescue
    ArgumentError -> false
  end

  defp owned_epoch?(_ctx, _index), do: false

  defp fail_scope do
    case Process.get(@scope) do
      scope when is_map(scope) -> Process.put(@scope, %{scope | failed: true})
      _ -> :ok
    end
  end

  defp finish_result(result) do
    if failed_result?(result), do: fail_scope()
    result
  end

  defp failed_result?({:error, _}), do: true
  defp failed_result?({:error, _, _}), do: true
  defp failed_result?(results) when is_list(results), do: Enum.any?(results, &failed_result?/1)
  defp failed_result?(_result), do: false

  defp register_fence(ctx, index) do
    scope = Process.get(@scope)
    key = {Map.get(ctx, :publication_epoch), index}
    table = fence_table(ctx, index)

    unless is_nil(table) or Map.has_key?(scope.fences, key) do
      row = {{__MODULE__, :publisher, index}, self()}
      inserted? = :ets.insert_new(table, row)
      Process.put(@scope, %{scope | fences: Map.put(scope.fences, key, {table, row, inserted?})})
    end
  end

  defp fence_clear?(ctx, index) do
    case fence_table(ctx, index) do
      nil -> false
      table -> :ets.lookup(table, {__MODULE__, :publisher, index}) == []
    end
  rescue
    ArgumentError -> false
  end

  defp lifecycle_clear?(ctx, index) do
    case fence_table(ctx, index) do
      nil -> false
      table -> :ets.lookup(table, {__MODULE__, :lifecycle, index}) == []
    end
  rescue
    ArgumentError -> false
  end

  defp lifecycle_generation(ctx, index) do
    case fence_table(ctx, index) do
      nil -> :unavailable
      table -> :ets.lookup(table, {__MODULE__, :lifecycle_generation, index})
    end
  rescue
    ArgumentError -> :unavailable
  end

  defp lifecycle_success?({:ok, handle}), do: not blocked_handle?(handle)
  defp lifecycle_success?({:ok, handle, _}), do: not blocked_handle?(handle)
  defp lifecycle_success?(result) when is_map(result), do: not blocked_handle?(result)
  defp lifecycle_success?(_result), do: false

  defp blocked_handle?(%{blocked_error: _}), do: true
  defp blocked_handle?(%{writes_paused: true}), do: true
  defp blocked_handle?(_result), do: false

  defp fence_table(%{latch_refs: refs}, index)
       when is_tuple(refs) and index >= 0 and index < tuple_size(refs),
       do: elem(refs, index)

  defp fence_table(_ctx, _index), do: nil

  if Mix.env() == :test do
    defp run_publish(fun) do
      case Process.get(:ferricstore_promoted_publication_hook) do
        hook when is_function(hook, 0) -> hook.()
        _ -> :ok
      end

      fun.()
    end
  else
    defp run_publish(fun), do: fun.()
  end
end
