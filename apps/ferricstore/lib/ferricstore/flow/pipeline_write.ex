defmodule Ferricstore.Flow.PipelineWrite do
  @moduledoc false

  alias Ferricstore.Store.Router

  def batch_independent(ctx, ops, callbacks) do
    started = callbacks.start.()

    results =
      ops
      |> Enum.map(fn op ->
        case callbacks.command.(op) do
          {:ok, kind, command} -> {:ok, kind, command}
          {:error, _reason} = error -> error
        end
      end)
      |> ordered_results(ctx, callbacks, [])

    callbacks.observe.(:pipeline_write, started, results)
    results
  end

  def ordered_results([], _ctx, _callbacks, results_rev), do: Enum.reverse(results_rev)

  def ordered_results([{:error, _reason} = error | rest], ctx, callbacks, results_rev) do
    ordered_results(rest, ctx, callbacks, [error | results_rev])
  end

  def ordered_results([{:ok, kind, command} | rest], ctx, callbacks, results_rev)
      when kind in [:state, :terminal] do
    {run, rest} = take_run(rest, kind, [command])

    results_rev =
      kind
      |> run_results(Enum.reverse(run), ctx, callbacks)
      |> Enum.reduce(results_rev, fn result, acc -> [result | acc] end)

    ordered_results(rest, ctx, callbacks, results_rev)
  end

  def create_attrs_from_commands(commands), do: create_attrs(commands, [], MapSet.new())
  def transition_attrs_from_commands(commands), do: transition_attrs(commands, [])

  defp take_run([{:ok, next_kind, command} | rest], kind, acc)
       when next_kind == kind and kind in [:state, :terminal] do
    take_run(rest, kind, [command | acc])
  end

  defp take_run(rest, _kind, acc), do: {acc, rest}

  defp run_results(:state, run, ctx, callbacks) do
    state_run_results(ctx, run, callbacks)
  end

  defp run_results(:terminal, run, ctx, _callbacks) do
    Router.flow_terminal_command_batch(ctx, run)
  end

  defp state_run_results(ctx, keyed_commands, callbacks) do
    case create_attrs_from_commands(keyed_commands) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_create_pipeline_batch(attrs_list)
        |> callbacks.notify.(attrs_list, :state)

      :generic ->
        transition_run_results(ctx, keyed_commands, callbacks)
    end
  end

  defp transition_run_results(ctx, keyed_commands, callbacks) do
    case transition_attrs_from_commands(keyed_commands) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_transition_batch(attrs_list)
        |> callbacks.notify.(attrs_list, :to_state)

      :generic ->
        Router.flow_command_batch(ctx, keyed_commands)
    end
  end

  defp create_attrs([], acc, _seen), do: {:ok, Enum.reverse(acc)}

  defp create_attrs(
         [{key, {:flow_create, _state_key, attrs}} | rest],
         acc,
         seen
       )
       when is_map(attrs) do
    if MapSet.member?(seen, key) do
      :generic
    else
      create_attrs(rest, [attrs | acc], MapSet.put(seen, key))
    end
  end

  defp create_attrs(_keyed_commands, _acc, _seen), do: :generic

  defp transition_attrs([], acc), do: {:ok, Enum.reverse(acc)}

  defp transition_attrs(
         [{_key, {:flow_transition, _state_key, attrs}} | rest],
         acc
       )
       when is_map(attrs) do
    transition_attrs(rest, [attrs | acc])
  end

  defp transition_attrs(_keyed_commands, _acc), do: :generic
end
