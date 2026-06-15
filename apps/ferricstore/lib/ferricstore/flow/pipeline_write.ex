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

  def start_and_claim_attrs_from_commands(commands),
    do: start_and_claim_attrs(commands, [], MapSet.new())

  def named_value_put_attrs_from_commands(commands),
    do: named_value_put_attrs(commands, [], MapSet.new())

  def signal_attrs_from_commands(commands), do: signal_attrs(commands, [])

  def step_continue_attrs_from_commands(commands), do: step_continue_attrs(commands, [])

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
    Router.flow_terminal_command_batch_independent(ctx, run)
  end

  defp state_run_results(ctx, keyed_commands, callbacks) do
    case create_attrs_from_commands(keyed_commands) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_create_pipeline_batch(attrs_list)
        |> callbacks.notify.(attrs_list, :state)

      :generic ->
        start_and_claim_run_results(ctx, keyed_commands, callbacks)
    end
  end

  defp start_and_claim_run_results(ctx, keyed_commands, callbacks) do
    case start_and_claim_attrs_from_commands(keyed_commands) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_start_and_claim_pipeline_batch(attrs_list)
        |> callbacks.notify.(attrs_list, :state)

      :generic ->
        named_value_put_run_results(ctx, keyed_commands, callbacks)
    end
  end

  defp named_value_put_run_results(ctx, keyed_commands, callbacks) do
    case named_value_put_attrs_from_commands(keyed_commands) do
      {:ok, attrs_list} ->
        Router.flow_named_value_put_pipeline_batch(ctx, attrs_list)

      :generic ->
        signal_run_results(ctx, keyed_commands, callbacks)
    end
  end

  defp signal_run_results(ctx, keyed_commands, callbacks) do
    case signal_attrs_from_commands(keyed_commands) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_signal_batch(attrs_list)
        |> callbacks.notify.(attrs_list, :state)

      :generic ->
        step_continue_run_results(ctx, keyed_commands, callbacks)
    end
  end

  defp step_continue_run_results(ctx, keyed_commands, callbacks) do
    case step_continue_attrs_from_commands(keyed_commands) do
      {:ok, attrs_list} ->
        ctx
        |> Router.flow_step_continue_batch(attrs_list)
        |> callbacks.notify.(attrs_list, :to_state)

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

  defp start_and_claim_attrs([], acc, _seen), do: {:ok, Enum.reverse(acc)}

  defp start_and_claim_attrs(
         [{key, {:flow_start_and_claim, _state_key, attrs}} | rest],
         acc,
         seen
       )
       when is_map(attrs) do
    if MapSet.member?(seen, key) do
      :generic
    else
      start_and_claim_attrs(rest, [attrs | acc], MapSet.put(seen, key))
    end
  end

  defp start_and_claim_attrs(_keyed_commands, _acc, _seen), do: :generic

  defp named_value_put_attrs([], acc, _seen), do: {:ok, Enum.reverse(acc)}

  defp named_value_put_attrs(
         [{key, {:flow_named_value_put, _state_key, attrs}} | rest],
         acc,
         seen
       )
       when is_map(attrs) do
    if MapSet.member?(seen, key) do
      :generic
    else
      named_value_put_attrs(rest, [attrs | acc], MapSet.put(seen, key))
    end
  end

  defp named_value_put_attrs(_keyed_commands, _acc, _seen), do: :generic

  defp signal_attrs([], acc), do: {:ok, Enum.reverse(acc)}

  defp signal_attrs(
         [{_key, {:flow_signal, _state_key, attrs}} | rest],
         acc
       )
       when is_map(attrs) do
    signal_attrs(rest, [attrs | acc])
  end

  defp signal_attrs(_keyed_commands, _acc), do: :generic

  defp step_continue_attrs([], acc), do: {:ok, Enum.reverse(acc)}

  defp step_continue_attrs(
         [{_key, {:flow_step_continue, _state_key, attrs}} | rest],
         acc
       )
       when is_map(attrs) do
    step_continue_attrs(rest, [attrs | acc])
  end

  defp step_continue_attrs(_keyed_commands, _acc), do: :generic

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
