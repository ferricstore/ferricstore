defmodule Ferricstore.ErrorReasons do
  @moduledoc false

  @write_timeout_unknown {:error, {:timeout, :unknown_outcome}}

  @spec write_timeout_unknown() :: {:error, {:timeout, :unknown_outcome}}
  def write_timeout_unknown, do: @write_timeout_unknown

  @doc """
  The client-facing text for an internal error reason, or `nil` when it has
  none. One replicated entry applies a bounded amount of work, and a single
  command past that bound is refused as a whole.
  """
  @spec client_message(term(), map() | nil) :: binary() | nil
  def client_message(:compound_member_apply_budget_exceeded, ctx),
    do: "ERR too many members in one command" <> limit_suffix(ctx, :compound_member_apply_budget)

  def client_message(:batch_command_apply_budget_exceeded, ctx),
    do: "ERR too many items in one command" <> limit_suffix(ctx, :batch_command_apply_budget)

  def client_message(_reason, _ctx), do: nil

  defp limit_suffix(%{apply_context: %{} = context}, field) do
    case Map.get(context, field) do
      limit when is_integer(limit) -> " (the limit is #{limit})"
      _unknown -> ""
    end
  end

  defp limit_suffix(_ctx, _field), do: ""
end
