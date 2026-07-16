defmodule Ferricstore.Raft.MembershipGate do
  @moduledoc false

  @resource {__MODULE__, :membership}

  @spec with_stable_membership((-> result)) :: result | {:error, :membership_gate_unavailable}
        when result: term()
  def with_stable_membership(fun) when is_function(fun, 0), do: with_lock(fun)

  @spec with_membership_change((-> result)) :: result | {:error, :membership_gate_unavailable}
        when result: term()
  def with_membership_change(fun) when is_function(fun, 0), do: with_lock(fun)

  defp with_lock(fun) do
    lock = {@resource, self()}
    nodes = [node() | Node.list()] |> Enum.uniq() |> Enum.sort()

    case :global.trans(lock, fun, nodes) do
      :aborted -> {:error, :membership_gate_unavailable}
      result -> result
    end
  end
end
