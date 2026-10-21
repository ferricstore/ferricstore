defmodule Ferricstore.Flow.Governance.Args do
  @moduledoc false

  # The error a governance call answers when no clause takes its arguments:
  # the first argument, in order, whose form is wrong, so a bad id, scope or
  # amount is not reported as bad opts. `checks` are {name, value, kind}.

  @spec error(binary(), [{atom(), term(), atom()}]) :: {:error, binary()}
  def error(subject, checks) do
    case Enum.find(checks, fn {_name, value, kind} -> not valid?(kind, value) end) do
      {name, _value, kind} -> {:error, "ERR flow #{subject} #{name} must be #{describe(kind)}"}
      nil -> {:error, "ERR flow #{subject} opts must be a keyword list"}
    end
  end

  defp valid?(:non_empty_string, value), do: is_binary(value) and value != ""
  defp valid?(:string, value), do: is_binary(value)
  defp valid?(:positive_integer, value), do: is_integer(value) and value > 0
  defp valid?(:non_negative_integer, value), do: is_integer(value) and value >= 0

  defp describe(:non_empty_string), do: "a non-empty string"
  defp describe(:string), do: "a string"
  defp describe(:positive_integer), do: "a positive integer"
  defp describe(:non_negative_integer), do: "a non-negative integer"
end
