defmodule Ferricstore.Flow.Governance.Decision do
  @moduledoc false

  def limit_exceeded(fields) do
    error("GOVERNANCE_LIMIT_EXCEEDED", fields)
  end

  def budget_exhausted(fields) do
    error("GOVERNANCE_BUDGET_EXHAUSTED", fields)
  end

  def approval_required(fields) do
    error("GOVERNANCE_APPROVAL_REQUIRED", fields)
  end

  def effect_denied(fields) do
    error("GOVERNANCE_EFFECT_DENIED", fields)
  end

  def circuit_open(fields) do
    error("GOVERNANCE_CIRCUIT_OPEN", fields)
  end

  def unavailable(fields) do
    error("GOVERNANCE_UNAVAILABLE", fields)
  end

  def conflict(fields) do
    error("GOVERNANCE_CONFLICT", fields)
  end

  def error(code, fields) when is_binary(code) and is_map(fields) do
    fields
    |> Map.put_new(:message, default_message(code))
    |> Map.put_new(:reason, reason(code))
    |> Map.put(:code, code)
  end

  defp default_message("GOVERNANCE_LIMIT_EXCEEDED"), do: "Governance limit exceeded"
  defp default_message("GOVERNANCE_BUDGET_EXHAUSTED"), do: "Governance budget exhausted"
  defp default_message("GOVERNANCE_APPROVAL_REQUIRED"), do: "Governance approval required"
  defp default_message("GOVERNANCE_EFFECT_DENIED"), do: "Governance effect denied"
  defp default_message("GOVERNANCE_CIRCUIT_OPEN"), do: "Governance circuit is open"
  defp default_message("GOVERNANCE_UNAVAILABLE"), do: "Governance is unavailable"
  defp default_message("GOVERNANCE_CONFLICT"), do: "Governance conflict"
  defp default_message(_code), do: "Governance denied"

  defp reason("GOVERNANCE_LIMIT_EXCEEDED"), do: "limit_exhausted"
  defp reason("GOVERNANCE_BUDGET_EXHAUSTED"), do: "budget_exhausted"
  defp reason("GOVERNANCE_APPROVAL_REQUIRED"), do: "approval_required"
  defp reason("GOVERNANCE_EFFECT_DENIED"), do: "effect_denied"
  defp reason("GOVERNANCE_CIRCUIT_OPEN"), do: "circuit_open"
  defp reason("GOVERNANCE_UNAVAILABLE"), do: "governance_unavailable"
  defp reason("GOVERNANCE_CONFLICT"), do: "governance_conflict"
  defp reason(_code), do: "governance_denied"
end
