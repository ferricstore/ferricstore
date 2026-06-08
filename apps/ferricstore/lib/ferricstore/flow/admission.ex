defmodule Ferricstore.Flow.Admission do
  @moduledoc """
  Low-overhead Flow write admission gate.

  This gate is intentionally tiny on the hot path: create commands do one
  atomic read. The operational guard owns the expensive RSS/disk checks and
  updates this projection periodically.
  """

  @pt_key :ferricstore_flow_admission
  @paused_slot 1
  @retry_after_ms_slot 2
  @reason_slot 3

  @reason_codes %{
    none: 0,
    rss_pressure: 1,
    disk_pressure: 2,
    operational_pressure: 3,
    memory_guard: 4
  }

  @reason_atoms %{
    0 => :none,
    1 => :rss_pressure,
    2 => :disk_pressure,
    3 => :operational_pressure,
    4 => :memory_guard
  }

  @default_retry_after_ms 1_000

  @spec init() :: :ok
  def init do
    _ = ensure_ref()
    :ok
  end

  @spec reject_new_creates?() :: boolean()
  def reject_new_creates? do
    case :persistent_term.get(@pt_key, nil) do
      nil -> false
      ref -> :atomics.get(ref, @paused_slot) == 1
    end
  rescue
    _ -> false
  end

  @spec pause_creates(atom(), non_neg_integer()) :: :ok
  def pause_creates(reason, retry_after_ms) do
    ref = ensure_ref()
    :atomics.put(ref, @paused_slot, 1)
    :atomics.put(ref, @retry_after_ms_slot, max(retry_after_ms, 0))

    :atomics.put(
      ref,
      @reason_slot,
      Map.get(@reason_codes, reason, @reason_codes.operational_pressure)
    )

    :ok
  end

  @spec clear_create_pause() :: :ok
  def clear_create_pause do
    ref = ensure_ref()
    :atomics.put(ref, @paused_slot, 0)
    :atomics.put(ref, @retry_after_ms_slot, 0)
    :atomics.put(ref, @reason_slot, @reason_codes.none)
    :ok
  end

  @spec status() :: map()
  def status do
    case :persistent_term.get(@pt_key, nil) do
      nil ->
        %{
          reject_new_creates?: false,
          retry_after_ms: 0,
          reason: :none
        }

      ref ->
        %{
          reject_new_creates?: :atomics.get(ref, @paused_slot) == 1,
          retry_after_ms: :atomics.get(ref, @retry_after_ms_slot),
          reason: Map.get(@reason_atoms, :atomics.get(ref, @reason_slot), :operational_pressure)
        }
    end
  rescue
    _ ->
      %{
        reject_new_creates?: false,
        retry_after_ms: 0,
        reason: :none
      }
  end

  @spec overload_error(atom(), non_neg_integer()) :: {:error, binary()}
  def overload_error(
        default_reason \\ :operational_pressure,
        default_retry_after_ms \\ @default_retry_after_ms
      ) do
    current = status()
    retry_after_ms = positive_int(current.retry_after_ms, default_retry_after_ms)
    reason = if current.reject_new_creates?, do: current.reason, else: default_reason

    {:error,
     "BUSY FerricStore overloaded: new Flow creates paused; " <>
       "retry_after_ms=#{retry_after_ms} reason=#{reason}"}
  end

  defp ensure_ref do
    case :persistent_term.get(@pt_key, nil) do
      nil ->
        ref = :atomics.new(3, signed: false)
        :atomics.put(ref, @reason_slot, @reason_codes.none)
        :persistent_term.put(@pt_key, ref)
        ref

      ref ->
        ref
    end
  end

  defp positive_int(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_int(_value, default), do: default
end
