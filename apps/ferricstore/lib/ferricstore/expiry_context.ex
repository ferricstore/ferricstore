defmodule Ferricstore.ExpiryContext do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.HLC

  @type t ::
          {:request, non_neg_integer(), non_neg_integer()}
          | {:replicated_apply, non_neg_integer()}
          | {:replicated_apply, non_neg_integer(), non_neg_integer()}
  @type classification :: :live | :expired | {:unsafe, :hlc_drift_exceeded}

  @spec capture() :: t()
  def capture do
    case {CommandTime.apply_now_ms(), CommandTime.apply_wall_ms()} do
      {{:ok, now_ms}, {:ok, wall_ms}} ->
        {:replicated_apply, now_ms, wall_ms}

      {{:ok, now_ms}, :none} ->
        {:replicated_apply, now_ms}

      {:none, _wall} ->
        {now_ms, wall_ms} = HLC.read_snapshot_ms()
        {:request, now_ms, wall_ms}
    end
  end

  @doc false
  @spec normalize(t()) :: t()
  def normalize({:request, now_ms, wall_ms} = context)
      when is_integer(now_ms) and now_ms >= 0 and is_integer(wall_ms) and wall_ms >= 0,
      do: context

  def normalize({:replicated_apply, now_ms} = context)
      when is_integer(now_ms) and now_ms >= 0,
      do: context

  def normalize({:replicated_apply, now_ms, wall_ms} = context)
      when is_integer(now_ms) and now_ms >= 0 and is_integer(wall_ms) and wall_ms >= 0,
      do: context

  @spec now_ms(t()) :: non_neg_integer()
  def now_ms({:request, now_ms, _wall_ms}), do: now_ms
  def now_ms({:replicated_apply, now_ms}), do: now_ms
  def now_ms({:replicated_apply, now_ms, _wall_ms}), do: now_ms

  @spec safe_expiry_cutoff_ms(t()) :: non_neg_integer()
  def safe_expiry_cutoff_ms({:request, now_ms, wall_ms}) do
    if HLC.unsafe_drift?(now_ms, wall_ms), do: wall_ms, else: now_ms
  end

  def safe_expiry_cutoff_ms({:replicated_apply, now_ms}), do: now_ms

  def safe_expiry_cutoff_ms({:replicated_apply, now_ms, wall_ms}) do
    if HLC.unsafe_drift?(now_ms, wall_ms), do: wall_ms, else: now_ms
  end

  @spec classify(t(), non_neg_integer()) :: classification()
  def classify(_context, 0), do: :live

  def classify({:request, now_ms, _wall_ms}, expire_at_ms)
      when is_integer(expire_at_ms) and expire_at_ms > now_ms,
      do: :live

  def classify({:replicated_apply, now_ms}, expire_at_ms)
      when is_integer(expire_at_ms) and expire_at_ms > now_ms,
      do: :live

  def classify({:replicated_apply, now_ms, _wall_ms}, expire_at_ms)
      when is_integer(expire_at_ms) and expire_at_ms > now_ms,
      do: :live

  def classify({:replicated_apply, _now_ms}, expire_at_ms)
      when is_integer(expire_at_ms) and expire_at_ms > 0,
      do: :expired

  def classify({:request, now_ms, wall_ms}, expire_at_ms)
      when is_integer(expire_at_ms) and expire_at_ms > 0 do
    if HLC.unsafe_expiry?(expire_at_ms, now_ms, wall_ms),
      do: {:unsafe, :hlc_drift_exceeded},
      else: :expired
  end

  def classify({:replicated_apply, now_ms, wall_ms}, expire_at_ms)
      when is_integer(expire_at_ms) and expire_at_ms > 0 do
    if HLC.unsafe_expiry?(expire_at_ms, now_ms, wall_ms),
      do: {:unsafe, :hlc_drift_exceeded},
      else: :expired
  end
end
