defmodule Ferricstore.Flow.Governance.Budget do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision

  @default_settled_reservation_limit 128

  defstruct [
    :scope,
    :limit,
    :window_ms,
    :window_start_ms,
    used: 0,
    reservations: %{},
    settled_reservations: %{},
    settled_reservation_order: [],
    settled_reservation_limit: @default_settled_reservation_limit
  ]

  def fixed_window(scope, limit, window_ms, opts)
      when is_binary(scope) and is_integer(limit) and limit >= 0 and is_integer(window_ms) and
             window_ms > 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    %__MODULE__{scope: scope, limit: limit, window_ms: window_ms, window_start_ms: now_ms}
  end

  def reserve(%__MODULE__{} = budget, amount, opts) when is_integer(amount) and amount > 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    reservation_id = Keyword.fetch!(opts, :reservation_id)
    budget = budget |> normalize() |> maybe_reset(now_ms)

    if budget.used + amount <= budget.limit do
      reservation = %{
        id: reservation_id,
        amount: amount,
        actual_amount: nil,
        status: :reserved,
        usage: nil,
        reserved_at_ms: now_ms,
        settled_at_ms: nil,
        window_start_ms: budget.window_start_ms
      }

      {:ok,
       %{
         budget
         | used: budget.used + amount,
           reservations: Map.put(budget.reservations, reservation_id, reservation)
       }, reservation}
    else
      {:error,
       Decision.budget_exhausted(%{
         scope: budget.scope,
         requested: amount,
         used: budget.used,
         limit: budget.limit,
         window_ms: budget.window_ms,
         retry_after_ms: retry_after_ms(budget, now_ms)
       }), budget}
    end
  end

  def commit(%__MODULE__{} = budget, reservation_id, actual_amount, opts)
      when is_binary(reservation_id) and reservation_id != "" and is_integer(actual_amount) and
             actual_amount >= 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    usage = Keyword.get(opts, :usage)
    budget = budget |> normalize() |> maybe_reset(now_ms)

    case Map.fetch(budget.reservations, reservation_id) do
      {:ok, %{status: :reserved} = reservation} ->
        {used, overage_amount} = committed_used(budget, reservation, actual_amount)

        settled =
          reservation
          |> Map.put(:actual_amount, actual_amount)
          |> Map.put(:status, :committed)
          |> Map.put(:usage, usage)
          |> Map.put(:settled_at_ms, now_ms)
          |> Map.put(:overage_amount, overage_amount)

        {:ok, %{settle_reservation(budget, reservation_id, settled) | used: used}, settled}

      {:ok, %{status: :committed, actual_amount: ^actual_amount} = reservation} ->
        {:ok, budget, reservation}

      {:ok, %{status: status}} ->
        {:error, "ERR flow budget reservation already #{status}", budget}

      :error ->
        settled_result(budget, reservation_id, :commit, actual_amount)
    end
  end

  def release(%__MODULE__{} = budget, reservation_id, opts)
      when is_binary(reservation_id) and reservation_id != "" do
    now_ms = Keyword.fetch!(opts, :now_ms)
    budget = budget |> normalize() |> maybe_reset(now_ms)

    case Map.fetch(budget.reservations, reservation_id) do
      {:ok, %{status: :reserved} = reservation} ->
        used =
          if reservation.window_start_ms == budget.window_start_ms do
            max(budget.used - reservation.amount, 0)
          else
            budget.used
          end

        settled =
          reservation
          |> Map.put(:actual_amount, 0)
          |> Map.put(:status, :released)
          |> Map.put(:settled_at_ms, now_ms)

        {:ok, %{settle_reservation(budget, reservation_id, settled) | used: used}, settled}

      {:ok, %{status: :released} = reservation} ->
        {:ok, budget, reservation}

      {:ok, %{status: status}} ->
        {:error, "ERR flow budget reservation already #{status}", budget}

      :error ->
        settled_result(budget, reservation_id, :release, 0)
    end
  end

  def public(%__MODULE__{} = budget) do
    budget = normalize(budget)

    %{
      scope: budget.scope,
      limit: budget.limit,
      window_ms: budget.window_ms,
      window_start_ms: budget.window_start_ms,
      used: budget.used,
      remaining: max(budget.limit - budget.used, 0),
      over_budget: budget.used > budget.limit,
      reservations_count: map_size(budget.reservations) + map_size(budget.settled_reservations)
    }
  end

  def public_reservation(reservation) when is_map(reservation) do
    %{
      reservation_id: reservation.id,
      reserved_amount: reservation.amount,
      actual_amount: reservation.actual_amount,
      status: reservation.status,
      usage: reservation.usage,
      overage_amount: Map.get(reservation, :overage_amount, 0),
      reserved_at_ms: reservation.reserved_at_ms,
      settled_at_ms: reservation.settled_at_ms
    }
  end

  def normalize(%__MODULE__{} = budget) do
    reservations =
      case Map.get(budget, :reservations) do
        reservations when is_map(reservations) -> reservations
        _other -> %{}
      end

    settled_reservations =
      case Map.get(budget, :settled_reservations) do
        settled_reservations when is_map(settled_reservations) -> settled_reservations
        _other -> %{}
      end

    settled_reservation_order =
      case Map.get(budget, :settled_reservation_order) do
        settled_reservation_order when is_list(settled_reservation_order) ->
          settled_reservation_order

        _other ->
          Map.keys(settled_reservations)
      end

    settled_reservation_limit =
      case Map.get(budget, :settled_reservation_limit) do
        limit when is_integer(limit) and limit > 0 -> limit
        _other -> @default_settled_reservation_limit
      end

    %{
      budget
      | reservations: reservations,
        settled_reservations: settled_reservations,
        settled_reservation_order: settled_reservation_order,
        settled_reservation_limit: settled_reservation_limit
    }
  end

  defp maybe_reset(%__MODULE__{} = budget, now_ms) do
    if now_ms - budget.window_start_ms >= budget.window_ms do
      %{
        budget
        | used: 0,
          window_start_ms: now_ms,
          reservations: keep_unsettled_reservations(budget.reservations),
          settled_reservations: %{},
          settled_reservation_order: []
      }
    else
      budget
    end
  end

  defp settled_result(%__MODULE__{} = budget, reservation_id, operation, actual_amount) do
    case Map.fetch(budget.settled_reservations, reservation_id) do
      {:ok, %{status: :committed, actual_amount: ^actual_amount} = reservation}
      when operation == :commit ->
        {:ok, budget, reservation}

      {:ok, %{status: :released} = reservation} when operation == :release ->
        {:ok, budget, reservation}

      {:ok, %{status: status}} ->
        {:error, "ERR flow budget reservation already #{status}", budget}

      :error ->
        {:error, "ERR flow budget reservation not found", budget}
    end
  end

  defp settle_reservation(%__MODULE__{} = budget, reservation_id, settled) do
    order =
      [reservation_id | List.delete(budget.settled_reservation_order, reservation_id)]
      |> Enum.take(budget.settled_reservation_limit)

    settled_reservations =
      budget.settled_reservations
      |> Map.put(reservation_id, settled)
      |> Map.take(order)

    %{
      budget
      | reservations: Map.delete(budget.reservations, reservation_id),
        settled_reservations: settled_reservations,
        settled_reservation_order: order
    }
  end

  defp keep_unsettled_reservations(reservations) do
    Map.filter(reservations, fn {_id, reservation} -> reservation.status == :reserved end)
  end

  defp committed_used(%__MODULE__{} = budget, reservation, actual_amount) do
    if reservation.window_start_ms == budget.window_start_ms do
      delta = actual_amount - reservation.amount
      used = max(budget.used + delta, 0)
      {used, max(used - budget.limit, 0)}
    else
      {budget.used, max(actual_amount - reservation.amount, 0)}
    end
  end

  defp retry_after_ms(%__MODULE__{} = budget, now_ms) do
    max(budget.window_ms - (now_ms - budget.window_start_ms), 0)
  end
end
