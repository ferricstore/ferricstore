defmodule Ferricstore.Flow.Governance.Budget do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision

  @default_settled_reservation_limit 128
  @max_active_reservations 4_096
  @max_usage_bytes 262_144
  @max_usage_depth 64
  @max_usage_nodes 4_096
  @max_reservation_id_bytes 256
  @max_dimension_bytes 65_535
  @max_record_bytes 900_000
  @max_exact_integer 9_007_199_254_740_991

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
    budget = normalize(budget)

    if now_ms < budget.window_start_ms do
      {:error, "ERR flow budget now_ms cannot precede window_start_ms", budget}
    else
      budget = maybe_reset(budget, now_ms)

      case existing_reservation(budget, reservation_id) do
        {:active, %{amount: ^amount} = reservation} ->
          {:ok, budget, reservation}

        {:active, _reservation} ->
          {:error, "ERR flow budget reservation id already exists", budget}

        :settled ->
          {:error, "ERR flow budget reservation id already exists", budget}

        :missing ->
          create_reservation(budget, amount, reservation_id, now_ms)
      end
    end
  end

  def commit(%__MODULE__{} = budget, reservation_id, actual_amount, opts)
      when is_binary(reservation_id) and reservation_id != "" and is_integer(actual_amount) and
             actual_amount >= 0 do
    now_ms = Keyword.fetch!(opts, :now_ms)
    usage = Keyword.get(opts, :usage)
    budget = normalize(budget)

    if now_ms < budget.window_start_ms do
      {:error, "ERR flow budget now_ms cannot precede window_start_ms", budget}
    else
      budget = maybe_reset(budget, now_ms)

      case Map.fetch(budget.reservations, reservation_id) do
        {:ok, %{status: :reserved} = reservation} ->
          if now_ms < reservation.reserved_at_ms do
            {:error, "ERR flow budget now_ms cannot precede reservation", budget}
          else
            with true <- valid_usage?(usage),
                 {:ok, used, overage_amount} <-
                   committed_used(budget, reservation, actual_amount) do
              settled =
                reservation
                |> Map.put(:actual_amount, actual_amount)
                |> Map.put(:status, :committed)
                |> Map.put(:usage, usage)
                |> Map.put(:settled_at_ms, now_ms)
                |> Map.put(:overage_amount, overage_amount)

              {:ok, %{settle_reservation(budget, reservation_id, settled) | used: used}, settled}
            else
              false -> {:error, "ERR flow budget usage must be a bounded portable term", budget}
              {:error, reason} -> {:error, reason, budget}
            end
          end

        {:ok, %{status: :committed, actual_amount: ^actual_amount} = reservation} ->
          {:ok, budget, reservation}

        {:ok, %{status: status}} ->
          {:error, "ERR flow budget reservation already #{status}", budget}

        :error ->
          settled_result(budget, reservation_id, :commit, actual_amount)
      end
    end
  end

  def release(%__MODULE__{} = budget, reservation_id, opts)
      when is_binary(reservation_id) and reservation_id != "" do
    now_ms = Keyword.fetch!(opts, :now_ms)
    budget = normalize(budget)

    if now_ms < budget.window_start_ms do
      {:error, "ERR flow budget now_ms cannot precede window_start_ms", budget}
    else
      budget = maybe_reset(budget, now_ms)

      case Map.fetch(budget.reservations, reservation_id) do
        {:ok, %{status: :reserved} = reservation} ->
          if now_ms < reservation.reserved_at_ms do
            {:error, "ERR flow budget now_ms cannot precede reservation", budget}
          else
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
          end

        {:ok, %{status: :released} = reservation} ->
          {:ok, budget, reservation}

        {:ok, %{status: status}} ->
          {:error, "ERR flow budget reservation already #{status}", budget}

        :error ->
          settled_result(budget, reservation_id, :release, 0)
      end
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

  @doc false
  def valid?(%__MODULE__{} = budget) do
    required_binary?(budget.scope) and non_negative_integer?(budget.limit) and
      positive_integer?(budget.window_ms) and non_negative_integer?(budget.window_start_ms) and
      non_negative_integer?(budget.used) and is_map(budget.reservations) and
      is_map(budget.settled_reservations) and is_list(budget.settled_reservation_order) and
      positive_integer?(budget.settled_reservation_limit) and
      budget.settled_reservation_limit <= @default_settled_reservation_limit and
      map_size(budget.reservations) <= @max_active_reservations and
      map_size(budget.settled_reservations) <= budget.settled_reservation_limit and
      valid_settled_order?(budget) and valid_active_reservations?(budget) and
      valid_current_window_reservations?(budget) and valid_settled_reservations?(budget) and
      :erlang.external_size(budget) <= @max_record_bytes
  end

  def valid?(_budget), do: false

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

      if used <= @max_exact_integer do
        {:ok, used, max(used - budget.limit, 0)}
      else
        {:error, "ERR flow budget aggregate usage exceeds durable integer range"}
      end
    else
      {:ok, budget.used, max(actual_amount - reservation.amount, 0)}
    end
  end

  defp retry_after_ms(%__MODULE__{} = budget, now_ms) do
    max(budget.window_ms - (now_ms - budget.window_start_ms), 0)
  end

  defp existing_reservation(%__MODULE__{} = budget, reservation_id) do
    case Map.fetch(budget.reservations, reservation_id) do
      {:ok, reservation} ->
        {:active, reservation}

      :error ->
        if Map.has_key?(budget.settled_reservations, reservation_id), do: :settled, else: :missing
    end
  end

  defp create_reservation(%__MODULE__{} = budget, amount, reservation_id, now_ms) do
    cond do
      map_size(budget.reservations) >= @max_active_reservations ->
        {:error, "ERR flow budget has too many active reservations", budget}

      budget.used + amount <= budget.limit ->
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

      true ->
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

  defp valid_settled_order?(budget) do
    order = budget.settled_reservation_order

    case collect_unique_ids(order, MapSet.new(), 0, budget.settled_reservation_limit) do
      {:ok, ids, count} ->
        count == map_size(budget.settled_reservations) and
          Enum.all?(ids, &Map.has_key?(budget.settled_reservations, &1))

      :error ->
        false
    end
  end

  defp collect_unique_ids([], ids, count, _limit), do: {:ok, ids, count}

  defp collect_unique_ids([id | rest], ids, count, limit)
       when count < limit and is_binary(id) and id != "" do
    if MapSet.member?(ids, id) do
      :error
    else
      collect_unique_ids(rest, MapSet.put(ids, id), count + 1, limit)
    end
  end

  defp collect_unique_ids(_ids, _seen, _count, _limit), do: :error

  defp valid_active_reservations?(budget) do
    Enum.all?(budget.reservations, fn {id, reservation} ->
      valid_reservation_id?(id) and is_map(reservation) and reservation[:id] == id and
        reservation[:status] == :reserved and positive_integer?(reservation[:amount]) and
        is_nil(reservation[:actual_amount]) and
        non_negative_integer?(reservation[:reserved_at_ms]) and
        is_nil(reservation[:settled_at_ms]) and
        valid_reservation_window?(reservation, budget)
    end)
  end

  defp valid_current_window_reservations?(budget) do
    reservable = min(budget.used, budget.limit)

    Enum.reduce_while(budget.reservations, 0, fn {_id, reservation}, total ->
      if reservation[:window_start_ms] == budget.window_start_ms do
        amount = reservation[:amount]

        if amount <= reservable - total do
          {:cont, total + amount}
        else
          {:halt, false}
        end
      else
        {:cont, total}
      end
    end)
    |> is_integer()
  end

  defp valid_settled_reservations?(budget) do
    Enum.all?(budget.settled_reservations, fn {id, reservation} ->
      valid_reservation_id?(id) and is_map(reservation) and reservation[:id] == id and
        reservation[:status] in [:committed, :released] and
        positive_integer?(reservation[:amount]) and
        non_negative_integer?(reservation[:actual_amount]) and
        non_negative_integer?(Map.get(reservation, :overage_amount, 0)) and
        valid_usage?(reservation[:usage]) and
        valid_settled_payload?(reservation) and
        non_negative_integer?(reservation[:reserved_at_ms]) and
        non_negative_integer?(reservation[:settled_at_ms]) and
        reservation[:settled_at_ms] >= reservation[:reserved_at_ms] and
        valid_reservation_window?(reservation, budget)
    end)
  end

  defp valid_settled_payload?(%{status: :released} = reservation) do
    reservation[:actual_amount] == 0 and is_nil(reservation[:usage]) and
      Map.get(reservation, :overage_amount, 0) == 0
  end

  defp valid_settled_payload?(%{status: :committed}), do: true
  defp valid_settled_payload?(_reservation), do: false

  defp valid_reservation_window?(reservation, budget) do
    non_negative_integer?(reservation[:window_start_ms]) and
      reservation[:window_start_ms] <= reservation[:reserved_at_ms] and
      reservation[:window_start_ms] <= budget.window_start_ms
  end

  defp valid_reservation_id?(id),
    do: is_binary(id) and id != "" and byte_size(id) <= @max_reservation_id_bytes

  defp valid_usage?(usage) do
    match?({:ok, _remaining}, validate_usage_term(usage, 0, @max_usage_nodes)) and
      :erlang.external_size(usage) <= @max_usage_bytes
  rescue
    _invalid_term -> false
  end

  defp validate_usage_term(_value, depth, _remaining) when depth > @max_usage_depth, do: :error
  defp validate_usage_term(_value, _depth, remaining) when remaining <= 0, do: :error

  defp validate_usage_term(value, _depth, remaining)
       when is_atom(value) or is_binary(value) or is_integer(value) or is_float(value),
       do: {:ok, remaining - 1}

  defp validate_usage_term([], _depth, remaining), do: {:ok, remaining - 1}

  defp validate_usage_term([value | rest], depth, remaining) do
    with {:ok, remaining} <- validate_usage_term(value, depth + 1, remaining - 1) do
      validate_usage_term(rest, depth + 1, remaining)
    end
  end

  defp validate_usage_term(value, depth, remaining) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> validate_usage_term(depth + 1, remaining - 1)
  end

  defp validate_usage_term(value, depth, remaining) when is_map(value) do
    Enum.reduce_while(value, {:ok, remaining - 1}, fn {key, item}, {:ok, left} ->
      with {:ok, left} <- validate_usage_term(key, depth + 1, left),
           {:ok, left} <- validate_usage_term(item, depth + 1, left) do
        {:cont, {:ok, left}}
      else
        :error -> {:halt, :error}
      end
    end)
  end

  defp validate_usage_term(_value, _depth, _remaining), do: :error

  defp required_binary?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= @max_dimension_bytes

  defp positive_integer?(value),
    do: is_integer(value) and value > 0 and value <= @max_exact_integer

  defp non_negative_integer?(value),
    do: is_integer(value) and value >= 0 and value <= @max_exact_integer
end
