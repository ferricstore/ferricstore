defmodule Ferricstore.Flow.RetryPolicy do
  @moduledoc false

  @max_attempts 1_000
  @max_delay_ms 2_592_000_000

  @default %{
    max_attempts: 3,
    backoff: %{kind: :exponential, base_ms: 1_000, max_ms: 30_000, jitter_pct: 20},
    exhausted_to: "failed"
  }

  @type t :: %{
          required(:max_attempts) => non_neg_integer(),
          required(:backoff) => %{
            required(:kind) => :none | :fixed | :linear | :exponential,
            required(:base_ms) => non_neg_integer(),
            required(:max_ms) => non_neg_integer(),
            required(:jitter_pct) => non_neg_integer()
          },
          required(:exhausted_to) => binary()
        }

  @spec default() :: t()
  def default, do: @default

  @spec normalize(term()) :: {:ok, t()} | {:error, binary()}
  def normalize(nil), do: {:ok, default()}

  def normalize(policy) when is_list(policy) do
    if Keyword.keyword?(policy) do
      policy |> Map.new() |> normalize()
    else
      {:error, "ERR flow retry policy must be a map or keyword list"}
    end
  end

  def normalize(policy) when is_map(policy) do
    with {:ok, max_attempts} <- normalize_max_attempts(policy),
         {:ok, backoff} <- normalize_backoff(policy),
         {:ok, exhausted_to} <- normalize_exhausted_to(policy) do
      {:ok, %{max_attempts: max_attempts, backoff: backoff, exhausted_to: exhausted_to}}
    end
  end

  def normalize(_policy), do: {:error, "ERR flow retry policy must be a map or keyword list"}

  @spec attempt_allowed?(t(), non_neg_integer()) :: boolean()
  def attempt_allowed?(%{max_attempts: max_attempts}, attempts) when is_integer(attempts),
    do: attempts <= max_attempts

  @spec next_run_at_ms(t(), binary(), non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def next_run_at_ms(%{backoff: backoff}, id, attempt, now_ms)
      when is_binary(id) and is_integer(attempt) and is_integer(now_ms) do
    delay =
      backoff
      |> delay_ms(attempt)
      |> apply_jitter(backoff, id, attempt, now_ms)
      |> min(backoff.max_ms)

    now_ms + delay
  end

  defp normalize_max_attempts(policy) do
    value = fetch_policy(policy, :max_attempts, "max_attempts", @default.max_attempts)

    if is_integer(value) and value >= 0 and value <= @max_attempts do
      {:ok, value}
    else
      {:error, "ERR flow retry max_attempts must be between 0 and #{@max_attempts}"}
    end
  end

  defp normalize_backoff(policy) do
    backoff = fetch_policy(policy, :backoff, "backoff", %{})

    backoff =
      cond do
        is_nil(backoff) -> %{}
        is_map(backoff) -> backoff
        is_list(backoff) and Keyword.keyword?(backoff) -> Map.new(backoff)
        true -> :invalid
      end

    if backoff == :invalid do
      {:error, "ERR flow retry backoff must be a map or keyword list"}
    else
      with {:ok, kind} <- normalize_backoff_kind(backoff),
           {:ok, base_ms} <-
             normalize_delay(backoff, :base_ms, "base_ms", @default.backoff.base_ms),
           {:ok, max_ms} <- normalize_delay(backoff, :max_ms, "max_ms", @default.backoff.max_ms),
           {:ok, jitter_pct} <- normalize_jitter(backoff) do
        {:ok, %{kind: kind, base_ms: base_ms, max_ms: max_ms, jitter_pct: jitter_pct}}
      end
    end
  end

  defp normalize_backoff_kind(backoff) do
    case fetch_policy(backoff, :kind, "kind", @default.backoff.kind) do
      kind when kind in [:none, :fixed, :linear, :exponential] ->
        {:ok, kind}

      kind when kind in ["none", "fixed", "linear", "exponential"] ->
        {:ok, String.to_existing_atom(kind)}

      _ ->
        {:error, "ERR flow retry backoff kind must be none, fixed, linear, or exponential"}
    end
  end

  defp normalize_delay(backoff, atom_key, string_key, default) do
    value = fetch_policy(backoff, atom_key, string_key, default)

    if is_integer(value) and value >= 0 and value <= @max_delay_ms do
      {:ok, value}
    else
      {:error, "ERR flow retry #{string_key} must be between 0 and #{@max_delay_ms}"}
    end
  end

  defp normalize_jitter(backoff) do
    value = fetch_policy(backoff, :jitter_pct, "jitter_pct", @default.backoff.jitter_pct)

    if is_integer(value) and value >= 0 and value <= 100 do
      {:ok, value}
    else
      {:error, "ERR flow retry jitter_pct must be between 0 and 100"}
    end
  end

  defp normalize_exhausted_to(policy) do
    case fetch_policy(policy, :exhausted_to, "exhausted_to", @default.exhausted_to) do
      "running" ->
        {:error, "ERR flow retry exhausted_to cannot be running"}

      value when is_binary(value) and value != "" ->
        {:ok, value}

      _ ->
        {:error, "ERR flow retry exhausted_to must be a non-empty string"}
    end
  end

  defp delay_ms(%{kind: :none}, _attempt), do: 0
  defp delay_ms(%{kind: :fixed, base_ms: base_ms}, _attempt), do: base_ms

  defp delay_ms(%{kind: :linear, base_ms: base_ms, max_ms: max_ms}, attempt) do
    min(base_ms * max(attempt, 0), max_ms)
  end

  defp delay_ms(%{kind: :exponential, base_ms: base_ms, max_ms: max_ms}, attempt) do
    steps = max(attempt - 1, 0)

    if steps == 0 do
      min(base_ms, max_ms)
    else
      Enum.reduce_while(1..steps, base_ms, fn _, acc ->
        if acc >= max_ms do
          {:halt, max_ms}
        else
          {:cont, min(acc * 2, max_ms)}
        end
      end)
    end
  end

  defp apply_jitter(delay, %{jitter_pct: 0}, _id, _attempt, _now_ms), do: delay
  defp apply_jitter(0, _backoff, _id, _attempt, _now_ms), do: 0

  defp apply_jitter(delay, %{jitter_pct: jitter_pct}, id, attempt, now_ms) do
    spread = div(delay * jitter_pct, 100)

    if spread <= 0 do
      delay
    else
      offset = :erlang.phash2({id, attempt, now_ms}, spread * 2 + 1) - spread
      max(delay + offset, 0)
    end
  end

  defp fetch_policy(policy, atom_key, string_key, default) do
    cond do
      is_map(policy) and Map.has_key?(policy, atom_key) -> Map.fetch!(policy, atom_key)
      is_map(policy) and Map.has_key?(policy, string_key) -> Map.fetch!(policy, string_key)
      true -> default
    end
  end
end
