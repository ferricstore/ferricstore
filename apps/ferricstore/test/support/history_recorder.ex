defmodule Ferricstore.Test.HistoryRecorder do
  @moduledoc """
  Records acknowledged writes during a fault injection test.

  After recovery, verifies every ACKed write is durable.

  A write is recorded as `{:ok, key, value, node, timestamp}` when FerricStore
  returns `:ok`. Any write that returned an error is discarded --
  FerricStore is not obligated to preserve unacknowledged writes.
  Only acknowledged writes must survive.

  ## Usage

      {:ok, recorder} = HistoryRecorder.new()
      HistoryRecorder.record_ok(recorder, "mykey", "myval", node_name)
      {:ok, 1} = HistoryRecorder.verify_durability(recorder, nodes)
      HistoryRecorder.stop(recorder)

  ## Design

  Uses an `Agent` to accumulate a write history list. Each entry is a tuple
  `{:ok, key, value, node_name, monotonic_time}` where `monotonic_time` is
  captured via `System.monotonic_time/0` for ordering purposes.
  """

  # ---------------------------------------------------------------------------
  # Lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Starts a new HistoryRecorder agent.

  Returns `{:ok, pid}` on success.
  """
  @spec new() :: {:ok, pid()}
  def new do
    Agent.start_link(fn -> [] end)
  end

  @doc """
  Stops the recorder agent.
  """
  @spec stop(pid()) :: :ok
  def stop(agent) do
    Agent.stop(agent)
  end

  # ---------------------------------------------------------------------------
  # Recording
  # ---------------------------------------------------------------------------

  @doc """
  Records a successful (ACKed) write.

  Only call this when the write operation returned `:ok` or `{:ok, _}`.
  The entry includes a monotonic timestamp for ordering.

  ## Parameters

    - `agent` -- the recorder agent pid
    - `key` -- the key that was written
    - `value` -- the value that was written
    - `node` -- the node name (atom) the write was sent to
  """
  @spec record_ok(pid(), binary(), binary(), atom()) :: :ok
  def record_ok(agent, key, value, node) do
    Agent.update(agent, fn history ->
      [{:ok, key, value, node, System.monotonic_time()} | history]
    end)
  end

  @doc """
  Returns all recorded entries in reverse chronological order (newest first).
  """
  @spec all(pid()) :: [{:ok, binary(), binary(), atom(), integer()}]
  def all(agent) do
    Agent.get(agent, & &1)
  end

  @doc """
  Classifies one-shot counter write results for an exact final-value oracle.

  A lost RPC reply or an explicit unknown-outcome timeout may have committed,
  so those writes contribute to the upper bound rather than the failure count.
  """
  @spec counter_outcome_counts([term()]) :: %{
          acknowledged: non_neg_integer(),
          unknown: non_neg_integer(),
          failed: non_neg_integer()
        }
  def counter_outcome_counts(results) when is_list(results) do
    Enum.reduce(
      results,
      %{acknowledged: 0, unknown: 0, failed: 0},
      fn result, counts ->
        Map.update!(counts, counter_outcome(result), &(&1 + 1))
      end
    )
  end

  defp counter_outcome({:ok, _value}), do: :acknowledged
  defp counter_outcome({:error, {:timeout, :unknown_outcome}}), do: :unknown
  defp counter_outcome({:badrpc, _reason}), do: :unknown
  defp counter_outcome(_result), do: :failed

  # ---------------------------------------------------------------------------
  # Verification
  # ---------------------------------------------------------------------------

  @doc """
  After recovery, verifies every ACKed write is present on the specified nodes.

  For each recorded write, reads the key from each node via `:rpc.call` and
  checks that the value matches. Returns `{:ok, checked_count}` if all writes
  are durable, or `{:error, violations}` if any are missing or wrong.

  ## Parameters

    - `agent` -- the recorder agent pid
    - `nodes` -- list of node maps (`%{name: atom()}`) to check

  ## Returns

    - `{:ok, count}` -- all `count` ACKed writes verified durable
    - `{:error, violations}` -- list of violation tuples
  """
  @spec verify_durability(pid(), [map()]) :: {:ok, non_neg_integer()} | {:error, list()}
  def verify_durability(agent, nodes) do
    history = Agent.get(agent, & &1)

    violations =
      Enum.flat_map(history, fn {:ok, key, value, _write_node, _ts} ->
        Enum.flat_map(nodes, fn node ->
          case :rpc.call(node.name, FerricStore, :get, [key]) do
            {:ok, ^value} ->
              []

            {:ok, nil} ->
              [{:lost_write, key, value: value, node: node.name}]

            {:ok, other} when is_binary(other) ->
              [{:wrong_value, key, expected: value, got: other, node: node.name}]

            {:badrpc, reason} ->
              [{:read_error, key, error: {:badrpc, reason}, node: node.name}]

            error ->
              [{:read_error, key, error: error, node: node.name}]
          end
        end)
      end)

    if violations == [] do
      {:ok, length(history)}
    else
      {:error, violations}
    end
  end

  @doc """
  Verifies a monotonically increasing counter was never decremented.

  Checks that the final counter value on each node is at least as large as
  the maximum value that was ever ACKed. This catches counter regressions
  caused by lost increments or state machine rollback.

  ## Parameters

    - `agent` -- the recorder agent pid
    - `nodes` -- list of node maps to check
    - `key` -- the counter key

  ## Returns

  A list of violation tuples (empty list means no violations).
  """
  @spec verify_counter_monotonic(pid(), [map()], binary()) :: list()
  def verify_counter_monotonic(agent, nodes, key) do
    history = Agent.get(agent, & &1)

    ok_values =
      history
      |> Enum.filter(fn {:ok, k, _v, _n, _t} -> k == key end)
      |> Enum.map(fn {:ok, _k, v, _n, _t} -> v end)

    max_acked =
      ok_values
      |> Enum.map(&String.to_integer/1)
      |> Enum.max(fn -> 0 end)

    Enum.flat_map(nodes, fn node ->
      case :rpc.call(node.name, FerricStore, :get, [key]) do
        {:ok, final} when is_binary(final) ->
          final_int = String.to_integer(final)

          if final_int >= max_acked do
            []
          else
            [{:counter_regression, key, expected_min: max_acked, got: final_int, node: node.name}]
          end

        {:ok, nil} ->
          [{:counter_missing, key, expected_min: max_acked, got: nil, node: node.name}]

        {:badrpc, reason} ->
          [{:read_error, key, error: {:badrpc, reason}, node: node.name}]

        other ->
          [{:read_error, key, error: other, node: node.name}]
      end
    end)
  end

  @doc """
  Verifies a set: no member that was ACKed-added is missing.

  For each node, reads `SMEMBERS` via `FerricStore.smembers/1` and checks that
  every ACKed member is present. Returns a list of violation tuples.

  ## Parameters

    - `agent` -- the recorder agent pid
    - `nodes` -- list of node maps to check
    - `key` -- the set key

  ## Returns

  A list of violation tuples (empty list means no violations).
  """
  @spec verify_set_durability(pid(), [map()], binary()) :: list()
  def verify_set_durability(agent, nodes, key) do
    history = Agent.get(agent, & &1)

    acked_members =
      history
      |> Enum.filter(fn {:ok, k, _v, _n, _t} -> k == key end)
      |> Enum.map(fn {:ok, _k, v, _n, _t} -> v end)
      |> MapSet.new()

    Enum.flat_map(nodes, fn node ->
      case :rpc.call(node.name, FerricStore, :smembers, [key]) do
        {:ok, members} ->
          present = MapSet.new(members)
          missing = MapSet.difference(acked_members, present)

          if MapSet.size(missing) == 0 do
            []
          else
            [{:missing_members, key, missing: MapSet.to_list(missing), node: node.name}]
          end

        {:badrpc, reason} ->
          [{:read_error, key, error: {:badrpc, reason}, node: node.name}]

        error ->
          [{:read_error, key, error: error, node: node.name}]
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Formatting
  # ---------------------------------------------------------------------------

  @doc """
  Formats a list of violations into a human-readable string for assertion messages.
  """
  @spec format_violations(list()) :: binary()
  def format_violations(violations) do
    Enum.map_join(violations, "\n", fn v -> "  #{inspect(v)}" end)
  end
end
