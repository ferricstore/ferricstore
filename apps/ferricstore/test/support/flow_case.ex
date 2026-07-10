defmodule Ferricstore.Test.FlowCase do
  @moduledoc """
  Shared setup for FerricFlow behavior and invariant tests.
  """

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: false

      @moduletag :flow

      alias Ferricstore.Test.ShardHelpers
      import Ferricstore.Test.Eventually

      setup_all do
        ShardHelpers.wait_shards_alive()
        :ok
      end

      setup do
        ctx = FerricStore.Instance.get(:default)

        case Ferricstore.Flow.Governance.LimitCache.clear(ctx) do
          {:ok, %{errors: 0}} -> :ok
          {:error, reason} -> raise "failed to flush governance limit cache: #{inspect(reason)}"
        end

        ShardHelpers.flush_all_keys()
        ShardHelpers.reset_memory_guard_pressure()
        :ok
      end

      defp unique_flow_id(prefix) do
        "#{prefix}:#{System.unique_integer([:positive])}"
      end
    end
  end
end
