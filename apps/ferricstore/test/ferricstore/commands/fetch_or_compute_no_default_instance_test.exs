defmodule Ferricstore.Commands.FetchOrComputeNoDefaultInstanceTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias Ferricstore.FetchOrCompute

  @default_key {FerricStore.Instance, :default}

  setup do
    original = :persistent_term.get(@default_key, :missing)
    :persistent_term.erase(@default_key)

    on_exit(fn ->
      case original do
        :missing -> :persistent_term.erase(@default_key)
        ctx -> :persistent_term.put(@default_key, ctx)
      end
    end)

    unless Process.whereis(FetchOrCompute) do
      start_supervised!({FetchOrCompute, compute_timeout_ms: 100})
    end

    :ok
  end

  test "fetch_or_compute returns an error and keeps coordinator alive before init" do
    assert {:error, "instance not initialized"} =
             FetchOrCompute.fetch_or_compute("foc:init", 1_000, "hint")

    assert Process.alive?(Process.whereis(FetchOrCompute))
  end

  test "result and error paths return bounded errors before init" do
    assert {:error, "instance not initialized"} =
             FetchOrCompute.fetch_or_compute_result("foc:init", "value", "token", 1_000)

    assert {:error, "instance not initialized"} =
             FetchOrCompute.fetch_or_compute_error("foc:init", "token", "failed")
  end
end
