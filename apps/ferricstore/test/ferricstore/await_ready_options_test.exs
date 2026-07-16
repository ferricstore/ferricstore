defmodule Ferricstore.AwaitReadyOptionsTest do
  use ExUnit.Case, async: true

  test "await_ready rejects invalid polling limits before checking health" do
    for opts <- [
          [timeout: -1],
          [timeout: "1000"],
          [interval: 0],
          [interval: -1],
          [interval: 1.5],
          [unknown: true]
        ] do
      assert_raise ArgumentError, ~r/await_ready/, fn -> FerricStore.await_ready(opts) end
    end
  end
end
