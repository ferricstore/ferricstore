defmodule Ferricstore.FetchOrComputeFailureTest do
  use ExUnit.Case, async: false

  alias Ferricstore.FetchOrCompute
  alias Ferricstore.FetchOrCompute.Worker

  test "propagates cache storage failures instead of returning them as hits" do
    name = :"fetch_failure_#{System.unique_integer([:positive])}"
    data_dir = Path.join(System.tmp_dir!(), Atom.to_string(name))
    ctx = FerricStore.Instance.build(name, data_dir: data_dir, shard_count: 1)

    on_exit(fn ->
      FerricStore.Instance.cleanup(name)
      File.rm_rf!(data_dir)
    end)

    assert {:error, _reason} = FetchOrCompute.fetch_or_compute(ctx, "missing", 1_000, "hint")
  end

  test "invalid compute timeout falls back to the bounded default" do
    pid = start_supervised!({Worker, compute_timeout_ms: -1})
    assert :sys.get_state(pid).compute_timeout_ms == FetchOrCompute.default_compute_timeout_ms()
  end
end
