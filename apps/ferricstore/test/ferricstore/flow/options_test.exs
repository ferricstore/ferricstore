defmodule Ferricstore.Flow.OptionsTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.Options

  test "binary options validate required and optional strings" do
    assert Options.required_binary([worker: "w"], :worker) == {:ok, "w"}
    assert Options.required_binary([], :worker) == {:error, "ERR flow worker is required"}

    assert Options.required_binary([worker: ""], :worker) ==
             {:error, "ERR flow worker must be a non-empty string"}

    assert Options.optional_binary([], :state, "queued") == {:ok, "queued"}

    assert Options.optional_binary([state: ""], :state, "queued") ==
             {:error, "ERR flow state must be a non-empty string"}

    assert Options.optional_binary_or_nil([], :parent, nil) == {:ok, nil}
    assert Options.optional_binary_or_nil([parent: "p"], :parent, nil) == {:ok, "p"}

    assert Options.optional_binary_or_nil([parent: 1], :parent, nil) ==
             {:error, "ERR flow parent must be a string"}
  end

  test "integer and boolean options preserve Flow error messages" do
    assert Options.required_non_neg_integer([now_ms: 0], :now_ms) == {:ok, 0}

    assert Options.required_non_neg_integer([], :now_ms) ==
             {:error, "ERR flow now_ms is required"}

    assert Options.optional_non_neg_integer([], :run_at_ms, nil) == {:ok, nil}
    assert Options.optional_non_neg_integer([], :run_at_ms, 10) == {:ok, 10}

    assert Options.optional_non_neg_integer([run_at_ms: -1], :run_at_ms, nil) ==
             {:error, "ERR flow run_at_ms must be a non-negative integer"}

    assert Options.optional_boolean([], :payload, false) == {:ok, false}
    assert Options.optional_boolean([payload: true], :payload, false) == {:ok, true}

    assert Options.optional_boolean([payload: "true"], :payload, false) ==
             {:error, "ERR flow payload must be a boolean"}
  end

  test "ref size and put helpers preserve existing semantics" do
    assert Options.validate_ref_size(:payload_ref, nil) == :ok
    assert Options.validate_ref_size(:payload_ref, String.duplicate("x", 4_096)) == :ok

    assert Options.validate_ref_size(:payload_ref, String.duplicate("x", 4_097)) ==
             {:error, "ERR flow payload_ref too large (max 4096 bytes)"}

    assert Options.maybe_put_keyword([a: 1], :b, nil) == [a: 1]
    assert Options.maybe_put_keyword([a: 1], :b, 2) == [b: 2, a: 1]
    assert Options.maybe_put_attr(%{a: 1}, :b, nil) == %{a: 1}
    assert Options.maybe_put_attr(%{a: 1}, :b, 2) == %{a: 1, b: 2}
    assert Options.maybe_put_default_attr(%{}, :priority, 0, 0) == %{}
    assert Options.maybe_put_default_attr(%{}, :priority, 1, 0) == %{priority: 1}
  end
end
