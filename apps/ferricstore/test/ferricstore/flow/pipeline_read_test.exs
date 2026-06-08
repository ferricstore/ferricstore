defmodule Ferricstore.Flow.PipelineReadTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.PipelineRead

  test "batch preserves order across history, other, and errors" do
    callbacks = %{
      start: fn -> :started end,
      command: fn
        _ctx, :history ->
          {:history, "flow-1", "tenant", "history-key", %{count: 10}, false, false,
           %{enabled?: false}}

        _ctx, :other ->
          {:other, fn -> {:ok, :other} end}

        _ctx, :bad ->
          {:error, "ERR bad"}
      end,
      decode_get: fn nil -> {:ok, nil} end,
      history_results: fn [
                            {0, "flow-1", "tenant", "history-key", %{count: 10}, false, false,
                             %{enabled?: false}}
                          ],
                          :ctx ->
        %{0 => {:ok, :history}}
      end,
      observe: fn :started, [:history, :bad, :other] -> :ok end
    }

    assert PipelineRead.batch(:ctx, [:history, :bad, :other], callbacks) == [
             {:ok, :history},
             {:error, "ERR bad"},
             {:ok, :other}
           ]
  end

  test "hydrate_get_results passes through non-record results and records without payload" do
    decoded = [
      {0, {:ok, %{id: "flow-1"}}, %{enabled?: false, max_bytes: 10}},
      {1, {:error, "ERR"}, %{enabled?: false, max_bytes: 10}},
      {2, {:ok, nil}, %{enabled?: false, max_bytes: 10}}
    ]

    assert PipelineRead.hydrate_get_results(decoded, :ctx) == [
             {2, {:ok, nil}},
             {1, {:error, "ERR"}},
             {0, {:ok, %{id: "flow-1"}}}
           ]
  end
end
