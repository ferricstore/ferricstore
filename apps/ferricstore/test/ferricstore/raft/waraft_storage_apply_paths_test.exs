defmodule Ferricstore.Raft.WARaftStorageApplyPathsTest do
  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.Store.Router

  @path_builders [
    {Path, :join, 1},
    {Path, :join, 2},
    {:filename, :join, 1},
    {:filename, :join, 2},
    {:filename, :basename, 1},
    {:filename, :dirname, 1},
    {:filename, :absname, 1}
  ]

  # A shard's storage process applies every write, one at a time. The roots of
  # its log and storage never change, so rebuilding them per write is pure
  # cost on the write path.
  test "applying writes on a shard builds no paths" do
    ctx = FerricStore.Instance.get(:default)
    key = "apply-paths:#{System.unique_integer([:positive])}"
    assert :ok = Router.put(ctx, key, "warm", 0)

    storage =
      Process.whereis(
        :wa_raft_storage.registered_name(
          :ferricstore_waraft_backend,
          Router.shard_for(ctx, key) + 1
        )
      )

    for mfa <- @path_builders, do: :erlang.trace_pattern(mfa, true, [:local])
    :erlang.trace(storage, true, [:call])

    on_exit(fn ->
      if Process.alive?(storage), do: :erlang.trace(storage, false, [:call])
      for mfa <- @path_builders, do: :erlang.trace_pattern(mfa, false, [:local])
    end)

    for index <- 1..10 do
      assert :ok = Router.put(ctx, key, "value-#{index}", 0)
    end

    :erlang.trace(storage, false, [:call])

    calls =
      Stream.repeatedly(fn ->
        receive do
          {:trace, ^storage, :call, {module, function, args}} ->
            {module, function, length(args)}
        after
          100 -> :done
        end
      end)
      |> Enum.take_while(&(&1 != :done))

    assert calls == [], "10 applies built paths: #{inspect(Enum.frequencies(calls))}"
  end
end
