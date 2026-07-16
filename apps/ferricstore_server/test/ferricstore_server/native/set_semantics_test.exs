defmodule FerricstoreServer.Native.SetSemanticsTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.Native.Commands

  @op_set 0x0102

  setup do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
    :ok = CatalogProjector.mark_ready()
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "typed SET KEEPTTL preserves the committed expiry" do
    ctx = FerricStore.Instance.get(:default)
    key = unique_key("keepttl")
    expire_at_ms = System.os_time(:millisecond) + 60_000

    assert :ok = Router.put(ctx, key, "old", expire_at_ms)

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_set,
               %{"key" => key, "value" => "new", "keepttl" => true},
               state(ctx)
             )

    assert {"new", ^expire_at_ms} = Router.get_meta(ctx, key)
  end

  test "typed SET rejects conflicting and malformed options before submitting a write" do
    ctx = FerricStore.Instance.get(:default)
    key = unique_key("invalid-options")

    for payload <- [
          %{"key" => key, "value" => "value", "nx" => true, "xx" => true},
          %{"key" => key, "value" => "value", "ttl" => 10, "keepttl" => true},
          %{"key" => key, "value" => "value", "nx" => "true"},
          %{"key" => key, "value" => "value", "ttl" => -1}
        ] do
      assert {:bad_request, message, _state} = Commands.execute(@op_set, payload, state(ctx))
      assert message =~ "ERR"
      assert Router.get(ctx, key) == nil
    end
  end

  test "concurrent typed SET NX commits exactly one value" do
    ctx = FerricStore.Instance.get(:default)
    key = unique_key("nx")

    results =
      1..32
      |> Task.async_stream(
        fn value ->
          Commands.execute(
            @op_set,
            %{"key" => key, "value" => Integer.to_string(value), "nx" => true},
            state(ctx)
          )
        end,
        max_concurrency: 32,
        timeout: 10_000,
        ordered: false
      )
      |> Enum.map(fn {:ok, {status, result, _state}} -> {status, result} end)

    assert Enum.count(results, &(&1 == {:ok, true})) == 1
    assert Enum.count(results, &(&1 == {:ok, false})) == 31
    assert Router.get(ctx, key) in Enum.map(1..32, &Integer.to_string/1)
  end

  defp state(ctx) do
    %{
      client_id: System.unique_integer([:positive, :monotonic]),
      client_name: nil,
      username: "default",
      authenticated: true,
      require_auth: false,
      acl_cache: :full_access,
      peer: {{127, 0, 0, 1}, 12_345},
      created_at: System.monotonic_time(:millisecond),
      instance_ctx: ctx,
      compression: :none,
      compact_flow_responses: false,
      subscribed_events: MapSet.new(),
      flow_wake_subscriptions: MapSet.new()
    }
  end

  defp unique_key(suffix) do
    "native-set-semantics:#{suffix}:#{System.unique_integer([:positive, :monotonic])}"
  end
end
