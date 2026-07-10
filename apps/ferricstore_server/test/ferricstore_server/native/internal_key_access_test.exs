defmodule FerricstoreServer.Native.InternalKeyAccessTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias FerricstoreServer.Native.{Commands, Session}

  @error "ERR access to internal Flow keys is not allowed"
  @op_pipeline 0x000E
  @op_get 0x0101
  @op_set 0x0102
  @op_mset 0x0105
  @op_hset 0x0110
  @op_flow_create 0x0201

  setup do
    ctx = FerricStore.Instance.get(:default)
    digest = Base.url_encode64(:crypto.hash(:sha256, inspect(make_ref())), padding: false)
    reserved = "f:{f:#{digest}}:s:native-probe"
    ordinary = "native-probe:#{System.unique_integer([:positive, :monotonic])}"

    on_exit(fn ->
      Router.delete(ctx, reserved)
      Router.delete(ctx, ordinary)
    end)

    {:ok, ctx: ctx, reserved: reserved, ordinary: ordinary}
  end

  test "typed reads and writes are denied on the full-access fast path", context do
    state = state(context.ctx)

    assert {:error, @error, _state} =
             Commands.execute(@op_get, %{"key" => context.reserved}, state)

    assert {:error, @error, _state} =
             Commands.execute(
               @op_set,
               %{"key" => context.reserved, "value" => "forged"},
               state
             )

    assert nil == Router.get(context.ctx, context.reserved)
  end

  test "typed multi-key and data-structure writes reject before mutation", context do
    state = state(context.ctx)

    assert {:error, @error, _state} =
             Commands.execute(
               @op_mset,
               %{"pairs" => [[context.ordinary, "value"], [context.reserved, "forged"]]},
               state
             )

    assert {:error, @error, _state} =
             Commands.execute(
               @op_hset,
               %{"key" => context.reserved, "fields" => %{"field" => "forged"}},
               state
             )

    assert nil == Router.get(context.ctx, context.ordinary)
    assert nil == Router.get(context.ctx, context.reserved)
  end

  test "compact batch rejects the whole request before its fast path", context do
    payload = %{
      "compact_pipeline" => {1, [{context.ordinary, "value"}, {context.reserved, "forged"}]}
    }

    assert {:bad_request, @error, _state} =
             Commands.execute(@op_pipeline, payload, state(context.ctx))

    assert nil == Router.get(context.ctx, context.ordinary)
    assert nil == Router.get(context.ctx, context.reserved)
  end

  test "MULTI rejects a reserved command before queueing", context do
    assert {:ok, "OK", multi_state} =
             Session.execute(%{"command" => "MULTI", "args" => []}, session_state(context.ctx))

    assert {:error, @error, rejected_state} =
             Session.execute(
               %{"command" => "SET", "args" => [context.reserved, "forged"]},
               multi_state
             )

    assert rejected_state.multi_queue == []
    assert rejected_state.multi_queue_count == 0
  end

  test "dedicated Flow opcodes remain on the trusted path", context do
    id = context.reserved

    assert {:ok, "OK", _state} =
             Commands.execute(
               @op_flow_create,
               %{"id" => id, "type" => "security-probe", "state" => "queued"},
               state(context.ctx)
             )
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
      stats_counter: ctx.stats_counter,
      compression: :none,
      compact_flow_responses: false,
      subscribed_events: MapSet.new(),
      flow_wake_subscriptions: MapSet.new()
    }
  end

  defp session_state(ctx) do
    Map.merge(state(ctx), %{
      multi_state: :none,
      multi_queue: [],
      multi_queue_count: 0,
      multi_error: false,
      watched_keys: %{},
      pubsub_channels: nil,
      pubsub_patterns: nil
    })
  end
end
