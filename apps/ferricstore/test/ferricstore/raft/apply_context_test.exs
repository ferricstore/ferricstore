defmodule Ferricstore.Raft.ApplyContextTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Hibernation, RetryPolicy}
  alias Ferricstore.Raft.{ApplyContext, StateMachine}

  test "history hot limits cannot exceed the total history maximum" do
    context =
      ApplyContext.new(
        flow_default_history_hot_max_events: 10,
        flow_default_history_max_events: 100,
        flow_max_history_hot_max_events: 50,
        flow_max_history_max_events: 5
      )

    assert context.flow_max_history_hot_max_events == 5
    assert context.flow_default_history_hot_max_events == 5
    assert context.flow_default_history_max_events == 5
  end

  @runtime_keys [
    :flow_default_retention_ttl_ms,
    :flow_default_history_hot_max_events,
    :flow_default_history_max_events,
    :flow_max_history_hot_max_events,
    :flow_max_history_max_events,
    :flow_retention_cleanup_key_budget,
    :flow_retention_cleanup_byte_budget,
    :flow_lmdb_history_cleanup_scan_limit,
    :flow_lmdb_value_cleanup_scan_limit,
    :flow_hibernation_enabled
  ]

  setup do
    original = Map.new(@runtime_keys, &{&1, Application.get_env(:ferricstore, &1)})

    on_exit(fn ->
      Enum.each(original, fn
        {key, nil} -> Application.delete_env(:ferricstore, key)
        {key, value} -> Application.put_env(:ferricstore, key, value)
      end)

      Hibernation.refresh_config!()
    end)

    :ok
  end

  test "captured retention limits do not change while a command is applying" do
    Application.put_env(:ferricstore, :flow_default_retention_ttl_ms, 111_000)
    Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 7)
    Application.put_env(:ferricstore, :flow_default_history_max_events, 31)
    Application.put_env(:ferricstore, :flow_max_history_hot_max_events, 20)
    Application.put_env(:ferricstore, :flow_max_history_max_events, 100)

    context = ApplyContext.from_runtime()

    Application.put_env(:ferricstore, :flow_default_retention_ttl_ms, 999_000)
    Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 70)
    Application.put_env(:ferricstore, :flow_default_history_max_events, 90)

    assert RetryPolicy.resolve_retention(nil, "queued", nil, context) == %{
             ttl_ms: 111_000,
             history_hot_max_events: 7,
             history_max_events: 31
           }
  end

  test "standalone shards carry their startup apply context into Flow commands" do
    Application.put_env(:ferricstore, :flow_default_retention_ttl_ms, 111_000)
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
    shard = elem(ctx.shard_names, 0)
    captured = :sys.get_state(shard).apply_context

    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    assert captured.flow_default_retention_ttl_ms == 111_000
    Application.put_env(:ferricstore, :flow_default_retention_ttl_ms, 999_000)

    id = "standalone-apply-context-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             Ferricstore.Store.Router.flow_create(ctx, %{
               id: id,
               type: "standalone-apply-context",
               state: "queued",
               partition_key: "standalone-apply-context",
               run_at_ms: 1_000,
               now_ms: 1_000
             })

    assert :sys.get_state(shard).apply_context == captured
  end

  test "captured cleanup and hibernation limits are flat immutable data" do
    context =
      ApplyContext.new(
        flow_retention_cleanup_key_budget: 64,
        flow_retention_cleanup_byte_budget: 65_536,
        flow_lmdb_history_cleanup_scan_limit: 512,
        flow_lmdb_value_cleanup_scan_limit: 256,
        flow_hibernation_enabled: false
      )

    assert context.flow_retention_cleanup_key_budget == 64
    assert context.flow_retention_cleanup_byte_budget == 65_536
    assert context.flow_lmdb_history_cleanup_scan_limit == 512
    assert context.flow_lmdb_value_cleanup_scan_limit == 256
    refute Hibernation.enabled?(context)
    assert context == context |> :erlang.term_to_binary() |> :erlang.binary_to_term([:safe])
  end

  test "invalid runtime values normalize once before replication" do
    context =
      ApplyContext.new(
        flow_default_retention_ttl_ms: -1,
        flow_default_history_hot_max_events: 50,
        flow_default_history_max_events: 10,
        flow_max_history_hot_max_events: 20,
        flow_max_history_max_events: 40,
        flow_retention_cleanup_key_budget: 0,
        flow_retention_cleanup_byte_budget: "bad"
      )

    assert context.flow_default_retention_ttl_ms == 604_800_000
    assert context.flow_default_history_hot_max_events == 20
    assert context.flow_default_history_max_events == 20
    assert context.flow_retention_cleanup_key_budget == 1_024
    assert context.flow_retention_cleanup_byte_budget == 8 * 1_024 * 1_024

    floored =
      ApplyContext.new(
        flow_retention_cleanup_key_budget: 1,
        flow_retention_cleanup_byte_budget: 1
      )

    assert floored.flow_retention_cleanup_key_budget == 8
    assert floored.flow_retention_cleanup_byte_budget == 4_096
  end

  test "replicated history limits retain the existing hot-history safety cap" do
    context = ApplyContext.new(flow_max_history_hot_max_events: 1_000_000)

    assert context.flow_max_history_hot_max_events == 10_000

    oversized =
      context
      |> ApplyContext.encode()
      |> put_elem(4, 1_000_000)

    assert {:error, :invalid_apply_context} = ApplyContext.decode(oversized)
  end

  test "replicated work limits clamp runtime values and reject oversized encodings" do
    oversized_value = :erlang.bsl(1, 256)

    context =
      ApplyContext.new(
        flow_retention_cleanup_key_budget: oversized_value,
        flow_retention_cleanup_byte_budget: oversized_value,
        flow_lmdb_history_cleanup_scan_limit: oversized_value,
        flow_lmdb_value_cleanup_scan_limit: oversized_value,
        flow_hibernation_hot_window_ms: oversized_value,
        flow_hibernation_safety_margin_ms: oversized_value,
        flow_hibernation_promote_window_ms: oversized_value,
        flow_hibernation_late_promote_window_ms: oversized_value
      )

    assert context.flow_retention_cleanup_key_budget == 100_000
    assert context.flow_retention_cleanup_byte_budget == 64 * 1_024 * 1_024
    assert context.flow_lmdb_history_cleanup_scan_limit == 1_000_000
    assert context.flow_lmdb_value_cleanup_scan_limit == 1_000_000
    assert context.flow_hibernation_hot_window_ms == 31_536_000_000
    assert context.flow_hibernation_safety_margin_ms == 31_536_000_000
    assert context.flow_hibernation_promote_window_ms == 31_536_000_000
    assert context.flow_hibernation_late_promote_window_ms == 31_536_000_000

    encoded = ApplyContext.encode(context)
    assert :erlang.external_size(encoded) <= 160

    for index <- [6, 7, 8, 9, 11, 12, 13, 14] do
      assert {:error, :invalid_apply_context} =
               encoded
               |> put_elem(index, oversized_value)
               |> ApplyContext.decode()
    end
  end

  test "only policy-sensitive commands carry the compact replicated context" do
    context = ApplyContext.new(flow_default_history_max_events: 17)
    flow_command = {:flow_create, "state-key", %{id: "id", type: "email"}}

    assert {:ferricstore_apply_context, encoded, ^flow_command} =
             ApplyContext.wrap_command(flow_command, context)

    assert {:ok, ^context} = ApplyContext.decode(encoded)
    assert :erlang.external_size(encoded) <= 160

    kv_command = {:put, "key", "value", 0}
    assert ApplyContext.wrap_command(kv_command, context) == kv_command

    generic_tx = {:cross_shard_tx, [{0, [{0, {:put, "key", "value", 0}}], nil}]}
    assert ApplyContext.wrap_command(generic_tx, context) == generic_tx

    flow_tx =
      {:cross_shard_tx,
       [{0, [{0, {:flow_create, "state-key", %{id: "id", type: "email"}}}], nil}]}

    assert {:ferricstore_apply_context, _encoded, ^flow_tx} =
             ApplyContext.wrap_command(flow_tx, context)
  end

  test "all policy-sensitive apply tags and shared-ref wrappers carry context" do
    context = ApplyContext.default()
    inner = {:flow_create, "state-key", %{id: "id", type: "email"}}

    direct_tags = [
      :flow_cancel,
      :flow_cancel_many,
      :flow_claim_due,
      :flow_complete,
      :flow_complete_many,
      :flow_cross_policy_put,
      :flow_cross_retention_cleanup,
      :flow_cross_spawn_children,
      :flow_cross_terminal,
      :flow_cross_terminal_many,
      :flow_create,
      :flow_create_many,
      :flow_create_pipeline_batch,
      :flow_extend_lease,
      :flow_fail,
      :flow_fail_many,
      :flow_named_value_put,
      :flow_named_value_put_pipeline_batch,
      :flow_policy_attribute_catalog_repair,
      :flow_policy_attribute_catalog_repair_request,
      :flow_policy_catalog_backfill_step,
      :flow_policy_migration_step,
      :flow_policy_put,
      :flow_reschedule,
      :flow_retention_cleanup,
      :flow_retry,
      :flow_retry_many,
      :flow_rewind,
      :flow_run_steps_many,
      :flow_schedule_replace,
      :flow_signal,
      :flow_signal_many,
      :flow_spawn_children,
      :flow_start_and_claim,
      :flow_start_and_claim_pipeline_batch,
      :flow_step_continue,
      :flow_step_continue_many,
      :flow_terminal_pipeline_batch,
      :flow_transition,
      :flow_transition_many
    ]

    commands =
      [
        {:batch, [inner]},
        {:async, self(), inner},
        {:ferricstore_latency_trace, inner},
        {:flow_shared_ref_write, 0, inner},
        {:cross_shard_tx,
         [
           {0,
            [
              {0, {:flow_shared_ref_write, 0, inner}}
            ], nil}
         ]}
      ] ++ Enum.map(direct_tags, &{&1, :payload})

    Enum.each(commands, fn command ->
      assert {:ferricstore_apply_context, encoded, ^command} =
               ApplyContext.wrap_command(command, context)

      assert {:ok, ^context} = ApplyContext.decode(encoded)
    end)
  end

  test "two-field async envelopes are not current context carriers" do
    context = ApplyContext.new(flow_default_history_max_events: 23)
    inner = {:flow_create, "state-key", %{id: "id", type: "email"}}

    assert {:async, ^inner} = ApplyContext.wrap_command({:async, inner}, context)
  end

  test "reserved wrappers cannot inject context or pessimize ordinary KV commands" do
    trusted = ApplyContext.new(flow_default_history_max_events: 23)
    injected = ApplyContext.new(flow_default_history_max_events: 99)
    flow_command = {:flow_create, "state-key", %{id: "id", type: "email"}}

    assert {:ferricstore_apply_context, encoded, ^flow_command} =
             ApplyContext.wrap_command(
               {:ferricstore_apply_context, ApplyContext.encode(injected), flow_command},
               trusted
             )

    assert {:ok, ^trusted} = ApplyContext.decode(encoded)

    kv_command = {:put, "key", "value", 0}
    wrapped_kv = ApplyContext.wrap_command(kv_command, trusted)
    assert :erts_debug.same(kv_command, wrapped_kv)

    assert ApplyContext.wrap_command(
             {:ferricstore_apply_context, ApplyContext.encode(injected), kv_command},
             trusted
           ) == kv_command

    nested_kv =
      {:ferricstore_latency_trace,
       {:ferricstore_apply_context, ApplyContext.encode(injected), {:get_intents}}}

    assert ApplyContext.wrap_command(nested_kv, trusted) ==
             {:ferricstore_latency_trace, {:get_intents}}

    nested_flow =
      {:ferricstore_latency_trace,
       {:ferricstore_apply_context, ApplyContext.encode(injected), flow_command}}

    assert {:ferricstore_apply_context, nested_encoded,
            {:ferricstore_latency_trace, ^flow_command}} =
             ApplyContext.wrap_command(nested_flow, trusted)

    assert {:ok, ^trusted} = ApplyContext.decode(nested_encoded)
  end

  test "ordinary command dispatch does not eagerly encode the apply context" do
    source =
      "../../../lib/ferricstore/raft/apply_context.ex"
      |> Path.expand(__DIR__)
      |> File.read!()

    [wrap_source] =
      Regex.run(
        ~r/^  def wrap_command\(.*?(?=^  defp )/ms,
        source,
        capture: :first
      )

    assert wrap_source =~ "context_command_shape?(command)"
    refute wrap_source =~ "encode(context)"
  end

  test "pre-stamped commands cannot bypass or inject replicated context" do
    trusted = ApplyContext.new(flow_default_history_max_events: 23)
    injected = ApplyContext.new(flow_default_history_max_events: 99)
    flow_command = {:flow_create, "state-key", %{id: "id", type: "email"}}
    stamp = %{hlc_ts: {1_234, 0}}

    assert {{:ferricstore_apply_context, encoded, ^flow_command}, ^stamp} =
             ApplyContext.wrap_command({flow_command, stamp}, trusted)

    assert {:ok, ^trusted} = ApplyContext.decode(encoded)

    spoofed =
      {{:ferricstore_apply_context, ApplyContext.encode(injected), flow_command}, stamp}

    assert {{:ferricstore_apply_context, resigned, ^flow_command}, ^stamp} =
             ApplyContext.wrap_command(spoofed, trusted)

    assert {:ok, ^trusted} = ApplyContext.decode(resigned)

    preencoded = {:ttb, :erlang.term_to_binary(spoofed)}

    assert {{:ferricstore_apply_context, preencoded_context, ^flow_command}, ^stamp} =
             ApplyContext.wrap_command(preencoded, trusted)

    assert {:ok, ^trusted} = ApplyContext.decode(preencoded_context)

    assert {:ferricstore_apply_context, invalid_preencoded_context, :invalid_preencoded_command} =
             ApplyContext.wrap_command({:ttb, <<0, 1, 2>>}, trusted)

    assert {:ok, ^trusted} = ApplyContext.decode(invalid_preencoded_context)
  end

  test "replicated command context overrides a replica's node-local context" do
    local = ApplyContext.new(flow_default_history_max_events: 10)
    leader = ApplyContext.new(flow_default_history_max_events: 20)

    state = %{
      applied_count: 0,
      release_cursor_interval: 1_000,
      apply_context: local,
      apply_context_encoded: ApplyContext.encode(local),
      cross_shard_intents: %{}
    }

    command =
      {:ferricstore_apply_context, ApplyContext.encode(leader), {:get_intents}}

    assert {next_state, %{}} =
             StateMachine.apply(%{}, command, state)

    assert next_state.apply_context == leader
    assert next_state.apply_context_encoded == ApplyContext.encode(leader)
  end

  test "rollout requires an exact acknowledgement from every raft group" do
    context = ApplyContext.new(flow_default_history_max_events: 20)
    encoded = ApplyContext.encode(context)
    parent = self()

    assert :ok =
             ApplyContext.rollout(context, 4, fn shard_index, command ->
               send(parent, {:rollout, shard_index, command})
               {:ok, encoded}
             end)

    assert Enum.sort(
             for _ <- 1..4 do
               assert_receive {:rollout, shard_index, command}, 500
               {shard_index, command}
             end
           ) ==
             Enum.map(0..3, &{&1, ApplyContext.barrier_command(context)})
  end

  test "rollout fails closed when any raft group does not acknowledge the context" do
    context = ApplyContext.new(flow_default_history_max_events: 20)
    encoded = ApplyContext.encode(context)

    assert {:error,
            {:apply_context_rollout_failed,
             %{1 => {:error, :not_leader}, 2 => {:unexpected_ack, {:ok, :stale}}}}} =
             ApplyContext.rollout(context, 3, fn
               1, _command -> {:error, :not_leader}
               2, _command -> {:ok, :stale}
               _shard_index, _command -> {:ok, encoded}
             end)
  end

  test "targeted rollout only writes the elected raft group" do
    context = ApplyContext.new(flow_default_history_max_events: 20)
    encoded = ApplyContext.encode(context)
    parent = self()

    assert :ok =
             ApplyContext.rollout_shards(context, [3], fn shard_index, command ->
               send(parent, {:targeted_rollout, shard_index, command})
               {:ok, encoded}
             end)

    assert_receive {:targeted_rollout, 3, command}, 500
    assert command == ApplyContext.barrier_command(context)
    refute_receive {:targeted_rollout, _shard_index, _command}
  end

  test "barrier adopts and acknowledges the exact replicated context" do
    local = ApplyContext.new(flow_default_history_max_events: 10)
    leader = ApplyContext.new(flow_default_history_max_events: 20)
    encoded = ApplyContext.encode(leader)

    state = %{
      applied_count: 0,
      release_cursor_interval: 1_000,
      apply_context: local,
      apply_context_encoded: ApplyContext.encode(local)
    }

    assert {:ferricstore_apply_context, ^encoded, {:ferricstore_apply_context_barrier, ^encoded}} =
             ApplyContext.wrap_command(ApplyContext.barrier_command(leader), leader)

    assert {next_state, {:ok, ^encoded}} =
             StateMachine.apply(
               %{},
               ApplyContext.wrap_command(ApplyContext.barrier_command(leader), leader),
               state
             )

    assert next_state.apply_context == leader
    assert next_state.apply_context_encoded == encoded
  end

  test "malformed replicated contexts fail closed without running the command" do
    context = ApplyContext.default()

    state = %{
      applied_count: 0,
      release_cursor_interval: 1_000,
      apply_context: context,
      apply_context_encoded: ApplyContext.encode(context)
    }

    assert {next_state, {:error, "ERR invalid replicated apply context"}} =
             StateMachine.apply(
               %{},
               {:ferricstore_apply_context, {:flow_apply_context_v1, -1}, {:put, "k", "v", 0}},
               state
             )

    assert next_state.applied_count == 1
    assert next_state.apply_context == context
  end

  test "malformed replicated wrapper commands fail closed and advance apply accounting" do
    context = ApplyContext.default()

    state = %{
      applied_count: 0,
      release_cursor_interval: 1_000,
      apply_context: context,
      apply_context_encoded: ApplyContext.encode(context)
    }

    assert {next_state, {:error, "ERR invalid replicated apply context command"}} =
             StateMachine.apply(
               %{},
               {:ferricstore_apply_context, ApplyContext.encode(context), "not-a-command"},
               state
             )

    assert next_state.applied_count == 1
    assert next_state.apply_context == context
  end

  test "retention cleanup budgets cannot exceed the replicated context" do
    state_machine_source = Ferricstore.Test.SourceFiles.state_machine_source()

    key_budget_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        state_machine_source,
        "flow_retention_cleanup_key_budget"
      )

    byte_budget_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        state_machine_source,
        "flow_retention_cleanup_byte_budget"
      )

    plan_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        state_machine_source,
        "flow_retention_build_cleanup_plan"
      )

    assert key_budget_source =~
             ~r/min\(context\.flow_retention_cleanup_key_budget\)/

    refute key_budget_source =~ "max(8)"

    assert byte_budget_source =~
             ~r/min\(context\.flow_retention_cleanup_byte_budget\)/

    refute byte_budget_source =~ "max(4_096)"

    assert plan_source =~
             ~r/min\(value, context\.flow_retention_cleanup_key_budget\)/

    refute plan_source =~ "max(value, 8)"
  end

  test "retention planning derives budgets from the instance apply context" do
    router_source = Ferricstore.Test.SourceFiles.router_source()

    planning_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        router_source,
        "flow_retention_planning_attrs"
      )

    budget_source =
      Ferricstore.Test.SourceFiles.private_function_source!(
        router_source,
        "flow_retention_planning_budget",
        "when is_integer(value)"
      )

    assert planning_source =~ "flow_retention_planning_attrs(ctx, attrs)"
    assert planning_source =~ "context.flow_retention_cleanup_key_budget"
    assert planning_source =~ "context.flow_retention_cleanup_byte_budget"
    refute planning_source =~ "Application.get_env"
    refute router_source =~ "defp flow_retention_positive_config"
    assert budget_source =~ "min(value, maximum)"
    refute budget_source =~ "max("
    assert router_source =~ "min(acc.remaining_limit, acc.remaining_keys)"
  end

  test "Raft apply limit readers do not consult application configuration" do
    root = Path.expand("../../../lib/ferricstore/raft/state_machine/sections", __DIR__)

    for path <- Path.wildcard(Path.join(root, "*.ex")) do
      source = File.read!(path)

      assert runtime_limit_reads(source) == [],
             "#{Path.basename(path)} reads node-local runtime limits during Raft apply"
    end
  end

  defp runtime_limit_reads(source) do
    source
    |> Code.string_to_quoted!()
    |> Macro.prewalk([], fn
      {{:., _, [{:__aliases__, _, [:Application]}, :get_env]}, _, [_app, key | _]} = node, issues
      when key in @runtime_keys ->
        {node, [{:application_env, key} | issues]}

      {{:., _, [{:__aliases__, _, [:RetryPolicy]}, :resolve_retention]}, _, args} = node, issues
      when length(args) != 4 ->
        {node, [{:retention_resolver_arity, length(args)} | issues]}

      {{:., _, [{:__aliases__, _, [:Hibernation]}, function]}, _, []} = node, issues
      when function in [
             :enabled?,
             :hot_window_ms,
             :safety_margin_ms,
             :promote_window_ms,
             :late_promote_window_ms
           ] ->
        {node, [{:runtime_hibernation_reader, function} | issues]}

      node, issues ->
        {node, issues}
    end)
    |> elem(1)
    |> Enum.reverse()
  end
end
