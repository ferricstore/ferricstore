# Run with: mix run --no-start scripts/run_dashboard_demo.exs
#
# `--no-start` lets this script configure an isolated store before FerricStore boots.
if Enum.any?(Application.started_applications(), fn {app, _, _} -> app == :ferricstore end) do
  raise "FerricStore is already running; restart with: mix run --no-start scripts/run_dashboard_demo.exs"
end

# Script to boot FerricStore with rich sample workflows for dashboard inspection
demo_data_dir =
  System.get_env("FERRICSTORE_DASHBOARD_DEMO_DATA_DIR") ||
    Path.join(
      System.tmp_dir!(),
      "ferricstore-dashboard-demo-#{System.system_time(:microsecond)}"
    )

Logger.configure(level: :warning)
Application.put_env(:ferricstore, :data_dir, demo_data_dir)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore, :native_port, 6389)
Application.put_env(:ferricstore, :health_port, 4000)
Application.put_env(:ferricstore, :protected_mode, false)
Application.put_env(:ferricstore, :dashboard_allow_insecure_http, true)
Application.put_env(:ferricstore, :flow_policy_migration_worker_initial_delay_ms, 0)
Application.put_env(:ferricstore, :flow_policy_migration_worker_interval_ms, 25)
Application.put_env(:ferricstore, :flow_policy_migration_worker_catchup_delay_ms, 5)
Application.ensure_all_started(:ranch)
Application.ensure_all_started(:ferricstore)
Application.ensure_all_started(:ferricstore_server)

now = System.system_time(:millisecond)

IO.puts("==> Seeding sample workflow policies...")

FerricStore.flow_policy_set("order_fulfillment",
  indexed_attributes: ["tenant", "region", "priority"],
  indexed_state_meta: "carrier",
  retry: [max_retries: 5, backoff: [kind: :exponential, base_ms: 2000, max_ms: 60000]]
)

FerricStore.flow_policy_set("ai_agent_pipeline",
  indexed_attributes: ["model", "session_id", "tier"],
  indexed_state_meta: "token_usage",
  retry: [max_retries: 3, backoff: [kind: :exponential, base_ms: 1000, max_ms: 30000]]
)

FerricStore.flow_policy_set("crypto_settlement",
  indexed_attributes: ["currency", "chain", "tx_hash"],
  indexed_state_meta: "block_height"
)

FerricStore.flow_policy_set("fraud_investigation",
  indexed_attributes: ["risk_level", "assigned_analyst"],
  indexed_state_meta: "case_priority"
)

FerricStore.flow_policy_set("video_transcoding",
  indexed_attributes: ["resolution", "codec", "format"],
  indexed_state_meta: "encoder_node"
)

FerricStore.flow_policy_set("database_migration",
  indexed_attributes: ["source_db", "target_db", "cluster"],
  retry: [max_retries: 4, backoff: [kind: :exponential, base_ms: 5000, max_ms: 120_000]]
)

{:ok, _} = FerricStore.flow_policy_set("invoice_dispatch", states: %{"queued" => [mode: :fifo]})

case Ferricstore.Flow.LMDBWriter.flush_all(:default, 1, 30_000) do
  :ok -> :ok
  error -> raise "Could not publish dashboard demo policies: #{inspect(error)}"
end

instance_ctx = FerricStore.Instance.get(:default)
migration_deadline = System.monotonic_time(:millisecond) + 10_000

wait_for_policy_migrations = fn wait ->
  case Ferricstore.Flow.LMDBWriter.flush_all(:default, instance_ctx.shard_count, 30_000) do
    :ok -> :ok
    error -> raise "Could not publish dashboard demo policy migrations: #{inspect(error)}"
  end

  pending? =
    Enum.any?(0..(instance_ctx.shard_count - 1), fn shard_index ->
      Ferricstore.Store.Router.flow_policy_migration_pending?(instance_ctx, shard_index)
    end)

  cond do
    not pending? ->
      :ok

    System.monotonic_time(:millisecond) >= migration_deadline ->
      raise "Dashboard demo policy migrations did not complete within 10 seconds"

    true ->
      Process.sleep(10)
      wait.(wait)
  end
end

wait_for_policy_migrations.(wait_for_policy_migrations)

IO.puts("==> Seeding sample workflows for FerricStore Dashboard Demo...")

# 1. Active Executing E-Commerce Workflow
id1 = "order-processing-8891"

:ok =
  FerricStore.flow_create(id1,
    type: "order_fulfillment",
    state: "payment_approved",
    partition_key: "tenant-acme",
    priority: 2,
    payload_ref: "ref:order:8891:input",
    run_at_ms: now - 30_000,
    now_ms: now - 30_000
  )

:ok =
  FerricStore.flow_transition(id1, "payment_approved", "inventory_allocated",
    partition_key: "tenant-acme",
    fencing_token: 0,
    run_at_ms: now - 20_000,
    now_ms: now - 20_000
  )

case FerricStore.flow_claim_due("order_fulfillment",
       state: "inventory_allocated",
       partition_key: "tenant-acme",
       worker: "worker-eu-central-1",
       lease_ms: 86_400_000,
       limit: 1,
       now_ms: now
     ) do
  {:ok, [claim1]} ->
    FerricStore.flow_transition(id1, "inventory_allocated", "shipping_label_created",
      partition_key: "tenant-acme",
      fencing_token: claim1.fencing_token,
      run_at_ms: now,
      now_ms: now
    )

  other ->
    raise "Could not claim the dashboard demo order workflow: #{inspect(other)}"
end

# 2. Suspended Workflow Waiting for Customer Signal
id2 = "order-wait-payment-4412"

FerricStore.flow_create(id2,
  type: "checkout_flow",
  state: "awaiting_customer_approval",
  partition_key: "tenant-globex",
  priority: 1,
  payload_ref: "ref:checkout:4412:cart",
  run_at_ms: now - 180_000,
  now_ms: now - 180_000
)

# 3. Retrying Workflow with Error (Stripe Sync)
id3 = "external-stripe-sync-9901"

FerricStore.flow_create(id3,
  type: "stripe_sync",
  state: "sync_queued",
  partition_key: "tenant-stripe",
  run_at_ms: now - 100_000,
  now_ms: now - 100_000
)

case FerricStore.flow_claim_due("stripe_sync",
       state: "sync_queued",
       partition_key: "tenant-stripe",
       worker: "worker-us-east-1",
       lease_ms: 30_000,
       limit: 1,
       now_ms: now - 80_000
     ) do
  {:ok, [claim3]} ->
    FerricStore.flow_retry(id3, claim3.lease_token,
      partition_key: "tenant-stripe",
      fencing_token: claim3.fencing_token,
      error: "HTTP 504 Gateway Timeout from api.stripe.com/v1/charges",
      run_at_ms: now + 45_000,
      now_ms: now - 75_000
    )

  _ ->
    :ok
end

# 4. Completed Workflow with Full Lifecycle (User Onboarding)
id4 = "user-onboarding-7720"

FerricStore.flow_create(id4,
  type: "user_lifecycle",
  state: "signup_submitted",
  partition_key: "tenant-acme",
  payload_ref: "ref:user:7720:profile",
  run_at_ms: now - 600_000,
  now_ms: now - 600_000
)

FerricStore.flow_transition(id4, "signup_submitted", "verification_email_sent",
  partition_key: "tenant-acme",
  fencing_token: 0,
  run_at_ms: now - 500_000,
  now_ms: now - 500_000
)

case FerricStore.flow_claim_due("user_lifecycle",
       state: "verification_email_sent",
       partition_key: "tenant-acme",
       worker: "worker-eu-central-1",
       lease_ms: 60_000,
       limit: 1,
       now_ms: now - 400_000
     ) do
  {:ok, [claim4]} ->
    FerricStore.flow_complete(id4, claim4.lease_token,
      partition_key: "tenant-acme",
      fencing_token: claim4.fencing_token,
      result: ~s({"status":"verified","account_id":"acc_99214","mfa_enrolled":true}),
      now_ms: now - 350_000
    )

  _ ->
    :ok
end

# 5. Future Scheduled Workflow (Nightly Audit)
id5 = "nightly-audit-0088"

FerricStore.flow_create(id5,
  type: "scheduled_audit",
  state: "scheduled_wait",
  partition_key: "system",
  run_at_ms: now + 300_000,
  now_ms: now
)

# 6. AI Agent Pipeline (Multi-Step LLM & RAG Workflow)
id6 = "ai-pipeline-agent-402"

FerricStore.flow_create(id6,
  type: "ai_agent_pipeline",
  state: "prompt_received",
  partition_key: "tenant-openai",
  priority: 2,
  payload_ref: "ref:ai:prompt:402",
  run_at_ms: now - 50_000,
  now_ms: now - 50_000
)

FerricStore.flow_transition(id6, "prompt_received", "embedding_generated",
  partition_key: "tenant-openai",
  fencing_token: 0,
  run_at_ms: now - 40_000,
  now_ms: now - 40_000
)

FerricStore.flow_transition(id6, "embedding_generated", "vector_rag_retrieved",
  partition_key: "tenant-openai",
  fencing_token: 0,
  run_at_ms: now - 25_000,
  now_ms: now - 25_000
)

case FerricStore.flow_claim_due("ai_agent_pipeline",
       state: "vector_rag_retrieved",
       partition_key: "tenant-openai",
       worker: "worker-gpu-node-04",
       lease_ms: 90_000,
       limit: 1,
       now_ms: now
     ) do
  {:ok, [claim6]} ->
    FerricStore.flow_transition(id6, "vector_rag_retrieved", "model_inference_streaming",
      partition_key: "tenant-openai",
      fencing_token: claim6.fencing_token,
      run_at_ms: now,
      now_ms: now
    )

  _ ->
    :ok
end

# 7. Crypto Settlement (Multi-Sig & Block Confirmations)
id7 = "crypto-settle-btc-5120"

FerricStore.flow_create(id7,
  type: "crypto_settlement",
  state: "order_matched",
  partition_key: "tenant-binance",
  priority: 2,
  run_at_ms: now - 120_000,
  now_ms: now - 120_000
)

FerricStore.flow_transition(id7, "order_matched", "multisig_keys_requested",
  partition_key: "tenant-binance",
  fencing_token: 0,
  run_at_ms: now - 90_000,
  now_ms: now - 90_000
)

FerricStore.flow_transition(id7, "multisig_keys_requested", "confirmations_pending",
  partition_key: "tenant-binance",
  fencing_token: 0,
  run_at_ms: now - 45_000,
  now_ms: now - 45_000
)

# 8. Video Transcoding (Distributed Chunk Processing)
id8 = "video-transcode-4k-7701"

FerricStore.flow_create(id8,
  type: "video_transcoding",
  state: "raw_mp4_uploaded",
  partition_key: "tenant-media",
  priority: 1,
  run_at_ms: now - 70_000,
  now_ms: now - 70_000
)

FerricStore.flow_transition(id8, "raw_mp4_uploaded", "audio_extracted",
  partition_key: "tenant-media",
  fencing_token: 0,
  run_at_ms: now - 50_000,
  now_ms: now - 50_000
)

FerricStore.flow_transition(id8, "audio_extracted", "hls_chunks_segmented",
  partition_key: "tenant-media",
  fencing_token: 0,
  run_at_ms: now - 20_000,
  now_ms: now - 20_000
)

case FerricStore.flow_claim_due("video_transcoding",
       state: "hls_chunks_segmented",
       partition_key: "tenant-media",
       worker: "worker-encoder-09",
       lease_ms: 180_000,
       limit: 1,
       now_ms: now
     ) do
  {:ok, [claim8]} ->
    FerricStore.flow_transition(id8, "hls_chunks_segmented", "thumbnails_generated",
      partition_key: "tenant-media",
      fencing_token: claim8.fencing_token,
      run_at_ms: now,
      now_ms: now
    )

  _ ->
    :ok
end

# 9. Fraud & Anti-Money-Laundering Investigation (Waiting for Compliance Officer)
id9 = "fraud-case-aml-338"

FerricStore.flow_create(id9,
  type: "fraud_investigation",
  state: "anomaly_flagged",
  partition_key: "tenant-fintech",
  priority: 2,
  run_at_ms: now - 300_000,
  now_ms: now - 300_000
)

FerricStore.flow_transition(id9, "anomaly_flagged", "risk_score_calculated",
  partition_key: "tenant-fintech",
  fencing_token: 0,
  run_at_ms: now - 240_000,
  now_ms: now - 240_000
)

FerricStore.flow_transition(id9, "risk_score_calculated", "manual_compliance_review",
  partition_key: "tenant-fintech",
  fencing_token: 0,
  run_at_ms: now - 180_000,
  now_ms: now - 180_000
)

# 10. Database Online Migration (Retrying with Backoff)
id10 = "db-migrate-postgres-to-ferric"

FerricStore.flow_create(id10,
  type: "database_migration",
  state: "schema_snapshot_taken",
  partition_key: "system-ops",
  run_at_ms: now - 150_000,
  now_ms: now - 150_000
)

FerricStore.flow_transition(id10, "schema_snapshot_taken", "dual_write_enabled",
  partition_key: "system-ops",
  fencing_token: 0,
  run_at_ms: now - 120_000,
  now_ms: now - 120_000
)

case FerricStore.flow_claim_due("database_migration",
       state: "dual_write_enabled",
       partition_key: "system-ops",
       worker: "worker-infra-primary",
       lease_ms: 45_000,
       limit: 1,
       now_ms: now - 90_000
     ) do
  {:ok, [claim10]} ->
    FerricStore.flow_retry(id10, claim10.lease_token,
      partition_key: "system-ops",
      fencing_token: claim10.fencing_token,
      error: "Checksum mismatch on billing_events table at offset 492019",
      run_at_ms: now + 60_000,
      now_ms: now - 80_000
    )

  _ ->
    :ok
end

# 11. IoT Fleet Telemetry Stream
id11 = "fleet-telemetry-truck-994"

FerricStore.flow_create(id11,
  type: "iot_telemetry_batch",
  state: "ingestion_buffered",
  partition_key: "tenant-fleet",
  run_at_ms: now - 15_000,
  now_ms: now - 15_000
)

FerricStore.flow_transition(id11, "ingestion_buffered", "geofence_validated",
  partition_key: "tenant-fleet",
  fencing_token: 0,
  run_at_ms: now - 5_000,
  now_ms: now - 5_000
)

# 12. SaaS Subscription Renewal (Completed)
id12 = "sub-renewal-enterprise-880"

FerricStore.flow_create(id12,
  type: "subscription_renewal",
  state: "invoice_generated",
  partition_key: "tenant-saas",
  run_at_ms: now - 400_000,
  now_ms: now - 400_000
)

FerricStore.flow_transition(id12, "invoice_generated", "card_charged",
  partition_key: "tenant-saas",
  fencing_token: 0,
  run_at_ms: now - 350_000,
  now_ms: now - 350_000
)

FerricStore.flow_transition(id12, "card_charged", "seat_licenses_extended",
  partition_key: "tenant-saas",
  fencing_token: 0,
  run_at_ms: now - 300_000,
  now_ms: now - 300_000
)

case FerricStore.flow_claim_due("subscription_renewal",
       state: "seat_licenses_extended",
       partition_key: "tenant-saas",
       worker: "worker-billing-01",
       lease_ms: 60_000,
       limit: 1,
       now_ms: now - 20_000
     ) do
  {:ok, [claim12]} ->
    FerricStore.flow_complete(id12, claim12.lease_token,
      partition_key: "tenant-saas",
      fencing_token: claim12.fencing_token,
      result: ~s({"status":"active","seats":500,"renewal_date":"2027-08-20"}),
      now_ms: now - 10_000
    )

  _ ->
    :ok
end

case Ferricstore.Flow.LMDBWriter.flush_all(:default, 1, 30_000) do
  :ok -> :ok
  error -> raise "Could not publish dashboard demo query projections: #{inspect(error)}"
end

[
  {id1, "tenant-acme"},
  {id2, "tenant-globex"},
  {id3, "tenant-stripe"},
  {id4, "tenant-acme"},
  {id5, "system"},
  {id6, "tenant-openai"},
  {id7, "tenant-binance"},
  {id8, "tenant-media"},
  {id9, "tenant-fintech"},
  {id10, "system-ops"},
  {id11, "tenant-fleet"},
  {id12, "tenant-saas"}
]
|> Enum.each(fn {id, partition_key} ->
  case FerricStore.flow_get(id, partition_key: partition_key) do
    {:ok, record} when not is_nil(record) -> :ok
    other -> raise "Dashboard demo workflow #{id} was not readable: #{inspect(other)}"
  end
end)

for {partition, prefix, count, claim_at, lease_ms} <- [
      {"customer-1042", "fifo-invoice", 10, now, 86_400_000},
      {"customer-2048", "fifo-expired", 3, now - 60_000, 5_000},
      {"customer-4096", "fifo-scheduled", 2, nil, nil}
    ] do
  for index <- 1..count do
    id = "#{prefix}-#{String.pad_leading(Integer.to_string(index), 2, "0")}"
    run_at = if is_nil(claim_at) and index == 1, do: now + 3_600_000, else: now - 120_000

    :ok =
      FerricStore.flow_create(id,
        type: "invoice_dispatch",
        state: "queued",
        partition_key: partition,
        run_at_ms: run_at,
        now_ms: now - 120_000 + index
      )
  end

  if claim_at do
    {:ok, [head]} =
      FerricStore.flow_claim_due("invoice_dispatch",
        state: "queued",
        partition_key: partition,
        worker: "billing-worker-#{partition}",
        lease_ms: lease_ms,
        limit: 10,
        now_ms: claim_at
      )

    expected_id = "#{prefix}-01"
    if head.id != expected_id, do: raise("Unexpected demo FIFO head: #{head.id}")
  end
end

:ok = Ferricstore.Flow.LMDBWriter.flush_all(:default, 1, 30_000)

index_deadline = System.monotonic_time(:millisecond) + 15_000

wait_for_indexes = fn wait ->
  {:ok, status} = Ferricstore.Flow.Query.IndexStatus.fetch(instance_ctx)

  cond do
    Enum.all?(status["indexes"], & &1["queryable"]) ->
      :ok

    Enum.any?(status["indexes"], &(&1["state"] == "failed")) ->
      raise "Dashboard demo index validation failed: #{inspect(status)}"

    System.monotonic_time(:millisecond) >= index_deadline ->
      raise "Dashboard demo indexes were not ready within 15 seconds"

    true ->
      {:ok, _} =
        Ferricstore.Flow.Query.IndexLifecycleWorker.run_once(
          Ferricstore.Flow.Query.IndexLifecycleWorker.name(instance_ctx)
        )

      Process.sleep(10)
      wait.(wait)
  end
end

wait_for_indexes.(wait_for_indexes)

{:ok, related} =
  FerricStore.flow_query(
    "FROM runs WHERE partition_key = @p AND type = @t ORDER BY updated_at_ms DESC LIMIT 40 RETURN RECORDS",
    %{"p" => "customer-4096", "t" => "invoice_dispatch"}
  )

if MapSet.new(Enum.map(related.records, & &1.id)) !=
     MapSet.new(["fifo-scheduled-01", "fifo-scheduled-02"]),
   do: raise("Dashboard demo query omitted a scheduled FIFO member: #{inspect(related)}")

IO.puts("==> 12 sample workflows and 15 FIFO examples successfully created!")
IO.puts("    -> FIFO:        http://localhost:4000/dashboard/flow/states?type=invoice_dispatch")

IO.puts(
  "    1. [Running]    http://localhost:4000/dashboard/flow/#{id1}?partition_key=tenant-acme"
)

IO.puts(
  "    2. [Awaiting]   http://localhost:4000/dashboard/flow/#{id2}?partition_key=tenant-globex"
)

IO.puts(
  "    3. [Retry due]  http://localhost:4000/dashboard/flow/#{id3}?partition_key=tenant-stripe"
)

IO.puts(
  "    4. [Completed]  http://localhost:4000/dashboard/flow/#{id4}?partition_key=tenant-acme"
)

IO.puts("    5. [Scheduled]  http://localhost:4000/dashboard/flow/#{id5}?partition_key=system")

IO.puts(
  "    6. [AI Pipeline]http://localhost:4000/dashboard/flow/#{id6}?partition_key=tenant-openai"
)

IO.puts(
  "    7. [Crypto Settle]http://localhost:4000/dashboard/flow/#{id7}?partition_key=tenant-binance"
)

IO.puts(
  "    8. [Transcode]  http://localhost:4000/dashboard/flow/#{id8}?partition_key=tenant-media"
)

IO.puts(
  "    9. [Fraud Rev]  http://localhost:4000/dashboard/flow/#{id9}?partition_key=tenant-fintech"
)

IO.puts(
  "   10. [DB Migrate] http://localhost:4000/dashboard/flow/#{id10}?partition_key=system-ops"
)

IO.puts(
  "   11. [IoT Fleet]  http://localhost:4000/dashboard/flow/#{id11}?partition_key=tenant-fleet"
)

IO.puts(
  "   12. [SaaS Renew] http://localhost:4000/dashboard/flow/#{id12}?partition_key=tenant-saas"
)

IO.puts("    -> Overview:    http://localhost:4000/dashboard/flow")
IO.puts("    -> Query:       http://localhost:4000/dashboard/flow/query")
IO.puts("    -> Data:        #{demo_data_dir}")
IO.puts("==> Server listening on http://localhost:4000 ...")

Process.sleep(:infinity)
