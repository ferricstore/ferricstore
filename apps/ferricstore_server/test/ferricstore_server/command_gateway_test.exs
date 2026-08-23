defmodule FerricstoreServer.CommandGatewayTest do
  use Ferricstore.Test.AclCase

  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias Ferricstore.Waiters
  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.AuthenticationGateway
  alias FerricstoreServer.AuthenticationGateway.Session
  alias FerricstoreServer.CommandGateway
  alias FerricstoreServer.CommandGateway.PreparedBatch
  alias FerricstoreServer.Native.ResourceBudget

  setup do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
    :ok = CatalogProjector.mark_ready()
    :ok = FerricstoreServer.AuthRateLimiter.reset()
    :ok = Ferricstore.Config.set("requirepass", "")
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      Ferricstore.Config.set("requirepass", "")
    end)

    :ok
  end

  test "authenticates an ACL credential without retaining the password" do
    put_http_user("http-user", "secret")

    assert {:ok, %Session{username: "http-user"} = session} =
             AuthenticationGateway.authenticate("http-user", "secret", peer: peer())

    refute inspect(session) =~ "secret"

    assert {:error, :unauthenticated} =
             AuthenticationGateway.authenticate("http-user", "wrong", peer: peer())
  end

  test "supports the legacy default requirepass credential" do
    assert :ok = Ferricstore.Config.set("requirepass", "legacy-secret")

    assert {:ok, %Session{username: "default"} = session} =
             AuthenticationGateway.authenticate("default", "legacy-secret", peer: peer())

    assert {:error, :unauthenticated} =
             AuthenticationGateway.authenticate("default", "wrong", peer: peer())

    assert :ok = Ferricstore.Config.set("requirepass", "replacement-secret")

    assert {:error, :reauthentication_required} =
             CommandGateway.execute_batch(session, [["PING"]])
  end

  test "uses the shared authentication rate limiter" do
    put_http_user("limited", "secret")
    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)

    on_exit(fn ->
      Application.delete_env(:ferricstore, :auth_rate_limit_max_attempts)
    end)

    assert {:error, :unauthenticated} =
             AuthenticationGateway.authenticate("limited", "wrong", peer: peer())

    assert {:error, {:rate_limited, retry_after_ms}} =
             AuthenticationGateway.authenticate("limited", "secret", peer: peer())

    assert retry_after_ms > 0
  end

  test "executes an ordered stateless command batch through the canonical command path" do
    put_http_user("writer", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("writer", "secret", peer: peer())

    key = allowed_key("allowed")

    assert {:ok,
            [
              %{status: :ok, value: "OK"},
              %{status: :ok, value: "value"}
            ]} =
             CommandGateway.execute_batch(session, [
               ["SET", key, "value"],
               ["GET", key]
             ])

    assert {:ok, native_set} =
             CommandGateway.native_command("SET", 0x0102, %{
               "key" => key,
               "value" => "native-value"
             })

    assert {:ok, [%{status: :ok, value: "OK"}]} =
             CommandGateway.execute_batch(session, [native_set])

    assert {:ok, [%{status: :ok, value: "native-value"}]} =
             CommandGateway.execute_batch(session, [["GET", key]])
  end

  test "executes SDK value commands through generic and native stateless paths" do
    username = "flow-values"
    password = "secret"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               ">#{password}",
               "-@all",
               "+FLOW.VALUE.PUT",
               "+@read",
               "~*"
             ])

    assert {:ok, session} =
             AuthenticationGateway.authenticate(username, password, peer: peer())

    assert {:ok, [%{status: :ok, value: %{"ref" => ref}}]} =
             CommandGateway.execute_batch(session, [["FLOW.VALUE.PUT", "value"]])

    assert {:ok, native_value_put} =
             CommandGateway.native_command("FLOW.VALUE.PUT", 0x020B, %{
               "value" => "native-value",
               "now_ms" => System.system_time(:millisecond)
             })

    assert {:ok, [%{status: :ok, value: %{ref: native_ref}}]} =
             CommandGateway.execute_batch(session, [native_value_put])

    assert is_binary(native_ref)

    assert {:ok, native_command} =
             CommandGateway.native_command("FLOW.VALUE.MGET", 0x020C, %{"refs" => [ref]})

    assert {:ok, [%{status: :ok, value: ["value"]}]} =
             CommandGateway.execute_batch(session, [native_command])

    assert :ok =
             FerricstoreServer.Acl.set_user("scoped-flow-values", [
               "on",
               ">#{password}",
               "-@all",
               "+@read",
               "~tenant:a:*"
             ])

    assert {:ok, scoped_session} =
             AuthenticationGateway.authenticate("scoped-flow-values", password, peer: peer())

    assert {:ok, [%{status: :noperm, value: denied}]} =
             CommandGateway.execute_batch(scoped_session, [native_command])

    assert denied =~ "NOPERM"
  end

  test "executes a structured Flow opcode through the stateless gateway" do
    username = "flow-structured"
    password = "secret"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               ">#{password}",
               "-@all",
               "+FLOW.START_AND_CLAIM",
               "~tenant:a:*"
             ])

    assert {:ok, session} =
             AuthenticationGateway.authenticate(username, password, peer: peer())

    id = allowed_key("start-and-claim")

    payload = %{
      "id" => id,
      "type" => "gateway-workflow",
      "initial_state" => "queued",
      "worker" => "gateway-worker",
      "lease_ms" => 30_000,
      "partition_key" => id,
      "now_ms" => System.system_time(:millisecond)
    }

    assert {:ok, native_command} =
             CommandGateway.native_command("FLOW.START_AND_CLAIM", 0x0223, payload)

    assert {:ok, [%{status: :ok, value: %{id: ^id, state: "running"}}]} =
             CommandGateway.execute_batch(session, [native_command])

    assert {:error, :invalid_native_command} =
             CommandGateway.native_command("FLOW.START_AND_CLAIM", 0x0224, payload)

    assert {:error, :invalid_native_command} =
             CommandGateway.native_command("PING", 0x0223, payload)

    assert {:error, {:malformed_command, 0}} =
             CommandGateway.execute_batch(session, [
               %{
                 "command" => "FLOW.START_AND_CLAIM",
                 "opcode" => 0x0223,
                 "payload" => payload
               }
             ])
  end

  test "executes the structured native FLOW.QUERY contract used by HTTP transports" do
    type = "gateway-query-#{System.unique_integer([:positive, :monotonic])}"
    partition = allowed_key("query-partition")
    id = allowed_key("query-record")

    assert {:ok, _policy} = FerricStore.flow_policy_set(type, indexed_state_meta: "version")

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               state: "attr",
               partition_key: partition,
               attributes: %{"tenant" => "acme", "tier" => "gold"},
               state_meta: %{"version" => "1"},
               now_ms: System.system_time(:millisecond)
             )

    put_http_user("query-reader", "secret", ["+FLOW.QUERY"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("query-reader", "secret", peer: peer())

    payload = %{
      "version" => "FQL1",
      "query" =>
        "FROM runs WHERE partition_key = @partition_key AND type = @type AND state = @state AND attribute['tenant'] = @attribute_0 ORDER BY updated_at_ms ASC LIMIT 100 RETURN RECORDS",
      "params" => %{
        "partition_key" => partition,
        "type" => type,
        "state" => "attr",
        "attribute_0" => "acme"
      }
    }

    assert {:ok, native_command} =
             CommandGateway.native_command("FLOW.QUERY", 0x0231, payload)

    assert %CommandGateway.NativeCommand{} = native_command

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 case CommandGateway.execute_batch(session, [native_command]) do
                   {:ok, [%{status: :ok, value: %{records: records}}]} ->
                     Enum.any?(records, &(&1.id == id))

                   _projection_not_ready ->
                     false
                 end
               end,
               "structured FLOW.QUERY did not expose the created record"
             )

    assert {:error, :invalid_native_command} =
             CommandGateway.native_command("FLOW.QUERY", 0x0230, payload)
  end

  test "returns command and key ACL failures in their original batch positions" do
    put_http_user("reader", "secret", ["+GET"])
    assert {:ok, session} = AuthenticationGateway.authenticate("reader", "secret", peer: peer())

    denied_key = denied_key("denied")
    allowed_key = allowed_key("allowed")

    assert {:ok,
            [
              %{status: :noperm, value: denied},
              %{status: :ok, value: nil},
              %{status: :noperm, value: command_denied}
            ]} =
             CommandGateway.execute_batch(session, [
               ["GET", denied_key],
               ["GET", allowed_key],
               ["SET", allowed_key, "value"]
             ])

    assert denied =~ "NOPERM"
    assert command_denied =~ "NOPERM"
  end

  test "keeps preparation failures as ordered command results" do
    put_http_user("worker", "secret", ["+PING"])
    assert {:ok, session} = AuthenticationGateway.authenticate("worker", "secret", peer: peer())

    assert {:ok,
            [
              %{status: :bad_request, value: unknown},
              %{status: :ok, value: "PONG"}
            ]} = CommandGateway.execute_batch(session, [["NOT.A.COMMAND"], ["PING"]])

    assert unknown =~ "unknown command"
  end

  test "stores definitions and creates durable invocations through the shared command engine" do
    username = "invocation-admin"
    password = "secret"
    name = "send-email-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.DEFINITION.PUT",
               "+INVOCATION.DEFINITION.GET",
               "+INVOCATION.DEFINITION.LIST",
               "+INVOCATION.CREATE",
               "+INVOCATION.GET",
               "+INVOCATION.PARTITION.LIST",
               "~invocation:*"
             ])

    assert {:ok, session} =
             AuthenticationGateway.authenticate(username, password, peer: peer())

    definition = %{
      "name" => name,
      "enabled" => true,
      "flow_type" => "email-delivery-#{name}",
      "initial_state" => "queued",
      "limits" => %{
        "max_payload_bytes" => 1_024,
        "idempotency_required" => true
      },
      "partition" => %{"key" => "invocation:{name}:{subject}"},
      "acl" => %{"invoke_key" => "invocation:create:{name}"}
    }

    assert {:ok, [%{status: :ok, value: "OK"}]} =
             CommandGateway.execute_batch(session, [
               ["INVOCATION.DEFINITION.PUT", Jason.encode!(definition)]
             ])

    assert {:ok, [%{status: :ok, value: ^definition}]} =
             CommandGateway.execute_batch(session, [
               ["INVOCATION.DEFINITION.GET", name]
             ])

    assert {:ok, [%{status: :ok, value: definitions}]} =
             CommandGateway.execute_batch(session, [["INVOCATION.DEFINITION.LIST"]])

    assert definition in definitions

    envelope = %{
      "attrs" => %{"payload" => %{"to" => "test@example.com"}},
      "idempotency_key" => "request-1"
    }

    execution_opts = %{
      request_context: %{"subject" => username, "scopes" => []}
    }

    expected_partition = "invocation:#{name}:#{username}"

    assert {:ok, [%{status: :ok, value: created}]} =
             CommandGateway.execute_batch(
               session,
               [["INVOCATION.CREATE", name, Jason.encode!(envelope)]],
               store:
                 FerricStore.Instance.get(:default)
                 |> Map.put(:request_context, execution_opts.request_context)
             )

    assert %{
             "invocation_id" => invocation_id,
             "name" => ^name,
             "flow_type" => "email-delivery-" <> _,
             "state" => "queued",
             "partition_key" => ^expected_partition
           } = created

    assert {:ok, [%{status: :ok, value: ^created}]} =
             CommandGateway.execute_batch(
               session,
               [["INVOCATION.CREATE", name, Jason.encode!(envelope)]],
               store:
                 FerricStore.Instance.get(:default)
                 |> Map.put(:request_context, execution_opts.request_context)
             )

    assert {:ok, [%{status: :ok, value: metadata}]} =
             CommandGateway.execute_batch(
               session,
               [["INVOCATION.GET", invocation_id]],
               store:
                 FerricStore.Instance.get(:default)
                 |> Map.put(:request_context, execution_opts.request_context)
             )

    assert metadata["id"] == invocation_id
    assert metadata["name"] == name
    refute Map.has_key?(metadata, "tenant")

    assert {:ok, [%{status: :ok, value: partitions}]} =
             CommandGateway.execute_batch(session, [["INVOCATION.PARTITION.LIST", name]])

    assert expected_partition in partitions

    conflicting = put_in(envelope, ["attrs", "payload", "to"], "other@example.com")

    assert {:ok, [%{status: :error, value: conflict}]} =
             CommandGateway.execute_batch(
               session,
               [["INVOCATION.CREATE", name, Jason.encode!(conflicting)]],
               store:
                 FerricStore.Instance.get(:default)
                 |> Map.put(:request_context, execution_opts.request_context)
             )

    assert conflict =~ "idempotency_conflict"
  end

  test "rejects a missing invocation attrs map without raising in native execution" do
    username = "invocation-validation-admin"
    password = "secret"
    name = "validation-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.DEFINITION.PUT",
               "+INVOCATION.CREATE",
               "~invocation:*"
             ])

    assert {:ok, session} =
             AuthenticationGateway.authenticate(username, password, peer: peer())

    assert {:ok, [%{status: :ok}]} =
             CommandGateway.execute_batch(session, [
               [
                 "INVOCATION.DEFINITION.PUT",
                 Jason.encode!(%{"name" => name, "enabled" => true})
               ]
             ])

    assert {:ok, [%{status: :error, value: reason}]} =
             CommandGateway.execute_batch(session, [
               ["INVOCATION.CREATE", name, Jason.encode!(%{})]
             ])

    assert reason == "ERR invalid invocation attributes"
  end

  test "checks invocation command and logical key ACLs before execution" do
    admin = "invocation-acl-admin"
    password = "secret"
    name = "acl-target-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             FerricstoreServer.Acl.set_user(admin, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.DEFINITION.PUT",
               "~invocation:definition:*"
             ])

    assert {:ok, admin_session} =
             AuthenticationGateway.authenticate(admin, password, peer: peer())

    definition = %{
      "name" => name,
      "enabled" => true
    }

    assert {:ok, [%{status: :ok}]} =
             CommandGateway.execute_batch(admin_session, [
               ["INVOCATION.DEFINITION.PUT", Jason.encode!(definition)]
             ])

    caller = "invocation-acl-caller"

    assert :ok =
             FerricstoreServer.Acl.set_user(caller, [
               "on",
               ">#{password}",
               "-@all",
               "+INVOCATION.CREATE",
               "~invocation:other"
             ])

    assert {:ok, caller_session} =
             AuthenticationGateway.authenticate(caller, password, peer: peer())

    envelope = Jason.encode!(%{"attrs" => %{}})

    store =
      FerricStore.Instance.get(:default)
      |> Map.put(:request_context, %{"subject" => caller, "scopes" => []})

    assert {:ok, [%{status: :noperm, value: key_denied}]} =
             CommandGateway.execute_batch(
               caller_session,
               [["INVOCATION.CREATE", name, envelope]],
               store: store
             )

    assert key_denied =~ "NOPERM"

    assert :ok =
             FerricstoreServer.Acl.set_user(caller, [
               "resetkeys",
               "~invocation:#{name}",
               "-INVOCATION.CREATE"
             ])

    assert {:ok, caller_session} =
             AuthenticationGateway.authenticate(caller, password, peer: peer())

    assert {:ok, [%{status: :noperm, value: command_denied}]} =
             CommandGateway.execute_batch(
               caller_session,
               [["INVOCATION.CREATE", name, envelope]],
               store: store
             )

    assert command_denied =~ "NOPERM"
  end

  test "invalidates a cached session when its credential epoch changes" do
    put_http_user("rotating", "old-secret")

    assert {:ok, session} =
             AuthenticationGateway.authenticate("rotating", "old-secret", peer: peer())

    assert :ok = FerricstoreServer.Acl.set_user("rotating", [">new-secret"])

    assert {:error, :reauthentication_required} =
             CommandGateway.execute_batch(session, [["PING"]])
  end

  test "reuses the immutable ACL snapshot while the credential epoch is unchanged" do
    put_http_user("stable", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("stable", "secret", peer: peer())

    assert {:ok, validated} = AuthenticationGateway.validate(session)
    assert :erts_debug.same(session, validated)
  end

  test "prevalidates the complete batch and rejects connection-scoped commands" do
    put_http_user("writer", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("writer", "secret", peer: peer())
    key = allowed_key("not-written")

    assert {:error, {:unsupported_command, 1, "MULTI"}} =
             CommandGateway.execute_batch(session, [
               ["SET", key, "value"],
               ["MULTI"]
             ])

    assert Router.get(FerricStore.Instance.get(:default), key) == nil

    assert {:error, {:malformed_command, 1}} =
             CommandGateway.execute_batch(session, [
               ["SET", key, "value"],
               []
             ])

    assert Router.get(FerricStore.Instance.get(:default), key) == nil
  end

  test "prepares independent request batches before executing them under one session validation" do
    put_http_user("writer", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("writer", "secret", peer: peer())
    key = allowed_key("prepared")

    assert {:ok, write} =
             CommandGateway.prepare_batch([["SET", key, "value"]],
               max_commands: 1,
               deadline_ms: System.system_time(:millisecond) + 1_000
             )

    assert {:error, {:unsupported_command, 0, "MULTI"}} =
             CommandGateway.prepare_batch([["MULTI"]], max_commands: 1)

    assert {:ok, read} = CommandGateway.prepare_batch([["GET", key]], max_commands: 1)

    assert {:ok,
            [
              [%{status: :ok, value: "OK"}],
              [%{status: :ok, value: "value"}]
            ]} = CommandGateway.execute_prepared_batches(session, [write, read])
  end

  test "rejects invalid prepared values before executing any request" do
    put_http_user("writer", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("writer", "secret", peer: peer())
    key = allowed_key("invalid-prepared")
    assert {:ok, write} = CommandGateway.prepare_batch([["SET", key, "value"]])

    assert {:error, {:invalid_batch, "prepared batches are invalid"}} =
             CommandGateway.execute_prepared_batches(session, [write, :not_prepared])

    assert Router.get(FerricStore.Instance.get(:default), key) == nil
  end

  test "revalidates forged prepared batches before execution" do
    put_http_user("writer", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("writer", "secret", peer: peer())
    key = allowed_key("forged-prepared")

    forged = %PreparedBatch{
      planned: [
        {:command_exec, %{"command" => "SET", "args" => [key, "value"]}},
        {:native, 0xFFFF, %{}}
      ],
      deadline_ms: 0
    }

    assert {:error, {:invalid_batch, "prepared batches are invalid"}} =
             CommandGateway.execute_prepared_batches(session, [forged])

    assert Router.get(FerricStore.Instance.get(:default), key) == nil
  end

  test "contains invalid UTF-8 command names as typed validation errors" do
    invalid_command = <<0xFF, 0xFE>>

    assert {:error, :invalid_native_command} =
             CommandGateway.native_command(invalid_command, 0x0101, %{})

    assert {:error, {:malformed_command, 0}} =
             CommandGateway.prepare_batch([[invalid_command]])

    put_http_user("invalid-command", "secret")

    assert {:ok, session} =
             AuthenticationGateway.authenticate("invalid-command", "secret", peer: peer())

    forged = %PreparedBatch{
      planned: [{:command_exec, %{"command" => invalid_command, "args" => []}}],
      deadline_ms: 0
    }

    assert {:error, {:invalid_batch, "prepared batches are invalid"}} =
             CommandGateway.execute_prepared_batches(session, [forged])
  end

  test "rejects every connection-scoped command family at the transport-neutral boundary" do
    put_http_user("worker", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("worker", "secret", peer: peer())

    for command <- ["AUTH", "MULTI", "SUBSCRIBE"] do
      assert {:error, {:unsupported_command, 0, ^command}} =
               CommandGateway.execute_batch(session, [[command]])
    end
  end

  test "executes one blocking list command and wakes it through the canonical waiter path" do
    put_http_user("blocking-list", "secret", ["+BLPOP", "+RPUSH"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-list", "secret", peer: peer())

    key = allowed_key("blocking-list")
    deadline_ms = System.system_time(:millisecond) + 2_000
    usage_before = ResourceBudget.usage()

    blocked =
      Task.async(fn ->
        CommandGateway.execute_batch(session, [["BLPOP", key, "0"]], deadline_ms: deadline_ms)
      end)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 usage = ResourceBudget.usage()

                 Waiters.count(key) == 1 and
                   usage.executions == usage_before.executions + 1 and
                   usage.blocking_requests == usage_before.blocking_requests + 1
               end,
               "gateway BLPOP did not register a waiter"
             )

    assert {:ok, [%{status: :ok, value: 1}]} =
             CommandGateway.execute_batch(session, [["RPUSH", key, "value"]])

    assert {:ok, [%{status: :ok, value: [^key, "value"]}]} = Task.await(blocked, 3_000)
    assert Waiters.count(key) == 0

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 usage = ResourceBudget.usage()

                 usage.executions == usage_before.executions and
                   usage.blocking_requests == usage_before.blocking_requests
               end,
               "completed gateway BLPOP leaked resource leases"
             )
  end

  test "executes the remaining blocking list commands through their waiter paths" do
    put_http_user("blocking-list-variants", "secret", [
      "+BRPOP",
      "+BLMOVE",
      "+BLMPOP",
      "+LPUSH",
      "+RPUSH"
    ])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-list-variants", "secret", peer: peer())

    brpop_key = allowed_key("brpop")

    assert_blocking_list_wake(
      session,
      ["BRPOP", brpop_key, "0"],
      brpop_key,
      ["LPUSH", brpop_key, "brpop-value"],
      [brpop_key, "brpop-value"]
    )

    blmove_tag = "gateway-blmove-#{System.unique_integer([:positive, :monotonic])}"
    blmove_source = "tenant:a:gateway:{#{blmove_tag}}:source"
    blmove_destination = "tenant:a:gateway:{#{blmove_tag}}:destination"

    assert_blocking_list_wake(
      session,
      ["BLMOVE", blmove_source, blmove_destination, "RIGHT", "LEFT", "0"],
      blmove_source,
      ["RPUSH", blmove_source, "blmove-value"],
      "blmove-value"
    )

    blmpop_key = allowed_key("blmpop")

    assert_blocking_list_wake(
      session,
      ["BLMPOP", "0", "1", blmpop_key, "LEFT", "COUNT", "2"],
      blmpop_key,
      ["RPUSH", blmpop_key, "one", "two"],
      [blmpop_key, ["one", "two"]]
    )
  end

  test "request deadline cancels an infinite blocking command and releases its waiter" do
    put_http_user("blocking-deadline", "secret", ["+BLPOP"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-deadline", "secret", peer: peer())

    key = allowed_key("blocking-deadline")
    started_ms = System.monotonic_time(:millisecond)
    usage_before = ResourceBudget.usage()

    assert {:ok, [%{status: :error, value: %{"code" => "deadline_exceeded"}}]} =
             CommandGateway.execute_batch(session, [["BLPOP", key, "0"]],
               deadline_ms: System.system_time(:millisecond) + 100
             )

    assert System.monotonic_time(:millisecond) - started_ms < 1_000

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 usage = ResourceBudget.usage()

                 Waiters.count(key) == 0 and usage.executions == usage_before.executions and
                   usage.blocking_requests == usage_before.blocking_requests
               end,
               "deadline-cancelled gateway BLPOP leaked its waiter"
             )
  end

  test "redacts blocking worker exit reasons from transport results" do
    put_http_user("blocking-worker-exit", "secret", ["+BLPOP"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-worker-exit", "secret", peer: peer())

    key = allowed_key("blocking-worker-exit")
    invalid_store = %{stats_counter: :transport_secret_must_not_escape}

    assert {:ok, [%{status: :error, value: "ERR internal server error"}]} =
             CommandGateway.execute_batch(session, [["BLPOP", key, "0"]],
               store: invalid_store,
               deadline_ms: System.system_time(:millisecond) + 1_000
             )
  end

  test "caller cancellation terminates the blocking worker and releases its waiter" do
    put_http_user("blocking-cancel", "secret", ["+BLPOP"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-cancel", "secret", peer: peer())

    key = allowed_key("blocking-cancel")
    usage_before = ResourceBudget.usage()

    caller =
      spawn(fn ->
        CommandGateway.execute_batch(session, [["BLPOP", key, "0"]])
      end)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Waiters.count(key) == 1 end,
               "gateway BLPOP did not register before caller cancellation"
             )

    caller_ref = Process.monitor(caller)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^caller_ref, :process, ^caller, :killed}, 1_000

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 usage = ResourceBudget.usage()

                 Waiters.count(key) == 0 and usage.executions == usage_before.executions and
                   usage.blocking_requests == usage_before.blocking_requests
               end,
               "cancelled gateway caller leaked its blocking waiter"
             )
  end

  test "executes a blocking stream read and wakes it after XADD" do
    put_http_user("blocking-stream", "secret", ["+XREAD", "+XADD"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-stream", "secret", peer: peer())

    key = allowed_key("blocking-stream")
    store = FerricStore.Instance.get(:default)
    assert {:ok, old_id} = FerricStore.xadd(key, ["field", "old"])

    blocked =
      Task.async(fn ->
        CommandGateway.execute_batch(
          session,
          [["XREAD", "BLOCK", "0", "STREAMS", key, old_id]],
          deadline_ms: System.system_time(:millisecond) + 2_000
        )
      end)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Ferricstore.Commands.Stream.stream_waiter_count(key, store) == 1 end,
               "gateway XREAD did not register a stream waiter"
             )

    assert {:ok, [%{status: :ok, value: entry_id}]} =
             CommandGateway.execute_batch(session, [["XADD", key, "*", "field", "value"]])

    assert is_binary(entry_id)

    assert {:ok, [%{status: :ok, value: [[^key, [[^entry_id, "field", "value"]]]]}]} =
             Task.await(blocked, 3_000)

    assert Ferricstore.Commands.Stream.stream_waiter_count(key, store) == 0
  end

  test "executes XREADGROUP BLOCK and wakes it after XADD" do
    put_http_user("blocking-stream-group", "secret", ["+XREADGROUP", "+XGROUP", "+XADD"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-stream-group", "secret", peer: peer())

    key = allowed_key("blocking-stream-group")
    group = "gateway-group"
    consumer = "gateway-consumer"
    store = FerricStore.Instance.get(:default)

    assert {:ok, [%{status: :ok, value: "OK"}]} =
             CommandGateway.execute_batch(session, [
               ["XGROUP", "CREATE", key, group, "$", "MKSTREAM"]
             ])

    blocked =
      Task.async(fn ->
        CommandGateway.execute_batch(
          session,
          [
            [
              "XREADGROUP",
              "GROUP",
              group,
              consumer,
              "BLOCK",
              "0",
              "STREAMS",
              key,
              ">"
            ]
          ],
          deadline_ms: System.system_time(:millisecond) + 2_000
        )
      end)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Ferricstore.Commands.Stream.stream_waiter_count(key, store) == 1 end,
               "gateway XREADGROUP did not register a stream waiter"
             )

    assert {:ok, [%{status: :ok, value: entry_id}]} =
             CommandGateway.execute_batch(session, [["XADD", key, "*", "field", "value"]])

    assert {:ok, [%{status: :ok, value: [[^key, [[^entry_id, "field", "value"]]]]}]} =
             Task.await(blocked, 3_000)

    assert Ferricstore.Commands.Stream.stream_waiter_count(key, store) == 0
  end

  test "blocking commands preserve one request pipeline but cannot cross-request coalesce" do
    for command <- ["BLPOP", "BRPOP", "BLMOVE", "BLMPOP", "XREAD", "XREADGROUP"] do
      assert {:ok, %PreparedBatch{}} = CommandGateway.prepare_batch([[command]])
    end

    first_key = allowed_key("mixed-first")
    second_key = allowed_key("mixed-second")
    assert {:ok, 1} = FerricStore.rpush(first_key, ["first"])
    assert {:ok, 1} = FerricStore.rpush(second_key, ["second"])

    put_http_user("blocking-batch", "secret", ["+BLPOP", "+BRPOP", "+PING"])

    assert {:ok, session} =
             AuthenticationGateway.authenticate("blocking-batch", "secret", peer: peer())

    pipeline = [
      ["PING"],
      ["BLPOP", first_key, "0"],
      ["BRPOP", second_key, "0"],
      ["PING"]
    ]

    assert {:ok,
            [
              %{status: :ok, value: "PONG"},
              %{status: :ok, value: [^first_key, "first"]},
              %{status: :ok, value: [^second_key, "second"]},
              %{status: :ok, value: "PONG"}
            ]} = CommandGateway.execute_batch(session, pipeline)

    assert {:ok, blocking_pipeline} =
             CommandGateway.prepare_batch([
               ["PING"],
               ["BLPOP", allowed_key("cross-request"), "1"]
             ])

    assert {:ok, ping} = CommandGateway.prepare_batch([["PING"]])

    assert {:error, {:invalid_batch, combined_reason}} =
             CommandGateway.execute_prepared_batches(session, [blocking_pipeline, ping])

    assert combined_reason =~ "blocking request"
  end

  test "bounds batches and propagates absolute deadlines" do
    put_http_user("worker", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("worker", "secret", peer: peer())

    assert {:error, {:too_many_commands, 1}} =
             CommandGateway.execute_batch(session, [["PING"], ["PING"]], max_commands: 1)

    expired = System.system_time(:millisecond) - 1

    assert {:ok, [%{status: :error, value: %{"code" => "deadline_exceeded"}}]} =
             CommandGateway.execute_batch(session, [["PING"]], deadline_ms: expired)
  end

  defp put_http_user(username, password, commands \\ ["+GET", "+SET"]) do
    assert :ok =
             FerricstoreServer.Acl.set_user(
               username,
               ["on", ">#{password}", "-@all" | commands] ++ ["~tenant:a:*"]
             )
  end

  defp assert_blocking_list_wake(session, blocking_command, wait_key, producer, expected) do
    blocked =
      Task.async(fn ->
        CommandGateway.execute_batch(session, [blocking_command],
          deadline_ms: System.system_time(:millisecond) + 2_000
        )
      end)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Waiters.count(wait_key) == 1 end,
               "gateway #{hd(blocking_command)} did not register a waiter"
             )

    assert {:ok, [%{status: :ok}]} = CommandGateway.execute_batch(session, [producer])

    assert {:ok, [%{status: :ok, value: ^expected}]} = Task.await(blocked, 3_000)
    assert Waiters.count(wait_key) == 0
  end

  defp peer, do: {{127, 0, 0, 1}, 12_345}

  defp allowed_key(suffix) do
    "tenant:a:gateway:#{suffix}:#{System.unique_integer([:positive, :monotonic])}"
  end

  defp denied_key(suffix) do
    "tenant:b:gateway:#{suffix}:#{System.unique_integer([:positive, :monotonic])}"
  end
end
