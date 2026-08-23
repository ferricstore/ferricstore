defmodule FerricstoreHttp.Invocations.DefinitionTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.{Config, ControlledTestBackend}
  alias FerricstoreHttp.Invocations.{Definition, DefinitionSeeder, SystemSession}

  test "normalizes a runnable HTTP endpoint definition" do
    assert {:ok, definition} =
             Definition.new(%{
               name: "send-email",
               target: %{kind: "http_endpoint", url: "https://example.com/invoke"},
               limits: %{idempotency_required: true},
               refs: %{allowed_read_names: ["receipt"]}
             })

    assert definition.flow_type == "invocation:send-email"
    assert definition.target["kind"] == "http_endpoint"
    assert Definition.idempotency_required?(definition)
    assert Definition.value_name_allowed?(definition, :read, "receipt")
    refute Definition.value_name_allowed?(definition, :read, "private")
  end

  test "rejects invalid names and malformed definition fields" do
    assert {:error, :invalid_invocation_name} = Definition.new(%{name: "bad/name"})

    assert {:error, :invalid_invocation_definition} =
             Definition.new(%{name: "valid", target: []})
  end

  test "rejects a named definition file containing a non-object value" do
    file =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_http_definitions_#{System.unique_integer([:positive])}.json"
      )

    on_exit(fn -> File.rm(file) end)
    File.write!(file, Jason.encode!(%{"send-email" => "not-an-object"}))
    {:ok, config} = Config.new(invocation_definitions_file: file)
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    assert {:error, :invalid_invocation_definitions_file} = DefinitionSeeder.start_link(config)
  end

  test "validates every definition before writing any of them" do
    start_supervised!(ControlledTestBackend)
    test_pid = self()

    ControlledTestBackend.put(:execute_result, fn _session, commands, _opts ->
      send(test_pid, {:definition_write, commands})
      {:ok, [%{status: :ok, value: "OK"}]}
    end)

    file =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_http_atomic_definitions_#{System.unique_integer([:positive])}.json"
      )

    definitions = [
      %{"name" => "valid-first", "enabled" => true},
      %{"name" => "invalid-second", "target" => []}
    ]

    on_exit(fn -> File.rm(file) end)
    File.write!(file, Jason.encode!(definitions))

    username_env = "FERRICSTORE_HTTP_TEST_SEEDER_USERNAME"
    password_env = "FERRICSTORE_HTTP_TEST_SEEDER_PASSWORD"
    System.put_env(username_env, "worker")
    System.put_env(password_env, "secret:with:colon")

    on_exit(fn ->
      System.delete_env(username_env)
      System.delete_env(password_env)
    end)

    {:ok, config} =
      Config.new(
        backend: ControlledTestBackend,
        invocation_definitions_file: file,
        runner_username_env: username_env,
        runner_password_env: password_env
      )

    start_supervised!({SystemSession, config})
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    assert {:error, :invalid_invocation_definition} = DefinitionSeeder.start_link(config)
    refute_receive {:definition_write, _commands}
  end
end
