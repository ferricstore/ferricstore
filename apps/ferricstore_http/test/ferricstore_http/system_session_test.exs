defmodule FerricstoreHttp.Invocations.SystemSessionTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.{Auth, Config}
  alias FerricstoreHttp.Invocations.SystemSession

  test "authenticates the runner with credentials read from named environment variables" do
    username_env = "FERRICSTORE_HTTP_TEST_RUNNER_USERNAME"
    password_env = "FERRICSTORE_HTTP_TEST_RUNNER_PASSWORD"
    previous = Map.new([username_env, password_env], &{&1, System.get_env(&1)})

    System.put_env(username_env, "worker")
    System.put_env(password_env, "secret")

    on_exit(fn ->
      Enum.each(previous, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end)

    {:ok, config} =
      Config.new(
        backend: FerricstoreHttp.TestBackend,
        runner_username_env: username_env,
        runner_password_env: password_env
      )

    start_supervised!({SystemSession, config})

    assert {:ok,
            %Auth.Context{
              session: {:session, {:internal, runner_id}},
              subject: "worker",
              scopes: ["invocations:runner"]
            }} = SystemSession.context()

    assert runner_id == config.runner_id
  end
end
