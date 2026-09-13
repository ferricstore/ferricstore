Code.require_file(Path.expand("../../../../config/dev_runtime.exs", __DIR__))

defmodule Ferricstore.DevRuntimeTest do
  use ExUnit.Case, async: false

  @config_path Path.expand("../../../../config/config.exs", __DIR__)
  @config_dir Path.dirname(@config_path)

  setup do
    env_names = [
      "FERRICSTORE_DATA_DIR",
      "FERRICSTORE_NATIVE_PORT",
      "FERRICSTORE_HEALTH_PORT",
      "FERRICSTORE_HEALTH_PROBE_PORT",
      "FERRICSTORE_HTTP_PORT"
    ]

    previous_env = Map.new(env_names, &{&1, System.get_env(&1)})
    Enum.each(env_names, &System.delete_env/1)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_dev_runtime_#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(root, "config"))

    on_exit(fn ->
      File.rm_rf!(root)

      Enum.each(previous_env, fn
        {name, nil} -> System.delete_env(name)
        {name, value} -> System.put_env(name, value)
      end)
    end)

    {:ok, root: root}
  end

  test "linked worktrees use checkout-local data and ephemeral listener ports", %{root: root} do
    File.write!(Path.join(root, ".git"), "gitdir: /tmp/git/worktrees/demo\n")

    settings = Ferricstore.DevRuntime.settings(Path.join(root, "config"))

    assert settings.linked_worktree?
    assert settings.isolated?
    assert settings.checkout_root == root
    assert settings.data_dir == Path.join(root, "data")
    assert settings.native_port == 0
    assert settings.health_port == 0
    assert settings.health_probe_port == 0
    assert settings.http_port == 0
    assert settings.node_identity == :nonode@nohost
    assert settings.discovery == :disabled
    assert settings.isolation_warning == nil
  end

  test "ordinary checkout keeps the existing fixed development ports", %{root: root} do
    File.mkdir_p!(Path.join(root, ".git"))

    settings = Ferricstore.DevRuntime.settings(Path.join(root, "config"))

    refute settings.linked_worktree?
    refute settings.isolated?
    assert settings.data_dir == Path.join(root, "data")
    assert settings.native_port == 6388
    assert settings.health_port == 4000
    assert settings.health_probe_port == 4001
    assert settings.http_port == 8080
  end

  test "ordinary checkout data stays absolute when the process cwd differs", %{root: root} do
    File.mkdir_p!(Path.join(root, ".git"))

    other_root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_dev_runtime_cwd_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(other_root)
    on_exit(fn -> File.rm_rf!(other_root) end)

    settings =
      File.cd!(other_root, fn ->
        Ferricstore.DevRuntime.settings(Path.join(root, "config"))
      end)

    assert settings.data_dir == Path.join(root, "data")
  end

  test "relative explicit data and port overrides resolve from the checkout and warn", %{
    root: root
  } do
    File.write!(Path.join(root, ".git"), "gitdir: /tmp/git/worktrees/demo\n")

    settings =
      Ferricstore.DevRuntime.settings(Path.join(root, "config"),
        data_dir: "custom-dev-data",
        native_port: 6389,
        health_port: 4002,
        health_probe_port: 4003,
        http_port: 8081
      )

    assert settings.data_dir == Path.join(root, "custom-dev-data")
    refute settings.isolated?
    assert settings.native_port == 6389
    assert settings.health_port == 4002
    assert settings.health_probe_port == 4003
    assert settings.http_port == 8081

    assert settings.explicit_overrides == [
             :data_dir,
             :health_port,
             :health_probe_port,
             :http_port,
             :native_port
           ]

    assert settings.isolation_warning =~ "can defeat linked-worktree isolation"
  end

  test "explicit environment overrides are honored and normalized", %{root: root} do
    File.write!(Path.join(root, ".git"), "gitdir: /tmp/git/worktrees/demo\n")
    System.put_env("FERRICSTORE_DATA_DIR", "env-dev-data")
    System.put_env("FERRICSTORE_NATIVE_PORT", "6390")

    settings = Ferricstore.DevRuntime.settings(Path.join(root, "config"))

    assert settings.data_dir == Path.join(root, "env-dev-data")
    assert settings.native_port == 6390
    assert settings.explicit_overrides == [:data_dir, :native_port]

    assert settings.isolation_warning ==
             "explicit listener and data_dir overrides can defeat linked-worktree isolation"
  end

  test "invalid ports and empty data paths are rejected", %{root: root} do
    File.write!(Path.join(root, ".git"), "gitdir: /tmp/git/worktrees/demo\n")
    config_dir = Path.join(root, "config")

    assert_raise ArgumentError, ~r/native_port must be an integer/, fn ->
      Ferricstore.DevRuntime.settings(config_dir, native_port: 65_536)
    end

    assert_raise ArgumentError, ~r/data_dir must not be empty/, fn ->
      Ferricstore.DevRuntime.settings(config_dir, data_dir: " ")
    end
  end

  test "dev config exposes the worktree marker and standalone discovery policy" do
    config = Config.Reader.read!(@config_path, env: :dev, target: :host)
    ferricstore = Keyword.fetch!(config, :ferricstore)

    assert Keyword.fetch!(ferricstore, :data_dir) ==
             Path.expand(Path.join(@config_dir, "../data"))

    assert Keyword.fetch!(ferricstore, :dev_worktree?) ==
             Ferricstore.DevRuntime.linked_worktree?(Path.expand("..", @config_dir))

    assert Keyword.fetch!(ferricstore, :node_name) == nil
    assert Keyword.fetch!(ferricstore, :cluster_nodes) == []
    assert Keyword.fetch!(ferricstore, :cluster_auto_join) == false
    assert Keyword.fetch!(Keyword.fetch!(config, :libcluster), :topologies) == :disabled

    http = Keyword.fetch!(config, :ferricstore_http)
    expected_http_port = if Keyword.fetch!(ferricstore, :dev_worktree?), do: 0, else: 8080
    assert Keyword.fetch!(http, :port) == expected_http_port
  end
end
