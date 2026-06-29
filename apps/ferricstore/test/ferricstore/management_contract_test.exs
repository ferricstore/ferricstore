defmodule FerricStore.ManagementContractTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.NativeAstParser
  alias Ferricstore.Test.MockStore

  defmodule FakeACL do
    @behaviour FerricStore.Management.ACL

    @impl true
    def set_user(username, rules, opts),
      do: {:ok, %{username: username, rules: rules, store?: Keyword.has_key?(opts, :store)}}

    @impl true
    def del_user(username, _opts), do: {:ok, %{deleted: username}}

    @impl true
    def get_user(username, _opts), do: {:ok, %{username: username}}

    @impl true
    def list_users(_opts), do: {:ok, ["user default on ~* &* +@all"]}

    @impl true
    def save(_opts), do: :ok
  end

  defmodule FakeCapabilities do
    @behaviour FerricStore.ManagementCapabilities

    @impl true
    def capabilities(_opts), do: %{acl_management: true, quota_management: true}
  end

  defmodule FakeResourceLimits do
    @behaviour FerricStore.ResourceLimits

    @impl true
    def set_limit(scope, limit_spec, opts),
      do: {:ok, %{scope: scope, limit: limit_spec, store?: Keyword.has_key?(opts, :store)}}

    @impl true
    def get_limit(scope, opts),
      do: {:ok, %{scope: scope, limit: %{"keys" => 10}, store?: Keyword.has_key?(opts, :store)}}

    @impl true
    def usage(scope, opts),
      do: {:ok, %{scope: scope, usage: %{"keys" => 0}, store?: Keyword.has_key?(opts, :store)}}

    @impl true
    def check(_scope, _resource, _amount, _opts), do: :ok

    @impl true
    def reserve(scope, resource, amount, _opts),
      do: {:ok, {scope, resource, amount}}

    @impl true
    def release(_reservation, _opts), do: :ok
  end

  defmodule FakeNamespace do
    @behaviour FerricStore.Management.Namespace

    @impl true
    def ensure_namespace(prefix, opts),
      do: {:ok, %{prefix: prefix, store?: Keyword.has_key?(opts, :store)}}

    @impl true
    def get_namespace(prefix), do: {:ok, %{prefix: prefix}}

    @impl true
    def list_namespaces, do: {:ok, [%{prefix: "tenant"}]}

    @impl true
    def delete_namespace(prefix, opts),
      do: {:ok, %{deleted: prefix, store?: Keyword.has_key?(opts, :store)}}
  end

  setup do
    Application.delete_env(:ferricstore, FerricStore.ManagementCapabilities)
    Application.delete_env(:ferricstore, FerricStore.Management.ACL)
    Application.delete_env(:ferricstore, FerricStore.Management.Namespace)
    Application.delete_env(:ferricstore, FerricStore.ResourceLimits)

    on_exit(fn ->
      Application.delete_env(:ferricstore, FerricStore.ManagementCapabilities)
      Application.delete_env(:ferricstore, FerricStore.Management.ACL)
      Application.delete_env(:ferricstore, FerricStore.Management.Namespace)
      Application.delete_env(:ferricstore, FerricStore.ResourceLimits)
    end)
  end

  test "default management capabilities expose OSS-safe feature flags" do
    assert FerricStore.ManagementCapabilities.capabilities() == %{
             sdk: true,
             health: true,
             telemetry: true,
             acl_management: false,
             namespace_management: false,
             quota_management: false,
             flow_observability: true
           }
  end

  test "FERRICSTORE.CAPABILITIES returns string-keyed SDK contract" do
    store = MockStore.make()

    assert Dispatcher.dispatch("FERRICSTORE.CAPABILITIES", [], store) == %{
             "sdk" => true,
             "health" => true,
             "telemetry" => true,
             "acl_management" => false,
             "namespace_management" => false,
             "quota_management" => false,
             "flow_observability" => true
           }
  end

  test "capabilities can be extended by a configured implementation" do
    Application.put_env(:ferricstore, FerricStore.ManagementCapabilities, FakeCapabilities)

    assert Dispatcher.dispatch("FERRICSTORE.CAPABILITIES", [], MockStore.make()) == %{
             "sdk" => true,
             "health" => true,
             "telemetry" => true,
             "acl_management" => true,
             "namespace_management" => false,
             "quota_management" => true,
             "flow_observability" => true
           }
  end

  test "management mutations fail closed in OSS" do
    store = MockStore.make()

    assert {:error, "ERR unsupported management command"} =
             Dispatcher.dispatch("ACL", ["SETUSER", "platform_read_abcd", "on", "+PING"], store)

    assert {:error, "ERR unsupported management command"} =
             Dispatcher.dispatch("FERRICSTORE.NAMESPACE", ["ENSURE", "tenant:namespace"], store)

    assert {:error, "ERR unsupported management command"} =
             Dispatcher.dispatch(
               "FERRICSTORE.QUOTA",
               ["SET", "tenant:namespace", "KEYS", "10"],
               store
             )
  end

  test "resource limit enforcement hooks are neutral no-ops by default" do
    assert :ok = FerricStore.ResourceLimits.check("tenant:namespace", :keys, 1)
    assert {:ok, nil} = FerricStore.ResourceLimits.reserve("tenant:namespace", :ops_per_sec, 1)
    assert :ok = FerricStore.ResourceLimits.release(nil)
  end

  test "FERRICSTORE.QUOTA delegates to configured resource limit implementation" do
    Application.put_env(:ferricstore, FerricStore.ResourceLimits, FakeResourceLimits)

    assert %{"scope" => "tenant", "limit" => %{"keys" => 10}, "store?" => true} =
             Dispatcher.dispatch(
               "FERRICSTORE.QUOTA",
               ["SET", "tenant", "KEYS", "10"],
               MockStore.make()
             )

    assert %{"scope" => "tenant", "limit" => %{"keys" => 10}, "store?" => true} =
             Dispatcher.dispatch("FERRICSTORE.QUOTA", ["GET", "tenant"], MockStore.make())

    assert %{"scope" => "tenant", "usage" => %{"keys" => 0}, "store?" => true} =
             Dispatcher.dispatch("FERRICSTORE.QUOTA", ["USAGE", "tenant"], MockStore.make())
  end

  test "FERRICSTORE.NAMESPACE delegates mutations with caller store opts" do
    Application.put_env(:ferricstore, FerricStore.Management.Namespace, FakeNamespace)

    assert %{"prefix" => "tenant", "store?" => true} =
             Dispatcher.dispatch("FERRICSTORE.NAMESPACE", ["ENSURE", "tenant"], MockStore.make())

    assert %{"deleted" => "tenant", "store?" => true} =
             Dispatcher.dispatch("FERRICSTORE.NAMESPACE", ["DELETE", "tenant"], MockStore.make())
  end

  test "ACL command delegates to configured implementation" do
    Application.put_env(:ferricstore, FerricStore.Management.ACL, FakeACL)

    assert %{
             "username" => "platform_write_abcd",
             "rules" => ["on", "+@write", "-@dangerous"],
             "store?" => true
           } =
             Dispatcher.dispatch(
               "ACL",
               [
                 "SETUSER",
                 "platform_write_abcd",
                 "on",
                 "+@write",
                 "-@dangerous"
               ],
               MockStore.make()
             )

    assert :ok = Dispatcher.dispatch("ACL", ["SAVE"], MockStore.make())
  end

  test "native AST parser recognizes management command names" do
    assert {:ok, "FERRICSTORE.CAPABILITIES", [], {:ferricstore_capabilities, []}, []} =
             NativeAstParser.parse("FERRICSTORE.CAPABILITIES", [])

    assert {:ok, "ACL", ["SETUSER", "u", "on"], {:acl, "SETUSER", ["u", "on"]}, []} =
             NativeAstParser.parse("ACL", ["SETUSER", "u", "on"])

    assert {:ok, "FERRICSTORE.QUOTA", ["GET", "tenant"], {:ferricstore_quota, ["GET", "tenant"]},
            ["tenant:*"]} =
             NativeAstParser.parse("FERRICSTORE.QUOTA", ["GET", "tenant"])

    assert {:ok, "FERRICSTORE.NAMESPACE", ["ENSURE", "tenant:a"],
            {:ferricstore_namespace, ["ENSURE", "tenant:a"]}, ["tenant:a:*"]} =
             NativeAstParser.parse("FERRICSTORE.NAMESPACE", ["ENSURE", "tenant:a"])

    assert {:ok, "FERRICSTORE.TELEMETRY", ["FLOW_QUERY", "PARTITION", "tenant:a"],
            {:ferricstore_telemetry, ["FLOW_QUERY", "PARTITION", "tenant:a"]}, ["tenant:a:*"]} =
             NativeAstParser.parse("FERRICSTORE.TELEMETRY", [
               "FLOW_QUERY",
               "PARTITION",
               "tenant:a"
             ])

    assert {:ok, "FERRICSTORE.TELEMETRY", ["FLOW_QUERY"],
            {:ferricstore_telemetry, ["FLOW_QUERY"]}, ["*"]} =
             NativeAstParser.parse("FERRICSTORE.TELEMETRY", ["FLOW_QUERY"])
  end
end
