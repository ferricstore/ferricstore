defmodule FerricstoreServer.Commands.ClientTest do
  @moduledoc false
  use ExUnit.Case, async: false

  alias FerricstoreServer.Commands.Client
  alias FerricstoreServer.ClientTracking
  alias Ferricstore.Test.MockStore

  setup do
    # Ensure tracking ETS tables exist for TRACKING tests
    ClientTracking.init_tables()

    try do
      :ets.delete_all_objects(:ferricstore_tracking)
      :ets.delete_all_objects(:ferricstore_tracking_connections)
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp make_conn_state(overrides \\ %{}) do
    Map.merge(
      %{
        client_id: 42,
        client_name: nil,
        created_at: System.monotonic_time(:millisecond) - 5_000,
        peer: {{127, 0, 0, 1}, 12_345},
        conn_pid: self(),
        tracking: ClientTracking.new_config()
      },
      overrides
    )
  end

  defp store, do: MockStore.make()

  # ---------------------------------------------------------------------------
  # CLIENT ID
  # ---------------------------------------------------------------------------

  describe "CLIENT ID" do
    test "returns the client_id as an integer" do
      conn = make_conn_state(%{client_id: 99})
      {result, _conn} = Client.handle("ID", [], conn, store())
      assert result == 99
      assert is_integer(result)
    end

    test "does not modify connection state" do
      conn = make_conn_state()
      {_result, updated} = Client.handle("ID", [], conn, store())
      assert updated == conn
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("ID", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT SETNAME / GETNAME round trip
  # ---------------------------------------------------------------------------

  describe "CLIENT SETNAME" do
    test "sets the client name and returns :ok" do
      conn = make_conn_state()
      {result, updated} = Client.handle("SETNAME", ["myconn"], conn, store())
      assert result == :ok
      assert updated.client_name == "myconn"
    end

    test "rejects names with spaces" do
      conn = make_conn_state()
      {{:error, msg}, _updated} = Client.handle("SETNAME", ["my conn"], conn, store())
      assert msg =~ "cannot contain spaces"
    end

    test "with no args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("SETNAME", [], conn, store())
      assert msg =~ "wrong number of arguments"
    end

    test "with too many args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("SETNAME", ["a", "b"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "CLIENT GETNAME" do
    test "returns nil when no name is set" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("GETNAME", [], conn, store())
      assert result == nil
    end

    test "returns the name after SETNAME" do
      conn = make_conn_state()
      {:ok, conn2} = Client.handle("SETNAME", ["myconn"], conn, store())
      {result, _conn3} = Client.handle("GETNAME", [], conn2, store())
      assert result == "myconn"
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("GETNAME", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  describe "CLIENT SETNAME / GETNAME round trip" do
    test "set then get returns the set name" do
      conn = make_conn_state()
      {:ok, conn2} = Client.handle("SETNAME", ["test-connection"], conn, store())
      {name, _conn3} = Client.handle("GETNAME", [], conn2, store())
      assert name == "test-connection"
    end

    test "can overwrite a previously set name" do
      conn = make_conn_state()
      {:ok, conn2} = Client.handle("SETNAME", ["first"], conn, store())
      {:ok, conn3} = Client.handle("SETNAME", ["second"], conn2, store())
      {name, _conn4} = Client.handle("GETNAME", [], conn3, store())
      assert name == "second"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT INFO
  # ---------------------------------------------------------------------------

  describe "CLIENT INFO" do
    test "returns a formatted string with connection details" do
      conn = make_conn_state(%{client_id: 7, client_name: nil})
      {result, _conn} = Client.handle("INFO", [], conn, store())

      assert is_binary(result)
      assert result =~ "id=7"
      assert result =~ "addr=127.0.0.1:12345"
      assert result =~ "name="
      assert result =~ "age="
    end

    test "includes client name when set" do
      conn = make_conn_state(%{client_name: "myconn"})
      {result, _conn} = Client.handle("INFO", [], conn, store())
      assert result =~ "name=myconn"
    end

    test "age reflects elapsed time" do
      # Connection created 10 seconds ago
      conn = make_conn_state(%{created_at: System.monotonic_time(:millisecond) - 10_000})
      {result, _conn} = Client.handle("INFO", [], conn, store())

      # Extract age value
      case Regex.run(~r/age=(\d+)/, result) do
        [_, age_str] ->
          {age, ""} = Integer.parse(age_str)
          assert age >= 9 and age <= 12

        nil ->
          flunk("Expected age= in CLIENT INFO output")
      end
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("INFO", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT LIST
  # ---------------------------------------------------------------------------

  describe "CLIENT LIST" do
    test "returns info for current connection" do
      conn = make_conn_state(%{client_id: 3})
      {result, _conn} = Client.handle("LIST", [], conn, store())

      assert is_binary(result)
      assert result =~ "id=3"
    end

    test "TYPE normal returns info" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "normal"], conn, store())
      assert is_binary(result)
      assert result =~ "id="
    end

    test "TYPE master returns empty string" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "master"], conn, store())
      assert result == ""
    end

    test "TYPE replica returns empty string" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "replica"], conn, store())
      assert result == ""
    end

    test "TYPE pubsub returns empty string" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("LIST", ["TYPE", "pubsub"], conn, store())
      assert result == ""
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT TRACKING ON
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKING ON" do
    test "enables default-mode tracking and returns OK" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["ON"], conn, store())

      assert updated.tracking.enabled == true
      assert updated.tracking.mode == :default
    end

    test "enables BCAST mode tracking" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["ON", "BCAST"], conn, store())

      assert updated.tracking.enabled == true
      assert updated.tracking.mode == :bcast
    end

    test "enables BCAST mode with PREFIX arguments" do
      conn = make_conn_state()

      {:ok, updated} =
        Client.handle(
          "TRACKING",
          ["ON", "BCAST", "PREFIX", "user:", "PREFIX", "session:"],
          conn,
          store()
        )

      assert updated.tracking.enabled == true
      assert updated.tracking.mode == :bcast
      assert updated.tracking.prefixes == ["user:", "session:"]
    end

    test "enables tracking with OPTIN option" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["ON", "OPTIN"], conn, store())

      assert updated.tracking.enabled == true
      assert updated.tracking.optin == true
      assert updated.tracking.caching == false
    end

    test "enables tracking with OPTOUT option" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["ON", "OPTOUT"], conn, store())

      assert updated.tracking.enabled == true
      assert updated.tracking.optout == true
    end

    test "enables tracking with NOLOOP option" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["ON", "NOLOOP"], conn, store())

      assert updated.tracking.enabled == true
      assert updated.tracking.noloop == true
    end

    test "enables tracking with REDIRECT option" do
      # Spawn a process to act as the redirect target
      redirect_pid = spawn(fn -> Process.sleep(5000) end)
      conn = make_conn_state()

      {:ok, updated} =
        Client.handle(
          "TRACKING",
          ["ON", "REDIRECT", inspect(redirect_pid)],
          conn,
          store()
        )

      assert updated.tracking.enabled == true
      assert updated.tracking.redirect != nil
    end

    test "enables tracking with multiple options" do
      conn = make_conn_state()

      {:ok, updated} =
        Client.handle(
          "TRACKING",
          ["ON", "BCAST", "PREFIX", "key:", "NOLOOP"],
          conn,
          store()
        )

      assert updated.tracking.enabled == true
      assert updated.tracking.mode == :bcast
      assert updated.tracking.prefixes == ["key:"]
      assert updated.tracking.noloop == true
    end

    test "returns error when OPTIN and OPTOUT are both specified" do
      conn = make_conn_state()

      {{:error, msg}, _conn} =
        Client.handle("TRACKING", ["ON", "OPTIN", "OPTOUT"], conn, store())

      assert msg =~ "mutually exclusive"
    end

    test "returns error when OPTIN is used with BCAST" do
      conn = make_conn_state()

      {{:error, msg}, _conn} =
        Client.handle("TRACKING", ["ON", "BCAST", "OPTIN"], conn, store())

      assert msg =~ "not compatible with BCAST"
    end

    test "returns error when OPTOUT is used with BCAST" do
      conn = make_conn_state()

      {{:error, msg}, _conn} =
        Client.handle("TRACKING", ["ON", "BCAST", "OPTOUT"], conn, store())

      assert msg =~ "not compatible with BCAST"
    end

    test "case-insensitive ON keyword" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["on"], conn, store())
      assert updated.tracking.enabled == true
    end

    test "case-insensitive option keywords" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["ON", "bcast", "noloop"], conn, store())
      assert updated.tracking.mode == :bcast
      assert updated.tracking.noloop == true
    end

    test "PREFIX without BCAST returns error" do
      conn = make_conn_state()

      {{:error, msg}, _conn} =
        Client.handle("TRACKING", ["ON", "PREFIX", "foo:"], conn, store())

      assert msg =~ "PREFIX"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT TRACKING OFF
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKING OFF" do
    test "disables tracking and returns OK" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON"], conn, store())
      assert conn_on.tracking.enabled == true

      {:ok, conn_off} = Client.handle("TRACKING", ["OFF"], conn_on, store())
      assert conn_off.tracking.enabled == false
    end

    test "OFF is idempotent when tracking is already disabled" do
      conn = make_conn_state()
      {:ok, updated} = Client.handle("TRACKING", ["OFF"], conn, store())
      assert updated.tracking.enabled == false
    end

    test "case-insensitive OFF keyword" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON"], conn, store())
      {:ok, conn_off} = Client.handle("TRACKING", ["off"], conn_on, store())
      assert conn_off.tracking.enabled == false
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT TRACKING with no ON/OFF
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKING with invalid toggle" do
    test "returns error when no ON/OFF specified" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("TRACKING", [], conn, store())
      assert msg =~ "wrong number of arguments" or msg =~ "syntax error"
    end

    test "returns error for invalid toggle value" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("TRACKING", ["MAYBE"], conn, store())
      assert msg =~ "syntax error" or msg =~ "ON or OFF"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT CACHING YES|NO
  # ---------------------------------------------------------------------------

  describe "CLIENT CACHING" do
    test "YES sets caching to true for OPTIN mode" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON", "OPTIN"], conn, store())
      assert conn_on.tracking.caching == false

      {:ok, conn_caching} = Client.handle("CACHING", ["YES"], conn_on, store())
      assert conn_caching.tracking.caching == true
    end

    test "NO sets caching to false for OPTOUT mode" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON", "OPTOUT"], conn, store())

      {:ok, conn_no} = Client.handle("CACHING", ["NO"], conn_on, store())
      assert conn_no.tracking.caching == false
    end

    test "returns error when tracking is not enabled" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("CACHING", ["YES"], conn, store())
      assert msg =~ "CLIENT CACHING can be called only after"
    end

    test "returns error when neither OPTIN nor OPTOUT is active" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON"], conn, store())

      {{:error, msg}, _conn} = Client.handle("CACHING", ["YES"], conn_on, store())
      assert msg =~ "OPTIN or OPTOUT"
    end

    test "case-insensitive YES" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON", "OPTIN"], conn, store())
      {:ok, updated} = Client.handle("CACHING", ["yes"], conn_on, store())
      assert updated.tracking.caching == true
    end

    test "case-insensitive NO" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON", "OPTOUT"], conn, store())
      {:ok, updated} = Client.handle("CACHING", ["no"], conn_on, store())
      assert updated.tracking.caching == false
    end

    test "returns error for invalid value" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON", "OPTIN"], conn, store())

      {{:error, msg}, _conn} = Client.handle("CACHING", ["MAYBE"], conn_on, store())
      assert msg =~ "YES or NO"
    end

    test "returns error with no arguments" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("CACHING", [], conn, store())
      assert msg =~ "wrong number of arguments"
    end

    test "returns error with too many arguments" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("CACHING", ["YES", "extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT TRACKINGINFO
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKINGINFO" do
    test "returns tracking info when tracking is disabled" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("TRACKINGINFO", [], conn, store())

      assert is_map(result)
      assert result["flags"] == ["off"]
      assert result["redirect"] == -1
      assert result["prefixes"] == []
    end

    test "returns tracking info with OPTIN and NOLOOP enabled" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON", "OPTIN", "NOLOOP"], conn, store())

      {result, _conn} = Client.handle("TRACKINGINFO", [], conn_on, store())

      assert is_map(result)
      assert "optin" in result["flags"]
      assert "noloop" in result["flags"]
      assert result["redirect"] == -1
    end

    test "returns tracking info with BCAST prefixes" do
      conn = make_conn_state()

      {:ok, conn_on} =
        Client.handle(
          "TRACKING",
          ["ON", "BCAST", "PREFIX", "user:", "PREFIX", "session:"],
          conn,
          store()
        )

      {result, _conn} = Client.handle("TRACKINGINFO", [], conn_on, store())

      assert result["prefixes"] == ["user:", "session:"]
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("TRACKINGINFO", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT GETREDIR
  # ---------------------------------------------------------------------------

  describe "CLIENT GETREDIR" do
    test "returns 0 when no redirect is set" do
      conn = make_conn_state()
      {result, _conn} = Client.handle("GETREDIR", [], conn, store())
      assert result == 0
    end

    test "returns redirect ID when redirect is set" do
      redirect_pid = spawn(fn -> Process.sleep(5000) end)
      conn = make_conn_state()

      {:ok, conn_on} =
        Client.handle(
          "TRACKING",
          ["ON", "REDIRECT", inspect(redirect_pid)],
          conn,
          store()
        )

      {result, _conn} = Client.handle("GETREDIR", [], conn_on, store())
      assert is_integer(result)
      assert result != 0
    end

    test "with extra args returns error" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("GETREDIR", ["extra"], conn, store())
      assert msg =~ "wrong number of arguments"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT KILL
  # ---------------------------------------------------------------------------

  describe "CLIENT KILL" do
    setup do
      FerricstoreServer.Connection.Registry.init_table()
      :ok
    end

    test "kills a registered client by ID" do
      parent = self()

      target =
        spawn(fn ->
          receive do
            :client_kill -> send(parent, :client_killed)
          end
        end)

      conn = make_conn_state(%{conn_pid: self()})

      FerricstoreServer.Connection.Registry.register(9001, target)

      assert {:ok, ^conn} = Client.handle("KILL", ["ID", "9001"], conn, store())
      assert_receive :client_killed

      Process.exit(target, :kill)
    end

    test "rejects killing the current client" do
      conn = make_conn_state(%{client_id: 9002, conn_pid: self()})
      FerricstoreServer.Connection.Registry.register(9002, self())

      {{:error, msg}, ^conn} = Client.handle("KILL", ["ID", "9002"], conn, store())
      assert msg =~ "won't kill myself"
    end

    test "returns an error for missing client ID" do
      conn = make_conn_state()

      {{:error, msg}, ^conn} = Client.handle("KILL", ["ID", "999999999"], conn, store())
      assert msg =~ "No such client"
    end

    test "rejects non-integer IDs" do
      conn = make_conn_state()

      {{:error, msg}, ^conn} = Client.handle("KILL", ["ID", "abc"], conn, store())
      assert msg =~ "not an integer"
    end

    test "rejects unsupported forms" do
      conn = make_conn_state()

      {{:error, msg}, ^conn} = Client.handle("KILL", [], conn, store())
      assert msg =~ "syntax error"
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT TRACKING round-trip: ON then OFF clears ETS
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKING ON then OFF clears ETS state" do
    test "enable then disable removes connection from ETS" do
      conn = make_conn_state()
      {:ok, conn_on} = Client.handle("TRACKING", ["ON"], conn, store())

      # Verify connection is registered
      assert :ets.lookup(:ferricstore_tracking_connections, self()) != []

      {:ok, _conn_off} = Client.handle("TRACKING", ["OFF"], conn_on, store())

      # Verify connection is removed
      assert :ets.lookup(:ferricstore_tracking_connections, self()) == []
    end
  end

  # ---------------------------------------------------------------------------
  # Unknown subcommand
  # ---------------------------------------------------------------------------

  describe "CLIENT unknown subcommand" do
    test "returns error with subcommand name" do
      conn = make_conn_state()
      {{:error, msg}, _conn} = Client.handle("BOGUS", [], conn, store())
      assert msg =~ "unknown subcommand"
      assert msg =~ "BOGUS"
    end
  end
end
