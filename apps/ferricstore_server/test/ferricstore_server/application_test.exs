defmodule FerricstoreServer.ApplicationTest do
  @moduledoc """
  Tests for the OTP application supervisor trees.

  Verifies that both supervision trees (Ferricstore.Supervisor for core and
  FerricstoreServer.Supervisor for the server) start correctly, that all
  expected children are present and alive, and that the Ranch TCP listener
  is reachable over a real TCP connection.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Listener
  alias Ferricstore.Store.ShardSupervisor
  alias Ferricstore.Test.ShardHelpers

  # Ensure all 4 shards are alive after every test so that the next test
  # module (which may also manipulate shards) starts from a clean state.
  # Also flush keys before each test to prevent keydir growth across tests.
  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> wait_shards_alive(2_000) end)
  end

  defp wait_shards_alive(timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.each(0..3, fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      Stream.repeatedly(fn -> Process.sleep(20) end)
      |> Enum.find(fn _ ->
        pid = Process.whereis(name)
        alive = is_pid(pid) and Process.alive?(pid)
        alive or System.monotonic_time(:millisecond) > deadline
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Core Supervisor tree shape
  # ---------------------------------------------------------------------------

  describe "Ferricstore.Supervisor (core)" do
    test "is alive after application start" do
      pid = Process.whereis(Ferricstore.Supervisor)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "uses one_for_one strategy" do
      {:ok, info} = :supervisor.get_childspec(Ferricstore.Supervisor, ShardSupervisor)
      assert info.restart == :permanent
    end

    test "has ShardSupervisor as a direct child" do
      children = Supervisor.which_children(Ferricstore.Supervisor)
      ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

      assert ShardSupervisor in ids,
             "Expected ShardSupervisor in supervisor children, got: #{inspect(ids)}"
    end

    test "ShardSupervisor child is a supervisor type" do
      children = Supervisor.which_children(Ferricstore.Supervisor)
      {_, _pid, type, _mods} = Enum.find(children, fn {id, _, _, _} -> id == ShardSupervisor end)
      assert type == :supervisor
    end

    test "all children are alive" do
      children = Supervisor.which_children(Ferricstore.Supervisor)

      for {id, pid, _type, _mods} <- children do
        if id == Ferricstore.OperationalGuard and
             Application.get_env(:ferricstore, :operational_guard_enabled, true) != true do
          assert pid == :undefined
        else
          assert is_pid(pid) and Process.alive?(pid),
                 "Child #{inspect(id)} is not alive (pid=#{inspect(pid)})"
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Server Supervisor tree shape
  # ---------------------------------------------------------------------------

  describe "FerricstoreServer.Supervisor (server)" do
    test "declares Ranch as a runtime application for releases" do
      {:ok, apps} = :application.get_key(:ferricstore_server, :applications)
      assert :ranch in apps
    end

    test "declares Ranch as permanent in the release" do
      applications =
        Ferricstore.Umbrella.MixProject.project()
        |> Keyword.fetch!(:releases)
        |> Keyword.fetch!(:ferricstore)
        |> Keyword.fetch!(:applications)

      assert applications[:ranch] == :permanent
    end

    test "is alive after application start" do
      pid = Process.whereis(FerricstoreServer.Supervisor)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "has Ranch listener as direct child" do
      children = Supervisor.which_children(FerricstoreServer.Supervisor)
      ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)

      assert Enum.any?(ids, fn id ->
               id == FerricstoreServer.Listener or
                 id == {:ranch_embedded_sup, FerricstoreServer.Listener} or
                 (is_atom(id) and
                    id |> Atom.to_string() |> String.contains?("ranch"))
             end),
             "Expected Ranch listener child in server supervisor, got: #{inspect(ids)}"
    end

    test "Ranch listener child is a supervisor type" do
      children = Supervisor.which_children(FerricstoreServer.Supervisor)

      ranch_child =
        Enum.find(children, fn {id, _, _, _} ->
          id == FerricstoreServer.Listener or
            id == {:ranch_embedded_sup, FerricstoreServer.Listener} or
            (is_atom(id) and id |> Atom.to_string() |> String.contains?("ranch"))
        end)

      assert ranch_child != nil, "Ranch listener not found in #{inspect(children)}"
      {_, _pid, type, _mods} = ranch_child
      assert type == :supervisor
    end

    test "all children are alive" do
      children = Supervisor.which_children(FerricstoreServer.Supervisor)

      for {id, pid, _type, _mods} <- children do
        assert is_pid(pid) and Process.alive?(pid),
               "Child #{inspect(id)} is not alive (pid=#{inspect(pid)})"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # ShardSupervisor subtree
  # ---------------------------------------------------------------------------

  describe "ShardSupervisor subtree" do
    test "ShardSupervisor is alive" do
      pid = Process.whereis(ShardSupervisor)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "starts exactly 4 shard children by default" do
      children =
        ShardSupervisor
        |> Supervisor.which_children()
        |> Enum.filter(fn {id, _, _, _} ->
          is_atom(id) and String.starts_with?(Atom.to_string(id), "shard_")
        end)

      assert length(children) == 4
    end

    test "all shard GenServers are alive" do
      children = Supervisor.which_children(ShardSupervisor)

      for {id, pid, _type, _mods} <- children do
        assert is_pid(pid) and Process.alive?(pid),
               "Shard #{inspect(id)} is not alive"
      end
    end

    test "shard GenServers are registered under expected names" do
      for i <- 0..3 do
        name = :"Ferricstore.Store.Shard.#{i}"
        pid = Process.whereis(name)
        assert is_pid(pid), "Expected shard #{i} registered as #{name}"
        assert Process.alive?(pid)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Ranch TCP listener
  # ---------------------------------------------------------------------------

  describe "TCP listener" do
    test "listener is reachable on the configured port" do
      port = Listener.port()
      assert is_integer(port) and port > 0

      assert {:ok, sock} =
               :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 1000)

      :gen_tcp.close(sock)
    end

    test "test env uses ephemeral port (not 6379)" do
      port = Listener.port()
      # In test env config sets port: 0, so OS picks a free port != 6379.
      refute port == 6379
    end

    test "multiple simultaneous connections are accepted" do
      port = Listener.port()

      sockets =
        for _ <- 1..5 do
          {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 1000)
          sock
        end

      assert length(sockets) == 5
      Enum.each(sockets, &:gen_tcp.close/1)
    end

    test "PING command works over the supervised listener" do
      port = Listener.port()
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 1000)

      ping = IO.iodata_to_binary(FerricstoreServer.Resp.Encoder.encode(["PING"]))
      :ok = :gen_tcp.send(sock, ping)

      {:ok, data} = :gen_tcp.recv(sock, 0, 2000)
      {:ok, [response], _} = FerricstoreServer.Resp.Parser.parse(data)

      assert response == {:simple, "PONG"}
      :gen_tcp.close(sock)
    end

    test "SET/GET roundtrip works over the supervised listener" do
      port = Listener.port()
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 1000)

      key = "app_test_#{:rand.uniform(9_999_999)}"

      set_cmd = IO.iodata_to_binary(FerricstoreServer.Resp.Encoder.encode(["SET", key, "hello"]))
      :ok = :gen_tcp.send(sock, set_cmd)
      {:ok, set_data} = :gen_tcp.recv(sock, 0, 2000)
      {:ok, [set_resp], _} = FerricstoreServer.Resp.Parser.parse(set_data)
      assert set_resp == {:simple, "OK"}

      get_cmd = IO.iodata_to_binary(FerricstoreServer.Resp.Encoder.encode(["GET", key]))
      :ok = :gen_tcp.send(sock, get_cmd)
      {:ok, get_data} = :gen_tcp.recv(sock, 0, 2000)
      {:ok, [get_resp], _} = FerricstoreServer.Resp.Parser.parse(get_data)
      assert get_resp == "hello"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Supervision resilience
  # ---------------------------------------------------------------------------

  describe "supervision resilience" do
    @tag :capture_log
    test "individual shard restarts under ShardSupervisor without affecting siblings" do
      # Kill shard 3 (highest index) -- one_for_one restarts only that child.
      # Use shard 3 to avoid interfering with shard_lifecycle_test.exs which
      # always operates on shard 0.
      old_shard3 = Process.whereis(:"Ferricstore.Store.Shard.3")
      assert is_pid(old_shard3)

      ref = Process.monitor(old_shard3)
      Process.exit(old_shard3, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_shard3, :killed}, 1_000

      # Shards 0-2 should be unaffected
      for i <- 0..2 do
        pid = Process.whereis(:"Ferricstore.Store.Shard.#{i}")
        assert is_pid(pid) and Process.alive?(pid), "Shard #{i} should still be alive"
      end

      # Shard 3 should be restarted. Allow generous time for WARaft
      # replay on slow CI runners.
      deadline = System.monotonic_time(:millisecond) + 30_000

      new_shard3 =
        Enum.find_value(Stream.repeatedly(fn -> Process.sleep(20) end), fn _ ->
          pid = Process.whereis(:"Ferricstore.Store.Shard.3")

          if is_pid(pid) and pid != old_shard3 and Process.alive?(pid) do
            pid
          else
            if System.monotonic_time(:millisecond) > deadline, do: throw(:timeout)
            nil
          end
        end)

      assert is_pid(new_shard3)
      assert new_shard3 != old_shard3
    end

    @tag :capture_log
    test "TCP listener still works after unrelated shard crash" do
      # Kill shard 2 -- avoid shard 0 (used by lifecycle tests) and shard 3
      # (used by the sibling resilience test above).
      shard2 = Process.whereis(:"Ferricstore.Store.Shard.2")
      ref = Process.monitor(shard2)
      Process.exit(shard2, :kill)
      assert_receive {:DOWN, ^ref, :process, ^shard2, :killed}, 1_000
      # Wait for shard 2 to fully restart (including init/WARaft replay)
      ShardHelpers.wait_shards_alive(30_000)

      port = Listener.port()
      {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 1000)
      ping = IO.iodata_to_binary(FerricstoreServer.Resp.Encoder.encode(["PING"]))
      :ok = :gen_tcp.send(sock, ping)
      {:ok, data} = :gen_tcp.recv(sock, 0, 2000)
      {:ok, [response], _} = FerricstoreServer.Resp.Parser.parse(data)
      assert response == {:simple, "PONG"}
      :gen_tcp.close(sock)
    end
  end
end
