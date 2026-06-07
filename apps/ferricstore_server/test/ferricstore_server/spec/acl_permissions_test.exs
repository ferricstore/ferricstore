Code.require_file("acl_permissions_test/sections/part_01.exs", __DIR__)
Code.require_file("acl_permissions_test/sections/part_02.exs", __DIR__)
defmodule FerricstoreServer.Spec.AclPermissionsTest do
  @moduledoc """
  Tests for ACL command-level permission enforcement.

  Verifies that the `FerricstoreServer.Acl.check_command/2` function correctly
  enforces command-level restrictions based on user ACL rules:

    - `+@all` grants access to every command
    - `-@all` denies access to every command
    - `+command` grants access to a specific command
    - `-command` denies access to a specific command
    - `+@category` / `-@category` grants/denies categories of commands

  Also verifies that `FerricstoreServer.Connection` integrates the check
  at dispatch time, returning NOPERM errors over TCP when a user is denied.

  These tests are `async: false` because they share the global ACL ETS table
  and the Config GenServer (for requirepass).
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  use FerricstoreServer.Spec.AclPermissionsTest.Sections.Part01

  use FerricstoreServer.Spec.AclPermissionsTest.Sections.Part02

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp connect_and_auth(port, username, password) do
    sock = connect_and_hello(port)
    send_cmd(sock, ["AUTH", username, password])
    resp = recv_response(sock)
    {sock, resp}
  end

  defp parsed_command_keys(wire) do
    assert {:ok, [{:command, cmd, _args, _ast, keys}], ""} = Parser.parse_commands(wire)
    {cmd, keys}
  end

  defp parser_supported_commands do
    parser_path =
      Path.expand(
        "../../../../ferricstore/native/resp_parser_nif/src/lib.rs",
        __DIR__
      )

    parser_source = File.read!(parser_path)

    parser_ast_commands =
      parser_source
      |> parser_match_block!(
        ~r/fn classify_command_ast\(cmd: &\[u8\], arity: usize\).*?match cmd \{(.*?)\n\s*_ => CommandAstKind::Unknown,/s
      )
      |> parser_command_literals()

    parser_tag_commands =
      parser_source
      |> parser_match_block!(
        ~r/fn command_tag_name\(cmd: &\[u8\]\).*?match cmd \{(.*?)\n\s*}\n}/s
      )
      |> parser_command_literals()

    MapSet.union(parser_ast_commands, parser_tag_commands)
  end

  defp parser_match_block!(source, pattern) do
    [_all, command_tag_block] =
      Regex.run(
        pattern,
        source
      )

    command_tag_block
  end

  defp parser_command_literals(block) do
    ~r/b"([A-Z0-9_.]+)"\s*=>\s*Some/
    |> Regex.scan(block, capture: :all_but_first)
    |> then(fn tag_commands ->
      ast_commands =
        ~r/b"([A-Z0-9_.]+)"(?:\s+if\s+.*?)?\s*=>\s*CommandAstKind::/
        |> Regex.scan(block, capture: :all_but_first)

      tag_commands ++ ast_commands
    end)
    |> List.flatten()
    |> MapSet.new()
  end

  # Sets requirepass for tests that need AUTH. Registers on_exit cleanup.
  defp enable_requirepass do
    Ferricstore.Config.set("requirepass", "testpass")

    on_exit(fn ->
      Ferricstore.Config.set("requirepass", "")
      Acl.reset!()
    end)
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    Acl.reset!()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Unit tests: Acl.check_command/2
  # ---------------------------------------------------------------------------





  # ---------------------------------------------------------------------------
  # Unit tests: Acl.check_command/2 with categories
  # ---------------------------------------------------------------------------






  # ---------------------------------------------------------------------------
  # TCP integration tests: NOPERM enforcement over the wire
  # ---------------------------------------------------------------------------







  # ---------------------------------------------------------------------------
  # Edge case: AUTH changes permissions mid-connection
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Stress test: 1000 commands with permission checking
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # Unit tests: category definitions
  # ---------------------------------------------------------------------------


  # ---------------------------------------------------------------------------
  # NOPERM error message format
  # ---------------------------------------------------------------------------

end
