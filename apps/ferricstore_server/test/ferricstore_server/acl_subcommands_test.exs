defmodule FerricstoreServer.AclSubcommandsTest do
  @moduledoc "ACL CAT, GENPASS, LOG and DRYRUN, as Redis answers them."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias FerricstoreServer.Acl.Protection

  # The server wires its ACL adapter at startup; another suite in the same
  # run may have reset it.
  setup do
    previous = Application.get_env(:ferricstore, FerricStore.Management.ACL)

    Application.put_env(
      :ferricstore,
      FerricStore.Management.ACL,
      FerricstoreServer.Management.ACL
    )

    on_exit(fn ->
      if previous,
        do: Application.put_env(:ferricstore, FerricStore.Management.ACL, previous),
        else: Application.delete_env(:ferricstore, FerricStore.Management.ACL)
    end)

    ctx = FerricStore.Instance.get(:default)
    %{acl: fn args -> Dispatcher.dispatch("ACL", args, ctx) end}
  end

  test "CAT lists the categories and a category's commands", %{acl: acl} do
    categories = acl.(["CAT"])
    assert "keyspace" in categories and "string" in categories and "read" in categories

    strings = acl.(["CAT", "String"])
    assert "get" in strings and "lcs" in strings
    refute "hset" in strings

    assert {:error, "ERR Unknown category 'nope'"} == acl.(["CAT", "nope"])

    assert {:error, "ERR wrong number of arguments for 'acl|cat' command"} ==
             acl.(["CAT", "a", "b"])
  end

  test "GENPASS answers random hex of the bits asked for", %{acl: acl} do
    password = acl.(["GENPASS"])
    assert password =~ ~r/\A[0-9a-f]{64}\z/
    refute password == acl.(["GENPASS"])
    assert acl.(["GENPASS", "5"]) =~ ~r/\A[0-9a-f]{2}\z/

    for bits <- ["0", "4097", "x"] do
      assert {:error,
              "ERR ACL GENPASS argument must be the number of bits for the output password, " <>
                "a positive number up to 4096"} == acl.(["GENPASS", bits])
    end
  end

  test "LOG answers denials newest first, merges repeats, and RESET clears it", %{acl: acl} do
    :ok = acl.(["LOG", "RESET"])
    assert [] == acl.(["LOG"])

    Protection.log_command_denied("alice", "SET", "10.0.0.1:5000", 7)
    Protection.log_command_denied("alice", "SET", "10.0.0.1:5000", 7)

    Protection.log_command_denied(
      "bob",
      "GET",
      "10.0.0.2:6000",
      8,
      "NOPERM this user has no permissions to access one of the keys mentioned in the command"
    )

    assert [bob, alice] = acl.(["LOG"])
    assert %{"reason" => "key", "object" => "get", "username" => "bob", "count" => 1} = bob

    assert %{"reason" => "command", "object" => "set", "username" => "alice", "count" => 2} =
             alice

    assert alice["client-info"] =~ "id=7"
    assert alice["context"] == "toplevel"
    assert is_float(alice["age-seconds"])
    assert alice["timestamp-last-updated"] >= alice["timestamp-created"]

    assert [%{"username" => "bob"}] = acl.(["LOG", "1"])

    assert {:error, "ERR value is out of range, must be positive"} == acl.(["LOG", "-1"])
    assert :ok == acl.(["LOG", "RESET"])
    assert [] == acl.(["LOG"])
  end

  test "DRYRUN checks a user's command and keys without running it", %{acl: acl} do
    user = "dry#{System.unique_integer([:positive])}"
    :ok = acl.(["SETUSER", user, "on", ">secret-pw", "+get", "~allowed:*"])
    on_exit(fn -> acl.(["DELUSER", user]) end)

    assert "OK" == acl.(["DRYRUN", user, "GET", "allowed:1"])

    assert "User #{user} has no permissions to run the 'set' command" ==
             acl.(["DRYRUN", user, "SET", "allowed:1", "v"])

    assert "User #{user} has no permissions to access the 'other:1' key" ==
             acl.(["DRYRUN", user, "GET", "other:1"])

    assert {:error, "ERR User 'nobody-#{user}' not found"} ==
             acl.(["DRYRUN", "nobody-#{user}", "GET", "x"])

    assert {:error, "ERR Command 'nopecmd' not found"} ==
             acl.(["DRYRUN", user, "NOPECMD"])

    assert {:error, "ERR wrong number of arguments for 'acl|dryrun' command"} ==
             acl.(["DRYRUN", user])
  end
end
