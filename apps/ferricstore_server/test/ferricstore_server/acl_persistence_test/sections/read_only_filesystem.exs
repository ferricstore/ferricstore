defmodule FerricstoreServer.AclPersistenceTest.Sections.ReadOnlyFilesystem do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Acl

  describe "read-only filesystem" do
    test "save returns error on permission denied", %{tmp_dir: dir} do
      # Make directory read-only
      File.chmod!(dir, 0o444)

      result = Acl.save(dir)
      assert {:error, msg} = result
      assert msg =~ "ACL save failed"

      # Restore permissions for cleanup
      File.chmod!(dir, 0o755)
    end
  end
  describe "command serialization" do
    test "explicit command list round-trips correctly", %{tmp_dir: dir} do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+get", "+set", "+del"])
      assert :ok = Acl.save(dir)

      Acl.reset!()
      assert :ok = Acl.load(dir)

      user = Acl.get_user("alice")
      assert MapSet.member?(user.commands, "GET")
      assert MapSet.member?(user.commands, "SET")
      assert MapSet.member?(user.commands, "DEL")
      assert MapSet.size(user.commands) == 3
    end

    test "allcommands round-trips correctly", %{tmp_dir: dir} do
      assert :ok = Acl.set_user("alice", ["on", "allcommands"])
      assert :ok = Acl.save(dir)

      Acl.reset!()
      assert :ok = Acl.load(dir)

      user = Acl.get_user("alice")
      assert user.commands == :all
    end

    test "nocommands user round-trips (empty command set)", %{tmp_dir: dir} do
      assert :ok = Acl.set_user("alice", ["on", "-@all"])
      assert :ok = Acl.save(dir)

      contents = File.read!(Acl.acl_file_path(dir))
      # User with no commands should not have +@all in the file
      alice_line =
        contents |> String.split("\n") |> Enum.find(&String.contains?(&1, "user alice"))

      refute alice_line =~ "+@all"

      Acl.reset!()
      assert :ok = Acl.load(dir)

      user = Acl.get_user("alice")
      assert user.commands == MapSet.new()
    end
  end
    end
  end
end
