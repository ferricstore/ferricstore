defmodule FerricstoreServer.AclAuthEpochPersistenceTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.AuthEpoch
  alias FerricstoreServer.Acl.FileParser

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    Acl.reset!()

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-acl-epoch-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(tmp_dir)

    on_exit(fn ->
      Acl.reset!()
      File.rm_rf!(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "old ACL files load with fresh epochs and saved metadata round-trips", %{tmp_dir: dir} do
    live_epoch = Acl.get_user("default").auth_epoch

    old_contents = """
    user default on nopass ~* &* +@all
    user alice on nopass ~* &* +info
    """

    assert :ok = Acl.load_contents(old_contents)
    assert Acl.get_user("default").auth_epoch > live_epoch
    saved_alice_epoch = Acl.get_user("alice").auth_epoch

    assert :ok = Acl.save(dir)
    contents = File.read!(Acl.acl_file_path(dir))
    assert contents =~ "# FerricStore ACL auth epoch "
    assert contents =~ "# FerricStore ACL user auth epoch "

    assert {:ok, users} = FileParser.parse(contents)
    assert {:ok, restored_users, restored_global_epoch} = AuthEpoch.restore(contents, users, 0)

    assert restored_users |> Map.new() |> get_in(["alice", :auth_epoch]) == saved_alice_epoch
    assert restored_global_epoch >= saved_alice_epoch
  end

  test "ACL LOAD and delete/recreate never lower the live epoch", %{tmp_dir: dir} do
    assert :ok = Acl.set_user("alice", ["on", "nopass", "~*", "+info"])
    assert :ok = Acl.save(dir)
    saved_epoch = Acl.get_user("alice").auth_epoch

    assert :ok = Acl.set_user("alice", ["+config"])
    mutated_epoch = Acl.get_user("alice").auth_epoch
    assert mutated_epoch > saved_epoch

    assert :ok = Acl.load(dir)
    loaded_epoch = Acl.get_user("alice").auth_epoch
    assert loaded_epoch > mutated_epoch

    assert :ok = Acl.del_user("alice")
    assert :ok = Acl.set_user("alice", ["on", "nopass", "~*", "+info"])
    assert Acl.get_user("alice").auth_epoch > loaded_epoch
  end
end
