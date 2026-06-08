defmodule FerricstoreServer.ACL.DoctorACLTest do
  use ExUnit.Case, async: true
  @moduletag :acl

  alias FerricstoreServer.Acl.CommandCategories

  test "FERRICSTORE.DOCTOR is admin and dangerous for ACL/UI permission hints" do
    assert {:ok, admin} = CommandCategories.category_commands("ADMIN")
    assert {:ok, dangerous} = CommandCategories.category_commands("DANGEROUS")

    assert MapSet.member?(admin, "FERRICSTORE.DOCTOR")
    assert MapSet.member?(dangerous, "FERRICSTORE.DOCTOR")
  end
end
