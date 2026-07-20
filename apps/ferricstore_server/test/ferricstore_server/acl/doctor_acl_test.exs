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

  test "flow attribute discovery commands are read ACL commands" do
    assert {:ok, read} = CommandCategories.category_commands("READ")
    assert {:ok, flow} = CommandCategories.category_commands("FLOW")

    assert MapSet.member?(read, "FLOW.ATTRIBUTES")
    assert MapSet.member?(read, "FLOW.ATTRIBUTE_VALUES")
    assert MapSet.member?(read, "FLOW.SEARCH")
    assert MapSet.member?(read, "FLOW.QUERY")
    assert MapSet.member?(flow, "FLOW.ATTRIBUTES")
    assert MapSet.member?(flow, "FLOW.ATTRIBUTE_VALUES")
    assert MapSet.member?(flow, "FLOW.SEARCH")
    assert MapSet.member?(flow, "FLOW.QUERY")
  end

  test "flow effect governance commands have read/write ACL categories" do
    assert {:ok, read} = CommandCategories.category_commands("READ")
    assert {:ok, write} = CommandCategories.category_commands("WRITE")
    assert {:ok, flow} = CommandCategories.category_commands("FLOW")

    assert MapSet.member?(read, "FLOW.EFFECT.GET")
    assert MapSet.member?(flow, "FLOW.EFFECT.GET")
    assert MapSet.member?(read, "FLOW.GOVERNANCE.LEDGER")
    assert MapSet.member?(flow, "FLOW.GOVERNANCE.LEDGER")
    assert MapSet.member?(read, "FLOW.GOVERNANCE.OVERVIEW")
    assert MapSet.member?(flow, "FLOW.GOVERNANCE.OVERVIEW")
    assert MapSet.member?(read, "FLOW.APPROVAL.GET")
    assert MapSet.member?(flow, "FLOW.APPROVAL.GET")
    assert MapSet.member?(read, "FLOW.APPROVAL.LIST")
    assert MapSet.member?(flow, "FLOW.APPROVAL.LIST")
    assert MapSet.member?(read, "FLOW.CIRCUIT.GET")
    assert MapSet.member?(flow, "FLOW.CIRCUIT.GET")
    assert MapSet.member?(read, "FLOW.BUDGET.GET")
    assert MapSet.member?(flow, "FLOW.BUDGET.GET")
    assert MapSet.member?(read, "FLOW.BUDGET.LIST")
    assert MapSet.member?(flow, "FLOW.BUDGET.LIST")
    assert MapSet.member?(read, "FLOW.LIMIT.GET")
    assert MapSet.member?(flow, "FLOW.LIMIT.GET")
    assert MapSet.member?(read, "FLOW.LIMIT.LIST")
    assert MapSet.member?(flow, "FLOW.LIMIT.LIST")

    for command <- [
          "FLOW.EFFECT.RESERVE",
          "FLOW.EFFECT.CONFIRM",
          "FLOW.EFFECT.FAIL",
          "FLOW.EFFECT.COMPENSATE",
          "FLOW.APPROVAL.REQUEST",
          "FLOW.APPROVAL.APPROVE",
          "FLOW.APPROVAL.REJECT",
          "FLOW.CIRCUIT.OPEN",
          "FLOW.CIRCUIT.CLOSE",
          "FLOW.BUDGET.RESERVE",
          "FLOW.LIMIT.LEASE",
          "FLOW.LIMIT.SPEND",
          "FLOW.LIMIT.RELEASE"
        ] do
      assert MapSet.member?(write, command)
      assert MapSet.member?(flow, command)
    end
  end
end
