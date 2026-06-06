defmodule Ferricstore.Flow.ChildPolicyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.ChildPolicy

  test "validate_children requires a non-empty list" do
    assert ChildPolicy.validate_children([%{id: "child"}]) == :ok

    assert ChildPolicy.validate_children([]) ==
             {:error, "ERR flow children must be a non-empty list"}
  end

  test "validate_no_parent_child_id rejects parent id as child id" do
    assert ChildPolicy.validate_no_parent_child_id("parent", [%{id: "child"}]) == :ok

    assert ChildPolicy.validate_no_parent_child_id("parent", [%{id: "parent"}]) ==
             {:error, "ERR flow child id must differ from parent id"}
  end

  test "optional_policy normalizes binary values and rejects unsupported values" do
    assert ChildPolicy.optional_policy(
             [on_child_failure: "fail-parent"],
             :on_child_failure,
             :ignore,
             [
               :ignore,
               :fail_parent
             ]
           ) == {:ok, :fail_parent}

    assert ChildPolicy.optional_policy([on_child_failure: "bad"], :on_child_failure, :ignore, [
             :ignore,
             :fail_parent
           ]) == {:error, "ERR flow on_child_failure has unsupported value"}
  end

  test "exhaust_to_opts accepts explicit map or success/failure options" do
    assert ChildPolicy.exhaust_to_opts(success: "done", failure: "failed") ==
             {:ok, %{"success" => "done", "failure" => "failed"}}

    assert ChildPolicy.exhaust_to_opts(exhaust_to: %{success: "done", failure: "failed"}) ==
             {:ok, %{"success" => "done", "failure" => "failed"}}

    assert ChildPolicy.exhaust_to_opts(exhaust_to: %{"success" => "done", "failure" => "failed"}) ==
             {:ok, %{"success" => "done", "failure" => "failed"}}
  end

  test "exhaust_to_opts rejects missing or empty states" do
    assert ChildPolicy.exhaust_to_opts([]) ==
             {:error, "ERR flow exhaust_to must include success and failure states"}

    assert ChildPolicy.exhaust_to_opts(success: "", failure: "failed") ==
             {:error, "ERR flow exhaust_to must include success and failure states"}
  end
end
