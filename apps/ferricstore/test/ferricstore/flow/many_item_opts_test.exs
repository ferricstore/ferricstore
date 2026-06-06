defmodule Ferricstore.Flow.ManyItemOptsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.ManyItemOpts

  test "create parses binary tuple and map item shapes" do
    assert ManyItemOpts.create("flow-1") == {:ok, "flow-1", []}
    assert ManyItemOpts.create({"flow-1", [payload: "p"]}) == {:ok, "flow-1", [payload: "p"]}

    assert ManyItemOpts.create({:id, "flow-1", :payload_ref, "ref"}) ==
             {:ok, "flow-1", [payload_ref: "ref"]}

    assert {:ok, "flow-1", opts} =
             ManyItemOpts.create(%{"id" => "flow-1", "type" => "email", "partition_key" => "p"})

    assert opts[:type] == "email"
    assert opts[:partition_key] == "p"
  end

  test "terminal item parsers preserve map values and lease tuple shapes" do
    item = %{id: "flow-1", lease_token: "lease", fencing_token: 7, result: "ok", payload: "p"}
    assert {:ok, "flow-1", "lease", opts} = ManyItemOpts.complete(item)
    assert opts[:fencing_token] == 7
    assert opts[:result] == "ok"
    assert opts[:payload] == "p"

    assert ManyItemOpts.retry({:id, "flow-1", :lease_token, "lease", :fencing_token, 7}) ==
             {:ok, "flow-1", "lease", [fencing_token: 7]}

    assert ManyItemOpts.fail({"flow-1", [lease_token: "lease", fencing_token: 7]}) ==
             {:ok, "flow-1", "lease", [lease_token: "lease", fencing_token: 7]}
  end

  test "cancel and transition preserve optional lease and partition keys" do
    assert ManyItemOpts.cancel(%{"id" => "flow-1", "fencing_token" => 3, "partition_key" => "p"}) ==
             {:ok, "flow-1", [fencing_token: 3, partition_key: "p"]}

    assert ManyItemOpts.transition(
             {:id, "flow-1", :partition_key, "p", :fencing_token, 3, :lease_token, nil}
           ) ==
             {:ok, "flow-1", [partition_key: "p", fencing_token: 3]}
  end

  test "merge applies item opts over base opts and pins partition key" do
    opts = ManyItemOpts.merge([type: "email", partition_key: "base"], [state: "queued"], "item")

    assert opts[:type] == "email"
    assert opts[:state] == "queued"
    assert opts[:partition_key] == "item"
  end

  test "invalid item opts keep existing error messages" do
    assert ManyItemOpts.create({"flow-1", [:bad]}) ==
             {:error, "ERR flow opts must be a keyword list"}

    assert ManyItemOpts.complete({"flow-1", [fencing_token: 1]}) ==
             {:error, "ERR flow lease_token must be a non-empty string"}

    assert ManyItemOpts.transition(:bad) == {:error, "ERR flow id must be a non-empty string"}
  end
end
