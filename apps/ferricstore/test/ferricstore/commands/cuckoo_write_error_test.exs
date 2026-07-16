defmodule Ferricstore.Commands.CuckooWriteErrorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Cuckoo

  test "CF.ADD and CF.ADDNX do not misreport storage failures as capacity exhaustion" do
    store = %{prob_write: fn _command -> {:error, :eio} end}

    assert {:error, "ERR cuckoo add failed: :eio"} ==
             Cuckoo.handle("CF.ADD", ["filter", "item"], store)

    assert {:error, "ERR cuckoo add failed: :eio"} ==
             Cuckoo.handle("CF.ADDNX", ["filter", "item"], store)
  end

  test "actual capacity exhaustion retains the filter-full response" do
    store = %{prob_write: fn _command -> {:error, "filter is full"} end}

    assert {:error, "ERR filter is full"} ==
             Cuckoo.handle("CF.ADD", ["filter", "item"], store)

    assert {:error, "ERR filter is full"} ==
             Cuckoo.handle("CF.ADDNX", ["filter", "item"], store)
  end
end
